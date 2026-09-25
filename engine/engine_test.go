package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

// testLogger 把引擎调度事件打进测试日志（任务图、调度顺序、判定依据）。
func testLogger(t *testing.T) func(string, ...any) {
	t.Helper()
	return func(format string, args ...any) {
		t.Helper()
		t.Logf("[engine] "+format, args...)
	}
}

// diamond 构造菱形图：a -> {b, c} -> d，d 汇总全部上游。
func diamond() []Task {
	return []Task{
		{ID: "a", Run: func(ctx context.Context, deps map[string]any) (any, error) { return 1, nil }},
		{ID: "b", Deps: []string{"a"}, Run: func(ctx context.Context, deps map[string]any) (any, error) {
			return deps["a"].(int) + 10, nil
		}},
		{ID: "c", Deps: []string{"a"}, Run: func(ctx context.Context, deps map[string]any) (any, error) {
			return deps["a"].(int) * 2, nil
		}},
		{ID: "d", Deps: []string{"b", "c"}, Run: func(ctx context.Context, deps map[string]any) (any, error) {
			return deps["b"].(int) + deps["c"].(int), nil
		}},
	}
}

func valuesOf(res *RunResult) map[string]any {
	out := make(map[string]any, len(res.Results))
	for id, r := range res.Results {
		out[id] = r.Value
	}
	return out
}

// TestDeterministicAcrossConcurrency 验证：同一图在不同并发度、多次运行下结果完全一致。
func TestDeterministicAcrossConcurrency(t *testing.T) {
	want := map[string]any{"a": 1, "b": 11, "c": 2, "d": 13}
	for _, conc := range []int{1, 2, 4, 16} {
		for run := 0; run < 5; run++ {
			e := New(Config{MaxConcurrency: conc, BufferSize: 2, Logger: testLogger(t)})
			res := e.Run(context.Background(), diamond())
			if !res.Complete {
				t.Fatalf("conc=%d run=%d: expected complete, got err=%v kind=%q", conc, run, res.Err, res.Kind)
			}
			got := valuesOf(res)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("conc=%d run=%d: got %v want %v", conc, run, got, want)
			}
			t.Logf("verdict: conc=%d run=%d results=%v 与基准一致", conc, run, got)
		}
	}
}

// TestDeterministicRandomDAG 用确定性伪随机生成较大的 DAG，验证汇总结果稳定。
func TestDeterministicRandomDAG(t *testing.T) {
	const n = 200
	var tasks []Task
	// 线性同余伪随机，保证图结构本身确定。
	seed := 42
	rnd := func(m int) int { seed = (seed*1103515245 + 12345) & 0x7fffffff; return seed % m }
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("t%03d", i)
		var deps []string
		for j := 0; j < i; j++ {
			if rnd(100) < 5 {
				deps = append(deps, fmt.Sprintf("t%03d", j))
			}
		}
		tasks = append(tasks, Task{
			ID:   id,
			Deps: deps,
			Run: func(ctx context.Context, deps map[string]any) (any, error) {
				sum := 1
				for _, v := range deps {
					sum += v.(int)
				}
				return sum, nil
			},
		})
	}
	var want map[string]any
	for _, conc := range []int{1, 3, 8, 32} {
		e := New(Config{MaxConcurrency: conc, BufferSize: 4, Logger: testLogger(t)})
		res := e.Run(context.Background(), tasks)
		if !res.Complete {
			t.Fatalf("conc=%d: incomplete: %v", conc, res.Err)
		}
		got := valuesOf(res)
		if want == nil {
			want = got
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("conc=%d: results diverged", conc)
		}
		t.Logf("verdict: conc=%d 与基准结果一致（%d 个任务）", conc, len(got))
	}
}

// TestDedup 验证同一输入重复提交只计算一次。
func TestDedup(t *testing.T) {
	var calls int32
	mk := func() Task {
		return Task{ID: "x", Run: func(ctx context.Context, deps map[string]any) (any, error) {
			atomic.AddInt32(&calls, 1)
			return 7, nil
		}}
	}
	e := New(Config{MaxConcurrency: 4, Logger: testLogger(t)})
	res := e.Run(context.Background(), []Task{mk(), mk(), mk()})
	if !res.Complete {
		t.Fatalf("incomplete: %v", res.Err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("dedup failed: computed %d times, want 1", got)
	}
	if res.Results["x"].Value != 7 {
		t.Fatalf("unexpected value: %v", res.Results["x"].Value)
	}
	t.Logf("verdict: 3 次重复提交仅计算 1 次，去重生效")
}

// TestCycleRejected 验证成环图被明确拒绝且原因可区分。
func TestCycleRejected(t *testing.T) {
	noop := func(ctx context.Context, deps map[string]any) (any, error) { return nil, nil }
	tasks := []Task{
		{ID: "a", Deps: []string{"c"}, Run: noop},
		{ID: "b", Deps: []string{"a"}, Run: noop},
		{ID: "c", Deps: []string{"b"}, Run: noop},
	}
	e := New(Config{Logger: testLogger(t)})
	res := e.Run(context.Background(), tasks)
	if res.Complete {
		t.Fatal("cyclic graph must not complete")
	}
	if !errors.Is(res.Err, ErrCycle) {
		t.Fatalf("want ErrCycle, got %v", res.Err)
	}
	if res.Kind != FailureCycle {
		t.Fatalf("want kind %q, got %q", FailureCycle, res.Kind)
	}
	t.Logf("verdict: 成环被拒绝，kind=%q err=%v", res.Kind, res.Err)
}

// TestMissingDepRejected 验证引用不存在节点被明确拒绝。
func TestMissingDepRejected(t *testing.T) {
	tasks := []Task{
		{ID: "a", Deps: []string{"ghost"}, Run: func(ctx context.Context, deps map[string]any) (any, error) { return nil, nil }},
	}
	e := New(Config{Logger: testLogger(t)})
	res := e.Run(context.Background(), tasks)
	if res.Complete {
		t.Fatal("graph with missing dep must not complete")
	}
	if !errors.Is(res.Err, ErrMissingDep) {
		t.Fatalf("want ErrMissingDep, got %v", res.Err)
	}
	if res.Kind != FailureMissingDep {
		t.Fatalf("want kind %q, got %q", FailureMissingDep, res.Kind)
	}
	t.Logf("verdict: 缺失依赖被拒绝，kind=%q err=%v", res.Kind, res.Err)
}

// TestFailureSkipsDownstream 验证单节点失败后下游被跳过、部分结果标记为不完整。
func TestFailureSkipsDownstream(t *testing.T) {
	boom := errors.New("boom")
	tasks := []Task{
		{ID: "ok", Run: func(ctx context.Context, deps map[string]any) (any, error) { return "fine", nil }},
		{ID: "bad", Run: func(ctx context.Context, deps map[string]any) (any, error) { return nil, boom }},
		{ID: "child", Deps: []string{"bad"}, Run: func(ctx context.Context, deps map[string]any) (any, error) { return "never", nil }},
		{ID: "grandchild", Deps: []string{"child"}, Run: func(ctx context.Context, deps map[string]any) (any, error) { return "never", nil }},
	}
	e := New(Config{MaxConcurrency: 2, Logger: testLogger(t)})
	res := e.Run(context.Background(), tasks)

	if res.Complete {
		t.Fatal("run with failing node must be incomplete")
	}
	if res.Kind != FailureTaskError {
		t.Fatalf("want kind %q, got %q", FailureTaskError, res.Kind)
	}
	if res.Results["ok"].Status != StatusSuccess || res.Results["ok"].Value != "fine" {
		t.Fatalf("unaffected node must succeed: %+v", res.Results["ok"])
	}
	if res.Results["bad"].Status != StatusFailed || !errors.Is(res.Results["bad"].Err, boom) {
		t.Fatalf("failing node misclassified: %+v", res.Results["bad"])
	}
	for _, id := range []string{"child", "grandchild"} {
		if res.Results[id].Status != StatusSkipped {
			t.Fatalf("downstream %s must be skipped: %+v", id, res.Results[id])
		}
	}
	t.Logf("verdict: 失败已归类 kind=%q，下游 child/grandchild 被跳过，部分结果不完整", res.Kind)
}

// TestTimeout 验证单任务超时被归类为 timeout 且下游跳过。
func TestTimeout(t *testing.T) {
	tasks := []Task{
		{ID: "slow", Timeout: 50 * time.Millisecond, Run: func(ctx context.Context, deps map[string]any) (any, error) {
			select {
			case <-time.After(5 * time.Second):
				return "done", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}},
		{ID: "after", Deps: []string{"slow"}, Run: func(ctx context.Context, deps map[string]any) (any, error) { return "never", nil }},
	}
	e := New(Config{MaxConcurrency: 2, Logger: testLogger(t)})
	start := time.Now()
	res := e.Run(context.Background(), tasks)
	elapsed := time.Since(start)

	if res.Complete {
		t.Fatal("timed-out run must be incomplete")
	}
	if res.Kind != FailureTimeout {
		t.Fatalf("want kind %q, got %q (err=%v)", FailureTimeout, res.Kind, res.Err)
	}
	if !errors.Is(res.Results["slow"].Err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", res.Results["slow"].Err)
	}
	if res.Results["after"].Status != StatusSkipped {
		t.Fatalf("downstream must be skipped: %+v", res.Results["after"])
	}
	if elapsed > 2*time.Second {
		t.Fatalf("timeout not enforced promptly: %v", elapsed)
	}
	t.Logf("verdict: 超时归类 kind=%q，耗时 %v，下游已跳过", res.Kind, elapsed)
}

// TestCancel 验证整体取消：不卡死、不误报成功、无 goroutine 泄漏。
func TestCancel(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 8)
	tasks := []Task{
		{ID: "blocker", Run: func(ctx context.Context, deps map[string]any) (any, error) {
			started <- struct{}{}
			select {
			case <-release:
				return "released", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}},
		{ID: "downstream", Deps: []string{"blocker"}, Run: func(ctx context.Context, deps map[string]any) (any, error) { return "never", nil }},
	}
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	e := New(Config{MaxConcurrency: 2, Logger: testLogger(t)})
	done := make(chan *RunResult, 1)
	go func() { done <- e.Run(ctx, tasks) }()

	<-started
	cancel()

	select {
	case res := <-done:
		if res.Complete {
			t.Fatal("canceled run must not report success")
		}
		if res.Kind != FailureCanceled || !errors.Is(res.Err, ErrCanceled) {
			t.Fatalf("want canceled classification, got kind=%q err=%v", res.Kind, res.Err)
		}
		if res.Results["downstream"].Status != StatusSkipped {
			t.Fatalf("downstream must be skipped: %+v", res.Results["downstream"])
		}
		t.Logf("verdict: 取消归类 kind=%q，未误报成功", res.Kind)
	case <-time.After(3 * time.Second):
		t.Fatal("canceled run deadlocked")
	}
	close(release)

	// 等待 worker 退出后确认无 goroutine 泄漏。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: before=%d after=%d", before, runtime.NumGoroutine())
}

// TestBoundedConcurrency 验证活跃并行数不超过上限（背压）。
func TestBoundedConcurrency(t *testing.T) {
	var active, maxActive int32
	const n = 300
	var tasks []Task
	for i := 0; i < n; i++ {
		tasks = append(tasks, Task{
			ID: fmt.Sprintf("w%03d", i),
			Run: func(ctx context.Context, deps map[string]any) (any, error) {
				cur := atomic.AddInt32(&active, 1)
				for {
					old := atomic.LoadInt32(&maxActive)
					if cur <= old || atomic.CompareAndSwapInt32(&maxActive, old, cur) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				atomic.AddInt32(&active, -1)
				return 1, nil
			},
		})
	}
	const limit = 5
	e := New(Config{MaxConcurrency: limit, BufferSize: 2, Logger: testLogger(t)})
	res := e.Run(context.Background(), tasks)
	if !res.Complete {
		t.Fatalf("incomplete: %v", res.Err)
	}
	if got := atomic.LoadInt32(&maxActive); got > limit {
		t.Fatalf("concurrency exceeded: max=%d limit=%d", got, limit)
	}
	t.Logf("verdict: 峰值并发=%d，未超过上限 %d；%d 个任务在小缓冲下完成（背压有效）",
		maxActive, limit, n)
}

// TestScheduleOrderLogged 打印任务图与调度顺序，展示判定依据。
func TestScheduleOrderLogged(t *testing.T) {
	e := New(Config{MaxConcurrency: 3, Logger: testLogger(t)})
	res := e.Run(context.Background(), diamond())
	if !res.Complete {
		t.Fatalf("incomplete: %v", res.Err)
	}
	ids := make([]string, 0, len(res.Results))
	for id := range res.Results {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		t.Logf("final: task=%s status=%s value=%v", id, res.Results[id].Status, res.Results[id].Value)
	}
	t.Logf("verdict: 全部成功，Complete=%v", res.Complete)
}
