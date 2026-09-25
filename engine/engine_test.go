package engine

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- 测试辅助 ---

// recorder 记录实际调度（开始执行）顺序，供日志打印与断言。
type recorder struct {
	mu    sync.Mutex
	order []string
}

func (r *recorder) add(id string) {
	r.mu.Lock()
	r.order = append(r.order, id)
	r.mu.Unlock()
}

func (r *recorder) get() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

// logGraph 打印任务图与拓扑序。
func logGraph(t *testing.T, g *Graph) {
	t.Helper()
	order, err := g.TopoOrder()
	if err != nil {
		t.Logf("任务图拓扑序不可用: %v", err)
		return
	}
	tasks := g.Tasks()
	var b strings.Builder
	for _, id := range order {
		fmt.Fprintf(&b, "  节点 %-4s 依赖=%v\n", id, tasks[id].Deps)
	}
	t.Logf("任务图（拓扑序 %v）:\n%s", order, b.String())
}

// logVerdict 打印判定依据。
func logVerdict(t *testing.T, out *Output, err error) {
	t.Helper()
	t.Logf("判定依据: Complete=%v overallErr=%v", out.Complete, err)
	for _, r := range out.Ordered() {
		t.Logf("  节点 %-4s 状态=%-8s 值=%v 错误=%v", r.ID, r.Status, r.Value, r.Err)
	}
}

// buildDiamond 构造菱形+尾链图：a -> {b,c} -> d -> e，e 汇总。
func buildDiamond(rec *recorder, inject map[string]error) *Graph {
	g := NewGraph()
	mk := func(id string, deps ...string) Task {
		return Task{
			ID:   id,
			Deps: deps,
			Run: func(ctx context.Context, inputs map[string]any) (any, error) {
				rec.add(id)
				if err := inject[id]; err != nil {
					return nil, err
				}
				sum := 0
				keys := make([]string, 0, len(inputs))
				for k := range inputs {
					keys = append(keys, k)
				}
				sort.Strings(keys) // 与依赖完成顺序无关
				for _, k := range keys {
					sum += inputs[k].(int)
				}
				return sum + 1, nil
			},
		}
	}
	for _, args := range []struct {
		id   string
		deps []string
	}{
		{"a", nil}, {"b", []string{"a"}}, {"c", []string{"a"}},
		{"d", []string{"b", "c"}}, {"e", []string{"d"}},
	} {
		if err := g.Add(mk(args.id, args.deps...)); err != nil {
			panic(err)
		}
	}
	return g
}

// --- 用例 ---

func TestDeterministicAcrossConcurrency(t *testing.T) {
	var want string
	for _, conc := range []int{1, 2, 3, 8} {
		for run := 0; run < 5; run++ {
			rec := &recorder{}
			g := buildDiamond(rec, nil)
			if conc == 1 && run == 0 {
				logGraph(t, g)
			}
			out, err := New(Options{MaxConcurrency: conc, QueueSize: 1}).Execute(context.Background(), g)
			if err != nil {
				t.Fatalf("conc=%d run=%d: %v", conc, run, err)
			}
			var b strings.Builder
			for _, r := range out.Ordered() {
				fmt.Fprintf(&b, "%s=%v;", r.ID, r.Value)
			}
			got := b.String()
			if want == "" {
				want = got
				t.Logf("基准结果: %s (调度顺序 %v)", want, rec.get())
			}
			if got != want {
				t.Fatalf("conc=%d run=%d 结果不一致: got %s want %s (调度顺序 %v)",
					conc, run, got, want, rec.get())
			}
			t.Logf("conc=%d run=%d 结果一致: %s (调度顺序 %v)", conc, run, got, rec.get())
		}
	}
}

func TestBoundedConcurrencyAndBackpressure(t *testing.T) {
	const limit = 3
	var active, maxActive atomic.Int64
	g := NewGraph()
	for i := 0; i < 200; i++ { // 输入规模远大于缓冲
		id := fmt.Sprintf("n%03d", i)
		if err := g.Add(Task{
			ID: id,
			Run: func(ctx context.Context, _ map[string]any) (any, error) {
				cur := active.Add(1)
				for {
					m := maxActive.Load()
					if cur <= m || maxActive.CompareAndSwap(m, cur) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				active.Add(-1)
				return 1, nil
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	logGraph(t, g)
	out, err := New(Options{MaxConcurrency: limit, QueueSize: 2}).Execute(context.Background(), g)
	if err != nil {
		t.Fatal(err)
	}
	logVerdict(t, out, err)
	t.Logf("判定依据: 峰值并发=%d 上限=%d", maxActive.Load(), limit)
	if maxActive.Load() > limit {
		t.Fatalf("并发超限: max=%d limit=%d", maxActive.Load(), limit)
	}
	if !out.Complete || len(out.Results) != 200 {
		t.Fatalf("结果不完整: complete=%v n=%d", out.Complete, len(out.Results))
	}
}

func TestIllegalGraphRejected(t *testing.T) {
	// 成环
	cyc := NewGraph()
	mk := func(id string, deps ...string) Task {
		return Task{ID: id, Deps: deps, Run: func(context.Context, map[string]any) (any, error) { return 1, nil }}
	}
	_ = cyc.Add(mk("a", "c"))
	_ = cyc.Add(mk("b", "a"))
	_ = cyc.Add(mk("c", "b"))
	out, err := New(Options{}).Execute(context.Background(), cyc)
	t.Logf("成环判定: out=%v err=%v", out, err)
	if !errors.Is(err, ErrCycle) || out != nil {
		t.Fatalf("成环应被拒绝且可区分: out=%v err=%v", out, err)
	}

	// 依赖不存在
	miss := NewGraph()
	_ = miss.Add(mk("a", "ghost"))
	out, err = New(Options{}).Execute(context.Background(), miss)
	t.Logf("缺依赖判定: out=%v err=%v", out, err)
	if !errors.Is(err, ErrMissingDep) || out != nil {
		t.Fatalf("缺依赖应被拒绝且可区分: out=%v err=%v", out, err)
	}
	if errors.Is(err, ErrCycle) {
		t.Fatal("缺依赖不应误判为成环")
	}
}

func TestFailureSkipsDownstream(t *testing.T) {
	rec := &recorder{}
	boom := errors.New("boom")
	g := buildDiamond(rec, map[string]error{"b": boom})
	logGraph(t, g)
	out, err := New(Options{MaxConcurrency: 4}).Execute(context.Background(), g)
	logVerdict(t, out, err)
	t.Logf("调度顺序: %v", rec.get())

	if !errors.Is(err, ErrTaskFailed) {
		t.Fatalf("整体错误应归类为任务失败: %v", err)
	}
	if out.Complete {
		t.Fatal("存在失败节点时结果必须标记为不完整")
	}
	if r := out.Results["b"]; r.Status != StatusFailed || !errors.Is(r.Err, ErrTaskFailed) {
		t.Fatalf("b 应为 failed: %+v", r)
	}
	for _, id := range []string{"d", "e"} {
		if r := out.Results[id]; r.Status != StatusSkipped || !errors.Is(r.Err, ErrUpstreamFailed) {
			t.Fatalf("%s 应因上游失败被跳过: %+v", id, r)
		}
	}
	if _, ok := out.Get("a"); ok {
		t.Fatal("不完整结果的成功项不得通过 Get 混出")
	}
	if errors.Is(err, ErrTaskTimeout) || errors.Is(err, ErrCanceled) {
		t.Fatalf("失败原因不得混淆: %v", err)
	}
}

func TestTaskTimeout(t *testing.T) {
	g := NewGraph()
	_ = g.Add(Task{
		ID:      "slow",
		Timeout: 20 * time.Millisecond,
		Run: func(ctx context.Context, _ map[string]any) (any, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Second):
				return 1, nil
			}
		},
	})
	_ = g.Add(Task{
		ID:   "down",
		Deps: []string{"slow"},
		Run:  func(context.Context, map[string]any) (any, error) { return 2, nil },
	})
	logGraph(t, g)
	out, err := New(Options{}).Execute(context.Background(), g)
	logVerdict(t, out, err)
	if !errors.Is(err, ErrTaskTimeout) {
		t.Fatalf("应归类为超时: %v", err)
	}
	if r := out.Results["slow"]; r.Status != StatusTimeout {
		t.Fatalf("slow 应为 timeout: %+v", r)
	}
	if r := out.Results["down"]; r.Status != StatusSkipped {
		t.Fatalf("down 应被跳过: %+v", r)
	}
	if errors.Is(err, ErrTaskFailed) {
		t.Fatalf("超时不应误判为普通失败: %v", err)
	}
}

func TestCancellation(t *testing.T) {
	g := NewGraph()
	started := make(chan struct{})
	var once sync.Once
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("n%d", i)
		_ = g.Add(Task{
			ID: id,
			Run: func(ctx context.Context, _ map[string]any) (any, error) {
				once.Do(func() { close(started) })
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(10 * time.Second):
					return 1, nil
				}
			},
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-started; cancel() }()
	done := make(chan error, 1)
	var out *Output
	go func() {
		var err error
		out, err = New(Options{MaxConcurrency: 2}).Execute(ctx, g)
		done <- err
	}()
	select {
	case err := <-done:
		logVerdict(t, out, err)
		if !errors.Is(err, ErrCanceled) {
			t.Fatalf("取消应归类为 ErrCanceled: %v", err)
		}
		if out.Complete {
			t.Fatal("取消后不得报成成功")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("取消后执行卡死")
	}
}

func TestDedup(t *testing.T) {
	var calls atomic.Int64
	mk := func() *Graph {
		g := NewGraph()
		_ = g.Add(Task{
			ID:    "x",
			Input: 42,
			Run: func(context.Context, map[string]any) (any, error) {
				calls.Add(1)
				return 7, nil
			},
		})
		return g
	}
	e := New(Options{Dedup: true})
	out1, err1 := e.Execute(context.Background(), mk())
	out2, err2 := e.Execute(context.Background(), mk())
	t.Logf("判定依据: 首次=%v/%v 重复=%v/%v 实际计算次数=%d",
		out1.Complete, err1, out2.Complete, err2, calls.Load())
	if err1 != nil || err2 != nil {
		t.Fatalf("err1=%v err2=%v", err1, err2)
	}
	if calls.Load() != 1 {
		t.Fatalf("重复提交应只计算一次, 实际 %d 次", calls.Load())
	}
	v, ok := out2.Get("x")
	if !ok || v.(int) != 7 {
		t.Fatalf("去重命中应返回缓存结果: %v %v", v, ok)
	}
}

func TestNoGoroutineLeak(t *testing.T) {
	base := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		rec := &recorder{}
		if _, err := New(Options{MaxConcurrency: 4}).Execute(context.Background(), buildDiamond(rec, nil)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	got := runtime.NumGoroutine()
	t.Logf("判定依据: 基准 goroutine=%d 结束后=%d", base, got)
	if got > base {
		t.Fatalf("存在未回收的执行单元: base=%d now=%d", base, got)
	}
}

// TestFaultInjectionDeterminism 注入确定性故障序列，验证多次运行归类一致。
func TestFaultInjectionDeterminism(t *testing.T) {
	inject := map[string]error{"c": errors.New("injected fault")}
	var want string
	for run := 0; run < 5; run++ {
		rec := &recorder{}
		g := buildDiamond(rec, inject)
		out, err := New(Options{MaxConcurrency: 1 + run}).Execute(context.Background(), g)
		logVerdict(t, out, err)
		t.Logf("run=%d 调度顺序: %v", run, rec.get())
		var b strings.Builder
		for _, r := range out.Ordered() {
			fmt.Fprintf(&b, "%s:%s;", r.ID, r.Status)
		}
		if want == "" {
			want = b.String()
		} else if b.String() != want {
			t.Fatalf("run=%d 状态归类不一致: %s vs %s", run, b.String(), want)
		}
		if !errors.Is(err, ErrTaskFailed) || out.Complete {
			t.Fatalf("run=%d 归类错误: complete=%v err=%v", run, out.Complete, err)
		}
	}
	t.Logf("判定依据: 全部运行状态序列一致: %s", want)
}
