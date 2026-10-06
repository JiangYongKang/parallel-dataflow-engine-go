package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countedTask 构造一个带执行计数与版本标识的任务。
func countedTask(id, cacheKey string, deps []string, counter *int32, fn func(map[string]any) any) Task {
	return Task{
		ID:       id,
		Deps:     deps,
		CacheKey: cacheKey,
		Run: func(ctx context.Context, deps map[string]any) (any, error) {
			atomic.AddInt32(counter, 1)
			return fn(deps), nil
		},
	}
}

// chainGraph 构造 a -> b -> c 链外加独立节点 x，版本可配。
func chainGraph(bKey string, counters map[string]*int32) []Task {
	return []Task{
		countedTask("a", "a-v1", nil, counters["a"], func(deps map[string]any) any { return 1 }),
		countedTask("b", bKey, []string{"a"}, counters["b"], func(deps map[string]any) any {
			if bKey == "b-v2" {
				return deps["a"].(int) + 100
			}
			return deps["a"].(int) + 10
		}),
		countedTask("c", "c-v1", []string{"b"}, counters["c"], func(deps map[string]any) any {
			return deps["b"].(int) * 2
		}),
		countedTask("x", "x-v1", nil, counters["x"], func(deps map[string]any) any { return 99 }),
	}
}

func newCounters(ids ...string) map[string]*int32 {
	m := make(map[string]*int32, len(ids))
	for _, id := range ids {
		m[id] = new(int32)
	}
	return m
}

func execCount(counters map[string]*int32, id string) int {
	return int(atomic.LoadInt32(counters[id]))
}

// TestCacheReuseAcrossRuns 验证：同一张图在同一实例上连跑三次，
// 实际执行次数与只跑一次相同，后续运行全部复用命中。
func TestCacheReuseAcrossRuns(t *testing.T) {
	counters := newCounters("a", "b", "c", "x")
	e := New(Config{MaxConcurrency: 4, CacheSize: 16, Logger: testLogger(t)})

	var prev map[string]any
	for run := 1; run <= 3; run++ {
		res := e.Run(context.Background(), chainGraph("b-v1", counters))
		if !res.Complete {
			t.Fatalf("run=%d incomplete: %v", run, res.Err)
		}
		got := valuesOf(res)
		if prev != nil && !reflect.DeepEqual(got, prev) {
			t.Fatalf("run=%d results diverged: %v vs %v", run, got, prev)
		}
		prev = got
		reused := 0
		for _, r := range res.Results {
			if r.Reused {
				reused++
			}
		}
		t.Logf("run=%d: reused=%d/%d values=%v", run, reused, len(res.Results), got)
		if run == 1 && reused != 0 {
			t.Fatalf("run=1: nothing should be reused, got %d", reused)
		}
		if run > 1 && reused != 4 {
			t.Fatalf("run=%d: all 4 tasks should be reused, got %d", run, reused)
		}
	}
	for _, id := range []string{"a", "b", "c", "x"} {
		if n := execCount(counters, id); n != 1 {
			t.Fatalf("task %s executed %d times across 3 runs, want 1", id, n)
		}
	}
	st := e.CacheStats()
	t.Logf("verdict: 3 次运行每任务仅执行 1 次；stats=%+v", st)
	if !st.Enabled || st.Hits != 8 || st.Entries != 4 {
		t.Fatalf("unexpected stats: %+v (want Enabled, Hits=8, Entries=4)", st)
	}
}

// TestCacheDisabledByDefault 验证：默认配置不启用缓存，行为与此前版本一致。
func TestCacheDisabledByDefault(t *testing.T) {
	counters := newCounters("a", "b", "c", "x")
	e := New(Config{MaxConcurrency: 4, Logger: testLogger(t)})
	for run := 1; run <= 2; run++ {
		res := e.Run(context.Background(), chainGraph("b-v1", counters))
		if !res.Complete {
			t.Fatalf("run=%d incomplete: %v", run, res.Err)
		}
		for _, r := range res.Results {
			if r.Reused {
				t.Fatal("cache disabled: nothing should be marked reused")
			}
		}
	}
	if n := execCount(counters, "a"); n != 2 {
		t.Fatalf("cache disabled: task a executed %d times, want 2", n)
	}
	if st := e.CacheStats(); st.Enabled {
		t.Fatalf("cache should be disabled by default: %+v", st)
	}
	t.Logf("verdict: 默认不启用缓存，两次运行均全量执行")
}

// TestIncrementalRecompute 验证：只改一个任务定义时，
// 它与下游重算、未受影响的上游与旁支复用，且最终结果与全量重算一致。
func TestIncrementalRecompute(t *testing.T) {
	counters := newCounters("a", "b", "c", "x")
	e := New(Config{MaxConcurrency: 4, CacheSize: 16, Logger: testLogger(t)})

	res1 := e.Run(context.Background(), chainGraph("b-v1", counters))
	if !res1.Complete {
		t.Fatalf("run1 incomplete: %v", res1.Err)
	}
	t.Logf("run1 (全量): values=%v", valuesOf(res1))

	// 只修改 b 的定义（CacheKey 升级，计算逻辑也变了）。
	res2 := e.Run(context.Background(), chainGraph("b-v2", counters))
	if !res2.Complete {
		t.Fatalf("run2 incomplete: %v", res2.Err)
	}
	t.Logf("run2 (增量): values=%v", valuesOf(res2))

	// a、x 未受影响必须复用（只执行 1 次）；b、c 必须重算（各执行 2 次）。
	if n := execCount(counters, "a"); n != 1 {
		t.Fatalf("unaffected upstream a executed %d times, want 1 (reused)", n)
	}
	if n := execCount(counters, "x"); n != 1 {
		t.Fatalf("unrelated branch x executed %d times, want 1 (reused)", n)
	}
	if n := execCount(counters, "b"); n != 2 {
		t.Fatalf("changed task b executed %d times, want 2", n)
	}
	if n := execCount(counters, "c"); n != 2 {
		t.Fatalf("downstream c executed %d times, want 2", n)
	}
	if !res2.Results["a"].Reused || !res2.Results["x"].Reused {
		t.Fatalf("a/x should be reused in run2: %+v", res2.Results)
	}
	if res2.Results["b"].Reused || res2.Results["c"].Reused {
		t.Fatalf("b/c must be recomputed in run2: %+v", res2.Results)
	}

	// 基准：全新实例全量重算 v2 图，不同并发度下结果都必须一致。
	want := map[string]any{"a": 1, "b": 101, "c": 202, "x": 99}
	for _, conc := range []int{1, 8} {
		fresh := New(Config{MaxConcurrency: conc, Logger: testLogger(t)})
		ref := fresh.Run(context.Background(), chainGraph("b-v2", newCounters("a", "b", "c", "x")))
		if !ref.Complete {
			t.Fatalf("reference run incomplete: %v", ref.Err)
		}
		if got := valuesOf(ref); !reflect.DeepEqual(got, want) {
			t.Fatalf("reference conc=%d: got %v want %v", conc, got, want)
		}
	}
	if got := valuesOf(res2); !reflect.DeepEqual(got, want) {
		t.Fatalf("incremental result %v != full recompute %v", got, want)
	}
	t.Logf("verdict: 增量重算结果与全量重算完全一致（%v），仅 b/c 重算、 a/x 复用", want)
}

// TestCacheCapacityLRU 验证：缓存条目数不超过容量上限，淘汰后按 miss 重算。
func TestCacheCapacityLRU(t *testing.T) {
	const total = 10
	counters := newCounters()
	var tasks []Task
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("n%d", i)
		counters[id] = new(int32)
		tasks = append(tasks, countedTask(id, "v1-"+id, nil, counters[id],
			func(deps map[string]any) any { return id }))
	}
	e := New(Config{MaxConcurrency: 4, CacheSize: 3, Logger: testLogger(t)})

	res1 := e.Run(context.Background(), tasks)
	if !res1.Complete {
		t.Fatalf("run1 incomplete: %v", res1.Err)
	}
	if st := e.CacheStats(); st.Entries > 3 {
		t.Fatalf("cache exceeded capacity: %+v", st)
	}
	t.Logf("run1 后 stats=%+v（容量上限 3 生效）", e.CacheStats())

	// 容量只有 3，大部分条目已被淘汰，第二次运行必然大量重算。
	res2 := e.Run(context.Background(), tasks)
	if !res2.Complete {
		t.Fatalf("run2 incomplete: %v", res2.Err)
	}
	reused := 0
	for _, r := range res2.Results {
		if r.Reused {
			reused++
		}
	}
	if reused > 3 {
		t.Fatalf("run2 reused %d tasks, want <= 3 (capacity bound)", reused)
	}
	if st := e.CacheStats(); st.Entries != 3 {
		t.Fatalf("entries=%d, want exactly capacity 3", st.Entries)
	}
	t.Logf("verdict: run2 复用 %d/%d（受容量约束），缓存条目始终 <= 3", reused, total)
}

// TestClearCache 验证：主动清空缓存后下一次运行全量重算。
func TestClearCache(t *testing.T) {
	counters := newCounters("a", "b", "c", "x")
	e := New(Config{CacheSize: 16, Logger: testLogger(t)})

	if res := e.Run(context.Background(), chainGraph("b-v1", counters)); !res.Complete {
		t.Fatalf("run1 incomplete: %v", res.Err)
	}
	e.ClearCache()
	if st := e.CacheStats(); st.Entries != 0 {
		t.Fatalf("cache not cleared: %+v", st)
	}
	res := e.Run(context.Background(), chainGraph("b-v1", counters))
	if !res.Complete {
		t.Fatalf("run2 incomplete: %v", res.Err)
	}
	for id, r := range res.Results {
		if r.Reused {
			t.Fatalf("task %s reused after ClearCache", id)
		}
	}
	if n := execCount(counters, "a"); n != 2 {
		t.Fatalf("task a executed %d times, want 2 (cache cleared)", n)
	}
	t.Logf("verdict: ClearCache 后全量重算，无复用")
}

// TestFailureTimeoutCancelNotCached 验证：失败、超时、取消的产物不进入缓存，
// 后续运行会真正重算这些任务；而成功的上游仍可复用。
func TestFailureTimeoutCancelNotCached(t *testing.T) {
	var okCount, badCount, slowCount int32
	boom := errors.New("boom")
	mk := func() []Task {
		return []Task{
			{ID: "ok", CacheKey: "ok-v1", Run: func(ctx context.Context, deps map[string]any) (any, error) {
				atomic.AddInt32(&okCount, 1)
				return "fine", nil
			}},
			{ID: "bad", CacheKey: "bad-v1", Run: func(ctx context.Context, deps map[string]any) (any, error) {
				atomic.AddInt32(&badCount, 1)
				return nil, boom
			}},
			{ID: "slow", CacheKey: "slow-v1", Timeout: 30 * time.Millisecond,
				Run: func(ctx context.Context, deps map[string]any) (any, error) {
					atomic.AddInt32(&slowCount, 1)
					select {
					case <-time.After(5 * time.Second):
						return "done", nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}},
		}
	}
	e := New(Config{MaxConcurrency: 3, CacheSize: 16, Logger: testLogger(t)})

	res1 := e.Run(context.Background(), mk())
	if res1.Complete || res1.Kind != FailureTaskError {
		t.Fatalf("run1: want incomplete task_error, got kind=%q err=%v", res1.Kind, res1.Err)
	}
	if res1.Results["slow"].Kind != FailureTimeout {
		t.Fatalf("run1: slow should time out, got %+v", res1.Results["slow"])
	}

	res2 := e.Run(context.Background(), mk())
	if res2.Complete {
		t.Fatal("run2 should still be incomplete")
	}
	// 成功的 ok 被复用；失败的 bad、超时的 slow 都重新执行。
	if n := atomic.LoadInt32(&okCount); n != 1 {
		t.Fatalf("ok executed %d times, want 1 (reused in run2)", n)
	}
	if !res2.Results["ok"].Reused {
		t.Fatal("ok should be reused in run2")
	}
	if n := atomic.LoadInt32(&badCount); n != 2 {
		t.Fatalf("bad executed %d times, want 2 (failure not cached)", n)
	}
	if n := atomic.LoadInt32(&slowCount); n != 2 {
		t.Fatalf("slow executed %d times, want 2 (timeout not cached)", n)
	}

	// 整体取消：在途被取消的任务同样不得入缓存。
	var cancelCount int32
	started := make(chan struct{}, 1)
	cancelTasks := []Task{
		{ID: "blocker", CacheKey: "blk-v1", Run: func(ctx context.Context, deps map[string]any) (any, error) {
			atomic.AddInt32(&cancelCount, 1)
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *RunResult, 1)
	go func() { done <- e.Run(ctx, cancelTasks) }()
	<-started
	cancel()
	if res := <-done; res.Kind != FailureCanceled {
		t.Fatalf("want canceled, got %q", res.Kind)
	}
	res3 := e.Run(context.Background(), []Task{
		{ID: "blocker", CacheKey: "blk-v1", Run: func(ctx context.Context, deps map[string]any) (any, error) {
			atomic.AddInt32(&cancelCount, 1)
			return "recovered", nil
		}},
	})
	if !res3.Complete || res3.Results["blocker"].Reused {
		t.Fatalf("canceled task must be recomputed, not reused: %+v", res3.Results["blocker"])
	}
	if n := atomic.LoadInt32(&cancelCount); n != 2 {
		t.Fatalf("blocker executed %d times, want 2 (cancel not cached)", n)
	}
	t.Logf("verdict: 失败/超时/取消均未入缓存，成功节点正常复用")
}

// TestInvalidTaskRejected 验证：任务定义不合法（空 ID / 无计算体）在开跑前
// 整体拒绝，与缺失依赖是可区分的不同类别，且引擎实例仍可继续使用。
func TestInvalidTaskRejected(t *testing.T) {
	noop := func(ctx context.Context, deps map[string]any) (any, error) { return 1, nil }
	cases := []struct {
		name  string
		tasks []Task
	}{
		{"empty id", []Task{{ID: "", Run: noop}}},
		{"nil run", []Task{{ID: "a", Run: nil}}},
		{"nil run among valid", []Task{{ID: "a", Run: noop}, {ID: "b", Run: nil}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var execCount int32
			e := New(Config{CacheSize: 8, Logger: testLogger(t)})
			res := e.Run(context.Background(), tc.tasks)
			if res.Complete {
				t.Fatal("invalid task must reject the whole run")
			}
			if !errors.Is(res.Err, ErrInvalidTask) {
				t.Fatalf("want ErrInvalidTask, got %v", res.Err)
			}
			if errors.Is(res.Err, ErrMissingDep) {
				t.Fatalf("invalid task must be distinct from missing dep: %v", res.Err)
			}
			if res.Kind != FailureInvalidTask {
				t.Fatalf("want kind %q, got %q", FailureInvalidTask, res.Kind)
			}
			if len(res.Results) != 0 {
				t.Fatalf("no task should have run: %+v", res.Results)
			}
			// 引擎未崩溃、缓存未污染，后续合法图照常执行。
			good := e.Run(context.Background(), []Task{
				countedTask("ok", "ok-v1", nil, &execCount, func(deps map[string]any) any { return 7 }),
			})
			if !good.Complete || good.Results["ok"].Value != 7 {
				t.Fatalf("engine unusable after rejection: %+v", good)
			}
			t.Logf("verdict[%s]: 整体拒绝 kind=%q err=%v，引擎可继续服役", tc.name, res.Kind, res.Err)
		})
	}
}

// TestConcurrentRunsSharedCache 验证：同一实例被多个 goroutine 并发运行时，
// 共享缓存不会造成数据竞争或结果错乱（配合 -race 运行）。
func TestConcurrentRunsSharedCache(t *testing.T) {
	var execCount int32
	mk := func() []Task {
		return []Task{
			countedTask("a", "a-v1", nil, &execCount, func(deps map[string]any) any { return 1 }),
			countedTask("b", "b-v1", []string{"a"}, &execCount, func(deps map[string]any) any { return deps["a"].(int) + 10 }),
			countedTask("c", "c-v1", []string{"a"}, &execCount, func(deps map[string]any) any { return deps["a"].(int) * 2 }),
			countedTask("d", "d-v1", []string{"b", "c"}, &execCount, func(deps map[string]any) any {
				return deps["b"].(int) + deps["c"].(int)
			}),
		}
	}
	want := map[string]any{"a": 1, "b": 11, "c": 2, "d": 13}
	e := New(Config{MaxConcurrency: 4, CacheSize: 16, Logger: testLogger(t)})

	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for run := 0; run < 5; run++ {
				res := e.Run(context.Background(), mk())
				if !res.Complete {
					errs <- fmt.Sprintf("g=%d run=%d incomplete: %v", g, run, res.Err)
					return
				}
				if got := valuesOf(res); !reflect.DeepEqual(got, want) {
					errs <- fmt.Sprintf("g=%d run=%d got %v want %v", g, run, got, want)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for msg := range errs {
		t.Fatal(msg)
	}
	st := e.CacheStats()
	t.Logf("verdict: 8 goroutine x 5 次并发运行结果一致，总执行 %d 次任务，stats=%+v",
		atomic.LoadInt32(&execCount), st)
	if st.Entries > 4 {
		t.Fatalf("cache entries=%d exceed distinct task count 4", st.Entries)
	}
}

// TestCacheKeySemantics 验证定义指纹的判定规则：
// CacheKey 非空时以它为准（闭包捕获变量不参与指纹，需调用方自行升版本）；
// CacheKey 变化即视为定义变化，触发重算。
func TestCacheKeySemantics(t *testing.T) {
	e := New(Config{CacheSize: 8, Logger: testLogger(t)})
	mk := func(n int, key string) Task {
		return Task{ID: "t", CacheKey: key, Run: func(ctx context.Context, deps map[string]any) (any, error) {
			return n, nil
		}}
	}
	res1 := e.Run(context.Background(), []Task{mk(1, "k1")})
	if res1.Results["t"].Value != 1 || res1.Results["t"].Reused {
		t.Fatalf("run1 should execute: %+v", res1.Results["t"])
	}
	// 相同 CacheKey：即使闭包捕获了不同的值，也按约定复用（见 README 兼容范围）。
	res2 := e.Run(context.Background(), []Task{mk(2, "k1")})
	if !res2.Results["t"].Reused || res2.Results["t"].Value != 1 {
		t.Fatalf("same CacheKey must reuse: %+v", res2.Results["t"])
	}
	// CacheKey 升级：定义视为已变化，真正重算。
	res3 := e.Run(context.Background(), []Task{mk(2, "k2")})
	if res3.Results["t"].Reused || res3.Results["t"].Value != 2 {
		t.Fatalf("changed CacheKey must recompute: %+v", res3.Results["t"])
	}
	t.Logf("verdict: CacheKey 相同复用、变化重算，捕获变量不参与指纹（约定行为）")
}
