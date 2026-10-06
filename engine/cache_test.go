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

// counter 构造带调用计数的计算体；multiplier 让 Version 变化可改变结果。
func countingTask(id string, deps []string, calls *int32, f func(deps map[string]any) (any, error)) Task {
	return Task{
		ID:   id,
		Deps: deps,
		Run: func(ctx context.Context, d map[string]any) (any, error) {
			atomic.AddInt32(calls, 1)
			return f(d)
		},
	}
}

// versionedDiamond 构造 a->{b,c}->d 的菱形图，每个节点独立计数，
// a 的取值由 seed 决定，计算体用 Version 声明语义版本。
func versionedDiamond(calls map[string]*int32, seed int, aVer, bVer, cVer, dVer string) []Task {
	get := func(id string) *int32 {
		c, ok := calls[id]
		if !ok {
			c = new(int32)
			calls[id] = c
		}
		return c
	}
	return []Task{
		countingTask("a", nil, get("a"), func(d map[string]any) (any, error) {
			return seed, nil
		}).withVersion(aVer),
		countingTask("b", []string{"a"}, get("b"), func(d map[string]any) (any, error) {
			return d["a"].(int) + 10, nil
		}).withVersion(bVer),
		countingTask("c", []string{"a"}, get("c"), func(d map[string]any) (any, error) {
			return d["a"].(int) * 2, nil
		}).withVersion(cVer),
		countingTask("d", []string{"b", "c"}, get("d"), func(d map[string]any) (any, error) {
			return d["b"].(int) + d["c"].(int), nil
		}).withVersion(dVer),
	}
}

func (t Task) withVersion(v string) Task { t.Version = v; return t }

func diamondWant(seed int) map[string]any {
	return map[string]any{
		"a": seed,
		"b": seed + 10,
		"c": seed * 2,
		"d": seed + 10 + seed*2,
	}
}

// TestCacheSameGraphReused 同一张图连续提交：第二次起全部命中，实际执行次数≈只跑一次。
func TestCacheSameGraphReused(t *testing.T) {
	calls := map[string]*int32{}
	e := New(Config{Logger: testLogger(t)})
	for run := 0; run < 3; run++ {
		res := e.Run(context.Background(), versionedDiamond(calls, 1, "v1", "v1", "v1", "v1"))
		if !res.Complete {
			t.Fatalf("run %d incomplete: %v", run, res.Err)
		}
		got := valuesOf(res)
		if !reflect.DeepEqual(got, diamondWant(1)) {
			t.Fatalf("run %d got %v want %v", run, got, diamondWant(1))
		}
		if run == 0 {
			if res.ReusedCount != 0 || res.ComputedCount != 4 {
				t.Fatalf("first run: reused=%d computed=%d", res.ReusedCount, res.ComputedCount)
			}
		} else {
			if res.ReusedCount != 4 || res.ComputedCount != 0 {
				t.Fatalf("run %d: reused=%d computed=%d, want 4/0", run, res.ReusedCount, res.ComputedCount)
			}
			for id, r := range res.Results {
				if !r.Reused {
					t.Fatalf("run %d task %s should be marked reused", run, id)
				}
			}
		}
	}
	for id, c := range calls {
		if got := atomic.LoadInt32(c); got != 1 {
			t.Fatalf("task %s executed %d times across 3 runs, want 1", id, got)
		}
	}
	t.Logf("verdict: 同图连跑 3 次，4 个任务各只真正执行 1 次，后两次 reused=4 computed=0")
}

// TestCacheIncrementalChangeUpstream 改动 a（seed 经 Version 声明变化）：
// a 及其下游 b/c/d 全部重算；结果必须与全新引擎全量重算一致。
func TestCacheIncrementalChangeUpstream(t *testing.T) {
	calls := map[string]*int32{}
	e := New(Config{Logger: testLogger(t)})

	r1 := e.Run(context.Background(), versionedDiamond(calls, 1, "a-v1", "v1", "v1", "v1"))
	if !r1.Complete {
		t.Fatalf("r1: %v", r1.Err)
	}
	// a 的定义变化（seed 1->2，版本 a-v1->a-v2）。
	r2 := e.Run(context.Background(), versionedDiamond(calls, 2, "a-v2", "v1", "v1", "v1"))
	if !r2.Complete {
		t.Fatalf("r2: %v", r2.Err)
	}
	if r2.ReusedCount != 0 || r2.ComputedCount != 4 {
		t.Fatalf("changing a must recompute whole subtree: reused=%d computed=%d", r2.ReusedCount, r2.ComputedCount)
	}
	if got := valuesOf(r2); !reflect.DeepEqual(got, diamondWant(2)) {
		t.Fatalf("incremental result mismatch: got %v want %v", got, diamondWant(2))
	}

	// 交叉验证：禁用缓存的全新引擎全量重算，结果必须一模一样。
	fresh := New(Config{DisableCache: true, Logger: testLogger(t)})
	freshCalls := map[string]*int32{}
	rf := fresh.Run(context.Background(), versionedDiamond(freshCalls, 2, "a-v2", "v1", "v1", "v1"))
	if !reflect.DeepEqual(valuesOf(r2), valuesOf(rf)) {
		t.Fatalf("incremental result %v != full recompute %v", valuesOf(r2), valuesOf(rf))
	}
	t.Logf("verdict: 改 a 后 a/b/c/d 全部重算，增量结果 %v 与全量重算一致", valuesOf(r2))
}

// TestCacheIncrementalChangeLeaf 只改 d：a/b/c 继续复用，仅 d 重算。
func TestCacheIncrementalChangeLeaf(t *testing.T) {
	calls := map[string]*int32{}
	e := New(Config{Logger: testLogger(t)})
	e.Run(context.Background(), versionedDiamond(calls, 3, "v1", "v1", "v1", "d-v1"))
	r2 := e.Run(context.Background(), versionedDiamond(calls, 3, "v1", "v1", "v1", "d-v2"))
	if !r2.Complete {
		t.Fatalf("r2: %v", r2.Err)
	}
	if r2.ReusedCount != 3 || r2.ComputedCount != 1 {
		t.Fatalf("only d should recompute: reused=%d computed=%d", r2.ReusedCount, r2.ComputedCount)
	}
	if r2.Results["d"].Reused {
		t.Fatal("d must be recomputed, got reused=true")
	}
	for _, id := range []string{"a", "b", "c"} {
		if !r2.Results[id].Reused {
			t.Fatalf("%s must be reused", id)
		}
	}
	if got := atomic.LoadInt32(calls["d"]); got != 2 {
		t.Fatalf("d executions=%d want 2", got)
	}
	for _, id := range []string{"a", "b", "c"} {
		if got := atomic.LoadInt32(calls[id]); got != 1 {
			t.Fatalf("%s executions=%d want 1", id, got)
		}
	}
	t.Logf("verdict: 只改 d，a/b/c 复用、d 重算，最终值 %v", valuesOf(r2))
}

// TestCacheIncrementalConsistentAcrossConcurrency 增量重算结果不因并发度不同而对不上。
func TestCacheIncrementalConsistentAcrossConcurrency(t *testing.T) {
	var want map[string]any
	// 每个并发度独立引擎、相同变更序列，结果必须一致。
	for _, conc := range []int{1, 2, 4, 16} {
		calls := map[string]*int32{}
		e := New(Config{MaxConcurrency: conc, BufferSize: 2, Logger: testLogger(t)})
		e.Run(context.Background(), versionedDiamond(calls, 5, "a-v1", "b-v1", "c-v1", "d-v1"))
		e.Run(context.Background(), versionedDiamond(calls, 5, "a-v1", "b-v2", "c-v1", "d-v1")) // 改 b
		r3 := e.Run(context.Background(), versionedDiamond(calls, 7, "a-v2", "b-v2", "c-v1", "d-v2"))
		got := valuesOf(r3)
		if want == nil {
			want = got
		} else if !reflect.DeepEqual(got, want) {
			t.Fatalf("conc=%d result %v != %v", conc, got, want)
		}
		// 改 a 后整棵子树重算。
		if r3.ComputedCount != 4 || r3.ReusedCount != 0 {
			t.Fatalf("conc=%d last run reused=%d computed=%d", conc, r3.ReusedCount, r3.ComputedCount)
		}
	}
	t.Logf("verdict: 增量序列在并发度 1/2/4/16 下结果一致：%v", want)
}

// TestCacheFailureNotStored 失败节点的产物不进缓存：修复后第二次运行应重新执行并成功。
func TestCacheFailureNotStored(t *testing.T) {
	var calls int32
	var failNow int32 = 1
	tasks := []Task{
		{ID: "flaky", Version: "v1", Run: func(ctx context.Context, deps map[string]any) (any, error) {
			atomic.AddInt32(&calls, 1)
			if atomic.LoadInt32(&failNow) == 1 {
				return nil, errors.New("boom")
			}
			return "ok", nil
		}},
	}
	e := New(Config{Logger: testLogger(t)})
	r1 := e.Run(context.Background(), tasks)
	if r1.Complete || r1.Kind != FailureTaskError {
		t.Fatalf("r1 should fail, got complete=%v kind=%q", r1.Complete, r1.Kind)
	}
	if e.CacheLen() != 0 {
		t.Fatalf("failed value must not be cached, len=%d", e.CacheLen())
	}
	atomic.StoreInt32(&failNow, 0)
	r2 := e.Run(context.Background(), tasks)
	if !r2.Complete {
		t.Fatalf("r2 should succeed: %v", r2.Err)
	}
	if r2.Results["flaky"].Reused {
		t.Fatal("previously failed task must recompute, not reuse")
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("task should run twice, got %d", got)
	}
	// 第三次：成功产物应已缓存，命中。
	r3 := e.Run(context.Background(), tasks)
	if r3.ReusedCount != 1 || atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("third run should hit cache: reused=%d calls=%d", r3.ReusedCount, calls)
	}
	t.Logf("verdict: 失败产物未缓存（len=0），修复后重算成功，再跑才命中")
}

// TestCacheTimeoutNotStored 超时产物不进缓存。
func TestCacheTimeoutNotStored(t *testing.T) {
	var calls int32
	mk := func(slow bool) []Task {
		return []Task{{
			ID:      "t",
			Version: "v1",
			Timeout: 30 * time.Millisecond,
			Run: func(ctx context.Context, deps map[string]any) (any, error) {
				atomic.AddInt32(&calls, 1)
				if slow {
					select {
					case <-time.After(2 * time.Second):
						return "x", nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return "fast", nil
			},
		}}
	}
	e := New(Config{Logger: testLogger(t)})
	r1 := e.Run(context.Background(), mk(true))
	if r1.Kind != FailureTimeout || e.CacheLen() != 0 {
		t.Fatalf("timeout must not cache: kind=%q len=%d", r1.Kind, e.CacheLen())
	}
	r2 := e.Run(context.Background(), mk(false))
	if !r2.Complete || r2.Results["t"].Reused || atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("after timeout task must recompute: reused=%v calls=%d", r2.Results["t"].Reused, calls)
	}
	t.Logf("verdict: 超时产物未缓存，后续正常运行重新执行")
}

// TestCacheCanceledNotStored 取消的运行整体不写缓存：再次提交全部重算。
func TestCacheCanceledNotStored(t *testing.T) {
	var calls int32
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	tasks := []Task{{
		ID: "blocker", Version: "v1",
		Run: func(ctx context.Context, deps map[string]any) (any, error) {
			started <- struct{}{}
			select {
			case <-release:
				return "released", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}, {
		ID: "side", Version: "v1",
		Run: func(ctx context.Context, deps map[string]any) (any, error) {
			atomic.AddInt32(&calls, 1)
			return "side", nil
		},
	}}
	e := New(Config{MaxConcurrency: 2, Logger: testLogger(t)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *RunResult, 1)
	go func() { done <- e.Run(ctx, tasks) }()
	<-started
	cancel()
	r1 := <-done
	close(release)
	if r1.Kind != FailureCanceled {
		t.Fatalf("want canceled, got %q", r1.Kind)
	}
	if e.CacheLen() != 0 {
		t.Fatalf("canceled run must not populate cache, len=%d", e.CacheLen())
	}
	r2 := e.Run(context.Background(), tasks)
	if !r2.Complete || r2.ReusedCount != 0 {
		t.Fatalf("after cancel everything recomputes: complete=%v reused=%d", r2.Complete, r2.ReusedCount)
	}
	t.Logf("verdict: 取消运行不写缓存（len=0），再次提交 %d 个任务全部重算", r2.ComputedCount)
}

// TestCacheCapacityEviction 容量上限生效，超限按 LRU 淘汰，被淘汰者下次重算。
func TestCacheCapacityEviction(t *testing.T) {
	const cap = 3
	calls := map[string]*int32{}
	mk := func(ids ...string) []Task {
		var out []Task
		for _, id := range ids {
			c := new(int32)
			calls[id] = c
			id := id
			out = append(out, countingTask(id, nil, c, func(d map[string]any) (any, error) {
				return id, nil
			}).withVersion("v1"))
		}
		return out
	}
	// 单并发使无依赖任务按 a,b,c 确定顺序存入，LRU 顺序才可预期。
	e := New(Config{MaxConcurrency: 1, MaxCacheEntries: cap, Logger: testLogger(t)})
	e.Run(context.Background(), mk("a", "b", "c"))
	if e.CacheLen() != cap {
		t.Fatalf("len=%d want %d", e.CacheLen(), cap)
	}
	// 再提交 a（刷新 a 为最近使用），然后加入 d：应淘汰最久未访问的 b。
	e.Run(context.Background(), mk("a"))
	r3 := e.Run(context.Background(), mk("d"))
	if e.CacheLen() != cap {
		t.Fatalf("len after eviction=%d want %d", e.CacheLen(), cap)
	}
	if r3.ComputedCount != 1 || r3.ReusedCount != 0 {
		t.Fatalf("d must compute: computed=%d reused=%d", r3.ComputedCount, r3.ReusedCount)
	}
	// b 应已被淘汰：再次提交 b 必须重算；c 仍命中。
	r4 := e.Run(context.Background(), mk("b", "c"))
	if r4.Results["b"].Reused || !r4.Results["c"].Reused {
		t.Fatalf("LRU expectation failed: b.reused=%v c.reused=%v", r4.Results["b"].Reused, r4.Results["c"].Reused)
	}
	t.Logf("verdict: 容量上限 %d 生效，访问 a 后加入 d，最久未用的 b 被淘汰并在下次重算", cap)
}

// TestCacheClear 主动清理入口可用，清理后下次全部重算。
func TestCacheClear(t *testing.T) {
	var calls int32
	tasks := []Task{{ID: "a", Version: "v1", Run: func(ctx context.Context, deps map[string]any) (any, error) {
		atomic.AddInt32(&calls, 1)
		return 1, nil
	}}}
	e := New(Config{Logger: testLogger(t)})
	e.Run(context.Background(), tasks)
	if e.CacheLen() != 1 {
		t.Fatalf("len=%d want 1", e.CacheLen())
	}
	if n := e.ClearCache(); n != 1 {
		t.Fatalf("cleared=%d want 1", n)
	}
	r := e.Run(context.Background(), tasks)
	if r.ReusedCount != 0 || atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("after clear must recompute: reused=%d calls=%d", r.ReusedCount, calls)
	}
	// 第二次运行成功后缓存被重新填充，再次清理应返回 1，随后为 0。
	if n := e.ClearCache(); n != 1 {
		t.Fatalf("re-cleared=%d want 1", n)
	}
	if n := e.ClearCache(); n != 0 {
		t.Fatalf("clear on empty should report 0, got %d", n)
	}
	t.Logf("verdict: ClearCache 清空 %d 条并返回数量，清空后命中归零", 1)
}

// TestCacheDisabled DisableCache 时行为等同无缓存：不读不写、每次全量。
func TestCacheDisabled(t *testing.T) {
	var calls int32
	tasks := []Task{{ID: "a", Run: func(ctx context.Context, deps map[string]any) (any, error) {
		atomic.AddInt32(&calls, 1)
		return 1, nil
	}}}
	e := New(Config{DisableCache: true, Logger: testLogger(t)})
	for i := 0; i < 3; i++ {
		r := e.Run(context.Background(), tasks)
		if r.ReusedCount != 0 || r.ComputedCount != 1 {
			t.Fatalf("run %d reused=%d computed=%d", i, r.ReusedCount, r.ComputedCount)
		}
	}
	if e.CacheLen() != 0 || e.ClearCache() != 0 {
		t.Fatal("disabled cache must stay empty")
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("with cache disabled task must run 3 times, got %d", got)
	}
	t.Logf("verdict: DisableCache=true 连跑 3 次全部重算（3 次执行，缓存恒空）")
}

// TestInvalidTaskRejected 任务定义不合法在开跑前整体拒绝，并与缺失依赖分开归类。
func TestInvalidTaskRejected(t *testing.T) {
	good := func(ctx context.Context, deps map[string]any) (any, error) { return 1, nil }
	cases := []struct {
		name  string
		tasks []Task
	}{
		{"empty id", []Task{{ID: "", Run: good}}},
		{"nil run", []Task{{ID: "a", Run: nil}}},
		{"duplicate second is nil-run", []Task{
			{ID: "a", Run: good},
			{ID: "a", Run: nil},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := New(Config{Logger: testLogger(t)})
			r := e.Run(context.Background(), tc.tasks)
			if r.Complete {
				t.Fatal("invalid task must not complete")
			}
			if !errors.Is(r.Err, ErrInvalidTask) {
				t.Fatalf("want ErrInvalidTask, got %v", r.Err)
			}
			if r.Kind != FailureInvalidTask {
				t.Fatalf("want kind %q got %q", FailureInvalidTask, r.Kind)
			}
		})
	}
	// 缺失依赖仍然是独立类别。
	r := New(Config{Logger: testLogger(t)}).Run(context.Background(),
		[]Task{{ID: "a", Deps: []string{"ghost"}, Run: good}})
	if !errors.Is(r.Err, ErrMissingDep) || r.Kind != FailureMissingDep {
		t.Fatalf("missing dep classification changed: %v / %q", r.Err, r.Kind)
	}
	t.Logf("verdict: 空 ID / 空计算体 -> invalid_task（开跑前拒绝），缺失依赖仍为 missing_dependency")
}

// TestPanicTaskDoesNotCrash 任务 panic 被转成 task_error，引擎与进程不崩、结果不完整。
func TestPanicTaskDoesNotCrash(t *testing.T) {
	tasks := []Task{
		{ID: "boom", Run: func(ctx context.Context, deps map[string]any) (any, error) {
			panic("kaboom")
		}},
		{ID: "child", Deps: []string{"boom"}, Run: func(ctx context.Context, deps map[string]any) (any, error) {
			return "never", nil
		}},
	}
	e := New(Config{Logger: testLogger(t)})
	r := e.Run(context.Background(), tasks)
	if r.Complete || r.Kind != FailureTaskError {
		t.Fatalf("panic must be task_error incomplete, got complete=%v kind=%q", r.Complete, r.Kind)
	}
	if r.Results["child"].Status != StatusSkipped {
		t.Fatalf("downstream must be skipped: %+v", r.Results["child"])
	}
	if e.CacheLen() != 0 {
		t.Fatal("panicked task must not be cached")
	}
	// 引擎仍可继续服务下一次运行。
	r2 := e.Run(context.Background(), []Task{{ID: "ok", Run: func(ctx context.Context, deps map[string]any) (any, error) {
		return 42, nil
	}}})
	if !r2.Complete || r2.Results["ok"].Value != 42 {
		t.Fatalf("engine must keep serving after panic: %+v", r2)
	}
	t.Logf("verdict: panic 被捕获为 task_error，下游跳过、不缓存，引擎后续运行不受影响")
}

// TestCacheDependencyShapeChanged 依赖集合变化（同一 Version）也要触发重算。
func TestCacheDependencyShapeChanged(t *testing.T) {
	var aCalls, dCalls int32
	one := func(id string, calls *int32, deps []string) Task {
		return Task{ID: id, Deps: deps, Version: "v1", Run: func(ctx context.Context, d map[string]any) (any, error) {
			atomic.AddInt32(calls, 1)
			sum := 0
			for _, v := range d {
				sum += v.(int)
			}
			return sum + 1, nil
		}}
	}
	e := New(Config{Logger: testLogger(t)})
	// 第一次：d 依赖 a。
	e.Run(context.Background(), []Task{
		one("a", &aCalls, nil),
		one("d", &dCalls, []string{"a"}),
	})
	// 第二次：d 无依赖（定义指纹中的依赖集合变化），必须重算。
	r := e.Run(context.Background(), []Task{
		one("a", &aCalls, nil),
		one("d", &dCalls, nil),
	})
	if !r.Complete {
		t.Fatalf("r2: %v", r.Err)
	}
	if r.Results["a"].Reused != true {
		t.Fatal("a should be reused")
	}
	if r.Results["d"].Reused {
		t.Fatal("d changed dependency set, must recompute")
	}
	if r.Results["d"].Value != 1 {
		t.Fatalf("d recomputed value=%v want 1", r.Results["d"].Value)
	}
	t.Logf("verdict: d 的依赖集合从 [a] 变为 []，即使 Version 未变也判定失效重算")
}

// TestCacheConcurrentRuns 同一引擎实例被并发反复 Run：无数据竞争、缓存不被搞乱、结果正确。
func TestCacheConcurrentRuns(t *testing.T) {
	e := New(Config{MaxConcurrency: 4, MaxCacheEntries: 64, Logger: testLogger(t)})
	var wg sync.WaitGroup
	const goroutines = 16
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			c := map[string]*int32{}
			// 每张子图用独立 ID 前缀，goroutine 之间只共享缓存容器与锁；
			// 同一 goroutine 内同图连跑两次验证命中。
			prefix := fmt.Sprintf("g%02d-", seed)
			tasks := []Task{
				countingTask(prefix+"a", nil, pick(c, prefix+"a"), func(d map[string]any) (any, error) {
					return seed, nil
				}).withVersion("v1"),
				countingTask(prefix+"b", []string{prefix + "a"}, pick(c, prefix+"b"), func(d map[string]any) (any, error) {
					return d[prefix+"a"].(int) * 2, nil
				}).withVersion("v1"),
			}
			r1 := e.Run(context.Background(), tasks)
			r2 := e.Run(context.Background(), tasks)
			if !r1.Complete || !r2.Complete {
				t.Errorf("seed %d incomplete: %v %v", seed, r1.Err, r2.Err)
				return
			}
			if !r2.Results[prefix+"b"].Reused {
				t.Errorf("seed %d: second run should reuse both nodes", seed)
			}
			if r2.Results[prefix+"b"].Value != seed*2 {
				t.Errorf("seed %d wrong value %v", seed, r2.Results[prefix+"b"].Value)
			}
		}(g)
	}
	wg.Wait()
	if e.CacheLen() > 64 {
		t.Fatalf("cache exceeded capacity under concurrent runs: %d", e.CacheLen())
	}
	t.Logf("verdict: %d 个 goroutine 并发共用同一引擎完成两轮运行，-race 通过、容量受控（len=%d）",
		goroutines, e.CacheLen())
}

func pick(local map[string]*int32, id string) *int32 {
	if c, ok := local[id]; ok {
		return c
	}
	c := new(int32)
	local[id] = c
	return c
}
