package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Engine 是并行数据流执行引擎。
type Engine struct {
	cfg   Config
	cache *resultCache // 跨运行共享；DisableCache 时为 nil
}

// New 以给定配置创建引擎。默认开启跨运行结果复用（见 DisableCache）。
func New(cfg Config) *Engine {
	cfg = cfg.withDefaults()
	e := &Engine{cfg: cfg}
	if cfg.cacheActive() {
		e.cache = newResultCache(cfg.MaxCacheEntries)
	}
	return e
}

// CacheLen 返回当前缓存的任务产物条目数（禁用缓存时恒为 0）。
func (e *Engine) CacheLen() int {
	if e.cache == nil {
		return 0
	}
	return e.cache.len()
}

// ClearCache 主动清空全部已缓存的跨运行产物，返回被清除的条目数。
func (e *Engine) ClearCache() int {
	if e.cache == nil {
		return 0
	}
	n := e.cache.clear()
	e.cfg.logf("cache cleared: removed=%d entries", n)
	return n
}

// Run 校验并执行整个任务图，返回汇总结果。
// 保证：
//   - 结果与并发度、调度顺序无关（按任务 ID 汇总，计算只依赖上游输出）；
//   - 任务定义不合法（空 ID/空计算体）返回 ErrInvalidTask，
//     依赖成环返回 ErrCycle，引用缺失节点返回 ErrMissingDep，均在执行前整体拒绝；
//   - 失败、超时、取消时下游被跳过，部分结果被标记为不完整；
//   - 并发受 MaxConcurrency 约束，内部缓冲受 BufferSize 约束（背压）；
//   - 默认开启跨运行复用：定义与上游产物均未变的任务直接复用上次成功结果
//     （Result.Reused=true），未正常收尾的产物绝不进缓存；
//   - 返回前所有执行单元均已回收，无 goroutine 泄漏。
func (e *Engine) Run(ctx context.Context, tasks []Task) *RunResult {
	nodes, topo, err := buildGraph(tasks)
	if err != nil {
		kind := FailureMissingDep
		switch {
		case errors.Is(err, ErrCycle):
			kind = FailureCycle
		case errors.Is(err, ErrInvalidTask):
			kind = FailureInvalidTask
		}
		e.cfg.logf("graph rejected: %v", err)
		return &RunResult{Results: map[string]Result{}, Complete: false, Err: err, Kind: kind}
	}

	e.cfg.logf("graph accepted: %d tasks, topo order=%v", len(topo), topo)
	for _, id := range topo {
		n := nodes[id]
		e.cfg.logf("  node %s deps=%v children=%v", id, n.deps, n.children)
	}

	r := &runner{
		cfg:       e.cfg,
		cache:     e.cache,
		nodes:     nodes,
		remaining: len(topo),
		pending:   make(map[string]int, len(nodes)),
		results:   make(map[string]Result, len(nodes)),
		ready:     make([]string, 0, len(nodes)),
		digests:   make(map[string]string, len(nodes)),
	}
	for _, id := range topo {
		r.pending[id] = len(nodes[id].deps)
		if r.pending[id] == 0 {
			r.ready = append(r.ready, id)
		}
	}
	return r.run(ctx)
}

// runner 承载单次运行的可变状态（仅主调度 goroutine 读写，
// worker 只通过有界通道与其通信；缓存的并发安全由 resultCache 自身保证）。
type runner struct {
	cfg       Config
	cache     *resultCache
	nodes     map[string]*node
	remaining int // 尚未到达终态的任务数
	pending   map[string]int
	results   map[string]Result
	ready     []string          // 已就绪待派发的任务 ID
	reused    int               // 缓存命中数
	computed  int               // 真正执行并成功数
	canceled  bool              // 主调度 goroutine 是否已观察到整体取消（仅此 goroutine 读写）
	digests   map[string]string // 本次运行每个任务产物指纹（命中或新算均记录）
	writes    []storeResult     // 本次运行的缓存写入记录（取消时逐条回滚）
}

// effectiveTimeout 返回某任务生效的单任务超时（任务级覆盖引擎默认）。
func (r *runner) effectiveTimeout(id string) time.Duration {
	if t := r.nodes[id].task.Timeout; t > 0 {
		return t
	}
	return r.cfg.TaskTimeout
}

// depKeysFor 依据本次运行已确定的上游产物指纹，构造任务 id 的候选依赖键。
// 调用时其全部直接依赖必已成功（真算或复用）。
func (r *runner) depKeysFor(id string) []depKey {
	n := r.nodes[id]
	keys := make([]depKey, 0, len(n.deps))
	for _, d := range n.deps {
		keys = append(keys, depKey{id: d, digest: r.digests[d]})
	}
	return keys
}

// tryReuse 在主调度 goroutine 上判定就绪任务可否直接复用缓存。
// 命中时写入结果、推进下游就绪并返回 true；未命中返回 false（由调用方派发执行）。
func (r *runner) tryReuse(id string) bool {
	if r.cache == nil {
		return false
	}
	n := r.nodes[id]
	keys := r.depKeysFor(id)
	ver := n.task.Version
	to := r.effectiveTimeout(id)
	fp := digestTask(ver, to, keys)
	val, ok := r.cache.lookup(id, ver, to, keys)
	if !ok {
		r.cfg.logf("cache miss task=%s fp=%s reason=definition or upstream changed (or first run)", id, fp)
		return false
	}
	dg := digestValue(val)
	r.digests[id] = dg
	res := Result{TaskID: id, Status: StatusSuccess, Value: val, Reused: true}
	r.reused++
	r.results[id] = res
	r.remaining--
	r.cfg.logf("cache hit task=%s fp=%s digest=%s reason=version/timeout/upstream-digests all match", id, fp, shortDigest(dg))
	for _, ch := range n.children {
		r.pending[ch]--
		if r.pending[ch] == 0 {
			r.ready = append(r.ready, ch)
			r.cfg.logf("ready task=%s", ch)
		}
	}
	return true
}

func (r *runner) run(ctx context.Context) *RunResult {
	dispatch := make(chan workItem, r.cfg.BufferSize)
	resultsCh := make(chan Result, r.cfg.BufferSize)

	var wg sync.WaitGroup
	for i := 0; i < r.cfg.MaxConcurrency; i++ {
		wg.Add(1)
		go r.worker(ctx, dispatch, resultsCh, &wg)
	}

	inflight := 0

	// finish 回收一个执行结果并推进调度状态。
	finish := func(res Result) {
		inflight--
		r.remaining--
		r.results[res.TaskID] = res
		if res.Status == StatusSuccess {
			r.computed++
			// 运行级原子化：正常完成前写入都记账；一旦取消，全部回滚。
			// 失败/超时绝不写。
			if r.cache != nil {
				keys := r.depKeysFor(res.TaskID)
				n := r.nodes[res.TaskID]
				r.digests[res.TaskID] = digestValue(res.Value)
				r.writes = append(r.writes,
					r.cache.store(res.TaskID, n.task.Version, r.effectiveTimeout(res.TaskID), keys, res.Value))
				r.cfg.logf("stored cache task=%s digest=%s", res.TaskID, shortDigest(r.digests[res.TaskID]))
			}
			r.cfg.logf("finished task=%s status=%s kind=%q", res.TaskID, res.Status, res.Kind)
			for _, ch := range r.nodes[res.TaskID].children {
				r.pending[ch]--
				if r.pending[ch] == 0 {
					r.ready = append(r.ready, ch)
					r.cfg.logf("ready task=%s", ch)
				}
			}
		} else {
			r.cfg.logf("finished task=%s status=%s kind=%q (not cached)", res.TaskID, res.Status, res.Kind)
			r.skipDescendants(res.TaskID, res)
		}
	}

	// 主调度循环：就绪任务先试缓存命中（同步消化），未命中才派发 worker；
	// select 同时监听派发与回收，通道均有界，形成背压。
	for r.remaining > 0 && !r.canceled {
		if len(r.ready) > 0 {
			next := r.ready[0]
			if r.tryReuse(next) {
				r.ready = r.ready[1:]
				continue
			}
			item := r.makeWorkItem(next)
			select {
			case dispatch <- item:
				r.ready = r.ready[1:]
				inflight++
				r.cfg.logf("dispatch task=%s inflight=%d", next, inflight)
			case res := <-resultsCh:
				finish(res)
			case <-ctx.Done():
				r.canceled = true
			}
		} else {
			select {
			case res := <-resultsCh:
				finish(res)
			case <-ctx.Done():
				r.canceled = true
			}
		}
	}

	// 停止派发，回收在途结果，等待全部 worker 退出（无执行单元残留）。
	close(dispatch)
	for inflight > 0 {
		res := <-resultsCh
		finish(res)
	}
	wg.Wait()

	// 所有任务可能在取消生效前恰好全部收尾，主循环因此提前退出；
	// 只要 ctx 已结束，本次运行仍按取消处理（并回滚缓存写入）。
	if !r.canceled && ctx.Err() != nil {
		r.canceled = true
	}

	if r.canceled {
		// 回滚本次运行写入缓存的全部产物（含取消前已完成的任务），
		// 保证“未正常收尾的运行”对跨运行缓存零影响。
		if r.cache != nil && len(r.writes) > 0 {
			for i := len(r.writes) - 1; i >= 0; i-- {
				r.cache.rollback(r.writes[i])
			}
			r.cfg.logf("run canceled: rolled back %d cache write(s)", len(r.writes))
		}
		// 凡未产生结果的任务统一标记为取消跳过，整体结论为取消而非成功。
		for _, id := range r.sortedIDs() {
			if _, done := r.results[id]; !done {
				r.results[id] = Result{
					TaskID: id,
					Status: StatusSkipped,
					Err:    fmt.Errorf("%w: run canceled before scheduling", ErrCanceled),
					Kind:   FailureCanceled,
				}
				r.cfg.logf("skip task=%s reason=canceled", id)
			}
		}
		return r.summarize(fmt.Errorf("%w: %v", ErrCanceled, ctx.Err()), FailureCanceled)
	}
	return r.summarize(nil, FailureNone)
}

// sortedIDs 返回全部任务 ID 的有序列表，保证判定与输出确定。
func (r *runner) sortedIDs() []string {
	ids := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// skipDescendants 将因上游未成功而无法运行的下游递归标记为跳过。
func (r *runner) skipDescendants(failedID string, cause Result) {
	for _, ch := range r.nodes[failedID].children {
		if _, done := r.results[ch]; done {
			continue
		}
		res := Result{
			TaskID: ch,
			Status: StatusSkipped,
			Err: fmt.Errorf("skipped: upstream %q finished with status=%s kind=%q",
				failedID, cause.Status, cause.Kind),
			Kind: cause.Kind,
		}
		r.results[ch] = res
		r.remaining--
		r.cfg.logf("skip task=%s reason=upstream %s %s", ch, failedID, cause.Status)
		r.skipDescendants(ch, res)
	}
}

// summarize 汇总终态并给出整体结论（确定性：按任务 ID 字典序取首个失败）。
func (r *runner) summarize(runErr error, runKind FailureKind) *RunResult {
	out := &RunResult{
		Results:       r.results,
		Complete:      true,
		ReusedCount:   r.reused,
		ComputedCount: r.computed,
	}
	r.cfg.logf("run summary: total=%d reused=%d computed=%d", len(r.nodes), r.reused, r.computed)
	if runErr != nil {
		out.Complete = false
		out.Err = runErr
		out.Kind = runKind
		return out
	}
	for _, id := range r.sortedIDs() {
		res := r.results[id]
		if res.Status != StatusSuccess {
			out.Complete = false
			if out.Err == nil {
				out.Err = fmt.Errorf("task %q: %w", id, res.Err)
				out.Kind = res.Kind
			}
		}
	}
	return out
}

// workItem 是派发给 worker 的执行单元，携带依赖快照，
// 使 worker 无需访问共享结果表（避免数据竞争）。
type workItem struct {
	id   string
	deps map[string]any
}

// makeWorkItem 在派发时刻构造依赖值快照（此时全部依赖已成功）。
func (r *runner) makeWorkItem(id string) workItem {
	n := r.nodes[id]
	deps := make(map[string]any, len(n.deps))
	for _, d := range n.deps {
		deps[d] = r.results[d].Value
	}
	return workItem{id: id, deps: deps}
}

// worker 从派发通道取任务执行，通道关闭后退出。
func (r *runner) worker(ctx context.Context, dispatch <-chan workItem, resultsCh chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for item := range dispatch {
		resultsCh <- r.execute(ctx, item)
	}
}

// execute 执行单个任务，附带超时控制，并对失败原因进行分类。
func (r *runner) execute(ctx context.Context, item workItem) Result {
	id := item.id
	n := r.nodes[id]
	timeout := n.task.Timeout
	if timeout <= 0 {
		timeout = r.cfg.TaskTimeout
	}
	taskCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		taskCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	r.cfg.logf("start task=%s", id)
	val, err := r.runTask(taskCtx, n, item.deps)
	if err == nil {
		return Result{TaskID: id, Status: StatusSuccess, Value: val}
	}
	// 区分取消 / 超时 / 普通错误。
	if ctx.Err() != nil {
		return Result{TaskID: id, Status: StatusFailed,
			Err: fmt.Errorf("%w: %v", ErrCanceled, err), Kind: FailureCanceled}
	}
	if taskCtx.Err() == context.DeadlineExceeded {
		return Result{TaskID: id, Status: StatusFailed,
			Err: fmt.Errorf("%w after %s: %v", ErrTimeout, timeout, err), Kind: FailureTimeout}
	}
	return Result{TaskID: id, Status: StatusFailed,
		Err: err, Kind: FailureTaskError}
}

// runTask 执行计算体并把 panic 转成普通任务错误，
// 保证单个坏任务不会让整个引擎进程崩溃（panic 也不产生缓存产物）。
func (r *runner) runTask(taskCtx context.Context, n *node, deps map[string]any) (val any, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("task %q panicked: %v", n.task.ID, rec)
			val = nil
		}
	}()
	return n.task.Run(taskCtx, deps)
}
