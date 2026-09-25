package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Engine 是并行数据流执行引擎。
type Engine struct {
	cfg Config
}

// New 以给定配置创建引擎。
func New(cfg Config) *Engine {
	return &Engine{cfg: cfg.withDefaults()}
}

// Run 校验并执行整个任务图，返回汇总结果。
// 保证：
//   - 结果与并发度、调度顺序无关（按任务 ID 汇总，计算只依赖上游输出）；
//   - 依赖成环 / 引用缺失节点时拒绝执行并返回可区分错误；
//   - 失败、超时、取消时下游被跳过，部分结果被标记为不完整；
//   - 并发受 MaxConcurrency 约束，内部缓冲受 BufferSize 约束（背压）；
//   - 返回前所有执行单元均已回收，无 goroutine 泄漏。
func (e *Engine) Run(ctx context.Context, tasks []Task) *RunResult {
	nodes, topo, err := buildGraph(tasks)
	if err != nil {
		kind := FailureMissingDep
		if errors.Is(err, ErrCycle) {
			kind = FailureCycle
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
		nodes:     nodes,
		remaining: len(topo),
		pending:   make(map[string]int, len(nodes)),
		results:   make(map[string]Result, len(nodes)),
		ready:     make([]string, 0, len(nodes)),
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
// worker 只通过有界通道与其通信）。
type runner struct {
	cfg       Config
	nodes     map[string]*node
	remaining int // 尚未到达终态的任务数
	pending   map[string]int
	results   map[string]Result
	ready     []string // 已就绪待派发的任务 ID
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
	canceled := false

	// finish 回收一个执行结果并推进调度状态。
	finish := func(res Result) {
		inflight--
		r.remaining--
		r.results[res.TaskID] = res
		r.cfg.logf("finished task=%s status=%s kind=%q", res.TaskID, res.Status, res.Kind)
		if res.Status == StatusSuccess {
			for _, ch := range r.nodes[res.TaskID].children {
				r.pending[ch]--
				if r.pending[ch] == 0 {
					r.ready = append(r.ready, ch)
					r.cfg.logf("ready task=%s", ch)
				}
			}
		} else {
			r.skipDescendants(res.TaskID, res)
		}
	}

	// 主调度循环：select 同时监听派发与回收，通道均有界，形成背压。
	for r.remaining > 0 && !canceled {
		if len(r.ready) > 0 {
			next := r.ready[0]
			item := r.makeWorkItem(next)
			select {
			case dispatch <- item:
				r.ready = r.ready[1:]
				inflight++
				r.cfg.logf("dispatch task=%s inflight=%d", next, inflight)
			case res := <-resultsCh:
				finish(res)
			case <-ctx.Done():
				canceled = true
			}
		} else {
			select {
			case res := <-resultsCh:
				finish(res)
			case <-ctx.Done():
				canceled = true
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

	if canceled {
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
	out := &RunResult{Results: r.results, Complete: true}
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
	val, err := n.task.Run(taskCtx, item.deps)
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
