package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Options 控制引擎的并发与缓冲行为。
type Options struct {
	MaxConcurrency int  // 活跃并行执行上限，<=0 时取 1
	QueueSize      int  // 就绪队列缓冲上限（背压），<=0 时取 1
	Dedup          bool // 相同指纹的重复提交直接命中缓存，不再实际计算
}

func (o Options) normalize() Options {
	if o.MaxConcurrency <= 0 {
		o.MaxConcurrency = 1
	}
	if o.QueueSize <= 0 {
		o.QueueSize = 1
	}
	return o
}

// Engine 是本地并行数据流执行引擎。
type Engine struct {
	opts Options

	mu    sync.Mutex
	cache map[string]*Output // 去重缓存：图指纹 -> 完整结果
}

func New(opts Options) *Engine {
	return &Engine{opts: opts.normalize(), cache: map[string]*Output{}}
}

// Execute 校验并执行整张图，返回汇总结果。
// 图非法（成环/缺依赖）时返回 nil 与可区分的错误，不执行任何任务。
// 执行类失败时返回非 nil 的 Output，其中部分结果被明确标记。
func (e *Engine) Execute(ctx context.Context, g *Graph) (*Output, error) {
	if err := g.Validate(); err != nil {
		return nil, err
	}
	fp := g.Fingerprint()
	if e.opts.Dedup {
		e.mu.Lock()
		cached, ok := e.cache[fp]
		e.mu.Unlock()
		if ok {
			return cached, cached.Err
		}
	}
	out := e.run(ctx, g)
	if e.opts.Dedup && out.Complete {
		e.mu.Lock()
		e.cache[fp] = out
		e.mu.Unlock()
	}
	return out, out.Err
}

// workItem 是调度器发给 worker 的最小执行单元，输入已随消息携带。
type workItem struct {
	task   Task
	inputs map[string]any
}

func (e *Engine) run(ctx context.Context, g *Graph) *Output {
	tasks := g.Tasks()
	n := len(tasks)
	children := make(map[string][]string, n)
	indeg := make(map[string]int, n)
	for id, t := range tasks {
		indeg[id] = len(t.Deps)
		for _, d := range t.Deps {
			children[d] = append(children[d], id)
		}
	}

	results := make(map[string]Result, n)
	dispatch := make(chan workItem, e.opts.QueueSize) // 有界队列：背压
	done := make(chan Result, n)                      // 足够缓冲，worker 永不阻塞

	var wg sync.WaitGroup
	for i := 0; i < e.opts.MaxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for wi := range dispatch {
				done <- runTask(ctx, wi.task, wi.inputs)
			}
		}()
	}

	terminal := 0   // 已有终态结果的节点数
	inflight := 0   // 已派发未回收的节点数
	dispatched := make(map[string]bool, n)

	tryDispatch := func(id string) {
		t := tasks[id]
		inputs := make(map[string]any, len(t.Deps))
		for _, d := range t.Deps {
			inputs[d] = results[d].Value
		}
		dispatched[id] = true
		inflight++
		dispatch <- workItem{task: t, inputs: inputs} // 队列满则阻塞，形成背压
	}

	// 初始就绪集合按字典序派发，保证调度顺序确定。
	var ready []string
	for id, d := range indeg {
		if d == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	for _, id := range ready {
		tryDispatch(id)
	}

	// skipDescendants 将未终态的下游全部标记为跳过。
	var skipDescendants func(id string)
	skipDescendants = func(id string) {
		for _, c := range children[id] {
			if _, ok := results[c]; !ok && !dispatched[c] {
				results[c] = Result{ID: c, Status: StatusSkipped, Err: ErrUpstreamFailed}
				terminal++
				skipDescendants(c)
			}
		}
	}

	canceled := false
	markCanceled := func() {
		for id := range tasks {
			if _, ok := results[id]; !ok && !dispatched[id] {
				results[id] = Result{ID: id, Status: StatusCanceled, Err: ErrCanceled}
				terminal++
			}
		}
	}

	for terminal < n {
		if canceled {
			if inflight == 0 {
				break
			}
			r := <-done // 等待在途任务收尾，不遗留执行单元
			inflight--
			results[r.ID] = r
			terminal++
			continue
		}
		select {
		case r := <-done:
			inflight--
			results[r.ID] = r
			terminal++
			if r.Status == StatusSuccess {
				var next []string
				for _, c := range children[r.ID] {
					indeg[c]--
					if indeg[c] == 0 {
						next = append(next, c)
					}
				}
				sort.Strings(next)
				for _, id := range next {
					tryDispatch(id)
				}
			} else {
				skipDescendants(r.ID)
			}
		case <-ctx.Done():
			canceled = true
			markCanceled()
		}
	}
	close(dispatch)
	wg.Wait() // 回收全部 worker，不残留执行单元

	return summarize(results, canceled)
}

// runTask 执行单个任务，处理超时与取消归类。
func runTask(ctx context.Context, t Task, inputs map[string]any) Result {
	if err := ctx.Err(); err != nil {
		return Result{ID: t.ID, Status: StatusCanceled, Err: ErrCanceled}
	}
	tctx := ctx
	cancel := func() {}
	if t.Timeout > 0 {
		tctx, cancel = context.WithTimeout(ctx, t.Timeout)
	}
	defer cancel()
	v, err := t.Run(tctx, inputs)
	switch {
	case err == nil:
		return Result{ID: t.ID, Status: StatusSuccess, Value: v}
	case t.Timeout > 0 && errors.Is(tctx.Err(), context.DeadlineExceeded) && ctx.Err() == nil:
		return Result{ID: t.ID, Status: StatusTimeout, Err: fmt.Errorf("%w: %s", ErrTaskTimeout, t.ID)}
	case ctx.Err() != nil:
		return Result{ID: t.ID, Status: StatusCanceled, Err: ErrCanceled}
	default:
		return Result{ID: t.ID, Status: StatusFailed, Err: fmt.Errorf("%w: %s: %v", ErrTaskFailed, t.ID, err)}
	}
}

// summarize 汇总终态结果，生成整体判定。
func summarize(results map[string]Result, canceled bool) *Output {
	out := &Output{Complete: true, Results: results}
	var failed, timeout []string
	for _, r := range results {
		switch r.Status {
		case StatusSuccess:
		case StatusFailed:
			failed = append(failed, r.ID)
		case StatusTimeout:
			timeout = append(timeout, r.ID)
		default: // skipped / canceled
		}
		if r.Status != StatusSuccess {
			out.Complete = false
		}
	}
	sort.Strings(failed)
	sort.Strings(timeout)
	switch {
	case canceled:
		out.Err = ErrCanceled
	case len(failed) > 0 || len(timeout) > 0:
		var errs []error
		if len(failed) > 0 {
			errs = append(errs, fmt.Errorf("%w: %s", ErrTaskFailed, strings.Join(failed, ",")))
		}
		if len(timeout) > 0 {
			errs = append(errs, fmt.Errorf("%w: %s", ErrTaskTimeout, strings.Join(timeout, ",")))
		}
		out.Err = errors.Join(errs...)
	}
	return out
}
