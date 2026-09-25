package engine

import "context"

// Options 控制引擎的并发与缓冲行为。
type Options struct {
	MaxConcurrency int  // 活跃并行执行上限，<=0 时取 1
	QueueSize      int  // 就绪队列缓冲上限（背压），<=0 时取 1
	Dedup          bool // 相同指纹的重复提交直接命中缓存，不再实际计算
}

// Engine 是本地并行数据流执行引擎。
type Engine struct {
	opts Options
}

func New(opts Options) *Engine { return &Engine{opts: opts} }

// Execute 校验并执行整张图，返回汇总结果。
// 即使整体失败也会返回非 nil 的 Output，其中部分结果被明确标记。
func (e *Engine) Execute(ctx context.Context, g *Graph) (*Output, error) {
	return nil, nil
}
