package engine

import "sort"

// Status 标记单个节点的终态。
type Status int

const (
	StatusSuccess Status = iota
	StatusFailed
	StatusTimeout
	StatusSkipped
	StatusCanceled
)

func (s Status) String() string {
	switch s {
	case StatusSuccess:
		return "success"
	case StatusFailed:
		return "failed"
	case StatusTimeout:
		return "timeout"
	case StatusSkipped:
		return "skipped"
	case StatusCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

// Result 是单个节点的执行结果。
type Result struct {
	ID     string
	Status Status
	Value  any
	Err    error
}

// Output 是一次执行的汇总。Complete 为 false 时，Results 中的成功项
// 仅为部分结果，不得当作整体成功使用。
type Output struct {
	Complete bool
	Results  map[string]Result
	Err      error // 整体级错误（如取消、存在失败节点）
}

// Get 仅当节点成功且整体结果完整时返回其值。
func (o *Output) Get(id string) (any, bool) {
	if o == nil || !o.Complete {
		return nil, false
	}
	r, ok := o.Results[id]
	if !ok || r.Status != StatusSuccess {
		return nil, false
	}
	return r.Value, true
}

// Ordered 按节点 ID 字典序返回结果，保证与调度顺序无关的稳定输出。
func (o *Output) Ordered() []Result {
	ids := make([]string, 0, len(o.Results))
	for id := range o.Results {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]Result, 0, len(ids))
	for _, id := range ids {
		out = append(out, o.Results[id])
	}
	return out
}
