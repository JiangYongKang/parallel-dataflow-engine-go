package engine

import "errors"

// 可区分的失败原因，调用方可用 errors.Is 判定。
var (
	ErrCycle          = errors.New("engine: dependency cycle detected")
	ErrMissingDep     = errors.New("engine: dependency references unknown node")
	ErrDuplicateID    = errors.New("engine: duplicate task id")
	ErrTaskFailed     = errors.New("engine: task failed")
	ErrTaskTimeout    = errors.New("engine: task timed out")
	ErrUpstreamFailed = errors.New("engine: skipped due to upstream failure")
	ErrCanceled       = errors.New("engine: execution canceled")
	ErrIncomplete     = errors.New("engine: result is incomplete")
)
