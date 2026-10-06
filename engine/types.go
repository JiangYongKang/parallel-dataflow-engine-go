// Package engine 提供本地并行数据流执行引擎：
// 按依赖拓扑调度任务，保证结果与并发度、调度顺序无关。
package engine

import (
	"context"
	"errors"
	"time"
)

// FailureKind 对运行或任务未成功完成的原因进行分类，便于调用方区分处理。
type FailureKind string

const (
	FailureNone        FailureKind = ""
	FailureInvalidTask FailureKind = "invalid_task"       // 任务定义本身不合法（空 ID / 无计算体）
	FailureCycle       FailureKind = "cycle"              // 依赖成环
	FailureMissingDep  FailureKind = "missing_dependency" // 引用不存在的节点
	FailureTaskError   FailureKind = "task_error"         // 任务自身返回错误
	FailureTimeout     FailureKind = "timeout"            // 任务超时
	FailureCanceled    FailureKind = "canceled"           // 整体取消
)

// 可区分的基础错误，配合 errors.Is 使用。
var (
	ErrInvalidTask = errors.New("engine: invalid task definition")
	ErrCycle       = errors.New("engine: dependency graph contains a cycle")
	ErrMissingDep  = errors.New("engine: task references a missing dependency")
	ErrCanceled    = errors.New("engine: run canceled")
	ErrTimeout     = errors.New("engine: task timed out")
)

// Status 是单个任务的终态。
type Status string

const (
	StatusSuccess Status = "success" // 正常完成，Value 有效
	StatusFailed  Status = "failed"  // 执行失败（错误/超时/取消）
	StatusSkipped Status = "skipped" // 因上游失败或整体取消被跳过
)

// TaskFunc 是任务的计算体。deps 为全部直接依赖的输出值（仅含成功依赖）。
type TaskFunc func(ctx context.Context, deps map[string]any) (any, error)

// Task 描述一个计算节点。
type Task struct {
	ID      string        // 唯一标识；重复 ID 会被去重，只计算一次
	Deps    []string      // 依赖的任务 ID
	Run     TaskFunc      // 计算体
	Timeout time.Duration // 单任务超时，0 表示使用引擎默认值
	// CacheKey 可选：显式声明任务定义的版本/内容标识。
	// 非空时参与跨运行复用指纹（替代函数指针），便于进程内多次构造
	// 等价闭包时仍能命中缓存；为空时以 Run 的代码指针作为定义指纹。
	CacheKey string
}

// Result 是单个任务的产出。
type Result struct {
	TaskID string
	Status Status
	Value  any   // 仅 StatusSuccess 时有效
	Err    error // 失败/跳过时的原因
	Kind   FailureKind
	Reused bool // true 表示本次未真正执行，复用了上一次成功运行的结果
}

// RunResult 是一次运行的汇总。
type RunResult struct {
	Results  map[string]Result // 按任务 ID 索引，与调度顺序无关
	Complete bool              // 全部为成功时才为 true；部分结果存在时为 false
	Err      error             // 整体失败原因（可 errors.Is 区分）
	Kind     FailureKind
}

// Config 是引擎可配置项。
type Config struct {
	MaxConcurrency int                              // 并行执行上限，<=0 时取默认值
	BufferSize     int                              // 内部调度/结果缓冲上限（背压），<=0 时取默认值
	TaskTimeout    time.Duration                    // 默认单任务超时，<=0 表示不超时
	Logger         func(format string, args ...any) // 可选：调度事件日志
	// CacheSize 是跨运行结果缓存的容量上限（LRU 淘汰）：
	// <=0 表示关闭缓存（默认，行为与此前版本完全一致）；
	// >0 时同一 Engine 实例的多次 Run 之间复用未变化任务的成功结果。
	CacheSize int
}

// CacheStats 是缓存运行状况的快照，用于观测与测试。
type CacheStats struct {
	Enabled bool // 缓存是否启用
	Entries int  // 当前缓存条目数
	Hits    uint64
	Misses  uint64
}

func (c Config) withDefaults() Config {
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = 4
	}
	if c.BufferSize <= 0 {
		c.BufferSize = 64
	}
	return c
}

func (c Config) logf(format string, args ...any) {
	if c.Logger != nil {
		c.Logger(format, args...)
	}
}
