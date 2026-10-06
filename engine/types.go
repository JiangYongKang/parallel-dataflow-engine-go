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
	FailureCycle       FailureKind = "cycle"              // 依赖成环
	FailureMissingDep  FailureKind = "missing_dependency" // 引用不存在的节点
	FailureInvalidTask FailureKind = "invalid_task"       // 任务定义本身不合法
	FailureTaskError   FailureKind = "task_error"         // 任务自身返回错误
	FailureTimeout     FailureKind = "timeout"            // 任务超时
	FailureCanceled    FailureKind = "canceled"           // 整体取消
)

// 可区分的基础错误，配合 errors.Is 使用。
var (
	ErrCycle       = errors.New("engine: dependency graph contains a cycle")
	ErrMissingDep  = errors.New("engine: task references a missing dependency")
	ErrInvalidTask = errors.New("engine: invalid task definition")
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
	Version string        // 任务定义版本：计算体语义或外部捕获输入变化时应递增/修改它，
	// 跨运行复用时会与依赖产物指纹共同参与一致性判定；留空表示“定义自首次提交起不变”
}

// Result 是单个任务的产出。
type Result struct {
	TaskID string
	Status Status
	Value  any   // 仅 StatusSuccess 时有效
	Reused bool  // StatusSuccess 且 Value 来自跨运行缓存命中时为 true（未真正执行计算体）
	Err    error // 失败/跳过时的原因
	Kind   FailureKind
}

// RunResult 是一次运行的汇总。
type RunResult struct {
	Results       map[string]Result // 按任务 ID 索引，与调度顺序无关
	Complete      bool              // 全部为成功时才为 true；部分结果存在时为 false
	Err           error             // 整体失败原因（可 errors.Is 区分）
	Kind          FailureKind
	ReusedCount   int // 本次运行直接复用缓存、未真正执行的任务数
	ComputedCount int // 本次运行真正执行计算体并成功的任务数
}

// Config 是引擎可配置项。
type Config struct {
	MaxConcurrency int                              // 并行执行上限，<=0 时取默认值
	BufferSize     int                              // 内部调度/结果缓冲上限（背压），<=0 时取默认值
	TaskTimeout    time.Duration                    // 默认单任务超时，<=0 表示不超时
	Logger         func(format string, args ...any) // 可选：调度事件日志

	// DisableCache 控制跨运行结果复用：默认 false（开启复用）。
	// 置为 true 时每次运行全量重算，既不读也不写缓存，行为与无缓存引擎一致。
	DisableCache bool
	// MaxCacheEntries 限制缓存的任务产物条目数，<=0 时取默认值。
	// 超限时按“最久未访问”淘汰；ClearCache 可主动清空。
	MaxCacheEntries int
}

// 默认缓存容量：防止长期运行的实例随提交次数无界增长。
const defaultMaxCacheEntries = 1024

func (c Config) withDefaults() Config {
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = 4
	}
	if c.BufferSize <= 0 {
		c.BufferSize = 64
	}
	if c.MaxCacheEntries <= 0 {
		c.MaxCacheEntries = defaultMaxCacheEntries
	}
	return c
}

func (c Config) cacheActive() bool {
	return !c.DisableCache
}

func (c Config) logf(format string, args ...any) {
	if c.Logger != nil {
		c.Logger(format, args...)
	}
}
