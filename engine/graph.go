package engine

import (
	"context"
	"time"
)

// TaskFunc 是节点的计算函数。inputs 为其直接依赖节点的输出（按依赖 ID 索引）。
type TaskFunc func(ctx context.Context, inputs map[string]any) (any, error)

// Task 描述数据流图中的一个节点。
type Task struct {
	ID      string
	Deps    []string
	Input   any // 参与去重指纹计算的输入载荷
	Timeout time.Duration
	Run     TaskFunc
}

// Graph 是任务的集合，执行前必须通过 Validate。
type Graph struct {
	tasks map[string]Task
	order []string
}

func NewGraph() *Graph { return &Graph{tasks: map[string]Task{}} }

// Add 向图中加入任务，ID 重复时报错。
func (g *Graph) Add(t Task) error { return nil }

// Validate 校验图：依赖必须存在且不得成环。
func (g *Graph) Validate() error { return nil }

// TopoOrder 返回确定性的拓扑序（同层按 ID 字典序），仅供日志与测试使用。
func (g *Graph) TopoOrder() ([]string, error) { return nil, nil }

// Fingerprint 返回图的确定性指纹，用于结果去重。
func (g *Graph) Fingerprint() string { return "" }

// Tasks 返回图中任务的快照。
func (g *Graph) Tasks() map[string]Task { return nil }
