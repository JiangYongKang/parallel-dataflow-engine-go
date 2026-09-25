package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
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
}

func NewGraph() *Graph { return &Graph{tasks: map[string]Task{}} }

// Add 向图中加入任务，ID 重复时报错。
func (g *Graph) Add(t Task) error {
	if t.ID == "" {
		return fmt.Errorf("%w: empty id", ErrDuplicateID)
	}
	if _, ok := g.tasks[t.ID]; ok {
		return fmt.Errorf("%w: %q", ErrDuplicateID, t.ID)
	}
	if t.Run == nil {
		return fmt.Errorf("engine: task %q has nil Run", t.ID)
	}
	g.tasks[t.ID] = t
	return nil
}

// Validate 校验图：依赖必须存在且不得成环。
func (g *Graph) Validate() error {
	for _, t := range g.tasks {
		for _, d := range t.Deps {
			if _, ok := g.tasks[d]; !ok {
				return fmt.Errorf("%w: task %q depends on unknown %q", ErrMissingDep, t.ID, d)
			}
		}
	}
	_, err := g.TopoOrder()
	return err
}

// TopoOrder 返回确定性的拓扑序（同层按 ID 字典序），仅供日志与测试使用。
// 成环时返回 ErrCycle 及环上节点信息。
func (g *Graph) TopoOrder() ([]string, error) {
	indeg := make(map[string]int, len(g.tasks))
	children := make(map[string][]string, len(g.tasks))
	for id, t := range g.tasks {
		indeg[id] = len(t.Deps)
		for _, d := range t.Deps {
			children[d] = append(children[d], id)
		}
	}
	var ready []string
	for id, d := range indeg {
		if d == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	order := make([]string, 0, len(g.tasks))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		var next []string
		for _, c := range children[id] {
			indeg[c]--
			if indeg[c] == 0 {
				next = append(next, c)
			}
		}
		if len(next) > 0 {
			ready = append(ready, next...)
			sort.Strings(ready)
		}
	}
	if len(order) != len(g.tasks) {
		var cyc []string
		for id, d := range indeg {
			if d > 0 {
				cyc = append(cyc, id)
			}
		}
		sort.Strings(cyc)
		return nil, fmt.Errorf("%w: involving %s", ErrCycle, strings.Join(cyc, ","))
	}
	return order, nil
}

// Fingerprint 返回图的确定性指纹，用于结果去重。
// 指纹覆盖节点 ID、依赖关系与输入载荷，与 map 遍历顺序无关。
func (g *Graph) Fingerprint() string {
	ids := make([]string, 0, len(g.tasks))
	for id := range g.tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	h := sha256.New()
	for _, id := range ids {
		t := g.tasks[id]
		deps := append([]string(nil), t.Deps...)
		sort.Strings(deps)
		fmt.Fprintf(h, "%s|%s|%#v|%d\n", t.ID, strings.Join(deps, ","), t.Input, t.Timeout)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Tasks 返回图中任务的快照。
func (g *Graph) Tasks() map[string]Task {
	out := make(map[string]Task, len(g.tasks))
	for k, v := range g.tasks {
		out[k] = v
	}
	return out
}
