package engine

import (
	"fmt"
	"sort"
)

// node 是图内部使用的节点状态（不可变，供调度器安全共享）。
type node struct {
	task     Task
	deps     []string // 去重后的依赖
	children []string // 下游节点
}

// buildGraph 校验任务集合并构建依赖图：
// 任务定义本身不合法（空 ID、空计算体）返回 ErrInvalidTask；
// 重复 ID 去重（只保留首个，保证同一输入只计算一次）；
// 引用不存在节点返回 ErrMissingDep；成环返回 ErrCycle。
// 校验在任何任务执行之前整体完成，非法输入不会触发任何计算。
// 返回确定性的拓扑序（按 ID 字典序打破平局，保证结果与调度顺序无关）。
func buildGraph(tasks []Task) (nodes map[string]*node, topo []string, err error) {
	nodes = make(map[string]*node, len(tasks))
	var order []string // 去重后的声明顺序，仅用于日志与确定性
	for idx, t := range tasks {
		if t.ID == "" {
			return nil, nil, fmt.Errorf("%w: task at index %d has empty id",
				ErrInvalidTask, idx)
		}
		if t.Run == nil {
			return nil, nil, fmt.Errorf("%w: task %q has no run function",
				ErrInvalidTask, t.ID)
		}
		if _, dup := nodes[t.ID]; dup {
			continue // 去重：同一输入重复提交只保留一次
		}
		nodes[t.ID] = &node{task: t}
		order = append(order, t.ID)
	}

	// 校验依赖存在性并连边（dep -> child）。
	for _, id := range order {
		n := nodes[id]
		seen := make(map[string]bool, len(n.task.Deps))
		for _, dep := range n.task.Deps {
			d, ok := nodes[dep]
			if !ok {
				return nil, nil, fmt.Errorf("%w: task %q depends on unknown task %q",
					ErrMissingDep, id, dep)
			}
			if seen[dep] {
				continue // 重复依赖只算一条边
			}
			seen[dep] = true
			n.deps = append(n.deps, dep)
			d.children = append(d.children, id)
		}
	}

	// Kahn 拓扑排序；就绪集合按字典序取最小者，保证确定性。
	indegree := make(map[string]int, len(nodes))
	ready := make([]string, 0, len(nodes))
	for _, id := range order {
		indegree[id] = len(nodes[id].deps)
		if indegree[id] == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	topo = make([]string, 0, len(nodes))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		topo = append(topo, id)
		var freed []string
		for _, ch := range nodes[id].children {
			indegree[ch]--
			if indegree[ch] == 0 {
				freed = append(freed, ch)
			}
		}
		if len(freed) > 0 {
			sort.Strings(freed)
			// 归并保持 ready 有序。
			merged := make([]string, 0, len(ready)+len(freed))
			i, j := 0, 0
			for i < len(ready) && j < len(freed) {
				if ready[i] < freed[j] {
					merged = append(merged, ready[i])
					i++
				} else {
					merged = append(merged, freed[j])
					j++
				}
			}
			merged = append(merged, ready[i:]...)
			merged = append(merged, freed[j:]...)
			ready = merged
		}
	}
	if len(topo) != len(nodes) {
		return nil, nil, fmt.Errorf("%w: %d of %d tasks are unreachable from sources",
			ErrCycle, len(nodes)-len(topo), len(nodes))
	}
	return nodes, topo, nil
}
