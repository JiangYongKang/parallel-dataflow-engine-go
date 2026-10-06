package engine

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"reflect"
	"sort"
	"sync"
)

// cacheEntry 是一条可复用结果：只有成功收尾的产物才会进入缓存。
type cacheEntry struct {
	fingerprint uint64 // 任务级联指纹（自身定义 + 全部上游指纹），用于判定是否可复用
	defHash     uint64 // 仅任务自身定义的哈希，用于在日志中区分失效原因
	value       any    // 上次成功运行的产出
}

// keyedEntry 让 list 元素携带任务 ID，淘汰时可反查删除 map 项。
type keyedEntry struct {
	id    string
	entry cacheEntry
}

// resultCache 是跨运行的结果缓存：固定容量、LRU 淘汰、并发安全。
// 同一 Engine 被多个 Run 并发使用时，读写都通过互斥锁串行化，
// 缓存本身不保存任何运行中状态，并发运行不会互相污染。
type resultCache struct {
	mu     sync.Mutex
	cap    int
	ll     *list.List // front = 最近使用
	items  map[string]*list.Element
	hits   uint64
	misses uint64
}

func newResultCache(capacity int) *resultCache {
	return &resultCache{
		cap:   capacity,
		ll:    list.New(),
		items: make(map[string]*list.Element),
	}
}

// get 查找可复用条目；命中会将其提升为最近使用。
func (c *resultCache) get(id string) (cacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[id]
	if !ok {
		return cacheEntry{}, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(keyedEntry).entry, true
}

// note 记录一次复用判定结果（命中/未命中），由调度方在比对指纹后调用。
func (c *resultCache) note(hit bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if hit {
		c.hits++
	} else {
		c.misses++
	}
}

// put 写入/更新条目，超出容量时淘汰最久未使用者。
func (c *resultCache) put(id string, e cacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[id]; ok {
		el.Value = keyedEntry{id: id, entry: e}
		c.ll.MoveToFront(el)
		return
	}
	c.items[id] = c.ll.PushFront(keyedEntry{id: id, entry: e})
	for c.ll.Len() > c.cap {
		back := c.ll.Back()
		if back == nil {
			break
		}
		delete(c.items, back.Value.(keyedEntry).id)
		c.ll.Remove(back)
	}
}

// clear 清空全部条目（计数保留，便于观测累计命中情况）。
func (c *resultCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ll.Init()
	c.items = make(map[string]*list.Element)
}

// stats 返回当前缓存状况快照。
func (c *resultCache) stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{
		Enabled: true,
		Entries: c.ll.Len(),
		Hits:    c.hits,
		Misses:  c.misses,
	}
}

// defHashOf 计算任务自身定义的哈希：计算体（代码指针或显式 CacheKey）+ 超时。
// 注意：不含依赖——依赖通过 fingerprintOf 级联纳入。
func defHashOf(t Task) uint64 {
	h := fnv.New64a()
	if t.CacheKey != "" {
		fmt.Fprintf(h, "key:%s;", t.CacheKey)
	} else {
		// 以函数代码指针作为定义标识：同一函数字面量多次构造视为同一定义。
		// 闭包捕获的变量不在指纹内（见 README 的兼容范围说明）。
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(reflect.ValueOf(t.Run).Pointer()))
		h.Write(buf[:])
	}
	fmt.Fprintf(h, "timeout:%d;", t.Timeout)
	return h.Sum64()
}

// fingerprintOf 计算任务的级联指纹：自身定义 + 全部直接依赖的指纹。
// 任一上游定义或产物来源变化都会传导改变下游指纹，从而触发增量重算。
// depFPs 为 depID -> 依赖的级联指纹（按拓扑序计算，此处必然已就绪）。
func fingerprintOf(t Task, defHash uint64, depFPs map[string]uint64) uint64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "id:%s;def:%d;", t.ID, defHash)
	deps := make([]string, 0, len(depFPs))
	for d := range depFPs {
		deps = append(deps, d)
	}
	sort.Strings(deps) // 排序保证指纹与依赖声明顺序无关
	for _, d := range deps {
		fmt.Fprintf(h, "dep:%s=%d;", d, depFPs[d])
	}
	return h.Sum64()
}
