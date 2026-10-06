package engine

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

// depKey 描述“某次候选计算”所依赖的一个上游产物：节点 ID 与该产物的内容指纹。
type depKey struct {
	id     string
	digest string
}

// cacheEntry 是一个任务上次成功运行留下的可复用产物。
// 只有任务定义指纹（Version/Timeout）与全部直接依赖的（ID, 产物指纹）
// 都与本次候选一致时，Value 才允许复用。
type cacheEntry struct {
	value   any           // 上次成功产出的值
	digest  string        // 本产物自身的内容指纹（供下游比对）
	version string        // 任务定义版本
	timeout time.Duration // 生效的单任务超时
	deps    []depKey      // 上次计算时的直接依赖快照（ID 字典序）
}

// resultCache 是引擎级、跨运行共享的成功产物缓存。
// 内部自带互斥锁，支持同一引擎实例被多个 Run 并发访问；
// 容量受 maxEntries 约束，超限按 LRU（最久未访问）淘汰。
type resultCache struct {
	mu      sync.Mutex
	max     int
	entries map[string]*cacheEntry
	lru     *list.List // 元素 *lruItem，队首为最久未访问
	elemOf  map[string]*list.Element
}

type lruItem struct {
	id string
}

func newResultCache(maxEntries int) *resultCache {
	if maxEntries <= 0 {
		maxEntries = defaultMaxCacheEntries
	}
	return &resultCache{
		max:     maxEntries,
		entries: make(map[string]*cacheEntry),
		lru:     list.New(),
		elemOf:  make(map[string]*list.Element),
	}
}

// sameDeps 判断两组依赖键是否完全一致（顺序无关，重复边在构图期已去除）。
func sameDeps(a, b []depKey) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]depKey(nil), a...)
	y := append([]depKey(nil), b...)
	sort.Slice(x, func(i, j int) bool { return x[i].id < x[j].id })
	sort.Slice(y, func(i, j int) bool { return y[i].id < y[j].id })
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// lookup 判定缓存是否命中：Version、生效 Timeout 与全部直接依赖的（ID, 指纹）
// 必须与已存条目完全一致。命中时返回缓存值并刷新 LRU。
func (c *resultCache) lookup(id, version string, timeout time.Duration, deps []depKey) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[id]
	if !ok {
		return nil, false
	}
	if e.version != version || e.timeout != timeout || !sameDeps(e.deps, deps) {
		return nil, false
	}
	if el, ok := c.elemOf[id]; ok {
		c.lru.MoveToBack(el) // 命中刷新为最近使用
	}
	return e.value, true
}

// digestOf 返回某任务已缓存产物的内容指纹；不存在返回 ""。
// 仅读取、不改变 LRU 顺序（产物指纹在同一次判定中会被反复查询）。
func (c *resultCache) digestOf(id string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[id]; ok {
		return e.digest
	}
	return ""
}

// store 写入或覆盖一个成功产物并置于最近使用；超容量时淘汰队首（最久未访问）。
// 返回 storeResult，供调用方在运行被取消时把本次写入整体回滚。
func (c *resultCache) store(id, version string, timeout time.Duration, deps []depKey, value any) storeResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := append([]depKey(nil), deps...)
	sort.Slice(d, func(i, j int) bool { return d[i].id < d[j].id })
	res := storeResult{id: id, hadOld: false}
	if old, ok := c.entries[id]; ok {
		res.hadOld = true
		res.old = *old // 快照旧条目（结构体复制即可，字段均不再原地修改）
		// 记录旧条目在 LRU 中的前驱，回滚时把元素精确插回原位置。
		if el := c.elemOf[id]; el != nil && el.Prev() != nil {
			res.after = el.Prev().Value.(*lruItem).id
		}
	}
	entry := &cacheEntry{
		value:   value,
		digest:  digestValue(value),
		version: version,
		timeout: timeout,
		deps:    d,
	}
	c.entries[id] = entry
	if el, ok := c.elemOf[id]; ok {
		c.lru.MoveToBack(el)
	} else {
		c.elemOf[id] = c.lru.PushBack(&lruItem{id: id})
	}
	for c.lru.Len() > c.max {
		front := c.lru.Front()
		if front == nil {
			break
		}
		victimID := front.Value.(*lruItem).id
		c.removeLocked(victimID)
	}
	return res
}

// storeResult 记录一次写入对缓存造成的影响，用于取消时回滚。
type storeResult struct {
	id     string
	hadOld bool
	old    cacheEntry
	after  string // 旧条目在 LRU 中的前驱 ID；空串表示它原本就在队首
}

// rollback 撤销一次 store：恢复旧条目到原 LRU 位置，或移除本次新增的条目。
func (c *resultCache) rollback(res storeResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if res.hadOld {
		old := res.old
		c.entries[res.id] = &old
		el, ok := c.elemOf[res.id]
		if !ok {
			// 极端情况下本条目在写入后又被容量淘汰，回滚重建元素。
			el = c.lru.PushBack(&lruItem{id: res.id})
			c.elemOf[res.id] = el
		}
		if res.after == "" {
			c.lru.MoveToFront(el)
		} else if pred, ok := c.elemOf[res.after]; ok {
			c.lru.MoveAfter(el, pred)
		}
	} else {
		c.removeLocked(res.id)
	}
}

// removeLocked 删除条目及其 LRU 元素，调用方需持锁。
func (c *resultCache) removeLocked(id string) {
	delete(c.entries, id)
	if el, ok := c.elemOf[id]; ok {
		c.lru.Remove(el)
		delete(c.elemOf, id)
	}
}

func (c *resultCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// clear 清空全部条目，原子地返回被移除的数量。
func (c *resultCache) clear() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.entries)
	c.entries = make(map[string]*cacheEntry)
	c.elemOf = make(map[string]*list.Element)
	c.lru.Init()
	return n
}

// digestValue 为任意产出值生成稳定的内容指纹。
// 采用 sha256(规范化的 Go 表示)，map 键经排序后输出，保证跨运行确定。
// 注意：最终的复用正确性以 reflect.DeepEqual 逐依赖比对为准，
// 指纹只用于快速排除、下游键构造与日志可读；含指针/通道/函数字段的值可能不跨进程稳定。
func digestValue(v any) string {
	var b strings.Builder
	digestReflect(&b, reflect.ValueOf(v))
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func digestReflect(b *strings.Builder, rv reflect.Value) {
	if !rv.IsValid() {
		b.WriteString("<nil>")
		return
	}
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface:
		if rv.IsNil() {
			b.WriteString("<nil>")
			return
		}
		b.WriteByte('&')
		digestReflect(b, rv.Elem())
	case reflect.Struct:
		t := rv.Type()
		b.WriteString(t.String())
		b.WriteByte('{')
		for i := 0; i < rv.NumField(); i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(t.Field(i).Name)
			b.WriteByte(':')
			digestReflect(b, rv.Field(i))
		}
		b.WriteByte('}')
	case reflect.Map:
		if rv.IsNil() {
			b.WriteString("<nilmap>")
			return
		}
		keys := rv.MapKeys()
		sort.Slice(keys, func(i, j int) bool {
			return fmt.Sprintf("%#v", keys[i].Interface()) < fmt.Sprintf("%#v", keys[j].Interface())
		})
		b.WriteString(rv.Type().String())
		b.WriteString("{")
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			digestReflect(b, k)
			b.WriteByte(':')
			digestReflect(b, rv.MapIndex(k))
		}
		b.WriteByte('}')
	case reflect.Slice, reflect.Array:
		b.WriteString(rv.Type().String())
		b.WriteByte('[')
		for i := 0; i < rv.Len(); i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			digestReflect(b, rv.Index(i))
		}
		b.WriteByte(']')
	case reflect.String:
		b.WriteString(strconvQuote(rv.String()))
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		// 无法稳定表达的类型：以类型名占位，交由 DeepEqual 兜底判定。
		b.WriteString(rv.Type().String())
	default:
		fmt.Fprintf(b, "%#v", rv.Interface())
	}
}

func strconvQuote(s string) string {
	return fmt.Sprintf("%q", s)
}

// shortDigest 截取指纹前 12 位用于日志，完整判定仍由缓存内部完整比较完成。
func shortDigest(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// digestTask 生成“任务定义 + 依赖集合”的候选指纹（确定性，供日志说明判定依据）。
func digestTask(version string, timeout time.Duration, deps []depKey) string {
	d := append([]depKey(nil), deps...)
	sort.Slice(d, func(i, j int) bool { return d[i].id < d[j].id })
	var b strings.Builder
	fmt.Fprintf(&b, "ver=%q|to=%d|deps=", version, int64(timeout))
	for _, k := range d {
		fmt.Fprintf(&b, "%s:%s;", k.id, k.digest)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:12])
}
