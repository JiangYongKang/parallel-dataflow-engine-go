# parallel-dataflow-engine-go

本地并行数据流执行引擎：按依赖拓扑调度任意 DAG 计算任务，保证**同一输入在多次运行、不同并发度下得到完全一致的结果**；并支持**跨运行结果复用与增量重算**——同一引擎实例反复提交相似图时，未变化的任务直接复用上一次成功产物，只重算受影响的子图。

## 快速开始

```go
e := engine.New(engine.Config{MaxConcurrency: 8, BufferSize: 64}) // 默认即开启跨运行复用
res := e.Run(ctx, []engine.Task{
    {ID: "a", Version: "v1", Run: func(ctx context.Context, deps map[string]any) (any, error) { return 1, nil }},
    {ID: "b", Version: "v1", Deps: []string{"a"}, Run: /* 读取 deps["a"] */ ...},
})
// 第二次提交同一张图：res.ReusedCount=2，计算体一次都不会被调用
res2 := e.Run(ctx, sameTasks)
_ = res2.Results["a"].Reused // true：该结果来自缓存而非真正执行
```


## 调度与并发模型

- **图校验**：`Run` 先构建依赖图——任务定义校验（空 ID/空计算体直接拒绝）、重复任务 ID 去重（同一输入只计算一次）、重复依赖边去重、Kahn 拓扑排序（就绪集按 ID 字典序打破平局，拓扑序确定）。
- **非法图拒绝**：依赖成环返回 `ErrCycle`（`Kind=FailureCycle`），引用不存在节点返回 `ErrMissingDep`（`Kind=FailureMissingDep`），均不执行任何任务。
- **主从调度**：单一主调度 goroutine 维护就绪队列与结果表；`MaxConcurrency` 个 worker 从有界派发通道取任务执行。派发与结果回收在同一 `select` 中进行，通道容量为 `BufferSize`，天然形成**背压**——输入规模远大于缓冲时也不会有无界内存增长。
- **数据竞争规避**：任务派发时携带依赖值快照（`workItem`），worker 不读共享结果表；结果汇总由主 goroutine 单点完成。
- **确定性**：结果按任务 ID 汇总为 map，任务计算只依赖上游输出值，因此与并发度、调度顺序无关。
- **资源回收**：`Run` 返回前关闭派发通道、排空在途结果并 `WaitGroup` 等待全部 worker 退出，无 goroutine 泄漏。

## 跨运行复用与增量重算

引擎在实例内维护一份**成功产物缓存**（按任务 ID 索引，带互斥锁，支持多个 `Run` 并发共用同一引擎）。任务就绪后先做复用判定，命中则在主调度 goroutine 上同步消化、不派发 worker；未命中才真正执行。

**判定依据（三者全部一致才命中）**：

1. **任务定义指纹**：`Task.Version`（计算体语义版本）与生效的单任务超时（`Task.Timeout`/`Config.TaskTimeout`）；
2. **直接依赖集合**：依赖了哪些上游 ID（增删依赖边即变化）；
3. **每个直接依赖的产物指纹**：上游产物内容（对产出值做规范化哈希，map 键排序后摘要）。

> `Version` 是显式契约：当计算体的**语义**或其捕获的外部输入发生变化时，应修改/递增它。留空表示“该 ID 的定义自首次提交起不变”——因此把不同逻辑塞进同一个空 `Version` 的 ID 是误用。最终正确性以逐依赖的深度一致比对兜底；指纹用于快速排除与日志说明。

**增量传播**：一个任务未命中时只重算它自己；它的新产物指纹会让其下游在依赖比对时失效，从而沿 DAG 向下连锁重算，未受影响的上游/旁路继续命中。改一个叶子只重算 1 个任务，改一个源头则整棵子树重算。重算结果与 `DisableCache: true` 的全新引擎全量重算**逐值一致**，且不随并发度变化。

**可观测性**：

- `Result.Reused`（bool）标记单个结果是缓存命中还是真正执行；
- `RunResult.ReusedCount` / `ComputedCount` 汇总本次命中数与真实成功执行数；
- 日志逐节点打印 `cache hit` / `cache miss`、候选指纹 `fp`、产物摘要 `digest` 与命中/失效原因，运行结束打印 `reused/computed` 汇总。

### 什么情况下会失效重算

| 情况 | 行为 |
|---|---|
| 首次提交 / 该 ID 从未成功过 | miss，正常执行 |
| `Task.Version` 变化 | 该任务 miss，下游按产物指纹级联失效 |
| 增删/改动依赖（依赖集合变化） | 即使 Version 不变也 miss |
| 任一直接上游的产物内容变化 | 该任务及其下游级联重算 |
| 生效超时变化（`Timeout` 配置不同） | miss（保守策略） |
| 任务返回错误 / 超时 / panic | 结果为失败，**绝不写入缓存**，下次重算 |
| 运行被整体取消（含取消前已完成的任务） | 本次运行的缓存写入**整体回滚**，缓存恢复到运行前状态 |
| 条目被容量策略淘汰或被 `ClearCache` 清除 | miss，正常执行 |

## 缓存容量与清理

- 容量由 `Config.MaxCacheEntries` 限制（默认 `1024`），超限按 **LRU（最久未访问）**淘汰，缓存不会随运行次数无限增长；
- `Engine.CacheLen() int` 查看当前条目数；
- `Engine.ClearCache() int` 主动清空并返回被清除的条目数；
- `Config.DisableCache: true` 完全关闭复用：不读、不写、不淘汰，每次运行全量重算，行为与无缓存引擎一致（用于对照验证或需要绝对隔离的场景）。

## 非法输入的分类拒绝

图在**任何任务执行之前**整体校验，非法输入不触发任何计算，并把两类问题分开返回：

| 问题 | 错误 / Kind |
|---|---|
| 任务定义本身不合法：空 ID、计算体为 `nil` | `ErrInvalidTask` / `FailureInvalidTask` |
| 依赖指向不存在的节点 | `ErrMissingDep` / `FailureMissingDep` |
| 依赖成环 | `ErrCycle` / `FailureCycle` |

此外，任务计算体 **panic 会被捕获**并归类为 `FailureTaskError`（下游跳过、产物不缓存），单个坏任务不会让整个进程崩溃，引擎仍可继续服务后续运行。


## 失败与取消语义

| 场景 | 单任务 `Status/Kind` | 整体 `RunResult` |
|---|---|---|
| 任务返回错误 | `failed` / `FailureTaskError` | `Complete=false`，`Kind=FailureTaskError` |
| 任务超时（`Task.Timeout` 或 `Config.TaskTimeout`） | `failed` / `FailureTimeout`，`errors.Is(err, ErrTimeout)` | `Complete=false`，`Kind=FailureTimeout` |
| 上下文整体取消 | 在途任务 `failed` / `FailureCanceled`；未调度任务 `skipped` | `Complete=false`，`Kind=FailureCanceled`，`errors.Is(err, ErrCanceled)`，**绝不误报成功、不会卡死** |
| 上游失败/超时/取消 | 下游递归 `skipped`，原因中注明上游来源 | 部分结果被标记为不完整，不混入成功结论 |

整体失败原因按任务 ID 字典序取首个失败，保证判定本身也是确定的。

## 可配置项（`engine.Config`）

| 字段 | 含义 | 默认 |
|---|---|---|
| `MaxConcurrency` | 并行执行上限 | 4 |
| `BufferSize` | 派发/结果通道容量（背压） | 64 |
| `TaskTimeout` | 默认单任务超时（`Task.Timeout` 可覆盖，<=0 不超时） | 不超时 |
| `Logger` | 调度事件回调（任务图、派发/就绪/完成/跳过、缓存命中/失效/回滚） | 无 |
| `DisableCache` | `true` 时关闭跨运行复用，每次全量重算 | `false`（默认开启复用） |
| `MaxCacheEntries` | 跨运行缓存条目上限，超限 LRU 淘汰 | 1024 |

`Task.Version string` 为可选的定义版本：计算体语义或捕获的外部输入改变时修改它；留空视为定义不变。

## 默认行为与兼容范围

- **默认开启复用**：现有调用方无需改动即获得跨运行复用；`Result` 仅新增字段、`Run` 签名不变。
- 缓存是**引擎实例内、内存态**的：不持久化、不跨引擎实例共享；产出值被视为不可变，调用方不应在返回后修改它们。
- 复用不改变单轮运行的任何既有保证：一轮内重复任务仍去重、结果仍按 ID 汇总、与并发度和调度顺序无关。
- 想回到“每次全量重算”的旧行为：`engine.New(engine.Config{DisableCache: true})`。
- 指纹基于产出值的规范化 Go 表示；含**指针身份、通道、函数**等不可稳定序列化的值可能无法跨实例表达，这类场景请使用可比较的值类型或显式 `Version`。

## 本地验证

```bash
go build ./...
go vet ./...
go test -race -v ./engine        # 竞态检测 + 调度/复用判定日志
go test -race -count=10 ./engine # 重复运行验证结果一致（含取消回滚的时序）
```

测试覆盖：并发度变化与重复运行下的结果一致性（含 200 节点随机 DAG）、重复提交去重、成环/缺失依赖/**非法任务（空 ID/空计算体）**拒绝与分类、失败传播与跳过、超时归类、整体取消（不卡死、不误报成功、无 goroutine 泄漏）、并发上限与背压，以及**任务 panic 不崩进程**。

复用与增量相关覆盖：同图连跑命中（4 节点各执行 1 次）、改源头后整棵子树重算且结果与禁用缓存的全量重算逐值一致、改叶子只重算 1 个任务、增量结果在并发度 1/2/4/16 下一致、失败/超时/取消**产物不入缓存**（取消含写入回滚）、LRU 容量淘汰、`ClearCache` 清理、`DisableCache` 全量重算、依赖集合变化失效、同一引擎被多 goroutine 并发 `Run` 的竞态安全与容量受控。单测日志会打印任务图、调度顺序、每个节点的 `cache hit/miss`、指纹/摘要与判定依据（`go test -v` 可见）。
