# parallel-dataflow-engine-go

本地并行数据流执行引擎：按依赖拓扑调度任意 DAG 计算任务，保证**同一输入在多次运行、不同并发度下得到完全一致的结果**。

## 快速开始

```go
e := engine.New(engine.Config{MaxConcurrency: 8, BufferSize: 64})
res := e.Run(ctx, []engine.Task{
    {ID: "a", Run: func(ctx context.Context, deps map[string]any) (any, error) { return 1, nil }},
    {ID: "b", Deps: []string{"a"}, Run: /* 读取 deps["a"] */ ...},
})
if !res.Complete { /* res.Err / res.Kind 给出可区分原因 */ }
```

## 调度与并发模型

- **图校验**：`Run` 先构建依赖图——重复任务 ID 去重（同一输入只计算一次）、重复依赖边去重、Kahn 拓扑排序（就绪集按 ID 字典序打破平局，拓扑序确定）。
- **非法图拒绝**：任务定义不合法（空 ID / 无计算体）返回 `ErrInvalidTask`（`Kind=FailureInvalidTask`），依赖成环返回 `ErrCycle`（`Kind=FailureCycle`），引用不存在节点返回 `ErrMissingDep`（`Kind=FailureMissingDep`）；三者可区分，且均在开跑前整体拒绝、不执行任何任务。
- **主从调度**：单一主调度 goroutine 维护就绪队列与结果表；`MaxConcurrency` 个 worker 从有界派发通道取任务执行。派发与结果回收在同一 `select` 中进行，通道容量为 `BufferSize`，天然形成**背压**——输入规模远大于缓冲时也不会有无界内存增长。
- **数据竞争规避**：任务派发时携带依赖值快照（`workItem`），worker 不读共享结果表；结果汇总由主 goroutine 单点完成。
- **确定性**：结果按任务 ID 汇总为 map，任务计算只依赖上游输出值，因此与并发度、调度顺序无关。
- **资源回收**：`Run` 返回前关闭派发通道、排空在途结果并 `WaitGroup` 等待全部 worker 退出，无 goroutine 泄漏。

## 跨运行复用与增量重算

同一 `Engine` 实例长期服役、反复提交相同或相近的图时，设置 `Config.CacheSize > 0`
即可开启跨运行结果复用：**定义未变、且上游产物来源与上次一致的任务不再真正执行，
直接复用上次成功的结果**（`Result.Reused=true`），只有受影响的子图被增量重算。

### 复用判定规则

- 每个任务计算一个**级联指纹** = 自身定义哈希 + 全部直接依赖的级联指纹（按拓扑序推导）。
- 定义哈希取自：`Task.CacheKey`（非空时，推荐显式指定版本号）；否则取 `Run` 的
  函数代码指针；再混入 `Task.Timeout`。
- 复用条件（同时满足）：
  1. 缓存中存在该任务条目且级联指纹与本次一致；
  2. 全部直接依赖本次也被复用（复用沿依赖链传递，断链即重算——依赖若要重算，
     其产物无法保证与上次一致，下游必须一并重算）。
- 同一张图连跑多次：第二次起全部命中，实际执行次数与只跑一次相同。

### 什么情况下会失效重算

| 触发 | 日志中的判定依据 | 重算范围 |
|---|---|---|
| 首次运行 / 缓存被清空 / 条目被淘汰 | `reason=no_entry` | 该任务及下游 |
| 任务定义变化（`CacheKey` 或 `Run` 函数或 `Timeout` 变了） | `reason=definition_changed` | 该任务及下游 |
| 上游定义变化传导导致指纹改变 | `reason=upstream_changed` | 该任务及下游 |
| 上游因任何原因需要重算（如缓存容量不足被淘汰） | `reason=upstream_recomputed` | 该任务及下游 |

增量重算的最终结果与完全不复用、每次全量重算**完全一致**，且与并发度无关
（有测试覆盖：增量结果 vs 全新实例全量结果在多种并发度下逐一比对）。

### 容量、清理与安全

- **容量**：`CacheSize` 为条目上限，LRU 淘汰最久未使用者，缓存不会随运行次数无限增长。
- **主动清理**：`Engine.ClearCache()` 清空全部条目；`Engine.CacheStats()` 返回
  条目数与累计命中/未命中，便于观测。
- **只缓存成功产物**：失败、超时、取消等未正常收尾的结果绝不写入缓存，
  后续运行会真正重算这些任务。
- **并发安全**：同一实例可被多个 goroutine 并发 `Run`，缓存读写经互斥锁串行化，
  缓存不保存任何运行中状态，并发运行互不污染（`-race` 测试覆盖）。
- **非法任务前置校验**：空 `ID` 或 `Run` 为 nil 属于"任务定义不合法"，
  开跑前整体拒绝并返回 `ErrInvalidTask`（`Kind=FailureInvalidTask`），
  与"依赖指向不存在节点"的 `ErrMissingDep`（`Kind=FailureMissingDep`）是
  两个可区分的类别；拒绝后进程与引擎实例不受影响，可继续提交合法图。

### 默认行为与兼容范围

- **默认关闭**：`CacheSize <= 0`（含零值）时行为与此前版本完全一致，每次全量执行。
- **纯函数假设**：复用语义假定任务输出只取决于定义与上游产物。默认指纹基于
  函数代码指针，**闭包捕获的变量不参与指纹**——若闭包捕获了会变化的状态，
  必须显式设置 `Task.CacheKey` 并在内容变化时升级它，否则会复用到旧结果
  （此为约定行为，有测试固化）。
- 缓存仅存于进程内存，`Engine` 实例销毁即失效，不做持久化。

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
| `Logger` | 调度事件回调（任务图、派发/就绪/完成/跳过、缓存命中/失效判定） | 无 |
| `CacheSize` | 跨运行结果缓存容量（LRU），<=0 关闭缓存 | 0（关闭） |

`Task.CacheKey`：可选的任务定义版本标识，非空时参与复用指纹（替代函数指针），
内容变化时应升级。`Result.Reused`：标记该结果是否来自缓存复用而非本次执行。

## 本地验证

```bash
go build ./...
go vet ./...
go test -race -v ./engine        # 竞态检测 + 调度/缓存判定日志
go test -race -count=10 ./engine # 重复运行验证结果一致
```

测试覆盖：并发度变化与重复运行下的结果一致性（含 200 节点随机 DAG）、重复提交去重、
成环/缺失依赖/非法任务三类前置拒绝且互相可区分、失败传播与跳过、超时归类、
整体取消（不卡死、不误报成功、无 goroutine 泄漏）、并发上限与背压；
跨运行复用（同图连跑仅执行一次）、增量重算（改动局部重算且与全量结果一致）、
缓存容量上限与 LRU 淘汰、主动清空、失败/超时/取消不入缓存、
并发运行共享缓存（`-race`）、`CacheKey` 指纹语义。
单测日志会打印任务图、调度顺序与每次缓存命中/失效的判定依据（`go test -v` 可见，
如 `cache hit task=a ...`、`cache miss task=b reason=definition_changed`）。
