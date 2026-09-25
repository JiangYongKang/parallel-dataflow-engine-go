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
- **非法图拒绝**：依赖成环返回 `ErrCycle`（`Kind=FailureCycle`），引用不存在节点返回 `ErrMissingDep`（`Kind=FailureMissingDep`），均不执行任何任务。
- **主从调度**：单一主调度 goroutine 维护就绪队列与结果表；`MaxConcurrency` 个 worker 从有界派发通道取任务执行。派发与结果回收在同一 `select` 中进行，通道容量为 `BufferSize`，天然形成**背压**——输入规模远大于缓冲时也不会有无界内存增长。
- **数据竞争规避**：任务派发时携带依赖值快照（`workItem`），worker 不读共享结果表；结果汇总由主 goroutine 单点完成。
- **确定性**：结果按任务 ID 汇总为 map，任务计算只依赖上游输出值，因此与并发度、调度顺序无关。
- **资源回收**：`Run` 返回前关闭派发通道、排空在途结果并 `WaitGroup` 等待全部 worker 退出，无 goroutine 泄漏。

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
| `Logger` | 调度事件回调（任务图、派发/就绪/完成/跳过） | 无 |

## 本地验证

```bash
go build ./...
go vet ./...
go test -race -v ./engine        # 竞态检测 + 调度日志
go test -race -count=10 ./engine # 重复运行验证结果一致
```

测试覆盖：并发度变化与重复运行下的结果一致性（含 200 节点随机 DAG）、重复提交去重、成环/缺失依赖拒绝、失败传播与跳过、超时归类、整体取消（不卡死、不误报成功、无 goroutine 泄漏）、并发上限与背压。单测日志会打印任务图、调度顺序与判定依据（`go test -v` 可见）。
