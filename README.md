# parallel-dataflow-engine-go

本地并行数据流执行引擎：对任意依赖拓扑（DAG）的计算任务进行并发调度，
保证同一输入在多次运行、不同并发度下得到完全一致的结果。

## 调度与并发模型

- 任务以 `Graph` 组织，每个 `Task` 声明 `ID`、`Deps`（依赖节点 ID）与
  `Run(ctx, inputs)` 计算函数；`inputs` 为其直接依赖的输出（按依赖 ID 索引）。
- 执行前 `Validate` 校验图合法性：**依赖不存在的节点**（`ErrMissingDep`）与
  **依赖成环**（`ErrCycle`）会被明确拒绝，返回 `nil` 输出与可 `errors.Is`
  区分的错误，不执行任何任务。
- 调度器为单控制循环 + 固定 worker 池：
  - 活跃并行数受 `MaxConcurrency` 上限约束（worker 数即上限）；
  - 就绪任务进入容量为 `QueueSize` 的有界派发队列，队列满时调度器阻塞，
    形成**背压**，输入规模远大于缓冲时不会无界增长；
  - 同层就绪任务按 ID 字典序派发，结果按节点 ID 汇总，因此输出与
    并发度、调度顺序无关；
  - 返回前 `WaitGroup` 回收全部 worker，运行结束后不残留执行单元。
- 开启 `Dedup` 后，引擎按图指纹（节点 ID + 依赖关系 + 输入载荷的 SHA-256）
  缓存完整结果；同一输入被重复提交时直接命中缓存，只产生一次实际计算。

## 失败与取消语义

每个节点的终态为 `success / failed / timeout / skipped / canceled` 之一：

| 场景 | 节点状态 | 整体错误（可 `errors.Is` 区分） |
|---|---|---|
| 节点 `Run` 返回错误 | `failed` | `ErrTaskFailed` |
| 节点超过 `Task.Timeout` | `timeout` | `ErrTaskTimeout` |
| 上游失败/超时的下游 | `skipped`（`ErrUpstreamFailed`） | 随上游原因 |
| `ctx` 整体取消 | 未运行节点 `canceled` | `ErrCanceled` |

- 任一节点失败或超时后，其全部下游被跳过，不再派发。
- 存在非成功节点时 `Output.Complete=false`，部分结果保留在 `Results` 中
  但被明确标记为不完整；`Output.Get` 仅在整体完整时返回节点值，
  部分结果不会混入成功结果。
- 整体取消时调度器停止派发新任务、等待在途任务收尾后返回 `ErrCanceled`，
  不会卡死，也不会错误地报成成功（`Run` 实现需响应 `ctx`）。

## 可配置项（`engine.Options`）

| 字段 | 含义 | 默认值（<=0 时） |
|---|---|---|
| `MaxConcurrency` | 活跃并行执行上限 | 1 |
| `QueueSize` | 就绪队列缓冲（背压阈值） | 1 |
| `Dedup` | 相同指纹的重复提交命中缓存 | false |

`Task.Timeout` 为单节点超时（>0 时生效）。

## 本地验证

```sh
go build ./...
go test -race -v ./...
```

测试覆盖：并发度 1–8 与重复运行下的结果一致性、有界并发与背压
（200 节点 / 上限 3 / 缓冲 2）、成环与缺依赖拒绝、失败跳过下游、
单节点超时、整体取消不卡死不误报、重复提交去重、goroutine 泄漏检查、
故障注入下多次运行归类一致。单测日志会打印任务图（拓扑序）、
实际调度顺序与判定依据。
