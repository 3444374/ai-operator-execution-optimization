# AI 算子外部执行 Infra 设计

## 1. 目标

在现有 `PostgreSQL -> Daft -> Arrow -> Ray task/actor -> vLLM` 文本链路上，
建立数据库 AI 算子外部执行 infra，再依次补齐数据组织、提交控制、actor
运行时和文献候选技术。

本设计不为尚未开始的多模态或代价模型创建空接口。基础设施必须先被当前
`AI_COMPLETE` 文本实验真实使用；出现第二个具体模态或第二种成本模型后，再从
两个真实实现中提取公共抽象。

这里的 infra 不是新的模型服务框架，也不修改 Daft、Ray 或 vLLM 内部调度器。
它负责把数据库产生的完整 AI 算子输入组织成请求，以可控节奏送入模型服务，并
记录从数据到达到结果完成的可审计执行过程。

## 2. 当前事实与缺口

截至 2026-07-25：

- fixed rows、token-budget、length-align、prefix-aware organizer 已实现；
- queue-adaptive flush 已通过真实单 GPU 64/1024 门禁和 512 行重复筛选；
- AIMD、EWMA-AIMD、PID 已有策略 core 和 profiler 接线；
- UCB 只有策略 core，没有 profiler reward epoch；
- request-cost pool、least-queued 和 exact-key prefix affinity 已有逻辑路由；
- scheduler 仍由 driver 同步协调，actor 没有本地 pending queue；
- profiler 已有 run、submission、flush、control、resource trace；
- 缺少逐输入行的 request lifecycle trace、单 prompt E2E、SLO goodput 和随机化
  scenario runner；
- `target_output_tokens` 已进入 PostgreSQL/Arrow 数据，但 organizer 和 replay
  仍统一使用 `completion_max_tokens` 估算每行输出成本；
- Best-Fit bin-packing、联合搜索 runner、受控 prefix 实验、多模态和 profile
  驱动代价模型尚未实现；
- Daft 当前负责读取与组织，模型调用仍由独立 Ray task/actor 执行，没有
  `@daft.cls` 模型执行对照路径。

## 3. 方案比较

### 3.1 一次性实现完整策略套件

优点是接口可以统一规划。缺点是 request metrics、batching、控制器、actor 和
多模态会同时变化，无法判断性能差异来源，也会产生大量尚无真实调用方的抽象。

不采用。

### 3.2 只写一次性实验脚本

优点是短期运行快。缺点是 scenario 顺序、trace schema、随机种子和统计逻辑会
在脚本间重复，后续多模态和联合搜索难以复用。

不采用。

### 3.3 文本链路驱动的纵向基础设施

每一阶段都形成一个可运行、可审计、可做真实 GPU 实验的纵向切片。公共结构只在
当前文本链路需要时创建；多模态和代价估计后续复用已经稳定的生命周期、scenario
和结果 schema。

采用此方案。

## 4. Infra 分层

```text
Execution Input
  PostgreSQL / Daft DataFrame / Arrow payload
        |
Data Organization
  request cost / grouping / packing / prefix organization
        |
Runtime Control
  arrival replay / flush / admission / actor pool / endpoint routing
        |
Ray Execution
  task / actor / local pending queue / fan-in
        |
Model Service
  vLLM compatible endpoint
        |
Observability and Experiment Control
  request/submission/run trace / resource trace / seeded scenarios
```

各层职责如下：

- **Execution Input**：读取数据库 AI 算子输入并保持完整行语义；
- **Data Organization**：决定哪些完整请求组成一个上游 batch；
- **Runtime Control**：决定 batch 何时提交、允许多少并发、提交到哪里；
- **Ray Execution**：执行 payload 传递、bounded in-flight 和结果收集；
- **Model Service**：作为不可修改的部署平台执行 continuous batching；
- **Observability and Experiment Control**：统一生命周期、版本、随机化和统计
  产物，使所有策略可以公平复验。

算法不是独立脚本，而是这些层的可替换策略。infra 必须保证策略切换不改变输入、
backend、结果 schema 和 exactly-once 语义。

## 5. 可观测性与实验控制

### 5.1 Request lifecycle trace

新增一行输入对应一行 trace 的 `requests.csv`。每行至少记录：

```text
schema_version
experiment_id / phase / repeat_index / scenario_id / random_seed
server_version / pgvector_version
job_id / request_id / submission_id / doc_id
pool_id / endpoint_id / gpu_id
prompt_tokens / estimated_output_tokens / client_estimated_output_tokens
actual_output_tokens / output_token_source / total_tokens
prefix_key / status / error_type
arrival_epoch_s
flush_epoch_s
submit_epoch_s
service_start_epoch_s
completion_epoch_s
buffer_s / submit_to_service_s / service_s / e2e_s
latency_granularity
slo_target_s / slo_met
```

`request_id` 表示完整输入行/model sequence，`submission_id` 表示承载一个或多个
输入行的 Ray/vLLM 提交。不得把两者混用。

当前 compatible completion endpoint 只返回整个多 prompt submission 的响应
时间。属于同一 submission 的行可以有不同 arrival 时间，但共享 flush、submit、
service start 和 completion 时间。此时：

```text
latency_granularity = submission
e2e_s = completion_epoch_s - arrival_epoch_s
```

该值是客户端可观测的逐行完成延迟，但不是 vLLM 内部单 sequence 精确完成时刻。
报告必须保留这一限制。以后若真实 backend 暴露 per-sequence timing，再新增
`latency_granularity=request`，不改变旧字段语义。

当前 endpoint 的 usage 也是 submission aggregate，不能拆成逐请求实际输出
token。`actual_output_tokens` 因此允许为空；客户端对输出文本的估算单独写入
`client_estimated_output_tokens`，并用 `output_token_source` 标记
`submission_aggregate_unavailable` 或未来真实的 `endpoint_request`。不得把
空格切词或客户端 tokenizer 估算冒充 endpoint 实际 usage。

### 5.2 时间基准

arrival replay 使用 monotonic clock 决定等待，但持久化 trace 使用 epoch 时间：

```text
arrival_epoch_s = replay_start_epoch_s + scaled_arrival_offset_s
```

同一次 run 同时保存 monotonic/epoch 起点用于审计。不得直接把源数据中的相对
`arrival_time_s` 与服务端 epoch 时间相减。

flush 时间取 pending batch 关闭时刻；submit 时间取 Ray handle 创建完成时刻；
service start/end 来自 model backend；completion 为结果被客户端收到的时刻。

### 5.3 Scenario runner

新增项目内可复用 runner，职责仅包括：

- 读取显式 scenario 列表；
- 用固定 seed 生成逐 repeat 策略顺序；
- 先执行每种策略 warm-up；
- formal repeat 内交错执行 scenario；
- 每个 scenario 启动前确认 vLLM health=200、running=waiting=0；
- 将完整 CLI、commit、seed、顺序、硬件和版本写入 manifest；
- 子进程非零、CSV 行缺失、vLLM 成功增量不符时停止后续规模；
- 不负责修改 vLLM 或 Ray 内部策略。

runner 不内置论文结论，也不自动丢弃失败数据。失败尝试进入 incident log，但不进入
formal summary。

### 5.4 结果 schema

保留并版本化：

```text
runs.csv
requests.csv
submissions.csv
flush_trace.csv
control_trace.csv
resource_trace.csv
manifest.json
```

派生 summary 只能从上述原始文件生成。正式画图不直接解析 stdout。

## 6. 数据组织层

### 6.1 输出成本模式

文本 organizer 明确支持三种实验口径：

1. `prompt_only`：只计算 prompt tokens；
2. `fixed_output_cap`：prompt + 本次统一 `completion_max_tokens`；
3. `trace_target_output`：prompt + 数据行的 `target_output_tokens`。

`trace_target_output` 是离线 oracle/trace-assisted 上界，不得写成在线可获得的完美
预测。缺失或负值必须显式失败，不能静默回退到 0。

模型调用的 `max_tokens` 与 organizer 的成本估计是两个不同概念。变长输出实验可
使用较高 generation cap 允许 EOS 自然结束，同时 organizer 分别使用 fixed cap
或 trace target 做消融。

### 6.2 Best-Fit Decreasing

新增确定性 Best-Fit Decreasing token packing：

1. 按 estimated total tokens 降序；
2. 将每行放入剩余容量最小但仍能容纳它的 batch；
3. 相同成本按源 `doc_id`/source order 稳定打破平局；
4. 超过 budget 的完整行独占一个 batch；
5. 不拆分 prompt，不跨 run 保存未完成 batch。

正式对照：

```text
fixed rows
sequential token budget
length-align + token budget
Best-Fit Decreasing token budget
prefix-aware + token budget
```

主指标为 observed tokens/s、workload E2E、request E2E P95/P99、batch token
spread、submission count 和 service P99。

### 6.3 Prefix 技术

先做受控 prefix ratio `{0, 30, 70, 100}%`，再决定是否实现最长前缀匹配。
现有 exact-key rendezvous router 保留为 baseline。

新增机制必须记录 vLLM 可获得的 prefix cache hit、cached tokens 或等价
Prometheus 指标。若当前 vLLM 版本不暴露可靠指标，只能报告 prefill token/time
proxy，不能声称 cache hit 提升。

## 7. 运行时控制与搜索

### 7.1 控制器实验

AIMD、EWMA-AIMD 和 PID 使用同一 request/run/control schema。UCB 只有在以下
数据完成后接入：

- 固定 epoch 完成请求数；
- epoch tokens/s；
- request SLO violation；
- service/request tail penalty。

UCB reward 的首个正式定义为：

```text
normalized_tokens_per_s
- slo_penalty_weight * slo_violation_ratio
- tail_penalty_weight * normalized_request_p99
```

reward 权重必须在 tuning workload 固定，held-out 不重调。UCB 不替代静态 grid
search baseline。

### 7.2 联合搜索

新增显式 scenario matrix runner：

```text
batching policy
× token budget
× flush policy/window
× admission policy/K_max
```

先分别搜索数据组织和提交控制的最优配置，再比较：

```text
independent_best_batching + independent_best_submission
vs
joint_grid_best
```

选择使用 512 行 tuning，1024 行 confirmation，2048 行 held-out。任何 held-out
配置不得重新调参。

### 7.3 SLO-aware batching

只有 request trace 完成后才加入 Clipper/Scorpio/NeuStream 启发的 delayed/
credit/deadline 策略。第一版只支持显式 workload SLO 列；没有真实 SLO 标签时，
该策略不进入正式结论。

## 8. Ray Actor 与路由运行时

当前同步 scheduler 和逻辑 pool 继续作为强 baseline。只有在组织与提交控制实验
稳定后，才增加 actor-local queue：

- short、long、prefix pool 各自维护 pending queue；
- 本地执行 flush 和 admission；
- driver 只做初始 pool/endpoint 路由和最终 fan-in；
- weighted service + aging 防止长请求饥饿；
- actor failure 未返回结果的请求显式失败，不做隐式重复提交；
- retry 若以后加入，必须使用 request idempotency key。

单 GPU只验证队列隔离、exactly-once、tail/fairness 和调度开销。多 endpoint/
GPU 性能结论等待真实硬件，不用同 GPU 多进程代替。

## 9. Daft、多模态与代价估计的后续接入

### 9.1 不提前创建空接口

当前不创建 `FrameCostEstimator`、`MultimodalOrganizer` 或空的多 GPU manager。
文本阶段先稳定 request lifecycle、scenario runner、结果 schema 和 packing
行为。

### 9.2 多模态接入点

当图像 workload 启动时：

- 在现有 Daft DataFrame 中把 `prompt` 列替换为 `image` 列；
- 为真实图像数据实现 frame/patch/visual-token 成本；
- 复用 batching、flush、admission、routing 和 trace；
- 若文本与图像成本函数出现稳定共同结构，再提取通用 cost protocol；
- 不复制一套 multimodal scheduler。

### 9.3 代价估计

第一版模型只使用已经采集且执行前可获得的特征：

```text
prompt tokens
estimated output tokens
batch rows
batch token sum/spread
prefix group ratio
```

先以简单线性/树模型预测 batch service time，在独立 held-out 上报告 MAPE、P95
绝对误差和系统性残差。MAPE 未达到 20% 或残差随 workload 明显漂移时，不把模型
接入 routing。

`target_output_tokens` oracle 结果和在线可用预测结果必须分开报告。

### 9.4 `@daft.cls`

只有当需要比较“Daft GPU UDF 调用模型”与“Daft 组织 + Ray actor 调用模型”时，
实现一个真实 `@daft.cls` 对照路径。它复用相同 source、workload、backend 参数和
result schema，不改变策略 core。该路径用于框架对照，不预设一定更快。

## 10. 文献来源与边界

| 技术 | 主要来源 | 本项目采用部分 | 不采用部分 |
|---|---|---|---|
| Token packing | Nexus、MultiBin/BucketServe | BFD 和 batch cost balance | GPU 集群放置 |
| Token/phase awareness | Sarathi-Serve、DistServe | prompt/output cost 与长短隔离 | 修改 vLLM、prefill/decode 分离部署 |
| Prefix organization | vLLM APC、SGLang、Parrot、Ray PrefixCacheAffinityRouter | 上游 grouping、LPM 候选、cache 指标 | attention kernel 修改 |
| Adaptive batching | Clipper | delayed batching、SLO goodput | 通用 DNN 的固定 batch 假设 |
| Admission | CONCUR | 简单 AIMD/deadband 对照 | agent middle-phase thrashing 假设 |
| SLO scheduling | Scorpio、NeuStream | deadline/credit 候选 | 无真实 SLO 时的人工收益结论 |
| Cost-aware decision | FlexPushdownDB、数据库 AI operator 工作 | 可解释成本特征、held-out 误差 | 未验证的 learned routing |
| Actor runtime | Ray | stateful local queue、async execution | 修改 Ray 内部调度器 |

Chunked prefill、PagedAttention、prefill/decode disaggregation 和 attention kernel
属于 vLLM/服务平台内部机制。本项目可以把它们作为部署 baseline 或交互变量，但
不实现为上游调度贡献。

## 11. 实施顺序

本总体设计拆为四个独立实施子项目：

1. **Request lifecycle 与 scenario runner**  
   产出 requests trace、单 prompt E2E/SLO、随机化运行和 schema 审计。
2. **Output-aware cost 与 bin-packing**  
   产出三种输出成本口径、BFD、变长输出消融和 512/1024/2048 分级实验。
3. **控制器、SLO 与联合搜索**  
   产出 AIMD/EWMA/PID/UCB 对比、独立拼接 vs joint grid。
4. **Actor-local runtime 与扩展验证**  
   产出 async pool、prefix/long-short 路由；多模态、代价模型和多 GPU 按真实
   数据/硬件触发。

每个子项目单独设计、TDD 计划、验证和提交。不得在同一提交中同时改变生命周期
计时、batch membership 和 admission control law。

## 12. 成功条件

AI 算子执行 infra 完成的最低条件：

- 每个输入文档在 requests trace 中 exactly once；
- request/submission/run 三层 ID 可无歧义关联；
- 所有延迟非负且阶段和不超过允许的计时误差；
- seeded runner 在相同 seed 下产生相同 scenario 顺序；
- formal CSV 均含真实 server/pgvector 版本；
- 静态路径不读取 adaptive metrics；
- 单元测试、真实 Daft→Arrow→Ray task/actor contract 和 compileall 全部通过；
- fake 仅用于单元/调试，正式实验只使用真实 PostgreSQL、Daft、Ray 和 vLLM。

策略晋级条件：

- tuning、confirmation、held-out 数据分离；
- 新策略相对强 baseline 给出 tokens/s 或 SLO goodput 收益；
- request E2E P99 不越过预先定义的 guardrail；
- 失败策略保留为消融或停止，不因已经实现而强行进入最终方案。
