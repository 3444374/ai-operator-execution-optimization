# AI 算子执行 Infra 当前状态

日期：2026-07-26

本文说明当前 Daft + Ray 上游执行基础设施已经完成什么、实际执行流程、研究证据
边界，以及下一步还需要实现和验证的内容。研究方向仍是数据库 AI 算子外部执行
链路，不修改 vLLM 内部。

## 1. 当前端到端流程

```text
PostgreSQL
  -> DaftPostgresSource
  -> DaftOrganizer / shared Arrow policy core
  -> BatchRequest + PayloadEnvelope
  -> optional arrival replay + flush policy
  -> admission controller (K_max)
  -> optional request-cost pool router
  -> endpoint router
  -> Ray task / actor adapter
  -> vLLM-compatible endpoint
  -> request/submission/control/resource traces
  -> optional PostgreSQL JSON/pgvector sink
```

边界是明确的：

- Daft 负责数据读取、分区和 dataframe 执行入口；
- Arrow table 是当前 payload boundary；
- 策略只读取 `BatchRequest` 元数据，不依赖 Daft、Arrow、Ray 或 HTTP；
- Ray task/actor 负责并发提交与收集；
- vLLM 负责模型内部 continuous batching，本项目不修改它；
- PostgreSQL/pgvector 写回是工程 baseline，不作为独立研究贡献。

## 2. Batch 数据组织部分

### 已完成

- Fixed rows。
- Sequential token-budget（当前默认）。
- Length-align × fixed rows/token-budget。
- Prefix-aware × fixed rows/token-budget。
- Classic best-fit-decreasing。
- BFD-inspired row-cap-first placement。
- Prompt-only、fixed-output-cap、trace-target-output 三种执行前代价模式及来源标签。
- Arrow 与 Daft 共用同一套纯策略函数；全局 packing 不在 Daft 分支复制实现。
- 统一记录 batch row/token 分布、packing utilization、oversized rows、
  submission count 与 per-request lifecycle。

### 本轮新增与结果

- 增加 row-cap-first placement：选箱时先减少剩余行槽，再考虑 token residual。
- 64 行真实门禁、512 行筛选/重复和 1024 行 held-out 均跑通。
- 512 行出现小幅正向信号；1024 行虽然 tokens/s `+0.82%`，但 10 秒 SLO
  violation 从 `50.39%` 升至 `88.67%`。
- 设计决定：sequential token-budget 继续默认；classic BFD 不采用；
  row-cap-first 只保留为可配置消融点。

### 当前流程

离线吞吐实验先由 organizer 根据完整输入决定 batch membership，再生成
`BatchRequest`。在线 arrival replay 为了保持到达因果关系，只允许 fixed rows
或 sequential token-budget；length/prefix/BFD 等会重排未来请求的策略被显式
拒绝，不会混入 flush 实验。

## 3. 调度与提交控制部分

### 已完成

- Static bounded inflight（`K_max`）。
- Legacy queue-adaptive baseline。
- Typed AIMD、EWMA-AIMD、PID controller。
- 非阻塞后台 vLLM metrics sampler；提交决策路径不再做网络 I/O。
- stale/missing metrics 保守 hold；control trace 记录 sample age、窗口、动作、
  原因、running/waiting/KV。
- Immediate、fixed-timeout、queue-adaptive flush，带 hard max wait。
- Arrival replay 使用 monotonic clock，保持完整行请求与到达间隔。
- Scheduler 组合顺序固定：admission → pool routing → endpoint routing →
  Ray submit → bounded collection。

### 当前流程

1. Flush 决定未满 batch 何时关闭；
2. Admission 决定已关闭 batch 是否允许提交；
3. Pool router 按请求代价/前缀选择逻辑池；
4. Endpoint router 在池内 round-robin、least-queued 或 prefix-affinity；
5. Ray adapter 提交 task/actor，并把完成结果恢复为原 submission 顺序；
6. lifecycle 层生成 exactly-once request/submission trace。

### 证据边界

- 静态 `K_max=8` 的必要性已有 shared-vLLM 干扰证据。
- Queue-adaptive flush 已有单 GPU 正向候选，但尚缺随机化、变长输出和 2048
  held-out，不能写成最终优于 static。
- AIMD/EWMA/PID 已完成代码与单元/集成契约，尚没有充分真实 GPU 对照。
- UCB 多臂老虎机已有有限 action set 与 SLO reward 的纯控制器代码，但尚未
  接入 profiler。原因是缺少稳定的 epoch-level reward/归因边界；现在接入会把
  跨 epoch 的请求完成错误归因给当前 arm。

## 4. Actor pool、endpoint 与 GPU 扩展

### 已完成的框架

- `EndpointSnapshot` 显式包含 endpoint、pool、GPU、健康状态和队列指标。
- Request-cost pool 可把 short/long/prefix 请求路由到不同逻辑池。
- 池缺失或 endpoint 不健康时有确定性 fallback。
- Prefix affinity 使用 rendezvous hashing；无 prefix 时回退 least-queued。
- CLI 支持多个 endpoint URL、pool ID 和 GPU ID。
- Ray task 与 actor 走同一 typed scheduler。

### 尚未完成的验证

当前正式结果均在一张 GPU 上。多 endpoint/多 GPU 的接口和策略代码存在，
但异构显存容量、跨 GPU 负载、故障迁移和真实吞吐公平性尚未实测，因此不能声称
多 GPU 调度已经完成。

## 5. 观测与实验运行基础设施

### 已完成

- CSV 同时记录 PostgreSQL/pgvector 版本、tokens/s、request P50/P95/P99、
  SLO、GPU utilization/memory/power/energy、energy/1k tokens、vLLM
  running/waiting/KV、FLOP delta 与 MFU。
- request、submission、flush、control、resource trace 分文件保存。
- seeded/interleaved scenario schedule、每次运行前 idle gate、命令脱敏和
  atomic manifest。
- 本轮增加安全续跑：已完成项不重复，失败项可重试，历史 incident 标记
  recovered。
- 本轮增加失败场景剪枝：不伪造 CSV，manifest 显式记录 skipped run。
- 本轮增加 `service_metadata`：vLLM 版本、prefix cache 和 MFU 开关进入
  manifest，并参与 resume 一致性校验。

### 本轮发现并修正的实验问题

仅检查 MFU metric 名称不足以证明计数有效。vLLM 0.25.1 必须使用
`--enable-mfu-metrics`，并通过真实请求验证正 FLOP delta。

此外，prefix cache 会让重复 prompt 实验出现强顺序依赖。本轮正式
512/1024 结论统一使用 `--no-enable-prefix-caching`；此前启用缓存的数据只作
事故审计，不进入性能结论。

## 6. 研究内容完成度

| 部分 | 代码完成度 | 真实证据 | 当前判断 |
|---|---|---|---|
| 数据读取与 Daft/Ray 主链路 | 高 | 64/512/1024 真实链路 | 已完成基础设施 |
| Fixed/token-budget batching | 高 | 多轮真实实验 | 机制成立，sequential 默认 |
| Length/prefix grouping | 中高 | 初步 ablation | 受控 workload 未完成 |
| BFD/row-cap-first | 高 | 512 + 1024 | 负向边界明确，不默认启用 |
| Static K_max | 高 | shared-vLLM | 必要性成立 |
| Queue-adaptive flush | 高 | 单 GPU 候选 | 最终复验未完成 |
| AIMD/EWMA/PID | 高（代码） | 缺正式 GPU 矩阵 | 不能声称有效 |
| UCB bandit | 中（纯控制器） | 无端到端实验 | 尚未接入执行路径 |
| Actor pool / endpoint routing | 高（接口/契约） | 单 GPU 为主 | 多 GPU 验证未完成 |
| 联合 batching × submission 搜索 | 低（实验） | 无完整矩阵 | 核心缺口 |
| 多模态复用 | 低 | 未启动 | 文本主线完成后进行 |
| 算子代价估计 | 低 | 已有 profile 数据 | 二次分析未完成 |

## 7. 后续设计与实施顺序

### 第一优先：完成提交控制结论

1. 使用自然 EOS 的变长输出，保留固定 16-token 输出作对照；
2. 随机化 immediate/fixed/adaptive 的场景顺序；
3. 在 512 行运行至少 5 次正式重复；
4. gate 同时要求 exactly-once、request P99、SLO goodput、tokens/s、
   control/resource trace；
5. 候选通过后原样扩展到 2048 行。

若三轮真实改进仍不能接近或超过 static `K_max=8`，研究内容收敛为：
“静态 admission guardrail 的必要性 + adaptive 的失败边界”，不继续堆控制器。

### 第二优先：两项策略联合实验

Sequential token-budget 作为 batching baseline，不再把完整 BFD 当默认候选。
先分别搜索：

- token budget `{4096,6144,8192}` × row cap `{16,32,64}`；
- static K_max `{4,8,16}` 与通过门禁的 flush policy。

然后比较“独立最优拼接”与局部联合搜索。目标函数必须是 SLO-constrained：
先满足 correctness/P99/SLO goodput guardrail，再比较 tokens/s、energy 和 MFU。

### 第三优先：受控 prefix 与多臂老虎机

- 构造 prefix ratio `0/30/70/100%` 的 workload，验证 prefix-aware 是否只在
  足够复用比例下有效。
- UCB 只在能按固定 epoch 封闭请求完成和 reward 归因后接入。每个 arm 是一个
  K_max，不同时搜索 batch 与 K_max；设置 static K=8 safety fallback，reward
  使用 SLO-constrained tokens/s。

### 后续：多 GPU、多模态与代价估计

- 多 GPU：先做同构双 endpoint，再做异构池；验证健康回退、队列均衡和公平性。
- 多模态：增加 image source/cost adapter，把 token cost 替换为 frame/pixel
  cost，复用 organizer、scheduler、routing 和 tracing。
- 代价估计：直接使用现有 profile CSV，按 prompt/output/batch/queue 特征拟合，
  使用 held-out workload 报告 MAPE/R²；不新增独立系统层。

## 8. 当前可安全采用的默认值

- 数据引擎：Daft；
- 执行：Ray task/actor 按实验目的选择，不把其差异包装成贡献；
- batching：sequential token-budget；
- admission：static `K_max=8`；
- flush：离线实验 immediate；在线候选必须单独声明 arrival replay；
- routing：单 endpoint 使用 round-robin；多 endpoint 实验前不启用复杂池路由；
- vLLM 重复 prompt 对比：明确记录 prefix cache；本轮公平比较使用 disabled；
- 任何策略晋级必须同时通过 SLO goodput，而不是只看平均吞吐或 MFU。
