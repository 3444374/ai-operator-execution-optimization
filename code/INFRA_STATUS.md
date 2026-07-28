# AI 算子执行 Infra 当前状态

日期：2026-07-29

本文说明当前 Daft + Ray 上游执行基础设施已经完成什么、实际执行流程、研究证据
边界，以及下一步还需要实现和验证的内容。研究方向仍是数据库 AI 算子外部执行
链路，不修改 vLLM 内部。

全部机制、代码测试和 20 个正式结果目录的逐项对应见
`experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md`。该台账明确区分代码完成、
真实链路门禁和性能证据。

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
- Arrival replay 可选择 `submission_granularity=request`：packing/flush 仍记录
  组织边界，关批后每个完整行请求独立提交，任一完成即释放一个 admission credit。
- Scheduler 组合顺序固定：admission → pool routing → endpoint routing →
  Ray submit → bounded collection。
- 静态或 service-quantum token budget；动态策略只在静态容量曲线标定的离散
  候选中逐步移动，并在 metrics 缺失时保持当前安全值。
- per-endpoint active-work credit：按 prompt + 预测 output token 记账，
  与 request-count K 独立开关。
- least-work endpoint routing：优先预测 drain/active work 较小的 endpoint，
  与 least-queued 保持独立消融入口。
- 多 job shared credit：Ray named actor 统一持有 endpoint request/work
  capacity，使用带权 deficit round robin 和空闲容量借用；联合
  `(job_id, request_id)` 防止不同作业的 batch ID 冲突。

### 当前流程

1. Flush 决定未满 batch 何时关闭；
2. Admission 决定已关闭 batch 是否允许提交；
3. Pool router 按请求代价/前缀选择逻辑池；
4. Endpoint router 在池内 round-robin、least-queued 或 prefix-affinity；
5. Ray adapter 提交 task/actor，并把完成结果恢复为原 submission 顺序；
6. lifecycle 层生成 exactly-once request/submission trace。

默认 `submission_granularity=batch` 仍按整个 submission 返回回收 credit；
显式 request 模式已经闭合逐请求 credit release。Daft packing group、Ray
submission 和 vLLM iteration batch 仍是三个不同层次。该实现有单元与真实本地
Daft→Ray task 合约证据，但 GPU 性能收益尚未建立。

### 证据边界

- 静态 `K_max=8` 的必要性已有 shared-vLLM 干扰证据。
- Queue-adaptive flush 已完成随机化变长输出、跨 arrival-rate、2048 held-out
  和 shared-vLLM 双作业：它稳定优于 fixed-25，但未优于 fixed-50；共享压力
  下约 89.4% 决策选择 50ms。当前默认采用 fixed 50ms。
- AIMD/EWMA/PID 已完成代码、单元/集成契约和单作业 512 请求真实 GPU
  矩阵。AIMD 又完成 shared-vLLM 128/512 双作业重复：0 次 decrease、窗口
  均值 15.953，相对 static K16 前台和吞吐均略差。当前没有动态反馈增量证据。
- UCB 多臂老虎机已有有限 action set 与 SLO reward 的纯控制器代码，但尚未
  接入 profiler。原因是缺少稳定的 epoch-level reward/归因边界；现在接入会把
  跨 epoch 的请求完成错误归因给当前 arm。
- 当前 queue-adaptive 仍是 25/50ms 两档、瞬时 running/waiting/KV 阈值
  baseline；动态 token-budget 已使用 arrival/service-rate EWMA，但 flush
  尚未加入 oldest-request SLO slack、token backlog EWMA 和滞回控制。
- 逐请求完成释放 credit 和持续补位已实现；此前 7B 云端 warm-up 误用
  `ray_batch_rows=1` 且仍为 batch granularity，不能作为该机制性能证据。
  下一步必须保留 packing row cap/token budget，并按等价请求负载比较 batch K
  与 request K。
- complete-row service quantum 已接入 offline/arrival replay：planning batch
  只定义组织边界，quantum 独立定义 HTTP/Ray completion 与 credit 释放边界，
  单行 prompt 永不拆分。active-work、service quantum、least-work routing 和
  shared multi-job credit 仍缺 GPU 性能证据；正式顺序是先标定 active-work，
  再固定总 actor slots 比较 pool 形状和 quantum，不能先跑组合策略再归因。

## 4. Actor pool、endpoint 与 GPU 扩展

### 已完成的框架

- `EndpointSnapshot` 显式包含 endpoint、pool、GPU、健康状态和队列指标。
- Request-cost pool 可把 short/long/prefix 请求路由到不同逻辑池。
- 池缺失或 endpoint 不健康时有确定性 fallback。
- Prefix affinity 使用 rendezvous hashing；无 prefix 时回退 least-queued。
- CLI 支持多个 endpoint URL、pool ID 和 GPU ID。
- Ray task 与 actor 走同一 typed scheduler。
- service endpoint 与 Ray actor worker 是两个维度：前者是独立 HTTP 服务地址，
  后者是面向该地址的 Ray 客户端 actor。配置并发上界为
  `endpoint_count × actor_workers_per_endpoint × ray_actor_max_concurrency`。
- HTTP worker 只向 Ray 申请 CPU，`ray_worker_num_gpus=0`；GPU 归外部模型服务。
  正式 completion 的 task retry、actor restart 和 actor task retry 均保持禁用。
- 正式/dry-run CSV 已记录 Ray 版本、解析后的 worker/resource 配置、endpoint 数、
  actor worker 数和逐 worker 提交计数；Python executor 的 `ray_version` 为空，
  actor concurrency/CPU 使用明确的非适用哨兵 0/0.0。Ray task 无 actor worker，
  记录 task 的实际 CPU 配额，actor-only concurrency 字段同样记 0。
- fake Ray task/actor 也应用相同的 CPU、零 GPU 与禁重试/重启 options；它仍只是
  debug backend，不是 HTTP 模型服务或性能证据。
- CSV 追加会校验既有 header 与当前 row keys 精确一致；空文件写 header，旧 schema
  不一致时在写入前明确失败，避免列静默错位。
- `ActorWorkerPoolSubmitter` 显式维护每 worker running/active-work/峰值/失败和
  slot-held 时间；round-robin/least-active-work 都只从有空 slot 的 worker
  选择，成功与失败都由 canonical handle 精确释放一次。
- effective per-endpoint admission 不超过
  `actor_workers_per_endpoint × ray_actor_max_concurrency`。正式 trace 记录
  worker ID/index/PID、planning/quantum identity、credit-held 和
  Ray-to-service delay；slot-held utilization 不是 GPU utilization。

### 尚未完成的验证

双 GPU per-endpoint K 功能门禁和 16K–131K active-work 扩展曲线已经完成；
65,536 是当前模型/workload 的预注册最小饱和点。Actor Pool 三形状 64 行 gate
已通过，正式重复运行中；least-work、动态预算、shared-credit、异构显存容量、
故障迁移和多 job 公平性仍待实测，因此不能声称多 GPU 调度已经完成。多个 Ray
actor worker 仍不能被当作多个 GPU endpoint。

## 5. 观测与实验运行基础设施

### 已完成

- CSV 同时记录 PostgreSQL/pgvector 版本、tokens/s、request P50/P95/P99、
  SLO、GPU utilization/memory/power/energy、energy/1k tokens、vLLM
  running/waiting/KV、FLOP delta 与 MFU。
- vLLM-compatible completion 可选请求逐 choice token IDs，并记录真实
  per-request output tokens 与 finish reason；generic compatible server 默认
  不发送该扩展字段。
- completion prompt envelope、temperature 与数据源最大 prompt-token
  过滤均为显式配置；超长行只排除，不截断或拆分 prompt。
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

### 本轮新增的可复用基础设施

1. **模型一致的 workload 计数入口**：workload importer 可调用当前 vLLM 的
   `/tokenize`，按实际模型 tokenizer 记录 `prompt_tokens`。模型上下文门禁采用
   “完整行保留或排除”，不通过截断、拆分或复制 prompt 凑实验规模。更换模型时
   只需切换 tokenizer endpoint/model name，不需要改 organizer 或 scheduler。
2. **受控 prefix workload 构造器**：从同一批基础行按稳定哈希选择精确且嵌套的
   0/30/70/100% 子集，只在完整原 prompt 前增加公共指令；session、arrival、
   tenant、目标输出元数据保持不变。每条变换后的 prompt 重新计数并通过上下文
   门禁，原 workload 不被原地修改。
3. **职责单一的 prefix organizer**：仅聚合真实重复的非空 `prefix_key`；
   唯一 prefix 和重复组内部均保持原始相对顺序。Length alignment 继续由
   `length_align_*` 独立策略承担，避免一个策略名隐式叠加两个机制。实现先建立
   prefix→row positions 映射，组织复杂度为 O(n)，不为每个 prefix 重扫输入。
4. **无执行后泄漏的代价估计边界**：离线 estimator 只读取提交前可知的行数、
   prompt token、输出上限、token budget、batch 统计、K_max、flush 和 arrival
   配置。实际输出 token、实测 E2E/service、vLLM、能耗和 MFU 只作为目标或评估
   证据，不进入特征。相同配置的重复运行按组切分，避免 train/test 泄漏。

### 按研究内容划分的当前状态

- **研究内容一——数据组织**：主机制和工程链路已经闭环。Sequential
  token-budget 是当前默认；fixed rows、length-align、prefix-aware、classic
  BFD、row-cap-first 都有可运行实现和对照入口。BFD/row-cap-first 已有负向规模
  边界，prefix-only 在 cache-off 下无稳定收益。尚未完成的是 prefix cache-on
  机制门禁、length-align×prefix 的显式联合消融，以及图像 frame/pixel cost
  adapter 的多模态复用验证。
- **研究内容二——调度与提交控制**：static K_max、arrival replay、flush、
  非阻塞 service observation、typed controller、pool/endpoint routing 和
  lifecycle trace 已形成完整流程。当前证据选择 static `K_max=8` + fixed
  50ms；queue-adaptive、AIMD/EWMA/PID 和 UCB 均未获得默认资格。尚未完成的是
  UCB 的 epoch reward 正确归因，以及真实多 endpoint/多 GPU 公平性和故障迁移。
- **两项策略联合关系**：18 单元筛选与候选重复已经完成；当前单 GPU 上联合候选
  未显著优于独立拼接，因此保留分层配置与联合搜索工具，不增加联合在线控制器。
- **多模态泛化验证**：策略接口和中性 `cost_units` 边界已具备，但真实图像
  source/cost adapter、CLIP/Qwen-VL workload 和 GPU 结果尚未完成。
- **算子代价估计（补充）**：初版实现与 grouped held-out 评估已完成，可提供
  粗粒度编排提示；独立时间段/新 workload 校准、预测区间和跨模型迁移仍未完成。

| 部分 | 代码完成度 | 真实证据 | 当前判断 |
|---|---|---|---|
| 数据读取与 Daft/Ray 主链路 | 高 | 64/512/1024 真实链路 | 已完成基础设施 |
| Fixed/token-budget batching | 高 | 多轮真实实验 | 机制成立，sequential 默认 |
| Length/prefix grouping | 高（代码） | 0/30/70/100% 受控 cache-off screen | prefix-only 无稳定收益；默认关闭 |
| BFD/row-cap-first | 高 | 512 + 1024 | 负向边界明确，不默认启用 |
| Static K_max | 高 | shared-vLLM | 必要性成立 |
| Queue-adaptive flush | 高 | 512 变长重复 + 跨 rate + 2048 held-out + shared-vLLM | 优于 fixed-25；未优于 fixed-50 |
| SLO-aware EWMA flush | 低（未实现） | 无 | 当前 two-level 仅为 baseline |
| Request-level continuous replenishment | 高（代码） | 双 GPU K 对照，但 offered work 未完全匹配 | 逐请求释放已实现；需在饱和 active work 下复验 |
| AIMD/EWMA/PID | 高（代码） | 单作业矩阵 + static K16 control + shared-vLLM 双作业 | AIMD 饱和至 K16，未保护前台；不默认启用 |
| UCB bandit | 中（纯控制器） | 无端到端实验 | 尚未接入执行路径 |
| Actor pool / endpoint routing | 高（有界 slots/trace） | 双 GPU endpoint 基线；pool shape 待测 | 固定 256 slots 后比较 1×256/2×128/4×64 |
| 联合 batching × submission 搜索 | 高（本地单 GPU） | 18 单元筛选 + 4 候选重复 | 独立拼接与联合最优不可分辨 |
| 多模态复用 | 低 | 未启动 | 文本主线完成后进行 |
| 算子代价估计 | 中 | 283 行、70 配置组、五个 held-out split | 粗粒度可用；不能作严格 SLO 预测 |

## 7. 后续设计与实施顺序

### 已闭环：提交控制与局部联合实验

- 自然 EOS 三组随机化重复中，fixed-50 与 queue-adaptive 相对 fixed-25
  tokens/s 分别 `+32.23% ± 3.90%` 与 `+32.09% ± 6.22%`；adaptive
  相对 fixed-50 为 `-0.10% ± 4.13%`，没有可分辨增量。
- 固定 16-token cap 的 18 单元联合筛选中，K16 虽然吞吐最高，但所有配置均
  违反 1% SLO guardrail。
- 候选重复中，独立拼接相对 fixed-25 tokens/s
  `+4.76% ± 2.29%`；联合候选相对独立拼接
  `-0.26% ± 2.07%`，没有可分辨增量。
- 相同 8192/K8 下 adaptive 相对 fixed-50 tokens/s
  `-0.75% ± 0.97%`。当前 workload 的主要收益来自 50ms coalescing
  window，而不是动态切换本身。

因此本地单 GPU 当前采用分层设计即可：sequential token-budget →
static K8 guardrail → workload-specific flush window。联合搜索保留为验证工具，
不引入联合在线控制器。

### 已完成：跨负载、2048 与受控 prefix 边界

1. 约 51.4/25.7/12.85 req/s 三档均由 fixed-50 保持最佳或与 adaptive
   等价，adaptive 未获得默认资格；
2. 2048 held-out 没有出现策略排序反转，但暴露持续积压的尾延迟放大；
3. prefix ratio `0/30/70/100%` cache-off 实验未显示 prefix-only 收益，
   并修复唯一 prefix 重排和隐式 length-align 耦合。

### 下一优先：饱和后 Actor Pool/持续补位、完整 flush 与缓存机制

1. 已完成 16K–131K active-work 扩展曲线，选择 65,536；
2. 正在该点固定每 endpoint 256 slots，比 1×256/2×128/4×64 actor pool；
3. 固定最佳 pool 与相同 planning/work，比较 whole-batch、
   service-quantum 512/1024/2048/4096 和 request diagnostic；
4. 再实现 oldest-request slack、token backlog 与 arrival/service EWMA 驱动的
   SLO-aware flush；
5. Prefix-aware 只有在单独启用 prefix cache、记录命中证据后才重新评估；
6. UCB 只在能按固定 epoch 正确归因跨 epoch 请求 reward 后接入，并保留 static
   K=8 safety fallback。

完整顺序与放弃条件见
`experiments/plans/literature_driven_pipeline_optimization_guide.md`。

### 后续：多 GPU、多模态与代价估计

- 多 GPU：先部署同构、各自独立占用 GPU 的双 service endpoint，再做异构池；
  验证健康回退、队列均衡和公平性。
- 多模态：增加 image source/cost adapter，把 token cost 替换为 frame/pixel
  cost，复用 organizer、scheduler、routing 和 tracing。
- 代价估计：当前 grouped held-out 五切分平均 MAE 11.68s、MAPE 50.60%、
  R² 0.776；相对误差仍不稳定，下一步增加独立时间段/新 workload 校准和
  预测区间，不新增独立系统层。

## 8. 当前可安全采用的默认值

- 数据引擎：Daft；
- 执行：Ray task/actor 按实验目的选择，不把其差异包装成贡献；
- batching：sequential token-budget；
- admission：static `K_max=8`；
- flush：离线实验 immediate；当前已验证的 accelerated-replay 负载范围使用
  fixed 50ms；更换模型、到达过程或硬件后重新校准；
- routing：单 endpoint 使用 round-robin；多 endpoint 实验前不启用复杂池路由；
- vLLM 重复 prompt 对比：明确记录 prefix cache；本轮公平比较使用 disabled；
- 任何策略晋级必须同时通过 SLO goodput，而不是只看平均吞吐或 MFU。
