# AI 算子执行 Infra 当前状态

日期：2026-08-01

本文说明当前 Daft + Ray 上游执行基础设施已经完成什么、实际执行流程、研究证据
边界，以及下一步还需要实现和验证的内容。研究方向仍是数据库 AI 算子外部执行
链路，不修改 vLLM 内部。

全部机制、代码测试和正式结果目录的逐项对应见
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

上图是已完成的**文本/vLLM 路径**。2026-08-01 内部执行方向转为 image-first A+B；
CLIP 5K motivation/profile 已通过门禁，但下列 path-B 仍是**待实现目标**，不能写成已跑通：

```text
PostgreSQL image source
  -> Daft
  -> Ray CPU decode + resize + normalize
  -> frame-cost organizer + endpoint-state-aware admission
  -> typed tensor-input CLIP backend (Ray GPU actor primary)
  -> PostgreSQL + pgvector
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
- 完整 SLO-aware flush 已加入 oldest-request slack、arrival/service EWMA、
  独立容量下界、hard deadline 和滞回，并完成双 4090 正式重复；相对 fixed-50
  未过 5% 晋升门槛。25–50ms 动作相对秒级 request P99 缺少一阶杠杆。
- 逐请求完成释放 credit 和持续补位已实现；此前 7B 云端 warm-up 误用
  `ray_batch_rows=1` 且仍为 batch granularity，不能作为该机制性能证据。
  下一步必须保留 packing row cap/token budget，并按等价请求负载比较 batch K
  与 request K。
- complete-row service quantum 已接入 offline/arrival replay：planning batch
  只定义组织边界，quantum 独立定义 HTTP/Ray completion 与 credit 释放边界，
  单行 prompt 永不拆分。active-work、pool shape 与 service quantum GPU
  对照均已完成；least-work routing 和 shared multi-job credit/fairness 仍缺
  GPU 性能证据，下一步只先做共享 credit/fairness 门禁，不能先跑组合策略再归因。

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
65,536 是当前模型/workload 的预注册最小饱和点。Actor Pool 三形状 gate 与
正式重复均已完成；固定 work/slots/CPU 后，多 actor 未达到 5% 晋升门槛，
当前保留 1×256。complete-row service quantum 正式重复也已完成：细粒度
把 credit-held 降约 16%，但稳态吞吐增益不足 5%，固定 quantum 不晋升；
request-level completion 保留作后续动态/多 job 精确控制基础。
shared-credit 与 1/2/4-job 核心矩阵已经完成；2-job 无增量，4-job 聚合指标过
5% 但逐 repeat 波动大。仍缺 held-out/staggered/weighted/异构 workload、故障迁移
和异构显存容量验证，因此不能声称多 GPU 调度已经普遍完成。多个 Ray actor
worker 仍不能被当作多个 GPU endpoint。上述文本遗留项在 image-first pivot 后为
`parked-conditional`。

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
  manifest，并参与 resume 一致性校验。（2026-07-31 起 runner 在 `main()` 额外
  校验 `prefix_caching` 与 live vLLM 进程标志一致，见 `code/src/vllm_probe.py`：
  不符 fail-closed、探不到则 warn。）

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
  边界，prefix-only 在 cache-off 下无稳定收益；cache-on 下 prefix-aware batching
  中性；prefix-affinity routing 在 2-ep/7B 中性（prefix_affinity vs least_queued
  −0.1%，<5% 门禁），但 4-ep/1.5B prefix_affinity +5.9%（46,943 vs 44,317 tok/s，
  3 repeat 不重叠、CV≤0.9%）跨过 5% 门禁。后续 matched-KV 扫描表明 2-ep/1.5B
  在 gpu_mem_util 0.3–0.9 均中性，因此当前更支持 endpoint consolidation，而非
  单纯 per-endpoint KV 大小，是驱动；4-ep 饱和深度仍未完全隔离。文本残留已 parked。
  尚未完成的是图像 frame/pixel cost adapter 的多模态复用验证。
- **研究内容二——调度与提交控制**：static K_max、arrival replay、flush、
  非阻塞 service observation、typed controller、pool/endpoint routing 和
  lifecycle trace 已形成完整流程。当前证据选择 static `K_max=8` + fixed
  50ms；queue-adaptive、AIMD/EWMA/PID 和 UCB 均未获得默认资格。尚未完成的是
  UCB 的 epoch reward 正确归因，以及真实多 endpoint/多 GPU 公平性和故障迁移。
- **两项策略联合关系**：18 单元筛选与候选重复已经完成；当前单 GPU 上联合候选
  未显著优于独立拼接，因此保留分层配置与联合搜索工具，不增加联合在线控制器。
- **多模态泛化验证**：策略接口和中性 `cost_units` 边界已具备；COCO val 5K 的
  CLIP motivation/profile 已完成并通过门禁（CPU 准备/GPU embed=13.8–18.3）。
  但真实 image source/frame-cost adapter、CLIP HTTP endpoint、path-B runner 和
  正式策略/baseline 结果尚未完成。
- **算子代价估计（共同使能组件）**：初版实现与 grouped held-out 评估已完成，可提供
  粗粒度编排提示；独立时间段/新 workload 校准、预测区间和跨模型迁移仍未完成。

| 部分 | 代码完成度 | 真实证据 | 当前判断 |
|---|---|---|---|
| 数据读取与 Daft/Ray 主链路 | 高 | 64/512/1024 真实链路 | 已完成基础设施 |
| Fixed/token-budget batching | 高 | 多轮真实实验 | 机制成立，sequential 默认 |
| Length/prefix grouping | 高（代码） | 0/30/70/100% cache-off screen + cache-on batching/routing 消融 | cache-off 无收益；cache-on batching **regime-dependent**（2-ep 近似中性、4-ep KV 饱和分化+排名反转，见 `rc1_data_organization/`）；2-ep/7B routing 中性（−0.1%），4-ep/1.5B +5.9% 跨过 5% 门禁但混淆待隔离，方向有条件重开 |
| BFD/row-cap-first | 高 | 512 + 1024 | 负向边界明确，不默认启用 |
| Static K_max | 高 | shared-vLLM | 必要性成立 |
| Queue-adaptive flush | 高 | 512 变长重复 + 跨 rate + 2048 held-out + shared-vLLM | 优于 fixed-25；未优于 fixed-50 |
| SLO-aware EWMA flush | 高 | 双 4090 high/arrival-limited 各三次 formal | 相对 fixed-50 未过 5% 门槛；不默认启用 |
| Request-level continuous replenishment | 高（代码） | 双 GPU K 对照 + 固定 active-work quantum/formal | 逐请求释放与 completion 已验证；保留为 shared-credit/fairness 基础 |
| AIMD/EWMA/PID | 高（代码） | 单作业矩阵 + static K16 control + shared-vLLM 双作业 | AIMD 饱和至 K16，未保护前台；不默认启用 |
| UCB bandit | 中（纯控制器） | 无端到端实验 | 尚未接入执行路径 |
| Actor pool / endpoint routing | 高（有界 slots/trace） | 双 GPU 1×256/2×128/4×64 formal | 多 actor 未过 5% 门槛；单 job 保留 1×256，多 job 分池待测 |
| Shared-vLLM group runner | 高（代码/模板/真实 formal） | 双 4090 36/36 group run、63 formal job | shared-credit 容量安全、公平性通过；2-job 无增量，4-job 聚合过 5% 门槛但逐 repeat 不稳定，暂作高竞争条件性候选 |
| 联合 batching × submission 搜索 | 高（本地单 GPU） | 18 单元筛选 + 4 候选重复 | 独立拼接与联合最优不可分辨 |
| 多模态复用 | 低（画像脚本已具备） | 5K CLIP motivation/profile GO | 当前主线；待 source/frame-cost、endpoint、path-B runner 与正式 baseline |
| 算子代价估计 | 中 | 283 行、70 配置组、五个 held-out split | 粗粒度可用；不能作严格 SLO 预测 |

## 7. 后续设计与实施顺序

### 当前优先：image path-B + A+B

1. ✅ `BatchRequest`/scheduler/Ray adapter 已支持中性 work-unit；lazy image source、
   typed batch/result、CPU CLIP preprocessor 和常驻 tensor actor 已实现并有单测；
2. 补齐通用 organizer 的 work-cost adapter，避免 image runner 复制 token batching；
3. 跑通 PG→Daft→Ray CPU preprocess→Ray CLIP GPU actor→pgvector，并补 exactly-once、阶段计时和队列 trace；
4. 分别校准 bounded direct、Daft `@daft.cls` Native、vLLM pooling、Ray Data、naive 与 ours；
5. 在强静态点上实现 endpoint-state-aware 请求成形和 `<100 LOC` 代价模型 v1；
6. 正式报告吞吐/JCT/tail/SLO、overlap、GPU busy、能耗和 Recall@10。

5K CLIP 画像只通过了“存在异构优化空间”的 fatal-flaw 门禁，不代表上述系统已经
实现或项目策略已经胜过 Daft Native。

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

### 已完成：多 job shared credit/fairness 核心矩阵

- 1/2/4-job × independent/static/shared-DRR 共 36/36 group run 完成；
  共享 request/work credit 无越界并最终归零；
- 1-job 协调开销可忽略，2-job 无可分辨增量；
- 4-job shared 聚合吞吐 +9.57%、max P99 -22.52%、max JCT -15.89%，
  但三次吞吐变化为 +8.43%/-0.28%/+22.60%，保留为高竞争条件性候选；
- staggered idle borrowing、weighted overlap fairness 和异构 workload
  尚未验证。

### 文本轨道遗留（parked-conditional）

1. 已完成 16K–131K active-work 扩展曲线，选择 65,536；
2. 已完成固定 slots/CPU 的 1×256/2×128/4×64 actor pool 对照，保留 1×256；
3. 已完成 whole-batch、service quantum 与 request diagnostic，固定 quantum
   未过 5% 门槛；
4. 已完成 SLO-aware EWMA flush 正式对照，25–50ms 动作未过 5% 门槛；
5. 已完成 Shared-vLLM 1/2/4-job 核心矩阵；正式推进前先补两层 baseline：
   OceanBase `AI_COMPLETE`/同 PostgreSQL bounded AsyncIO 的无 Daft/Ray 核心
   对照，以及 Daft `prompt()` Native/Ray/Ray Data 的官方 runtime 对照；
6. 两层统一使用 Chat Completions、同一双 endpoint 与请求 manifest；每个
   baseline 独立 calibration，不能以弱默认值对比已调优 ours；
7. baseline 同时承担 transient saturation/ramp 实验：固定总工作量与下游
   容量，报告 direct ceiling、time-to-ceiling、ramp regret 和最小饱和 work；
8. baseline 锁定后再做 4-job held-out、staggered idle borrowing 与 weighted
   fairness；
9. Prefix-aware 已在 cache-on 下评估：batching regime-dependent（2-ep 近似中性、4-ep 饱和分化，见 `rc1_data_organization/`）；routing 在 2-ep/7B 中性
   （−0.1%），4-ep/1.5B prefix_affinity +5.9% 跨过 5% 门禁但受 model×endpoint×KV
   与过饱和 regime（SLO 违约 25–31%）混淆，方向有条件重开，待隔离消融；
   per-arm 命中率待 runner 增采；
10. UCB 只在能按固定 epoch 正确归因跨 epoch 请求 reward 后接入，并保留 static
   K=8 safety fallback。

截至 2026-07-29，统一 Chat Completions、不可变 manifest、固定双 endpoint
分片、bounded HTTP、vLLM Bench、Daft Native/Ray、Ray Data HTTP、
OceanBase `AI_COMPLETE` adapter、归一化结果和 fail-closed gate 均已有实现与
单元测试。首轮 64 行双 GPU core gate 已 5/5 通过：每项 64/64
exactly-once、0 incident、双 endpoint、work skew 0.0085%，最终队列归零。

该门禁仍不是远端性能 baseline。通过后的等价性审计发现 vLLM Bench 会对
custom prompt 与 openai-chat 重复套 chat template；Ray Data 的整数 concurrency
在小作业中只起一个 autoscaling actor；Daft/Ray Data 只有 shard-barrier 级
延迟，且 Daft 不返回 output usage。vLLM Bench `--skip-chat-template`、Ray Data
`(n,n)` 固定 actor pool 与 `timing_granularity/token_accounting` 已在提交
`f2e82bd` 的全新 re-gate 再次 5/5 通过。小 gate 固定创建 4 actor，但可并行
task 不足，实际只使用 1 actor，不能据此得出扩展结论。

当前最后一个 calibration 前置缺口是统一服务端工作量计数。gate runner 已增加
每个 cell、每个 endpoint 的 vLLM prompt/generation cumulative counter 前后
快照和差分，并按 adapter accounting 能力交叉核验客户端字段。Daft 以服务端
差分补齐 output-work 证据；shard-barrier P95 仍不得与 request-level P95 横比。
全新真实双 GPU service-counter gate 通过前不能启动 calibration/formal。

统一服务端计数门禁随后已通过。256 行 scale gate 的五个 core arm 为 5/5、
0 incident；vLLM Bench C32 与 bounded HTTP C32 均约 4.93K total tokens/s，
而 Daft Native 单次约 9.82K。该差异目前只说明直接客户端 C32 可能未饱和，
不能证明 Daft 提升了 vLLM 计算速度。runner 已提供 fail-closed 的
`--include-cell` 与 `--concurrency-override id=N`，下一步只用同一 manifest
校准 vLLM/bounded C64→C128，不再远端临时改配置，也不重复运行 Daft/Ray
Data。每个并发档使用全新输出目录，先过 exactly-once、服务端 counter 和空
队列门禁，再比较 JCT、generation/total tokens/s 与 3% 饱和阈值。

C64 校准中 vLLM Bench/bounded 分别达到 8,342/8,333 total tokens/s，
JCT 均约 12.02s；相对 C32 提升约 69%。vLLM Bench C128 的真实 peak
concurrency=128，达到 12,762 total tokens/s、JCT 7.849s，相对 C64 再提升
53%。bounded C128 被 httpx 默认 100-connection pool 截断，8,711
tokens/s 数据作废；`async_http.py` 已把总连接与 keepalive 容量显式绑定为
`concurrency_per_endpoint × endpoint_count`。全新 bounded-only C128 re-gate
观测到 endpoint running=124/125，得到 12,472 total tokens/s、JCT 8.048s，
与 vLLM Bench C128 只差约 2.3%，修复已通过真实双 GPU 门禁。

512 行 direct calibration 随后完成：vLLM Bench/bounded C256 分别为
15,351/14,532 total tokens/s、JCT 11.931/12.569s；C128→C256 仍提升
24.3%/33.0%。因此 8.0–8.2K 只能称为历史 project runner/arrival-replay
链路平台，C256 只能称当前 `max_num_seqs` 配置硬上限。

project profiler 现已支持 manifest 锁定的离线 request-level replenishment、
固定 endpoint routing、raw Chat/temperature=0/trace-target payload 契约、
逐行源数据核验和 `source_row_offset`。512 行模板扫描 static K32–256 与
active work 16K–98K，并在正式 CSV 记录 manifest SHA 与 validated rows。
远端持久 Ray head `127.0.0.1:6380` 已只读确认可用。

首次 64 行 project gate 在任何 HTTP 请求前 fail closed：数据库有
`target_output_tokens>256` 的行，而 official manifest 的有效输出 work 已按
请求 cap 裁为 256；project 旧路径仍使用未裁剪 trace target。统一语义已改为
`min(trace target, completion_max_tokens)`，同时修正调度 work 与 manifest
校验；guard 另行重算 exact `source_row_hash`，不会把两个不同的 above-cap
raw targets 当成同一源行。旧失败目录保留，512 校准未启动；完整测试和全新 64 行 re-gate 通过前
不得继续。

行数门禁已解除：数据库现已持有多个 2048 行 workload（sharegpt_multiturn，
doc_id 300000-302047；sharegpt_concentrated 2048 行；sharegpt_burstgpt 2048 行）
以及 lmcache_agent（851 行）等，2,048 formal 不再因行数不足或 held-out 复用被
阻塞。2,048 formal 当前唯一的前置阻塞为下文的 5% 等价性门禁（K256 vs W98K），
该门禁未达阈值前完整 calibration、2,048 formal 与新上游策略均不启动；manifest
导出改用上述独立 workload 的只读切片，不再需要向 `0..2047` 追加 `2048..2559`。

第二次 64 行 re-gate 已在 `beeee20` 通过，但随后 512 行校准首场景暴露
active-work 背压语义缺陷：调度器曾把“该请求会超过 endpoint-local work
credit”复用为 `healthy=false`。当冻结 manifest 指定的 endpoint 暂满、另一
endpoint 仍有容量时，pinned router 会误报服务不健康而不是等待。当前模型已
明确拆成长期/观测健康 `healthy` 与 request-specific `available`；容量不足是
可重试背压，真实不健康仍立即失败。preferred endpoint 也固定其 pool，不能被
pool fallback 改写。fixed-pool、multi-pool pinned 与 shared-credit oversized
边界均已测试锁定。失败校准 0/9、无该失败请求的 HTTP 提交且无 `runs.csv`，
现场保留；新提交通过全新 64 行远端门禁前仍不得恢复 512 校准。

`0c370ce` 的全新 64 行 gate 随后已通过。512 行 9-cell calibration 虽为
9/9、0 incident，但理论等价的 static K256 与 nonbinding W98K 分别只有
11,736/4,153 total tokens/s，不能用于参数选择。只读诊断确认两者 manifest、
payload、max inflight=512、endpoint work、bounded wait 和 output work 等价；
主差异是 W98K 首个 full-concurrency cell 在 HTTP/vLLM request wall 多约
28.6s，actor readiness 只贡献约 3s。

当前实现增加显式 actor-ready barrier，barrier 在 E2E timer 之前并记录
`actor_ready_s`；非流式 Chat HTTP 结果与 submission trace 记录 request
start、response headers、body complete、headers wait 和 body read。校准模板
改为同压力 warm-up + 3 repeats，并新增只包含 K256/W98K 的等价性门禁。
该门禁未达到 5% 等价阈值前，完整 calibration、2,048 formal 和新策略均不
启动。

完整顺序与放弃条件见
`experiments/plans/literature_driven_pipeline_optimization_guide.md`。

### Image-first pivot 后的多 GPU、多模态与代价估计

- 多 GPU：先部署同构、各自独立占用 GPU 的双 service endpoint，再做异构池；
  验证健康回退、队列均衡和公平性。
- 多模态：5K CLIP 画像已完成；下一步增加 image source/cost adapter，把 token
  cost 替换为 frame/pixel cost，复用 organizer、scheduler、routing 和 tracing，
  并接入独立 CLIP endpoint。
- 代价估计：当前 grouped held-out 五切分平均 MAE 11.68s、MAPE 50.60%、
  R² 0.776；相对误差仍不稳定，下一步增加独立时间段/新 workload 校准和
  预测区间，不新增独立系统层。

## 8. 当前可安全采用的默认值

以下是**文本/vLLM 轨道**的历史验证默认值，不可直接复制为 image/CLIP 的最优点。
Image 路径在 baseline calibration 完成前没有可声称的默认 K/frame budget/actor shape。

- 数据引擎：Daft；
- 执行：Ray task/actor 按实验目的选择，不把其差异包装成贡献；
- batching：sequential token-budget；
- admission：static `K_max=8`；
- flush：离线实验 immediate；当前已验证的 accelerated-replay 负载范围使用
  fixed 50ms；更换模型、到达过程或硬件后重新校准；
- routing：单 endpoint 使用 round-robin；多 endpoint 实验前不启用复杂池路由；
- vLLM 重复 prompt 对比：明确记录 prefix cache；本轮公平比较使用 disabled；
- 任何策略晋级必须同时通过 SLO goodput，而不是只看平均吞吐或 MFU。
