# 当前方向与计划

最后更新：2026-08-14

> 本文是两分钟快速参考卡片。完整定义以 `PROJECT_OUTLINE.md` 为准；当前执行顺序以
> `opening/claim_matrix.md` 与 `experiments/plans/experiment_status_and_gaps.md` 顶部
> 开题冻结优先级为准；实验数字以各结果目录的
> README、manifest 和 CSV 为准。

## 1. 当前重点

- **开题 framing 已冻结**：题目保持“数据库 AI 负载的执行优化与调度研究”，统一对象是
  Database 与 Model Service 之间的 AI Data Execution Layer。
- **两项内容不变**：workload 感知的 work-unit 构造；容量感知的提交、路由与多 job 调度。
  cost estimator 是共同使能组件，文本和图像是跨模态证据轨道。
- **三臂 database-E2E correctness 护栏已完成**：24/24 单元、18 formal 的 source/sink、
  exactly-once 与稳定性通过；后续 ShareGPT C32–C256 扫描证明旧 C32 direct 仅达已测峰值
  52.07%，正式原生矩阵冻结 C128，旧 154.57% 比值不作方法排名。
- **SAOR fixed-envelope formal 已完成但未晋级**：40/40、0 incident、exactly-once；resolution-aware v2 完整 validation passed；SAOR
  在 credit 臂内 fg JCT/P99 最好，但 static 的 fg P99 29.2s、SLO violation 0% 显著更强。
  原始 failed validation 保留审计；250 ms resolution-aware v2 已在完整 artifact 上 passed、
  credit mechanism effective 12/12。当前实现 `slo_weight=0`，只验证了
  fairness/release；strict-priority upper-bound 两轮短测 fg P99 14.27s、SLO 0%。下一候选已冻结为
  通用有界词典序 release（显式 priority/SLO budget + actual-work debt cap + 队首定向 reclaim
  barrier/普通 priority fitting-head fallback），首轮只测 2 Job 的 $0.125W_e/0.25W_e$ 两个 cap；
  selector/coordinator/scheduler/Ray/runner、timeout cleanup、无损事件账本、四臂 readiness 与
  两轮 fail-closed 汇总器已完成。旧 single-head 双轮 GPU development gate 没有任何 cap 全过：
  $0.25W_e$ 第 2 轮 debt-recovery=0，两个 cap 的 fg P99 约 56/49–50s、SLO violation
  93–95%/85–88%。
  请求/event 交叉验证显示每个可见前台 head 都获 priority，但 per-Job 同步 pull 只向 coordinator
  暴露一个 head，完整 ready backlog 在相邻 acquire 间不可见。状态为
  `development-run/not-promoted/not-formal-registered`。bounded-ready 修订随后从两个全新 root
  完成 8/8 cell：$0.125W_e$ 两轮约 12.36K tok/s、fg P99 17.58–18.15s、fg SLO 0%、bulk
  30s miss 65.8%–66.6%，通过开发门；$0.25W_e$ 被 bulk guard 拒绝。但 ready-window 与 selector
  同时变化，随后完成了同 ready-window 的**项目内部** FIFO/DRR/VTC-style/strict-priority/
  proposed 双轮归因：DRR/VTC-style 约 12.90K tok/s、fg P99 27.23/26.16s、30s SLO 零违约；
  proposed 12.28K tok/s、fg P99 17.85s。proposed 是用约 4.8% 吞吐与约 5.2% bulk JCT 换 tail
  的观测非支配折中点，不是 selector winner；固定顺序 n=2，`formal_authorized=false`。
  现已另建位置平衡六臂 1+3 Project mechanism 合同，冻结 5% headline 与 throughput/JCT/SLO/
  no-service/repayment 保护边界；`63d17300` final rehearsal 已通过固定 output cap 与 repayment
  证据门，当前只进入独立审核，仍不授权 formal。系统层另补 Daft
  Native/Daft Ray/Ray Data/project static/proposed 的 PG source/sink matched comparison。
  `single-head + shared FIFO` bridge 已完成：shared capacity 使 tok/s +25.96%
  但 fg P99 +99.17%；bounded-ready 在同 FIFO 下再使 tok/s +7.30%、fg P99 −33.62%，但
  fg SLO violation 仍约 39.7%。FIFO/DRR/VTC 为 Project 本地实现的标准算法 controls，不是
  Daft/Ray/vLLM 原生实现；reservation 仍后置。
- **实现边界已审计**：shared work credit、completion release、neutral work admission 和
  least-work routing 已进入调度器；图像 staged descriptor 与 observe-only fresh snapshot
  已接入 project runner 且 24/24 正式门通过，但不改变决策；snapshot 100% fresh、构建均值
  0.141 ms，static/proposed group JCT 只差 0.98%。stage controller 与 CE5 在线接线仍未
  进入正式路径，不能把观测接线写成完整 state-aware 方法已落地。
- **图像原生多 Job 已完成**：Daft built-in/Ray Data single→four-job 40/40 runs、30 formal
  group 通过；两轨均出现非均匀 Job slowdown，只作系统内干扰/状态证据，不作跨框架排名。
- **当前范围冻结为单租户多 Job**：coordinator 按 `job_id` 记账，用于同一租户内 interactive/
  batch 等 workload class 的 service differentiation、公平与隔离。论文 formal 将公平（weighted
  service/lag）与隔离（固定 victim 在 aggressor burst 下的 P99/goodput/SLO 变化）分开；`group
  JCT` 作为 makespan 使用，不新增同义字段。多租户不阻塞当前实验，未来只在现有 Job scheduler
  外增加 tenant entitlement/debt 与 per-tenant buffer cap，不把 flat Job 竞争直接外推为租户公平。

## 2. 课题定位

研究数据库 AI 算子外部分布式数据处理链路中，上游如何组织请求、估计工作量、控制提交节奏，
并根据模型服务状态协调 CPU 数据准备与 GPU 推理。项目不修改 vLLM、Ray scheduler 或模型 kernel。

两项研究内容保持不变：

1. **数据组织策略**：token/frame budget、长度/局部性感知分组和 Daft 引擎参数。
2. **调度与提交控制**：active-work、request-level credit、共享 credit、idle borrowing 和多 job 公平队列。

图像 workload 用于正文的多模态泛化验证；算子代价估计是两项策略的共同使能组件，不独立扩张为
第三项研究内容。局部代价估计通过 ranking/regret 晋级门槛后，保留 TPC-H-derived AI 查询计划
held-out 验证其计划选择价值；该项为 `planned-conditional`，不改变当前 320-run。写回使用
PostgreSQL + pgvector 工程 baseline。

## 3. 当前技术路径

**Image 目标路径（operator-E2E runner/formal 已完成，system-E2E 待补）**：

```text
PostgreSQL
  → Daft DataFrame
  → Ray CPU decode / preprocess + organizer / scheduler
  → typed tensor-input CLIP backend（有界、独立校准的 Ray GPU actor pool）
  → PostgreSQL + pgvector
```

文本证据路径仍保留：

```text
PostgreSQL → Daft → Ray organizer / scheduler → vLLM → PostgreSQL
```

## 4. 已建立的关键证据

| 证据 | 当前可得出的结论 |
|---|---|
| 统一文本三臂 replacement：24/24 单元 correctness 护栏 | SQuAD 三静态路径近似中性；ShareGPT C32 欠供给，旧 project/C32-direct=1.546 只作配置诊断 |
| ShareGPT bounded C32/C64/C128/C256：9,455/14,058/17,834/18,158 tok/s | C128 是达到已测峰值 97% 的最小点；C256 waiting/KV/TTFT 显著恶化，支持状态感知与有界提交动机 |
| 原生单 job 1+3：bounded/Daft Native/Daft Ray/Ray Data=17,800/17,286/16,747/3,551 tok/s | Daft 两臂稳定过量排队，Ray Data 当前路径稳定欠供给；同一服务需要 work-rate + running/waiting/KV/MFU 联合感知 |
| 5s guaranteed-overlap：原生三轨 short JCT +82.42%/+104.84%/+32.76%；Project online 下 shared 提升aggregate但伤short/Jain，eager下quota-only +59.00%、matched static+long +58.77%、shared+long +28.90%，shared相对static short JCT−48.94% | 后到Job干扰、idle borrowing和arrival-regime dependence已证明；只比较各轨内部normalized impact，跨轨T0/绝对JCT不排名 |
| 四 Job full/quarter/static/shared：按三次 formal 均值，shared 相对 static 总吞吐 +8.68%，四个 Job JCT 均改善；shared/quarter-solo=0.45/1.29/1.14/0.68，raw-work Jain 0.960→0.923 | 仅效率/JCT 子向量构成 baseline-relative empirical Pareto；long1/2 未达到经验性保留份额非劣，Jain 表示收益更不均。compact 数据不能补 event-level lag/starvation，不称 DRF/Themis/VTC 性质 |
| DuckDB AI ShareGPT：service tok/s≈direct，4,921/6,144 cap 语义失败 | 产品语义兼容性必须进入 correct throughput，不能把问题写成纯速度排名 |
| 65,536 active work/endpoint 达最大吞吐的 97.8% | 固定 token-aware credit 是当前简单、稳健的文本默认点 |
| AIMD/PID/EWMA、动态 flush、多 actor 及 capacity-only SAOR 多数未过强静态门槛 | dynamic K 不作为主方法；不能声称复杂动态策略普遍胜过强静态 baseline |
| SAOR capacity-only vs K160 约 +0.52%，且 Jain/tail 未改善 | K160/最小饱和点应固定为总 envelope；动态对象改为 Job active-set 的份额借用、回收与释放顺序 |
| SAOR fixed-envelope 2-Job formal：SAOR 12,393 tok/s、fg P99 50.3s；static 9,508 tok/s、fg P99 29.2s、SLO 0%；strict-priority smoke 11,791 tok/s、fg P99 14.27s | soft fairness score 与 fg tail/SLO 目标错位；release-only 在已知 foreground 存活信号下可达，但 hard priority 必须增加 anti-starvation、bulk lag/SLO 约束 |
| 2-ep 与 4-ep cache-ON 数据组织排名反转 | 上游组织/准入价值依赖 endpoint consolidation 与 KV 饱和 regime |
| matched-KV：2-ep 中性、4-ep prefix routing +5.9% | 目前更支持 endpoint consolidation，而非单纯 per-endpoint KV 大小是驱动；仍有饱和深度混淆 |
| CLIP 5K 串行画像：CPU 准备/actor forward=`13.8–18.3` | 图像链路存在异构流水线候选空间；尚未证明 CPU、Ray/host copy 或 PCIe 谁是主瓶颈 |
| CLIP operator-E2E：project/fused-Daft=单卡 1.296×、双卡 1.138× | 独立校准后，静态阶段拆分优于 fused UDF；staged 两臂仅通过小规模 gate、尚无正式排名，故不能声称优于主流异构流水线 |
| HSE static core：descriptor/lease/真实 ready/byte-work 预留已接 runner | 本地只证明执行安全与可观测性；尚无 GPU 性能数据，不能声称优于 direct-dependency static |
| Ray Data vs project matched-resource 两轮正式实验 | 相同 CPU 下 project 方向一致；开题 headline 冻结为约 13% 到 15% operator-JCT 改善，不使用旧 45.7% |
| 429 formal cost-model LOO | CE5 pooled/macro/max regret 为 1.67%/2.90%/14.72%，candidate pairwise 0.808；只算 marginal pass |

CLIP 画像进一步表明主要瓶颈位于 CPU processor 整体（fast path 约
4.4–4.8ms/image）；子阶段实验只直接测得 resize 约 1.3ms，剩余时间尚未充分归因，
不能全部写成 normalize。后续 operator-E2E 已把该候选空间转化为静态阶段拆分的
正结果，但状态感知策略仍未与冻结最佳静态 pipeline 对照。

## 5. 当前实施顺序

1. 保持 Claim Matrix、统一三臂 replacement 与开题停止规则一致；当前图表只整理数据：A/C 待标签级重绘，F/H 待首次生成，B、WorkDescriptor 总览、D、E 不重画，phase-change 无结果且不画。
2. 同一 ShareGPT Chat manifest 的 bounded、Daft Native/Ray、Ray Data 原生单 job 1+3 已完成并归档。
3. 原生 short/long 两 job 错峰观察与项目 static/shared 同上限 A/B 已完成；它们没有 global
   FIFO/no project Job scheduler 对照，不能单独证明 SAOR 必要。
4. SAOR fixed-envelope 2-Job 决定性 benchmark 已完成；原始 failed validation 保留作审计，
   resolution-aware v2 完整重汇总已 passed。foreground strict-priority 两轮短测已证明
   release-only 上界可达；`saor-v0.5.1` 的 $0.125W_e/0.25W_e$ 双轮 development gate 已按门禁停止，
   没有 formal candidate。随后把 Daft/Ray ready work 以 bounded async ready-set/ready-count
   显式暴露给 coordinator。独立 `saor_bounded_ready` 路径已完成：旧 policy 不改写，窗口
   由冻结 effective K 与 endpoint 数×W 派生，trace 分开 ready/registered/granted/submit；
   coordinator 对 register/grant 记录 request ID+epoch，并在 actor 同一时钟域内要求 foreground
   registered-ready 时 foreign fallback=0。两个全新 development root 已完成：只冻结
   $0.125W_e$ 候选；$0.25W_e$ 两轮 bulk miss 越界已拒绝。同 ready-window 的 Project FIFO、
   DRR/WFQ、VTC-style、strict-priority 与 proposed 双轮归因，以及 single-head shared FIFO→
   bounded-ready FIFO observation bridge 均已完成。proposed 是以约 4.8% 吞吐和约 5.2% bulk
   JCT 代价换更低 foreground tail 的观测非支配折中，不是 selector winner；固定顺序 n=2，
   历史结果的 `formal_authorized=false` 不变。首个最终 root 已证明单 recovery 在途不能形成
   debt repayment 并 fail closed；修正为 residual-aware projected-debt work budget 后，最终
   `63d17300` 全新六臂 rehearsal 已通过：固定 admission output cap=256 的 6,144-request audit
   通过，15/15 repayment completed、P95 3.234s、0 unresolved，1,108/1,108 离线投影一致。
   单次 SAOR 相对 VTC-style 的 service lag P95 改善 13.15%，longest no-service 仅 +0.014%，
   故只说明值得做 1+3，不是 winner。审核已登记的 validation/archive SHA 后
   才能由单独提交解锁 1+3 formal。Daft Native/Daft Ray/Ray Data/project frozen-static/proposed 的同一
   2-Job native-system matched comparison 独立推进，原生 baseline 继续使用自身调度。期间不扫
   cap，不跑 4-Job/reservation/dynamic K。
5. 当前暂停新图、PPT、云文档和 Wiki，只同步本地报告、聚合数据、待画图清单与 Git。

晋级门槛：相对各自独立标定的强静态/系统 baseline 至少改善约 5%，重复方向一致，且质量不退化。

## 6. 仍不能声称

- 不能把静态阶段拆分胜过项目自写 `daft_native/daft_ray` UDF 写成“动态状态感知策略已胜出”；后者尚未
  与冻结最佳静态 project pipeline 正式对照。
- 不能把赢项目自写 fused Daft UDF 写成“优于 Daft/PolarDB 原生流水线”；旧 staged gate 也只是 adapter 可行性，native baseline 尚无正式规模排名。
- 不能把 CPU preprocess 主导写成“CPU→GPU 数据传输主导”。
- 不能把 4-ep 病态 bounded 值当作服务上限，或把 text/image 跨协议吞吐直接比较。
- 不能把 prefix/KV 机制迁移到 CLIP；CLIP 没有自回归 KV cache、TTFT 或 TPOT。
- 不能把 PG18.4 AutoDL rehearsal 写成 PG18.3 内部平台结论。
- 不能把“内部执行锁定”写成“导师已确认最终 scope”。

## 7. 文档入口

| 内容 | 权威入口 |
|---|---|
| 项目总纲 | `PROJECT_OUTLINE.md` |
| 开题 Claim Matrix 与停止规则 | `opening/claim_matrix.md` |
| 当前执行状态与 parked 项 | `experiments/plans/experiment_status_and_gaps.md` §0 |
| 图像 workload、baseline 与门禁 | `experiments/plans/image_clip_workload_lock_20260731.md` |
| 5K CLIP 初始画像 | `motivation/results/gpu/image_clip_bottleneck_profile_20260801.md` |
| 当前实现边界复测 | `motivation/results/gpu/image_clip_preprocess_variants_20260801/` |
| CLIP 项目自写 Daft UDF diagnostic | `motivation/results/gpu/image_clip_native_baseline_20260801/` |
| 正式机制证据台账 | `experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md` |
| 代码完成度与边界 | `code/INFRA_STATUS.md` |
| 文献与设计依据 | `research/knowledge_hub.md` |
