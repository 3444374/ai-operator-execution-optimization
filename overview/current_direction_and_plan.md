# 当前方向与计划

最后更新：2026-08-09

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
- **state-aware 仍是拟研究方法**：现有证据支持 strong static、regime dependence、图像
  matched-resource 结构收益和代价估计可行性，但没有证明 state-aware 优于冻结静态点。
- **实现边界已审计**：shared work credit、completion release、neutral work admission 和
  least-work routing 已进入调度器；staged descriptor、fresh stage snapshot/controller 与
  CE5 在线接线尚未进入正式 runner，不能把接口或离线结果写成完整方法已落地。

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
| 5s guaranteed-overlap：Daft Native/Ray/Ray Data short JCT +82.42%/+104.84%/+32.76%；项目 shared vs static 总吞吐 +21.03%，但 short JCT +4.98%、Jain 下降 | 后到 Job 的前台干扰与效率—隔离—公平权衡已证明；原生只作外部观察，shared/dynamic 不作全面胜出表述 |
| DuckDB AI ShareGPT：service tok/s≈direct，4,921/6,144 cap 语义失败 | 产品语义兼容性必须进入 correct throughput，不能把问题写成纯速度排名 |
| 65,536 active work/endpoint 达最大吞吐的 97.8% | 固定 token-aware credit 是当前简单、稳健的文本默认点 |
| AIMD/PID/EWMA、动态 flush、多 actor 多数未过 5% 门槛 | 不能声称复杂动态策略普遍胜过强静态 baseline |
| 2-ep 与 4-ep cache-ON 数据组织排名反转 | 上游组织/准入价值依赖 endpoint consolidation 与 KV 饱和 regime |
| matched-KV：2-ep 中性、4-ep prefix routing +5.9% | 目前更支持 endpoint consolidation，而非单纯 per-endpoint KV 大小是驱动；仍有饱和深度混淆 |
| CLIP 5K 串行画像：CPU 准备/actor forward=`13.8–18.3` | 图像链路存在异构流水线候选空间；尚未证明 CPU、Ray/host copy 或 PCIe 谁是主瓶颈 |
| CLIP operator-E2E：project/fused-Daft=单卡 1.296×、双卡 1.138× | 独立校准后，静态阶段拆分优于 fused UDF；staged 两臂仅通过小规模 gate、尚无正式排名，故不能声称优于主流异构流水线 |
| Ray Data vs project matched-resource 两轮正式实验 | 相同 CPU 下 project 方向一致；开题 headline 冻结为约 13% 到 15% operator-JCT 改善，不使用旧 45.7% |
| 429 formal cost-model LOO | CE5 pooled/macro/max regret 为 1.67%/2.90%/14.72%，candidate pairwise 0.808；只算 marginal pass |

CLIP 画像进一步表明主要瓶颈位于 CPU processor 整体（fast path 约
4.4–4.8ms/image）；子阶段实验只直接测得 resize 约 1.3ms，剩余时间尚未充分归因，
不能全部写成 normalize。后续 operator-E2E 已把该候选空间转化为静态阶段拆分的
正结果，但状态感知策略仍未与冻结最佳静态 pipeline 对照。

## 5. 当前实施顺序

1. 保持 Claim Matrix、现有六张叙事资产、统一三臂 replacement 与开题停止规则一致；原生状态指纹和两 Job 图仅整理数据，phase-change 只保留计划，不实际绘图。
2. 同一 ShareGPT Chat manifest 的 bounded、Daft Native/Ray、Ray Data 原生单 job 1+3 已完成并归档。
3. 原生 short/long 两 job 错峰观察与项目 static/shared 同上限 A/B 已完成；开题前停止扩扫 offset、weight、4+ job 追正。
4. 当前暂停新图、PPT、云文档和 Wiki，只同步本地报告、聚合数据、待画图清单与 Git。

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
