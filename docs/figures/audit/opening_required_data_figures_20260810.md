# 开题所需实验数据图：第一性原理审计（2026-08-10）

## 1. 开题要证明什么

开题不要求项目已经在所有 baseline 上取得最终性能胜利，但必须让题目、研究问题、设计、
baseline 边界和实验计划之间形成闭环。对本课题，最小证明义务不是“模块已经实现”，而是
以下八个事实：

1. 数据库 AI 算子的外部数据执行层是独立研究对象，不等于数据库内核、vLLM 内部调度或
   模型 kernel；
2. 记录数或图片数不是可迁移的 AI work 代理，因此需要带阶段、局部性、期限和不确定性的
   WorkDescriptor；
3. 配置上限不是当前运行状态，模型服务在供给、队列、KV、MFU 和 tail 上存在非线性容量
   区间，因此需要新鲜状态观测和有界准入；
4. 多 Job 同时访问共享服务时，静态份额会叠加配额损失与真实竞争；共享 work credit 能
   提高 work conservation，但必须同时约束前台隔离、long-job 稳定性和公平；
5. 同一抽象能覆盖文本 token work 与图像 prepare/model/tensor work，而不是假设不同模态
   成本相同；
6. 算子代价估计必须改善配置排序与决策 regret，才能同时使能数据组织和调度。
7. 现有文本与图像 baseline 必须按可比合同分轨呈现；语义、规模或计时边界不一致时，
   “不可排名”本身就是系统边界证据，不能用总排行榜掩盖。
8. 图像多 Job 的干扰会随 Job 类型与执行图改变；跨模态调度必须感知每个 Job 的阶段进度，
   但 observe-only 状态和共享额度的现有结果不等于动态策略已经胜出。

## 2. 数据图与证明义务的最小映射

| 顺序 | 正文数据图 | 回答的问题 | 设计对应 | 证据边界 |
|---:|---|---|---|---|
| 1 | `opening_motivation_work_state`；主讲拆分版 `part1_work` / `part2_state_capacity` | 同行数为何不是同 work；同 W 为何不是同状态；容量为何不能无限加压 | WorkDescriptor、fresh snapshot、offline safe envelope | 三个 panel 来自两个硬件/模型合同，只作机制拼图，不作跨 panel 性能比较；P07A/P07B 仅拆版 |
| 2 | `opening_text_baseline_evidence_map` | DuckDB、Daft Native/Ray、Ray Data 当前各能在哪条合同下比较 | 产品 database-E2E 与官方 Chat graph 分轨 | SQuAD 产品轨与 ShareGPT Chat 轨不能跨 panel 排名；DuckDB ShareGPT cap 失败单列 |
| 3 | `opening_native_fourjob_normalized_impact` | 当前原生系统是否也会在共享服务下出现 short/long 干扰 | 全局 work/state 可观测、多 Job 管理；方法前的动机证据 | 只画各系统 `four-job / isolated single`，不画跨系统绝对 JCT 排名，不归因内部算法，不证明项目胜出 |
| 4 | `opening_work_organization_regime_v2` | 数据组织为何必须同时考虑 work 与 locality；相同策略跨regime、相同regime跨策略的吞吐与cache-hit如何变化 | token/frame budget、locality-preserving organization；候选臂独立消融后再做联合约束 | 当前 2-endpoint 合同未过 95% feeding 门，只证明 regime/locality 机制 |
| 5 | `opening_multijob_interference_tradeoff` | Project中多少是quota、多少是竞争；shared credit改善什么、牺牲什么；各控制/策略/机制是互斥还是联动 | 方法后的项目机制 A/B；每条线固定为同一个Job，依次经过独立、1/4配额、Static与Shared；Static/Shared是同上限A/B | 一个 `Short@0s → 3×Long@5s`、equal weight；折线表示受控场景顺序而非时间；不是最终动态控制器胜出 |
| 6 | `opening_image_stage_aware_evidence`；主讲拆分版 `part1_prepare` / `part2_transfer_window` | 图像 CPU prepare、tensor transfer 与 GPU model stage 为何要显式建模；提交窗口为何不能只看图片数 | staged WorkDescriptor、CPU/GPU 队列观测、frame/byte work、有界准入 | prepare/actor 与 transfer 为 microprofile；active-window 为单次 screening；只证明机制，不作系统排名或动态胜出结论；P08A/P08B 仅拆版 |
| 7 | `opening_image_baseline_evidence_map` | 12K 诊断与 120K 同资源 formal 各显示什么数据结果 | 纯数据图；能力/角色边界改用报告独立表格 | 12K 三臂 setup-dominated；120K 仅 Ray Data/Project 可排名；blocked 路径不生成性能值 |
| 8 | `opening_image_fourjob_normalized_impact` | 图像 short/long Job 在原生执行图和项目路径中分别受到多大并发影响 | 与文本图统一的路径/策略×Job slowdown 矩阵；per-Job staged work/state、共享额度、隔离与公平约束 | 只比较各路径内部 `four-job / isolated`；Project static/shared 为互斥臂，状态快照只观测，不能称动态收益；原生路径无统一阶段计时 |
| 9 | `opening_cost_model_decision_quality_v2` | 代价估计是否能选对配置，而不只是预测误差较小 | parse + profile calibration + residual correction | 20 个文本 context 的 marginal pass；尚未证明跨模态或在线收益 |

九张图分别承担不同证明义务，不能用一张综合雷达图或“项目比 baseline 快”的总柱状图替代。

## 3. 正文、报告和备份的分层

### PPT 正文优先

正文按 `1 → 2 → 3 → 研究边界/WorkDescriptor 方案图 → 4 → 5 → 6 → 7 → 8 → 9` 讲。若答辩时间有限，
图 3 与图 5 可分成连续两页：先证明现有原生路径存在多 Job 干扰，再证明项目 shared credit
的条件性收益与公平缺口。不要把两者压缩成同一坐标系的绝对性能排名。

### 报告正文或答辩备份

- `opening_native_single_job_request_latency`：报告正文先独立展示 Job JCT、waiting、queue time
  与 TTFT，证明相近批任务 makespan 会掩盖请求级排队。
- `opening_native_single_job_state_fingerprint`：保留六个原单位 small multiples，展示 tok/s、
  running、waiting、KV、MFU 与 GPU utilization，解释 Daft Native/Ray 的 overqueue 与 Ray Data
  当前路径的 underfeed；它是 JCT 主图的状态补充，不承担独立性能排名。
- 两 Job online/eager arrival-regime 表：作为图 4 的最小因果复核，不与四 Job 主图混画。
- database-E2E replacement：只用 correctness/语义/可排名性表，不画 ShareGPT 三臂性能图。
- DuckDB AI：只放有界输出产品语义和能力表；没有多 Job 正式结果，不生成性能图。

## 4. 明确不画的内容

- static–dynamic phase change：尚无同上限正式结果，不能画示意结果曲线；
- DuckDB 多 Job：当前只有准备合同，没有正式结果；
- Daft built-in 图像 60K×2：object-store 容量边界不同，只作可扩展性说明，不混入主排名；
- database-E2E ShareGPT 三臂：bounded C32 欠供给且 DuckDB cap 语义失败，只保留附录表；
- 跨框架绝对 short JCT：Project 与原生 adapter 的 T0/arrival 边界不同，禁止混排。

## 5. 当前数据是否足以完成开题图集

足以。图 1–9 的权威数据已经冻结；文本原生四 Job 图直接由
`opening_fourjob_interference_20260809/data/combined/job_formal_runs.csv` 生成，不需要重跑。
当前缺口属于论文主实验而非开题证据缺口：同上限 state-aware phase change、weighted/SLO
多 Job、图像动态 phase-change、跨模态 cost held-out。开题应把这些写成预注册实验与停止规则，
不能为得到更好看的图而追加扫描。

## 6. 绘图与统计规则

- warm-up 不进入统计；正式重复 `n=3` 时显示均值 ± SD，或显示全部 formal 点；
- 连续/容量轴带单位；比例明确 0–1 或百分比；禁止双 y 轴、雷达图和 3D；
- 原生多 Job 图使用各自 isolated-single 均值归一化，raw four-job formal 点可见；
- 颜色之外同时使用 marker、位置和直接标签；灰度预览必须可区分；
- 图注必须写数据合同、`n`、误差类型和不能声称边界；
- PNG 300 DPI + SVG；本地可生成 PDF 做字体/矢量 QA，但仓库按现有规则不提交 PDF。
