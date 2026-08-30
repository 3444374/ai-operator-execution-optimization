# SemLoom 当前方向与计划

最后更新：2026-08-30

这是两分钟快速参考卡片。完整定义以根 [`PROJECT_OUTLINE.md`](../PROJECT_OUTLINE.md) 为准；
实验完成度以
[`experiments/plans/experiment_status_and_gaps.md`](../experiments/plans/experiment_status_and_gaps.md)
为准；数字只从结果报告和原始数据读取。

## 1. 一句话定位

SemLoom 是面向数据库 AI 语义算子的工作量感知执行与多作业调度系统。本项目研究 PostgreSQL
内置 AI 语义算子的外部分布式物理执行与调度优化：参考 Sema-like 数据库原生
语义算子架构，由数据库拥有 SQL、关系 child plan、snapshot、权限、语义计划、结果解析以及取消、
错误和结果生命周期；Daft、Ray、vLLM、CLIP 与 pgvector 是可替换的物理执行和验证平台。

项目不做广泛 PostgreSQL fork；只有 extension 对目标 LOTUS/Cortex plan optimization 或稳定 node
lifecycle 出现已复现阻断时，才使用最小 PG18.3 core semantic patch。项目不修改 vLLM continuous
batching、Ray scheduler、模型结构或 GPU kernel，也不使用
`SELECT/fetchall → Python → HTTP → INSERT` 作为主路径。

## 2. 当前最短路径

核心研究链路是：PostgreSQL 定义 semantic intent/reference policy，生成并比较 physical paths；
SemLoom 只组织和执行数据库已经封闭的 tasks；completion 回到 PostgreSQL 后恢复关系结果。

1. **已完成 carrier 资格**：`REL_18_3` extension / planner-visible recording `SemMap` 与 exact
   `SemFilter`、shared runtime、同步单在途 UDS 和公共 compatibility suite 已通过；它们证明数据库
   生命周期与外部 seam，不代表已经执行真实 AI 语义或获得性能收益。
2. **当前先完成真实语义合同**：把 instruction、prompt program、result parser、model/generation
   constraints 与 NULL/error/order policy 编译为数据库拥有的 `SemanticPlanSpec`，用同步 provider 跑通
   exact `SemFilter` 真实模型纵切面；此时不扩异步、多在途或调度器。
3. **再完成数据库优化资格**：为同一逻辑 SemFilter 建立 reference 与 proxy/oracle physical paths，
   显式保存 algorithm/model role、quality policy、calibration evidence、AI-work cost 和 reference fallback。
4. **随后审查载体**：用真实 paths 验证 prepared-plan identity、hook coexistence 和有限 predicate
   placement；extension 能表达就继续使用，只有已复现阻断才增加最小 core patch。
5. **最后进入数据执行研究**：扩 accepted-prefix、多在途/乱序 completion 和增量 SemLoom session，
   比较 IMLane-like database batch 与 provider rebatching。Kalypso-like lineage/KV 机制只有出现真实
   多阶段依赖后才另行立项。
6. 上述数据库资格完成前不扩 GPU 矩阵、不调 SAOR；既有外部物理执行证据继续保留原身份。

实施入口：

- [`postgresql_ai_semantic_operator_architecture_20260827.md`](../experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md)

## 3. 研究内容

| 部分 | 研究对象 | 主要评价 |
|---|---|---|
| 数据组织策略 | token/frame work budget、长度/局部性分组、引擎级 batch 参数 | packing、work skew、cache/locality、吞吐、尾延迟、质量 |
| 调度与提交控制 | active work、completion release、共享 credit、路由、idle borrowing、多 Job | 吞吐、Job JCT、P99/SLO、公平、隔离、恢复 |
| 多模态泛化 | 同一 work/credit/state 抽象从文本迁移到图像 | 策略代码复用，以及在不同模态下何时不再有效 |
| 算子代价估计 | stage/service/remaining work 与 SLO slack | 误差、配置排序、decision regret、预测区间 |

算子代价估计是两项策略的共同使能组件，不是第三项研究内容。写回固定使用 PostgreSQL + pgvector
的 COPY + deferred index 工程 baseline。

## 4. 已有证据与仍待验证

已建立：

- exact `SemMap`/`SemFilter`、公共 runtime 与 PostgreSQL 18.3 compatibility suite 已通过功能、取消和
  RSS/FD 生命周期验证；尚无真实模型或性能结论；
- 文本 cache-on 数据组织效果随 endpoint consolidation、KV 压力与 prefix 结构变化；
- 固定/shared credit 和 1/2/4 Job 实验显示效率、隔离与公平存在权衡，动态策略尚未普遍超过强静态点；
- 图像 5K 画像、原生静态 baseline、多 Job 观察、matched-resource 与 observe-only 接线已经完成；
- 双 4090 算子代价 v2 cache-on 正式运行 320/320 有效，首次无效运行仍独立保留；
- 五臂系统对照已有 rehearsal，可说明可运行性与观测合同，不构成 formal 排名。

仍待验证：

- 真实 `SemanticPlanSpec`、同步 exact 真实模型 reference path；
- SemFilter 第二 physical path、AI-work cost、近似质量 policy 与 reference fallback；
- extension/core 载体审查与 LOTUS/Cortex semantic alternatives；
- IMLane-like execution-batch placement；Kalypso-like dependency/KV execution 仅作后续参考；
- 图像 HSE/static 非劣与受控 state-aware 动作；
- 五臂系统级 matched formal、SAOR 跨层 capability 和部分条件性纠正补测；
- 代价估计在新时间段或新 workload 上的校准与在线决策价值。

## 5. 证据能支持什么

- 原生 baseline 必须由被测系统拥有执行与调度；SemLoom 自写 UDF/actor/credit/router 按真实身份标注。
- rehearsal、CPU/fake、microbenchmark 和 observe-only 不能写成完整方法或正式性能结论。
- 动态方法必须与同资源上限、在实验开始前选定且运行期间不变的最佳静态配置比较，并满足预先规定的重复、正确性和稳定性条件。
- 文本与图像协议不同，不直接比较吞吐；CLIP 没有自回归 KV cache、TTFT 或 TPOT。
- 无效或失败运行保留审计，但必须从有效聚合和 headline 中排除。

## 6. 常用入口

| 内容 | 文件 |
|---|---|
| 项目总纲 | [`PROJECT_OUTLINE.md`](../PROJECT_OUTLINE.md) |
| 项目导航 | [`PROJECT_INDEX.md`](../PROJECT_INDEX.md) |
| 实验计划导航 | [`experiments/plans/README.md`](../experiments/plans/README.md) |
| 实验状态与缺口 | [`experiment_status_and_gaps.md`](../experiments/plans/experiment_status_and_gaps.md) |
| 正式证据注册表 | [`EXPERIMENT_EVIDENCE_REGISTRY.md`](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| 代码实现状态 | [`code/INFRA_STATUS.md`](../code/INFRA_STATUS.md) |
| 文献与设计依据 | [`research/knowledge_hub.md`](../research/knowledge_hub.md) |
| 开题 claim 边界 | [`opening/claim_matrix.md`](../opening/claim_matrix.md) |
