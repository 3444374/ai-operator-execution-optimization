# 项目日志

本页记录跨目录决定。当前实现、接口和实验结果分别维护在代码说明、状态页和所属报告中。

## 2026-11-10：开始原请求分析与 Core 同语义接入

原始请求画像进入[文本组织研究](code/src/research/data_organization/README.md#native-request-opportunity)，
补充文本入口通过[独立实验适配](code/src/experiments/sembench_core_operator.py)调用既有 Core owner，不改变原生对照的调度归属。
真实 Q9 的 CPU 观察与方法／IPC 检查见[主报告](experiments/results/diagnostics/sembench_request_preparation_20261110/README.md)；
原生引擎观察、增强 Ray／Daft 接入和模型比较仍按各自准备推进。

## 2026-11-10：以执行 DAG 特性选择异构资源编排方法

总纲与架构明确两项研究内容共用“真实观察、特性归纳、单项干预、完整查询验证”的路线；
[原观察章节](experiments/plans/数据组织.md#dag-first-observation)更新首批对象、采集缺项、资源判定与停止条件，避免另建平行方案。
[GPU／引擎候选](docs/research/优化方法依据.md#gpu-engine-coordination)补入关系前缀与 attention／CUDA Graph、
动态消费者与缓存留存、实际未缓存工作和数据位置选择，并区分 MLSys 2025 字段布局的质量前提。
允许为具体假说修改 vLLM／Ray，根规则和现行说明同步；[比较规范](experiments/plans/reference/对照规范.md#backend-change-comparison)保留原生、单独补丁与联合方法。
本批只修改研究与实施文档，没有修改执行代码、运行模型或认定候选收益；既往失败、原始结果和未合并原型保持原身份。


## 2026-11-10：精简常用文档

状态页集中说明当前能力、可选路径和未验证项；README 按现用入口导航。
已合入版本的候选描述、重复测试摘要、旧首页跳转和多处退役过程说明不再混入当前用法。
实验报告、原始结果、失败和有用途的研究材料保留，执行代码与配置不变。

<a id="semantic-chain-orchestration-work"></a>
算子链与资源编排分别由[架构计划](experiments/plans/系统架构.md#semantic-chain-gpu-orchestration)和
[数据组织计划](experiments/plans/数据组织.md#single-gpu-supply-engineering)维护。

此前完整日志保留在 [Git 历史](https://github.com/3444374/ai-operator-execution-optimization/blob/f44f174a27e564734774c015fa0847334698a321/PROJECT_LOG.md)。
