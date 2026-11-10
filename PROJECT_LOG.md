# 项目日志

本页记录跨目录决定。当前实现、接口和实验结果分别维护在代码说明、状态页和所属报告中。

## 2026-11-10：实际 v2 runner 的观察覆盖修正

首个 Q8 off/on 原生运行完成，但实际 v2 runner 未进入原 v1 决策接点。按固定官方源码补充 v2 的原输入准备返回观察，
[主报告](experiments/results/diagnostics/semloom_execution_candidates_20261110/README.md)保留未知原件、先失败后通过的最小复现和实际请求统计。
该路径没有 cascade dispatch，不能以 false 解释为分组未利用机会。

## 2026-11-10：继续尝试其余执行候选

在独立候选分支增加提示布局分析、图片表示／准备位置、有界提前准备、合法就绪选择、成本路由与原生阶段观察。
[主报告](experiments/results/diagnostics/semloom_execution_candidates_20261110/README.md)分开真实输入 CPU、构造消费轨迹、
原生状态机和实机协议；没有从代码存在推定完整查询加速。新开关保持显式，研究分支不合入 main。

## 2026-11-10：机制原型、诊断与内部简化

在已有批决策观察上补充完整请求排序、表示生命周期／留存选择和供给等待分析。
采用有限、默认关闭的策略接点；通用读取复用同一事件解析器，内部直接使用既有 owner 保证。
[主报告](experiments/results/diagnostics/semloom_mechanism_prototypes_20261110/README.md)保留原请求回放、
图片准备观察及首次组合失败，不将观察工具或 CPU 原型写成真实查询收益。

## 2026-11-10：机制研究的原生批决策观察切片

复用现有资源采集器，加入实际批成员、cascade／graph 选择与填充的观察及离线诊断。
诊断保留缺项和下一项干预，不由利用率或 padding 计数判断加速；[验证范围](experiments/results/diagnostics/native_gpu_decisions_20261110/README.md)区分 CPU 与实机。
机制研发顺序另由研究计划维护，普通基线接入不属于此切片。

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
