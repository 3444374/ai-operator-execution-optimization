# 文本数据组织研究

本目录保留原有 Arrow／Daft 文本组织原型、到达回放，以及尚未接入主查询的排序、装箱和等待候选。
它原先为 PG 画像程序构造输入批；该程序已退役，算法和研究材料继续保留。
普通 records、链执行和二进制 payload 物化不导入本包。

## 现有实现

[text_materializers.py](text_materializers.py) 保留原 `OrganizerConfig`、`ArrowOrganizer`、
`DaftOrganizer`、`make_organizer` 及原指标函数；源码从旧位置逐字迁入。
固定行数、顺序 token 组批、长度排序、前缀分组及两种装箱策略的参数和行为保持。
输入是调用方已有的 Arrow 表，输出为 Arrow 批列表；部分 Daft 路径收集完整表，
不能把它当作当前 Core 的渐进准备或主机驻留控制。

本原型复用核心的[标量装箱](../../planning/packing/scalar.py)、
[token 成本](../../modalities/text/costs.py)和[Daft runtime](../../infrastructure/daft_runtime.py)。
这些公共组件继续独立维护；研究代码依赖它们，普通执行不反向依赖研究包。

当前异步 prepared 组批由[PreparedTokenOrganizer](../../execution_provider/adapters/lotus_chain_token_batches.py)
处理合法就绪请求，并复用 `PendingBatchBuilder`。它没有接入本原型的长度／前缀排序和装箱策略；
目录迁移不表示这些候选已接入，也不增加真实模型或性能结论。

## 到达回放与策略原型

[arrival_replay.py](arrival_replay.py)拥有回放时钟、服务观测、轨迹和原ArrivalReplayBatcher。
[flush.py](flush.py)保留即时／固定等待、两档队列响应，以及服务目标（SLO）／指数加权平均（EWMA）等待原型；
[token_budget.py](token_budget.py)保留静态／按服务速率选择token预算B及到达速率估计。算法、原参数和模拟行为保持。
它们只供现有研究回放使用，不接入当前records的默认准备或真实反馈调度。

生产[batching.py](../../scheduling/organization/batching.py)只拥有RowArrival、PendingBatch和PendingBatchBuilder，
普通准备继续从原位置直接导入，不加载回放或上述策略。研究代码单向复用该构建器。
旧策略模块和顶层调度包的研究导出已退出；历史调用从准确原版本恢复，不保留转发层。

## 测试与原件

[原型测试](../../../tests/research/data_organization/test_text_materializers.py)检查批成员、排序、
成本和 Arrow／native Daft 一致性；[Daft／Ray 组合测试](../../../tests/research/data_organization/test_daft_ray_composition.py)
仍需要真实 Ray，应按运行安排单独执行。
原始实验、装箱汇总脚本和研究计划保持各自来源。
旧导入不再提供，五份具名投影读取[准确原件](../../../../experiments/reproduction/native_baselines/README.md#text-organization-research-sources)。

原等待、B和回放专测归本目录对应的[研究测试](../../../tests/research/data_organization/)，
生产构建器的八项检查保留在[原调度测试](../../../tests/scheduling/test_runtime_batching.py)。
旧等待／B模块的[来源与恢复](../../../../experiments/reproduction/native_baselines/README.md#arrival-replay-research-sources)独立登记。
