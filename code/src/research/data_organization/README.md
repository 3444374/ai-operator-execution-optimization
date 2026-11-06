# 文本数据组织研究

本目录保留原有 Arrow／Daft 文本组织原型及尚未接入主查询的排序、装箱候选。
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

## 测试与原件

[原型测试](../../../tests/research/data_organization/test_text_materializers.py)检查批成员、排序、
成本和 Arrow／native Daft 一致性；[Daft／Ray 组合测试](../../../tests/research/data_organization/test_daft_ray_composition.py)
仍需要真实 Ray，应按运行安排单独执行。
原始实验、装箱汇总脚本和研究计划保持各自来源。
旧导入不再提供，五份具名投影读取[准确原件](../../../../experiments/reproduction/native_baselines/README.md#text-organization-research-sources)。
