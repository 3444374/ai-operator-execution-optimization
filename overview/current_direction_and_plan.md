# SemLoom 当前方向与计划

更新：2026-10-01。本页用于快速阅读；方向以[项目总纲](../PROJECT_OUTLINE.md)为准，
实现以[代码状态](../code/INFRA_STATUS.md)和源码为准，数字以[证据台账](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)
及原始结果为准。

## 研究对象

本项目研究**PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化**。
数据库负责 SQL、计划、snapshot、权限和查询生命周期；SemLoom 组织数据库确定的任务，
通过可替换的 Daft、Ray、vLLM 和图像 backend 执行。文本 `AI_COMPLETE` 是首版主场景，
图像 `AI_EMBED/AI_CLASSIFY` 用于检验方法能否跨模态使用。

## 两项研究内容

| 内容 | 要比较什么 |
|---|---|
| 数据组织 | token/frame/阶段 work 与局部性如何影响完整查询耗时、模型供给和资源留存 |
| 提交与调度 | 固定 request/work capacity 下的补位、服务实例路由和多 Job 服务分配 |

算子代价估计同时服务于数据组织、调度和数据库计划比较。研究设计见
[数据执行计划](../experiments/plans/data_organization_batching.md#design-hypotheses)。

## 已有结果与近期工作

已完成[文本 Map 四路径匹配比较](../experiments/plans/completed/text_map_matched_comparison.md)：
SemLoom Daft/Ray、PG-source direct、原生Ray Data与Daft Native，比较完整查询时间和质量。
[新版四路径比较](../experiments/results/postgresql/text_map_main_real_20260930/README.md)完成68条查询、41,024次请求及独立评价。
1024行中位数：SemLoom18.101秒、direct7.705秒、Ray22.849秒、Daft11.668秒；质量差异和旧失败保留。
后续[worker复用真实复测](../experiments/results/postgresql/text_map_worker_reuse_real_20260930/README.md)累计20,688次请求；
固定配置下完整查询16.502→14.794秒、准备4.737→2.831秒，仍慢于direct与Daft，配对输出0–5行不同。
[gateway与首行诊断](../experiments/results/postgresql/text_map_gateway_lifecycle_20261001/README.md)完成10,272次fixture请求，
共享gateway完整查询12.428→7.569秒，启动单列；PG节点首行0.233秒、客户端2.688秒，与发送缓冲相符。
后续三路径真实15条查询/12,336次通过，新gateway14.626→共享10.131秒、准备约14毫秒，direct7.681秒仍更快。
中止11,312次、共享启动2.277秒与输出差0–4行保留；下一项细化消费时间，不证明质量等价、容量平台或通用优势。

- PostgreSQL 18.3 extension 已完成受限 Filter/Map 语义、公共执行接口、Map 有界多在途及可选图像嵌入的
  工程检查。可选 Daft/Ray 文本传输在[受控查询](../experiments/results/postgresql/incremental_transport_20260927/README.md)
  后完成[12 条真实模型 SQL、4,144 次请求](../experiments/results/postgresql/transport_real_20260927/README.md)。
  输出存在 0–2 行配对差异；多节点和持续供给还需验证。
- [完整容量复查](../experiments/results/postgresql/m1_full_recheck_20260920/README.md)保留原清理告警及后续资源核对；
  两条路径均未满足持续供给要求，原异常未复现且原因未确定。该结果不阻塞有限配置的系统比较，不能改称容量已确定。
- [五种全局信息方式](../experiments/results/postgresql/m1_m2_f_real_20260920/README.md)尚无稳定收益，
  全局预扫尚未接入 SemMap；图像路径完成 151 次真实 CLIP 前向及指定恢复检查，
  GPU 计算中故障与匹配性能仍待验证。
- Filter 的质量、真实成本校准、第二物理路径与近似策略仍需独立证据；
  它们不阻止使用公开任务或受控输入研究 SemLoom 核心。实际 PostgreSQL 路径仍逐条验证语义、
  任务关联、取消、错误和资源使用。

计划、原始数据和历史运行的准确身份分别见[实验计划入口](../experiments/plans/README.md)、
[结果入口](../experiments/results/README.md)及[项目日志](../PROJECT_LOG.md)。
