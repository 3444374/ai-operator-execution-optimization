# 正式研究实验

更新日期：2026-08-30

本目录回答“提出的方法或系统改动是否有效”。动机测试回答“问题是否存在”，放在
`../motivation/`；组件和环境验证放在 `../feasibility/`。

## 当前状态

当前不扩展 GPU 实验矩阵。PostgreSQL 18.3 recording `SemMap/SemFilter`、shared runtime、同步 UDS 和
compatibility suite 已完成；短期先实现真实 `SemanticPlanSpec` 与同步 exact model reference，再实现
第二 physical path 和 cost/quality，随后做载体审查。路径选择资格完成后才扩增量 SemLoom 并比较
IMLane-like batch placement；
Kalypso lineage、`SemJoin`、fusion/AQE 仅作后续参考。LOTUS compatibility/native baseline 不再是前置
依赖。现阶段性能实验可使用明确标注的 emulated operator contract，但不能称为已实现数据库内算子。

已有文本与图像证据继续保留：

- 文本数据组织、静态/shared credit、多 Job 与算子代价估计已有正式结果；
- 图像 workload 画像、原生静态 baseline、多 Job 观察和 observe-only 证据已完成；
- 动态 state-aware 图像方法、SAOR 跨层 capability 和部分纠正补测仍未完成或未获 formal 授权。

完成度和下一步只看 [`plans/experiment_status_and_gaps.md`](plans/experiment_status_and_gaps.md)；
真实数字和可引用结论只看
[`results/EXPERIMENT_EVIDENCE_REGISTRY.md`](results/EXPERIMENT_EVIDENCE_REGISTRY.md) 与对应结果报告。

## 目录结构

| 路径 | 作用 |
|---|---|
| [`plans/`](plans/) | 当前计划、状态入口与 baseline 总入口 |
| [`plans/completed/`](plans/completed/) | 已完成且由结果报告接管的预注册合同 |
| [`plans/reference/`](plans/reference/) | 跨实验协议、检查清单和设计依据 |
| [`plans/archive/`](plans/archive/) | 被替代、暂停或仅供历史追溯的方案 |
| [`results/`](results/) | 正式结果、raw/manifest、事故证据与结论边界 |

## 研究内容

| 研究内容 | 评价重点 |
|---|---|
| 数据组织策略 | 固定行数与 work-aware 组织、长度/prefix 分组在不同压力条件下的吞吐和尾延迟 |
| 调度与提交控制 | 同资源上限下的 active work、补位、共享 credit、路由、公平与隔离 |
| 多模态泛化 | 同一策略抽象从 token/work budget 映射到 frame/image work 后是否仍成立 |
| 算子代价估计 | 预测误差、配置排序、决策 regret 和预测区间；作为两项策略的共同支撑 |

写回采用 PostgreSQL + pgvector 的 COPY + deferred index 作为统一工程 baseline，不单列为研究内容。

## 证据边界

- 正式 baseline 必须由被测系统拥有执行与调度；项目自写 actor/credit/router 只能按其真实身份标注。
- CPU/fake、microbenchmark、旧 UDF 和项目自写 Daft UDF 只作诊断或历史参考。
- 原生系统 comparison 与项目方法 comparison 必须使用同 workload、模型、资源和计时边界。
- 无效、失败与被排除的运行不得删除；必须保留原因和原始证据，且不得混入有效聚合。
- 任何“动态优于静态”的结论都必须与同上限、实验开始前选定且运行期间不变的静态点比较，并满足
  预先规定的重复次数与阈值。

具体运行、落盘和报告要求见 [`AGENTS.md`](AGENTS.md)；实验计划导航见
[`plans/README.md`](plans/README.md)。
