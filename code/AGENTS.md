# code/AGENTS.md

本文件继承根 `AGENTS.md`，只增加 `code/` 的实现规则。目录与当前接口见 `README.md`，实现状态见 `INFRA_STATUS.md`，脚本入口见 `scripts/README.md`。

## 1. 放置与入口

可复用实现、脚本和测试按 README 的目录职责放置；原始数据、临时 CSV、一次性 notebook 和运行
产物留在对应数据、结果或临时目录。`docs/design/history/` 仅作历史参考，当前接口从源码和代码 README 核对。

## 2. 模块职责

新增实现沿以下依赖方向组织：

```text
data source/materializer
  -> modality adapter + work/cost description
  -> organization
  -> admission/routing/runtime
  -> serving backend
  -> fan-in + sink
  -> observability/experiment adapter
```

- `planning/`、`scheduling/` 只消费 typed contract，不直接依赖 Daft、Arrow、psycopg、vLLM 或某个
  图像库；供应商 API 放在 data/modality/serving/baseline adapter。
- 新增或重构的公共策略使用中性 `work_units`/descriptor；迁移期间保留现有 token 字段兼容层。文本
  token、图像 frame/pixel/prepare cost 在模态 adapter 中换算；模态不支持的 locality/length 能力
  必须显式声明，不静默退化。
- 新 backend 保留遵循同一协议的旧 backend 作为对照或回退。生产 runner 不反向 import profiling 脚本。
- PostgreSQL 语义层与外部物理 backend 分开：数据库 plan/task/result 协议定义默认语义，scheduler
  不重定义 prompt、output parser 或关系行为；LOTUS parity 只由可选 compatibility adapter 验证。
- 为自有方法向公司系统移植保持职责可分：可独立表达的算子策略、prompt/parser 与 work 计算留在
  相应语义 Module，PG 的 SQL/Plan/Datum/slot 与生命周期适配留在 carrier；provider 接通不等于
  planner 已支持算子优化。实际移植差异由对应 Adapter 处理，不提前建立跨数据库万能框架。
- 新的系统所有权接口使用 `SemLoom`；DB-AIEL 只作架构层名称。PostgreSQL、provider protocol、
  planning、scheduling、serving 等可替换 module 使用领域角色命名。既有 `project_*` schema/arm 和
  `Project*` import 只作兼容入口，不复制到新接口，也不重写历史 evidence。

## 3. 请求与流式语义

- 在一个已确定的生成阶段内，每行对应一个完整、独立的模型请求。token-budget 只组织行间 batch，
  不把单行 prompt 拆成多个 vLLM 请求。经算子方法明确授权的多个阶段可以分别产生请求；
  阶段转换由方法决定，调度器不能借此擅自改写语义。超长行处理由对应算子语义或 workload 规范明确规定；不从批处理策略推导截断权限。
  生成型 SemMap 发送完整请求，超限按其错误规则处理；只有输入规范明确允许时才预处理截断或排除。
- 正式 image runner 不在 driver 上全量 `to_arrow()`、`list(to_arrow_iter())` 或等价 collect；只有写明
  规模上限的 smoke/profile 可以 materialize。
- image 路径必须固定输入表示、decode/resize/normalize 归属、model/processor revision、dtype、输出
  维度、projection、normalization 与服务端隐藏 batching；baseline 间不得悄悄改变这些条件。
- 写回前校验行 ID、shape、finite、exactly-once；retry/cancel 不得破坏数据库结果语义。

## 4. Baseline 与可比性

- 正式 baseline 直接使用官方 benchmark、内置 AI Function 或官方 native API graph，并让被测系统
  拥有 batching、backpressure 和 task/actor scheduling。
- SemLoom adapter 只统一 source、sink、质量与观测；不得向 baseline 注入 SemLoom credit、inflight、
  router 或重写执行器。
- 自写 UDF 可以提供 workload kernel；自写执行图只能命名为 `diagnostic_reference`。
- Python、Ray、Daft 和服务 baseline 共享输入、输出、计时、失败和写回要求，不共享 SemLoom 策略实现。

## 5. 代码质量

- 新增算子、方法或 PG 接入切片时，按[架构计划](../docs/plans/postgresql_ai_semantic_operator_architecture_20260827.md)
  §8.7–8.8 核对受影响的工程参照；公司移植使用其中的完整对照。普通缺陷修复、内部重构和测试维护
  以受影响源码、接口与既有切片记录为入口；改变职责、语义或接入设计时再补读对应架构章节。
- 工程参照的版本/符号、已核对行为、采用或保留决定、自有落点与验证用例写入工程计划或切片记录，
  不重复放入代码和测试注释。已读且未变化的对照直接复用；来源不可访问时标明未核对项，按公开
  依据继续不受影响的自有工作。公司材料的复用与发布仍遵守根规则。
- 重构默认保持外部可观察行为：公共 import/API、CLI 参数、默认值、退出码、输出 schema/字段语义、
  已承诺的错误规则、事务与 cancel/retry/exactly-once 语义及指标口径。任务明确要求改变行为时，将
  行为变更与结构重构拆成可独立审阅和验证的变更。
- 生产代码和测试按职责与可验证性组织，不以统一行数阈值拆分。编排保留清楚的生命周期，阶段细节
  按领域动作命名；名称与常量说明角色、单位和归属，协议值留在 adapter 中。
- 仅对语义和变化原因一致的逻辑提炼复用；配置按 policy、capacity、request、sink 等内聚概念组成
  typed、自校验对象。设计模式只解决已识别的替换需求或耦合，并能用行为测试验证。
- 测试 helper/fixture 保持最小作用域；从用例名称和失败信息能判断破坏了哪项行为要求。
- 自适应策略先实现最小、可解释的静态/单步规则；只有实验显示必要性后才增加状态和参数。
- 非平凡机制在上述工程记录中注明论文/官方来源；没有外部依据时明确写“工程决策”，不虚构文献支持。
- 不顺手重构无关模块；改动若产生新的孤儿 import、配置或路径，在同一变更中清理。

## 6. 观测、测试与完成条件

- runner 按根实验规则记录版本、配置签名、阶段时间、工作量、失败/重试、资源、质量和 provenance；
  具体指标定义从 `../docs/plans/baseline_reference.md` 读取，不在本文件复制。
- 行为变化用最小测试/复现验证；重构需有现有测试或稳定输出覆盖需保留的行为，缺少时补行为表征。
  格式、注释等低影响修改不新写同义测试；共享接口、协议和执行层变化须覆盖受影响的旧路径。
- 检查通过后，仅因新改动、失败或未解决问题才扩大或重跑；本地验证的持续执行按根 §4。
  缺依赖时记录 `pending` 并按 runtime 规则处理，不在当前环境混装依赖。
- 实现状态变化时同步 `INFRA_STATUS.md` 和实验证据台账；CLI/路径变化时同步
  `scripts/README.md`、目录 README、测试和调用方。
- 代码或实验事实变化若影响教学材料，再同步 `learning/`；不因单纯格式修改机械改写学习文档。
