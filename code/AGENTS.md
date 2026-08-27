# code/AGENTS.md

本文件继承根 `AGENTS.md`，只增加 `code/` 的实现规则。目录与当前接口见 `README.md`，已实现/未实现
边界见 `INFRA_STATUS.md`，脚本入口见 `scripts/README.md`。

## 1. 目录职责

- `src/` 保存可复用实现；`scripts/` 保存配置、运行、服务、采集和分析入口；`tests/` 保存验证。
- 实验计划与结果分别进入 `experiments/plans/`、`experiments/results/`；动机和可行性结果进入各自
  `results/`；正式绘图脚本进入 `figures/scripts/`。
- `code_doc/` 是历史设计/实施记录，不是当前接口或待办来源。
- 原始数据、临时 CSV、一次性 notebook 和运行产物不进入 `code/`。

## 2. 模块边界

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
- 新 backend 保留同合同旧 backend 作为对照或回退。生产 runner 不反向 import profiling 脚本。
- PostgreSQL/LOTUS 语义层与外部物理 backend 分开：LOTUS prompt/output/error parity 由 operator
  adapter 验证，scheduler 不重定义语义。

## 3. 请求与流式语义

- 每行对应一个完整、独立的模型请求。token-budget 只组织行间 batch，不把单行 prompt 拆成多个
  vLLM 请求；超长行由预处理截断、独占 batch 或按 workload 规则排除。
- 正式 image runner 不在 driver 上全量 `to_arrow()`、`list(to_arrow_iter())` 或等价 collect；只有写明
  规模上限的 smoke/profile 可以 materialize。
- image 路径必须固定输入表示、decode/resize/normalize 归属、model/processor revision、dtype、输出
  维度、projection、normalization 与服务端隐藏 batching；baseline 间不得悄悄改变这些条件。
- 写回前校验行 ID、shape、finite、exactly-once；retry/cancel 不得破坏数据库结果语义。

## 4. Baseline 与可比性

- 正式 baseline 直接使用官方 benchmark、内置 AI Function 或官方 native API graph，并让被测系统
  拥有 batching、backpressure 和 task/actor scheduling。
- 项目 adapter 只统一 source、sink、质量与观测；不得注入项目 credit、inflight、router 或重写执行器。
- 自写 UDF 可以提供 workload kernel；自写执行图只能命名为 `diagnostic_reference`。
- Python、Ray、Daft 和服务 baseline 共享输入、输出、计时、失败和写回合同，不共享项目策略实现。

## 5. 代码质量

- 每个模块/CLI 用简短 docstring 写清目标、输入、输出和通过条件；计划来源指向对应实验或实现计划。
- 函数、类和文件保持单一职责；配置、阶段执行、扫描编排和输出分离。重复逻辑稳定出现后再抽象。
- 自适应策略先实现最小、可解释的静态/单步规则；只有实验显示必要性后才增加状态和参数。
- 非平凡机制注明论文/官方来源；没有外部依据时明确写“工程决策”，不虚构文献支持。
- 不顺手重构无关模块；改动若产生新的孤儿 import、配置或路径，在同一变更中清理。

## 6. 观测、测试与完成条件

- runner 按根实验规则记录版本、配置签名、阶段时间、工作量、失败/重试、资源、质量和 provenance；
  具体指标合同从 `experiments/plans/baseline_reference.md` 读取，不在本文件复制。
- 新代码至少有针对其合同的最小测试；修 bug 先用测试或最小复现使问题可观察，再修复。
- 先运行受影响的最小测试，再运行相关 suite 和语法/静态检查；缺依赖时记录 `pending`，按 runtime
  规则处理，不在当前环境直接混装依赖。
- 实现状态变化时同步 `INFRA_STATUS.md` 和实验证据台账；CLI/路径变化时同步
  `scripts/README.md`、目录 README、测试和调用方。
- 代码或实验事实变化若影响教学材料，再同步 `learning/`；不因单纯格式修改机械改写学习文档。
