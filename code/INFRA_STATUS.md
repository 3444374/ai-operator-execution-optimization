# SemLoom 实现状态

本页只维护当前能力、可选路径和未验证项。接口与命令见[代码入口](README.md)、
[配置总览](../docs/执行配置与优化开关.md)；逐次运行的版本、完整结果与失败见
[证据台账](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)及所属报告。
CPU、软件替身、真实 IPC、真实 Ray、模型功能和性能分别解释，不把旧版本通过数累加为当前整树结果。

[2026-11-12 实现与方案核查](../experiments/results/diagnostics/semloom_execution_candidates_20261110/README.md#implementation-review-20261112)
区分各 PR 的窄实现与未完成项：完整查询候选接线、实际未缓存工作／成本输入、动态消费者与多层表示、
真实位置路由及 #101 短期限具名资源回收仍未完成；负结果候选按停止条件处理。

<a id="native-gpu-decision-observation"></a>
本分支继续增加显式就绪工作选择、排队／传输／重建成本路由、有界提前准备与原生引擎可选参数。
[执行候选报告](../experiments/results/diagnostics/semloom_execution_candidates_20261110/README.md)记录 Q8／Q9 布局、
真实照片 CPU 传输、竞争留存和提前准备；原生 encoder 提示仍为 CPU 状态机探针。
Q8 已完成 960 次实机请求与原评价；该提示变体减少本次请求耗时但质量更差，不作为默认采用。
GPU 阶段关联、实际配置和未完成项以该报告为准。
后续修正了提前准备取消顺序、CPU 传输超时收尾和布局原对照的重复格式化；
第二轮审查补齐失败后关闭期间到达的回执，提交／接收／确认／未接收分别记账，见
[审查和验证](../experiments/results/diagnostics/semloom_execution_candidates_20261110/README.md#second-code-review-20261111)。
[固定 PR 审查收尾](../experiments/results/diagnostics/semloom_execution_candidates_20261110/README.md#pr100-review-closeout)
记录 CPU 验证范围、历史计时偏差及仍不支持的结论。

本分支增加 vLLM 0.25.1 v1／v2 的被动批决策观察与离线诊断，支持实际成员、graph／padding 及采集缺项核对；
v1 另有 cascade 接点，当前 v2 没有该分支。[代码与 CPU 验证](../experiments/results/diagnostics/native_gpu_decisions_20261110/README.md)
说明初始范围；Q8 的实机 off/on、业务请求别名关联与采集到的 kernel 费用见上述执行候选报告。
完整语义链、其他模型／任务及稳定优化收益尚未验证。

本分支另有默认关闭的完整请求排序、图片 URI 留存选择，以及表示寿命／供给等待诊断。
[实现与本地证据](../experiments/results/diagnostics/semloom_mechanism_prototypes_20261110/README.md)
区分 Core 接线、原请求离线回放、实际 LOTUS 图片准备、CPU 检查与准确 b1d965f 的 MMF GPU 对照。
[原配置服务器回执](../experiments/results/diagnostics/semloom_mechanism_prototypes_20261110/README.md#server-qualification-20261111)
保留严格 token 46/48 与该 head 的 v1 决策缺项，不转授后继配置资格。完整查询收益、encoder／KV 留存及动态实际工作供给策略尚未验证或实现。

<a id="current-refactor-status"></a>
<a id="refactor-completion-checklist"></a>
## 当前能力与待处理项

| 部分 | 当前实现 | 未完成或未验证 |
|---|---|---|
| 文本语义与查询 | Filter／Map／Extract 保留原提示、解析、顺序和关系屏障；原生系统各自拥有执行与调度 | records 的 Join／TopK 未接入；完整查询与质量按各路径报告判断 |
| 普通与有限链执行 | 普通 Filter、融合批和逐记录查询各有 owner；接纳、poll、ACK、取消和结果交付使用现有实现 | 不同组合的真实模型与最终清理尚未全部覆盖 |
| 容量与存储 | 请求额度、在途 work、输入／结果字节、驻留与未消费结果分别记账；省略值从声明解析 | 没有自动取得物理 RAM 或完整查询内存的能力；逻辑额度不是硬件容量证明 |
| 模型与期限 | vLLM 可选参数省略时交引擎解析；整查询默认不设总期限，显式值保留；启动、通信与关闭分别计时 | 真实在途到期／abort 和重复关闭仍须匹配后端验证 |
| 准备与反馈 | 无 B、rolling、交错／事件驱动准备及显式请求反馈可按支持条件组合 | 动态 B／C 联合控制未实现；功能检查不证明吞吐改善 |
| 常驻查询 | 模型 owner 可复用；内部反馈逐查询绑定，用户显式配置保持 | 同一 owner 连续 Extract 的真实 SDK／模型验证仍见对应报告，不外推到任意并发查询 |
| 基础多 Job | Session 保留固定份额、普通轮转和统一资源责任；共享策略通过显式研究适配接入 | 已有模型五请求中一条 unknown 的失败保留；不据此声明完整多查询组合通过 |
| 图像 | 文件图像、LOTUS 检索及已有阶段执行分别维护；公共 CLIP 组件不依赖 PG 数据工具 | 软件替身不证明真实 Ray／模型进程退出，增量图像与阶段执行不互相替代 |
| 观测 | 查询时钟、事件、模型复用、资源采样和额度由各自模块维护 | 事件重叠、API 断开和引用释放不等于物理并发、进程退出或性能收益 |
| 原生对照 | LOTUS、Ray、Daft、IMLane、Kalypso 保留各自原生图、作者服务或方法职责 | 输入、生成、缓存、生命周期和实际版本匹配后才能比较；不转授其他路径的验证结果 |

<a id="pr52-g1-qualified-scope"></a>
<a id="record-local-sections-c827"></a>
<a id="record-local-completion-progression"></a>
## 查询路径

- 默认 fused 与可选 `record_local_sections` 使用不同编排；后者可显式选择同一异步后端上的完成推进。
- 原生 baseline 不能注入 Core 调度；完整查询保留原关系处理、评价与工作量。
- 容量、结果关联及逐行方法的接口见[执行适配](src/execution_provider/adapters/README.md)、
  [LOTUS 方法](src/semantic_methods/lotus/README.md)与[原生对照](src/baselines/README.md)。

<a id="records-runtime-modules-cpu"></a>
<a id="records-config-refactor-cpu"></a>
<a id="records-model-context-cpu"></a>
<a id="resource-allocation-candidate"></a>
<a id="pressure-sample-cpu-fix"></a>
<a id="finite-query-token-candidate"></a>
## 容量、上下文与反馈

C／R／work 与存储预算的具体取值和支持组合由[容量类型](src/execution_provider/adapters/records_capacity.py)及
[配置总览](../docs/执行配置与优化开关.md)维护。未消费结果、unknown 和晚到请求继续占据其实际资源责任。
模型 ready 返回的真实上下文用于长度核对，不能以配置省略推断上下文无限。
宿主内存与 cgroup 指标是观测，不等于资源授予。

<a id="prepared-request-batching"></a>
<a id="async-record-token-batches"></a>
<a id="batch-supply-feedback"></a>
<a id="finite-query-successor-candidate"></a>
## 可选组织与提交

| 选项 | 使用条件与说明 |
|---|---|
| 跨读取块 token 组批 | 由实际组织器处理完整请求，不切分单行提示；同步与异步链分别核对配置 |
| token 在途额度 | 仅在已支持路径显式启用；不能按其他算子的检查推断 Extract 支持 |
| 固定 B 的批反馈 | 调整已支持的批并发，保留原批大小和容量声明；不等于动态调整 B |
| 后继优先与等待老化 | 使用既有方法 driver 和 ready 时钟；没有竞争的运行不能证明选择器收益 |

具体参数、默认与组合要求从[执行适配](src/execution_provider/adapters/README.md)和
[配置总览](../docs/执行配置与优化开关.md)核对，不从旧候选记录复制。

<a id="model-query-reuse-scope"></a>
## 模型复用与多查询

模型常驻、仅 runtime 复用和原生单线程 session 是不同运行方式，分别记录生命周期与查询时间。
records resident 的顺序复用不能直接解释为共享 owner 上完整查询并发。
单查询也可能包含多个 flow，Job／flow 注册、容量、结果与取消责任都应保留。
研究接入与普通 Session 的职责见[多任务说明](src/research/multitask/README.md)。

<a id="record-results-migration"></a>
<a id="shared-chain-ipc-scope"></a>
<a id="本地目录与依赖整理尚未发布"></a>
<a id="候选交接状态2026-10-27"></a>
<a id="优化与观测进展2026-10-27"></a>
<a id="current-runtime-cleanup-composition"></a>
## 模块与清理责任

[模块职责](README.md#核心文件职责)区分方法状态、查询 owner、传输、同步／异步引擎、结果关联与观测。
清理沿原持有者执行；未完成 RPC、未消费结果和 unknown 不因调用返回或窗口改变而提前释放。
实际共享链与 IPC 检查从[CPU 指南](tests/records_cpu.md)及[主报告](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md)定位。

<a id="current-image-status"></a>
## 图像与未接入能力

当前文件图像与 LOTUS 检索按各自输入表示和方法核对。阶段执行与指标累计分工维护，
不将准备进程、工作量或资源采样的软件检查扩展成多卡或真实模型能力。
跨模型任意依赖图、自动物理 RAM 授予和完整消费阶段内存计量仍未实现。

<a id="legacy-request-retirement"></a>
<a id="http-call-configuration-cpu"></a>
<a id="physical-probe-policy-cpu"></a>
## 不再支持的入口

旧 SemLoom fixed HTTP、PG 实验入口及旧请求反馈配置已退出当前运行路径。
现在的 `resource_feedback` 与 `batch_feedback` 分别按现有解析器使用，不能恢复旧具名 profile 或提示缓存。
需要回查旧实验时使用[已有复现说明](../experiments/reproduction/native_baselines/README.md)，不添加兼容执行路线。

## 证据与仍需验证的事项

| 问题 | 事实入口与适用范围 |
|---|---|
| 当前重构及取消／unknown／资源清理 | [重构主报告](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md)记录逐批准确源码和未解决项；不同版本不合并为一次通过 |
| 常驻 Extract 反馈 | [3 项 CPU 检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#extract-resident-feedback-20261109)含真实 CPU IPC，Ray 与语义使用替身；模型复验单列 |
| Ray 原生进程退出 | [三类关闭路径](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#ray-close-three-paths)分别核对 Core RPC、自有 UDF 与官方 Processor；驱动断开不证明物理退出 |
| 原生 Movie Q3 | [原生 Q3 报告](../experiments/results/diagnostics/native_sembench_q3_20261106/README.md)保留输出差异、质量与资源结果；不据此给新组合性能排名 |
| 旧关闭故障与负结果 | 原 1596 第二项关闭失败及模型 unknown 等仍按主报告保存；后来检查通过不解释旧故障原因 |

没有运行回执的能力保留为 pending。正式模型比较使用对应计划、实际环境和准确版本，不能从本页推断已运行。
