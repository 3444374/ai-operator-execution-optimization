# SemLoom 实现状态

本页只维护当前能力、可选路径和未验证项。接口与命令见[代码入口](README.md)、
[配置总览](../docs/执行配置与优化开关.md)；逐次运行的版本、完整结果与失败见
[证据台账](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)及所属报告。
CPU、软件替身、真实 IPC、真实 Ray、模型功能和性能分别解释，不把旧版本通过数累加为当前整树结果。

Ray 完整文本查询新增[显式原生 processor 接点](src/baselines/README.md#ray-native-prepared-sembench)：
完整预测物化后、关闭前执行原 parser/关系并记录内存 sink；旧 `query_seconds` 不追认为统一口径。
`ac4440328` 已完成 Q10 full2000、Q8 120、Q9 Map256 的原生模型、实际 token、原 sink 和物理清理审计；
共享 `48a6b42d0` 题号绑定的 Ray Q1/Q2/Q3 也已完成 2000/120/120 次真实运行，2240 条终态、0 条 unknown。
Q9 Map 不授予 TopK 资格。当前整合保留原生执行与完整 sink，合并后的源码仅做受影响 CPU 回归，
不把这些冻结来源的成绩改记为整合分支实测，也不称全部 baseline 或全部题的统一计时已完成。

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

Daft 原生 AI 的[单 F/M/E 完整查询入口](src/baselines/README.md#daft-native-prepared-sembench)
已从历史私有接线整理到正式源码，保留分离依赖和原生执行器，并将完整原 sink 与模型关闭分开计时。
`ffbc352ac` 已完成 Q3a25/Q4 24 的真实单冷资格；最初的 main 整合保留相同 Daft 执行代码，
原整合22项CPU回归通过；后续sink计时修正的19项检查与Q1/Q3a/Q4完整软件回放见[报告](../experiments/results/diagnostics/daft_native_semantics_20261110/README.md)。旧真实时长含部分审计，不能追认为修正后的统一口径。
Movie Q1/Q2/Q3 已开放正式单算子查询绑定，复用同一 Daft native AI、原 parser 与完整 sink；
`48a6b42d0` 已完成 2000/120/120 次真实单冷及逐条审计，完整 Filter 后的 head(5)／COUNT、
原提示、重复 occurrence 和全阴性 schema 由[原查询回归](tests/experiments/test_sembench_text_original.py)保留。
其余题按各自来源判断；同 owner 暖轮仍未取得资格，不把旧版本真实记录称为整合候选实测。

LOTUS 的[显式 resident 文本入口](src/baselines/README.md#lotus-native-resident-sembench)复用原生stage、
模型owner及既有生命周期，默认isolated保持；当前源码/输入/模型绑定通过配置声明。
124个不同CPU检查及Q3a50/Q4 48条完整软件冷暖回放通过，e097目标环境导入/绑定及CPU监督通过；准确范围按[报告](../experiments/results/diagnostics/lotus_mmqa_q3a_native_20261110/README.md)分列；
已完成的Q3a50次真实冷暖仍绑定ee625/fd06，不自动成为新正式候选的模型结果。
完成后的 session 报告按实际大小读取，配置上限与符号链接／非有限数拒绝保持；
`50eae2e04` 已完成 Movie Q10 冷暖各2000次及离线审计。当前合并不重跑模型。
Movie Q9 的[显式作者 TopK 候选](src/baselines/README.md#lotus-movie-q9-topk)复用 resident owner，
保留原 quick、K=N 与比较预算，和 Q9 Map 分开验证；`c9cadb1df` 的 8 occurrence 样本实际用了 20 次比较，
未超过 28 次上限，不授予完整 256 项排序资格。

LOTUS 原视觉查询与 MMQA Q5 聚合现于原完整关系返回时记录内存 sink，source 包括本地 LM／tokenizer
准备，旧 `query_seconds` 保持。5 项受控时钟检查与 17 项原生 SDK 软件检查通过；Q5 原 sf25 资产已回放，
但使用软件 tokenizer／生成，不能据此确定目标模型的分层请求数。视觉原查询的新增完整回放缺本地
Animals manifest／SQL，目标环境验证仍待完成。

[Kalypso 原生客户端](src/baselines/text/frameworks/kalypso_native/README.md#内存结果边界)新增完整原关系
就绪时间，公共报告从真实 `query_start_ns` 计算 `source_to_sink_seconds`；trace 审计、结果编码、
评价和证据持久化在该终点后。旧 `answer_ready_s` 保留，缺少新接点的路径明确留空。
共享接点的 29 项 CPU 检查与公共时钟／生命周期的 42 项检查通过；后置失败仍保留 sink 并报告失败。
现有 Q10 Map 与 Q3b–g 私有入口已给出新副本迁移补丁；4000 条 Map 保存答复和 12×25 条 MMQA
归档响应的完整结果／评价回放相等。Q8/Q9 Map、Q3a/Q6a–c、Medical Q1/Q4 的迁移又通过 16 轮保存
响应回放与 18 个失败边界检查。原 Map 最坏 2N、Filter N 的实际请求额度保持。作者 `05024f0` 的
Linux 原环境已通过零模型 native import／request object 门检，CUDA 未初始化；此前失败来自门检
导入次序，按既有服务入口修正后通过，不是作者依赖缺失。新运行绑定及真实 sink／清理仍待验证，
该门检也不授予作者原版结构化 Extract 资格。Q9 本批独立全输入 hash 未知，原 occurrence／必要 CSV／结果相等，
不能以其他输入身份补齐该未知项。这些 CPU 结果不是新 GPU 或性能资格。

[IMLane VARCHAR 资格入口](integrations/imlane_duckdb/README.md)明确区分作者 `072f8257`、可选
schema-child 和 varchar-owned 工程变体。连同[完整原关系交接](src/experiments/native_imlane_relation.py)，
原接线的 29 项不同 CPU 检查及 14 份保存 sink 关系回放通过。作者原版 Linux 零模型 15 例实际为
4 过、11 失败：NULL、offset、长字符串借用和 PROCESS 的根 schema 错配已复现；THREAD 字符串
SQL 实际返回 0 行，不能仅归因于字符串内容。新 varchar-owned 候选处理 child schema、有效位、
offset 及字符串复制；原头文件语法检查、真实 Arrow C Data 与显式存储替身的 4 项检查通过，
真实 DuckDB DSO 和 PROCESS／THREAD SQL 仍待资格，不能预先称 THREAD 行数问题已解决。
schema-child 仅保留独立候选身份，尚无该变体的目标资格。原版 TINYINT Filter／DOUBLE Map 的已有
真实结果不受影响；不能用工程变体或 Python fixture 宣称原版 Extract 已支持。新交接要求实际 DuckDB/result
等待完整 sink ACK 后退出；目标原生生命周期尚待验证。联合 150 行的六题批次只有一个批次时钟，
不能拆成六个独立查询时长。

[Ray Animals Q7](src/baselines/image/ray_animals_q7/README.md)现保留第一 stage 的完整关闭，最终原 City
关系就绪后才 ACK 并关闭最后 runtime。原 19 项 CPU、真实本地子进程交接及 128 条旧答复身份／原 parser
回放通过；`909f842c4` 又通过目标 64 张原图／128 条保存答复的完整 City／evaluator 回放，并在独立
冷轮完成 128 条真实终态、原 City P/R/F1 = 1 和物理清理。此前因本任务未登记的 Python 观察进程
被 guard 阻断的 64 条失败轮单独保留；改用 shell 观察，未放宽 guard，也不拼接两轮成绩。
后续失败诊断与 EXIT 状态修正仅有 CPU 资格，不把 `909` 的模型结果改记到新 HEAD。
不能从公共 runtime ACK 推断内部模型 actor 驻留或 kernel 时间。

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
