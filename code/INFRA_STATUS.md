# SemLoom 实现状态

本页只维护当前能力、可选路径和未验证项。接口与命令见[代码入口](README.md)、
[配置总览](../docs/执行配置与优化开关.md)；逐次运行的版本、完整结果与失败见
[证据台账](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)及所属报告。
CPU、软件替身、真实 IPC、真实 Ray、模型功能和性能分别解释，不把旧版本通过数累加为当前整树结果。

[隔离组合报告](../experiments/results/diagnostics/baseline_validated_integration_20261111/README.md)记录 `2e2616a5` 与 `6a31cc38` 的独立组合、限定 CPU 回归及仍缺资产的检查；
候选尚未合入 main。最新组件目标状态来自协调器报告，按原提交分列，不转授组合树模型资格。

Ray 完整文本查询新增[显式原生 processor 接点](src/baselines/README.md#ray-native-prepared-sembench)：
完整预测物化后、关闭前执行原 parser/关系并记录内存 sink；旧 `query_seconds` 不追认为统一口径。
`ac4440328` 已完成 Q10 full2000、Q8 120、Q9 Map256 的原生模型、实际 token、原 sink 和物理清理审计；
共享 `48a6b42d0` 题号绑定的 Ray Q1/Q2/Q3 也已完成 2000/120/120 次真实运行，2240 条终态、0 条 unknown。
Q9 Map 不授予 TopK 资格。当前整合保留原生执行与完整 sink，合并后的源码仅做受影响 CPU 回归，
不把这些冻结来源的成绩改记为整合分支实测，也不称全部 baseline 或全部题的统一计时已完成。

<a id="current-refactor-status"></a>
<a id="refactor-completion-checklist"></a>
## 当前能力与待处理项

IMLane Filter 不再用历史输入行数作为配置白名单；完整原 occurrence 数仍须等于显式冻结预算。
现有 `freeze-config` 零模型入口从原题准备生成同 schema 的 SHA、实际 occurrence 数和首次观察的
原始列顺序；正式 Filter 复用共享 prepare 消费冻结顺序，不再重复 formatter 接线。
生成配置不是 GPU 运行授权；本地原源码／SDK／tokenizer 检查不转授 Linux SQL 或模型资格。
prepared Q1/Filter 的薄 driver 构建身份补齐源文件及编入的 ACK header，旧记录保留历史资格，
当前重用需重链薄 driver 补齐身份。详见[边界说明](integrations/imlane_duckdb/README.md#filter-源规模与构建身份)。
这不改变原生算法、采样、源列顺序、时钟、DSO 或运行额度；新目标 native SQL 验证仍待执行。

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

Movie Q5/Q6/Q7 的[共享原生 Join 候选](src/baselines/README.md#lotus-movie-exact-join)复用上述 resident，
每个原始有序 pair 独立关联和扣预算，原 parser、完整 Join 与后置筛选/限制不变。
[首批 CPU 报告](../experiments/results/diagnostics/lotus_movie_join_native_20261111/README.md)
只覆盖每题 8×8 软件答复回放及 sink 前后边界；没有新的模型、GPU 或完整 256×256 资格。

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
导入次序，按既有服务入口修正后通过，不是作者依赖缺失。各题目标 sink／清理按上述隔离组合报告和准确原件分别核对，
该门检也不授予作者原版结构化 Extract 资格。Q9 本批独立全输入 hash 未知，原 occurrence／必要 CSV／结果相等，
不能以其他输入身份补齐该未知项。这些 CPU 结果不是新 GPU 或性能资格。

[Kalypso Movie Q9 Map 维护接线](src/baselines/text/frameworks/kalypso_native/README.md#movie-q9-map-维护版接线)
已进入原 query/server/CPU CLI，保留完整 256 发生项、原评分／排序／评价和作者 length→8192
重试，物理上界为每题 512；不是 TopK。新增本地完整保存输出／token／时钟回放通过，但不继承
旧模型成绩。204faa 的目标 CPU 预检暴露作者环境无 pandas 的边界错误，修正版把原查询准备
放回 driver Python，作者 CPU CLI 仅消费摘要绑定的完整输入；不安装或混用环境。协调器已报告 `2e2616a5` 的 Q9 目标冷暖及逐条审计通过，
同源 Q1–Q4 冷暖共4720次也由协调器报告完成，但 Filter runtime 完整token数组未采集。
这些结果不转记当前组合源码，逐题来源见上述报告。当前输入完整 SHA 独立绑定，
不补写旧 Q9 缺少 full-source hash 的历史未知项。

[IMLane VARCHAR 资格入口](integrations/imlane_duckdb/README.md)明确区分作者 `072f8257`、可选
schema-child 和 varchar-owned 工程变体。连同[完整原关系交接](src/experiments/native_imlane_relation.py)，
原接线的 29 项不同 CPU 检查及 14 份保存 sink 关系回放通过。作者原版 Linux 零模型 15 例实际为
4 过、11 失败：NULL、offset、长字符串借用和 PROCESS 的根 schema 错配已复现；THREAD 字符串
SQL 实际返回 0 行，不能仅归因于字符串内容。新 varchar-owned 候选处理 child schema、有效位、
offset 及字符串复制；`0f562cf0` 已完成真实 Linux DSO／converter／PROCESS／THREAD 15/15 零模型
检查，该工程变体的 THREAD strings／null／offset 各 3 行、empty 0 行、ownership 1 行均正确，
不改记作者原版的失败。新增[MMQA Q1 接线](integrations/imlane_duckdb/README.md#mmqa-q1-工程版接线)
复用该 DSO、原 JSON prompt/parser、完整关系 sink 和既有 owner；25 条保存答复的完整消息/token、
关系／评价、25/26 物理预算及双时钟 CPU 检查通过。协调器已报告 `73ee255e` 的 Q1 目标25次及完整 sink 完成，结果仍绑定原提交。
schema-child 仅保留独立候选身份，尚无该变体的目标资格。原版 TINYINT Filter／DOUBLE Map 的已有
真实结果不受影响；不能用工程变体或 Python fixture 宣称原版 Extract 已支持。新交接要求实际 DuckDB/result
等待完整 sink ACK 后退出；逐题目标状态见上述报告，Q3e 外层 guard 失败保留。联合 150 行的六题批次只有一个批次时钟，
不能拆成六个独立查询时长。


[IMLane Medical Q1](integrations/imlane_duckdb/README.md#medical-q1-原版-filter-接线)已最小接入同一
TINYINT Filter／owner／ACK，原 patient_id Series、parser 和 evaluator 不变。完整历史
cold/warm 各1200保存答复经真实 CPU Arrow／Unix IPC 回放，历史 CPU 完整token数组逐条相等；
原 GPU 仅长度不升级资格。冻结源码 `70a153810cbe901ae3d4ee24c9af5cbe9c966c5c` 已独立完成
Linux 原DSO/SQL门及一次冷1200，实际完整消息/token逐条一致；结果40行，precision=0.5、
recall=0.4、F1=0.4444444444444445，hardcap1200、warmup0、retry0。原件与审计定位见
[Medical Q1说明](integrations/imlane_duckdb/README.md#medical-q1-原版-filter-接线)。
这不授予新组合目标资格；Q9维护CLI的独立Linux零模型门与原件本机核验已通过；
当前组合未作整体目标／GPU执行。

[IMLane Medical Q4](integrations/imlane_duckdb/README.md#medical-q4-原版-filter-接线) 复用原版
TINYINT 单题入口，1200 个发生项进入完整原 join/filter/mean 内存 sink 后才 ACK；回执通过
batch position 与本地 occurrence ID 关联 43 个重复消息组，不改变模型请求。历史 cold/warm
2400 条保存答复的真实 Arrow／Unix IPC、原完整消息、CPU token 数组、关系／评价与物理预算
回放通过，后置失败不隐藏，cold 与 ready 起点独立保留。77 项受影响 CPU 测试通过；协调器另报告 `6a31cc38` 的冷轮1200次及完整 sink
通过，来源与当前组合 CPU 分列，不改记历史质量／时长。

[IMLane Movie Q1–Q4](integrations/imlane_duckdb/README.md#movie-q1q4-原版-filter-接线) 已接入现有
单题 TINYINT owner／provider／完整 sink ACK：Q1 2000 次，Q2–Q4 每题 120 次，首个目标门
只选 Q2。76 项局部 CPU 测试与 2600 条 Movie 保存回复回放通过；Q3 的 120 条来源是旧
`168bc` project resident bridge，不是作者默认。共享 Medical 2400 条保存回复回归通过。
各题保留原 source、prompt/parser、head/count/ratio/evaluator 和独立 cold／ready 实际时钟；
上述 CPU 回放不授予目标资格；`5c69f037` 的 Q1–Q4 后续均已独立完成目标 SQL 与真实冷轮。

Movie Filter 的 `9507` 目标 Q2 尝试在旧 native CLI 的25/1200白名单处失败，generate=0、
无 sink，资源已清理。现修复为必填正 int64 与实际 count／DISTINCT 一致，无枚举或默认25；
当前 driver 源 SHA 在模型启动前校验，必须重链薄 driver，DSO/kernel不变。生产参数函数
实际 C++ 编译执行和旧driver前置拒绝检查通过；新增软件服务的真实子进程／Unix IPC 120条
完整 token／逐发生项答复本机通过，最终80项 CPU 测试通过。`5c69f037` 的 Q2 已完成原版 DSO
新薄 driver 的 Linux 全120行／缺行／重复ID三门，以及真实冷120次、完整原 sink、逐发生项
运行时 token 和资源清理。后续 Q1 全2000条、Q3/Q4 各120条也分别完成原生 SQL、真实冷轮
及完整 token／发生项核对；Q3/Q4 的原评价误差保留，不写成全题质量通过。单一测试入口现按
原配置绑定逐题数量，仅两份已完成测试回执按自身大小读取；生产与全局读限未改。
来源、旧失败、质量结果与单冷轮边界见上述模块说明，不扩展为稳定性能排名。

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

### Movie Q9 原DOUBLE Map维护入口

[唯一CLI](src/experiments/native_imlane_movie_scores.py)已收口原Q9薄driver/UDF、source/audit与完整内存sink接线；
单cold256永久预算，原DSO不变。机器资源控制器与冻结数据仍在仓库外。
当前迁移的资格范围及复现配置见[集成说明](integrations/imlane_duckdb/README.md)。
本机软件IPC/原256消息-token/关系回放与目标模型运行分开记录。`4c08b57b` 维护CLI已独立
通过Linux完整SQL256／零模型及实际加载原52bf DSO检查，1173份原件已回收核验；
其运行token是软件placeholder，不是新GPU token资格。当前组合未作整体目标／GPU执行。
