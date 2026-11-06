> 本开发组合另含旧PG／HTTP实验归档与原生LOTUS本地IPC；
> [清理CPU报告](../experiments/results/diagnostics/runtime_entry_cleanup_20261105/README.md)区分实际实现、未运行项及历史资格。

# SemLoom 实现状态

本页记录所处开发分支的实现与尚未验证范围。源码与接口从[代码入口](README.md)核对，
逐次运行、源码身份、校验值和失败由[证据台账](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)与主报告维护。
Git 接纳、CPU 接线、模型功能、质量及性能分别判断；较早切片文字保留其对应版本的状态。

<a id="current-refactor-status"></a>
## 当前收尾状态（2026-11-06）

<a id="refactor-completion-checklist"></a>
### 整体完成清单与收尾事项

下表覆盖当前支持的records单查询执行及保留的对照。Core职责整理、已确认旧HTTP观测胶水和提示／观测脚本修正已完成，分别保留真实验证身份；不据此宣称全仓历史内容或新增数据库接入均已完成。
ACK指结果已交接的确认，IPC指进程间通信，RPC指远程调用；未消费结果和未知远端执行继续分别保留责任。

| 模块 | 已整理 | 仍需处理或验证 |
|---|---|---|
| 配置与容量 | 中性规范、一次解析、未指定参数保留；普通 async／rolling 的 C／R／结果槽／字节分别负责；5539实际ready解析已记录 | 低层直接链与非 rolling 整批仍用其显式有限声明；物理 RAM 自动授予未实现 |
| Session 与查询推进 | Session、owner、数据运行器分工；普通、prepared／rolling、resident、fused关闭入口均已反查；5539两路径16条模型功能及关闭通过 | f4e原两项IPC各通过一次；真实模型重复close、取消／unknown注入未覆盖 |
| 模型与 IPC | sync／async构造分开，父进程／worker／帧编码分工，序列化使用定义模块；回执在actor释放前交接 | 5539的八符号Ray worker往返及原五项Future／取消检查均通过；原第二项历史错误原因仍未解明 |
| 算子与结果 | 共享LOTUS方法、F／M／E、原数据、记录身份、完整Extract、未消费结果和unknown责任 | 现有方法行为保持；Join／TopK等未接入能力不列为已完成 |
| 研究隔离 | SAOR／多任务实现独立，核心只消费可选中性协议 | 独立拆仓可后续做，不让研究配置进入单查询默认路径 |
| 旧入口与脚本 | 旧PG／HTTP实验、旧动态变体、重复导出和旧ACK重放退出当前入口；旧choice观测六文件删除，共享函数迁回预算模块；低层异步actor三种旧阻塞RPC及专用等待也已删除 | 27项纯内存通过；87ce实际IPC为15通过、1项测试同步失败，修正后该单项在d6abc一次通过；现用owner、同步run_core、批反馈、provider和研究能力保留 |
| 观测与验证 | 来源／时钟、真实压力与声明额度分开；1cfd修正单请求观测漏记和提示检查空通过；b422跨查询实际CPU IPC单项通过 | 原第二项历史错误原因未解明；模型功能通过不代表gold全正确或调度提速 |

关闭修正以实际返回回执登记结果可见性，并在释放actor前恢复结果、排空ACK；异常或中断仍保存unknown及真实RPC终态。
原79项本地回归属于28ba；准确f4e服务器两项IPC各通过一次，清理和运行许可归还确认，真实Ray／模型均为零。原1596首例ACK拒绝有异常链，第二例旧原因仍未解明，当前通过不改写旧记录。
新的5539组合只增改两份测试，保留448e的Medical／MMQA准备；12项本地选择为11通过、1真实Ray项跳过，1,063份摘要匹配准确源码，不累计旧运行。
合并后静态核查988份Python、3,328处项目内导入，未发现指向缺失模块的静态引用；动态导入和可选外部依赖不在该结论内。
准确5539在Ray2.56.1的六项检查全部通过，八个生产符号完成worker往返，21个已观察进程清理和许可归还确认，模型调用为零。
1cfd将单请求handoff探针迁到实际非阻塞owner，修正观测器对awaitable的处理；单／批开关和原结果／异常／取消传播5项通过。b422的增强CPU IPC单项随后通过，核对168次合成引擎提交、14个查询的完整行与消息，无模型或Ray。
准确5539普通async与record-local各8条模型功能全部成功、0失败／unknown，无预热或重放；输入／输出token IDs一致，但各有3条漏判，不能称全部gold通过。五项引擎可选参数省略后实际解析值已记录，未启用动态反馈，不作性能结论。
旧观测清理d5已整合为284ec，69个不同CPU方法全部通过，含五份3010文件历史恢复；1,057份源码摘要匹配，982份Python／3,304处本地静态导入无缺模块。7项进程／崩溃方式未选，不计为跳过或通过。
旧观测清理组合284ec的核心57,011行、src＋scripts114,290行，本批分别为不变／减少514行。随后保留远端e0ec的原生基线与IMLane接线，整合2aa的相同69项全部通过；1,067份code摘要匹配，991份Python／3,336处静态引用无缺模块，原3010份历史文件摘要保持。
最新组合核心仍57,011行，src＋scripts114,637行；相对284ec新增347行属于并行基线工作。较3fd分别减少4,617／35,008行，较main核心仍增加2,920行、宽口径减少25,698行。未把拆文件或并行新能力算成本批减少。
随后eab7e246退役低层异步actor三种阻塞RPC、两个专用执行辅助与一层分发，非阻塞接纳、poll／ACK、unknown和reader责任保持。旧钩子两项报错用例删除，原8efed的两项空事件IndexError保留；旧忙轮询检查随其生产循环删除，未用测试helper的sleep替代生产证据。
本批27项不同纯内存方法通过，1,067份code摘要匹配，16项实际CPU IPC未运行；受影响10条历史来源仍与原摘要一致，不改原3010映射，也不重跑五树。当前核心56,943行、src＋scripts114,569行，本批各减68行、测试减30行；较3fd分别少4,685／35,076行，较main为核心多2,852行、宽口径少25,766行。详情见[旧异步RPC退役](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#retired-blocking-async-rpcs)。
后续准确87ce的16项实际CPU IPC已各执行一次：15通过、1失败，无错误或跳过；独立一次诊断仍失败并捕获预发送取消。原因是reader用例迁移后只等应用排队就触发close，未等其原定unknown回执，不是reader保护提前放行。
d6abc0e3只增加两行测试同步，先观察现有IPC结果就绪再触发close，不消费结果、不释放reader、不改生产及10项原断言；该单项在准确d6abc一次通过，原unknown、最终关闭及usage11断言均完成。其余15项不重跑，继续按87ce身份解释；原失败和缺失的完整close／retired_unknown字段保留，不补推测值。此次两个登记进程退出、SHM无残留、许可已归还，真实Ray／模型为零。
没有明确重复、职责混杂或正确性问题的长函数可以保留。自动物理容量、额外算子和独立研究拆仓是尚未实现或后续工作，不为完成本轮重构临时新增控制层。
[整体收尾核对与待验范围](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#core-closeout-verification)记录最新状态；
[关闭修正及原失败](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#close-audit-ack-handoff)保留当时记录；
[此前历史检查退役](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#retired-ack-source-replay)仍只描述删除范围。PR继续为草稿，不合并main。

此前一次解析切片代码为 `91fb08d1`，已保留并行基线提交 `295065c6`：普通 owner 只解析一次执行配置，直接构造时的阶段计时选项与动态反馈报告一致；公共工厂的最终报告与实际计时行为保持。
链 owner 删除未使用的普通 profile 解析及已由类型构造器完成的重复检查，直接保存不可变分包配置；资源、模型与可变 token 预算的归一化和复制保持。
[原反例、31项相关回归及整合后42项检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#single-pass-records-configuration)
按该批准确源码记录，未运行真实IPC、Ray或模型；原两项关闭错误的后续调查见上方清单。

上一批核心代码为 `9de3db29`：删除八处假模块身份赋值，类型／函数及容量值的新序列化使用实际定义模块，不再加载单Filter查询host；现有公共导入和同对象关系保持。
关闭finalizer失败保留原异常原因，外层消息及stop／join、重试和unknown责任沿原规则；源读取的已确认记录类型去掉一轮临时describe字典复制。
[源码与69项回归](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#actual-adapter-modules-and-close-causes)
记录原9项中的3个反例、修改后9项和相关69项通过，1053源码摘要匹配及五份3010文件恢复。
原两项实际IPC关闭错误仍待服务器查看完整异常链后定位；本批没有验证真实Ray序列化、模型或性能。

上一批核心重构代码为 `b50d0b1b`，保留已发布 `5849807c` 的原查询与图像准备接线。
共享反馈配置、供给状态和成对计量移至各自文件；现代请求路径不再加载批反馈实现。
退役无现用构造点的旧平滑／队首等待控制器及其专属导出／测试，现用批策略、加性增加和乘性降低基类及观察字段保持。
三个内部记录快照使用既有构造器直接复制，不先生成临时describe字典；外部JSON恢复保持。
[模块职责与组合证据](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#feedback-duties-and-record-copies)
记录准确328项纯对象／fixture通过、完整历史恢复和原390选择中的两项IPC关闭错误；两项尚未定位为实现或夹具问题，不被新结果覆盖。

普通单查询不以新增独占RAM管理器为完成条件：继续由部署给有限额度，已有Core／方法／IPC和模型分别负责接纳、数据、结果与KV。
当前auto解析已有声明，cgroup／PSI只提供观察；真正物理RAM自动授予与完整查询内存计量仍未实现，有实际管理需求时另行实施。

SemBench 补充接线从 PR90 `0677c7a3` 独立推进：普通 Movie task 增加 Q4，保持其原提示、
完整 eligible Filter 和 positivity ratio；Q1/Q2/Q3 的入口、默认与输出不变。
Movie 原查询／评价适配另接 Q8、Q9-map、Q10；Q10 必须提供原 `Movies.csv.audienceScore` 金标，
不能用评论情感或模型评分代替。Map 的执行接线另走已存在的记录方法，不扩大普通 Filter 类型。
MMQA 的十二道文本查询、各变体原 JSON 评价和原文本生成方法见
[MMQA 适配](src/baselines/text/sembench_mmqa.py)。现成原资产可在 CPU 生成 sf25，
三张文本表各25行、Q4 原预筛24行，每个 baseline 完整十二题预计299次调用；不生成图像或下载。
本轮仍为 CPU 准备范围，真实模型、作者启动、质量及性能不因此通过；现有测试入口保留。

[新增文本入口](scripts/experiments/run_sembench_text_query.py)现将十六道原查询的单个 F/M/E 接到
原生 LOTUS、已有的 Ray／Daft 有限记录图和 IMLane 作者链；未重写执行器或引入 Core。
真实 LOTUS1.2.4／LiteLLM1.95.1／OpenAI2.50.0 的离线 CPU 差分覆盖原查询、共享方法和
原 accessor 内存传输的完整结果、提示与顺序；十六道 `check/fixture` CLI 及原评价也已运行。
这是本地 CPU 软件组合，不是服务器 Linux LiteLLM1.95.0 或真实模型资格。
真实 MMQA sf25 的 Q6 包含一个缺失目的地；DEFAULT 逐单元文本表示在原数据保留 NaN 的同时
产生与原 SDK 相同的 `nan` 消息，未填值或删行。输入、限制及图像准备见
[SemBench 扩展范围](src/baselines/README.md#additional-sembench-text)。

Daft 补充文本图现先配置 Native runner、再创建 source DataFrame，修复显式 `runner_threads` 的启动顺序错误；
省略参数继续使用 Daft 原生默认，不修改已有 runner 或原生执行图。
[原生 LOTUS 视觉运行](scripts/experiments/run_sembench_lotus_image_query.py)已接原 LM SDK 到独立 VLM 服务，
保留原 PNG／Join／JSON Extract、既有持久预算和查询子进程监督；实际模型与服务生命周期仍待服务器验证。
该入口不启动或停止模型服务，既有 `check_sembench_image_query.py` 保持零模型预检职责。
与5a19221b合并后的修复组合310项CPU通过，包含真实Daft0.7.21和12项原生视觉SDK/软件HTTP集成；原3010历史绑定匹配。
这仍是本地CPU资格，实际Linux软件组合、VLM加载、质量及物理服务清理待服务器验证。

MMQA Q2a/Q2b/Q7 另修复原 `"zs-cot"` 字符串在 LOTUS1.2.4 枚举接口静默进入非 CoT 提示的问题：
仅在独立查询调用边界映射为 `ReasoningStrategy.ZS_COT`，保留原 Join／Extract／解析与评价。
Q2b 另在原零行 Extract 返回边界保留声明列，修复空 Join 后最终投影缺 `color` 的错误；
非空 `{}` 缺字段仍失败，Join、Extract 的调用与请求计数不变。
66项相关离线CPU检查通过、无跳过，覆盖原生策略、HTTP 消息、零行 schema／零次 Extract POST、离线评价和正常／异常恢复。
依据 `d061df8c` 保存的零行调用记录重建空表，原评价器离线 F1=0；原失败及失败耗时保留，不补成功端到端计时。
上述CPU验证未访问网络、加载模型或操作服务器。现象与修复见[基线说明](src/baselines/README.md#sembench-image-preparation)，修复版本的真实服务调用仍待验证。

[Medical Q1／Q4／Q10](src/baselines/README.md#sembench-medical-text)新增原 CSV、原查询和原评价接线，
复用四种既有普通 F/M/E 文本 baseline；准备仅复制已有三份作者 CSV，不运行图音 setup 或重采样。
完整11112名患者资产有1200条症状，三题每 baseline 共3600次；原 sf25 只有1条症状，不按患者数猜调用量。
原评价源码需要 Python 3.12。补充 Movie／MMQA／Medical 生成缺项均沿用原8192 token／无 stop，显式值保持；
旧补充 Movie／MMQA 的省略项解析已修正，公共类型与旧普通 Filter 默认不变，历史结果不改写。
[MMQA Q5 聚合](src/baselines/README.md#sembench-mmqa-aggregation)另走原生 LOTUS 整查询服务入口，
保持原分层 `sem_agg`，不放进单个 F/M/E 的记录边界。sf25 原筛选16行，正文12403个 UTF-8 字节；
仅首层单个请求时总调用精确为1，多层总量依赖真实摘要长度，按既有有限预算逐 POST 消费。
首层预检使用声明的本地 tokenizer 和原 SDK 计数，服务端 tokenizer／上下文、真实模型及外部物理清理仍待机器核查。
本批不启动 GPU；Movie 大 Join／TopK 不执行，其他 baseline 的聚合及 Kalypso Medical 留空。
组合172个不同CPU方法最终通过；首轮两个真实数据用例未给环境路径而跳过，补齐路径后单独通过，首轮记录保留。
原生服务抽取后的14项视觉方法另复核通过；Medical 12组 query／backend 的 check＋fixture 通过，
共14400次固定软件回复，完整1200行的原 accessor／记录／原生传输逐消息与结果相同。
Q5 的单层与多层原算法、实际生成上限及预算失败保留均通过软件 HTTP 验证；软件 tokenizer 的单请求结果不替代实际服务分块。
上述CPU验证的网络／真实引擎／实际推理／服务器操作均为0，原3010历史绑定匹配。

### 直接原生Movie Q3的关闭与环境修复（2026-11-06）

随后67a4各执行一次120条：[复测与IPC诊断](../experiments/results/diagnostics/native_sembench_q3_20261106/README.md#retest-and-ray-ipc)
记录Daft正常收尾，Ray查询完成但原SHM失败；Ray随后人工回收不改原失败，两组均保留七个FN，不作稳定性能排名。
完整actor日志确认Ray正常GC时杀死了本次EngineCore，之后仍有完整MPClient关闭回执；信号量创建者尚缺栈证据。
现有IMLane作者源码／DBEnd-C/C++库与Arrow16.1可定位，但作者DuckDB未有已核对的构建产物，原生Q3接线待CPU资格。
当前下一步为零模型CPU生命周期隔离，未改第三方库、安装或追加推理。

[首测与修复记录](../experiments/results/diagnostics/native_sembench_q3_20261106/README.md)保留e0ec的120条Daft输出及进程收尾失败；
模型质量仍为COUNT7／gold14，不作正常完整JCT。独立单查询进程以公共vLLM工厂／shutdown作用域关闭实际引擎，
READY前后的预测／终态分开保存；新监督入口按实际READY启动120秒收尾，含8秒强制终止，查询窗口1200秒不重复扣减。
真实Daft执行器＋CPU模型替身复现原退出等待并验证修复，物理GPU／后代进程退出仍由外层确认；不宣称模型常驻。
Ray2.56.1／vLLM0.25.1的现有环境通过守卫CPU API审计，发布extra契约仍不满足；真实actor／CUDA／推理未验。
本批只读环境核查、未安装软件或启动GPU；复测入口和有限规模见原校准计划。

### 原生基线参数与直接 AI 准备（2026-11-06）

普通Ray图将模型构造后的真实ready沿首个成功批次传回 `backend_close.engine_ready`，
不从配置猜实际容量；空源／未启动仍保持未知。普通Daft及隔离session透传显式 `runner_threads`，
省略时沿原生线程默认；专门的单线程profile保持独立。普通Ray不再静默忽略非空session profile。
同步模型owner、单GPU actor和fused图实际支持范围保留，未声称所有baseline已经充分调优。

IMLane桥的旧16行块、4096行／调用、64查询上限改为有限workload／model预算推导，
Filter、packed链、cascade和resident保持各自wire计数；显式pending、128MiB作者Lane及SHM／RSS／字节边界保持。
正常caller与echo fixture中的重复旧门槛已清理。实际作者桥二进制必须重建；本地C++只验证标准组件，
没有构建／运行真实作者bridge。六份准确旧源补30条历史映射，五份完整树实际还原、3010条全部匹配，旧预期与投影身份未改。

[直接原生AI Movie Q3](src/baselines/README.md#direct-native-movie-q3)另准备Daft原生prompt和Ray Data LLM公开processor，
共享中立任务／质量合同而不加载LOTUS／Core执行。真实LOTUS消息／解析、真实Daft表达式及普通CPU关系、
Ray API替身覆盖完整cohort筛选、一次物化和计数；原2000行CSV用120条金标软件回复通过原评价。
这些不是实际模型质量／性能结果。DuckDB＋作者IMLane的native构建、函数alias和Arrow结果转换尚未资格，
不把records桥或普通pip DuckDB算作该完整路径。模型依赖、实际重试／token与物理退出仍待机器验证。
具体参数归属及有限测试条件见[配置说明](../docs/执行配置与优化开关.md#native-baseline-controls)与
[校准计划](../experiments/plans/原生基线校准.md)，本批未启动GPU或服务器实验。
合并最新并行修改后的相关检查去重249个方法最终通过，无未解决失败／跳过；首轮三项验证器的PYTHONPATH／
CPU替身拦截设置问题，仅修正验证器后补跑三项，生产源码摘要保持。标准C++边界包含在其中。
已知私有子进程context用例的macOS进程枚举权限问题未修、未重跑，不计通过；原失败保留。
新原生AI仍没有真实Ray／vLLM／作者桥资格；本地软件通过不转授物理GPU退出或性能结论。

[图像检查入口](scripts/experiments/check_sembench_image_query.py)另覆盖原 Animals Q1/Q3/Q7/Q10 与 MMQA Q2a/Q2b/Q7。
原 ImageArray／PNG 消息、Filter／完整 Join→Extract 与原评价只做 CPU 软件回复验证；
原生 VLM 服务、实际 processor／JSON Extract 和物理关闭尚未实机验证，不能以 CLIP 或旧 HTTP 结果补齐。
Movie Q9 新有限链明确使用原 Map，不替换原 TopK；精确 Join／TopK 预算与缺失数据分开见
[逐项限制](src/baselines/README.md#sembench-image-preparation)。本批没有运行模型或 GPU。

当前组合保留 `8a2bf329` 的原生基线容量修正，以及此前 worker／时钟整理和历史来源修复。
HTTP 代理退役代码为 `7a23e6dc`，旧 Ray 轮询退役整合代码为 `a8c7dd87`；此前配置组合代码为 `6ac1fe56`。
上一批代码为 `015c3095`，已保留 `08c3ebf5` 的原查询接线与数值精度修正：旧 IMLane 桥按显式请求数配置并在 ready 回执中核对，正常 Movie、chain 和 cascade 入口已迁移；
IPC 父进程管理、子进程循环与共享帧编码分属各自文件，现用导入和观测接点同步。
查询验证表逐条放开已读取的旧记录引用，保留原比较时点；记录反序列化删除外层重复复制。
[组合源码与检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#runtime-structure-and-bridge-capacity)
记录最新组合381项 CPU 通过和准确历史来源；真实 IPC、完整作者桥与模型尚未验证，物理 RAM 授予未实现。
仅移除旧转发代理、其专属回环测试和冷导入项，共享 trace 摘要、HTTPX、request identity、Unix 执行接口保留。
代理退役切片的 11 项 CPU 通过，独立复核无需修正；代码仍在开发 PR，未合并 main。
[具体改动、剩余文件与证据](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#retired-http-observation)
区分旧组件选择、最新组合及实际模型结果，没有累计重叠检查。

原十处历史来源差异已由并行基底补齐，本次 3,010 项来源全部匹配；旧失败按原版本保存，未运行历史模型环境。
已核对的完整模型 Q1 属于 `a1b5cdd`：2,000 条成功，但与旧动态结果有一条 Filter 判断差异；不覆盖当前新组合。
本轮没有启动真实 Ray、模型、GPU 或服务器。原 socket／sysctl 与缺少 SemBench checkout 的用例仍未在这里验证。

普通 Movie／Session 的未组批 async 及已支持 rolling 入口，省略 R 现按 None 解析已有有限额度；显式 R 保持原规则，缺额度在启动前拒绝。
SDK 全局 R 也已改为 None，删除 Movie 按后端补值及不可变资源对象的重复检查；静态、非 rolling prepared、直接链／cascade 仍需其支持的有限声明，缺来源先拒绝再启动。
底层 IPC 删除隐式请求数 8；总字节省略时按声明请求／结果槽及单项大小计算，显式额度保持。JSON 有显式 max_inflight 即可提供请求数，无须重复填九字段。
auto 仍依据声明额度，不是物理 RAM 授予或整查询 RSS 保证；IMLane 桥不再内部补两块在途，必须传入有限声明并重建实际桥程序。
五项可选引擎参数均可省略；dtype／显存比例不再补 BF16／0.8，显式值原样保留，ready 读取实际原生配置。
此前参数组合通过 108 项；本次准确 6ac1 组合 317 项 CPU 全通过，含五份共 3,010 文件完整历史恢复。真实 vLLM、模型质量及物理容量尚未验证。
[配置修正及剩余项](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#sdk-ipc-capacity-followup)区分入口、声明、观察和真实资源。
剩余大文件按实际职责继续整理。旧 Ray 字典 HTTP metrics 轮询及其独占轮转状态已退役，
static／全局与端点 admission 对象继续使用原调度器；不含 admission 对象的旧字典在提交前直接报错。
[同组选例与准确历史来源](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#retired-ray-http-polling)
记录旧实现参照 27 项和最终 29 项通过，没有为文档链接改动重复运行。

原生 owner 启动修正基于 PR90 `4f409aa4`：通用观测控制器为 IMLane 传递既有 host 的
RSS／SHM 声明、GPU idle allowance 与本次 affinity 数量，CPU 准备和真实启动复用同一接线。
字段确有资源采样／退出核验消费者，没有填旧 echo／8 CPU 缺省或新增容量门槛。
Kalypso 外置辅助的 reader、writer、脱敏及延迟报告读取改指当前公共层；仍被使用的
资源观察器按原 AST 迁到中性观测目录。真实辅助的独立副本已通过禁止 Core、退役模块、
模型库、子进程与网络的 CPU bootstrap 检查；原私有文件保持。补丁与新绑定要求见
[外置辅助迁移](src/baselines/text/frameworks/kalypso_native/README.md#外置-owner-辅助的退役引用迁移)。
相关 CPU 回归 202 项、98 个 subtest 通过，无失败／跳过；历史 3010 条绑定匹配、五套完整恢复通过。
新增观察器及其测试 Ruff 通过，三个既有修改文件的 186 条旧诊断无新增；五份 Markdown 本地链接通过。
本轮不启动 GPU，不改作者算法或既有结果；实际作者环境、模型初始化及余下两项 SemBench 仍待服务器验证。

原生 baseline 容量修正基于 PR90 `4376d927`：ReferenceVllmEngine 原先把未指定的三项 vLLM
容量补成 4096 上下文／8 序列／4096 token，链、cascade 与 IMLane 还会重写上下文或据此提前拒绝。
当前删除隐式补值和无人使用的 legacy 属性／覆盖点；省略／null 保留至引擎构造，显式正整数原样透传。
就绪后仍核对实际上下文，原生 `LLM.chat`、LOTUS 客户端批大小和框架组批职责保持；
实现与检查入口见[参数归属](src/baselines/README.md#共享层和参数)。
下方早期切片中“原生参照保留原参数”只描述当时版本；历史冻结源码及结果身份不因此重写。
CPU 扩大回归为 819 成功、15 跳过、1 失败；失败是 ACK 历史派生方法的 AST 摘要检查，
在未修改的 `4376d927` 单独复跑同样失败，本轮未修改该方法、夹具或断言。
跳过项为原生 Daft 图像 SDK 5 项、未指定 SemBench checkout 3 项、processor 专项 1 项及未指定固定 vLLM 源码 6 项。
历史 3010 条绑定匹配，五套完整 materialize 通过；28 份修改的 Python 文件无新增 Ruff 诊断，原有 73 条保持。
本轮只做 CPU 验证，实际 vLLM 解析容量、服务器模型可用性、SemBench 质量与性能仍待实机确认。

本批精简 handoff 观测：删除 poll 的重复 Future 适配／完成订阅，成员通知、driver 交接和 Core key
时点复用原有有界缓冲，不再在该模式逐成员扫描全量 usage。事件保留入队时钟／phase，缺失 usage
记为未知；full 模式保留详细采集，关闭仍核对 drain、fsync、错误与丢弃，见[观测说明](scripts/profiling/record_observation/README.md)。
与最新 profiler 归档组合后的 220 个不同 CPU 方法为 215 成功、5 个未通过方法；这五项失败均在准确基底的完整原工作流复现，未删除或放宽断言。
四个单请求／批路径 off/on CPU 探针输出哈希一致，保留分页、ACK、unknown 与关闭责任；真实 Ray、模型和查询收益未测。
Core 的单轮交付预算公平性是另一项执行问题，本批没有改变该预算或调度策略。

原生 baseline 执行依赖审查将有限链无调度 worker 移到 `serving/backends/lotus_chain_worker.py`，
Core 的 prepared batching／复用留在原执行目录的实际子类；详细计时的时钟身份改用公共观测函数，
不再加载 Core transport。五类文本 baseline 的当前入口、共享基础与实机待验范围见
[baseline 依赖审查](src/baselines/README.md#执行依赖审查与可测范围)。
两份仅引用已退役 HTTP／project_static 的测试及其无效路径豁免退出当前树，准确字节与恢复入口保留；
普通 Movie、有限链／cascade 和 CLIP 接线分别验证，不以 CPU fixture 宣称完整 SemBench／模型／性能通过。
最终 CPU 回归 704 成功、9 跳过，无失败；跳过三项未指定原 SemBench checkout 的查询、五项原生 Daft 图像 SDK 检查和一项 processor 专项。
禁止 Core 导入的原生入口／启用计时／完整方法批次检查通过；Core 分组、准备复用、关闭重试和启动报告写入失败仍保留原责任。
历史五树完整 materialize 通过，3010 条绑定匹配；新增时钟旧源及基底已有十处缺失映射均固定准确旧字节，
原 expected SHA／投影身份保持。初次导入拒绝、残留退役测试收集错误与恢复失败按各自原因保留。

SAOR 与通用多任务共享额度已迁入[独立研究包](src/research/multitask/README.md)，
普通 records 默认与核心调度测试不依赖研究策略；原同步调度／shared-vLLM 研究接线保留。
本批不改 Session 或反馈实现，并无冲突组合核心线程的最新拆分；
源码保持核对与 CPU 范围见[整理记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#saor-multitask-isolation)。

本地资源大步候选移除“两份物理样本／只首次大步／后续最多翻倍”限制；按真实许可、ready前沿和合格资源余量试探，
旧慢成员未完成不单独挡住资源试探，性能确认仍基于真实可比完成工作。压力回退保留全部既有责任和查询成本，
静态默认、B和Core／IPC硬预算不变；[规则与CPU边界](src/scheduling/runtime/calibration.README.md#当前资源请求接纳)不代表实机收益。

首帧发布本地候选：异步 IPC 等待实际 scheduler 统计就绪后立即发送第一帧，后续保持原周期；
不制造 idle、零完成或当前 session。CPU socket 检查覆盖首个请求未结束时的统计交付与关闭，
真实 vLLM／Ray 的首帧时刻及早期扩张仍待实机复验，见[采样接点](src/execution_provider/adapters/README.md#snapshot-rpc-ownership)。

LOTUS 语义层候选将固定版本／源码摘要相等检查改为实际安装来源记录；参考 commit 与未知的
安装包 commit 分开，普通绑定、formatter 和 SDK 准备不读取来源摘要。
既有 67 项与新增 4 项来源／执行边界 CPU 检查通过，环境仍为 LOTUS1.2.4／LiteLLM1.95.0／OpenAI2.50.0。
Filter cascade 的原 accessor／学习器私有绑定保留；其他版本、同进程并发 cascade、真实模型与服务器资格仍待验证。
接口与来源字段见[LOTUS 说明](src/semantic_methods/lotus/README.md#来源记录与运行合同)。
后续按记录／阶段合同、单阶段原生方法、链推进及关系组装拆分文件，原 `record_chain` 导入与 JSON 合同保持；
原有 16 个类／函数定义逐项核对后语法树不变，其中 10 个迁入所属模块、6 个留在链模块。
cascade 复用已验证的响应，原校准／抽样／阈值算法保持。
最终相关 CPU 回归为 359 成功、4 跳过，无失败；跳过原生 processor 专项与缺少显式 SemBench checkout 的三项官方查询。
固定／学习 cascade 的完整提示、重复抽样、fallback、决策和原生 LM 用量差分均通过；无效 helper 元数据保留已知用量并失败。
首次消费端检查的六项本地进程权限错误保留；启用进程查看权限后的同范围重跑通过，不把前次错误记作成功。
与 PR90 `1f92fce5` 的并行清理／监控／超时改动整合后无冲突；15 个内部消费者改从实际所属语义模块导入，
并清除五个无调用导入，公共兼容入口仍保留。源码检查未发现旧 LOTUS 哈希常量的当前引用，未改 CLI 或配置字段。
本组合扩大 CPU 回归为 482 成功、4 跳过，无失败；增加取消、campaign 监控、资源观测、探测超时及记录验证接线。
LOTUS 模块 Ruff 通过；15 个消费者保留原有 128 条格式诊断，无新增诊断。上述检查仍不提供真实 Ray／模型／服务器资格。

本地反馈节拍候选修正提前回收后重复占用下一派发 tick；验证后的 provider 新帧复用现有 Core wake，
可在控制轮询期限前由原控制线程消费。策略、默认、准入与未知责任不变；
具体条件与未覆盖范围见[采样与控制接点](src/execution_provider/adapters/README.md#snapshot-rpc-ownership)。
仅 CPU 场景验证，不代表真实 Ray／GPU 延迟或查询收益已经通过。

<a id="legacy-request-retirement"></a>
当前组合以已发布 646216fa 为源：旧 Movie HTTP 与固定 campaign 退役，普通本地 IPC、
原 LOTUS 方法和现用 Matrix 保留；无 B 的准备／请求反馈组合与目录职责整理另见页末本轮记录。
较早段落描述当时切片，不能据此恢复已删除的入口或将 CPU 回执当成真实模型结果。

## 旧请求反馈入口退役（2026-11-05，本地候选）

首个退役候选 `e70d11ff` 基于已发布的 `3fd3887e`；后续组合 `678250ee`，105 项相关 CPU 通过；再接入无重叠的 `9652108d` 租约修复，定向 1 项通过。
当前组合未推送、未合并，等待最终独审。普通、rolling、Movie 与
session 的请求反馈只接受显式 `resource_feedback`，默认固定请求策略保持。旧 `legacy_resource_feedback`、
两个具名 profile、跨查询推荐缓存和专用回调从现用入口删除；共享批反馈与计量继续使用原实现。
[逐文件清单、检查与代码量](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#legacy-request-retirement)
说明本批实际范围，早期段落中的旧入口均按当时版本解释。

空查询不加载模型或开始资源采样，常驻查询清除上一查询的采样来源。合法依赖、原始数据、
未消费结果与未知请求责任保持。本批没有实现物理 RAM 授予，也没有取消模型、IPC 或字节容量。
历史机制及原测试从[准确 Git 来源](../experiments/reproduction/native_baselines/README.md#retired-request-feedback)
独立恢复，不能将旧文件叠加回当前运行器。9da 服务器对照和原生 baseline 未修改。

<a id="records-runtime-modules-cpu"></a>
## Records 执行模块整理与独立预算（2026-11-04）

这一早期批次开始时 PR90 为 bfb；下列数字按该批本地整理候选解释。后续状态见本页最新记录。
上一批完整受测源码 `ce75c7c5`，tree `aa69f857bd9c16626e544d9dd35b613416853b98`。
[主记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md)维护完整职责图、
457 个源码 Python 文件／127 个脚本／22 类模块的复用、重构、退出与待做清单。

已分开配置、容量、纯观测、模型适配、传输及 record-local 查询迭代；现有导入仍指向同一对象，新序列化使用实际定义模块。
迭代器与其余 owner 定义的语法树保持，准备／推进／消费顺序、合法后继及 Extract 关系要求不变。
PID 与通用准入拒绝非法计数／数值，有效公式保持；未接入的 UCB 退出当前模块，准确原件与原用例仍可恢复。
配置预检直接读取声明模块，不加载查询或模型 owner。

完整独立预算现在为省略的 IPC 字段推导有限缺省值，支持部分覆盖，完整显式值保持。
90 组旧默认与 30 组完整显式配置差分为零。普通 `budget=None` 仍从旧 R 推导，尚未全面解耦。
Core／Method／IPC 仍分别记账；未消费结果、unknown、ACK 和 producer 清理责任保持，不推断硬件容量。
旧脚本清理移除一个无调用私有签名 helper，并延迟 PGAI 可选驱动导入；七个待审入口按实际用途登记保留。
两份准确脚本原件新增十条来源映射，原摘要／投影身份不变；3010 条历史绑定仍有原十处不匹配。

最终 956 个不同方法为 955 成功、1 个真实 Ray 资格跳过，无失败或错误，运行中源码保持；
七项 CLI 解析检查成功。首次反例、旧失败与跳过各按原选择范围保存，不以本轮替代早期证据。
[最终摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/followup_final_checks.json)
区分本轮成功与尚未解决的 10 毫秒增长时序问题。模型／真实 Ray／GPU／SQL／服务运行为零。

尚余普通默认 R、查询私有状态依赖及历史脚本逐项整理。
当前资源请求反馈已与准备交错及独立预算接通，保留原 Method／Core／IPC 管理对象，
相关 CPU 范围见[组合检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#prepared-feedback-interleave)。
CPU 证据不证明完整查询性能、真实模型或多卡能力。
下一阶段独立树已按准确 b99 字节导入三个策略文件及三个测试文件，当前 327 项成功，无失败、错误或跳过。
原选择多出的五项 UCB 原型用例已有独立归档恢复；此导入尚未切换公共工厂或容量默认值。
随后分离原生采样器，十五份原方法语法树保持，单 RPC 和回执状态继续复用；最终相关 126 项成功。
初轮模拟时钟位置问题及原输出保留，所有原断言保持。后续源端年龄／实际 ready 归属增量 51 项成功，
公共策略尚未切换；同源重复时间保持，无新增历史来源不匹配。实际服务与公共工厂另批核对。


<a id="records-model-context-cpu"></a>
## 异步上下文解析候选（2026-11-03）

基于已发布 b018 与独立配置提取，异步 SemLoom 未指定／null 的 `max_model_len` 省略传入vLLM，
在原模型 ready 回执绑定实际解析长度；提示加输出上限、通信、工作量与 live cache 身份使用该值。
未知或不匹配值拒绝；没有异步4096影子值。同步／native的旧缺省、历史显式值、数据精度和显存比例保持。
首个上下文切片01297ff仅有CPU证据：去重后372个方法成功，6项既有失败在b018独立复现，22个方法跳过，另1类4方法未运行；
最新方法级错误为0，失败／缺依赖／fixture修正原件保留。尚待独审与统一整合，未推送、合并或运行模型。
后续独立增量已补公共离线校准接线：省略上下文时先复用首个已声明样本的实际owner回执，
在生成前核对每个执行owner；缓存命中也明确记录该确认样本的调用与耗时。显式上下文保留原缓存流程。
cc9eeb2切片选择214个方法，206成功、1个方法跳过，另2类7方法因固定资产缺失未运行，无新用例失败；
缺DuckDB的初轮记录保留，已有完整CPU环境复核后无该错误。独审、真实模型与模型前输入专用token验证仍pending，
不能据此声称完整查询加速。
独审随后发现token wrapper的常驻／独立查询接线及有限IMLane省略值兼容问题，另有本地修订待复审：
通过owner只读`model_ready`取得回执，保留独立查询内的延迟分配，在迭代提交前核对；
IMLane只在有限适配层归一原4096默认，异步委托不变。此修订250项选择中242成功、1方法跳过，另2类7方法未运行；
9项新增反例通过。旧失败、首次fixture问题和独审原件保留，未推送、合并或运行模型。
[修订与证据](../experiments/results/diagnostics/records_config_refactor_20261103/README.md#owner-context-compatibility)
分列真实CPU子进程与尚未验证的原生IMLane／vLLM。
[职责与验收](../experiments/results/diagnostics/records_config_refactor_20261103/README.md#目标到实现的验收)、
[实际接口](src/execution_provider/adapters/README.md#records-model-context)及
[首个切片证据](../experiments/results/diagnostics/records_config_refactor_20261103/raw/context_checks.json)与
[离线接线](../experiments/results/diagnostics/records_config_refactor_20261103/README.md#offline-model-context)记录各自范围。

<a id="records-config-refactor-cpu"></a>
## Records 配置模块整理候选（2026-11-03）

基于 PR #90 的 `b018d88`，普通 Movie 运行器的五个配置 helper 移入共享模块，
Session 直接使用该模块；旧导入名保留。请求准备、接纳、派发、结果消费及 baseline 执行保持原实现。
55组配置的返回、异常、缓存身份及输入可变性逐字节一致；同一187个方法选择中，unittest运行183项，181个方法成功、2个方法跳过，另1个测试类跳过。
沙箱首次两轮因临时 Unix socket 拒绝而报错的记录保留；允许本地 socket 的重跑没有失败。
本候选没有模型、GPU、真实 Ray 或查询耗时资格，仍待独审与统一整合。
来源及适用范围见[主报告](../experiments/results/diagnostics/records_config_refactor_20261103/README.md)；
上下文委托的行为变化另行实现和验证，不以结构整理推定已完成。

<a id="http-call-configuration-cpu"></a>
## 独立 HTTP 请求配置候选（2026-11-03）

本地候选基于 `5ff54bfe`，尚未合入：独立 LOTUS HTTP 接点接受显式 180 秒请求／批／两阶段查询期限，
有限正数、模型匹配、较短调用方期限及未知结果责任保持。fixed HTTP 读取层采用可配置单条响应额度；
native Engine 同步单项额度与显式总预算。PG 现有帧保持 1 MiB，更大配置在创建传输前拒绝。
接口与适用范围见 [HTTP 说明](src/execution_provider/adapters/README.md#python-consumer)和
[LOTUS 接点](src/semantic_methods/lotus/README.md#接口与所有权)。

初次本地 CPU 回归 174 项通过；34 项缺少固定版本 LOTUS、12 项缺少 Daft／Arrow，另 1 项真实 CPU Ray
资格未选择，共跳过 47 项。新增 19 项使用内存 HTTP 响应、SDK 转换替身和真实 Core。
旧同步 HTTP／PG Python 协议亦用回环 fixture 检查。
初次扩大检查中的 records 导入因缺少 `pyarrow` 单列 pending；没有安装依赖或执行数据库写入。
模型、GPU、真实 Ray、数据库运行及性能资格尚未完成；baseline 执行与本地模型启动不变。
源码与证据身份见[台账](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#http-call-configuration-cpu)。

独审后补齐组织化 PG 的自定义准备函数：`rows/work/length` 三种模式均使用配置的逐响应额度，
同时更新任务预留与 typed result work；4 KiB 配置可接纳并交付替身结果，不放大用户额度。
修订本地 CPU 回归 181 项通过、47 项跳过，包含三种模式的准备、接纳与 gateway 交付及历史来源检查。
另用已有 Arrow 环境复核 records，类初始化因缺少 LOTUS 未执行其 4 项；日志保留，不改为通过。
10 份准确旧源由历史投影共享，追加 50 条来源映射，旧摘要、投影标识、版本和已有映射保持。
3010 条绑定重算中本次新增不匹配为 0；基线已有 10 处仍在，完整历史还原检查继续报错并单列。
修订仍待复审，不增加真实 Ray、模型、PG 数据库或性能资格。

<a id="pressure-sample-cpu-fix"></a>
## 压力采样与 rolling 内存配置（本地 CPU 修复）

这一历史候选先将新鲜抢占／内存压力的响应提前，并保持旧任务的资源责任；
后来请求路径已由当前资源策略取代。旧双 50% 试探、`physical_probe_policy` 与具名策略不再接受配置。
[当前采样说明](src/scheduling/runtime/calibration.README.md#pressure-sample-response)、
[压力与记账用例](tests/scheduling/test_resource_pressure_clock.py)及
[公共配置检查](tests/execution_provider/test_records_request_feedback.py)仍维护。
本轮只提供 CPU 行为证据，真实 Ray、模型、GPU 与完整查询性能另行验证。

<a id="physical-probe-policy-cpu"></a>
旧物理试探选项随旧请求入口退役；当时源码、说明和专属测试从
[准确历史来源](../experiments/reproduction/native_baselines/README.md#retired-request-feedback)读取。
实际资源观测、现代许可和独立批反馈没有因此删除。

<a id="resource-allocation-candidate"></a>
资源归属候选以 `5ff54bfe` 为底座独立修改；`9b731ab9` 经独审后修订，当前等待复审。机器观测、SemLoom records 启动声明与校准身份使用有效 CPU 配额；
反馈使用可见 cgroup 的用量／配额，并单独报告主机内存和容器内存余量。
原生反馈通过已注册的具名 worker 方法传递实际单设备 UUID，缺测保留；函数对象 RPC 已移除，默认序列化设置保持。
五份历史校准投影只追加准确旧源的映射，旧哈希与投影 ID 保持。双 50% 策略、请求额度、模型参数与原生 baseline 不变。
本轮仅本地 CPU／替身／真实本地进程通信检查，真实 Ray、模型和 GPU 验证仍 pending。
检查、环境错误与原基线复核见[记录](tests/records_cpu.md#resource-allocation-cpu)。

固定 rolling 路径新增默认关闭的 token 切包旁路实验选项；保留正值 B 配置，
只跳过 B 封包并禁止动态请求反馈组合；旧预算模式C_request=R，显式独立budget分别约束C与硬R。FIFO、兼容性、成员/驻留与字节容量、
独立结果和收尾责任不变，fused/默认路径不变。
[入口及 CPU 回放边界](src/execution_provider/adapters/README.md#fixed-rolling-token-packing-ablation-cpu-experiment-only)
与成员等价的准备/dispatch降开销补丁分开；未进行真实模型/Ray/GPU、完整 SemBench 或性能资格。

资源采样的拒绝前诊断候选在现有单个 snapshot RPC 上保留有界元数据：会话未绑定、native
缓存缺失与 CPU 区间资格分别记录，按本次 Core session 重置计数和最近记录。拒绝数据仍不进入
调度，采样频率、1秒资格窗口、默认关闭行为与策略不变。仅 CPU／本地 Future 覆盖，尚未做
此候选的真实 Ray／模型验收；历史零可采纳视图与未知 CPU 区间不回填为通过。
字段与边界见[观测说明](src/observability/README.md)。

records Core 容量纠偏候选将应用层请求额度与 vLLM 每轮序列上限分开；
显式 async／反馈 owner 继续按调用方 `max_active_batches` 分配原有 Core、actor 与 IPC 额度，
`max_num_seqs` 的显式值保留；本地默认修正候选在未指定／null时让SemLoom异步 Core
省略参数，交 vLLM 0.25.1 自行解析；当前 SemLoom 同步也只转发显式值。旧 Movie LOTUS HTTP 已退役。
显式选择原生同步ReferenceVllmEngine的对照仍保留旧默认8，Kalypso不改。尚无新增真实模型资格；
这不是自动性能调优，不改token/context/memory或外层额度，历史显式设置及原回执保留。CPU 检查覆盖高于引擎每轮
序列上限的外层请求、真实 CPU IPC、取消与已知 usage 回收；没有 GPU 或性能资格。
同一异步 Core 的 `max_num_batched_tokens` 默认修正候选在未指定／null时也省略该参数，
由安装的 vLLM 解析有限预算；显式正值保留，启动回执始终读回实际配置，缺失仍记未知。
普通 Movie admission 原有身份绑定完整 EngineSpec；驻留 ascending 继续绑定实际 native 配置，
未指定与历史显式4096不会匹配同一提示身份，无需新增缓存格式。
当前 SemLoom 同步的未指定 token 预算也由 vLLM 解析；
ReferenceVllmEngine对照保留旧默认4096，Kalypso与历史显式配置不变。
旧 Movie HTTP 及其冷／常驻入口现已删除，原参数省略修订只作为 Git 历史保留，
不再构成当前默认路径。当前本地 IPC 的显式上下文早拒绝和模型实际长度核对保持；
真实 vLLM／模型尚未验证，不据此推断性能或显存容量。
完整记录链的 record-local 分支也经此共享 resolver 检查；其 B1、方法／角色、行／字节及
生命周期检查仍在，fused 默认不变。不代表资源平台已找到；[接口边界](src/execution_provider/adapters/README.md#records-core-可选动态接纳)以实际路径区分。
同一候选继续取消显式动态配置隐含的10秒、剩余32／64条与8次调整截止，默认改为
`exploration_s=None, min_remaining=0, max_changes=None`；原 sync/fixed 默认不变。
两具名配置明确为 legacy 参数对照，正值仍可显式设置；历史源码投影、旧失败与负结果保留。
缺失 pending 反馈不作结束证明；短查询即使未 probe 也标证据不足。这只清理 C 扫描的
无依据截止，没有接入 CPU／GPU 联合资源决策或 B 扫描；当前验证仍限 CPU，不能转授GPU资格。

[PR73](https://github.com/3444374/ai-operator-execution-optimization/pull/73)已合入 main `a421c9d4`，将固定四配置的完整记录查询检查入口纳入Git；机器配置、额度、输入与结果仍在仓外。
复用现有监督函数，不迁入历史原件或改变生产算法／默认。
[入口与私有配置边界](scripts/README.md#通过-git-同步完整记录查询检查)、21项CPU检查及独审不代替当前源码的GPU验收。
原W129零调用失败保留；修复改用排序后最小两项之和减1，并将count与token资格分开，迁移未改生产算法或默认。
main0689底座配合已审现场验收入口完成四臂各48次；两token臂W134均有42个不同调用阻塞后恢复，后继竞争未触发。
W134是刻意触发阻塞／恢复的功能测试值，不是每批 token 目标或推荐运行值；常规限流应为按实际容量确定的宽松安全护栏，防止下游过量排队。
上述交付时Git入口组合只有CPU回归；`check`不加载真实tokenizer。2026-10-29后续当前源码的模型功能复验及严格输出／性能边界见[配置总览的证据索引](../docs/执行配置与优化开关.md#e1)，不将小功能检查推广为稳定性能。

[PR75](https://github.com/3444374/ai-operator-execution-optimization/pull/75)已合入的[同容量检查](scripts/README.md#matched-record-chain-execution)固定fused／逐记录两臂C1、B1和每查询一个新模型，
保留默认四配置、fused容量限制和真实Ray归属；增加owner阶段及worker的Ray启停计时，并拒绝混用资源／模型生命周期的回执。
CPU检查覆盖完整F→M的相同结果／父调用和每查询一次模型创建；模型与Ray RPC为替身，逐记录子进程IPC实际运行。
这不增加跨查询模型复用或纯粒度／性能资格；新增对照尚未部署或GPU验证。

<a id="pr52-g1-qualified-scope"></a>
## Records、有限链与本地模型

[PR52](https://github.com/3444374/ai-operator-execution-optimization/pull/52)已接纳 records/Core 方法底座、
Native／Ray／Daft／IMLane 薄适配、Movie runner、整批有限链及文本本地模型接点。
通用 Filter 要求显式非空 instruction；本地 records 格式器与公共文本引擎拒绝图像输入。
CSV／本地 records 按所选环境能力预检，只有 database／sql-readers 需要数据库配置与实际连接，见[运行手册](../deploy/runtime/README.md)。

模型实测仍绑定 `45d184dfaf0ef3a85ccccc805be7fc38e646b527`，最终 e2bc 来源检查不转授新源码推理资格。
Native resident、Core 同步／显式 async、Ray／Daft 及 Kalypso、IMLane 入口分别按
[G1](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#pr52-g1-negative-records)、
[G3](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#records-g3-small-qualification)与
[短 RPC 补测](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#records-imlane-rpc-qualification)核对；旧失败与 unknown 保留。
这些小样只覆盖两条 eligible 负例，非空有限链、helper cascade、正例质量、完整正式 CLI/sealed packet 与稳定性能仍须另证。
[CPU 资产绑定](tests/records_cpu.md)、[具名历史复现](../experiments/reproduction/native_baselines/README.md)和
[e2bc 零模型部署来源](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#records-e2bc-v0)分别保存接线与来源要求。

其他尝试做到了哪一步、是否正在推进及下一步，统一见下方[优化与观测进展](#优化与观测进展2026-10-27)。

<a id="record-results-migration"></a>
## 共享链结果模块

[PR56](https://github.com/3444374/ai-operator-execution-optimization/pull/56)已将结果身份实现归入
[方法公共模块](src/semantic_methods/lotus/README.md)，旧 import 保留同对象重导出，旧 pickle 可读。
worker 须同时部署新模块和兼容层并冷启动，不混用旧源码。CPU 迁移与历史投影修复不增加模型资格；
逐次检查与来源路由见台账和具名复现入口。

<a id="本地目录与依赖整理尚未发布"></a>
## 目录与依赖整理（PR62、PR64）

[PR62](https://github.com/3444374/ai-operator-execution-optimization/pull/62)已合入公共进程工具、同步模型进程移动和运行依赖整理：进程检查与退出工具在
[`infrastructure/owned_processes.py`](src/infrastructure/owned_processes.py)，同步模型进程及阶段清理在
[`serving/backends/vllm_sync_process.py`](src/serving/backends/vllm_sync_process.py)和
[`stage_process_cleanup.py`](src/serving/backends/stage_process_cleanup.py)。旧导入、旧同步进程命令和已有错误处理保留。

运行所需的记录、计时与请求身份已移到`observability/`，纯请求预算类型移到`infrastructure/request_budget.py`；
17处运行代码对实验实现的依赖已清除。仍有3处明确的旧比较入口兼容导入，不属于默认生产执行。
五份历史复现继续生成原有文件与摘要，不用新实现替换旧实验源码。

无维护执行消费者的纯双阶段`saor_pipeline`原型及`control_benchmark` CPU微基准已退出`code/`，
实现、专属测试和原包导出按准确54ff2324保存在[历史复现来源](../experiments/reproduction/native_baselines/README.md)。
活的stage broker、capacity adapter、records执行与策略默认保留；此清理不构成动态资源闭环或查询性能资格。

PR62整合版本已通过239项CPU检查和4次取消对照；原先受目录权限影响的12项检查也已在原位置通过。
这些检查没有调用模型或GPU，不代替真实查询与性能验证。

[PR64](https://github.com/3444374/ai-operator-execution-optimization/pull/64)已将逐阶段模型进程移到
[`serving/backends/staged_generation_process.py`](src/serving/backends/staged_generation_process.py)，共享链执行移到
[`execution_provider/method_workers/lotus_chain_batch.py`](src/execution_provider/method_workers/lotus_chain_batch.py)。这是 PR64 当时的归属；
当前共享无调度计算位于[`serving/backends/lotus_chain_worker.py`](src/serving/backends/lotus_chain_worker.py)，
Core 子类只拥有自身 prepared batching／复用，原生 Ray／Daft／IMLane 直接使用共享基类。
<a id="shared-chain-ipc-scope"></a>
冻结组合`baa45eb7`的[共享链进程通信测试](tests/serving/test_shared_chain_migration.py)已在Linux单项通过：测试0.447秒，含清理1.253秒，自有进程退出、租约释放，零失败／跳过、零模型、零重试。
PR67整合`563bb65b`复用相同同步IPC组件字节，并通过定向CPU组合检查；这不是新整合整体的Linux执行回执。Mac EPERM原失败保留，不再将该同步组件列为待测缺口。
逐记录查询的接纳与验证范围见[下方说明](#record-local-sections-c827)。

本地清理竞态修正让空闲stop／deadline先通过健康IPC取得真实API关闭回执，再禁止复用；已损坏的通道仍保留unknown及进程清理责任。原先空闲reaper先标记broken会跳过交握，该顺序在beb和b0均可复现。定序Core与同步进程CPU测试覆盖成功前缀、实际关闭回执及坏通道unknown；不据此增加GPU退出或性能资格。

<a id="候选交接状态2026-10-27"></a>
## 优化与观测进展（2026-10-27）

研究目标见[公共物理阶段描述与实测](../experiments/plans/系统架构.md#physical-stage-research-goal)。
下表区分已在 main 的基础、单独保存的尝试及未完成工作；候选标识只用于追溯对应检查，不表示已合入 main。
候选行依据本轮实现、独立检查及运行准备的进展回报；没有实际执行回执时，不填写真实查询或资源结果。
未默认启用、尚未合入或一次对照无净收益，分别表示采用或验证范围，不表示优化方向被淘汰。
固定历史源码用于恢复当时版本；算法继续完善与该版本保持不变分别记录。是否正在运行与是否仍待开发也分开判断。

| 尝试或检查 | 已做／未做 | 是否正在推进 | 依据 | 下一步 |
|---|---|---|---|---|
| 已有图像与文本执行基础 | main 已有图像分阶段执行（HSE）、完整矩阵收集、records 文本记录及整批有限链；不同路径的模型结果分别解释 | 已有实现；后续改进按下列项判断 | [图像执行](src/modalities/image/staged_execution.py)、[矩阵收集](src/semantic_methods/lotus/image_backend.py)、上方 records 说明及下方各路径报告 | 复用已支持的行为；完整矩阵和必要结果不能因省内存被丢弃 |
| 动态参数扫描、查询控制与参数缓存 | 保留`IncreasingSearch`、完整矩阵和公共校准API；已交付工作量扫描与计算工作量／局部配对两种原实现及原参数选项均已接main，普通Movie与session共享显式入口，默认固定。已完成CFDC/main43与ee39/main0689的原参数GPU单轮n1首次／复用 | 两组均保留输出ID差异；当前n1不与历史源码n3合并，不宣称严格等价或稳定收益 | [原配置与验收条件](src/scheduling/runtime/calibration.README.md#原实验配置选项)、[旧短查询实测边界](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#records-dynamic-sad24-621) | 与历史源码、模型、输入、生成、预热、缓存和计时逐项对应；明显差异须诊断，不按期望成绩调参。旧负结果保留，截断搜索不当最优 |
| 逐请求参数、先到先处理与 token 工作量限制 | `49d`已验证W阻塞和完成后恢复；PR63已含预算、FIFO和actor组件。PR71已将同角色完整F/M查询接入显式`token_budget`，Extract和角色切换仍拒绝token模式；C64输出差异和无净收益结果保留 | 已合入main；本轮两token臂各48次成功，W阻塞／恢复均42次 | [本地候选范围](#finite-query-token-candidate)、[原对照及停止原因](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#token-c64-comparability-gate) | 原接口已合并；本轮W功能通过但输出ID不完全相同，默认count保持，Extract的token支持仍待做，不将CPU结果写成模型质量或性能收益 |
| 逐记录有限查询与 Filter／Map 局部归并 | PR63已合入main，允许显式选择`execution="record_local_sections"`；默认fused保持。原c827有17次正常成功及1次取消unknown；本轮main0689底座四配置各48次正常调用完成，失败／取消与性能另计 | 可选功能已合并；性能改善仍待验证 | [PR63及验证边界](https://github.com/3444374/ai-operator-execution-optimization/pull/63) | 后续优化在当前接口上逐项检查；不把功能成功或API重叠当作稳定加速 |
| 减少结果审计对象存活时间 | [PR67](https://github.com/3444374/ai-operator-execution-optimization/pull/67)已合入；Core fused、Ray／Daft有限链Python入口及各Extract阶段支持同一`release_completed_audit`选项，默认False；普通Movie单Filter CLI不经过此worker，record_local_sections明确拒绝开启。成功编码且无失败、取消、unknown或清理错误时只撤掉worker自身列表引用，消费者账本保持 | 可选实现与组合检查完成；不证明RSS或查询收益 | [选项与边界](src/semantic_methods/lotus/README.md#completed-chain-audit-lifetime)、[IPC组件检查](#shared-chain-ipc-scope) | 按真实持有与消费者行为评估，区分Python引用、物理内存和查询性能；不自动开启 |
| 共享链结果关联索引 | 原审七字段完整身份索引由Core fused、Ray／Daft有限链共用，固定、随机差分、碰撞及取消检查通过；普通Movie单Filter和record_local_sections不适用 | 实现与组合检查完成；真实查询收益仍待验证 | [范围与测试](src/semantic_methods/lotus/README.md#finite-chain-result-index)、[共享worker](src/serving/backends/lotus_chain_worker.py) | 保留原异常、顺序和身份规则；关联操作减少不代表完整查询收益 |
| 图像准备分离、字节背压与多工作进程 | main的HSE底层已有独立准备／模型额度、字节broker、ready优先提交；`hse_static`诊断入口仍在。普通LOTUS资源入口拒绝HSE和CPU>1，执行器将worker及阶段深度固定为1 | 已有实现，确有普通入口缺口 | [HSE循环](src/modalities/image/staged_execution.py)、[资源解析](src/modalities/image/resource_budget.py)、[LOTUS图像入口](src/semantic_methods/lotus/image_backend.py) | 动态接入后先恢复单GPU、多CPU准备的显式参数与转发，补CPU及输出回归；保留当前TF32和单GPU检查，不默认开启、不声称多卡或位置优化已验证 |
| 可选图像阶段dispatcher | `StageModelDispatcher`及HSE接入仍在`reference/optional-baselines-image-runtime`，main尚无这两个模块；是可继续维护的可选实现，未成为位置／拷贝代价优化器 | 待按小范围接入核对，不整支合并 | [阶段选择器](https://github.com/3444374/ai-operator-execution-optimization/blob/e9a54e3e43ef9ce9e3b6973530715655028b1da5/code/src/scheduling/runtime/stage_dispatch.py)、[HSE接入](https://github.com/3444374/ai-operator-execution-optimization/blob/e9a54e3e43ef9ce9e3b6973530715655028b1da5/code/src/modalities/image/hse_dispatch.py) | 单独适配显式配置、容量和速率过期合同，保持默认与原生baseline边界；不因为保存在参考分支就淘汰 |
| 私有记录方式的对照 | 原`6a263`准备记录不作为当前运行状态；已有`464`／`416`外层观察，未证明硬件瓶颈 | 已有观察；本页不表示GPU正在运行 | [既有对照的解释边界](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#record-layout-comparison-interpretation) | 复用已有事件，按阶段方案补缺失证据；启动布局、并发与实际工作量不同须单列 |
| 小规模真实事件观测 | 已完成 5 次调用、取得 56 条事件；这不是完整画像，也没有提供新的优化收益结论 | 否，本次观测已完成 | 已有 5 调用／56 事件回报 | 复用已有事件，按实际缺失项补充，不把调用成功当成瓶颈已定位 |
| 字节限制与阶段预算组合对照 | 14 项 CPU 检查仅覆盖参考实现，尚未使用真实执行接口 | 否，当前为参考实现 | 本轮 14 项 CPU 检查回报 | 公共阶段具备真实接口后，按同一查询验证这一简单对照，不能把模拟结果当原生系统结果 |
| 普通输入准备 | 已有`TaskPreparation`接口，普通准备尚未使用它；静态／原生路径的准备批次与Core显式async仍在，Core的B1限制不能只删检查就解除 | 已有接口、实际准备接入仍缺；不是配置被废弃 | [接口定义](src/scheduling/core/session_contract.py)、[Core入口](src/execution_provider/adapters/lotus_records_core.py) | 在已有输入／结果身份和准备输出容量合同上实现，核取消、持有和归还；本轮不把大批次或物理释放写成已支持 |
| 最终结果消费的字节预算 | 执行程序仍积累 `raw` 结果，尚未把消费期间持有的数据纳入统一字节预算 | 否，当前缺项 | 本轮完整查询与内存检查回报 | 记录真实消费者和最后使用时刻，纳入字节预算并验证慢消费；模型完成不能代替结果已消费 |
| 公共物理阶段描述与实测 | 已有外层事件和资源观察，公共阶段描述与完整时间线尚未齐备；画像只作支撑 | 仍需补齐阶段观测，不是已经定位硬件瓶颈 | [架构中的目标](../experiments/plans/系统架构.md#physical-stage-research-goal)、[观测方案](../experiments/plans/数据组织.md#dag-resource-observation) | 先选一个完整查询补实际缺项，不预设瓶颈或自动向别的链迁移 |
| 后继优先、反序与等待老化 | PR72已恢复9231f088原driver及ready时钟修正，并透传完整F/M查询选项。原两轮约10%变慢及输出差异保留 | 已合入main；两后继臂各48次正常完成，均无竞争选择 | [本地候选](#finite-query-successor-candidate)、[原对照与负结果](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#priority-reverse-small-pair) | 功能正常不等于优先效果；本轮未触发争用，轮转/FIFO各自默认保持，不把当前fresh-query接口称为历史resident first/reuse复现 |
| 跨模型、任意依赖图、预加载、多 GPU 与查询时限策略 | 本轮暂缓，不作为以上工作的前置项 | 否，暂缓 | 当前研究范围 | 先完成单卡完整查询和公共阶段实测，再按实际问题决定是否需要扩展 |

## main 与集成分支的阅读范围

| 已接纳能力 | 接口与资格来源 |
|---|---|
| Core 推进、完整 HTTP 响应、取消／unknown 与结果租约 | [adapter 接口](src/execution_provider/adapters/README.md)、[CPU／真实 Ray 基础检查](../experiments/results/diagnostics/main_foundation_20261015/README.md) |
| LOTUS 原 Movie Q1/Q2/Q3 四臂完整请求 | [LOTUS 模块](src/semantic_methods/lotus/README.md)、[文本接线](../experiments/results/diagnostics/main_lotus_text_20261015/README.md) |
| Animals 原图 Q1 四臂与观测模式 | [图文接线](../experiments/results/diagnostics/main_lotus_animals_20261015/README.md)、[固定 main 模型与连续查询](../experiments/results/diagnostics/main_queries_20261015/README.md)；保留质量不足及 RSS 待归因 |
| Animals 显式 Q7 与共享图像表示 | [PR68](https://github.com/3444374/ai-operator-execution-optimization/pull/68)已合入；原双Filter／City交集及默认关闭的有界URI保留；[冻结来源native128正常功能与限制](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#animals-q7-shared-media-native128)。main适配已通过CPU及历史还原检查；其他执行路径、真实unknown／abort和性能对照未验证，逻辑持有不代表RSS回收 |
| LOTUS 原 RM／FAISS 建库／检索与显式单卡入口 | [CLIP 接纳与资格](../experiments/results/diagnostics/main_lotus_clip_20261015/README.md)；默认精度失败、HSE 环境缺项与多卡未验证分列 |
| 可选 vLLM 部署资源解析与身份核验 | [服务及原查询单卡复验](../experiments/results/diagnostics/main_service_auto_20261015/README.md)；不借此认证 CLIP、多卡或其他平台 |
| 公共统计与离线结果工具 | [统计定义](src/observability/README.md#repeat-mean-confidence)、[离线输入与 CPU 范围](scripts/analysis/OFFLINE_RESULTS.md) |
| 旧 PG 同步／增量 Map、受限 Filter→Map 与 query-job | 集成分支已暂停该接线；[准确来源和恢复说明](../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md)保留原形态及报告，不表示当前入口可直接执行 |

接口说明只解释相应路径，模型结果仍按报告固定源码、输入、质量和生命周期使用。
未列为 main 接纳的集成能力从[固定 9c59dc9b 来源](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/code/INFRA_STATUS.md)回查。

<a id="current-image-status"></a>
## 固定图像来源与可选实现

`bdae61bb` 的图像能力、默认及未验证项属于该固定实现，不能认证 main 的执行代码；
完整表及原报告链接保留在[精简前能力表](https://github.com/3444374/ai-operator-execution-optimization/blob/0b8aca5ff599e1b6525b77608e3865b20a4fd996/code/INFRA_STATUS.md#current-image-status)。
可选实现的当前归属见上表；固定来源只用于恢复当时版本，不能据此判定整个优化方向已经结束。

文本 Map 的 worker／gateway 复用、Arrow、准备分段、尾块追加与计数诊断已有独立结果；
默认与接口见[诊断选项](README.md#文本map诊断选项)，完整失败与重复从原报告读取；既有单轮诊断结束不表示同类优化方向被淘汰。
2026-10-08 及更早的实现流水、历史调度默认与完成度表见[精简前状态](https://github.com/3444374/ai-operator-execution-optimization/blob/0b8aca5ff599e1b6525b77608e3865b20a4fd996/code/INFRA_STATUS.md#原-main-实现记录)。

<a id="record-local-sections-c827"></a>
## 单模型完整查询的可选逐记录入口

[PR63](https://github.com/3444374/ai-operator-execution-optimization/pull/63)已合入显式可选的逐记录查询入口。
实现来源为`c82714bb`运行快照；快照不是Git提交。`SemLoomRecordChainCore`显式
`execution="record_local_sections"`使用一个prepared Async model owner跨合法F/M段和完整Extract；
默认fused保持，prepare actor为0，B1和请求数C显式给定，不顺带启用token准入或优先级。
同模型有限组合的CPU验收为144个不同测试／232子用例，默认37份计划／150检查点相同；
这些数量属于原候选，不与新检查相加。PR63所用组合源码`35a0e292`的432个运行文件
已通过226个不同CPU测试／261子用例；该组合不是原c827实机源码，不改写原实机身份。

实机只覆盖3行M→E→M→F的12成功及3行F→M的3F+2M成功；同查询一个model UID，
实际token／generation／TaskKey及原方法回放通过，资源已清理。补测第0行Map在另一行Filter完成前
有88.156ms模型API区间重叠；这是功能时序，不是kernel并行或净收益。
另一个新owner的取消项保留1次unknown，只证明IPC queued后触发取消，未证明原生运行中abort完成。
没有独立gold、任意链质量、跨查询模型复用、RSS/KV物理上限、可信P99或稳定加速资格。
原始计时、资源采样及局限见[证据身份](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#record-local-query-owner-c827)。

<a id="record-local-completion-progression"></a>
### 同异步后端的可选完成推进

[PR74](https://github.com/3444374/ai-operator-execution-optimization/pull/74)已在既有逐记录异步owner上接入可选
`progression="record"`（默认）和`"batch_stage"`／显式`progression_batch_rows`。
后者按接纳输入顺序固定有限组，每阶段完成后放行合法后继；末尾EOF封组，过滤终结不等待后继。
原单记录请求、方法转换、Core容量、取消与unknown结清不变，已确认回执及时释放，组内状态仍计费。
首版限同模型同角色完整F/M、请求计数和FIFO，组必须同时容纳于行数与字节grant；不实现token组批或调整W。
原默认fused及Extract关系屏障保留；Ray继续由调用环境持有，该batch-stage组合每查询模型仍新建并关闭，不提供跨查询借用。独立rolling resident候选见[模型寿命范围](#model-query-reuse-scope)。
CPU检查与差分范围见[接口与测试](src/execution_provider/adapters/README.md#optional-completion-progression-on-the-same-async-backend)；
GPU／质量资格仍按各固定源码的报告解释；新main组合尚未GPU复测，不作完整质量或启动提速／稳定加速结论。该组屏障为内部机制消融，不替代原生完整对照。
PR74同时提供[同一Git入口的双策略套件](scripts/README.md#record-progression-validation)，
将策略／组大小传入真实owner，并检查实际策略、同C、held／行／字节grant和配置摘要；默认四臂保持。

<a id="finite-query-token-candidate"></a>
## 完整F/M查询的可选token在途配置

原候选基于`77fe087b`，已与main`43a463b6`本地组合并通过36项CPU检查；`SemLoomRecordChainCore(..., execution="record_local_sections", token_budget=budget)`
复用原`ChainTokenBudget`、FIFO、actor和Core账本，仅补查询owner的配置、工作量单位和容量传递。
默认`None`仍按请求数；一个模型部署及一个角色内的完整Filter/Map计划可选token在途W，Extract、角色切换及fused明确拒绝该`token_budget`组合。
这里的 `token_budget` 是在途上限 W：按每条实际完整消息的估计输入 token 加阶段输出 cap 预留。取消或unknown本身不释放W；权威终态或现有协议确认的生产者退出证明可结算预留，后者不将unknown结果改为成功。它没有决定一批成员和批次边界。
用户原目标是以每批累计 token 上限 B 代替固定行数作为主要组批条件；同步fused单阶段的范围见[跨读取块组批](#prepared-request-batching)，异步记录链固定B/C_batch的实现与实机范围见[PR84组批接口与验收](#async-record-token-batches)。
W应保留为较高的安全护栏，与B和请求数C分开；输入／输出估计不等于实际计算量或物理KV用量。接口细节见[adapter说明](src/execution_provider/adapters/README.md#chain-token-batching-boundary)。
本地126项相关CPU检查通过，含12项新检查；真实LOTUS格式器、微型tokenizer、Core和CPU子进程通信工作，Ray与模型推理为替身。
另3项历史还原检查通过，五个具名版本的完整文件摘要及原身份保持，未改历史清单。
前述CPU范围核对实际生成列、后继参数、W阻止/恢复、取消unknown、晚到终态和原count/Extract路径；本轮现场修复后的两token臂各48次正常完成，输出ID仍有差异，不作性能结论。
独审33项通过；首次集成迟到关闭失败保留，后续独立复现确认释放测试信号早于transport Future交付。测试仅等真实交付后再检查，原断言和timeout保持；生产unknown及W预留不提前清零。
[PR71](https://github.com/3444374/ai-operator-execution-optimization/pull/71)已合入main`230789c4`，默认仍关闭；本轮现场仅覆盖同部署同角色M→M→F的小功能检查，不作性能结论。接口见[adapter说明](src/execution_provider/adapters/README.md#optional-single-model-finite-record-plans)，
CPU身份及原49d负结果从[台账](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#finite-query-token-local-cpu)读取。

<a id="prepared-request-batching"></a>
## 跨读取块token组批（同步单阶段与异步记录链）

PR78的`c228d59d`候选保留历史身份：它仅在固定外层输入块内拆分，未完成跨读取块token组批。
[PR80](https://github.com/3444374/ai-operator-execution-optimization/pull/80)后续修正复用既有`PendingBatchBuilder`、原LOTUS方法和Core/私有引擎路径；默认`prepared_batching=None`不变。
显式token模式对一个Filter或Map按完整prompt token加阶段输出上限累计B，读取块尾不封模型批次。
token配置默认`max_rows=None`，没有每批行数上限；按B、独立逻辑字节额度或EOF封批，不以更大的固定行数替代。
读取块、B、字节额度、并发批数C和vLLM内部容量分别记录。显式正整数行帽只供历史受限配置复现；固定行模式作为对照。
旧B=17506、行帽128的测量保留，但不作为无行数上限目标验收；后续SemBench使用准确新提交和`max_rows:null`，不修改在跑配置。
Core调用数记账仅受整条查询已声明调用总量约束，不作为batch封批条件。单条超过B仍完整单独提交并标记。
准备结果复用修正让token请求只在producer按原方法准备并估算一次，worker复用消息及成本，仍核对身份、顺序、方法/部署/生成参数签名和B。
完整prepared身份与消息计入Core输入字节额度；因此字节受限组可能比旧近似计费更早封批。固定行与默认路径保持原准备行为。
准确`2212e331`已完成原MovieQ1的beb/2212同条件n2实机对照：输入、成员、参数、工作量与关闭责任通过核验，准备/计数记录工作量由各4000降为2000。Q1原检索F1均为1，但严格逐行等价未通过；观察到的计时下降不代表稳定收益、P99收益或token优于固定行，不把全部调用间隙归因于重复准备。
C仍固定1、W默认关闭的边界均未在本修正中改变；逻辑字节额度不宣称是进程RSS或GPU/KV显存上限。
同步fused路径的多阶段token链在消费源或模型预检前拒绝；该路径的跨阶段后继缓冲、反馈动态调整B/C、阻塞源定时flush仍未实现。
原固定行链与默认路径保留。同步历史实机范围按准确head区分：f3为旧行帽功能，beb为无行帽功能及MovieQ1/Q2/Q3，2212仅MovieQ1；2212 的 Q2/Q3 尚未实机验证。Q3现有COUNT为7、gold为14，不能因Q1/Q2原检索F1通过而宣称全任务质量通过。各范围与局限见[PR83验收说明](https://github.com/3444374/ai-operator-execution-optimization/pull/83)，不以新异步组合覆盖旧head的资格。
接口、逻辑字节边界及unknown责任见[所属说明](src/execution_provider/adapters/README.md#prepared-request-batching)。

<a id="async-record-token-batches"></a>
### 异步记录链token组批（PR84）

早期独立候选 `c79fcc6d` 基于 `beb5966c`，未包含PR83的同步重复准备修复。
[PR84](https://github.com/3444374/ai-operator-execution-optimization/pull/84)的准确验收头 `41ce4c24` 以 `2212e331` 为基底，接入异步核心、关闭分页修复及正常入口，已合入main；`cc3980c9`与该验收头仅两份文档不同，生产代码一致。
它复用原MethodRun/Core/actor/engine，以一次数据RPC及IPC提交一个token批，保留逐成员完成、后继推进、取消与unknown责任及Extract关系屏障。
可选范围为固定B、固定应用批并发C_batch、FIFO、record推进、W关闭；无每批行帽，R仍同时约束在途成员与resident行。
默认仍为`prepared_batching=None`、关闭；同步对照保留。原records工厂已接正常Movie `check/run`和isolated session，保留冻结任务、source及原评价器绑定，并核对实际B/C_batch/R和逐成员回执。

普通SemLoom records及Movie Session入口新增显式`core_capacity`和部分`async_limits`传递候选，
复用原`CoreRecordsCapacity`／`AsyncEngineLimits`；缺省／空对象／仅反馈选项绑定解析后的owner R，
Core单项与总量、IPC单项／槽位与总量分别推导，显式小额度和原unknown责任保留。
[接口映射](src/experiments/movie_matrix.README.md#semloom-records-的-coreipc-容量)与
[CPU反例](tests/experiments/test_records_capacity_config.py)已覆盖普通／常驻入口和连续查询重置；
未新增真实Ray／模型／GPU资格，不改变其他baseline或历史具名实验。
有效容量已进入普通admission cache身份，省略与等效默认不分叉；离线Core calibration绑定固定容量规则，
保留随扫描C派生的owner额度，并在owner创建前拒绝显式计数不匹配的候选。
[容量身份CPU测试](tests/experiments/test_core_capacity_identity.py)覆盖缓存命中／隔离、默认归一和候选约束；
仅授予本地组合接线验证，不继承真实模型或GPU性能资格。

记录链补料被拒后保留单个pending输入，结束本轮补料并沿原advance／wake等待；方法结果消费释放预算时通知原wake。
[CPU拒绝／唤醒反例](tests/execution_provider/test_lotus_chain_refill_wait.py)覆盖慢消费、可重试拒绝与unknown责任；
这是独立字节预算迁移的先决修复，尚不等于容量解耦或物理内存边界已完成。

默认关闭的`core_capacity.budget`本地候选已将Core任务槽／输入／结果预留／硬请求槽、方法行／字节预算
与性能C分开，IPC另需显式有限槽位和总量。普通async Filter及rolling token通过原Movie／Session入口接入，
旧默认和历史R推导保留；sync、fused及非rolling批槽不在新预算范围。
边界与字段见[显式独立预算](src/experiments/movie_matrix.README.md#显式独立资源预算默认关闭本地cpu候选)。
CPU接线与子进程IPC不构成物理RSS／真实Ray／模型或性能证明；全查询关系与审计保留仍不受这些活跃工作预算完整覆盖。
独审反例的本地修复恢复旧普通owner的硬容量闸门，新预算仍逐查询设置软C；token组批身份采用校验后默认值，
并拒绝新预算直接接同步owner。[跨查询／等效配置反例](tests/experiments/test_records_budget_query_identity.py)仅授予CPU回归资格。

本地可选`rolling_refill=True`候选复用同一路径，以既有请求窗口R滚动补交token批；慢成员不再占一个整批供给槽阻塞已释放请求额度。
默认关闭，原C_batch模式保留；启用时省略`batch_concurrency`，并拒绝混入`batch_feedback`。
B只组织每次传输，批成员提交后不追加；请求、驻留、字节、ACK/unknown与Extract屏障责任保持。
正常Movie JSON和isolated session沿既有工厂进入，旧预算模式回执记录`C_request=R`与`C_batch=null`；显式独立budget另记录软C和硬R。
目前仅[CPU场景](tests/execution_provider/test_lotus_token_rolling_refill.py)与既有入口检查；真实Ray/模型/GPU及性能收益尚未验，不能借用下述历史整批成绩。

本地默认关闭的请求级动态候选在上述rolling中显式增加`core_profile.admission.kind="resource_feedback"`；
无合格提示时初始C_request=1，硬R仍独立，C_batch=null、B固定，复用需求门控与已修复的单RPC/session反馈。
每条完成即可补位，缩容只停止新选择，已准入成员（含后端暂拒的不可拆分组）继续有界排空；原unknown、关系屏障及资源责任不变。
默认无候选次数／探索时间硬停止、不读gold或全表token参考。新本地`search_policy="ascending"`候选
在可比收益后继续上探，仅平台／回退／冲突时局部确认；显式`reference`保留旧逐档回访。
候选接通健康 resident 查询的条件提示续扫；新资源机会可唤醒旧选择，跨query旧速率区间只作失效判据。
同一snapshot RPC可读模型actor分配设备的NVML和cgroup／affinity CPU范围；联合空闲份额在ready供给／硬R内确定可变大步探测目标，
精确请求成本只辅助步幅，不是试探前提。显式预算、硬R／字节／unknown责任保留，无动态B或实机收益资格。
CPU 区间归属修复改为采集器实例累计计数器差分，并保留拒绝的原始间隔／原因；不改采样频率、1 秒资格或 actor 并发。
该增量有无模型 CPU 回归，新增真实 Ray 组合资格和模型 LIGHT 复验仍待执行；历史 21 条被拒间隔的原值无法补作已知。
具体判据、边界与旧动态负结果见[策略说明](src/scheduling/runtime/calibration.README.md#rolling-请求的上探与条件续扫)。
当时策略的[CPU状态机](https://github.com/3444374/ai-operator-execution-optimization/blob/9da299e90f79fd0b71f2a4983a90884f8ba960b5/code/tests/scheduling/test_ascending_request_scan.py)、[成本反例](tests/scheduling/test_resource_step.py)
和[完整查询入口](https://github.com/3444374/ai-operator-execution-optimization/blob/9da299e90f79fd0b71f2a4983a90884f8ba960b5/code/tests/experiments/test_movie_ascending_feedback.py)、[资源触发链](https://github.com/3444374/ai-operator-execution-optimization/blob/9da299e90f79fd0b71f2a4983a90884f8ba960b5/code/tests/experiments/test_movie_resource_probe.py)
验证，待复审及真实Ray／模型／性能复验；旧实测不改标本候选。

准确`41ce4c24`的真实Ray/模型功能范围独审通过：198条提交含174成功、17项预期确认容量失败、7项取消unknown；覆盖129+1组批、后继提前推进、关系屏障、取消、关闭分页7→3→0及ACK释放。unknown不计为成功或确认失败。

Movie Q1/Q2/Q3的同步fixed128、同步token及异步token C_batch=1/2各n2，共24组、17,920次成功原生请求完成，全部物理清理与租约释放通过核对。token臂均固定B=17506、无每批行帽；异步臂另设R=259、W关闭，CB1/CB2为手动固定批并发，不是资源反馈自动选择，也不是最优推荐。
Q2/Q3各120条，固定128行和token预算17506在本轮均实际只有一批，CB2峰值仍为1；仅Q1的CB2实测到两批重叠。

本轮三任务沿用同一Movie Q1全量2000条输入的token长度预分析所设参数。已找到的早期准备脚本按`ceil(mean(prompt_tokens + output_cap) * 64)`给B；沿用的17506与`ceil((128.76 + 8) * 128)`一致，但首次64→128决策脚本尚未找到，不能将算术一致当作原始决策已溯清。
R由全量token reference按B=17506源序预分组的最大组129推导为`2 * 129 + 1 = 259`；native128／fixed128继承旧对照，现有原件未给出平台搜索依据。矩阵明确`concurrency_search=False`，属于输入统计预设的固定工程对照。

token reference生成、长度统计、预分组／R推导及此前实验均在worker开始前，未计入本轮冷时间；正常查询内仍重新执行方法准备和token计量，不以该离线reference替代。已核对的B/R准备代码未见gold标签选参证据，但未知任务零先验自适应及包含离线选参成本的总时间均未获验证。

未证明完整冷查询加速；冷计时从worker入口标记至原Movie查询结果就绪，含其后的必要启动，不含父控制器／Python bootstrap及后置评价、写证据与最终清理。启动栈、实际分组和缓存工作差异不允许只归因于推进粒度。Q1严格原生输出等价未通过；Q3各配置共同COUNT=7、gold=14，仍是质量失败。

未覆盖MMF独立完整返回payload等价、完整Core的GPU容量失败映射（GPU已测direct actor，完整映射为CPU）、四种独立读取块GPU对照、P99或资源最优；该次验收不覆盖后续rolling resident候选，动态B/C联合控制仍未实现。具体配置、耗时、CPU跳过与资产摘要限制见[公开验收摘要](https://github.com/3444374/ai-operator-execution-optimization/pull/84)；无CI通过声明，后续源码修正须另行验收。
准确参数单位、尾批行为、字节边界与测试入口见[异步组批接口](src/execution_provider/adapters/README.md#optional-async-record-token-batches)。

### 组批计量与批提交的CPU开销修正

`5814f44e`复用已准备请求的逐成员帧计量及当前提交头，保留发送时的完整编码和字节校验；
`c9b07b00`只在原生固定Core准入、单执行端、无pool路由的多成员提交中使用调用内容量快照。
token RD的多成员提交也可命中该路径；单成员、普通records动态准入、自定义策略和其他路由继续逐成员检查。
两项保持批成员、顺序、封批与拒绝位置、参数、unknown责任及默认配置不变。
相关检查见[帧计量回放](tests/execution_provider/test_lotus_request_frame_metering.py)与[批提交差分](tests/scheduling/test_batch_dispatch_snapshot.py)。
原候选的CPU fixture分别观察到每千条请求计量编码5,000→2,003次，以及32成员提交容量表扫描96→3次；
两种微基准覆盖不同局部成本，不能相加或外推完整查询收益。本组合只有CPU等价性资格，尚无真实Ray、模型或查询性能验收。

组包诊断增加常数空间的全query封批原因计数，复用原`selected_batches`；最近64条history仍只作示例。
close报告另列首次actor批ACK的accepted/rejected/unknown与尚待确认数，不与选择次数、owner内部重试、
成员终态或GPU内部批次混用。原打包、参数、责任与默认值不变；新增计数只完成CPU验证，尚待独审及对应实机核验。
字段与界限见[全query包计数](src/execution_provider/adapters/README.md#query-packet-counters)。

<a id="model-query-reuse-scope"></a>
## 模型所有者与跨查询复用范围

异步模型 owner 的本地 RPC 容量修正将数据线程预算从 R／请求数中解耦，复用非阻塞
提交、既有逐成员轮询与 ACK；普通 sync 默认不变，模型／引擎容量和输出语义不变。
数据许可来自 `RecordsResources.model_rpc_workers`，省略时按声明的 `cpu_workers` 解析，
不是硬件探测或性能选优。旧直接阻塞 actor 调用保留，owner 提交 awaitable 的迁移和
两个控制位置的职责见[接口说明](src/execution_provider/adapters/README.md#records-core-可选动态接纳)。
目前只有 CPU fixture／子进程 IPC 检查；没有新真实 Ray、GPU 或 2,000 条查询成绩。

`e7373c5b` 尚无 token 跨查询复用；后继`0585e9bf`接通下表的 rolling resident 路径，
已有真实模型 RF／旧 RD 的首次／复用单配对；旧动态是负结果，见[扫描边界](src/scheduling/runtime/calibration.README.md#rolling-请求的上探与条件续扫)。
本轮新扫描／提示接线只完成 CPU 资格，不能继承旧源码的实机或性能验收。
架构目标及三类复用见[所属章节](../experiments/plans/系统架构.md#model-query-lifetime)。

| 实际入口 | 模型与查询寿命 |
|---|---|
| SemLoom Core 的 Movie 单查询 CLI | 每次调用创建自己的 owner 并在 `finally` 关闭；再次启动 CLI 不构成同模型复用 |
| 非 token Core 的 `resident` Movie Session | [`SemLoomRecordsCore.start()`](src/execution_provider/adapters/lotus_records_core.py)在 Session 查询执行前等待真实模型 ready；`evaluate()`可连续执行独立查询，最终 Session 关闭模型 |
| token 整批与默认 `isolated` RF／RD | 工厂使用 [`PreparedRecordsCore`](src/execution_provider/adapters/lotus_records_batches.py)；保持原有限查询启动及 EOF 关闭，不跨查询复用 |
| 显式 `resident` 的 token 固定滚动 RF／请求级动态 RD | 只接通 `rolling_refill=true`、`C_batch=null`；`start()`等待实际模型 ready，正常 EOF 证明查询责任／RPC 已排空后保留模型／worker／IPC，Session 最终关闭模型。查询账本、额度、组织器与控制器重新建立，Core 的数值 session ID 继续递增 |

普通非 token Core 可在同模型、引擎／生成配置及兼容数据合同下逐查询绑定受支持的 Filter 指令；这不代表任意 Map／Extract 链或模型／角色切换可共用实例。
其 `_CoreFeedback.begin()`逐查询重建／重置控制器、进度和测量；保留模型或采样 provider 不等于自动继承推荐窗口，显式调优缓存／提示另按其配置解释。
旧 `reference` RD 的 resident 查询各自从 `C_request=1` 开始。新 `ascending` 候选仅在完整查询成功及
责任结清后复用同 owner、精确兼容身份下的已确认档位和未完进度；新鲜工作量重验证，不继承旧速率。
RF 保持声明的请求容量。模型原生缓存仍可能跨查询保留，不能将其计算复用直接算作调度收益。

[`movie_records_session.check_plan()`](src/experiments/movie_records_session.py)只放行上述 rolling resident 组合；批槽反馈、admission cache 及非 rolling token resident 仍拒绝。不同模型身份或运行绑定拒绝；更换部署／生成／容量配置须创建新的 owner。执行失败、取消、提前关闭或未确认排空会关闭当前 owner 并禁止继续复用，不清零 unknown。
[复用 CPU 测试](tests/experiments/test_movie_token_resident_session.py)覆盖连续查询、不同合法 Filter 指令、空查询、账本／原生请求身份隔离、显式旧策略重置以及失败／取消和最终关闭；[新策略入口](https://github.com/3444374/ai-operator-execution-optimization/blob/9da299e90f79fd0b71f2a4983a90884f8ba960b5/code/tests/experiments/test_movie_ascending_feedback.py)另核验条件续扫及完整结果处理后才提交提示。模型为 CPU 子进程替代，Ray 为 threaded fixture。这不是新策略 GPU 常驻或加速证据，也不开放任意链、动态 B 或跨 owner 学习。
已测首次查询按原边界保留，不能从时间中减去估计加载成本后补成复用成绩；Movie 与历史 `802b0b50` 的口径见[对照规范](../experiments/plans/reference/对照规范.md#movie-session-query-timing)。

<a id="batch-supply-feedback"></a>
## 固定B的供给反馈（独立本地CPU候选）

基于`cc3980c9`的可选切片复用原异步token组织器、Core、actor、IPC原生统计和工作量比较器，只调整应用批并发C_batch，B保持固定。
显式`SupplyFeedbackConfig(max_batches=...)`从一批开始，默认`batch_feedback=None`不安装采样器；R、字节／IPC责任、后继和关系屏障保持。
实际就绪WorkDescriptor、方法到Core准备／供给速率、确认完成工作量与同owner/session的running/waiting/KV／抢占信息进入可解释判断；缺失／陈旧、供给不足、后端排队、资源压力、尾部／依赖等待及配置碰顶分列。
重用原在线比较及保护逻辑，不读同任务token reference或gold；配置上限不表示性能平台，在线观察和探索成本不扣除。默认computed比较缺计数时不确认收益。
反馈间断使当前比较失效并回退，只恢复被中断候选的重试资格、保留已发生次数和耗时；采样RPC取消失败时持有原引用直到终态，不逐次叠发。
供给策略默认不设时间／变化次数／候选尝试次数预算，`min_remaining=0`；旧有限预算仅在显式沿用时保留，不把64条日志保留量当作并发上限。默认规则与源码依据集中在所属说明。
Python owner／普通Core工厂及正常Movie `check/run` JSON、isolated session共享同一`batch_feedback`配置；省略或null默认关闭，仍拒绝逐请求admission/cache和resident组合。
B运行时调整或联合控制尚未接入；CPU/GPU利用率、物理可用内存和原生启动容量readback仍未知，仅有IPC回执与声明容量。
接口及CPU边界见[所属说明](src/execution_provider/adapters/README.md#optional-fixed-b-supply-feedback-local-cpu-candidate)。本轮修复与入口增量仍待独审及GPU验收，不代表资源收益、饱和点或Kalypso复现。

<a id="finite-query-successor-candidate"></a>
## 后继优先与等待老化的可选配置

原候选基于`77fe087b`，先在main`43a463b6`独立整合，再在已含PR71的main`230789c4`本地组合；通用`MethodDriver`与9231f088的driver逐字相同，包含可选观察读时异常及必需策略读时失败的处理。
`LotusChainFIFO`保留已在main的token参数，另接原ready选项；完整查询owner提供相同选择与`ready_clock`透传。
通用driver默认轮转，FIFO wrapper及查询默认FIFO；自定义选项限同部署同角色F/M计划，Extract、角色切换及fused明确拒绝该组合。
原main43a独立组合77项CPU检查通过；当前main230组合106项CPU检查通过（17项联合检查，含原token生命周期重放）。逐文件摘要与五份历史还原身份保持。
该接口支持同部署同角色F/M内单独选择或与token联合选择；Core队列中的已接纳调用不被重排，token W只限制活跃调用的估计工作量。
PR72已合入main0689；本轮两个后继配置各48次真实调用完成，但竞争选择均为0，不证明优先调度效果。新的Git验收入口组合另行版本化，不继承整树模型资格。
原两轮负结果与输出差异仍从[台账](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#priority-reverse-small-pair)引用；
新接口不是原resident first/reuse的实机复现，适用条件见[adapter说明](src/execution_provider/adapters/README.md#optional-method-ready-successor-selection)。


## Git 同步的资源观测入口

测试采集器和最小 on/off 工作流位于 [profiling 工具](scripts/profiling/record_observation/README.md)，
复用既有 Git/CPU 资格、原监督／额度及逐文件私有原件检查；配置、认证、数据和回执不进仓库。
CPU 探针覆盖单请求和现有 token 批 poll／成员 ACK 路径，批页重读不重复统计成员首次返回；
源码没有批 API 时明确标记该 CPU 范围不支持，事件、sink 或时钟缺失仍不能宣称完整。
设备 UUID 绑定与 native 反馈身份审计继续复用该入口；新增有界缓冲、消费阶段与有效容量回执的
范围见上述 README。该增量只有本地 CPU 验证，准确新树的 NVML/Ray/vLLM 与 on/off 开销待实机核对，
不继承旧树结果；未改变生产策略、默认或 B/C 配置。

同一入口新增显式的[在途取消与新 owner 恢复功能检查](scripts/profiling/record_observation/README.md#inflight-cancellation)。
它复用现有 Movie 驱动、取消回调和 guard，核对 native API 活跃请求／abort 身份及 Core、Method、RPC 的收尾，
保留预期取消 unknown 与保守计费。一次真实在途取消在 actor 到最终账本的回执桥接处失败，后续恢复及性能组未运行。
本地修正候选在 Core 收尾 ACK 前将验证过的回执移交原查询审计，保留 unknown、任务与批成员身份；CPU 回归不替代独审和实机复验。
审计 sink 拒收时，待补交回执及注册限额跨 ACK 保留，后续 close 持续报告未完成，成功交接后才清除；模型／Core 退休与审计交接分开，补交不重新推理或解析。实机原件时序仍未独立锁定。
物理 GPU 清理、原生接口版本兼容和覆盖率待实机验收。修正覆盖现有 record_local_sections 关闭审计，生产默认与模型语义不变。

普通 Core 与滚动 token 公共工厂的新资源请求接线最终为 309 项中 308 成功、1 个上游 checkout 跳过，
无失败或错误；旧 KV／busy 选项仅由显式历史策略接收，当前请求策略没有跨查询提示。
实际许可来自已有 Core／IPC／结果字节，采样来源按同次读取绑定；历史原十处不匹配保持。
[新接线摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/public_request_wiring_checks.json)
保留全部首次失败、加载选择错误及尚未解决的 10 毫秒增长波动，不提供实机或性能结论。

公共工厂独审提出的两项历史用例／链接说明迁移已修正，原数值与工作量断言保持；
30 项针对性 CPU 检查成功，采样文件尾空行已清理，待复审。原独审的 Unix socket 权限缺项、
上游 checkout 跳过及作者原运行分别保留，见[修订摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/wiring_review_checks.json)。

普通 async Filter／rolling 工厂的默认存储已在新 CPU 候选与 R 分开，Movie、session 及离线校准
共用解析结果；新默认身份为 records_capacity_policy_v2，历史显式策略保持原身份。
388 项中 387 成功、1 个上游 checkout 跳过，无失败或错误。R 默认仍是声明的 2，没有自动硬件额度；
低层直接构造、非 rolling 整批及同步／异步规范拆分仍待做，不能宣称全项目迁移完成。
[默认容量摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/default_capacity_checks.json)保留全部初轮问题与历史原十处不匹配。

历史脚本清理移除三个无现用调用的 SAOR 启动入口，共 281 行；准确原件以 gzip 保留，
新增十五条历史映射，3010 条来源仍只有原十处不匹配。49 项相关 CPU 检查及三个恢复后 CLI
解析检查成功，离线分析和实际后端保持，见[逐文件摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/obsolete_entry_checks.json)。

历史恢复映射已在另一独立候选补齐：找到两份与原 SHA 完全相同的旧源码，新增十条共享映射，
3010 条来源现全部匹配；51 项 CPU 检查成功，五份完整投影实际构造及逐文件核对通过。
原失败与旧阶段数字保留，维护实现、既定 SHA 和投影身份未改，详见[恢复摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/history_source_repair_checks.json)。
这不构成环境、模型或性能资格，候选仍待独审。

原生参数省略测试已跟随新的公共资源解析迁移：请求 R 与初始 C 分开，结果槽取已有 IPC 配置；
旧跨查询缓存用例显式选历史策略，并新增当前策略拒收旧缓存的检查。改前 14 项中 10 成功，
改后相关 47 项全部成功；只改测试，生产行为和原参数签名差分保持，见[补充摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/capacity_native_test_checks.json)。

查询容量与非默认配置又补齐七个旧测试：分别核对 owner R、当前 C、请求峰值与驻留额度，
保留输入、usage、取消及小 RPC 池断言。52 项 CPU 检查成功，生产源码未改；保存一份准确原件并
新增五条恢复映射后，3010 条全部匹配。见[补充摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/query_capacity_test_checks.json)。

同步／异步的共享规范、请求身份和结果转换已迁到 vllm_protocol；旧导入与 pickle 可用，异步不再
加载同步 owner 或借用其采样方法。跨模块选中 1350 方法，实际运行 1345：1315 成功、30 方法跳过，
另一个包含五个选中方法的类跳过，无失败或错误。同步旧默认、启动清理、IPC 大单项修复及新策略组合仍待做，
见[准确范围](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/engine_protocol_checks.json)。

省略 IPC 总额的大单项拒绝问题已在独立 CPU 候选修正：先使未指定的总额至少容纳对应 IPC 单项，
再交原类型检查；显式小总额、显式单项及 typed limits 保持。107 项检查成功，原审查的 16 个构造
案例由作者重放全部接纳、模型分配为零；尚待独立复审。见[修正摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/implicit_ipc_total_checks.json)。

已审 c4d5e5c 的策略原件已组合到普通／rolling 实际入口；同时用已交付累计值加同 session
保留回执修正普通工作量反馈，采样／时钟、原消费、ACK、释放与旧策略方法保持。定向 441 项 CPU
全部成功，历史 3010 条全部匹配；没有自动硬件容量。完整回归按自身提交交接，尚待独审及真实模型验证，
见[接线摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/current_request_policy_checks.json)。


显式 `max_active_batches=None` 现可经普通 async／rolling 工厂使用已有完整独立预算；
原声明保持 None，实际 Core、Method 和 IPC 许可仍有限。无完整预算时在 actor 分配前拒绝，
不回落到 2 或解释为无限。167 项 CPU 检查成功，历史 3010 条来源全部匹配；
此处记录当时的普通 R=2 与 IPC 8 槽；当前省略 IPC 的解析见下方“普通 IPC 省略值”。
[本次摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/unspecified_request_checks.json)保留原九项反例；
独审、动态回执问题的另项修复和真实模型检查仍待后续。


unknown／failed 回执缺少工作量时，native 压力帧的新鲜度现仍按实际来源判定；性能计量可暂停，
压力响应继续核对原序号和时间。已审两文件增量与 None／完整预算候选在独立树组合，
原 unknown 持有、ACK 和释放路径保持；完整 CPU 组合结果及未验证项由
[组合摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/capacity_pressure_composition_checks.json)维护。
默认 R 与自动容量仍未完成迁移，组合仍需独审，不继承真实模型资格。


旧工具清理从维护目录移除入门 Daft smoke、一次性 DuckDB 耗时探针与旧短长 Job manifest
生成器，共 502 行；准确原字节压缩保存，五份历史投影仅新增十五条恢复映射。
逐文件盘点覆盖 132 个脚本 Python 文件、509 个测试／fixture 文件，所有既有测试保留。
组织器、DuckDB adapter、统一 baseline 入口和历史结果保持；
[清理摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/entry_slimming_checks.json)维护恢复检查和未处理项，
[完整静态清单](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/entry_inventory.json.gz)保留每个文件的证据。


独审发现的 static None 假通过已在独立候选修正：CPU 检查和直接 driver 启动均要求有限
static 请求额度；Core 配置也在目录创建与 Ray 初始化前解析。合法 async／rolling 的
None 加完整预算保持，相关检查见[预检摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/static_capacity_preflight_checks.json)。
公共帧交接仍有可复现的空轮询覆盖风险，旧 geometric 仍未取得完整资格；新压力修复的局部
通过不能替代公共 provider 的完整检查。[调查摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/geometric_source_handoff_checks.json)
保留原失败和新增 CPU 跟踪，不声明唯一原因已证明或问题已修。


同步 ready 现回传同一实例的实际上下文，并拒绝缺失、非法、显式不符和运行中变化的值；
复用已有身份格式，原构造参数、chat、结果转换与关闭实现保持；关闭未确认时拒绝 ready，
确认后仍可查询已关闭回执。相关组选中 146，运行 143 项全部成功，另一个包含三方法的类
因上游 checkout 未提供而跳过；历史 3010 条来源匹配。
[同步回执摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/sync_context_checks.json)
保留首次反例与环境问题。同步旧默认、链／缓存完整接线、部分构造失败清理和真实模型检查仍待处理。


已审容量预检与同步上下文回执现置于同一组合候选，生产文件保持各自已审字节；五份公共文档保留双方内容。
[组合结果](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/reviewed_context_capacity_checks.json)
记录实际运行范围；独审缺 DuckDB 的三项与作者缺上游 checkout 的三项分别保留。同步旧默认、
geometric 失败、绕过预检的组批参数拒绝时机和自动容量仍需后续处理，未推送或合并。


直接 session start 的组批参数现调用正常预检共用的纯解析器；非法批并发、rolling／批并发冲突
与缺少组批策略在目录和 Ray 创建前拒绝，合法配置继续进入原工厂。
[组批预检检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/session_batching_preflight_checks.json)
补充上一组合尚未处理的提前拒绝项，不声称已验证所有绕过 check_plan 的无效输入。


准备交错候选已移入当前 query runtime，并通过现用工厂显式选择；两个选项默认关闭，
不向 Movie JSON／CLI 增加模式。Method 行数／字节、Core 存储、请求许可和 RPC 槽分别使用原负责者，
通知只触发重查，不释放 unknown、compute 或未消费结果。
[组合摘要](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/prepared_supply_composition_checks.json)
区分原候选、当前移植、反例及 CPU 检查；未证明实机性能，自动容量与普通默认 R 迁移仍未完成。

首次交错完整回归出现 13 方法错误，来自旧对象未声明新增开关；现将缺省保持为关闭，
86 项相关检查成功。最终组合全量复查另行记录，原失败输出未删除。


当前另有包含交错候选、已审容量／上下文修正及组批提前检查的统一候选，六处文档追加均保留双方。
[最终组合结果](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/system_followup_checks.json)
维护准确源码与实际 CPU 范围，不替换原几何失败及旧基线问题。
[入口与容量来源审计](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/entry_capacity_source_audit.json)
补齐两个同目录图像 profiling 调用；保留四项仍具独有用途的旧工具，同步旧默认有 14 个文件的
22 处读取尚待迁移。自动容量没有实现，普通 R=2／默认结果槽=8 继续明确记录。


legacy 资源反馈现以独立候选保留尚未消费的上一帧，直到新 RPC 返回、原请求到期或来源不可用；
保留原时间／序号，模型或查询变化时清除，不重复制造新样本，也不改变原公开 sample 的空返回。
[交接检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/pending_frame_checks.json)
区分确定性反例、原 geometric 失败和一次只观察输入的 CPU 跟踪。控制器及其年龄判断未改，
同步默认和普通 R／IPC 槽迁移仍未完成，不能据此宣称自动容量或实机收益。


Movie 单次与 session 配置现有默认关闭的准备模式候选，`preparation_mode` 的 serial 与省略相同；
interleaved／event_driven 复用已审 SDK，只支持固定 rolling token 请求。共用预检拒绝无效模式、
非 rolling、动态反馈或其他 backend，直接 session start 也在目录／Ray 创建前拒绝。
模式进入实际回执和校准身份；None 加完整独立预算保留有限请求与存储。
[CLI 接线检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/preparation_cli_checks.json)
分别保存原反例、CPU worker／连续查询、合法前缀用量和最终组合检查；不表示已做真实模型验证。


同步生成检查另有增量候选：ready 与每次 run_prepared 复用同一实际模型上下文核对，在调用 chat
和预留调用数之前拒绝过大输出、不可用／变化的上下文以及显式值不匹配。运行中的长度缓存改用该回执值。
[同步逐调用检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/sync_generation_checks.json)
保留原反例及本地进程身份读取受限的一轮；构造参数和 chat 保持原路径，4096／8／4096 的构造缺省
与 fused 的旧检查尚未迁移，完整提示加输出的提前拒绝也不由此增量证明。自动容量仍待实际字节授予。


优先级路径的延迟关闭检查改为复用已有“确认模型成功后延迟交付”的用例，保留 successor_first 设置，
核对结果／用量、未完成查询、Core 持有和无 parser／后继重放。原接纳确认不能证明模型成功，
旧完整回归的一次失败和父候选／当前候选的单次对照均保留；接纳但仍计算的关闭情况仍未由此用例证明。


同步构造与 fused 上下文现有后续 CPU 候选：当前 SemLoom 省略未指定的三项引擎参数，使用实际 ready
长度；原生完整提示经一次渲染，总长超限在提交和调用数预留前拒绝。fused 的旧 4096 重写已移除，
模型身份、实际长度和关系复用记录在审计中；不可用 ready 不变成无限或影子值，清理 unknown 另行保留。
原生 Ray／Daft／IMLane 显式保留原参数与 chat，共用原 IPC／方法／关闭，旧导入和历史来源仍保留。
[同步参数检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/sync_default_checks.json)
记录实际 CPU 范围；BF16／0.8、普通 R=2、默认结果槽=8、自动容量和旧 geometric／关闭问题没有由此解决。

同步公开证据另有修订：两份 CPU 归档只规范化公开副本的个人 home 前缀，保留 24 份私有原件的摘要、
公开副本摘要及对应关系；失败身份、栈行号、用量和代码来源不改。
当时的补充脚本现已[退役并保留准确源码](../experiments/reproduction/native_baselines/README.md#retired-ack-source-replay)；
固定上游函数体继续从[现用 unittest](tests/records_cpu.md#sync-native-source-check)核对，不再改写当前测试去适配旧 ACK 演示。
两个原生源文件先前私有副本多一个尾部换行，选定方法的语法树一致。
[公开修订记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#public-cpu-evidence-copy)
区分原 f577 检查、修订复查和缺项；已有 489 份运行时 Python 文件保持，未增加模型、Ray／Daft 或 GPU 资格。

计算中关闭另补明确的 CPU 行为检查：FIFO 与后继优先分别覆盖晚到成功、失败和 unknown，
计算未结束的首次短等待保留 Core 请求／输入／结果额度；同一生产者停止后收集真实回执并归还，
不解释结果、不推进后继、不补报查询完成，重复 close 不新增调用。三项方法包含六个场景，
初次检查误读已有报告层级的错误保留；本批不修改运行时，不证明真实 Ray／模型／GPU 退出。
[关闭检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#computing-close-cpu)
与[容量来源核对](../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/service_grant_source_audit.json)
分别记录 CPU 资格及源码判断，普通 R=2／默认结果槽=8 和自动授予仍未处理。

直接 async actor owner 的缺省 IPC 组装现复用公共容量解析器；旧副本会忽略显式 Core 单项大小，
固定回到 2 MiB，已由小项未拒绝及大项被拒绝两项实际 CPU IPC 反例复现。显式 IPC 对象、
独立预算要求和原省略值保持，未改动态控制器。
[容量来源检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#actor-capacity-source)
另保留父候选上同样失败的三项旧检查：提示观测移到实际 try_submit_core，驻留与请求分别核对。
原历史投影和失败不改报；此处统一逻辑额度来源，不提供物理内存自动授予。

CoreRecordsCapacity.with_storage_bytes 另可从已经授予的三份有限载荷字节推导计数，沿现有公共工厂使用
R=None；Core、续算和 IPC 继续分别记账，原六字段显式预算不改。请求数来自 IPC 的完整结果预留，
续算计数由合法最小预留推导，实际字节检查先起作用。没有新增控制器、物理探测或 CLI 开关。
[字节派生说明](src/execution_provider/adapters/README.md#byte-derived-records-capacity)明确调用方仍须拥有真实授予；
独占性、跨实例归属及全查询保留未由该转换证明，普通未配置路径仍为 R=2／默认结果槽=8。

已审 faab48cd 来源期限修正另与当前容量候选组合：legacy_resource_feedback 的 sample 响应使用同一
provider 实际声明的 stale_after_s，工作测量仍用原 window_s；未绑定的直接调用及历史 window 响应保持。
五个接入文件与已审候选逐字节一致；现代请求策略、模型参数和容量／unknown 责任未随来源判断修改。
[组合检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#capacity-source-clock-composition)
分别记录原输入回放与实际组合回归；固定输入上的四次调整不是新查询或性能结果。
后续运行须声明新的 execution.source_revision 或使用新缓存，当前提示键不自动散列实现源码。
异机来源时钟、真实 Ray／模型／GPU 和自动服务授予仍未验证。


普通 async SDK 另有服务载荷授予候选：`RecordsPayloadPool` 在一个服务进程内原子分配互不重复的有限字节，
工厂领取后才建立实际 owner，R／结果槽从已有字节预留派生；未配置服务池的现行缺省不变。
ready 的模型与 producer 身份、未确认清理及未取走结果共同决定授予是否保留；不新增请求控制器。
[接线与 CPU 检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#service-payload-grants)
仍是待独审候选。总字节来自服务声明，不是自动硬件探测；CLI／session、rolling、多进程恢复、
真实 Ray／模型／GPU 和完整查询内存尚待后续，不能据此宣布整个重构完成。


服务载荷候选补充观测 RPC 的归还检查：普通现代反馈 owner 关闭时读取既有 sampler 当前终态证明，
未确认观测 RPC 使关闭继续报告 unknown；授予归还再次核对实时证明，不以采样线程停止或旧查询报告代替。
[修订记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#service-snapshot-retention)
保存原独审反例与 CPU 检查。测试替身的来源期限字段另有修订；两者均仍需后续准确组合审查和专用环境续测。


服务字节授予继续接入普通 Movie CLI／session：显式 service_payload_bytes 由纯预检生成计划，
实际领取在 Ray 初始化前完成；同进程逐次查询共用原池，外来池不由 session 关闭。
配置提取到 records_service_config，原始 R／结果槽缺省只在未配置池的路径保留。
同进程清理使用原 owner、Ray module／job／node 身份，不初始化或连接替代 runtime；
已确认关闭而仅结果待保存时可在 Ray 结束后继续本地转交。文件及父目录同步先于精确回执接受，
写入失败、未知清理和未确认 RPC 继续保留授予与恢复句柄。
[接线与检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#service-entry-recovery)
另有完整结果数组的纯计量与验证；尚未接 rolling 的实际 Method／结果持有，不能声称该路径已支持服务授予。
本批未运行真实 Ray／模型／GPU，不提供物理 RAM 探测或跨进程重连；整个重构仍有未完成项。

prepared 普通结果另减少重复拷贝：报告不再复制随后马上覆盖的 calls；永久关闭清除额外迭代引用，
只在同一实际流健康 EOF 且 Core／transport／data RPC／审计交接均清空时借用原稳定结果快照。
unknown 或缺证明时继续读取实时调用；完整交付字节由已核每项长度及原编码标点求和，不再编码整数组。
这些操作不新增派发、接受或释放动作；服务 rolling 授予仍未接入，完整结果及保存所需载荷仍须另计。


rolling 普通 Filter 的 Python SDK 候选现接收服务池和实际完整交付声明：R、Method 与 IPC 的实例额度
由同一有限逻辑字节计划派生，整查询结果按实际发生项独立计入；resident 查询结束仍保留实例授予。
最终关闭及结果转交后显式清理后台整批结果引用，保留 metadata；unknown／未消费结果不提前归还。
同一原 chain 的部分退休可在当前进程重试，改变 producer 或证书不能接替原授予。
[接口](src/execution_provider/adapters/README.md#rolling-service-grants)与
[检查记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#rolling-service-delivery)
更新此前 1ae 阶段的“未接 rolling SDK”状态；普通无池缺省、原生模型参数及历史路径未随此变更。
此前独审发现的数字／布尔值快照混淆另由共享严格 JSON 比较修复，原失败保留。
候选尚待准确组合独审；rolling CLI 保存、自动物理 RAM、跨进程恢复和真实 Ray／模型／GPU 仍未完成。

rolling 结果交接修订候选另从 99aa 建树：无 expected 的非空取回先返回行，后续 close／原池 retry_cleanup
才执行可能失败的退休；内部取回中断会恢复原交付状态。显式 expected 仍表示调用方已有该快照。
清理现最后才解除 producer，避免中间中断留下不可继续的 iterator／授予。
[修订及原反例](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#rolling-transfer-repair)
保留 99aa 和独审证据，未改变普通无池、原生 baseline、模型参数或未知 RPC 的责任。

批量观察 CPU 探针另修模拟 IPC 缺少 ready_report 的问题：与实际异步引擎共享就绪字段的含义，
同一 owner、原生样本和年龄证明一并核对；原 off/on 失败及修订后的检查分别保存。
仅修改测试夹具与检查，没有放宽生产采样、批分页、ACK 或未知 RPC 的处理。
[诊断记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#bulk-probe-ready-report)
说明服务器较早源码的失败及最新组合的复现；5175 原候选及其独审证据保持不变。

主机内存继续重构候选从 9da 独立建树：现有 require_memory_feedback 选项可接模型 actor 可见 cgroup v2
的内核停顿／回收事件，保留单一 snapshot RPC、实际查询／来源时钟与独立 safety 序号。默认要求仍关闭。
首次、缺项、旧来源及读取错误不授权增长；原生统计未更新也可接收已确认的独立压力，已有任务／结果占用保持。
[实现和分层检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#host-memory-pressure)
不提供物理 RAM 授予、RSS 上限、完整关系存储或跨进程恢复；真实 Linux、Ray 和模型验证仍未完成。

Ray 资格入口另修正测试总数写死为五项的问题：按实际方法结果确认原五项及其他选中方法均通过，
不再解析日志的总数；监督、完整输出和原清理责任保留。
[修订与本地检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#ray-qualification-results)
不修改服务器此前停止记录，也没有在本机运行真实 Ray、模型或 GPU。

普通 records 所有权另有小批精简候选：等待期限由原基类统一处理，服务额度读取已初始化字段，
结果声明只整理一次身份。151 项相关 CPU 夹具检查通过，公共导入、异常类型及未消费结果责任保持。
[实际范围与暂缓项](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#ownership-simplify)
保留运行时绑定的错误处理差异；此批未推送、合并或运行真实 Ray／模型／GPU。


未接入现用执行链的 `BoundedStageWorkController` 和专用 `WorkCreditDecision` 已移出生产模块；
原三项行为测试检查已有历史原件，五份具名代码投影保持准确字节。供给／请求控制器和
显式历史 profile 在这一早期批次仍保留；后续已按本页退役记录移出现用入口。
[退役清单及 CPU 检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#stage-work-retirement)
同时记录两项旧测试字段断言的更新，没有改变模型、资源或查询条件。


已单独独审的普通 records 精简与旧工作量原型退役现组合到 a255574f；八份 Python 文件与各自已审候选
逐字节相同，文档保留两批和内存压力的各自记录。
[组合检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#reviewed-cleanup-composition)
的 21 模块 317 项通过。测试资格修复和第二轮精简保持独立，没有混入本次组合。


上述已审清理随后接到已发布的 c5ebddc8，保留其 Ray 资格修复七份文件的原字节。
[新来源组合](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#c5e-cleanup-composition)
完成 73 项相关 CPU 检查；清理部分八份 Python 与原 317 项通过的候选相同，不重复运行同一组。
第二轮内部标记、配置解析及 JSON 类型候选仍未混入；原 9da 服务器对照保持。


服务 rolling 的内部退役交接另去掉可变字典及其镜像校验：只保留原对象标记，
实际 producer、完整未消费结果、RPC 终态与私有载荷清理仍由原路径确认。
[第二轮精简检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#retirement-marker-simplify)
覆盖原对象重试、不同标记、未消费结果和 unknown；没有增加控制器或修改容量配置。

配置解析另有最小精简候选：CLI 复用同次 IPC 解析结果，共享解析器复用已展开的 profile。
显式／省略报告、兼容 helper 及启动前检查保持；21 项相关 CPU 检查通过，未推送或合并。
[实际改动与保留原因](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#config-parse-simplify)。

观察工作流的配置核对另修数值／布尔值误判：复用既有 JSON 比较，拒绝 seed 从 0 改为 false 后借用旧回执。
原失败与改后 27 项 CPU 成功见[诊断记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#workflow-json-types)；
相同配置及键顺序变化仍通过，本地候选未推送或合并。


第二轮三个已审候选另在包含第一批的 0f2e2141 上组合：内部退役标记、配置复用和工作流 JSON 类型比较
保持各自已审字节，第一批和已发布 Ray 资格修复保留。
[170 项组合 CPU 检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#second-cleanup-composition)
全部通过；没有新增测试方法、控制器或配置项，未改变 9da 服务器对照。


Ray 画像工具另从 9652108d 独立整理：提交与 actor 状态留在 `profiling/ray.py`，
拓扑、调度选项和既有执行器组装移到 `profiling/ray_scheduler.py`；无生产调用的旧参数包装删除。
两文件合计净减 37 行，原／后相同 194 项 CPU 检查均通过；历史五树按原摘要重建。
[范围与剩余项](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#ray-profiler-layout)
说明此树未叠加待审退役候选；真实 Ray、模型、GPU 和 Ruff 未运行，没有新增控制策略。


Core 调度文件另有独立职责整理：原七个配置／结果／提交接口定义移到 `scheduler_types.py`，
原调度器继续负责执行、输入线程和资源记账，原公开导入保持。文件从 1,125 行变为 981 行，类型模块 165 行；
必要导入使对应生产代码净增 17 行，未把搬动当删除。[原／后同 236 项 CPU 与历史恢复](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#scheduler-types-layout)
通过；Ruff 和真实执行未运行。本批以 965 独立保存，未叠加前两份候选，组合后再核总体数量。


两份职责整理现以已发布 d559b942 组合，17 个已发布清理生产文件保持原字节。
[236 项组合 CPU 检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#layout-d559-composition)
全部通过；Ray 提交／组装与 Core 调度／类型各自分开，实际调度器和公开调用保持。
本次结构整理净减 20 行；与 3fd 的相同核心口径合计减 1,457 行，较固定 main 仍多 6,080 行。
本记录只覆盖职责整理组合，新的配置行为修复在独立树处理，没有运行真实 Ray、模型或 GPU。


不设应用 B 的 rolling 准备请求现接到原有执行链，固定或现代资源反馈均可组合 interleaved／event_driven。
模型请求、MethodDriver、Core、IPC 与结果持有者不变；源关系仍先完整建立，未实现 CSV 读取交错。
[实际修改及 CPU 原件](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#prepared-requests-no-token-packing)
记录四种原拒绝配置现均通过且不分配 actor，197 项组合中 195 成功、2 项 Reference 夹具组合问题待处理，
服务小增量 21 项通过；不加总重复方法，不称整轮通过。引擎默认及旧 HTTP 退役另树处理，未推送或合并。


11 个生产消费者现直接导入已存在的中性脱敏／私有文件实现，删除经过 baseline 导出的反向导入。
[来源与检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#neutral-shared-tool-imports)
没有新增函数、包装或校验，也未减少生产行数；原 117 项中 115 成功，历史格式元数据修正后恢复检查 4 项通过，
另一项原 Kalypso 夹具错误保留。历史身份不改，真实模型、性能和其余语义能力迁移未据此验证，未推送或合并。


<a id="current-runtime-cleanup-composition"></a>
当前组合以 646216fa 为源完成旧 Movie HTTP／固定 campaign 退役、两批职责拆分、
无 B 的准备／请求反馈接线及 11 个中性工具消费者迁移。`auto` 配合资源反馈现在使用原有初始许可 1，
上限仍来自实际 owner 的请求／IPC／结果声明；不会把上限设为 1，也不代表物理自动容量。
[准确检查范围](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#current-runtime-cleanup-composition)
记录首轮 721 方法中 717 成功、修正旧测试预期／路径后 26 方法通过、混合关系检查 1 方法通过，
以及 auto 配置增量 84 方法通过。重叠不加总，721 个不同方法均有通过回执，未二次全量运行。
默认 R 与存储仍有限；真实 Ray／模型／GPU、物理容量及剩余大文件尚未完成，候选未推送或合并。


### 普通 IPC 省略值

以 e7e593d6 为源，普通与 rolling async 共用同一 IPC 值解析。省略结果／命令槽按最终 IPC 请求数，
省略输入／结果总字节按各自完整单项补齐；完整独立预算及显式字段保持，不再被 8 槽／16 MiB 压低。
C 的显式／auto 值与动态起步含义不变，vLLM 参数未改。R 默认仍为 2；省略存储随声明请求数变化，
完整预算和显式存储可独立设置，尚非物理容量探测或完整 R 解耦。
[原反例和 CPU 检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#ordinary-ipc-omissions)
保存两个改前失败、改后通过与 151 个相关方法通过；真实模型、Ray、GPU 和性能未测。


同批 `shared_credit.py` 已合并三处相同的活动租约统计；端点／Job 筛选、工作量和 SAOR 逐轮授予保持，
没有新增缓存、状态或检查。两份生产源码已审，组合检查与可比代码量见
[组合记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#ordinary-ipc-shared-composition)。


### 测试与实验的时间安排

`campaign_guard` 不再因剩余工作时间小于单项最大时限而拒绝启动，实际整轮监控与清理保持。
[反例与 CPU 范围](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#campaign-timeout-upper-limit)
分别记录初轮权限错误和复跑；其他实验入口另行核查，不影响运行中的 GPU 任务。


整轮监控现通过 controller／CPU／collect 的实际监督调用显式传递，不再改写模块全局函数，
也不保留该旧机制的防嵌套检查。[增量记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#campaign-direct-monitors)
说明独审发现、缓存函数别名、已观察进程登记及原清理；真实服务器与模型仍待单独验证。


三份现用 probe 已共享实际剩余时间，取消夹具已接上显式监控；保留最新采样修正、原生 LOTUS
本地 IPC 及旧路径退役。[最终组合记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#experiment-timeout-composition)
提供 147 项 CPU 检查及按同一口径的代码量，未据此验证模型或查询性能。默认 R、物理容量授予
和剩余大文件整理仍未完成。


融合批传输与迭代已从链查询 owner 拆到 `lotus_chain_fused_runtime.py`；查询配置、组装、
阶段切换和最终资源清理保持原 owner。[职责整理检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#fused-chain-runtime-roles)
使用相同的现有 CPU 行为用例，未新增控制器、策略或容量，未验证模型与性能。

该职责切片已与 f23 的 LOTUS 方法／值整理组合，新模块直接采用 `record_types`；
相同 11 项 CPU 行为再次通过。生产默认与资源责任保持，详见同一记录的后续组合。


异步模型 actor 与 driver RPC 状态已分文件；重复 rows／ACK 条件、共享额度子包的提前导出及旧测试预期已清理。
Session 的无状态占用投影归入既有策略模块，整数身份上限共用原协议定义。`R=None` 可复用完整 typed IPC 声明，
默认 R 仍为 2，尚非自动物理容量。[本轮源码和组合检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#runtime-duties-0657)
保留 408 项中的原失败及单项修正，未运行本候选的真实模型或服务器测试。


共享额度按账本与策略职责分文件，公开事件仍由原 coordinator 生成；非法完成工作量先拒绝，租约可用正确值重试。
删除已完成 Job 的重复占用扫描及两处内部配置检查；Session 不再复制 registered Job 的关闭标记。
[准确源码与组合检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#credit-session-duties)
记录 140 项全部通过，unknown、结果租约和既有异常收尾保持。本批没有改变 R、反馈试探或物理容量，
未运行实际 Ray、模型或服务器，其他大文件仍需继续整理。


Session 派发与 flow 生命周期分文件，单请求游标前移收敛到实际接收后；record-local 传输类迁到既有 backend 文件。
无调用的 581 行 profiler 解析器退出维护源码，准确源继续可恢复；近期测试扩展漏补的五条历史来源已修正。
[本批证据](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#core-duties-4679)
记录 Session 144 项与最新试探组合上的链 65 项通过；原内存反馈超时原因仍未确定，完整历史恢复仍有十处来源差异。
本批生产 Python 净减 570 行，未运行模型或服务器；多任务／SAOR 独立整理另行推进。


链 owner 不再直接读写运行器的三个阶段数据容器；record-local 清理与实际持有判断、fused 输入／session 转换各归其运行器。
owner 保留阶段选择、结果记账、原交接凭证和真实清理确认。根规则的脱敏路径已指向现用实现。
[准确组合与证据](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#runtime-owned-query-state)
保存 50 项组合全部通过、一个私有旧 pickle 专属测试退役及两项同步 CPU 子进程检查的 sysctl EPERM。
原 unknown 与十处历史来源差异保持，未运行模型、GPU 或服务器，未改研究策略。


SessionEngine 与 flow 的实际实现按服务回执和消费者生命周期分文件，仍使用同一任务表；
接纳中的三个存储用量递增收敛为一个表达式，严格工作量类型检查只做一次。
[本批记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#session-flow-ownership)
保存原成功 176 项的同选择回归、另 25 项交付／额度检查和五项改前 socket 错误；不把未运行部分算作通过。
公开类导入和运行时类型注解保持，两处诊断源码清单与真实测试接点已迁移；未改策略、容量或默认配置。


socket 测试夹具现在先报告启动原异常再交出路径，启动失败也进入原 stop／join 清理。
[一次最小检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#socket-fixture-startup)
明确停在 `listener.bind()` 的 `EPERM`，不再误报为客户端 `FileNotFoundError`；五个原 socket 方法仍未通过。
一个模拟启动失败的行为测试通过，未重试实际绑定或 sysctl，未改生产代码、时限或跳过规则。


普通 Filter 输入／结果迭代已从模型 owner 移入 records_filter_runtime，原 factory、26 个方法、ACK、租约与收尾处理保持。
[同选择 CPU 记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#ordinary-filter-runtime)
保存改前／改后各 81 项通过；没有新增测试或资格条件。原 socket 与 sysctl 未验证状态不变，没有重试被拒操作。
owner 781→539 行，运行模块 259 行，生产 Python 净增 17 行；历史来源仍有十处旧差异，未改策略或配置。


Core 与静态 reference 的 Native CSV 扫描准备共用 records_scan，原重复块和专用 os 导入已删除。
[本批检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#shared-record-scan)
记录同选择的 108 项改前／改后通过、一个类级跳过对应三项未运行；原因是没有配置 SEMBENCH_CHECKOUT。
Core evaluate 119→98 行，生产 Python 净减 13 行；原方法预检、模型启动、迭代及批次不变，未触碰并行 poll／ACK 文件。
原 socket／sysctl 拒绝没有重试，历史来源仍有十处旧差异，未取得模型或性能结论。

异步 prepared 批的已保存成员 ACK 合并到下一次有界 poll，末页也确认排空；页内容、重读、
sink 拒收、unknown 用量和模型借用责任保持。已完成查询先排空最终 ACK 再关闭 actor，
中断仍执行原清理，未完成工作保留原取消／回执恢复顺序。没有新增容量、策略或 CLI 开关。
[定向机制测试](tests/execution_provider/test_lotus_batch_poll_ack.py)及观测开关／footer 检查纳入
354 项 CPU 选择：349 通过，四个同步 prepared-reuse 方法和一个实验控制入口方法在准确基底
`d0b19dbb` 同样受 PermissionError 影响，未算通过。3010 条历史绑定仍有原十处失配，无新增。
本批未运行真实 Ray、模型或 GPU，不能据此认定原 Q1 时间差已消除；同步热点日志尚未改动。


同步调度器的阻塞输入生产移入 envelope_source，两条调度路径的服务池选择共用原优先顺序；
普通路径只在实际被选中后创建账本。两处由配置和公开入口已建立的内部断言删除，结果关联、
接纳、额度与 unknown 清理保持。[本批检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#scheduler-source-duties)
记录同一组 76 项 CPU 改前／改后全部通过，生产 Python 净减 2 行；历史仍有十处旧来源差异。
没有修改并行结果 poll／ACK 文件或研究策略，原 socket／sysctl 未验证项保持，未运行真实模型或服务器。


本地在途占用投影归入已有 topology 模块，调度器三个实际调用和既有测试已迁移，旧静态方法删除。
原端点查询、投影函数体、端点接纳及指标采样顺序保持，没有代理或额外状态。
[同选择检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#local-occupancy-projection)
记录改前／改后各 89 项 CPU 通过、历史五条来源映射及原十处差异；本批没有改 R、资源授予或物理容量。
生产 Python 净增 5 行；未运行真实 Ray、模型、GPU 或服务器，原 socket／sysctl 未验证状态保持。


融合批关闭先完成原私有目录删除，再保存 closed 报告，修正删除异常后重试直接返回旧成功报告的问题。
[最小 CPU 反例](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#fused-artifact-close-retry)
在原生产代码上失败，修正后与原九项审计生命周期用例合计十项通过；确认目录被再次删除、调用记录和原模型关闭凭据保持。
生产只移动两行，没有增加状态或检查，没有改 prepared／常驻模型关闭；真实 Ray、模型及原权限未验证项保持。


旧 profiler replay 的八个无现用调用接线函数与专用导入退出维护源码；五个实际 Arrow 转换函数归 batch_envelopes。
Ray SDK 和现用测试已迁移导入，两个仅由旧接线使用的私有关键字参数删除，共享算法与协议字段保持。
[本批记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#retired-profiler-replay)
保存同组七项改前／改后通过、845 行原件及五条历史来源；生产 Python 净减 707 行，十处旧历史差异未增加。
未改 fused 修复、策略、容量、模型默认或并行 poll／ACK，没有真实 Ray／模型／性能结论。
