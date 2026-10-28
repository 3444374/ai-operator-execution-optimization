# 实验与机制证据台账

<a id="finite-query-token-local-cpu"></a>
## 2026-10-28：完整F/M查询token配置的本地接线检查

基于`77fe087b`的独立本地候选，仅在查询owner传递现有`ChainTokenBudget`，未改49d的预算、FIFO、actor backend或估算算法。
126项相关CPU检查通过，含[12项新检查](../../code/tests/execution_provider/test_lotus_record_local_tokens.py)；使用原LOTUS、微型Hugging Face分词器、Core及CPU子进程通信，Ray RPC和模型推理为替身，模型/GPU/SSH调用0。
另3项历史还原检查通过，五个具名版本的逐文件摘要和身份保持，未改历史清单或49d原件。
覆盖默认count、显式token、实际生成列成为后继输入、输入估计加阶段输出cap、W低于C时阻止/终态后恢复、取消unknown、未确认关闭保留W及晚到终态不重放后继。
只支持同部署同角色F/M完整查询；Extract、角色切换及fused在token模式启动前拒绝，count路径原支持保持。
候选尚未合入或部署；原[49d真实可比性失败及负结果](#token-c64-comparability-gate)不变，本次CPU检查不增加模型质量或性能资格。

<a id="animals-q7-shared-media-native128"></a>
## 2026-10-27：原 Animals Q7 的共享图像表示正常功能

运行`q7-native128-retain-895c3a-20261027-r1`及独审对应冻结来源
`9dcfe589d550bf782a8fd4440af18a22aaf5f59b02b7726f13f43222407cdc82`（1112份code文件，非Git提交），
基于`2cc7b340`；当前main适配单独检查，不改写原运行来源。Qwen2-VL-2B-Instruct固定revision
`895c3a49bc3fa70a340399125c650a463535e71c`，仅LOTUS native、64原图、128 MiB URI保留单臂：
128请求全部success/stop，无unknown或重试；编码64次、命中64次。最后消费者正常结束后保留字节、活跃借用和待消费项均为0。

原SQL evaluator最终City集合正确，不表示逐图Species判断全对：Species有23个假阴性、4个假阳性，
各City仍保留真阳性。没有关闭保留的性能对照；真实unknown／abort未测试，逻辑引用归还不证明RSS或GPU内存回收，
也不认证其他执行后端或通用依赖图编排。默认关闭，开启须显式配置且核对实际模型、生成和图像处理身份。

原始材料留在仓库外，按运行ID及摘要取得；216196618字节的请求原件留在原机器，SHA256为
`2ce486fe4b9d2a9e84b00327510cd223b80c515a161f78bdc4331a32360960cd`，不进入公开代码。
运行交接摘要SHA256为`a8b7c7e2264a6c5247400fd3eea175dbdaed550d7907ceda17d075240401b8cd`，独审回执SHA256为`223ee8e9855ed219676f58215b3061d83255aac7c495b93f65b0bc5b5b48cdec`。

<a id="records-dynamic-sad24-621"></a>
## 2026-10-27：显式动态入口的短查询与提示复用

运行`semloom-sad24-session-20261027-r3`及其独审只认证冻结运行来源
`6210499fd178b33f799892722461de5098db9ea1117443e45557c04e9e10c6b4`（583份非测试Python，含4份未改PG fixture）；
packet SHA为`613211f6620b7edb689826a553933c6add957c53e575c8d921282f30d0ac3c61`。
当前main整合保留PR63的公共关闭函数，另做CPU与历史还原检查；不改写原实机来源，也不借此宣称新组合整体实机通过。

固定同步参照、固定异步与显式动态异步三组各完成first/reuse两次查询，共6查询、24次生成全部成功，失败／unknown／缺失均为0。
实际RequestOutput的输入IDs、消息与调用身份相符，prompt/completion usage合计2934/96 token；各组两次查询复用同一模型实例。
同步参照在Core之外按有限batch4执行，不是Core支持batch4；批API数不作逐行独立延迟样本。
动态组反馈样本、探索、调窗均为0，仅证明短查询安全返回，以及初值2的提示保存与精确复用。
不能据此声称真实反馈响应、搜索收益、完整质量或稳定性能；reuse时间不能归因于hint或prefix复用。
旧24额度中已领取的8次记录保留，本轮新24额度独立核对；105个所属进程身份完成退出，租约释放。

原始材料留在仓库外，可按运行ID及摘要定位：`RESULTS.json` SHA256为
`2fa91dc5a4cb17e439903ca946eac397ed0e2b1e26f0e68f8cd9a8a7798a40e0`；
167项材料核验的独审回执SHA256为`2094198d3ef13f3dc376a032e7503e2c541ed292ce069ed7cc99b13d9b995b3f`。
公开代码不包含有机器路径、现场账本绑定和原始输入的私有目标包。

<a id="record-local-query-owner-c827"></a>
## 2026-10-27 c827：完整单模型查询与F→M提前推进

来源是独立运行快照`c82714bb980c3cc5befc0083801e1c0de1ffee12aec57b292085121e6b4e64b1`，
421个运行文件，不是Git提交；本地累计代码接纳与该来源实机范围分开记录，旧fused和baseline未改。

|运行|实际工作|成功／失败／unknown|完整caller|功能事实|
|---|---|---|---|---|
|`record-local-owner-c827-gpu-20261027-r1`正常单元|3行M→E→M→F，共12请求|12／0／0|41.121750s|一个模型跨三段，原参数、token和关系回放一致|
|同轮独立取消单元|一个新owner的1行Map|0／0／1|39.088032s，抛出|IPC queued后取消，unknown不改写|
|`record-local-fm-c827-gpu-20261027-r1`|3F+2M，共5请求|5／0／0|39.004460s，含最后事件排空|第0行parse→ready 2.183ms，ready→driver submit 6.925ms；与另一Filter模型API重叠88.156ms|

首轮领取13、实际IPC13，完整生成数仅能给12–13；第二轮领取6、实际5、已领未用1不复用。
合计17成功和原1unknown不表示18次已确认生成，取消未知token不能填0。每个正常查询仅n=1，
不同链和计时细节不可相减宣称加速；两轮均无自动重试和额外预热。caller已含各自加载和关闭，
监督全程分别129.939272s／63.713184s，Ray初始化、tokenizer准备、返回后审计和退出在caller外。

两轮分别核清61／31个自有出生身份，不把它们相加冒称跨轮唯一数。各轮GPU显存1MiB、compute/RPC为0、
本轮租约释放，旧SHM保持；逻辑结果unknown与资源退出分别报告。未验证原生运行中abort、跨查询复用、
独立语义gold或广泛性能。成功API样本n=12／5、B1，最大0.850724s／0.234626s，不报可信P99。
正常caller采样n=38／37，GPU busy点均值9.79%／7.78%、峰值均100%，显存峰19552MiB，
功耗峰254.21W／203.91W；采样点首尾跨度／caller为96.24%／98.31%，不是连续覆盖。
名义周期1s，实际中位1.072261s／1.071398s，缺项／失败0。数据含加载和关闭，短模型区间采样稀疏；
净观测开销未测、未事后扣除，MFU均N/A，不能用busy替代。

私有原件按运行标识和下列报告摘要取得，不把原始提示日志、服务器信息或本机路径加入公开源码：
M/E/M/F报告SHA256`bbdd97afd681ae601803cbf548630d55d392b9bbb375d3fd173bb1be7c7fc6f4`，
F/M报告SHA256`7a414e11307c3f18cea6a54e6acb5e9bbd2cd1daabd77bfed5512654270b6baa`。
两份原始报告及取消unknown不改写；这些功能证据不替代组合源码回归和代表性性能实验。


<a id="token-c64-comparability-gate"></a>
## 2026-10-26 C64 token准入：机制成立，可比性门控停止

运行`records-chain-budget-c64-abba-49d-20261026-r2`固定421文件内容指纹`49d05984229d551401f221461c60ccd5940dfd1943d8b8198e28b2829c738830`，基于Git main66754a8f；指纹不是Git提交，该逐记录Actor／IPC链候选未进main。它执行真实派生Movie的Filter→Map，不是完整SemBench Q1／Q2／Q3。B1／H1、engine容量64，count C64，token C64／W8192；每格完整query n=1，只有mixed-first两臂配对，其余count查询保留为无token对应的诊断。

- **机制证据：** 36个不同W阻塞状态、16个不同候选恢复。首例活跃61<C64，预留8175+候选115>8192；一次确认成功终态释放148后，同候选被接受，预留变为8142。接受后的60条终态未算作恢复前证据。W是完整输入+输出cap的估计预留，不是物理KV／显存上界，也不是GPU kernel并行度。
- **严格可比性未通过：** 两臂353对调用输入token IDs全部相同，Filter保留集合均97行、Filter质量相同；但29条Map文本／输出IDs不同，14条输出长度不同，4条finish_reason不同，输出token合计3420／3432。没有Map gold可判谁更好，不能称Filter质量下降。已按原门控停止后续矩阵；私有观测开销未事后扣除，不声明公平策略排名或稳定收益。
- **完成量与时间：** 本轮1817成功=1777正式+40预热，模型失败／unknown均0；这不是1817个完整查询。唯一配对mixed-first caller为5.344680／6.611279秒（token慢23.70%），带观测且输出不一致，只作诊断。353是每臂独立B1记录／阶段API样本数；API经验P99不能代替query P99。新轮领取2624，未用807不退款；旧冷导入失败轮领取2080、实际0及其预算原件另留，旧单元未重领或退款、失败不抹去；新轮使用独立恢复账本。
- **资源与责任：** 63个观测到的自有出生身份全退出，GPU计算进程0／显存1 MiB、活动RPC0、租约释放、采样器停止，SHM基线不变。仅确认本轮正常收尾；不能清零旧取消unknown，不能由物理退出授予原生运行中取消资格。全程／查询GPU观测和MFU边界按原报告，busy不等MFU。

前置`records-chain-token-main49d-qualification-20261026-r1`已走Actor／IPC与原方法回放：正常路径12成功，另1次取消unknown保留、实际执行数12–13，未用领取4不退款；它不是旧r5 direct-engine资格的转授。当前新增的是独立49d候选的真实接线和W机制证据，不是main已接纳逐记录FIFO／token、Extract／cascade或完整质量性能资格。门控恢复任务已终止；后续只先处理可比性与证据缺口，需新计划和预算后才能新增模型运行。

报告SHA-256 `e2e6577fb16da6123922671103eefab38fe1a453e172bcd64e1dc3659104f78c`；紧凑原始144文件压缩包SHA-256 `8fc2f8982583e4a6bb126353e44f883456aba0d204604d97302b7b6fee4e4706`，按上述运行标识经既有artifact交接取得。已有独立增量核对复算调用量、W恢复前终态、输出差异及63身份清理；本次文档同步只核回执，不重跑模型或替换原始记录。

<a id="legacy-work-line-archives"></a>
## 2026-10-26 旧开发线的具名历史恢复引用

当前维护代码由PR52／56选择性接纳；以下tag仅保存完整历史，不表示旧分支整支合入main或授予新源码实机资格。三个tag均已从公开远端实际fetch到独立验证ref，并核对准确末端；旧工作分支的删除审批未通过、尚未执行，tag不替代这一批准。

| 来源与用途 | 持久归档tag（均以`archive/semloom/20261026/`为前缀） | 准确末端 |
|---|---|---|
| 旧请求级baseline与可选方法来源 | `legacy/request-baselines-and-optional-methods` | [e9a54e3e43ef9ce9e3b6973530715655028b1da5](https://github.com/3444374/ai-operator-execution-optimization/tree/e9a54e3e43ef9ce9e3b6973530715655028b1da5) |
| Movie任务开发／旧准备封包与CPU记录 | `reproduction/records-development-history` | [a8d6acd253cfa9c63b9a2d9d1c711518b7ec8a05](https://github.com/3444374/ai-operator-execution-optimization/tree/a8d6acd253cfa9c63b9a2d9d1c711518b7ec8a05) |
| Native Q3历史失败与复验血缘 | `evidence/native-q3-history` | [452505e80f7d13dcbfbc838fe7fae6f127790b62](https://github.com/3444374/ai-operator-execution-optimization/tree/452505e80f7d13dcbfbc838fe7fae6f127790b62) |

Q3的两份`native_q3_*_20261020.json`已由PR51原字节入main，旧failed与新复验分列；该来源无相对Movie独有执行器。Movie的旧`deploy/autodl/sembench_records/`及`movie_task_switching_cpu_20261020.json`留在其归档树：11文件隐私形状检查无新增问题，541项runtime lock匹配历史树；原记录35项受影响CPU通过、模型／预算分配0，未在本轮重跑，不改成当前资格。

旧baseline尾项归属：main已有的原生CLIP图和SDK／请求清理继续维护，RayHTTP身份防护已PR57接纳；额外Ray Serve CLIP是独立`diagnostic_reference`，固定版本PZ是另一方法的可选baseline，二者当前仅归档保源、未接入main默认，不称已替代或无效。后续若需要它们，按功能提取适配、配置与测试并单独验收，不能借旧实测授予当前records图文／方法资格。其余旧prepared API、Sema／DuckDB／IMLane及HSE／SAOR历史同样保留来源，不改变当前策略。

完整三末端恢复bundle SHA-256 `7b6a8a16d2504336e31b481ba4bd3dca9369334520a6969a776f9cb32c7769e5`，仓库外原件按`legacy-final-retirement-20261026-r1`交接；642140102字节，bundle verify通过。已有7fb data-execution历史仍可从原archive tag及上述Movie／Q3血缘取回。没有永久丢弃唯一内容、强推、重算实验或启动GPU。

2026-10-26：[共享链结果合同归位](../../code/INFRA_STATUS.md#record-results-migration)。固定`45d184df`的独立后继单模块迁移，旧import同对象、六类行为差分与旧collector pickle读取通过；17项新增CPU检查及51项既有回归通过。首次`-S`隐藏既有依赖的测试错误保留，仅修正测试启动方式。无新模型、原生图或GPU运行，无性能/内存收益结论，不转授records资格。 完整树的历史复现来源漂移由七份旧字节快照和33条override路由修复，expected SHA与projection ID保持，五投影另作CPU还原核验。 当前最终后继71个不同CPU用例通过、0跳过（原68项迁移/方法与3项还原回归，不重复累加）；五投影完整文件摘要核同。 本地汇合e2bc后保留双方路由，相对45d共新增43条override；71项CPU在整合树复验通过、0跳过，不与原回执重复累加。

<a id="pr52-g1-negative-records"></a>
2026-10-26：固定45d的records G1窄功能范围。两轮实际14次成功调用仅覆盖Native resident、Core默认同步及显式async的首次/复用和两条eligible负例；新轮8次无失败/unknown/missing/重试，旧轮私有审计失败仍保留。完整资源退出与调用确认分列，不证明正例质量、Map/Extract、P99或稳定性能。[PR52](https://github.com/3444374/ai-operator-execution-optimization/pull/52)已合并；该条只登记G1，其余入口从各自后续结果取资格，见[代码状态](../../code/INFRA_STATUS.md#pr52-g1-qualified-scope)。

G2运行`records-production-ray-daft8-45d184df-20261026-r1`另有8真实请求成功（4个batch API各2请求）、0预热/失败/unknown/missing/重试；45d累计22成功。Ray同runtime、每query新model UID并退出；Daft显式单线程、同model UID跨query后关闭。106个本轮身份退出及GPU/RPC/租约释放分别核验。每格n=1、两条负例，未转授isolated、其他后端或性能结论；原件按运行标识及HANDOFF SHA-256 `b1976a373ccba25d8c603cb51526a04729e0524f2c96c0dd859810bfb1810b18`经既有artifact交接取得。

原45d的8处预检/说明漂移，加上最新main的RayHTTP/importer差异，在独立整合树中共需18条历史override。Git整合者回执确认4份精确旧快照、18条路由及五投影实际materialize全部匹配，原hash/ID/既有路由不变；原45d与[PR56](https://github.com/3444374/ai-operator-execution-optimization/pull/56)未改。该回执只证明历史代码还原，不认证新模型/执行闭包，也不重算或改写历史观测、发布原始运行材料。

以下两条仅记录e8eb初期预检与CPU交付时点；新G1不覆盖或删除其原失败，也不与383/80项CPU相加。

2026-10-26：records e8eb首阶段资格停在模型前公共预检，CSV-only被旧profile强制要求DATABASE_URL；0模型调用、0预留，尚未完成输入/模型/资源资格。后续按[能力合同](../../deploy/runtime/README.md)修复DB可选检查，不改旧失败或继承实机资格。

2026-10-26：[records生产闭包review状态](../../code/INFRA_STATUS.md)、[CPU资产与范围](../../code/tests/records_cpu.md)。原e8eb整合源码383个去重CPU用例通过、0跳过；旧失败输出及两次调用环境修正保留，不叠加旧r1/r2计数。固定真实输入不把合成响应变成模型质量；无新GPU/SSH/预算，本次源码独立模型与资源资格仍为merge前置，历史投影按[具名来源](../reproduction/native_baselines/README.md)解释。

<a id="priority9231-small-pair"></a>
2026-10-26：后继优先的隔离小对照。

运行标识`continuation-ready-priority-gpu-9231-20261026-r1`，策略父快照`9231f088`、实际源码快照`d7f0245c`；
它们是内容指纹，不是Git提交或PR52当前源码资格。仓库外`HANDOFF.json`的SHA-256为
`d9fdaf02dc27f4359f2b37461766bdbf237683e519af66928238caadc7cb934c`，结果摘要为
`dc03850f281ff505f5917c18707471b829e88e05ddaaebec7290cc2c3a2e7949`；原件按运行标识及摘要经既有artifact交接取得。

| 选择方式 | 首次完整查询（秒） | 复用完整查询（秒） |
|---|---:|---:|
| FIFO | 1.063662 | 1.034930 |
| 后继优先 | 1.147095 | 1.166761 |

每格仅1次查询、16行派生输入，首次与复用合计慢10.26%；6次真实非FIFO选择，配对输出与token一致，
主对照88次成功（含8次预热）。本轮无收益，隔离候选默认仍FIFO，下一步仅分析已有机制记录；
不据小样本否定其他负载，不新增性能调参或模型运行任务，不将单次首次／复用合为稳定性能证据。

<a id="priority-reverse-small-pair"></a>
2026-10-26反序补证：`continuation-ready-priority-reverse-gpu-r3-20261026-r1`按priority→FIFO执行，
同一`d7f0245c`源码快照、559份生产运行文件原字节保持；私有runner r3已完成独审，44项CPU及28个子用例的核验按执行负责人回执解释，不计作本批重跑。
报告SHA-256为`18cf581bb2234e8216e72167e9505d7ceee42c851f04575d8fe0a83fd5b5dac7`，原件按运行标识和摘要经既有artifact交接取得。

| 反序选择方式 | 首次完整查询（秒） | 复用完整查询（秒） |
|---|---:|---:|
| 后继优先 | 1.156905 | 1.089293 |
| FIFO | 1.036028 | 1.013191 |

本轮88次模型调用成功（含8次预热、80次正式调用），失败／unknown均0；未运行取消探针，旧unknown不改成功。每格query n=1、16行，API n=20、P99为NA。
首次与复用合计慢9.61%，只表示两次完整查询之和；复用有1条摘要文本／token不同，输出长度相差3个token，分类相同，不能称严格同输出。
与原轮慢10.26%共同支持在当前负载停止简单后继优先的自动扩测、保留FIFO默认；不合并两波为稳定性能统计，不泛化为所有负载或优先策略无效。

原轮取消探针保留1次unknown；API close后仍有1个held请求和1项未持久交付结果，不能升级为成功终态。
独立清理确认物理GPU／RPC／租约已释放，物理退出不代替调用结果确认。原生运行中的取消与真实aging尚未取得资格；
独审定位P2为原轮私有runner关闭后漏收晚到终态；45d生产owner已有close后collect和调用收据合并，11项针对性CPU通过、0skip，本探针不构成PR52新增合并阻断。
私有runner r3的独审已完成；未以新主对照替代原取消探针资格，原unknown不可改写，也不将原88次成功主对照一律改为失败。
独审原件按运行标识`priority-cancel-impact-independent-review-20261026-r1`及报告SHA-256 `7fc0f74285cb055d51f7000d30d2943fb0138c21bb22ea2e68ce9d4e557079c2`经既有artifact交接取得。
另据执行负责人回执，records三入口小资格（G1）已完成两轮、累计14次成功；该条保留原G1时点；后续资格及[PR52](https://github.com/3444374/ai-operator-execution-optimization/pull/52)接纳状态见下文。

<a id="records-g3-small-qualification"></a>
2026-10-26 records剩余入口小资格：`records-production-g3-remaining14-45d184df-20261026-r1`固定PR52的`45d184df`、570份运行文件。
Ray isolated 2次、Daft isolated 2次、Kalypso-native 4次业务请求成功；IMLane isolated／resident均在模型初始化时出现`ZMQError`，业务调用0、该轮资格未通过；后续独立补资格见[短RPC复验](#records-imlane-rpc-qualification)。
模型业务失败／unknown均0，另保留2条未提交失败记录；不猜测ZMQ根因，不把初始化失败当作2次真实模型失败。物理清理通过，不代替未知的模型API关闭确认。
本轮不是完整矩阵、链／cascade或稳态排名资格；该轮PR52仍为draft，现接纳状态见下文。报告SHA-256为
`d5686fe0f5a87a90ac2f2c01587a35afd316a1a5c1d181d96139c05f851e2338`，原件按运行标识和摘要经既有artifact交接取得。

<a id="records-imlane-rpc-qualification"></a>
2026-10-26 IMLane独立短RPC补资格：`records-production-imlane6-rpc-45d184df-20261026-r1`仍固定`45d184df`及570份运行文件，作者源码／桥／调度不变；仅私有环境显式设置短`VLLM_RPC_BASE_PATH`。
isolated 2次、resident首次／复用各2次，共6条唯一native请求成功；3个query、3次同步B2 API样本，0预热／模型失败／unknown／missing／重试。resident保持同worker和model出生身份，初始化一次、累计预留2→4、最终一次关闭；原方法、实际token／模板及COUNT0／TN2核验通过。
本轮独立预留／领取各6，旧G3领取未用6不转移或退款。原控制器isolated成功后因自有陈旧socket目录收尾失败停止，原失败与未释放收据保留；核实所有权及物理退出后只续行原账本未领取resident4，不重放isolated、不重置原900秒deadline，最终在原起点后831.593秒释放。
最终GPU／RPC／两个短目录／租约释放，正常调用资格通过不抹去前置CPU及首次外层清理失败，也不改旧G3的API关闭unknown。每query仅2条eligible负例、query n=1，不认证正例、完整Q123、chain／cascade、取消或稳定性能。
报告SHA-256为`7cd3edd7985939fd24971ebc8686c8af4330d9ff2910e7b6874a7a6ebc9e8786`，原件按运行标识和摘要经既有artifact交接取得。无需因旧初始化失败强制隔离IMLane；`e2bc889b`的零模型V0部署来源已通过，PR52／56已接纳，仍不能把45d实测改写为e2bc新模型运行。

<a id="records-e2bc-v0"></a>
2026-10-26 records最终部署来源检查：`records-e2bc-v0-execution-20261026-r1`固定`e2bc889b331a07c6ebb7148e71c74f4106dfbbab`，9/9角色、574文件及159模块来源一致，29.477秒，模型／调用／预算／GPU预留均0；17个观测到的自有进程退出，GPU compute 0／1 MiB、RPC及租约释放。
该检查是CPU角色及模块来源探针，未启动生产推理actor，不是新source推理资格；目标环境的LOTUS可用性使原负例fixture首轮失败，私有fixture按实际可用性修正后17项通过，冻结生产源码未变、原失败保留。外部权重和完整vendor／解释器／驱动树未重新全量校验。
报告SHA-256 `8a96f649103c7f7f3bd8389039cd905b81b2601b1a6e53d11aec6850bc70eca3`，原件按运行标识与摘要经既有artifact交接取得。45d→e2bc源码影响独审及V0合起来支持本次接纳，不能改写历史模型身份。
[PR52](https://github.com/3444374/ai-operator-execution-optimization/pull/52)合并为`c0da4d2d`，随后[PR56](https://github.com/3444374/ai-operator-execution-optimization/pull/56)合并为`eed2ef64`：迁移head `58c76ee9`与71项CPU、五投影完整还原通过的`7c5f4eff`文件树一致，旧import／pickle保留；所有worker须同时部署新模块和兼容层，不能将e2bc V0标签转授迁移版。当前无配置Actions检查，不称CI通过。

2026-10-26：[离线工具CPU核验范围](../../code/scripts/analysis/OFFLINE_RESULTS.md)已随[PR53](https://github.com/3444374/ai-operator-execution-optimization/pull/53)合并为`380cfbda`。
原来源47个不同方法通过，同一31方法的三种入口不计成93个；保留两类schema的计数检查，不重算旧原始trace或认证模型质量。

2026-10-26历史证据迁入：[2026-10-20 Native Q3失败资格与独立复验](diagnostics/lotus_records_local_model_20261016/README.md)。原生产85133c6f、两次独立120调用；旧campaign失败不改，新复验仅Native单臂通过，质量不足和缺失现场仍保留。不导入records实现，不认证后来九配置或稳定性能。

2026-10-16：[Animals观测模式CPU回归](diagnostics/main_lotus_animals_20261015/README.md#observation-body-capture)。紧凑body留证与原SDK／Daft连续查询按新源码单列，旧模型与RSS证据保持原身份。

2026-10-15：[固定main Movie／Animals连续运行](diagnostics/main_queries_20261015/README.md#long-run)。
来源`eb697477`，Movie每臂19次／Animals每臂8次；首轮、后续、请求分布、生命周期与实际质量分别报告。
独立离线审计核同MFU分子及同墙钟分母；四个固定Animals客户端RSS增长尚未归因，终态进程退出不证明存活期间内存回落。
完整原件独立备份并逐文件核验，r1失败排除且保留；不支持质量等价、稳定性能或其他模型／前端资格。

2026-10-15：[main原生CLIP最小闭包](diagnostics/main_lotus_clip_20261015/README.md)。8834 CPU／SDK、源d2的66/66历史模型与main570新增40/40行实机分列；四路径八臂完整查询、像素／全向量、GPU身份、物理退出与lease通过，默认数值及HSE环境失败保留，无稳定性能结论。

2026-10-15：[main 可选 vLLM 部署资源](diagnostics/main_service_auto_20261015/README.md)。
固定候选`7b20feb5`四生产文件闭包及12次原SDK服务POST，与合并后main`2041da67`分列；新main的Mac／Linux各30项和原Movie Q2／Animals Q1四臂auto／显式1共16查询／1472实际请求通过。
新轮请求值与逐发生项判定一致，原SDK／质量／像素关联复算、物理退出与正常lease通过；Movie Q2仍FN7、Animals八格COUNT3／真值3。仅所用单卡和查询，不继承CLIP／其他来源／多卡或稳定性能结论。

2026-10-15：[固定 main 原 Movie／Animals 实测](diagnostics/main_queries_20261015/README.md)。
固定`464de926`，四臂16完整查询、9216实际模型POST及原始响应关联复算；Movie Q3四臂相对误差50%，Q1发生项判定不全相同，Animals64图一致。
仅支持同源码接入／资源／有限质量检查及描述性时间，无稳定性能或新auto／多卡资格。

2026-10-15：[HSE 身份与基础修正](https://github.com/3444374/ai-operator-execution-optimization/blob/42ba085f80dba78937a3e2b3efb3e00685401d90/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md#foundation-restoration-20261015)。

固定实现 `bdae61bb`；旧 `671a22dd` 与历史 B/C 原来源分开。CPU定向、真实Ray零GPU及同任务33图ABBA均按[独立摘要](https://github.com/3444374/ai-operator-execution-optimization/blob/3f8748fd273f5a0d5c99e31fa6663dc500a68af9/experiments/results/diagnostics/hse_static_lifecycle_20261014/restoration_qualification.json)定位。
8完整矩阵逐字同、真实forward计数和物理退出通过；NumPy／broker组件全值与原失败保留，小轮不证明GPU收益或总RSS有界。

本页保留 main 已有证据并归并重复摘要。后续集成结果从
[固定 `9c59dc9b` 台账](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)定位；
文档引用不把该分支的实现或运行资格转给 main，原始报告、全部失败及重复值保持原身份。

2026-10-15：[main Animals 图文接入](diagnostics/main_lotus_animals_20261015/README.md)。
固定`a0d1f6ac`的必要图文／容量闭包在当前main底稿重新验证，默认PG/native/wire1MiB保持；真实Ray和软件模型请求分列。
先保留原像素、发生项、SDK及COUNT/evaluator，再做同一最终main的单4090实际查询；不借旧模型范围或声称收益。

2026-10-15：[main LOTUS 文本接入](diagnostics/main_lotus_text_20261015/README.md)。
固定main底稿上采用必要四臂闭包，保留原SDK、Movie Q1/Q2/Q3和评价器；本批CPU/实际库、原数据身份和失败分列，模型质量与性能仍待统一main资格。

2026-10-15：[main Core 与完整响应资格](diagnostics/main_foundation_20261015/README.md)。
main `06854ecd` 上的七文件实现闭包通过 Linux 调度 432 项、执行层 183／192 项，9 项未配置 PG 图像检查跳过；
包含实际 CPU Ray、localhost HTTP 与旧路径回归。元数据重复 offer 的 CPU 成本诊断保留全部重复，不能外推查询或 GPU 收益。

2026-10-15：[重复均值置信区间](../../code/src/observability/README.md#repeat-mean-confidence)从 `4b1c834b` 独立移植到 main 底稿。
40 项统计/原 metrics 回归及实际 CSV→JSON 入口通过；固定 `277d8f48` 的已保存摘要影响核查范围仍按原说明解释。
本批不改原始观测、不新增模型实验；不把均值 t 区间当总体 P99 区间，独立性与覆盖条件仍需各实验自身满足。

2026-10-08：[返回等待离线及本地隔离](diagnostics/map_queued_preparation_20261008/README.md#return-wait-isolation)、[实际Ray返回分段](diagnostics/map_queued_preparation_20261008/README.md#ray-return-wait)。
原六次1024行记录复算，追加的同步guard重叠中位范围78.088%–80.260%，2512项独立区间检查通过；未作单因素归因。
两行最小复现及五次重复、记录控制与40case扩展共71个case/4107次模拟，通过原始行/连续登记/future时序/资源复算。
无登记等待时线程控制仍更慢，CPU控制的同进程GIL资格单列；生产源码及默认保持，新增HTTP/PG/模型/GPU0。
真实Ray/Daft与SQLite新增20个case/16,448次模拟actor调用通过，future回调时刻、行/持久登记/资源及跨机器备份复算。
回调至读取缩短，线程控制完整消费在即时/追加中仍分别慢79.774%/8.902%；供给和排队变化单列，不启用。
原件251份与公开55份观察独立恢复核对，正式开始前的其他GPU进程条件保留；原模型额度继续结束，不借组件数据推算GPU收益。

2026-10-08：[待准备尾块追加](diagnostics/map_queued_preparation_20261008/README.md)。
三份原公开事件字节/SHA核对与3,072行接受/借用复算通过，后三段各896次候选均为1；未采用Core动作份额修订。
新增默认False的有限队尾追加及broker编码前段扩展，保留每行请求、资源与取消；有效最小复现原先失败，全部失败及装配错误保留。
本地30个case/24,672次模拟调用完成，独立逐行计费摘要、生命周期、对象和阶段时间复算通过；HTTP/模型/PG0。
0.5毫秒准备下追加0.859秒、即时0.762秒，慢12.638%；15毫秒准备下追加1.195秒、即时1.547秒，少22.748%，不外推真实GPU。
861份源码独立上传核对、只读环境检查通过；最终本地147项中136通过/11跳过，服务器实际库147项全部通过。
库检查阶段的Ray调用替身、稀疏立即启动、取消及压力另建块分别核对；其后独立PG16项/26次本机HTTP及10查询/8224次真实请求通过。
即时与追加完整查询中位数10.069/12.686秒，追加慢25.987%，首行变慢；准备块中位数319，PSS没有支持改善，默认保持。
全部行/持久计费/服务成功/对象和RSS/PSS复算通过，配对输出差3/2/3行；独立清理确认GPU空闲，不推荐启用。
计时源码与最后形状检查修订分别登记，公开紧凑原始事件/复算归主报告，私有SQLite及原件保持仓库外。

2026-10-04：[准备分段的PG、模型与物理内存验证](postgresql/text_map_preparation_validation_20261004/README.md)。
main `380824f2`通过16个不同PG生命周期检查、累计26次本机HTTP；230项Linux检查通过。
独立真实清单在32次请求后因put前对象留存观测少记而停止，失败保持，1024行测量为0；32行关联与请求离线复核通过。
已修订对象观测与取消/put事件顺序，3个新回归及本地236项中225通过/11缺库或平台跳过。
用户另行授权的 `12ec50de`通过236项Linux、16项PG/26次本机HTTP及10查询/8224次真实请求；完整JCT中位数慢86.597%，首行变慢，PSS无一致改善，默认保持。
每1024行生成928个准备块，其中896个为单行；全部行/计费/对象及PSS从独立备份与公开恢复数据复算一致，配对输出差3/3/1行，不能声称质量等价。
新公开240记录/213归档对象5483962字节，252份新私有原件核对与独立清理通过；重复group导出和PG全日志按单一恢复来源精简。
RSS/PSS短检查采样可用，不作为完整峰值结论。必要压缩证据282656字节，156记录恢复核对，163份私有原件独立备份，服务清理通过。

2026-10-04：[单Job文本Map准备分段原型](diagnostics/map_input_preparation_20261004/README.md)。
`18e3e97a`加明确原型源码，Core只借用已接纳输入，阶段broker持有编码/准备数据，实际请求/work保持原值；默认无准备接口。
本地替身两轮各544次，相关旧路径与取消/未知结果检查通过，实际库跳过单列；Linux144项及修正探针2项全部通过。
服务器首次0HTTP失败和4次登记保留；新授权补测20个case/2,112次HTTP通过，模型/PG0。
消费小幅改善伴随首结果更慢和留存增多，默认保持；紧凑证据/复算归同一报告，不支持GPU、SQL、原生排名或多Job结论。
[合并审查补充](diagnostics/map_input_preparation_20261004/README.md#merge-review)修正取消通知与同Job归还推进；
新增5方法在原Core产生11个子用例失败，修订后230项中219通过/11跳过，HTTP/模型/PG均0，服务器原样本与新本地资格分列。

2026-10-04：[真实分批成本与单／双Job供给](diagnostics/map_preparation_shape_20261004/README.md)。
`d4edc803`独立Git副本、857份源码摘要匹配；24项检查、3,632行实际Daft／Arrow准备和2,080次本机HTTP通过，模型／PG0。
双Job约99%窗口为单行；Arrow大幅减少准备调用，但完整消费收益小，包含新建actor的完整case方向也不一致，生产默认保持。
原始事件、请求字节／计费／结果和资源复算通过；初次地址属性误调用0HTTP失败保留，修订后按新授权完成，组件未重放。
公共131成员、私有155原件独立恢复核对通过；两份公共归档1,849,603字节，服务器与独立本机原件保留，不更新真实数据库／模型或原生排名。
同题[就绪队列补充](diagnostics/map_preparation_shape_20261004/README.md#ready-queue-snapshots)完成40个case／4,160次HTTP、1,821个快照和12方法在本地／Linux的检查；模型／PG0。
双Job749个测量窗口仅17个有同Job零等待合并机会，当前不开发重排，观察开关的供给差异单列。
新增157公共成员／202私有原件独立恢复，3,412,027字节压缩公开证据归同一报告；生产源码和默认保持，准备／在途分段接入pending。

2026-10-04：[共享预付观察API与服务器核对](diagnostics/mapped_request_budget_20261003/README.md#server-observer)。
`f4646daa`加明确摘要的观察API与诊断来源；macOS/Linux各27项、Linux平台16项通过，真实Ray/Daft/Arrow与本机HTTP运行。
SDK先保留37/0的耗尽样本，再完成19/18的双调用方检查；Core累计1,040次HTTP、两个SDK检查74次Mock POST，模型及PG0。
每行计数明显减少，但完整消费略慢，正式默认保持；实际RPC、准备、全部重复、错误、未知与引用/句柄检查归主报告。
平台另完成直接HTTP/Ray HTTP 272次；公共173成员压缩证据与178私有原件独立恢复核对通过，唯一保存位置由[gateway报告](diagnostics/gateway_isolation_20261004/README.md#server-platform)管理。
这些是有限CPU/替身接入与成本信号，不更新数据库、模型、GPU或原生系统结论。

2026-10-04：[gateway多查询干扰](diagnostics/gateway_isolation_20261004/README.md)。
基于main `f4646daa`与明确SHA的诊断入口，真实UDS/登记/共享Core/Map循环运行，厂商API、表、服务和kernel peer为fixture。
40个新进程case、1,088次模拟调用、逐键/容量/退出及主指标和阶段等待复算通过；HTTP／模型／PG均为0。
相关30项中29通过，1项Linux凭据检查跳过；首次0调用装配错误及其未正常排空状态另存，不并入成功清单。
支持继续核对同步观察耦合与准备/活动名额含义，不证明实际Ray、PG取消、重启恢复或GPU收益；生产默认和执行未改。
主报告定位全部重复、来源、失败与170成员的压缩证据，恢复映射含唯一summary共171份核对；原件继续保留Git外。

2026-10-03：[同机共享预付计数](diagnostics/mapped_request_budget_20261003/README.md)。
基于main `53155f8c`，新增实验组件及本地诊断接入，生产执行与正式配置未改。
16,384次组件领取、8,224次Core模拟调用及104项检查通过，HTTP／模型／PG均为0，原始逐条编号／摘要／时间复算通过。
组件与Core替身呈正向信号，准备和退出已付费；支持继续准备统一观察候选，不证明Linux、厂商SDK、SQL或GPU收益。
源码、全部重复、慢准备、错误注入修订和99成员的紧凑原始证据归主报告，其他原件保留仓库外。

2026-10-03：[Core／Map观察成本隔离](diagnostics/map_observation_isolation_20261003/README.md)。
基于main `2b1ea713`的真实Core、异步backend和Map传输循环，有限表、厂商API和独立worker使用明确替身；生产源码未改。
主清单20次执行／16,448次模拟调用通过，HTTP／模型／PG均为0，103项检查及全部原始字段复算通过。
同观察条件下guard改变提交等待、worker活动与准备窗口；支持完善对照观察设施，不证明SQL、GPU、原生排名或成组RPC收益。
原始失败、排空计数更正、运行身份、源码、全部重复和99成员的压缩公开证据归主报告；其他展开原件按清单保留仓库外。

2026-10-03：[SemLoom Daft／Arrow真实分批对照](postgresql/text_map_arrow_real_20261003/README.md)。
main 1be050a0生产源码及独立测试fixture修订，830份来源摘要核对；10条查询／8,224次真实请求通过。
复用输入和固定资源下观察到完整查询中位数少2.793%，配对输出差1/2/3行；支持保留直接Arrow显式选项，
默认Daft保持，不证明质量等价、通用多模态收益或新的原生排名。原始失败、重复、分段和清理证据由主报告维护。
239份原件独立备份核对；120份公开记录恢复及私有输入复算一致，原始评论、SQLite和编译缓存不进入Git。
后续仅离线重放6,144行提交记录，区分逻辑接纳、Ray调用、worker及终态；不把名额占用当GPU利用率。
计算口径、全部样本和后续安排归主报告及现行查询调优计划，新增模型请求0。

2026-10-03：[就绪窗口聚合](diagnostics/ready_window_coalescing_20261003/README.md)。
来源为main `7cbc888c`及按摘要标识的本地候选；候选源码补丁已归档，恢复摘要与原记录一致，未并入运行代码。真实传输循环使用受控批次和厂商调用，HTTP及模型请求0。
同脚本复现与72项相关检查核对行为，12项实际库检查因缺依赖跳过；固定模拟成本的对照不支持启用，默认即时提交保持。
原始重复、失败、版本、执行标识及计算来源归主报告；仅支持准备粒度取舍的工程判断，不改变真实模型或原生排名。

2026-10-03：[批量持久记账](diagnostics/request_budget_batch_20261003/README.md)。
源码基于88173f85，分阶段精确摘要保留；组件、替身和真实运行独立记账，行为/关联/资源检查通过。
真实阶段10条查询、8,224次请求，计费/成功增量/访问日志一致。批量完整查询中位数慢7.31%，三对均更慢，
因此只保留诊断选项与负结果，默认同步保持；不更新原生baseline排名或声称质量等价。
全部条件、重复、输出差异和准备失败从主报告及恢复清单读取。
合并前审查补齐失败/取消的记录及公开批次编号，新增检查使用本地临时账本，无模型请求；
原执行源码与归档分别核验；修订后独立8,224次真实请求通过，同步/批量中位数10.118/14.218秒，批量慢40.52%，
配对输出相差3/2/1行，不证明质量等价。一次Ray路径过长导致的0请求失败与新授权重测分别保存；默认同步保持。

<a id="premerge-evidence-20261002"></a>

2026-10-02合并前复核：逐份比较217份实验报告，原有正文、主数字、结论和适用条件全部保留；
已有实验台账条目逐字相同。保存规则明确保留实验结论、实际对照、适用条件与数据及计算来源。
整理前4,414个直接保留的非Markdown文件摘要不变，6,134个转存路径与原件字节数及SHA-256一致，缺失路径0。
提交`146f692d`的108份清单独立恢复9,422个文件通过；两份pilot归档的42/44份科学记录与原件一致。
只有已说明的macOS附加元数据及相应归档SHA发生变化；这项复核验证保存完整性，不重新评价历史结论或运行模型。

<a id="legacy-retention-20261002"></a>

2026-10-02：用户授权后整理旧结果并准备提交。176个历史结果目录按读取与复现用途分类，
8个已有压缩目录沿用；其中108个目录采用版本化路径清单与共享归档，直接引用的数据、复算脚本及紧凑记录继续保留。
6,134个松散路径转存为4,526个唯一归档对象，归档37,825,045字节、清单3,371,224字节，
相对所替代263,627,947字节减少222,431,678字节；运行身份、全部重复、失败与完整原始观测未删减。
独立仓库外备份10,779个文件逐项核对通过；恢复工具逐项还原9,422个文件并与保存前字节比较通过。
两份旧pilot tar移出88份macOS附加元数据，科学记录42/44份字节一致；旧归档另有备份，当前SHA清单已更新。
文件恢复工具8项临时fixture检查通过，含坏摘要、路径冲突、tar链接、源链接及输出覆盖拒绝；
入口与适用存储版本见[恢复说明](../../code/scripts/README.md#实验结果恢复)。本次模型请求0，实验结论和原始计分样本不变。
共享归档可能被多个结果引用，归档数据与脚本按各结果的`raw/storage-manifest.jsonl`恢复，报告与说明仍在仓库阅读；Git历史体积未改写。

<a id="storage-review-20261002"></a>

2026-10-02：结果保存与导出检查。按本次修改前提交`eaf998d9`覆盖方法、动机与可行性三个结果目录的
10,778个已跟踪文件；21个tar归档可读取，11份存储清单的6,307项公开材料和86份旁存副本精确恢复通过。
两份Ray Data历史校准CSV从服务器原件恢复18条完整记录，复算吞吐与[原报告](motivation/gpu/ray_data_calibration_20260803/README.md)一致；
[本地比较准备](postgresql/text_map_comparison_preparation_20260928/README.md)补充与原SHA一致的压缩日志，修复忽略文件引用。
[历史dry-run](diagnostics/README.md)的12/14列混合格式已标注，原始输出继续保留。
逐文件字节相同副本共292组、1,703份额外副本、4,303,894字节；其中跨运行配置与观测仍需保留来源映射。
两个旧tar的附加元数据在进一步逐成员检查时确认为88份，后续处理见上方；
10项私有原件本次未独立恢复，公开材料核验不代替私有备份核验。
本次只检查与修复存储和导出，没有新增模型请求或重新运行实验。保存要求统一见[结果保存规则](AGENTS.md)。

2026-10-02：[直接Arrow分批对照](postgresql/text_map_arrow_batches_20261002/README.md)，源码f89ba28a。
目标92项及10条查询/8,224次fixture通过，模型0次；同步记账下完整查询8.354→8.149秒，少2.457%。
批次next执行约1→0.2秒，恢复等待与约5.5秒持久记账重叠；仅保留显式选项，默认Daft保留。
821源码摘要、原PG库与独立清理通过；源码准备503及辅助脚本0请求错误保留。
210份原件私有备份，209份公开文本去重为162份、47份精确恢复；SQLite不提交。

2026-10-02：[同步/线程实验记账对照](postgresql/text_map_threaded_accounting_20261002/README.md)，源码2f49660c。
70项目标检查及累计8,224次fixture通过，模型0次；线程完整查询中位数11.570秒，同步8.287秒，慢39.606%。
恢复等待约6→0.5秒，但窗口21→460–495、批次next执行约1→7秒；保留同步默认，线程仅作显式诊断。
控制器同名摘要失败保留32次，复用账本和原截止时间完成剩余8,192次；821源码摘要、原PG库与独立清理通过。
227份原件私有备份，225份公开文本去重为168份、57份精确恢复；2份SQLite不提交。

2026-10-01：[热路径独立计时](postgresql/text_map_gateway_lifecycle_20261001/README.md#热路径独立计时与无模型验证)。
源码8399b084；本地37项中30项通过、7项缺Daft/Arrow跳过，目标37项全部通过。
5条查询/4,112次fixture通过，模型0次；每1024行同步实验账本占用事件循环5.172–5.287秒，
批次next实际执行约1秒、事件循环恢复约5.7秒，区间重叠，真实占比和优化收益尚待确认。
时钟、绑定、原PG库与PG/Ray/worker/HTTP/ACL独立核对通过，两次0请求准备失败保留。
131份公开文本以103份唯一成员存储，28份精确恢复通过；1份SQLite仅存私有原件备份。

2026-10-01：[SemLoom消费分段](postgresql/text_map_gateway_lifecycle_20261001/README.md#semloom消费时间的已有记录拆分)。
只读取已有共享gateway三次真实测量；3,072行独立关联、请求区间与Core名额面积核对通过，新增请求0。
消费9.558–10.145秒，末尾Ray回复至结束5.2–7.6毫秒；每行RPC前176–232、worker外RPC合计94–147、
worker内258–279毫秒。vLLM内部排队平均0.051–0.056毫秒，优先细分提交链路和实验记录器，不推断独立函数收益。
客户端六批交付的前五批约8KiB；仍缺真实PG节点及worker绝对时刻，首行差值不作为完整查询的额外固定成本。

2026-10-01：[gateway复用与首行交付诊断](postgresql/text_map_gateway_lifecycle_20261001/README.md)。
源码11714bdc、同条件两臂fixture8,224次，另用独立追踪库2,048次，模型POST0。
共享gateway准备2.686→0.011秒、完整查询12.428→7.569秒，启动2.951秒单列；仅有限工程观察。
后续诊断节点首行0.233秒、客户端2.688秒，前三批DataRow约8KiB，与PG18.3发送缓冲相符；不是逐包观测。
早期三次微型执行没有逐次采集第三方遥测GET，后续两臂统一退出遥测；两个控制器失败、32次有效检查不重放。
公开339份无模型文本压缩恢复核验、独立原件备份与资源/ACL清理通过。
后续同服务15条查询、12,336次真实POST通过，另保留中止11,312次及0请求准备失败，本阶段真实累计23,648次。
新gateway14.626→共享10.131秒，准备2.785→0.01429秒，共享启动2.277秒；direct7.681秒，仍更快。
测量准确率84.961%–85.156%、配对输出差0–4行；已使用评价输入、两常驻worker/初始6CPU名额及遥测设置单列。
当前执行层源码未修改，原PG库不变；两份真实压缩归档公开302/331份文本，原件备份与独立清理核对通过。

2026-10-01：[worker复用真实复测](postgresql/text_map_worker_reuse_real_20260930/README.md)。
准备从9月30日开始，最后29条查询、20,624次请求通过，606.337秒；此前64次成功的16行子查询不重放，累计20,688次。
全部1024行性能重复来自最后同一服务。三次完整查询中位数direct7.660、Daft12.307、Ray22.756、
原SemLoom16.502、复用版14.794秒；准备4.737→2.831秒。worker服务启动0.593秒、结束0.0045秒单列。
相对旧方式完整查询减少10.35%，仍慢于direct和Daft；配对输出0–5行不同，不证明质量等价或通用收益。
控制器接线、原生常驻服务误判及protobuf写JSON错误均保留，已修订检查器并经无模型复现；未修改baseline图。
源码817文件前后一致；89项相关及2项记录器检查通过，126个记录PID、服务、GPU空闲与原ACL核对通过。
8,224份逐客户端事件与合并记录字节一致，仅公开合并版及压缩原始文本；原件在仓库外备份。

2026-09-30：[文本Map准备诊断与worker复用](postgresql/text_map_preparation_tuning_20260930/README.md)。
24条最终诊断与18条最终内部对照通过；保留16次配置错误前请求及295次HTTP失败请求，累计21,079次fixture、真实模型0次。
分段定位重复查询的新Ray worker就绪成本约2.0–2.5秒；可选调用方worker服务保留模型/容量核对、独占借用及未确认额度。
1024行三次中位数准备4.951→2.888秒、完整查询16.263→13.963秒，worker服务启动0.355秒单列；不更新真实四路径排名。
87项相关及11项最终worker检查通过（89个不同用例），实际Ray5项检查0次HTTP；201个PID身份、服务清理与原ACL核对通过。
1733份选择文本脱敏归档并逐项解压核验，原始完整导出保存在仓库外；真实模型收益及旧真实故障根因仍待验证。

2026-09-30：[文本Map分支审查与存储修订](postgresql/text_map_four_path_comparison_20260930/README.md#branch-review)。
阶段校验补充实际配置/查询编号比较，68份原真实记录逐项一致；74项本地检查中69通过、5项依赖缺失跳过。
准备和首批处理已有分段观测，后续目标环境耗时见上方；模型请求0次。1,599份原文件无损归档并独立恢复，
失败与输出差异保留；审查提交07a8cfd9已推送到比较分支，历史体积未改变。

2026-09-30：[新版主表完整比较、失败与诊断](postgresql/text_map_four_path_comparison_20260930/README.md)。
后续独立运行完成68条查询、41,024次请求及1024行评价；三次中位数SemLoom18.101秒、direct7.705秒、Ray22.849秒、Daft11.668秒。
准确率84.961–85.254%，路径配对输出差1–4行；未证明质量等价或平台。本次请求计数、源码与清理一致。
前次容量128失败（预扣16,862/服务成功16,861）、错误记录修订和2,064次诊断分别保留，不合并成功样本。
下一项定位SemLoom查询专属准备成本；原真实异常根因仍未确定。

2026-09-30：[原生候选与baseline补充](postgresql/text_map_native_candidates_20260930/README.md)。
Ray候选实际峰值16/64/128；新增Daft Native SQL/HTTP执行图，相同三档也通过。最终六组3072次合成POST，模型0次。
主表改为SemLoom Daft/Ray、direct、原生Ray、原生Daft，本地执行另作消融；6项集成、96项本地及8项环境检查通过。
该次只做合成验证，后续真实运行见上方记录；LOTUS/Sema等语义系统仍有独立缺项。全部失败、源码身份与清理记录保留。


2026-09-30：[文本 Map 服务器验证与有限模型比较](postgresql/text_map_matched_20260930/README.md)。
旧配置完成68条查询、41,024次真实POST及清理；原生Ray实际HTTP峰值仅2，随后按上方记录修订并复测，旧值不能用于完整系统优劣判断。

2026-09-28：[文本 Map 四路径比较准备](postgresql/text_map_comparison_preparation_20260928/README.md)。
共享本地 Ray 连接、完整记录的有限选点及评价配置核对已有 82 项本地检查。
实际 PG/Ray 生命周期和真实系统比较仍待验证；旧容量未选点与原始结果保持不变。

2026-09-27：[增量执行与可选Daft/Ray接入](postgresql/incremental_transport_20260927/README.md)保留全部诊断与未采用方案。
PG受控查询、行关联、对象占用及多查询生命周期通过；该受控记录真实模型0次。
追加[真实模型验证](postgresql/transport_real_20260927/README.md)完成12条SQL、4144次请求及清理；C64三轮中位数HTTP 10.552秒、Daft/Ray 5.573秒，
配对输出差异0–2行，未据此判定质量等价或稳定性能优势。旧M1未选点状态保持；工程提交`0df9a86d`与两轮证据已合入main。

2026-09-20：[完整容量复查与工程合并](postgresql/map_capacity_recheck_20260920/README.md)完成32个查询单元，16,400次请求与服务端一致，无未确认请求。
原控制器记录过一个短暂残留PID并返回失败，随后独立只读清理核对通过；原告警和PID身份缺项保留。
容量扫描修复与异常诊断改动合入main，原故障未复现、根因仍待确定。PG/direct均为持续供给不足，M1尚未选点。
Linux provider79项中70通过、9项环境跳过；没有启动第二验证单元或后续方法阶段。

2026-09-20：[C64异常诊断与有限复测](postgresql/map_c64_request_diagnostic_20260920/README.md)补齐传输异常类型、原因链类型及代码位置记录；原异常传播与不重试行为保留。
本轮3,088次记账与服务成功一致，C64四次各512条全部完成，无未确认请求；11项聚焦检查通过，Linux provider77项中9项跳过。
原故障未复现，根因仍待确定；不称已修复连接/模型缺陷，不进行方法选点。测试资源已清理，代码与证据仍在分支。

2026-09-20：[M1供给复查](postgresql/map_supply_diagnostic_20260920/README.md)已完成容量扫描开销定位和受控修复，真实筛查在C64预热时中止。
原128行fixture的PG查询约22–26秒，完整修复后约2.1–2.25秒；派生使用量只在一次只读扫描内复用。
Linux调度386项、图像81项通过；provider76项中9项环境跳过，另2项实际PG/HTTP集成通过。
真实账本1,552次尝试、服务端成功1,550次；两项执行异常归为MODEL_UNAVAILABLE，具体异常类型缺失。
443行保持临时结果；测试服务/端口/GPU进程清理、ACL恢复，无自动重跑。
修复与证据保存在 `codex/m1-supply-followup`，本次不合入main；M1尚未完成选点及后续条件阶段。

2026-09-20：[M1、M2 与 F 有限真实验证](postgresql/capacity_organization_image_validation_20260920/README.md)完成本轮清单。
文本8,248次POST与服务日志/counter一致；M1三种容量均未取得平台候选，后续调参/评价/观测对照未运行。
M2五种PG源信息方式完成正确性与1+3轮评价，固定C4仅作功能参照，未支持稳定优势；全局规划尚未接入SemMap。
F真实CLIP151次前向完成，三路径逐维差异0，受控前向后错误/取消及恢复通过；不代表GPU计算中抢占或性能验证。
列表释放修复后本地/Linux各26项通过、2项专用PG跳过；这些PG集成另有2项实际通过。失败原始记录保留。
模型/PG/Ray/任务进程与自有端口已清理，两GPU空闲，临时ACL恢复。本轮没有剩余额度的追加运行。

以下为按原运行身份保留的历史记录；当时的暂停或未验证描述由上方新结果补充。

2026-09-20：[M1 研究问题与选点调整](postgresql/map_capacity_selection_check_20260920/README.md)完成。
旧 M1 作为测量反例结束，44,544 次表撤下；当前按可达性、容量筛查、工作量调参和独立评价分阶段进行。
v2 编排拒绝旧配置，复用常驻 gateway/direct，保留实际供给、配对吞吐、质量与资源观测；本地 35 项检查通过。
该次修订没有 PG/HTTP/模型调用；后续真实结果见本页顶部。


2026-09-20：[工作包 F 图像接入](postgresql/image_stage_execution_check_20260920/README.md)完成类型、同步 reference 和有界阶段的工程检查。
`ai_semantic.embed(bytea,jsonb)→real[]` 使用独立 schema 5/wire 7，复用 MethodDriver、共享 Engine 与阶段资源管理。
PG18.3 编译、回归 1/1、TAP 2,159 项及最终 9 组 PG/Ray/解码检查通过；17 个 Job 回收，方法与 Core 责任分别核对。
保留启动路径、fixture 预期及已关闭会话重复失败的记录；最后一次集成全部通过，服务器测试资源与 ACL 已清理。
真实模型 0 次，CPU 模型 fixture 不构成真实 CLIP 数值、GPU 或性能证据。

2026-09-14：[M1真实运行准备](postgresql/waiting_positions_real_preparation_20260914/README.md)完成：排除旧电影/评论后按电影分组，准备512行调参与1024行评价输入；
原文保留服务器私有目录。完整/摘要观测完成8查询/128次受控HTTP，本地/Linux各22项检查通过。
当时的 44,544 次/30 分钟运行表已于 2026-09-20 撤下；数据与原始准备证据保留，**该准备记录当时没有真实调用**。
PG/HTTP/任务进程已清理，ACL恢复；后续实际调用及真实结果复核尚未完成。

2026-09-14：[常驻服务对照](postgresql/waiting_positions_persistent_20260914/README.md)完成：24查询/768受控POST，另以旧入口1查询/120次回归，真实模型0次。
C8五次均快于对应轮次C4；紧W相对宽W的HTTP P99降低42.23%，完整查询时间却增加233.06%。
token组一次启动约5.5秒，常驻后每查询准备约14–15毫秒；四实例RSS同时采样，资源/会话/transport均完成回收。
本地/Linux各16项行为检查、761份code文件哈希核对通过；PG/HTTP/进程/socket/GPU/ACL清理完成。
下一步准备新的真实输入、服务签名和有界请求计划；尚无真实模型性能或低扰动观测结论，M2未开始。

2026-09-14：[等待位置小实验](postgresql/waiting_positions_pilot_20260914/README.md)已完成M1首轮受控检查：新增调用至EOF计时和五段等待分解，复用已有FIFO/工作量限制与PG事件。
两轮24查询/672受控HTTP，实际模型0次；保留首轮backlog5的异常，backlog32跟进固定C8/W3096/512。
跟进紧W的HTTP P99降低39.03%，SQL至EOF增加181.29%；当前token臂每查询新gateway/tokenizer准备约5秒。
840份代码一致，PG/HTTP/进程/端口/ACL已清理。该轮提出的常驻服务/C4-C8检查已由上方新结果完成；两轮原始证据继续保留。

2026-09-14：[查询依赖与共享计算](postgresql/query_sharing_lifecycle_check_20260914/README.md)完成：复用统一责任表与Job/flow轮转，新增可选shared计算策略和query-job Map窗口，存储独立保留。
Linux385/57/116、PG回归1/TAP2138、专项163次与worker预演63次受控HTTP通过。
首次真实准备0次失败保留；获确认后63次真实POST、19个Job、55条Map关联与8条Filter决定通过，195.917秒内清理。
Map逐字复述0/55，保留为质量负结果；不据执行与回收通过声称质量或性能通过。838份源文件一致，PG/模型/进程/GPU/ACL已清理。

2026-09-14：[异步期限与输入/交付诊断](postgresql/async_deadline_flow_20260914/README.md)完成：正常PG回归1/1、TAP2138项、12单元96次fixture通过。
首轮真实24次因配置准备错误停止并保留；获批补跑33次全部完成，累计57次生成请求。
56条正常结果关联通过，各8行均2个情感假阴性；1条真实结果在末次同步写入跨期限后保持失败、SHA完整、拒绝评分。
真实flow四次暂停时队首均未就绪，不能归因ready-first收益；默认次序保留。两段服务/PG/进程/GPU/ACL已清理。

2026-09-14：[容量等待与零任务 Map 服务器验证](scheduling/capacity_wait_server_20260914/README.md)完成：Linux调度376、provider55、PG Python合同116通过；
实验工具482通过/15环境跳过，PG选定6项集成另行通过，共262次受控HTTP、真实模型0次。
修复诊断脚本的write-once summary冲突后，普通PG四查询完成：小结果首行约0.65秒、大结果约0.25秒，
支持发送缓冲解释，不据此归因旧SemMap全部等待。PG/socket/任务进程和ACL已清理，失败记录保留。

2026-09-11：[C/D审计与真实预算诊断](postgresql/cd_validation_20260911/README.md)完成提交→权威终态的work/request独立重建、
实际组内顺序核验及可选容量拒绝观测。旧9轨迹516任务重放通过；Linux370/55/116及审计19通过。
新16单元1152模型POST/319.11秒全部有效；同配置C的equal/total约3.75/3.81秒，未显示total更快。
D在C32下W4096确实限制提交，三种紧预算查询均慢于W16384；rows紧预算有1行模型输出差异，关联正常。
733份非Markdown代码一致，模型/PG/进程/ACL已清理；单次短诊断，不是性能排名。

2026-09-11：[A/B验证补充](postgresql/ab_validation_20260911/README.md)：统一结果文件校验、direct/Ray逐行关联、
进入/迭代错误与清理时点、全过程逻辑资源和慢源交付。Ray资源分别声明，短命读取峰值保持未知。
代码与受控集成验证；新增真实模型请求0，强静态参照、正常容量点开销和原生真实质量仍待验证。

2026-09-10：[PG组织工作包D](postgresql/map_organization_20260910/README.md)。
单Job Map v6、固定tokenizer的rows/work/length；Linux369/55/116及审计6、PG TAP2138通过。
108受控HTTP与新408/408真实POST通过，244.96秒无失败/重试；726份代码一致，token/关联/候选/分组与清理验证通过。
输入序两个控制真实组数16/28但顺序相同；长度分组改变顺序，128行预测一致。仅行为/资源诊断，强静态参照及稳态性能仍待验证。
模型、PG、任务进程停止，临时ACL恢复；原文/预测私有，公开组织事件去原文后再次审计。

2026-09-10：[PG总字节预算工作包C](postgresql/pg_window_budget_20260910/README.md)。
PG18.3完整TAP2134、最终专项37（重叠）、回归1，本地/Linux各116合同；120行受控HTTP。
独立288/288真实POST、202.55秒，无失败/重试；B1MiB/L32峰值13行和1028312字节，清理后责任0。
功能/资源诊断；L与内存策略同变，不作性能归因。模型停止，PG/ACL暂留D受控验证。

2026-09-10：[数据库原始输入与公共查询](postgresql/database_queries_20260910/README.md)。
来源：源码、真实PG18.3、原生Ray2.56.1/LOTUS1.2.4、确定性本地HTTP；真实模型0次。
真实Movie v4由原SemBench generator生成2000行并完整PG往返，135次重复原始reviewId均保留。
最终查询fixture5项/多个子场景、1111POST通过；Map三臂、COUNT/LIMIT、错误、空源与CLI清理均检查。
PG回归1/1、Linux PG合同116、两端工具65通过；TAP2101中一条事件断言误判，修正文件28/28通过。
原失败/各次尝试独立保留。新WHERE/COUNT默认关闭，真实语义质量与性能仍待新额度，未形成方法结论。

2026-09-10：[A2同配置真实诊断](postgresql/execution_repairs_20260910/README.md#同配置的真实模型修复前后诊断)完成。
来源：真实PG18.3/Qwen2.5-7B-Instruct/vLLM0.25.1；18单元、8448预留/实际POST、无重试、423.47秒。
L64/结果32MiB重复offer显著减少且两次JCT约17→10秒；其他配置无一致改善。
512行短查询、两次重复，不登记强D0或组织方法优势；全部出站/关联/资源核对通过，服务已正常停止。
首轮0POST启动失败保留。下方历史状态按各自运行时记录保留。

2026-09-10：A真实匹配诊断因gateway子进程缺源码路径在首个预热前停止，0模型POST、128预留未领取；
模型正常退出，两套诊断PG停止，204.31秒内结束。runner源码路径修复通过实际PG/HTTP与相关37项检查，
没有自动重试。[失败与修复](postgresql/execution_repairs_20260910/README.md#真实诊断的零请求启动失败与修复)。

2026-09-10：[查询计时、错误保留与执行推进修复](postgresql/execution_repairs_20260910/README.md)。
来源：源码、受控时钟、真实PG/production gateway和本地HTTP fixture；真实模型请求0。
旧pump受控32行offer660次、修复后62次，接纳/后端提交均32，无相关释放的重复探测598→0；不据此声称模型性能收益。
Linux调度369、provider51、PG静态116、受影响工具78项通过；PG回归1/1、TAP2043项通过。
历史bytes-result重新审计24708/512，旧预算对照和direct日志身份补充限定；原数字/失败保留。
真实模型验证待新额度；专用PG暂供后续工作包受控验证，HTTPfixture/gateway已关闭，最终清理另记。

2026-09-10：[单 Map 容量工具与单卡画像](postgresql/map_capacity_20260910/README.md)。
来源：源码、真实PG/vLLM与完整私有原始记录；非模拟、非新组织/调度算法。
96601模型POST、99808预留；62完整查询/1资源拒绝，37成功PG查询独立producer核验，2个关闭观测的诊断另列。
本地/Linux各214项，PG回归1/1和TAP2032项；五次4000行中位JCT：direct C64 62.679s、PG C16 80.201s、PG C32 73.024s。
候选C32/L64；高并发同规模平台未完成，strong_d0_qualified=false。独立样本各2000行通过身份、质量、资源核验，不充当稳态重复。
失败单元756行部分结果/793出站/4000预留均保留；所有服务和临时ACL已清理，77.94min内结束。

2026-09-10：[数据准备与执行记录修复验收](postgresql/data_evaluation_harness_20260910/README.md)：
本地/Linux 各 168 项相关测试、7 项真实 PG 记录检查通过；新 ShareGPT 原文准备和 PG 146 行读回通过。
新账本 SQuAD 10/10 次模型请求通过完整消息、token usage、关联及答案检查，增量资源最终归零。
两次模型准备失败均 0 POST，失败保留，服务/ACL 已清理；旧 48/68 账本与缺失 JCT 不改写。
实现提交 `5085c6ff` 已合入 main。这是工具与小样本功能验收，ShareGPT 质量、静态容量、组织性能和双副本路由仍待验证。

2026-09-09：[PG 单 Map 真实数据尝试](postgresql/data_execution_pilot_20260909/README.md)，
单 GPU/单 endpoint、48/68 次请求。SQuAD 同步/增量各16行 EM/F1 均为100%，仅为小样本观察；
增量乱序完成按序号/payload/输出离线重审通过，原检查错误与未保存的 JCT 不隐去。
ShareGPT 使用14次请求后在同步第12行 length 停止；存在任务遵从问题及第7行准备文本被脱敏改写，
增量臂未运行，不是合格原样数据或性能证据。本地/Linux各125项相关测试通过，源代码不改PG/Engine。
模型/PG/gateway已清理，未开启双卡性能或旧矩阵。当前更改在 `codex/data-execution-pilot` 分支维护，尚未合入 main。

2026-09-09：[有界多行方法驱动](scheduling/method_driver_20260909/README.md)已合入main（实现提交f7de79c3），提供固定共享payload预算、阶段推进、最终结果反压和取消回收。
独立producer复用同一Engine/组织器/异步HTTP；Linux完整调度361项及最终专项55项通过（有重叠）。
首次0POST失败暴露缺TaskInfo，修复后双Job10次及最终两阶段2次真实POST通过，合计12次；预算与服务均清理。
这不是PG多阶段方法桥接或质量/性能实验；ShareGPT真实数据切片已规划、尚未运行。


2026-09-09：[查询生命周期与连接接纳加固](scheduling/query_lifecycle_20260909/README.md)分开已登记流空闲与帧期限，统一查询/standalone/握手名额，补齐登记与加入失败回滚。
查询结束策略集中到QueryRegistry，PG runtime持有幂等流句柄，provider不再隐式登记节点。
旧25c0f090的空闲取消、预留被占用及令牌失败遗留Job已受控复现。Linux521项、PG回归1项/TAP2006项及26次真实POST通过；654份源码一致，测试服务已清理。


2026-09-09：[查询级Job归属](scheduling/query_job_20260909/README.md)接入普通Filter v3与Map v6；PG查询上下文、可信登记、流与查询分开结束，复用共享Engine和非阻塞UDS工具。
外部执行层将一份Job存储预算分给多个流；PG不分配Job份额。Linux517项、PG回归1项/TAP2005项通过，652份源码一致。
真实复测24次POST通过，12个Job均回收；首轮4次因非空条件返回UNKNOWN失败，单独保留，不视为质量问题已修复。
测试服务已清理。新模式Map窗口1；choice Filter、重扫、更广SQL形状和方法状态总预算尚未完成。


2026-09-08：[三项会话接口清理](scheduling/session_api_cleanup_20260908/README.md)完成公开fail/派发控制/消费者关闭、可注入Job选择，以及独立单流适配器退役。
网关不再访问session私有账本/状态；默认轮转、同步语义参照和未知请求计费规则保留。生产源码净减70行，旧测试迁到生产网关。
Linux519项、PG回归1项/TAP1963项及最终12/18次真实模型通过；首轮12次记录保留，累计42次POST。最终644项源码/配置身份一致，测试服务已清理。
消费者释放空间后的wake遗漏已通过失败复现修复；74份原始材料脱敏归档，另清理46个可从现有文件或Git恢复的本地副本。

2026-09-08：[多Job共享执行与网关复用](scheduling/multijob_20260908/README.md)完成可信Job登记、共享账本、Job轮转、有界连接代理及局部取消/未知结果隔离。
Job份额策略归外部执行组装层，PG只管本地缓冲和查询生命周期；一个/多个Job使用同一增量循环，同步Filter/Map等复用连接工具并保留协议处理。
Linux515项、PG回归1项/TAP1963项通过；单Job12次、多Job18次真实请求通过，不同Job HTTP峰值2，9个Job最终归零。644份源码一致，测试服务已清理。
PG同查询多算子可信归属、动态借用、方法状态总预算和GPU资源模型仍待完成；一次启动端口检查失败为0 POST，原证据保留。

2026-09-08：[消费期限修复与v5桥接退役](scheduling/bridge_retirement_20260908/README.md)补齐结果交付时的消费期限，迁移测试后删除旧窗口1适配器、CLI、PG注册与专属身份，相关实现净减101行。
Linux503项、本地52项、PG回归1项/TAP1963项通过；迁移前10次、最终窗口1共10次/窗口2共12次真实请求全部通过，最终640份源码一致，测试服务已清理。
同步语义参考和wire v5保留；旧窗口1名称明确报错，多session/多Job资源管理仍待实现。

2026-09-08：[阶段超时与执行组装整理](scheduling/phase_policy_20260908/README.md)分开排队、后端和消费期限；网关不再用模型超时限制排队。
HTTP和默认执行组装移出会话模块，可注入已有策略与工作量描述。Linux501项、PG回归1项/TAP1961项及12次真实请求通过，641份源码一致，测试服务已清理。
v5窗口1桥接、同步参考和单活动session范围仍保留；过渡路径退役与多Job资源管理待后续完成。

2026-09-08：[增量容量与结果回收整理](scheduling/capacity_cleanup_20260908/README.md)拆开接纳任务、输入/结果字节和后端并发预算，移除PG固定64项数组，统一完成解析并修复错误路径的已完成结果回收。
Linux提供者31项、PG合同115项、PG18.3回归1项/TAP1961项通过；两轮各12次真实请求通过，最终639份源码一致，服务和资源已清理。
v5窗口1桥接仍保留，多Job动态资源管理尚未实现。

2026-09-08：[公共实现归位](scheduling/shared_modules_20260908/README.md)将共享runtime、Map codec、请求类型、模型配置和策略接口从旧执行模块移出；旧导入保留别名，功能范围不变。
43个迁移定义及24个原地定义的AST保持一致；Linux495项、PG18.3回归1/1958项TAP和独立12次真实请求通过。
638份源码一致，同一查询HTTP峰值2，资源归零且测试服务已停止；本轮27份原始材料压缩归档，历史证据未移动或删除。

2026-09-08：[生成Map的PG多在途接入](postgresql/async_window_20260908/README.md)已完成v6 offer/receive、PG有界行槽和按身份返回结果，复用原组织器与增量核心。
PG18.3严格构建、回归1项、12个TAP共1958项及Linux199项Python检查通过；另核对v6权限、RLS和快照。
独立12次真实POST全部通过，同一PG查询HTTP峰值2，增量10任务/6会话排空后资源归零，服务已停止。
633份源码/测试文件与真实运行一致；当前只对受限生成Map显式开启，复杂表达式窗口1，Filter/组合和多活动session仍待完成。

以下为之前的核查与实现记录，不能将旧记录中的待实现项当作最新状态。

2026-09-08：[PG多在途接入前核对](postgresql/incremental_window_one_20260908/README.md#async-readiness)确认现有v5桥接拒绝在途期间的下一帧。
新增反例及桥接套件本地/Linux各7项通过，旧请求回收后新会话恢复；生产实现未改，多在途仍待实现。
本轮未启动PG/模型，新增真实模型请求0；不能复用窗口1结果声称已完成多在途。

2026-09-08：[生成Map的PG窗口1接入](postgresql/incremental_window_one_20260908/README.md)新增独立provider执行身份，
复用现有v5与PG语义、SessionEngine和异步HTTP。一个活动连接共用一个Engine，未知结果停止接纳。
115项PG合同、23项provider、9项observer测试及PG18.3回归/1926项TAP通过；628份源码一致。
9次真实POST验证参考SELECT、增量SELECT/INSERT、事务错误、取消57014及新查询恢复；
其中7个增量任务、5个会话排空后资源归零，PG/网关/传输/模型已退出。临时planner修改已撤回。
PG多在途、该profile的Filter/组合、多活动session及GPU显存管理仍未实现。

2026-09-08：[有界组织窗口验证](scheduling/organized_window_20260908/README.md)接入既有work分组与显式局部性，
组织批次展开为独立单成员请求；行/阶段与批次身份贯穿backend和结果，原路径保持兼容。
最终本地54项、Linux335项通过，624份源码一致；两轮各4次真实请求均通过，峰值并发2，服务已停。
首轮后新增重排进展用例复现旧判断缺陷，修正并保留失败及两轮记录。
PG异步、单请求多成员、准备缓冲/方法总预算及多活动session仍待实现。

2026-09-08：[逐行方法框架验证](scheduling/method_framework_20260908/README.md)补充预声明任务能力与有界方法续体，复用现有session账本。
本地44项、Linux325项通过，621份源码哈希一致；两行各两个生成阶段共4次真实请求通过。
首次诊断脚本参数错误在0次请求时停止，失败记录保留；修正脚本后复用同一4次预算。
最终资源归零、传输与模型退出、端口关闭；embedding/级联算法、总方法内存计费及PG接入仍待实现。

2026-09-08：[PG同步组合真实验证](postgresql/filter_map_real_20260908/README.md)完成12次模型请求；
[增量session真实验证](scheduling/incremental_real_20260908/README.md)用独立预算完成5次请求，实际HTTP并发峰值2。
本地37项核心/传输测试、Linux318项全调度回归通过，618源码哈希一致；账本和服务清理通过。
两者不等于PG异步接入，不建立模型质量、正式资源或性能结论。旧12次账本在后一次运行后保持原字节。

[单流增量执行验证](scheduling/incremental_session_20260908/README.md)记录2d2dee35之后的受控核心实现：
接受前缀、结果lease、取消与跨session额度；任务选择复用/适配已有策略，FIFO可替换。
本地33项新增测试、Linux314项全调度测试通过，616项源码哈希一致；零模型请求、Raylet残留0。
旧同步入口保持；真实Backend、批处理与PG接入未由本轮完成。


[Filter→Map验证](postgresql/filter_map_binding_20260907/README.md)以19326609为基线，开发分支支持一个关系Filter接一个生成Map。
本地115、Linux138、PG18.3回归1及TAP1910项通过；611源码哈希一致，四轮递增测试均通过且分别保留。
覆盖原生求值、权限/钩子、RLS/快照/取消与计划解码；模型请求0，服务已停，尚未合入main。

以下早期记录保持对应版本的能力与证据范围。

[共同调用与tuple绑定](postgresql/semantic_binding_20260907/README.md)在11e89b08基线上实现：本地115、
Linux138、PG18.3严格编译/regression1及TAP1848项通过，605源码哈希一致。保留测试头文件缺失的首轮
失败及两轮成功；零模型请求、服务已停。setrefs与OFFSET原型修正设计，未据此开放Filter→Map。

[Map调用分析提取](postgresql/semantic_call_extraction_20260907/README.md)以`b4b93b2e`为行为基线，
源码由618项哈希固定；15个相关函数体等价，本地115/115、Linux138/138、PG18.3严格编译、
regression1/1、8个TAP1808/1808通过。首次本地端口权限失败原样保留并说明重跑环境差异。
模型请求0，隔离服务已停；这是A1准备步骤，未完成共同调用/绑定、Filter→Map或增量Core。

[两个Filter AND与有界gateway会话](postgresql/semfilter_and_20260907/README.md)在`codex/semfilter-and`
完成同步组合：Linux138/138、PG18.3严格构建、regression1/1、8个TAP1808/1808，584项源码哈希
一致。覆盖独立输入/身份/计数、VOLATILE两次求值、权限/RLS/快照/INSERT取消恢复和40会话FD/线程
回收；保留7轮PG失败/成功记录。测试服务已停，模型请求0；现已合并main，未证明Filter→Map、
单节点异步、正式资源、质量或性能。

[扩展目录重构验证](postgresql/pg_module_layout_20260907/README.md)记录基于 `41e103f2` 的
`codex/pg-module-layout` 工作树及文件SHA-256：本地算子合同112/112、网关4/4、严格C11编译8/8，
45个文件的机械迁移及marker函数体/extern声明一致性通过。旧1ms HTTP deadline测试的计数假设
失败保留，单独修正测试；没有改变生产超时或重试策略。用户随后授权服务器补验，`20b22a55`通过
Linux112+4项、PG18.3严格构建、regression1/1及完整7个TAP1758/1758；源码107项哈希匹配，安装件
与构建件相同。此前下载、临时驱动PATH与SQL_ASCII初始化失败分别保留，最终独立UTF8运行通过。
自有活跃进程0、socket关闭；用户授权后已快进合入本地main，模型请求0。本轮PG证据独立于下列旧版本和真实模型记录。

[两算子完整回归与合并验证](postgresql/semmap_resource_lifecycle_20260906/README.md#main-integration)
绑定`5771cef1`（运行时代码`fcd12373`）：Linux247/247、PG18.3严格构建、regression1/1、
全部7个TAP1758/1758通过；新9次真实请求覆盖Filter v3/v4与Map v5各自的SELECT/INSERT。
NULL零调用、输出/筛选、usage和写回通过，6会话关闭、9任务完成、所有测试服务退出。
旧8/8账本不变；质量、性能、正式资源及组合/异步能力没有据此升级。


[退役清理记录](postgresql/semmap_resource_lifecycle_20260906/README.md#retirement)登记 19 个删除文件的
固定 Git 恢复身份、当前入口迁移与验证。`fcd12373` 本地 233 项中 231 通过、2 项 Linux 专属
跳过，Linux 233/233 通过；PG18.3 严格构建与四个受影响 TAP 1297/1297 通过。
模型请求 0，测试进程残留 0；历史失败和真实模型结果保持各自提交身份。

[推送前真实模型复验](postgresql/semmap_prepush_20260906/README.md)绑定 `b7eeea53`，新预算8/8，
旧32/32账本SHA不变。SELECT、独立审计连接下的INSERT、cancel/recovery/reject/recovery六阶段
均valid/passed，结束FD/线程增量0；INSERT资源复查已完成。1162项原始哈希全部匹配，
本轮PG/gateway/模型退出，端口关闭、两GPU均1MiB。首次ninja/PATH准备失败为0请求并保留。
这是带100ms任务派发辅助等待的小规模真实接线/资源证据，不是质量、性能或正式3×2000资格。

同日的 [共享观测与配置重构](postgresql/semmap_resource_lifecycle_20260906/README.md#reuse-refactor)
本地212项回归中210通过、2项Linux专属跳过；`b7eeea53` 在Linux212项全部通过。
同提交使用新端口55499完成隔离PG18.3实际1×100 fixture，四场景九阶段全部valid/passed，
1190项原始哈希全部匹配，8个已观测进程退出。旧32/32预算只读兼容且原字节不变，模型请求0。
这是行为兼容、小规模资源与配置传递证据，没有新增真实模型、质量、性能或正式资源结论。

2026-09-06 的 [SemMap 资源工具生命周期修复](postgresql/semmap_resource_lifecycle_20260906/README.md)
以 `e5f4dd12` 为起点，仅改实验工具；最终运行时代码 `77a123de`，measurement schema v2.1 / phase-lifecycle-3。
本地119项检查中117通过、2项Linux专属跳过；Linux130项通过（118/129项完整组加1项后增基线回归）。
隔离PG18.3实际1×100 fixture-only诊断的四场景九阶段全部有效且通过，模型请求0；同一存活进程
完成清理，结束FD/线程增量0。diagnostic通过不构成正式资源资格，3×2000仍未授权、未运行。
三次诊断的源码/修订/失败分别保留，原始哈希原地校验无差异；公开摘要与原始raw分列哈希。
旧v1 failed与v2原始证据不改写，四D整体仍未完成。

同日追加的[有限真实模型复查](postgresql/semmap_real_followup_20260906/README.md)在同一32次账本
增加7次，25→32：SELECT/INSERT字节与usage、NULL、真实取消/拒绝与恢复功能核对完成；SELECT和
剩余四个故障/恢复阶段资源有效通过。INSERT的原资源测量被验收JOIN的系统目录FD污染，按条件停止。
无模型PG反例支持独立审计连接修正，真实INSERT没有重试；不同run不拼成完整资格。
原始哈希核验无差异，原25条预算前缀字节不变，本轮所有模型/PG/gateway进程和端口已清理。


更新日期：2026-09-07

文档角色：本文只回答机制是否实现、通过何种验证以及是否已有真实 GPU 性能证据；它不定义架构、
接口或下一步顺序。具体数字和逐次运行证据仍以各结果目录的 `README.md`、`manifest.json` 和 CSV 为准。
主线能力与独立开发分支分开记录；未合并分支的验收不改变 main 的源码或运行能力。

2026-09-04 的[生成型 Map 真实模型与资源检查](postgresql/semmap_real_model_resource_20260904/README.md)
绑定 `main@b19486a1`。精确 PG18.3、Qwen2.5-7B-Instruct revision `a09a3545…`、vLLM 0.25.1、
单 RTX 4090/BF16 下，持久账本累计 25/32。受限 SELECT/INSERT 各 10 个非 NULL task，SQL NULL
零调用；空串、Unicode、取消 `57014`、模型请求拒绝 `38000` 及两类恢复通过，模型/usage 和 PG 输出
逐项对应。真实链路通过只证明执行接线，不是质量或性能结果。

同轮 fixture-only 资源运行完成 1 个 warmup 和 3×2,000 个 task，每个输入/输出为 100,000/65,536
bytes，C 客户端使用 libpq single-row mode；但是 60 秒恢复窗口结束时至少一项固定 RSS/FD 条件未满足。
后续已同步到 `raw/semmap_res2/stress/measurements-aggregate.json`（93 个 attempt 全部
violations/baseline/peaks/ending 摘要）及 attempt 1/47/93 代表散件，明确超限项为 `gateway+backend.uds_peak_delta`
（observed 3/limit 2，额外字段 `fd`）；取消/断连/gateway-exit 子项按停止条件未运行。
因此四 D 仍不完整，不能进入可组合执行资格。原始失败、模型账本和清理证据均保留，未放宽阈值重跑。
已纳入 main 的 [Map C v5/PG golden 切片](postgresql/semmap_pg_wire_20260903/README.md)原资格绑定 `5031bb50`：
生成型 SELECT/INSERT、常量/相同表达式列位置、任意有效文本、NULL/空串、输入输出上限、严格协议与
child/provider 错误、取消/savepoint 恢复通过。PG18.3 `-Werror`、regression 1/1、TAP 1741/1741、
本地/服务器各 137/137 与 8/8 C11；来源/原生权限检查保留。
合并前 `f46fe936` 修正错误帧阶段与独立 usage 字段校验，重新通过 PG18.3 `-O2 -Werror`、
regression 1/1、TAP 1758/1758、两端各 139/139 和 C11 8/8。两次预期红测试、一次测试 socket
路径过长中止和短路径完整重跑分别保留，详见同一记录的[合并复核](postgresql/semmap_pg_wire_20260903/README.md#merge-review)。
该切片当时真实模型仍为 0/32、资源压力未运行；后续结果使用上方独立 2026-09-04 身份，
不把新运行重新绑定到旧提交或声称资源、性能、四 D 整体通过。
以下原始证据保留对应提交和阶段身份。

此前独立 [Map PG plan/权限切片](postgresql/semmap_pg_plan_20260903/README.md)绑定 `2205ccbb`：
SQL 0.2.0/schema 4、固定参数、计划复制、严格解码、原生函数权限/hook 和重规划通过验证；
PG18.3 `-O2 -Werror`、regression 1/1、TAP 1260/1260（新 plan 238）、两端各 136/136 与 C11 通过。
该次只支持普通 EXPLAIN，实际执行在 child/provider 前明确拒绝；当时未合入或推送 main，没有真实 Map 或资源资格。
中间反例、fixture 语法错误与最终结果分别保存；旧 1022 TAP 不重新绑定到本次提交。
[SQL wrapper 补充](postgresql/semmap_pg_plan_20260903/README.md#sql-wrapper-source-check)绑定 `676615fa`：
实际复现 custom 接受而 generic 拒绝隐藏 Map 指令参数，修复来源识别后两者均拒绝、零连接；
PG18.3 regression 1/1、TAP 1283/1283、两端各 136/136 与 8/8 C11 通过。
普通函数内联和权限检查保留；当时新 Map 未执行，模型累计 0/32，未合并或推送 main。
Map 消息、纯值、Python v5 及深层 JSON 修复已随 `b0400944` 合入本地 main；
[合并复查](postgresql/semmap_values_20260903/README.md#main-integration)重新通过本地 136/136 和 C11，未运行服务器或模型。
[Map 纯值与 Python v5](postgresql/semmap_values_20260903/README.md)的服务器资格仍绑定 `425d2b1c`：
C/Python 身份/完成值、严格 Python v5 和共享 gateway 通过本地/服务器各 135/135，精确 PG18.3
重新构建与旧 SQL 路径 regression 1/1、TAP 1022/1022。没有 PG schema 4/C v5/三参 Map 执行、
真实模型或资源资格；独立审查修复与失败证据保留。下方消息子切片保持原始范围和身份。
其后[深层 JSON 修复](postgresql/semmap_values_20260903/README.md#json-depth-repair)绑定 `a1bbdd30`：
本地 136/136 与 C11 通过，坏连接后同一 gateway 可继续完成合法请求；未重跑服务器、PG 或模型。
新产物使用独立 SHA 清单，不改绑原 135/135 和 1022 TAP。
[Map 消息子切片](postgresql/semmap_messages_20260903/README.md)的验证仍绑定 `6903cf46`：
C/Python 规范消息与共享 JSON 编码通过本地/服务器各 107/107，精确 PG18.3 新构建与旧路径
regression 1/1、TAP 1022/1022。没有完整 Map identity/output、三参 SQL/schema 4/wire v5 或模型/资源资格；
这不是生成型 Map 已进入 executor 的证据，也没有新增性能或质量结论。
当前证据支持 `REL_18_3` extension recording `SemMap/SemFilter`、三参数 exact `SemFilter` golden/fixed-model
reference、独立 cost/cardinality metadata 及 planner-only static calibration mechanism、同步单在途 UDS、
shared runtime、neutral provider port、
schema v1/v2 planner-owned plan spec、wire v2/v3 和公共 compatibility suite 的功能与资源生命周期；
schema 3 / wire v4 已另有 SELECT 与受限 INSERT 的 fixture 功能验证、受控资源与受限真实 choice 服务检查，
实现和记录已合入本地 main；各次运行仍绑定原始源码身份。
小规模真实模型只证明纵切面可运行，
deterministic calibration artifact 只证明生成/验证/planner 消费，不支持真实 cost accuracy、语义质量、第二 physical path、
bounded async 或性能优化已经完成。当前顺序看工程计划和
`experiments/plans/archive/进度汇总_20261001.md`，既有文本、图像和 SAOR 条目继续保持外部物理执行身份。
2026-09-01 首轮真实 calibration 采集已执行，但固定模型在首个 training 查询产生非法 tristate，
PG18.3 正确终止语句；held-out 未运行、未拟合、没有 artifact。该失败证据不改变机制功能资格，
也不提供成本精度或语义质量通过结论。
后续诊断又复现了旧 builder 对完全共线观测误发 artifact：有限精度消元留下伪非零主元。拟合前的精确
有理数/归一化主元检查已通过 9/9 calibration 测试。`6c111b24` 的 PG18.3 服务器回归 1/1、TAP 437/437、
Python 59/59 通过，普通统计 estimate 从 8 修正为 64；choice 候选格式 30/30，但语义预期仅符合 12/27，
未通过 reference 资格。不能把该修复或候选格式通过写成真实校准完成。
最终数值补充 `44f6632c` 又以精确 Gram 无穷范数条件检查拒绝组合病态，独立通过 PG18.3 regression
1/1、TAP 437/437、Python 60/60（calibration 10/10）；原 6c 模型/统计结果与最终数值测试分别保存。
后续[单一 prompt 对照](postgresql/semfilter_prompt_qualification_20260901/README.md)实查 HTTP messages
和 chat template 一致，但 1.5B 新 prompt 旧/新各 5/9、matched 7B 为 7/9 与 6/9，仍无配置通过。
这些分母是独立工程样例，每例重复三次；中止的默认参数失配 7B 尝试单列保留。生产配置未改，
整轮校准仍暂停；本轮 Python 60/60 复跑，不新增 PG18.3 TAP 或资源证据。

choice 各切片原在独立分支完成，当前集成版本已包含源码和结果目录。下节逐项保存原始提交身份，
不把集成提交或后续清理重新绑定为旧实验的运行版本。

## 1. 证据等级

2026-09-02 的[函数身份加固](postgresql/function_identity_20260902/README.md)及其证据已合入 main。
`792a0408` 在已复现非成员误接管后最小修改 `extension.c`；最终测试提交 `934f4f61` 通过 PG18.3
`-O2 -Werror`、regression 1/1、TAP 1022/1022、本地/服务器各 Python 94/94 与中立 C11。
103 项身份测试覆盖函数替换/删除重建自动失效，以及仅成员 ADD/DROP 后读会话 `DISCARD PLANS` 的
正确重新规划；只改成员关系的跨会话自动失效仍未实现。受控使用须暂停查询、结束旧事务/游标，DDL
提交后刷新连接池在内所有相关物理连接；不是在线透明变更或即时权限撤销。无真实模型或新资源 smoke。
首次 2 项生产反例失败、夹具参数名失败、990 项中间通过与最终结果分别保存，不重新绑定历史四 C 证据。

2026-09-02 的 [choice 真实服务检查](postgresql/choice_service_20260902/README.md)绑定工具 `0a1c12d3`，
复用 `39007150` 的 PG18.3 二进制；Qwen2.5-1.5B/vLLM 0.25.1 下原定 14 次（old/choice 各 7）
与两个 NULL 对照通过，实际请求仅差 choice，原 parser/SQL rows/calls/usage 一致。累计预算 15/100，
首轮工具误计 BatchEncoding 长度导致的 1 次失败保留，另保留所有启动尝试。本地/服务器各 94/94，
未重跑 TAP/构建。只证明工程接入，汤食谱 UNKNOWN、含糊输入 TRUE 等原始结果不作质量通过依据；
不恢复校准、不改变默认 reference，也没有第二路径或多算子结果。

2026-09-02 的 [choice 资源检查](postgresql/choice_resources_20260902/README.md)绑定工具 `4464fe9b`，
复用并核对 `39007150` 的 PG18.3 二进制：v3/v4 各 5,164 次 65,536-byte fixture 调用、10 次取消、
10 次阻塞 DNS 超时与恢复通过。保留最初 FD 减少导致等值断言失败及诊断权限失败；独立受控重跑
在测试表维护完成后关闭该表自动 vacuum，阈值不变。仅证明本规模单会话，无新 TAP/构建或真实模型请求。
8 项预算/HTTP seam 测试与既有本地 83 项均通过；零任务双会话探测复现当前串行服务限制，多会话未实现。

2026-09-02 的 [Filter INSERT 修复](postgresql/semfilter_insert_20260902/README.md)绑定最终 `39007150`：
生产改动只在 `sem_filter_path.c`，支持上拉后的单表 INSERT 源查询，公共层与语义身份不变。
PG18.3 warning-free `-O2 -Werror`、regression 1/1、完整 TAP 919/919、本地/服务器各 83/83 与 C11
通过；新增 INSERT 171 项实际验证行集、三值/NULL、计数、LIMIT、prepared/invalidation、部分写入
回滚、目标约束及 choice RLS/权限/取消恢复。不支持形状继续拒绝，旧失败与所有原始尝试保留。
未运行真实模型、新资源 smoke 或校准；不是四 C 全部完成，也不是模型质量或成本准确性证据。

2026-09-02 的 [PG choice 接线](postgresql/choice_pg_wire_20260902/README.md)绑定最终 `80bb7fc5`：
中立 open spec 与 C v4 已进入公共 runtime；PG18.3 warning-free `-O2 -Werror`、regression 1/1、
TAP 748/748、本地/服务器各 83/83 通过。218 项新 SELECT 检查覆盖三值/NULL、Unicode/空串、
prepared/invalidation、savepoint、错误/取消、旧端拒绝和实际 HTTP 参数对照。仅有 fixtures，
未做新资源或真实模型验证。旧/新二进制均复现 Filter INSERT 未 lowering（`55000`），不能再将
Map 的 INSERT 验证外推到 Filter。早期 150 项因人工结束等待及断言漏检，不计入有效验收。

2026-09-02 的 [gateway v4 接入](postgresql/choice_gateway_v4_20260902/README.md)绑定 `7d72d9ad`：
本地/服务器各 83/83，PG18.3 warning-free `-O2 -Werror`、regression 1/1、TAP 537/537 通过。
新增 15 项验证严格 profile/字段/版本/摘要、显式 choice HTTP 映射、拒绝后不降级、原始结果保留。
仅使用 fixtures；C/SQL/TAP 未修改，PG schema 3 仍以 `0A000` 拒绝执行。没有真实模型、新资源
smoke 或成本/质量结论；不能把 gateway 支持写成 PG choice 已接通。

2026-09-02 的 [PG plan 接入](postgresql/choice_pg_plan_20260902/README.md)绑定最终 `134447dd`
（生产实现 `00cc6bbf`）：PG18.3 warning-free `-O2 -Werror`、regression 1/1、TAP 537/537，
本地/服务器 Python 各 68/68。新 SQL option/schema 3 保存完整 profile，验证复制、严格解码、
prepared/invalidation、旧校准拒绝与零 provider 连接；实际执行明确拒绝，port/wire v4 尚未接入。
前一版 socket 测试配置不正确，536 项结果不作为最终零连接证据；修订前后日志均保留。

2026-09-02 的[choice profile 值合同切片](postgresql/choice_profile_contract_20260902/README.md)绑定
`d26e210d`：本地/服务器 Python 68/68、standalone C11 与 PG18.3 warning-free `-O2 -Werror`
仅构建通过。它只证明新值类型、严格校验和 canonical bytes 一致；SQL opt-in/schema 3/wire v4
未接入，AiOpenSpec 未扩展。没有新 PG regression/TAP、真实模型或性能/质量证据。

| 等级 | 含义 | 可以声称 | 不可以声称 |
|---|---|---|---|
| 设计预留 | 只有接口、设计或后续计划 | 架构允许扩展 | 机制已经可用或有效 |
| 功能测试 | 有单元测试或纯控制器测试 | 局部语义符合预期 | 端到端可运行或性能更好 |
| 契约门禁 | 真实组件小规模链路通过，或执行路径集成测试通过 | 数据、身份、约束和指标链路正确 | 小样本性能排序可泛化 |
| 真实 GPU 筛选 | 有真实 PostgreSQL/Daft/Ray/vLLM/GPU 对照 | 在给定设置中观察到差异 | 跨规模、跨模型或跨硬件泛化 |
| 重复或留出验证 | 有随机顺序重复、跨到达率或留出规模验证 | 当前实验范围内的机制判断 | 未测试环境中的普遍结论 |

代码测试和性能证据必须分开记录。通过单元测试仅说明实现符合约定，不等于吞吐、尾延迟、能耗或 MFU 得到改善。

## 报告与历史机制快照

报告按[结果导航](README.md)定位；实际设置、全部重复、失败和机制判断只在原主报告维护。
截至 2026-09-02 的机制完成度表与早期目录摘要保留在[固定版本](https://github.com/3444374/ai-operator-execution-optimization/blob/0b8aca5ff599e1b6525b77608e3865b20a4fd996/experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#2-机制完成度与当前结论)，按原日期解释。

## 4. 统一复现入口

大多数新实验由 seeded scenario runner 执行：

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  code\scripts\experiments\run_ai_operator_scenarios.py `
  --config <scenario_config.json> `
  --profiler code\scripts\profiling\postgres_ai_operator_profile.py `
  --python-executable D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  --output-dir <run-output-directory> `
  --health-url http://localhost:8000/health `
  --metrics-url http://localhost:8000/metrics
```

`scenario_config.json` 是参数来源；`manifest.json` 的 `completed_runs[].command` 保存每一轮展开后的精确命令，`incidents` 保存失败与恢复记录。早期 baseline 使用独立脚本，按其目录 README 复现。

代价估计使用：

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  code\scripts\analysis\estimate_operator_cost.py `
  --input-csv <runs.csv> `
  --output <model.json> `
  --target e2e_s `
  --test-fraction 0.25 `
  --seed 20260726 `
  --alpha 1.0
```

## 5. 一次实验应保留的证据

按[实验规则](../AGENTS.md)、[结果保存规则](AGENTS.md)与[报告核对](../plans/reference/报告核对.md)核对本次适用证据，
台账不重复维护指标、归档或统计要求。

当前能力与缺项从[代码状态](../../code/INFRA_STATUS.md)读取，2026-08-31 的缺口快照按上方固定版本回查。
以下保存当时的镜像摘要与存放决定，已由[当前保存规则](AGENTS.md)和本台账的恢复核验记录替代；
其中的“待迁出”与“不进git”不作为当前要求。

<a id="storage-history-20260807"></a>
<a id="storage-history-20260807-实验数据存放政策2026-08-07-修订"></a>
## 历史存放记录（2026-08-07）
<a id="storage-history-20260807-政策"></a>
### 政策

- **raw（per-run requests/submissions/resources/flush CSV + stdout/stderr log + artifacts + during-cell gauges）**：
  存 AutoDL 服务器 `/root/autodl-tmp/{ai-operator/experiments/results, ai-operator/motivation/results, experiment-artifacts}`
  + 开发者本地机 `C:\Users\ays\Desktop\results\`（2026-08-07 全量镜像）。**不进 git。**
- **git 只放**：aggregated CSV（runs.csv / formal_summary.csv）、summary（ramp_aggregate.{json,md} / ramp_run.json）、
  manifest、README、代码、计划文档。

<a id="storage-history-20260807-本地-raw-镜像cusersaysdesktopresults2026-08-07"></a>
### 本地 raw 镜像（C:\Users\ays\Desktop\results\，2026-08-07）

3 个 transport tar.gz 从服务器打包下载，sha256 与服务器逐一校验一致：

| archive | size | sha256 | 内容 |
|---|---|---|---|
| experiments_results.tar.gz | 189M | `1c9e940d6a190f5cc6cc97008ed74d56dd825a0da6f2552edfb941f05a41377c` | experiments/results/ 全量（10082 entries，含全部正式 + screening 实验 per-run raw） |
| experiment_artifacts.tar.gz | 57M | `e3d586fcb3ba7399d4598a908f856a390f15d0e2fd34c85aebf3e0d0283769cc` | experiment-artifacts/ 去 retired-worktrees（7813 entries；含 320-run raw 67M、highcv-rerun 89M、gates、image gates） |
| motivation_results.tar.gz | 1.2M | `de4198e81c0fa4894be2da88ea71039315ab0a97de29147579a2896cf208eff4` | motivation/results/（174 entries，image CLIP/embed 实验） |

解压后约 1.2G，镜像服务器目录结构（`experiments/results/<exp>/`、`experiment-artifacts/`、`motivation/results/`）。

<a id="storage-history-20260807-订正"></a>
### 订正

- 2026-08-07 早些曾称"320-run per-request raw 已被服务器清理"——错误。raw 实际在
  `experiment-artifacts/dual_gpu_cost_profile_formal_v2_cache_on_20260807/`（67M），已随 experiment_artifacts.tar.gz 落本地。

<a id="storage-history-20260807-已撤回的-git-tarball同日早些"></a>
### 已撤回的 git tarball（同日早些）

曾把 7 个实验 raw 打成 tarball 提交 git（commit 40471bf：07-29 dual_gpu batch 5 个 + enhanced ramps 2 个）。
按本修订政策已 force-push 撤回（reset 回 c250e19），raw 改存本地。formality 三审记录（6-dim 对抗式 workflow，
24 候选，T1/T2/T3 分类）见会话 workflow journal 与 PROJECT_LOG 2026-08-07。

<a id="storage-history-20260807-仍残留在-git-的-raw-tarball待定夺"></a>
### 仍残留在 git 的 raw tarball（待定夺）

`operator_cost_profile_pilot_20260804/v1_diagnostic_raw.tar.gz` + `v2_raw.tar.gz`（更早 commit 的 pilot/diagnostic
raw，被该实验 README 引用）。按新政策属应迁出项，但非本次新增，未单方面删除。

<a id="result-index-notes"></a>
## 原结果入口的补充说明

早期目录摘要已按上方固定版本保存；以下保留额外目录或来源说明。

以下合并原结果首页截至2026-10-02的独有说明，原日期、失败与数据来源继续保留；较早的未完成描述由本报告后续记录补充。

来源章节：原结果入口。

结果、日志的保留与去重见[保存规则](AGENTS.md)；2026-10-02的全目录检查、导出修复和剩余存储问题见
[证据台账中的存储检查](EXPERIMENT_EVIDENCE_REGISTRY.md#storage-review-20261002)。

历史结果已按这些要求分类和整理，[处理与恢复核验记录](EXPERIMENT_EVIDENCE_REGISTRY.md#legacy-retention-20261002)
说明范围与来源。报告直接引用的数据、复算脚本和原始运行身份继续保留；归档数据与脚本从各结果的
`raw/storage-manifest.jsonl`按[文件恢复说明](../../code/scripts/README.md#实验结果恢复)恢复。

来源章节：统一入口。

先读 [`EXPERIMENT_EVIDENCE_REGISTRY.md`](EXPERIMENT_EVIDENCE_REGISTRY.md)。该台账只登记主要机制结果与证据强度，
区分“设计预留、功能测试、真实链路验证、GPU 筛选、重复或留出验证”，避免把代码完成度误写成性能证据。
它不定义工程架构或下一步实施顺序。

来源章节：状态感知 phase-change（2026-08-11）。

| Directory | Content | Boundary |
|---|---|---|
| `phase_change_state_aware_corrected_early_stop_20260811/` | 修正门禁、HTTP tail-drain 与多 Job 全局 arrival clock 后的 A-only + 三档 pressure | A-only 证明 K160 相对 K128 service rate +7.77%；B=2.5/3.5/4.5 均未形成双 endpoint、双周期降档压力，按门禁停止，未跑 action/formal。 |
| `state_aware_work_unit/saor_matched_ready_selector_rehearsal_20260813/` | frozen-static 与同 bounded-ready observation 下 FIFO/DRR/VTC-style/strict-priority/SAOR 的双轮 Project 内部 selector 归因 rehearsal | 12/12 cell、0 incident；DRR/VTC-style 约 12.90K tok/s 且 fg 30s SLO 零违约，SAOR 12.28K tok/s、fg P99 17.85s。SAOR 是观测到的效率—tail 非支配折中点，不是 selector winner；固定顺序 n=2 且 selector 级 non-inferiority margin 未预注册，`formal_authorized=false`。原生 baseline 数量为 0。 |
| `state_aware_work_unit/saor_ready_observation_bridge_rehearsal_20260813/` | frozen-static→single-head shared FIFO→bounded-ready FIFO 三臂双轮 Project bridge | 6/6 cell、0 incident；共享容量 tok/s +25.96% 但 fg P99 +99.17%，bounded-ready 在相同 FIFO 下再使 tok/s +7.30%、fg P99 −33.62%，仍有约 40% fg SLO violation。分离了效率/隔离与 observation 效应；不是 native baseline/formal。 |
| `state_aware_work_unit/saor_project_mechanism_rehearsal_20260814/` | frozen-static + 同 bounded-ready observation 的 FIFO/DRR/VTC-style/strict-priority/SAOR 最终六臂 Project mechanism rehearsal | `63d17300` root 6/6、0 incident，validation passed；6,144-request fixed-output-cap audit 通过，SAOR 96/96 recovery completion、15/15 repayment completed、P95 3.234s、0 unresolved，1,108/1,108 projection 一致。单次相对 VTC-style lag P95 −13.15%、no-service +0.014%；只进入独立审核，不判排名、不授权 formal。 |
| `state_aware_work_unit/saor_native_system_matched_manifest_20260819/` | native-system matched 的 Job0/Job1 SHA、512-row、合并顺序与独立 formal 授权模板 | 完整 prompt manifest 留在 Git 外；readiness 逐 Job 校验并要求 combined=Job0+Job1。merge 与 rehearsal 均不自动授权 formal。 |
| `state_aware_work_unit/saor_native_system_matched_calibration_20260819/` | Project selection 与 Daft Native/Daft Ray/Ray Data 原生执行 selection identity | Daft C1/B1 是 vendor control，Ray Data C8/B16 是一次 development screen 冻结点；只声明身份匹配，不夸大为统计最优。 |
| `state_aware_work_unit/saor_native_system_matched_gateway_rehearsal_20260821/` | 五臂统一 T0--T4 与 observation-only gateway 的单次 GPU rehearsal | 5/5、exactly-once、archive validation 通过；SAOR 相对同 executor static 吞吐 +31.01%、group JCT −23.70%，但 P99/lag/no-service 变差、Jain −1.50%。只作 rehearsal 观察；0s/5s pre/post isolation 样本不足，formal 未运行。 |

来源章节：开题统一文本 database-E2E（2026-08-08 历史结果）。

以下开题时期结果按原实验身份保留；开题已结束，不从这些记录自动安排补测。
归档不改变当时的有效性、失败判定或结果适用范围。

| Directory | Content | Boundary |
|---|---|---|
| `opening_database_e2e_text_20260807/` | 首轮相同三臂合同 | project feeding 89.93%/91.38% 未过门，只保留为 failed-feeding 历史诊断，性能数字已被 replacement 取代。 |


来源章节：开题文本多 Job 干扰（2026-08-09）。

| Directory | Content | Boundary |
|---|---|---|
| `opening_fourjob_interference_20260809/` | 1 short+3 matched long 的 Project full/quarter/static/shared 因果分解，以及 Daft Native/Ray、Ray Data 原生 single→four-job 三重复 | 按三次 formal 均值，Project shared 相对 static 总吞吐 +8.68%，四个 Job JCT 全部改善，属于效率/JCT 子向量上的 baseline-relative empirical Pareto；但 raw-work Jain 0.960→0.923、long 收益不均，且 long1/2 未达到 quarter-solo 非劣。报告 full/reserved/static 三反事实；历史 compact 数据不足以补 event-level service lag。只作轨内证据，不作完整 Pareto、理论公平性质或跨框架绝对排名。 |

来源章节：图像 AI_EMBED operator（2026-08-03/04）。

| Directory | Content | Boundary |
|---|---|---|
| `image_ai_embed_operator_formal_20260803/` | 60K×2 held-out Ray Data/project 2×2 CPU formal、Daft 12K capacity consistency，以及 schema-v12 派生观测 | Ray/project 同规模 matched-resource 结论有效；Daft 因物化容量上限单列。跨规模只描述独立平台上的 images/s/单位资源，absolute JCT/first output 不混排。 |

来源章节：双 GPU 调度与容量（2026-07-28/29）。

| Directory | Content | Boundary |
|---|---|---|
| `multicard_scale_ramp_enhanced_20260807/` | bounded HTTP + harness-pre-split DuckDB AI 的九档规模曲线，三重复并补齐 during-cell vLLM gauges、身份、能耗和失败 cell 紧凑证据 | bounded 27/27、DuckDB 22/27；两路径身份与计时粒度分开，不作产品原生多 endpoint 排名；逐请求输出和日志留服务器。 |
| `multicard_lbrr_scale_ramp_enhanced_20260807/` | 单 DuckDB→nginx round-robin→双 vLLM 的九档规模曲线，27/27 三重复紧凑证据 | `gateway_system_diagnostic`，query-barrier 口径；不冒充 DuckDB 原生调度，也不与 request-level E2E 延迟混排。 |


来源章节：Local Baselines。

| Directory | Content | Boundary |
|---|---|---|
| `operator_cost_profile_pilot_20260804/` | 双 4090 四候选 cost-profile v1/v2 运行合同门禁与完整 raw trace。 | v2 8/8、0 incident、512 unique requests/cell、23 维四向量同 context；n=1 只验证采样合同和约 4 小时 formal 预算，不作配置排名。 |
| `operator_cost_profile_dual4090_formal_20260804/` | 首次双 4090 320-run formal 的并发 runner 与空 Ray 地址事故审计。 | 两套输出均排除：几乎全程共享 GPU/vLLM 竞争，且 640/640 子运行启动 local Ray；本目录不含性能结论。 |

本目录保存正式研究实验结果和小范围优化测试记录。

来源章节：当前状态。

正式优化实验已经开始；本目录保存方法实验。早期 GPU-backed 画像和动机实验仍位于：

```text
experiments/results/motivation/gpu/
experiments/results/motivation/pg18_4_fake/
experiments/results/motivation/fake_cpu/
```

新增结果必须对应两项研究内容、共同代价估计或多模态泛化中的明确问题，并同步
`EXPERIMENT_EVIDENCE_REGISTRY.md`；不要从本目录列表推断当前执行优先级。

保存、命名、共享与精简按 [结果保存规则](AGENTS.md)和[上级实验规则](../AGENTS.md)执行。
本页维护导航摘要，详细数字与原值以单个结果报告及证据台账为准。

来源章节：历史实验解读。

组件检查的读法见[诊断入口](diagnostics/README.md#早期组件检查的读法)，fake/CPU机制分析见
[原始分析](motivation/fake_cpu/analysis.md)，数据库预演见[系统画像](motivation/pg18_4_fake/system_profile.md)。
实际GPU模型、CPU模型、fake、PG18.4预演与当前PostgreSQL载体各自保留身份；判断先核对来源与计时。
多Job实验从真实重叠与独立参照解释干扰，数据组织和提交变化回到完整查询与任务质量中评价。
