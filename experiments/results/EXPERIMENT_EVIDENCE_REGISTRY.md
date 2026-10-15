# 实验与机制证据台账

2026-10-15：[原始 Animals Q1](diagnostics/sembench_animals_20261015/README.md)固定官方完整目录／seed42 SF64及原SQL／聚合评价。原查询CPU资格、许可逐文件证明及单VLM三后端真实小轮已分列；保留原始失败、Daft初轮并发超限与容量补轮、完整请求及逐图核验，Core显式大请求入口已通过113项CPU／真实Ray／PG回归，Core原64图模型仍pending，不作CLIP、排名、稳态或尾分位结论。

2026-10-15：[PZ Movie 原生接线](diagnostics/palimpzest_movie_20261015/README.md)锁定 c814e／6efc 源码，实际原库 CPU 与8行localhost fixture的 Q3／Q2共12POST通过，模型0／GPU0。Q2 LIMIT5仍6调用；FP1／FN1抵消COUNT的反例保留。完整原始 Movie 模型、五组执行及性能资格尚未完成。
2026-10-15：[Ray2.56.1迭代器构造故障](diagnostics/lotus_native_image_retrieval_20261014/README.md#ray-iterator-construction)。
真实CPU Data／Serve替身证实构造异常未走原生end hook，最小兜底使本图线程停止；unknown和原MemoryError保留，owner仍负责最终退出。
68项单测及独立1项显式CPU实库回归通过，原件留仓外、最终Ray进程清单为空；无模型／GPU或旧来源资格外推。

2026-10-15：[常驻query trace归档边界](diagnostics/lotus_observation_modes_20261012/README.md#persistent-query-archive)。
固定331源码的64×120合成已结束记录／实际asyncio复现保留7,680行、累计复制249,600行；按query归档后累计仅复制7,680行，
unknown／未结束责任及失败证据保持。68项本地定向通过、4项条件跳过；原始失败及复现留仓外，无新增模型／GPU，不归因旧延迟。

2026-10-15：[普通图片输入与 Ray Serve 补充](diagnostics/lotus_native_image_retrieval_20261014/README.md#native-input-and-serve)
分列`460d3870`三格与`17ac2aeb`八格实际来源；共28查询／39完整矩阵复用已有4个数组，原件独立复核通过。
同形状图片向量逐值同stock，文本CPU／CUDA与跨batch差异保留；实际生命周期、完整查询原值和退出按来源记录。
新默认支持小规模正确性和复用资格，不支持稳态、并发、性能排名或可靠尾分位；IMLane图像继续pending。

2026-10-15：[文本原生配置的有限校准](diagnostics/native_text_capacity_20261015/README.md#native-config-calibration)。固定`1c05a191`的六配置连续原Q2；首次、五次后续、资源／HTTP原值和私有观察器失败由同题报告拥有。IMLane n4仅推荐后续同预算配置，Ray A4不冻结稳态赢家；单owner固定块顺序、质量不足与历史独立服务scope保持，不作调度归因或长测完成结论。

2026-10-15：[IMLane两作者模式连续LOTUS查询](diagnostics/native_text_capacity_20261015/README.md#imlane-owner)。固定`4271dc44`、显式`reuse_bridge=True`，20fixture／0模型软件检查独审通过；随后现有公开Q2各同owner两次，480模型／1440三跳、全200与自然退出实际通过，原件独立审核通过。bridge身份、创建／注册1→0、usage与SDK关联单列；和旧四路径分开服务scope，累计12查询／1440模型／4320三跳。0发送私有核验失败保留，无新模型预算或旧四臂重跑；每臂n2、7FN与未调优工作点不支持稳态排名。

2026-10-15：[Daft显式RGB输入](diagnostics/lotus_native_image_retrieval_20261014/README.md#daft-rgb-input)固定`793204cc`：
本机SDK51项、Linux144项及25个通过子测试分列且零跳过，实际像素与完整slow张量同PNG逐位相同；
stock／Daft PNG／RGB的12条完整LOTUS查询、15矩阵与退出通过。全部PNG／RGB向量与top10逐位相同，
图片对stock最大差0、文本`5.96046448e-8`。准备失败／未知状态保留，三模型CLI均成功；小样本不提供排名或尾分位。

2026-10-15：[LOTUS图像建库后连续调用](diagnostics/lotus_native_image_retrieval_20261014/README.md#resident-query-measurement)固定`485c86a8`，九格180查询／189完整矩阵及实际模型出生计数已核对。原监督stdout失败仍failed，续跑只补两格；CPU只计一次，十二次实际监督调用均退出并清理。公共输入减负改善与Ray逐RM加载的负结果按当前接法解释，五次固定payload不提供可靠尾分位。


2026-10-15：[LOTUS文本薄观测与常驻连接](diagnostics/native_text_capacity_20261015/README.md#thin-lotus)。3个新完整LOTUS函数／4次CPU执行通过，Ray Data／Daft缓存三态、200／429与既有SemLoom路径分列；performance原SDK重复raw由32条／46,202B降为0，未据此推导性能收益。`efa65c18`跨query池复用与Cookie隔离保持drain／失败停止；`1ba0fc1e`三臂720及原生LOTUS另scope240模型请求、共八条完整Q2均闭环并独审通过。HTTP经验P99每臂n240，完整query仅n2；质量仍7FN/120，不作稳态排名。`4271dc44`的IMLane owner复用资格单列。

2026-10-15：[文本原生API、容量与Daft清理](diagnostics/native_text_capacity_20261015/README.md)。
固定`f2749bf1`的CPU localhost SDK观测、回归及原失败分别保存；支持活动HTTP／批次存储分离和清理责任修补，
不支持模型收益、全图内存保证或Mac上的Linux完整query观测。LOTUS方法与IMLane生命周期边界见同题说明。

2026-10-15：[重复均值区间修复](../../code/src/observability/README.md#repeat-mean-confidence)来自独立 `4b1c834b`，本集成保持三个交付文件同blob，固定源码40项本地定向检查通过。
仅修正保守临界值查表与可规避的半宽中间溢出；固定 `277d8f48` 留存材料未确认已发布摘要受影响，未改写历史观测。样本独立性、总体尾分位与既有有符号极值的标准差／CV可表示性不由本修复认证；无服务器、模型或GPU运行。

本台账登记证据身份与可支持的判断，实际当前能力见[代码状态](../../code/INFRA_STATUS.md)，
执行顺序见[总纲](../../PROJECT_OUTLINE.md#8-当前执行顺序)，报告导航见[README](README.md)。
下列日期条目是按来源保留的历史执行／接纳记录，后条不把旧来源实测转移到新源码。
最终文本三条Q2的新来源验证未通过，图像常驻连续查询仍在准备；旧Q2及图像单次资格不能替代它们。

2026-10-14：[融合源码LOTUS图像资格](diagnostics/lotus_native_image_retrieval_20261014/README.md#final-integrated-qualification)固定`2afcaf47`；十格完整原生查询、全量数值及物理清理核验通过，参数、SDK子测试口径与缺失观测见同题结果。仅支持该固定配置的单次资格；旧轮数值及失败身份保持。

2026-10-14：[原Movie任务完成期限兼容](postgresql/native_executor_baselines_20261012/README.md#original-movie-completion-integration)已接纳；默认有限期限保持，显式空总期限不安装故障监督，本地资格及历史ab3来源分列。

2026-10-14：[LOTUS PNG 参数](diagnostics/lotus_native_image_retrieval_20261014/README.md#png-candidate)在`e50dd2b9`后可选接纳；
融合版本地103项全通过，保留关闭／未知责任、默认容量与拒复用，补观测取消首错及清理错误优先级。
固定`eab9f11a`的Direct／HSE两12图及四256图格保存30矩阵／24完整原生查询，固定字节容量及退出检查通过。
每格一次观测支持输入编码成本诊断；Direct／HSE单轮应用区间约40%降低不含启动，不支持性能排名、六路径新资格或可靠尾分位。
共享编码成本不作SemLoom独有创新；原六路径、默认结果与CPU诊断失败来源分别保留。

2026-10-14：[文本 Ray Data 默认／详细计时清理差异修补](diagnostics/optional_execution_timing_20261014/README.md#ray-cleanup-parity)。
基于固定`da379b2577118d58b890cff3fca6400af7bda8f2`，实际导入仓库函数复现首错被close替换、shutdown跳过及Iterable未关闭实际iterator。
最终本地定向57通过／1缺Ray导入跳过，审查发现的次错交接与异常notes自复制反例已修复；修前失败及源码摘要保留。
较早修订的共享计时42项与24次localhost替身POST独立记录，不合计为最终源码或真实Ray资格。
接续固定`1d6453d`的[限定实库核验](diagnostics/optional_execution_timing_20261014/README.md#ray-cleanup-sdk)：
Ray2.56.1／Arrow24.0.0、393份输入前后匹配，唯一四图／8次localhost POST、4 CPU／0模型／0GPU。
预定清理／writer故障首错与notes通过，查询仍non-clean／unconfirmed拒绝复用；controller及60份已登记自有出生身份absent、lease释放。
4份可见计时文件46事件／22对span／2个注入未配对边界保持；不新增模型、GPU性能或活动worker ValueError组合资格。

2026-10-14：[HSE源关闭核查](diagnostics/hse_static_lifecycle_20261014/README.md#source-iterator-closure)：
固定da379b完整函数的traceback／gc反例及修前16组失败保留；修后10模块102项本地替身通过，源关闭不代替actor／device完成证明。未运行服务器、真实SDK或模型。

2026-10-14：[单GPU文件CLIP三条已有路径](diagnostics/image_clip_file_operator_20261014/README.md)。
实际来源45da0a54／发布同字节58462a0d，3资格＋3scale＋9轮转重复，共61,731模型图；全部原row、18次退出收据及两次模型前部署失败保留。
固定冷算子数据只作这一工作点观察；原15轮没有逐ID向量，其摘要差异不由单纯输出顺序解释。[最小另窗诊断](diagnostics/image_clip_file_operator_20261014/README.md#numeric-diagnostic-20261014)随后完成963模型图与5次退出核查：257行Direct/HSE逐位相同，Ray输入指纹一致、模型返回不同，实际批形态不同；全部差异及1e−3超界保留。5000图逐行等价、具体浮点因果及下游质量仍未证实；诊断不改原15性能值，未重跑性能矩阵。
私有原包按run ID＋SHA保留，公开只保存脱敏紧凑观测／来源和复算；PG、稳态最优、HSE非劣及GPU异常责任未由健康轮追加。

2026-10-14：[LOTUS／Daft 图像集成接纳](diagnostics/lotus_native_image_retrieval_20261014/README.md#集成接纳与关闭修补)：四运行文件关闭与证据优先级修补，105本地通过／14条件跳过；96条原查询完整复算，source-d/f 数值和失败保持，未追加融合版模型／性能资格。

2026-10-14：[原生 LOTUS 图像检索资格](diagnostics/lotus_native_image_retrieval_20261014/README.md)。
保留实际模型参数／预处理身份、图片输入负例、完整图库／在线向量及原生top10；六臂12／64／256图和已有索引通过。
24个模型CLI、96条完整查询及所有保存矩阵可独立重算，Daft原交付blob保持、两次模型前失败逐运行登记；
原件与逐次物理退出及lease释放保留，不提供性能排名或P99／P99.99。

2026-10-14：[Daft 本地 CLIP 原生批次资格](diagnostics/daft_clip_native_20261014/README.md)。
固定0.7.21真实图及 RGB decode，CPU kernel 替身本机／匹配 Linux 各 13 项、旧图像回归 39 项通过；软件 fixture 模型／GPU／HTTP 0。
独立9图GPU复测四次完整512维FP32向量同批形状Direct逐位一致，398参数／2buffers恰好终结一次，CLI／lease正常退出；
只支持batch32下9行未满批次的数值及正常生命周期，不支持完整LOTUS耗时、持续稳态或尾分位。
最初SDK重载假设及首GPU错误清理判据失败均保留；生产后端两轮未改，可选计时借用原模块。

2026-10-14：[HSE可选设备组选择](diagnostics/hse_device_dispatch_20261014/README.md)：A上显式策略的本树187通过／2缺驻留切片跳过，
固定42186dbe七成员组合2通过；实际Core／broker和Torch／Ray替身分别记录。
随后固定4c7efa79单节点CPU实际Ray软件11/11通过，4 CPU／0 GPU／512MiB，49 PID与控制器退出、lease释放；首轮fixture失败保留。
Torch／CUDA、多卡／GPU性能及P99／P99.99仍未验证。
接续[整合检查](diagnostics/hse_device_dispatch_20261014/README.md#integration-acceptance)：组合独立literal guard与unknown queue，
318通过／14明确跳过，原件与5份源码差异分列；不改标原11项实库来源，不合B/C或驻留实现。

2026-10-14：[缺失waiting的真实helper回归](../../code/tests/experiments/test_shared_vllm_runtime.py)。
默认四样本中未知回到(2,20)，已知0仍升到(4,40)、已知1仍降到(1,10)；另核有效KV／恢复及typed消费者，155项本地通过，HTTP／模型／GPU／服务器0。仅正确性证据。
最终[单提交整合](../../code/src/experiments/shared_vllm/README.md#unknown-waiting-integration)13模块196项全通过，覆盖保存重读与旧消费者；与155项重叠分列，没有新服务器或性能实验。

2026-10-14：[可选执行计时](diagnostics/optional_execution_timing_20261014/README.md)。
在已发布 runtime／EOF 修补之上增加默认关闭的账本与公共适配计时；复用原调用身份和局部时钟，
六文本接点／图像外层的 fixture、开销、失败及原件对应分别保存。未采集的内部阶段保持未知，
服务器四个独立 Ray 图恰好 8 次固定响应、结果／计数／EOF 清理通过，缺 worker 关闭标记如实保留。
软件结果不作为模型／GPU 性能证据，原模型窗口及账本不改。
接续[集成验收](diagnostics/optional_execution_timing_20261014/README.md#integration-acceptance)与两处图像故障修补，
原输入334项及修后57项按来源／重叠分列；未新增服务器或实际GPU资格。

2026-10-14：[公共Ray启动、正常EOF关闭及实库补验](postgresql/native_executor_baselines_20261012/README.md#native-adapter-completion)。
从实际整合`1b90c1ea`恢复，仅修项目适配层的跨后端误拒与EOF关闭异常复用；原图、执行器和方法保持。
受影响本地Ran90，89通过／1缺Ray导入检查跳过；两项修前反例、专项重叠关系、摘要及交叉审核归于同一报告。
随后五个目标用例按各自源码通过，三次窗口共14逻辑／28两跳localhost POST；七次尝试中的
工具断言失败和CPU1 Data零发送超时保留。CPU声明两行修补后，CPU1在runtime前拒绝、CPU2／A1实际图通过；
原生图与kernel保持，源码包与各测试修订、完整响应／发送账本复算和独立退出由同一报告定位。
原5项／120POST软件证据及单卡八查询／8480行关联另按既有固定来源解释；新增模型／GPU／安装均0。
`58e15328`接续`ed1867eb`的[整合验收](postgresql/native_executor_baselines_20261012/README.md#native-completion-integration)
另有98通过／13明确跳过及原件离线复算；源码、重叠与实库来源分列，没有新的服务器执行。

2026-10-14：[JPEG 文件源及 CPU 工厂退出的固定 SDK 检查](diagnostics/image_file_sdk_20261014/README.md)。
`964e70ef` 三项实际检查通过、0 跳过；reader 各 4 行顺序／编码字节与两个 CPU actor 的 shutdown 前物理退出分别核验。
首次 lease 交接未进入 SDK，失败与成功及 66 Git 依赖摘要保留；仅支持这些组件，不增加 HSE／GPU／CLIP 性能或完整 CLI 退出资格。

2026-10-14：[原生参数与HTTP活动核查](postgresql/native_executor_baselines_20261012/README.md#native-http-activity)。
本地配置、图替身与HTTP／事件循环责任检查按本地来源保留；随后固定`81a38e52`的
Daft／Ray Data活动及Ray新进程导入5项实库检查通过、120localhost POST，模型／GPU0。
逐成员配置、观测、资源与独立退出核查由同一报告定位；单次软件检查不增加方法性能或端到端资格。

2026-10-14：[IMLane桥生命周期检查](diagnostics/imlane_bridge_lifecycle_20261014/README.md)。
Python协议／localhost fixture与真实Arrow复制原语分别验证；
默认逐批桥保持，不将context复用当作HTTP连接复用或新调度算法。
新增[薄验证器本机阶段](diagnostics/imlane_bridge_lifecycle_20261014/README.md#local-verifier)覆盖同执行器多批、
原SDK消费替身及错误隔离；源锁、资源／lease和首错保护已检查。
接续[固定6916四格实库软件资格](diagnostics/imlane_bridge_lifecycle_20261014/README.md#author-session)通过，生产适配来自9766，
保留6项Ray故障SHM清理的原始非clean记录；只支持同步内核与有限原SDK消费，不替代原规模模型／PG或async组合。
2026-10-14：[HSE A集成检查](diagnostics/hse_static_lifecycle_20261014/README.md)：29项定向CPU／受控Ray替身通过，
A不含历史扫描优化和ready优先。机器记录逐片绑定source摘要；原47f完整CPU成本与失败证据独立保留，不提供A实库／GPU资格。
接续[独立45da A-only资格](diagnostics/hse_static_lifecycle_20261014/README.md#integration-a-qualification)
5项实际Ray和1项真实Daft/Arrow准备数据适配通过、0跳过；源码、两环境与27进程／controller／lease退出另记，模型／GPU／HTTP0。
原ABC五项保留原source；本轮CPU synchronize不增加CUDA、CLIP性能或完整CLI资格。
独立[重复GPU handle校验](diagnostics/hse_static_lifecycle_20261014/README.md#literal-gpu-handle-guard)
仅有本地拒绝与正常分派回归，未新增实际Ray／GPU资格；原固定A测量不转移至新guard。

2026-10-13：[集成分支LOTUS整批文本预检](../../code/src/semantic_methods/lotus/README.md#whole-text-preflight)。
本地内存后端定向66项，31通过／35缺固定依赖跳过；新增后窗口超限／配置不符、取消／到期、本地和远程factory及首call持有检查。
旧实现12组后窗口反例已复现；修后显式预检零flow／零发送，默认分块的合法前缀及未消费责任保留。该本地轮SDK／HTTP／服务器／模型均0。
随后[固定1afa实际SDK最小资格](postgresql/native_executor_baselines_20261012/README.md#lotus-text-sdk-preflight)6项通过／0跳过、13localhost POST，模型／GPU0；首次0POST profile失败、原300秒累计窗口与独立释放保持，不替代35项全量或远程IPC资格。
固定stage修补仅为来源；其图文／实际Daft/Ray软件结果不作为本集成HEAD的实库验证或性能结论。

2026-10-13：[DuckDB六路径源码、实库与公开SQL检查](postgresql/native_executor_baselines_20261012/README.md#duckdb-paths-20261013)。
固定native来源`a0f3fbd3`，原调用、解析及当前向量屏障保留；新`native4`构建、固定实际库和六条常驻CLI软件检查完成。
逐次运行／源码、失败、预算与清理归于同题机器事实和原件；模型／GPU及新匹配性能未运行，不能借用旧LOTUS成绩。
统一分支按来源移植专属交付`3f0b62d6`并去重公共修补，本地受影响组合120通过／32实际库或opt-in跳过；
LOTUS最小session工厂及Ray退出报告接口组合15通过／21固定SDK跳过，两组重叠。
以上整合检查没有新增服务器执行，接入owner的固定source实库结果与新统一HEAD分别记录。

2026-10-13：[原Q2阶段复算与私有账本诊断](diagnostics/native_sembench_model_dual_supplement_20261012/README.md#retained-stage-analysis)。
72个已有性能查询分解同机墙钟及匹配服务计数；三个服务器账本只读确认无索引、四条计数全表扫描。正／反顺序localhost三跳fixture的扫描CPU及完整请求随历史增长，机制在本地成立；现场因果占比未测，旧真实9360逻辑／28080POST不减估计开销。同事务持久计数修订已通过14项回归；12组相同条件配对中23000行完整fixture请求从约97–105降至39–43毫秒，小账本略慢保留。上述诊断本身模型新增0；修订随后已部署到[四组匹配原Q2](diagnostics/native_sembench_model_dual_supplement_20261012/README.md#matched-budget-repair)，独立完成48槽／96查询、11520逻辑／34560POST，全部原值、两个服务历史与质量限制保持；后续工作点与长测仍待各自方案。

2026-10-13接续：[原Q2最后S／R独立补测](diagnostics/native_sembench_model_dual_supplement_20261012/README.md)。
固定3bd／原模型与参数，短Ray父目录22字节，四次真实零模型预检通过；只S／R各预热1测量1，480逻辑／1440POST、完整150.487／600秒。每副本240请求、337身份退出、CPU违规0／GPU空，两个旧时钟与账本保持。双卡三波11＋5＋2覆盖18／18，分波样本与旧失败保留；LIMIT5 F1=1.0／逐行recall0.5，不宣称连续n3或稳定排名。

2026-10-13：[Q2双卡五项独立补测与两项阻塞](diagnostics/native_sembench_model_dual_supplement_20261012/README.md)。
固定3bd53587，原双卡11项之外只补第二轮J与第三轮D／N／J／I，五个预热＋五个测量；实际1200逻辑／3600三跳POST、209.108秒，独立1800秒窗口。第三轮S因新Ray父目录38字节超过24字节预检退出1，R未运行。原时钟及账本保持；493组身份复查无存活、CPU违规0、GPU空。原LIMIT5 F1=1.0／完整recall0.5并列，两个窗口不合成连续n3或排名。

2026-10-12：[原Q2真实模型单／双卡有限观察](diagnostics/native_sembench_model_20261012/README.md)。
固定3bd53587，原sf2000／120完整出现／14原gold正例；质量6槽、单卡18槽、双卡11槽共7680模型／23040POST。
64个完成查询的决策／质量一致，官方LIMIT5 F1=1.0；完整FN7及召回0.5保留，不能称完整过滤满分。
单卡n3／请求n360，双卡J n1／其余n2，所有原值、错误、服务计数／token／GPU采样与资源归零保持。
旧质量父决定等待超时及接续CLI bootstrap JSON异常各退出1，双卡剩7槽未运行；源码、原首次85分钟计时和额度未重置。
质量480成员／960解码、性能2815成员／9120解码载荷隐私0，两stdlib重核无模型／网络；二进制SDK wire私有原件保留和公开摘要分开。

2026-10-12：[同步IMLane原Q2公开入口补充](diagnostics/native_release_actual_software_20261012/README.md#imlane-sync-public-q2-software)。
固定3bd53587，两作者模式各原Q2完整120请求、CLI退出0，240逻辑／480传输POST，49.861/300秒，模型/GPU0。
原2000行关系、SDK/parser/evaluator、真实worker/计数身份、31进程清理及错误替身质量均保存；1001源码未改。
232成员公开观测、逐成员双摘要、720解码payload隐私扫描及离线核查通过；旧桥preflight失败与导出补齐记录保留，未重跑窗口。
同步六路径原Q2最低软件公开入口已有证据；真实Q2模型质量／单卡完成及双卡部分见本页开头，完整Q1/Q3／可选async CLI及未完成模型项分别保留。


2026-10-12：[IMLane原生async接续](../../code/integrations/sema/README.md#imlane薄客户端)。
发布者d004e720包含已实际aiohttp3.14.5核验的共享cookie修补；五个async文件与旧a54e1456一致。
旧[失败原件与脱敏观测](postgresql/native_executor_baselines_20261012/README.md#imlane-native-async-http)保留；
修后固定77cc1ed0的[两作者模式及完整Q1](postgresql/native_executor_baselines_20261012/README.md#imlane-native-async-followup)均通过。
新生命周期347/384 POST、53.543/600秒、2GET；原Q1各2000调用/一个DBEnd批次，共4000/4096 POST、90.019/900秒，模型/GPU0。
本机67项无跳过；公共配置/容量/工厂/实际delegate及完整payload/wire、响应、稳定循环有实库证据；最终143组进程身份/10实例清理及库摘要通过。
完整CLI/观测代理/官方质量评价、Q2/Q3和性能/RSS未运行；旧失败、未知责任及独立预算记录保持。
发布者[本地组合与有限整合](postgresql/native_executor_baselines_20261012/README.md#imlane-async-publisher-integration)95独立用例通过/0跳过，初7项沙箱bind错误及仅7项重验另记；同步默认保持，原第三窗口CLI原件不改。

2026-10-12：[固定发布版实库软件核查](diagnostics/native_release_actual_software_20261012/README.md)。
4566f1fc的共享SDK解释、真实LOTUS＋Daft／Ray Data及原生图分项保存，首轮整体未通过；有效源码、POST、失败与清理见同题记录。
公开CLI只读来源路径修补与aiohttp原始cookie header委派另以固定源码复核，未运行模型／GPU、未改变IMLane默认，也不借旧容量资格为async候选背书。
固定6629846a另授第二窗口3项通过／0跳过，104.662/600秒；fixture101＋唯一代理72＝173/256POST，监督计数器仅覆盖前者。
实库3.14.5重定向与两loop、Daft／Ray Data发送／关闭时间戳、两观察模式×三主路径×原Q2/Q3的12个software_fixture单元通过，原官方评价保留误报／漏报。
四次公开CLI原2000行数据／运行时预检通过、完整CLI执行未跑；源码1003摘要核对、68个登记进程身份无残留，模型/GPU0。准确范围及两份原始档案见同题报告。
后续第三窗口固定d004e720经实际公开--execute完成四路径原Q2/Q3八查询及原LOTUS performance Q2配对；1080逻辑调用＝1080上游fixture，另1080网关POST，总2160/2560，359.252/900秒含首次预检与准备间隔。
原2000行关系与每查询120行保持，实际原parser/evaluator与四路径库运行；Q2 FP54/FN5、Q3 FP52/FN8和绝对误差44均留存，只支持软件差分。
45登记身份、269归档成员与1080收据分别核对，模型/GPU0，早停未知责任保持；真实模型[同任务单双卡计划](../plans/语义系统对照.md#sembench-native-model-next)实际质量／完整单卡与部分双卡见本页开头，async不作为其他baseline的启动条件。

2026-10-12：[LOTUS原生观测准备核查](diagnostics/lotus_observation_modes_20261012/README.md)。
固定发布版1e969356确认原请求池前观测逐条预期构造，替换臂观察实际准备值；显式性能选项省去前者，默认资格模式保持。
交付方57项本机通过／25项跳过、16次localhost替身POST，真实模型/GPU0；源码摘要、失败及模式元数据保留。
128／2000行的JSON-only准备替身局部测量不支持实际LOTUS／模型性能结论；真实SDK、厂商图与官方评价验证另列。

2026-10-12：[IMLane原Q1完整2000行软件容量](postgresql/native_executor_baselines_20261012/README.md#imlane-2000-capacity)。
2cca502d的七文件原样接到1e969356形成7bb10980；两作者模式完整128/129/2000调用、持续供给、身份、慢消费、拒绝及错误取消/清理后下一查询通过，组合异常路径另核12POST。
服务器4559＋本机4＝4563/5000软件POST，作者测试从首次启动计时783.993/900秒；自有进程/共享内存归零，作者库未重建，模型/GPU0。
Q1完整payload与原wire摘要匹配，两个模式各一个2000行DBEnd批次；旧拒绝、监督记录缺失及两次脚本断言失败均保留。
只支持该软件容量与生命周期；发布者已贯通MovieQuery与执行器的显式行配置，并补桥已创建但无退出码的同Coordinator未知责任保护。
[发布接续](postgresql/native_executor_baselines_20261012/README.md#imlane-publisher-integration)记录105通过／14缺依赖跳过、四态合成回归及旧默认；新服务器／POST／模型／GPU0。
真实模型/质量/性能和共享Coordinator独立取消尚未验证。

2026-10-12：[原生消费关闭诊断修补](postgresql/native_executor_baselines_20261012/README.md#execution-repairs)。
5项真实消费循环／图替身回归通过，HTTP与模型0；提前关闭附加异常可见，首错及未知远端状态保持。实际厂商库复验pending。
共享SDK响应重放接入五条替换批次，11项控制及2项入口检查通过、13项SDK差分/清理缺依赖跳过；HTTP/模型0。
IMLane执行错误分类5项替身检查通过，正常wire兼容，未运行作者过程、构建或模型；不外推全部异常类型等价。
共享SDK客户端按替换批次复用，3项入口/关闭检查通过，3项实际SDK检查本机缺依赖跳过；指定CPU首次采样未满足空闲条件，未启动服务器测试。
后续指定CPU空闲后，c4f7f157真实SDK/清理20项、12次localhost POST通过；模型/GPU0、自有进程退出。
原生池6项本机检查通过，含4次真实HTTP及同一连接；Daft EOF loop仍运行的实库零POST探针通过，实际新池图资格pending。
共同容量/期限profile历史6项本机检查通过；旧IMLane128为项目桥接范围，当时2000/512未支持。
桥源码在完整输出后释放完成调用引用；97af50c5和新扩容源码2cca502d的编译分别记录，新增作者资格见上述2000行记录。
不声称32MiB聚合驻留保证或新增作者并发能力；未借编译证明2000行能力。

2026-10-12：[SemBench Movie原查询与评价准备](diagnostics/sembench_lotus_readiness_20261012/README.md)。
基线fd8c00bc只接消费提示与确定失败两项稳定性依赖，原方法与六执行接点不复制核心。
CSV ad5def7d、清单内容e753baa5固定2000行／1865唯一reviewId／135次额外重复，taken_3为120行、正向14行。
LOTUS原生／Daft／Ray Data各Q1/Q2/Q3正常，累计60次CPU8–15替身POST；进度文件、旧Ray目录及token参数键断言失败均保留，原生Q3换owner单列。
父授权追加85.6秒窗口后，软件16 pass／2 unsupported／0 notrun：SemLoom三条和两种IMLane各Q2/Q3正常，原Q1两个适配器发送前拒绝；模型/GPU/PG0。
累计128POST：正常104、作者完整HTTP/超时/取消16、原生授权错误8；进程/共享内存归零。保留远端HTTP状态unavailable、底层容量归属待独立追溯；C4/C6及旧C8来源分别登记，不排名。
发布净变更已在原生分支组合检查；首四块独立发布后，追加后两块与两项HTTP需求复审修正。
最终本机86项通过、14项缺LOTUS／OpenAI／LiteLLM依赖跳过，跳过未计为通过；新服务器／HTTP／模型／GPU运行0。
[发布组合记录](postgresql/native_executor_baselines_20261012/README.md#native-release-composition)保留来源与待验证项，不增加服务器或模型资格。

2026-10-12：[原生执行接续整合](postgresql/native_executor_baselines_20261012/README.md)。
保留LOTUS／DuckDB原交付及作者IMLane630行／132次Arrow调用，公共三方法IMLane两个模式成功轮48软件POST；前轮21／11／14失败及归档符号链接拒绝保留，累计94、模型0。
同driver Daft重设runner反例与中立修复通过54次软件调用；较早源码单GPU只完成三条warmup／24真实POST，32名额占用、测量0、服务已退出。
公共关联保持原方法，Sema HTTP序号不作源行；固定native／stability组合247项无失败／29项跳过，真实公共入口追加32软件POST通过，累计126。
新独立单GPU轮`lotus-six-model01/attempt02`六条LOTUS路径18查询／144模型POST全部通过，三种计数一致、0错误／超时／重试及自有资源退出核对通过。
完整18查询／144请求原值可从同一报告离线复算；人工8行、每条测量n=2／16、固定路径次序，只支持真实模型接线，P99为样本最大值。
本轮192上限占用144、剩48未用；旧轮24实际／32占用独立保留，累计168实际／176占用。首次连接相关启动失败0POST／0分配及其退出记录保留。
双GPU公共固定路由软件资格累计62POST／64上限、模型0；随后唯一模型轮六路径18查询／144真实POST通过，内层／路由／SQLite均144、两份vLLM日志各72。
两TP1服务实际GPU UUID／engine PID已核，每GPU测试区间241利用样本、全部原值与慢样本保留；API退出0，自有存活／GPU进程0。新288上限剩144未用，累计312真实／320占用。
SDK响应解释、逐call连接与容量／关闭诊断差异保留并由独立owner修补，旧源码不支持公平排名、稳定P99或饱和结论；DuckDB／Sema模型未完成。


2026-10-12：[Sema后置Daft、Ray Data和IMLane检查](postgresql/native_adapter_integration_20261009/README.md#sema-native-executors)。
实际原生库及完整响应服务检查通过；三条作者SQL各8行，共24次确定性HTTP，真实模型0。
IMLane采用作者Python执行器与实测原生任务线程数，未证明原生Ray配置；容量、CPU、首次图创建与池后身份分别保留。
正常输入结束与取消分开，最终响应后继续核对EOF和清理；准备失败、原始日志和源码摘要均保留在仓库外。
LOTUS/DuckDB旧接入的36项受控检查通过，13项实际LOTUS库未启用，不替代新编译扩展或模型验证。

2026-10-12：[执行稳定性与多副本组合](diagnostics/semloom_stability_composition_20261012/README.md)。
固定runtime／副本／入口修补及必要祖先，本机270项中249通过／21跳过；同execution已知失败归还与未知RPC／HTTP的C/W及37字节对象留存并置通过。
来源与跳过原因保留，入口定点独立复审9项通过；修后实库、真实设备、模型及PG资格未增加。

2026-10-12：[Sema入口复审修补](postgresql/native_adapter_integration_20261009/README.md#sema-intake-review-fix)。
原`e960fc91`存在未知native停止证明与合成错误writer遗漏；修补后本机7项针对性／120项受影响检查通过，失败原件保留。历史126仍为118入口请求＋8替身上游POST，本轮未新增远端运行。

2026-10-12：[Sema入口正文存储与期限](postgresql/native_adapter_integration_20261009/README.md#sema-intake-storage)。
起点`2fad623e`，三份运行代码与`9c642ec7`一致；本机113项不同检查通过，服务器最终21项／53次合成请求通过，所有slice累计126次。
MiB同步断言失败和自动CPU资格报告均保留；确认本地输入责任的正常归还及未确认图消费的留存，模型、SQL、vendor runtime和性能运行均0。

2026-10-12：[Sema原请求池前接入可行性](../../code/src/execution_provider/adapters/sema_request_service.md#prepool-feasibility)。
公开main、分支/标签/发行清单、原ZIP/ELF及旧135项设置重新核对，未找到当前产物可小改使用的池前入口；元数据摘要进入原整合verification，历史证据保持。
仅保留原生整体与后置服务成对比较，不实施算子重写或二进制注入；新二进制/查询/模型运行均0，未接触目标服务器。

2026-10-12：[SQuAD同一调参输入的次序描述](diagnostics/squad_native_length_20261011/README.md#candidate-order-description)。
`squad-tuning-candidate-order-20261012`从原`099b994c`画像重建原seed调参512项，按两个256项源序块稳定升序描述，保留元数据与复算程序摘要。
仅支持输入块内存在次序差异，不支持实际候选、服务时间或吞吐结论；原选样条件与停止记录未改写，模型/查询/运行代码修改均0。
后续以完整查询完成为主目标，保留自然参照和同信息对照；90/10比值2不作为通用研究条件。

2026-10-11：[SQuAD原生请求长度检查](diagnostics/squad_native_length_20261011/README.md)。
`099b994c`在目标环境通过只读preflight并计数10,570条完整LOTUS消息；seed20261011自然调参512行的90/10比值1.84127，未达到预先要求的2，因此未执行模型比较。
模型/查询/运行代码修改均0；评价切片2.02924不用于改选输入。全部元数据及14成员原件已独立下载核验，离线复算由同一报告定位。

2026-10-11：[Sema接入收尾](postgresql/native_adapter_integration_20261009/README.md#sema-closeout)补同一转发期限和交错归档自动选择。
两项旧反例已复现并修正，104项相关检查通过；省略参数复算与此前30查询观察完全一致，未追加模型或改写历史结果。

2026-10-11：[Sema同轮匹配与独立输入](postgresql/native_adapter_integration_20261009/README.md#sema-matched-evaluation)。
`78c45679`在原电影分组评价集完成两组三路径交错比较，30查询／12,336次调用、十二组成对完整请求和清理通过。
单等待者修订保留，SemLoom SQL中位数仍比同组薄转发长约3.3%～3.7%；新增Core提交／终态区间由留存探针复算，未改默认或宣称算法收益。

2026-10-11：[Sema同前端三层诊断](postgresql/native_adapter_integration_20261009/README.md#sema-layer-comparison)。
`a342ea5d`完成薄转发／本地Core／Daft及Arrow两个Ray点，20查询／8,224次调用和九组完整请求核对通过。
本地Core更慢、Ray接近薄转发；全部控制CPU、扫描计数、分位样本及客户端差异保留，未宣称纯Core代价或新默认。
源于测试字段／准备清单／解释器的失败均在0模型调用单独保留，成功组及旧账本不重写；原件已独立备份逐成员核验。

2026-10-11：[Sema接纳重试核对](postgresql/native_adapter_integration_20261009/README.md#sema-admission-retry)。
`d10e3e1c`单等待者减少重复校验与容量扫描；12个HTTP／Core模拟单位、目标71项回归及两后端实际连续查询通过。
16查询／6,176次模型调用和六组测量请求核对通过，完整SQL中位数下降13.1%／15.0%；全部分位样本、归档及有限解释保留，不声称原生Sema比较已重做。

2026-10-11：[接纳、传输与完成推进核对](postgresql/native_adapter_integration_20261009/README.md#execution-cost-repair)
分别保留17配置／68查询的分项模型观察和八配置／32查询的原适配对照；CPU、模拟HTTP、模型与失败原件分开登记。
元数据重复分配及满额收取后等待已有结果已修订；Sema通知等待候选变慢而恢复原流程，短Map仍有接入成本，不宣布通用加速。
延后记录首错修订另通过两后端／8查询／3,088次正常模型核对；与上一来源请求集合相同，故障注入和实际模型验证分别保存。


[当前版本三系统短对照](postgresql/native_adapter_integration_20261009/README.md#latest-three-supplier-short)在`fd319309`完成九路径／63查询／27,720次真实调用，
模型计数一致且无重试；45个整查询测量、23,040个HTTP样本与30组完整调用值对拍通过，原件均已独立下载。
五个整查询样本的P99仅是最大值，Sema原生／接入HTTP峰值256／64及缺少上游就绪探针分别保留；不作跨系统执行器排名。

[附件补充问题修订](postgresql/native_adapter_integration_20261009/README.md#adapter-control-repairs)记录LOTUS准备停止、首错退出、有限准备与观察共用，
以及公共静态检查复用和DuckDB跨vector查询Job；227项共同本地检查通过、45项在本机跳过，目标247项共同回归及18次模拟查询通过。
CPU序列化减少与旧模型性能分别保留，不把实现修订当调度算法贡献。

[HTTP空闲连接复现与修订](postgresql/native_adapter_integration_20261009/README.md#adapter-http-pool-repair)：
原网关第二次POST可复现502／`ServerDisconnectedError`；查询间更换池、查询内复用、连接事件及不重试检查通过。
18项网关和17项常驻入口通过，3项实际库待指定依赖；后续原机器8查询／3,592次模型调用通过，原追加失败不改记账。
[分层成本诊断](postgresql/native_adapter_integration_20261009/README.md#adapter-cost-diagnosis)在`986e61e5`完成10配置／60查询／25,680次真实调用，
21组正文摘要集合一致；三次查询原值、阶段和质量见紧凑观测。Sema组级复用完整查询下降约30%，C64仍慢于简单转发；
DuckDB真实接纳成本与独立CPU重复校验profile分开解释；`08523a20`的可选准备块及Sema C128另通过12查询／5,136次调用。
两次诊断共27组正文摘要核对，12配置／72查询／30,816次成功调用；可选改善仍未超过原生，默认保持。
[接入工程检查](postgresql/native_adapter_integration_20261009/README.md#adapter-engineering-audit)核对原生接点、物理选择和重复工作；
Sema局部修订及Arrow显式配置的89项本地检查通过；目标机器四配置／12查询／576次模拟HTTP调用通过，退出无残留，真实模型原值不据此改写。

2026-10-10：[阶段复核与容量筛查继续执行](postgresql/native_adapter_integration_20261009/README.md#capacity-screening-live)
来源`00623f6e`的三条实库替身确认准备、传输与返回事件；两配置12查询／5,136次模型调用完成后，编排进度写入报错停止。
原件保留，继续运行只覆盖剩余配置并沿用原结束时间；当前为启动观察，尚未登记完整校准结论。
后续逐组核对确认17配置／102查询全部完成，模型成功计数43,656一致，51条测量查询保留；有限选点仍待交错复验。
`native-adapter-tail-20261010-06`开始交错复验，成功后以固定配置执行独立评价；当前只登记实跑观察，评价结果仍待完成。
随后[首次停止](postgresql/native_adapter_integration_20261009/tail-workflow-stop-20261010.json)归属观测网关上游断连，服务成功11,816次、另1次尝试没有成功回执；
已通过28查询、其中13次测量，完整原件保留，200次整查询评价尚未启动。
[初步分析](postgresql/native_adapter_integration_20261009/README.md#capacity-screening-analysis)同步全部51个原值、直接提交计时、HTTP／worker分位与token核对；
LOTUS C64有下降信号，DuckDB增加，Sema准备和发送规模差异分别解释，尚无通用性能结论。

2026-10-10：[适配分析保存精简](postgresql/native_adapter_integration_20261009/README.md#保存与复核)
四份分析改为完整字节的单一gzip副本，[保存登记](postgresql/native_adapter_integration_20261009/verification.json)保留原提交、路径、字节数与SHA-256，原始观测、运行身份及复算入口继续保留。

2026-10-10：[容量筛查的首次停止](postgresql/native_adapter_integration_20261009/README.md#capacity-screening-stop)
保留8次真实调用、零完成查询及运行包标签错误；修订参考表示与允许值的3查询／24次fixture POST另计，修正版没有启动真实模型。

2026-10-10：[原生方法容量准备](postgresql/native_adapter_integration_20261009/README.md#capacity-preparation)
按实际源码分别保存H128／C与Sema线程、Arrow/Ray字节计数、原生库请求与退出检查、独立输入及环境记录。
最终源码43查询／19,160次fixture POST与早期来源分别保存；CPU fixture与BF16小张量不作为模型质量或性能结论，模型筛查已准备但未启动。

2026-10-09：[同题持续成本修复](postgresql/native_adapter_integration_20261009/README.md#adapter-performance-repair)。
公共空释放、DuckDB重复封装和Sema重复通知分别复现与修订；独立字段、原生完整响应、同步记账和统计保持。
整合24查询／1,632次fixture POST的618成员投影由[离线复算](postgresql/native_adapter_integration_20261009/observation_replay.py)核对相同请求、原响应字节、函数墙钟／线程CPU和Q0／H0／H1／B1／Q1。
十个初始故障回归旧版2失败／6错误，修复后13项通过；真实模型0，重启中断及原探针上限失败分别保留，旧模型负结果不改数字。
三家异常退出修复由`adapter-repair-source02`／`aef350b2`共同核对，1,028份文件一致；173项回归170通过／3项实际入口通用suite未启用，编译扩展20项全通过。
八路径26正常查询／1,154 fixture POST、输入／URL更新、完整返回及资源退出由[487成员复算](postgresql/native_adapter_integration_20261009/adapter_repair_final_replay.py)核对，真实模型0；三家失败原件与旧来源均保留。
[修复来源模型记录](postgresql/native_adapter_integration_20261009/README.md#repair-model)在`aef350b2`完成120查询／7,680唯一响应，1,890成员复算质量、请求、所有五次测量、时钟及386资源采样；Sema服务入口分位与实际128／4分别登记，不能据gateway时间作完整请求尾部结论。
后续DuckDB内层与摘要源`2b53857d`只作CPU针对性检查，250成员保留28＋4项回归及两个来源12正常查询／576 fixture POST；两份启动器失败原件保留，既有90查询和CPU诊断不改写。

2026-10-09：[同题报告的DuckDB／Sema常驻检查](postgresql/native_adapter_integration_20261009/README.md#persistent-supplier-fixture)。
`resident-supplier-source01`在`3a5f7bc9`上只新增测试，五条各`8 → 8 → 128`行及两次Sema更换URL探针，17查询／722 fixture POST、模型0。
340份公开成员与[离线复核](postgresql/native_adapter_integration_20261009/supplier_resident_replay.py)核对当前输入、实际请求、结果、native owner、调用ID和原失败。
显式CPU-only预检通过但当前GPU不可访问；Sema后置Core／worker按查询创建，性能接入问题仍由后续具体修复与回归判断。

2026-10-09：[同一适配主报告的常驻fixture](postgresql/native_adapter_integration_20261009/README.md#persistent-fixture)。
`resident-source02`以`2a7ab18a`及13份逐文件摘要定位，固定Map与LOTUS六条各连续3次，18查询／96测试POST、真实模型0。
旧DuckDB两入口共8次测试POST通过，原加载失败实际POST0；93项Linux检查92通过，1项独立库检查另在六臂运行。
350份紧凑公开成员及[复算入口](postgresql/native_adapter_integration_20261009/persistent_replay.py)核对身份、时间、计数和原失败；冷样本与Sema线程拒绝保留，不据此改默认或作模型性能判断。
同一[常驻模型章节](postgresql/native_adapter_integration_20261009/README.md#persistent-model)独立保留六臂90查询／5,760唯一响应、source02与15次组件／actor复用。
完整owner884.689804秒，两次零请求准备失败、原截止与账本保持；协议用量522,669输入／17,280生成／0缓存，计数与退出一致。
2956份公开投影和[离线复算](postgresql/native_adapter_integration_20261009/resident_replay.py)支持全部重复、结果与中位数；按臂分块、观察等待不同和低扰动／内部CPU缺项不作性能采用结论。

2026-10-09：[跨系统方法与执行适配工程核对](postgresql/native_adapter_integration_20261009/README.md)。
起点`cbfd952b`，公共、LOTUS、DuckDB和Sema交付分别引入，原Daft／Ray图和旧SQL入口身份保持。
本地39项与私有PG18.3的3项回归通过；完整响应、发生序号、实际POST及清理分别核对，供应商10条实际库入口与主执行入口均通过。
模型预演13路径各3次，共39查询／384次POST，初次8＋后续376，预留392和原512上限保持；服务／进程退出通过。
调用起止27条复算，其他12条不可观测；Sema后置供给和不可观测时刻、SDK／HTTP表示区别、失败与来源由主报告维护，不更新性能结论。

2026-10-09：[统一比较的新增就绪入口](postgresql/query_ready_timing_20261008/README.md#unified-entry-preparation)。
基于`a0bf5cf3`及独立源码摘要，PG-source direct、原生Ray Data与Daft Native完成本地提交／EOF及兼容回归；
327项相关检查中315通过、12项缺Daft／Arrow依赖跳过。服务器连接在认证前关闭，新的实际库接入、PG与模型比较尚未运行；不更新既有性能排名。

2026-10-08：[数据就绪后的SQL／原生API与统一HTTP计时](postgresql/query_ready_timing_20261008/README.md)独立保留八路径真实预演。
准备零调用、原生职责、行关联、账本和模型增量核对通过；每路径10条测量查询仅作有限样本观察，PG为本地HTTP内部参照。
整合保留原始响应与首错修订，273项本地检查、26个原路径恢复和逐字节分析复算通过；真实运行对应原源码快照，模型0次新增。

2026-10-08：[同应用原生语义系统的真实比较](postgresql/semantic_system_comparison_20261008/README.md)。
基于main `1cdd94f8`及明确摘要的适配，在确认五系统各512行、原始Q3两路径模拟可运行后，
用户授权单张RTX4090、缓存Qwen2.5-7B、最多14,408次／45分钟及首次错误停止。
新清单62条查询、账本／POST／服务成功14,408，五系统Map各预热1次＋测量3次，PG／LOTUS原始Q3及三控制工程检查完成。
原生提示、解析、实际并发与模型工作量分别保留；当前PG为本地HTTP，既有Daft／Ray主实现排名保持。
Sema修订JSON编码和Daft8192临时文件额度通过512行评价，LOTUS本地元数据三点重做；
原32次非法输出停止、8,009次文件额度停止及0请求准备错误分别保留，不混入有效聚合。
168项文本baseline及4项观察器通过，43项旧原生控制中1项base已有文档断言失败保留。
62条结果摘要、外部Map提示集合及Sema关联／Q3 COUNT独立核对，PG／模型／GPU／端口／ACL清理通过；首次独立绑定探针错误单列。
2,179份新私有原件、11,288,072字节独立备份逐成员核对，公开逐行、协议、事件、全部重复及复算入口由主报告定位。
供给复算为`inconclusive_supply`，三控制各8次仅验证工程；方法性能、多Job、图像与Filter成本校准继续待完成。

同日尾延迟离线补充：独立备份18个单元／7,704行时刻恢复与摘要核对通过，新增模型0。
[同一报告](postgresql/semantic_system_comparison_20261008/README.md#三种同后端控制及两类尾延迟)分别记录单行交付、HTTP观察和完整查询的最近秩P99／P99.99样本值。
五系统各1,536行／3查询，三控制各8行／1查询；高分位落在样本最大值，总体P99.99保持unavailable。
用户已批准新823,424次／6小时扩展清单；三控制共1,536次512行模拟响应、只读环境和身份核对通过。
该清单在235.253分钟、请求数FIFO第137轮出现HTTP读取错误后停止，五系统各137轮、三控制各136轮；
完整查询的563,840次模型成功、564,150条请求记录及564,149次服务成功分别保存，未完成各200轮。
[故障核对](postgresql/semantic_system_comparison_20261008/README.md#tail-transport-failure)保留原始异常、时序与无模型复现；
本机空闲关闭与复用竞争5次复现原始异常链，4秒回收、移除暂停及恒定暂停下4秒回收均通过，新增模型0。
原始连接关闭与复用的精确触发原因仍待验证，生产配置及旧运行结果保持。
[完整应用与尾延迟](postgresql/semantic_system_comparison_20261008/README.md#tail-comparison)已离线核验1,109条完整查询；三控制136轮配对的完整耗时未支持token表征或工作量限制收益，仍需区分冷准备和统一就绪后的执行。
[主实现返回诊断](postgresql/semantic_system_comparison_20261008/README.md#main-return-observation)完成20次／16,448行受控actor处理，模型0；线程登记减少局部返回等待但完整消费更慢，默认保持。
分支审查的失败响应保留修订通过170项文本baseline与6项观察器检查，旧服务器源码和实验身份保持。
展开记录已按原路径归档，144份公开文件恢复与六组离线复算通过；获取入口统一见[保存说明](postgresql/semantic_system_comparison_20261008/README.md#保存与核验)。

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

## 2. 机制完成度与当前结论

本节为截至2026-09-02的早期机制快照，保留当时的“当前不存在／尚未实现”及失败判断。
后续 accepted-prefix、增量 adapter、LOTUS／IMLane 接入与图像资格从上方日期条目、原报告及[代码状态](../../code/INFRA_STATUS.md)核对，
不把本表当作整个当前集成版本的完成清单。

| 机制 | 代码与测试入口 | 真实结果 | 当前证据与结论 |
|---|---|---|---|
| 校准前独立资格小切片 | `6c111b24`；[逐响应数据、普通 SQL EXPLAIN 与旧/新 builder 对照](postgresql/semfilter_qualification_20260901/README.md) | PG18.3 `-Werror`、regression 1/1、TAP 437/437、Python 59/59（其中 calibration 9/9）；同模型同 prompt 的 baseline/choice 各 30 次测量和各 1 次预热 | 两项完成、一项未通过：builder 拒绝完全/近似共线；MCV/dependencies 将普通谓词 estimate 8 修正为 64、actual=64；choice 格式由 27/30 提高至 30/30，但两配置都仅符合 12/27 预设语义判断。没有访问 held-out payload，未拟合真实数据，choice 未接入生产 SQL/spec/wire，整轮校准继续暂停。 |
| exact SemFilter 真实 reference calibration 首轮采集 | 生产源码 `dcde2be5`，采集合同 `2feab7d4` / `7042132e`；[观测、脚本与 SHA 清单](postgresql/semfilter_reference_calibration_20260901/README.md) | PostgreSQL 18.3 + Qwen2.5-1.5B-Instruct + vLLM 0.25.1 + 单 RTX 4090；64 条预热完成，首个 training 查询第 23 个响应为非法 3-byte 内容，SQLSTATE `22000`。完整 training/held-out observation 均为 0，55/55 既有合同与 6/6 采集检查通过 | 失败采集：保留全部 87 条响应的脱敏观测、错误和事后身份/readback 核验；未拟合、未运行 held-out、未发布或加载 artifact。20% 阈值和原有语义未修改；当前固定模型在该 workload 下尚不能稳定遵守输出格式。不能将其作为真实成本、质量或四项系数可辨识性验证。 |
| PostgreSQL planner-visible `SemMap` / exact `SemFilter` | `code/postgres/semloom_pg/`、`code/src/execution_provider/`、`code/src/planning/semfilter_reference_calibration.py`；20 项 PostgreSQL static contract、8 项冻结 v2 protocol、9 项 v3 semantic protocol、8 项 fixed-model adapter、5 项 gateway migration 和 5 项 calibration contract，另有 PGXS SQL regression 与 TAP lifecycle/fault tests | 双 RTX 4090 服务器隔离源码构建 `REL_18_3`（upstream `62d6c7d3…`）；历史 4A/4A.1、4B、4B.1、cost-observability 与 gap-hardening 结果继续绑定各自提交。`dcde2be5` 通过 clean warning-free `-Werror`、regression 1/1、TAP 437/437、Python/static/gateway 55/55 和 neutral/machine C11 compile；`postgresql_semfilter_reference_calibration_dcde2be5_20260901` 保存源码/提交身份、原始日志、actual/expected、二进制、状态和已校验 manifest | 功能测试：recording Map/Filter compatibility 与三参数 exact Filter 保持 ordinary child、三值/NULL/cardinality、`LIMIT` 前 placement、prepared/generic plan、RLS/权限、snapshot/savepoint、cancel/recovery、多 backend 与 direct `INSERT ... SELECT`。fixed adapter 的既有 HTTP/DNS 边界保持。planner 能严格加载与 semantic/physical/model/role/provider 匹配的静态 calibration artifact，并在损坏、escaped NUL、duplicate、缺失或失配时保留 uncalibrated reference；EXPLAIN 分列 artifact/workload/service identity、predicted service milliseconds 和 held-out error。当前 artifact 是 deterministic fixture，真实 matched cost、第二 path、rescan/EPQ/parallel、join/aggregate 和更宽 query shape 仍未支持。 |
| PostgreSQL-private plan/runtime + neutral provider seam | `code/postgres/semloom_pg/src/{sem_plan_spec,sem_filter_calibration,sem_filter_cost,semantic_filter_contract,recording_contract,pg_semantic_runtime,sem_pump,sem_operator_machine,sem_map_machine,sem_filter_machine,ai_provider_port,provider,recording_provider,uds_provider,wire_common,wire_v2,wire_v3}.{h,c}`；`code/src/execution_provider/{wire,adapters,server.py}`；旧 `gateway/` compatibility wrappers 与上述 contract/TAP | 历史 resource、4A/4B、4B.1 与 cost-observability 证据继续绑定原提交；`dcde2be5` 只追加 planner calibration qualification，不替换 runtime/resource 或真实模型 evidence | schema v1/v2、pump binding、shared runtime 与 provider seam 保持；cost metadata schema v2 和 calibration loader 只位于 planner，runtime 仍只计数实际 model calls 和 prompt/output usage。`wire_common.c`、v2/v3 和 gateway golden/fixed-model 职责不变。endpoint/model/timeout/auth 只来自仓库外配置；artifact workload/service signature 是离线适用性身份，不是 backend 对 endpoint/hardware 的在线探测。真实 matched artifact、accepted-prefix、多在途/乱序 completion 与增量 SemLoom adapter 尚未实现。 |
| extension/core semantic carrier | 条件性 `code/postgres/pg18_core_patch/`；当前不存在 | 无 | extension 已证明当前 exact plan identity、prepared/generic plan 与 lifecycle；LOTUS/Cortex alternatives 和第二路径的 carrier 反例审查尚未完成，不能据此声称 core 永远不需要修改。 |
| IMLane batch-placement profile / Kalypso reference direction | gateway/SemLoom 后续实现；当前不存在 | 无 | database-batch placement 在数据库资格后验证；lineage/prefix lease 与 KV-aware execution 仅作参考，未进入当前排期。 |
| `SemFilter` 关系语义 | `sem_filter_path.c`、`sem_filter_calibration.c`、`sem_filter_cost.c`、`sem_filter_machine.c`、公共 runtime、wire v2/v3 与 PGXS/TAP | 历史 recording、golden/resource、真实模型 capability、HTTP boundary 与 cost-observability 结果继续保留；`dcde2be5` 追加 TAP 437/437 planner calibration qualification；无新性能/质量结果 | 一参 recording 兼容不变；三参 path 严格解析 uppercase TRUE/FALSE/UNKNOWN，SQL NULL 不建 task，并已验证 duplicate identity、`LIMIT` 前 keep/drop、Unicode/空串、savepoint、invalid raw output/error frame、cancel/recovery、EXPLAIN plan identity/estimate/actual usage 和 fixed endpoint。静态 artifact 匹配时校准 reference cardinality/work/service cost，拒绝时回退 uncalibrated reference。真实模型单次结果和 deterministic artifact 分别只证明执行链路与 calibration control flow；真实 cost accuracy、第二 proxy/oracle path、近似质量 policy 与异步乱序 completion 尚未实现。 |
| LOTUS v1.2.4 compatibility/native baseline | companion extension profile、`code/tests/compatibility/lotus_v124/` fixtures 与独立 native runner；当前不存在 | 无 | 只保留为后置兼容和完整系统 baseline，messages/output/error parity 尚未实现且不阻塞核心。 |
| 固定行 batching | `code/src/scheduling/organization/batching.py`、profiler 与 baseline tests | `local_vllm_qwen15b_baseline/` | 真实 GPU baseline；用于和计算量感知组织方式比较。 |
| Sequential token-budget | `code/src/scheduling/organization/{batching,token_budget}.py`、对应 tests | baseline、joint、BFD 与 row-cap 系列；`rc1_data_organization/`（07-31 系统重测） | 已重复验证；当前数据组织默认，必须同时满足 token budget 和 row cap。**07-31 干净平台**：2-ep/4-ep 均属第一梯队（保 prefix 局部性，ratio 0.13），4-ep KV 饱和 regime 下**最稳**（50k，命中 0.47–0.48，SLO 17%）。 |
| Length-align | batching 实现与测试 | local baseline 早期消融；`rc1_data_organization/`（07-31 系统重测，2-ep+4-ep） | 早期仅初筛。**07-31 干净平台**：2-ep（无 KV 压力）length_align 已是最慢（50.3k vs fixed 56.3k，命中最低 0.60——按长度排序破坏 prefix 局部性）；4-ep（KV 饱和）进一步崩到 39.4k、命中 0.06。**cache-ON 下 length-align 的 HOL 收益被丢掉的 cache 复用抵消**，regime-dependent 不稳定收益。 |
| Prefix-aware | batching 实现与语义测试 | `prefix_aware_batching_20260726/`、`prefix_cache_data_org_20260730/`、`prefix_cache_routing_req_20260730/`、`prefix_routing_agent_20260730/`、`prefix_routing_concentrated_20260730/`、`prefix_cache_routing_4ep_1.5b_20260731/`、`rc1_prefix_routing/kv_budget_sweep_20260731/` | cache-off 0/30/70/100% 无稳定收益；cache-on 2-ep/7B batching/routing 跨数据集吞吐均中性（\|Δ\|<2%）。4-ep/1.5B routing +5.9% 跨门禁，但 matched-KV 隔离显示 2-ep/1.5B 在 gpu_mem_util 0.3–0.9 均中性（−0.1%～+1.0%），因此当前证据更支持 **endpoint consolidation** 是信号显现的驱动因素，而不是单纯 per-endpoint KV 大小或“cache 压力开关”；4-ep 饱和深度仍是残余混淆。agent-trace 的 P50/SLO 信号属于不同 workload，不能合并成统一 cache-pressure 结论。文本 prefix 遗留实验已 parked。 |
| BFD、output-aware、row-cap-first | batching 与 cost-mode tests | output-aware BFD、row-cap-aware packing 全系列；`rc1_data_organization/`（07-31 系统重测） | 512 行有局部信号，1024 行未泛化且 SLO 明显恶化；不采用为默认，只保留可复用设计点。**07-31 干净平台**：best_fit 在 2-ep 命中最高（0.76）；row_cap 命中 0.68（第 4/5）。两者在 4-ep KV 饱和时**一起崩最惨**（命中 0.07、SLO 60%、TTFT ~1.1s）——重排序装箱打散 prefix 组（ratio 0.03）。regime-dependent：仅适合无压力 regime。 |
| Arrival replay 与 request lifecycle | lifecycle、runner 和 trace tests | `request_lifecycle_gate_20260725/` 及 flush 系列 | exactly-once、request→submission、arrival/flush/complete 时间链已闭环。 |
| Per-endpoint active-work admission | active-work credit、least-work/least-queued routing 与 profiler tests | `dual_gpu_active_work_saturation_20260729/` | 双 4090 八档、每档三次 formal。65K 达到最大吞吐 97.80%，下一档仅 +0.92%；按预注册规则选为最小饱和点。98K→131K 吞吐持平且 P99/SLO 更差。 |
| Static credit workload-existence audit | `summarize_static_credit_workload_surface.py` 与等价臂门禁模板 | `static_credit_prompt_length_screen_20260730/` | Short/long 48/48 成功，但 urllib/no-token-ID、非 factorial、short 无准入压力等价臂分裂 48.5%，且均值/中位数选择冲突；证据等级为真实 GPU screening，动态 GO/NO-GO=`inconclusive`。 |
| Request-level continuous replenishment | request 粒度 replay、逐请求 credit release、Ray adapter 与 trace tests | `dual_gpu_request_replay_20260728/` | 双 4090 三次重复已跑通。等名义工作量的 request K48 与 batch K16 吞吐不可分辨；K64 吞吐最高但 offered work 高约 33%，且 P99 更差。机制可用，但独立性能增量尚未证明。 |
| Complete-row service quantum | `slice_service_quanta`、offline/replay expansion、completion/credit trace tests | `dual_gpu_service_quantum_20260729/` | 固定 65K work 的三次重复完成；512/request 将 credit-held 降约 16%，但四档 quantum 吞吐相对 batch 仅 -0.03%～+0.54%，request +1.75%。固定 quantum 不晋升，request 保留作精确控制基础。 |
| Static K_max | admission 与 profiler tests | local baseline 干扰实验、joint search | shared-vLLM 下 `K_max=8` 有必要性证据；当前静态安全基线。 |
| Immediate/fixed/adaptive flush | scheduler、flush policy 与 trace tests | accelerated/window/randomized/cross-rate/2048/joint/shared-vLLM | Adaptive 稳定优于 fixed-25，但未优于 fixed-50；shared-vLLM 下约 89.4% 决策选择 50ms，也没有稳定增量。当前默认 fixed 50ms。 |
| SLO-aware EWMA flush | `SloAwareEwmaFlush`、feedback/provider、arrival-gap completion 与 trace tests | `dual_gpu_slo_ewma_flush_formal_20260729/` | 双 4090 高压/arrival-limited 各三次 formal；相对 fixed-50 吞吐 -0.52%/+0.10%，P99 -0.94%/-0.49%，30s SLO 全部零违约。未过 5% 晋升门槛，fixed-50 保持默认。 |
| AIMD、EWMA、PID admission | `code/src/scheduling/submission_control/{adaptive,pid}.py` 及测试 | `adaptive_admission_controller_20260726/`、`shared_vllm_adaptive_admission_20260726/`、`hol_age_diagnostic_512_20260728/` | 单作业与 shared-vLLM 双作业重复均完成。AIMD 在共享服务中 0 次 decrease、窗口均值 15.953；相对 static K16 前台与吞吐均略差，当前没有动态反馈增量证据。hol_age 诊断进一步确认：换 HOL-age 信号 + request-level replenish 仍未击败 static K16（SLO-goodput 反而 −56%/−70%/−81%），锁定刻画型 framing。 |
| UCB 多臂老虎机 | `code/src/scheduling/submission_control/ucb.py`、对应 scheduling tests | 无端到端结果 | 有有限 K_max action set、探索/利用和 SLO reward 的纯控制器测试；尚未接入 profiler。必须先封闭 epoch 内请求完成与 reward 归因，避免把跨 epoch completion 记到错误 arm。 |
| SAOR capacity-only development | `scheduling/submission_control/saor.py`、`scheduling/runtime/saor_capacity.py`、shared-vLLM adapter 与 SAOR tests | `saor_capacity_development_20260811/` | 纯策略、非因果 paired replay 与真实 capacity actuation 已接。四臂各一次：SAOR 相对 K128 +4.36%，但相对 K160 +0.52%、相对 threshold −1.46%，Jain 最低；未过晋级门。完整 ordered release/fairness/stage queues 未执行，不能称完整 SAOR 或定理已验证。 |
| SAOR fixed-envelope active-set release | named shared-credit runtime、direct/project 统一 runner、matched solo、lifecycle/mechanism fail-closed 汇总 | `saor_active_set_release_formal_20260812_69affc7e/` | 40/40、0 incident、exactly-once；resolution-aware v2 在服务器完整 artifact 上重汇总为 passed，credit mechanism effective 12/12；原 failed 文件保留审计。SAOR 12,393 tok/s、fg P99 50.3s，为 credit 臂 fg 最好；static fg P99 29.2s/SLO 0%，故 SAOR 未晋级。 |
| Foreground strict-priority release-only upper bound | shared-credit `strict_priority`、Job lifecycle close、priority evidence、三臂 readiness/summary | `saor_priority_reachability_smoke_20260812_91ffcaa/` | 两轮 rehearsal-only：11,791 tok/s、fg JCT/P99 20.04/14.27s、SLO 0%，相对 SAOR fg P99 −73.02%。证明 non-preemptive release ordering 可达，不是 formal/proposed；hard priority 仍缺 anti-starvation 和 bulk lag/SLO guard。 |
| SAOR bounded-priority development | `saor_bounded_priority` selector、actual-work debt/reclaim barrier、lossless event ledger、双轮 fail-closed 汇总 | `state_aware_work_unit/saor_bounded_priority_gate_20260813/` | 双轮四臂 GPU gate 无 cap 晋级：0.25K 第 2 轮 debt-recovery=0；两个 cap fg P99 49–56s、SLO violation 85%–95%。event ledger 证明可见前台 head 都获 priority，但 per-Job 同步 pull 只暴露单 head，完整 ready backlog 不可见；先修 observation contract，不启动 formal/4-Job/reservation。 |
| SAOR bounded-ready development | `saor_bounded_ready` 有界 concrete-request 预注册、跨 submission/request trace lifecycle join、actor-side register/grant event ledger | `state_aware_work_unit/saor_bounded_ready_gate_20260813/` | 两个独立 rehearsal root、8/8 cell、0 incident；0.125K 两轮全过 correctness/mechanism/foreground/bulk/efficiency 门，均值 12,361 tok/s、fg P99 17.87s、fg SLO 0%、bulk SLO 0.662，注册 formal candidate。0.25K 两轮 bulk SLO 0.752/0.744 越界，拒绝。尚无 formal repeats、4-Job、reservation 或理论公平结论。 |
| SAOR matched-ready selector attribution | completion-accounted registered-ready lag、bounded-ready FIFO/DRR/VTC-style/strict-priority/guarded-debt 同 observation 双轮汇总 | `state_aware_work_unit/saor_matched_ready_selector_rehearsal_20260813/` | 12/12 cell、0 incident、12,288 requests exactly-once。DRR/VTC-style 约 12.90K tok/s、fg P99 27.23/26.16s、fg SLO 0；SAOR 12.28K tok/s、fg P99 17.85s、lag P95 57.7K work。相对 VTC-style，SAOR 用约 4.8% 吞吐、5.2% bulk JCT 和 22.7% longest-no-service 代价换取 31.8% fg P99 与 11.7% lag 改善。它是 development 中的非支配折中点，不是已授权 formal 的 selector winner；native baseline count=0。 |
| SAOR ready-observation bridge | frozen-static/single-head shared FIFO/bounded-ready FIFO 三臂配置、readiness 与双轮 raw-effect 汇总 | `state_aware_work_unit/saor_ready_observation_bridge_rehearsal_20260813/` | 6/6 cell、0 incident、6,144 requests exactly-once。共享容量双轮均值 tok/s +25.96%、fg P99 +99.17%；bounded-ready 在同 FIFO 下 tok/s +7.30%、fg P99 −33.62%，但 fg SLO violation 仍约 39.7%。证明 static isolation、shared efficiency 与 ready exposure 是不同效应；不授权 formal，native baseline count=0。 |
| SAOR 五臂共同观测系统对照 | 五臂 runner、T0--T4、严格透传 observation-only gateway、archive/identity/trace validator | `state_aware_work_unit/saor_native_system_matched_gateway_rehearsal_20260821/` | `93271012` 的 5/5 rehearsal cell、exactly-once 与独立 archive validation 通过；五臂 request P99/SLO/Jain/lag/no-service 已可比较。单次 SAOR 相对同 executor static 提高吞吐、降低 group JCT，但 P99、Jain、lag 和最长无服务变差，只支持效率—尾延迟—公平权衡。formal 未运行，0s/5s isolation 只有 partial。 |
| Actor pool 分池与 endpoint routing | `code/src/scheduling/runtime/ray_adapter.py`、profiler/trace 与契约测试 | `dual_gpu_actor_pool_shape_20260729/` | 固定 65K work、256 slots 和 0.5 CPU/endpoint 的三次重复已完成；2×128/4×64 相对 1×256 仅 +2.00%/+0.75%，未达 5% 晋升门槛。当前同构单 job 保留 1×256；多 job 分池仍待验证。 |
| Shared-vLLM endpoint credit 与 DRR | `code/src/experiments/shared_vllm/`、named shared-credit runtime 与测试 | `dual_gpu_shared_vllm_formal_20260729_1135/` | 双 4090 36/36 group run、0 incident；全局 256 request/65,536 work 安全与归零通过。2-job 无增量；4-job 聚合吞吐 +9.57%、max P99 -22.52%，但逐 repeat 不稳定，暂作高竞争条件性候选。 |
| Batching × submission 联合搜索 | scenario runner 与汇总工具 | `joint_batching_submission_512_20260726/` | 18 单元筛选和候选重复完成；当前单 GPU 下联合候选未显著优于独立拼接。 |
| vLLM CUDA Graph | 服务配置与相同 profiler 路径 | `vllm_cuda_graph_512_20260726/` | 重复真实对照显著优于 eager；作为本地部署 baseline，不作为上游调度研究贡献。 |
| 算子代价估计 | `code/src/planning/costs/`、`code/scripts/analysis/estimate_operator_cost.py`、context-LOO driver 及测试 | `operator_cost_profile_dual4090_formal_v2_cache_on_20260807/` 等 | cache-on 有效集含 429 formal、20 context × 4 candidate；Hybrid pooled/macro/max regret 1.67%/2.90%/14.72%，candidate pairwise 0.808。max regret 仅以 0.28 pp 通过 15% 门，属于 marginal pass，仍需跨时间/workload/硬件校准。 |
| 多模态 cost adapter / image path | 中性 `work_units` + lazy Daft image source + typed CLIP tensor actor、staged descriptor、真实 static HSE broker 与 observe-only snapshot 已实现；两个 CLIP profiler；`code/scripts/data/import_coco_images.py` | `motivation/results/gpu/image_clip_bottleneck_profile_20260801.md`、`image_clip_preprocess_variants_20260801/`、`experiments/results/image_ai_embed_operator_formal_20260803/`、`opening_image_native_fourjob_formal_20260810/`、`opening_image_project_fourjob_observe_only_formal_20260810/` | 静态 operator-E2E、matched-resource、原生多 Job 干扰和 Project observe-only 证据已完成；尚未完成 HSE static GPU 非劣、stage/CE5 在线动作、小规模 pgvector 检索质量闭环和跨 workload/硬件验证。vLLM pooling 0.25.1 能力门仍 blocked，不生成性能排名。 |
| 图像原生四 Job 干扰 | Daft built-in/Ray Data vendor-owned single→four-job runner、immutable 1+3 manifest 与 fail-closed 汇总 | `opening_image_native_fourjob_formal_20260810/` | 40/40 runs、30 formal group 通过；两条路径均出现非均匀 Job slowdown。只作各路径内部 normalized 干扰和状态证据，不作跨框架绝对排名。 |
| 图像 staged descriptor / observe-only snapshot | production staged descriptor、freshness/signature 检查与 Project control trace | `opening_image_project_fourjob_observe_only_formal_20260810/` | 24/24 group、99K formal rows exactly-once；3,114 个 snapshot 全 fresh、构建均值 0.141 ms。static/proposed group JCT 差 0.98%，只证明观测接入，不证明 state-aware 动作收益。 |
| 多 endpoint/多 GPU 调度 | endpoint/pool 配置与 routing contract | request replay、active-work saturation、Actor Pool、Shared-vLLM formal 与 5s 两 Job 干扰实验 | 真实双 4090 容量、admission、worker identity、equal-weight 1/2/4-job 和 5s staggered 两 Job 证据已建立；尚不能声称 weighted/held-out、多 Job 路由增量或故障迁移有效。 |
| 两 Job guaranteed-overlap 干扰 | online exact-short、Project eager full/half/static/shared、原生两 Job runner与统一逐请求时间汇总器 | `opening_multijob_interference_20260809/` | 5s offset下项目因果链与三条原生观察已闭合。online下shared提aggregate但伤short/Jain；eager下quota-only+59.00%、matched static/shared竞争+58.77%/+28.90%，shared相对static short JCT−48.94%、aggregate+31.85%。证明arrival-regime dependence与idle borrowing，不证明动态普遍胜出。 |
| 文本四 Job quota/competition/shared 因果分解 | Project full/quarter single + static/shared multi；Daft Native/Ray、Ray Data 各自 single→four-job | `opening_fourjob_interference_20260809/` | 按三次 formal 均值，shared 相对 static 使 group throughput +8.68%、四个 Job JCT 全部改善，是效率/JCT 子向量的 baseline-relative empirical Pareto；但 raw-work Jain 0.960→0.923，long1/2 的 shared/quarter-solo=1.29/1.14，未达到经验性保留份额非劣。compact CSV 可算三反事实，不能补 event-level service lag/starvation；不称完整 Pareto 或 DRF/Themis/VTC 理论性质。 |
| Project short all-at-t0 计时诊断 | 同 short manifest、冻结 K128/W65K/8×32 actor 的 Project 1+3 与 T0–T4 raw 汇总 | `opening_project_short_all_at_t0_diagnostic_20260809/` | T3 model-request window 11.354s vs Daft Native11.059s，service tokens/s与MFU只差约−2.5%；排除“Project模型请求路径慢6.4×”。Daft缺外层T0，完整系统E2E不排名；short不要求60s。 |
| 文本原生框架入口 | official/native adapter、单 job matrix 与 native multi-job runner tests | `opening_text_native_gate_20260808/` | bounded、Daft Native/Ray、Ray Data 的 6/6 capability/calibration gate 通过；只证明入口、provenance、exactly-once 和最小 Ray Data 筛选可用，n=1 不作性能排名。 |
| 文本原生单 job 正式观察 | bounded C128、Daft Native/Ray、Ray Data official graph + vLLM/GPU time series | `opening_text_native_single_job_formal_20260808/` | 16/16 cells、12 formal，吞吐/JCT CV<0.7%。Daft waiting mean=783/742、KV max≈1；Ray Data running=17.3、MFU=0.112；证明压力形态不同，不证明项目方法胜出。 |
| ShareGPT bounded 饱和校准 | bounded HTTP C32/C64/C128/C256 + vLLM/GPU time series | `opening_bounded_saturation_calibration_20260808/` | 8/8 cells 通过；formal C128 达 C256 已测峰值 98.22%，冻结为正式对照。C32 仅 52.07%，高 GPU util 不等于喂饱；C256 明显过量排队。 |
| Ray task/actor 与 vLLM capacity 调优 | 执行接口、参数字段和实验设计 | CUDA Graph、双 GPU request replay、active-work、Actor Pool 与 service quantum | 已固定 vLLM 8192 batched-token/256 seq capacity，并标定上游 65K work 饱和点；增加 actor 或固定 quantum 均未过 5% 门槛。 |

## 3. 主要结果目录（非穷举）

本节和同目录 `README.md` 都是高频结果的非穷举导航；未列出的结果直接查看目录及其 README。
机制是否实现、运行和可声明以最新来源条目、[代码状态](../../code/INFRA_STATUS.md)及对应原始结果为准，避免两个目录清单因新增结果产生
无声分叉。

| 结果目录 | 角色 | 当前状态或结论 |
|---|---|---|
| `postgresql/choice_pg_plan_20260902/` | 四 C 的 PG plan 接入 | 最终 `134447dd`：PG18.3 regression 1/1、TAP 537/537、Python 68/68；新配置仅可规划/EXPLAIN，不执行、不回落 v3，不恢复质量或成本校准。 |
| `postgresql/choice_profile_contract_20260902/` | 四 C 的第一个值合同实现 | C/Python 114-byte encoding 和拒绝测试通过；68/68 本地/服务器合同与 PG18.3 仅构建，未安装/启动 PG、未调用模型。SQL/plan/wire 接入 pending。 |
| `postgresql/semfilter_prompt_qualification_20260901/` | 实际 messages/template 核对、唯一 prompt 与条件性 7B 对照 | 两个完整尝试的消息与 token IDs 一致，全部格式合法，但旧/新样例未全对；321 次 completion 包含 83 次中止记录。无生产变更，校准仍暂停。 |
| `postgresql/semfilter_qualification_20260901/` | 三项独立前置验证 | 秩检查修复、PG18.3 普通多列统计通过；choice 仅通过格式，语义预期仍不符，reference 资格未通过。 |
| `postgresql/semfilter_reference_calibration_20260901/` | exact SemFilter 首轮真实 reference calibration，失败采集 | 64 条预热完成；首个 training 查询第 23 个响应违反严格 tristate，PG18.3 报 `22000`。held-out、拟合、artifact 均未运行；保留失败，不改变阈值或静默排除样本。 |
| `saor_capacity_development_20260811/` | SAOR控制microbenchmark、paired trace replay与2×4090四臂capacity-only开发门：两档SAOR、frozen lower/upper及legacy threshold。 | 4/4 arm、5,266 requests/arm、0 incident，动作安全；SAOR相对K128 +4.36%、K160 +0.52%、简单threshold −1.46%。一次顺序运行，server dirty-worktree provenance及旧manifest漏字段与后续修补分别保留；原轮仅属development／not-promoted，不作formal排名。 |
| `saor_active_set_release_formal_20260812_69affc7e/` | fixed-envelope 2-Job：六active-set臂＋四matched-solo，1+3 formal。 | 40/40、0 incident、exactly-once；原failed validation保留，resolution-aware v2完整validation通过、credit mechanism effective 12/12。SAOR在credit臂内fg最好，但static显著更强，状态formal-valid／not-promoted。 |
| `saor_priority_reachability_smoke_20260812_91ffcaa/` | static／SAOR／foreground strict-priority三臂，两轮rehearsal-only release上界短测。 | 两轮lifecycle／mechanism通过；strict-priority 11,791 tok/s、fg JCT/P99 20.04/14.27s、fg SLO 0%，fg P99相对SAOR −73.02%。formal repeats=0，仅证明release-only可达性，不作正式排名或proposed／winner结论。 |
| `state_aware_work_unit/saor_bounded_priority_gate_20260813/` | static／SAOR／$0.125W_e$／$0.25W_e$两轮bounded-priority开发门。 | Round 1 clean；Round 2的0.25K（$0.25W_e$）debt-recovery=0，机制门fail-closed，汇总diagnostic_only。全部请求正确完成且GPU饱和，负结果不是欠供给；两cap fg P99 49–56s、SLO violation 85%–95%，均未晋级。ready backlog未完整暴露给coordinator，formal／4-Job／reservation未运行。 |
| `state_aware_work_unit/saor_bounded_ready_gate_20260813/` | 同selector改用项目自有有界concrete-ready预注册，两轮四臂development gate；原生baseline不使用该机制。 | 8/8 cell、0 incident，两轮clean；仅$0.125W_e$（0.125K）全过并冻结，均值12,361 tok/s、fg P99 17.87s、fg SLO 0%、bulk 30s miss 0.662。0.25K（$0.25W_e$）因两轮bulk SLO越界拒绝。汇总formal_registration_candidate，仍待project-only matched-observation归因；不是正式排名，formal／4-Job／reservation未运行。 |
| `opening_text_native_gate_20260808/` | 开题文本原生框架：bounded、Daft Native/Ray、Ray Data的256-row capability gate及Ray Data C4/C8/C16最小筛选。 | 6/6 gate正确性与provenance通过；冻结单次measured peak C8/B16供正式矩阵。n=1的256-row gate不支持框架正式性能排名。 |
| `opening_text_native_single_job_formal_20260808/` | 开题原生单Job观察：同一2,048-row ShareGPT manifest的bounded C128、Daft Native/Ray、Ray Data，1+3 formal，含MFU、服务压力及能耗。 | 16/16 cells、12 formal通过，状态指纹稳定；四臂依次17,800／17,286／16,747／3,551 tok/s。Daft两臂waiting/KV高，Ray Data该冻结路径running/MFU低；仅报告官方graph／冻结点外部现象，不作内部因果归因或项目优势结论。 |
| `opening_multijob_interference_20260809/` | short/long guaranteed-overlap的online exact-short、Project eager full/half/static/shared、single/multi及三条原生5s overlap观察，含逐请求阶段／状态分解。 | online quota-only≈0，shared提高aggregate但伤short/Jain；eager quota-only +59.00%、matched static竞争+58.77%、shared竞争+28.90%，shared相对static short JCT −48.94%。long同时影响service与上游pending；Project／原生仅作各轨normalized影响，支持arrival-regime dependence与idle borrowing动机，不证明动态普遍胜出。完整raw留服务器，Git保存紧凑派生数据。 |
| `opening_image_native_fourjob_formal_20260810/` | 同一2K short＋3×3K long、0.5s offset：Daft built-in／Ray Data single→four-job，1+3正式观察。 | 40/40 runs、30 formal group及exactly-once／overlap检查通过；仅比较各系统内部Job slowdown，不作跨框架绝对排名或Project／state-aware胜出结论。 |
| `opening_image_project_fourjob_observe_only_formal_20260810/` | 同一图像manifest的Project single/static/proposed-role staged descriptor＋observe-only snapshot，1+3正式门禁。 | 24/24 group、99K formal rows exactly-once；3,114个snapshot 100% fresh，构建均值0.141 ms；static/proposed group JCT差0.98%，近似中性。仅证明观测接入，不证明动态收益。 |
| `opening_project_short_all_at_t0_diagnostic_20260809/` | 同一short manifest的Project eager single／all-at-t0 1+3（三次formal），统一T0–T4 timer并对齐Daft raw T3/T4。 | Project T0=14.957s，Daft T0未采集，完整系统E2E不排名；可比短诊断T3为11.354s／11.059s，Project service tokens/s与MFU约−2.5%。不扩为60s容量排名。 |
| `opening_bounded_saturation_calibration_20260808/` | ShareGPT bounded HTTP C32/C64/C128/C256容量校准，含MFU、服务状态与服务器原始归档SHA。 | 四档formal tok/s依次9,455／14,058／17,834／18,158；C128达C256的98.22%，是首个满足原预定97%选择条件的点，C256已过量排队。单次formal仅用于确定bounded对照并纠正旧C32欠供给口径，不作统计或框架性能排名。 |
| `opening_database_e2e_text_refeed_20260808/` | 开题统一PG source/sink的database-E2E文本三静态臂replacement：SQuAD均匀＋ShareGPT异质，含MFU、能耗及服务状态。 | 24/24单元、18 formal的完成性、质量、source/sink、identity与稳定性核对通过。SQuAD的Project计时额外含指标采集、记录写入及结束处理，不按不到1%的差异排名；ShareGPT C32后续证实欠供给，旧1.546比值不排名，DuckDB有4,921/6,144行cap语义失败，不支持方法性能排名。旧_20260807/仅保留为failed-feeding诊断。 |
| `operator_cost_profile_dual4090_formal_v2_cache_on_20260807/` | cache-on 双 4090 代价估计正式结果 | 429 formal、20 context × 4 candidate；Hybrid pooled/macro/max regret 1.67%/2.90%/14.72%，pairwise 0.808；最坏 regret 为边界通过。 |
| `hol_age_diagnostic_512_20260728/` | HOL-age 诊断实验实际运行（6 臂 × 3 formal，24/24 ok） | **负向**：aimd_hol/replenish/aimd_hol_replenish SLO-goodput（6.78/4.62/2.91）远低于 static_k16（15.27），P99 恶化 4–13×。「诊断优先」假设被否定——补 HOL-age 信号 + request-level replenish 后动态稳态仍不优于最佳静态。 |
| `hol_age_diagnostic_512_20260727/` | HOL-age 诊断预注册设计 + 配置（设置 A） | 07-27 本机无 GPU 未运行；实际执行在 `_20260728/`。含预注册判据与 6 臂设计。 |
| `oceanbase_b1_gate_20260731/` | OceanBase B1 baseline 门禁 #1 验证 + 部署阻塞 | 门禁通过（CE 4.5.0 含 `AI_COMPLETE`/`DBMS_AI_SERVICE`，静态确证）；当前 AutoDL 容器 observer init step 4/18 clog errcode -9100 自杀（seccomp 等，容器内不可修）。降为待部署，复跑需特权容器/VM。 |
| `prefix_cache_routing_req_20260730/` | cache-on prefix-affinity routing 消融（request 粒度，3 臂） | 12/12 ok；纯 routing -0.1%、length-align +1.9%（均 <5% 门禁）。2-ep/7B 下 prefix 方向收口。 |
| `prefix_cache_routing_4ep_1.5b_20260731/` | cache-on prefix-affinity routing 消融（4×Qwen2.5-1.5B，request 粒度，2 臂；含 4-endpoint 调整与 stale-Ray 事故记录） | 8/8 ok、0 incident；prefix_affinity 相对 least_queued **+5.9%**（46,943 vs 44,317 tok/s，raw 不重叠、CV≤0.9%）、SLO −6.3pp、P95 −3.15s，**跨过 5% 门禁**。⚠️ 混淆（1.5B×4-ep×更小 KV）、过饱和 regime（SLO 违约 25–31%），需隔离消融后正式晋级。 |
| `rc1_prefix_routing/kv_budget_sweep_20260731/` | KV-budget × prefix_affinity 隔离扫描（2-ep/1.5B，1 endpoint/GPU，扫 gpu_mem_util 0.3–0.9；新存储约定 raw/+README） | 2-ep 全 KV 范围 prefix_affinity **中性**（Δ ∈ [−0.1%, +1.0%]，含 13–15% SLO 抖动点）；matched-KV（~7GB）：2-ep/0.45 −0.1% vs 4-ep/0.43 +5.9% → **endpoint 数（consolidation）是驱动，非 per-endpoint KV 大小**。跨引擎共享池价值定位在多 endpoint consolidation，不在小 KV。 |
| `rc1_data_organization/` | RC1 数据组织系统重测（5 策略 × {2-ep/0.9, 4-ep/0.43}, 1.5B, multiturn, cache-ON, P0 指标 prefix_hit/TTFT；新存储约定 raw/+README；**取代 07-25/26 gropy；07-18/19 保留作历史动机参照**） | 20+20 formal、CV 1–6%、GPU 79–90%。**regime-dependent 闭合**：2-ep（KV max 7–10% 无压力）5 策略 E2E 50–56k 紧凑、fixed≈seq>bestfit>rowcap>lenalign；4-ep（KV max **98–100% 饱和**）分化 39–50k、**排名反转为 seq>fixed>>rowcap≈bestfit>lenalign**。机制 smoking gun `prefix_group_ratio`：重排序类 organizer（length_align/best_fit/row_cap）打散 prefix 组（0.03）→ 4-ep 命中从 0.60–0.76 **崩到 0.06–0.07** → prefill 重算激增 → TTFT 翻倍（0.2–0.3s→0.6–1.1s）、best_fit/row_cap SLO 60%；保序类 fixed/sequential ratio 0.13–0.29、命中 0.47–0.48。consolidation 是惩罚（4-ep −10～−26% + 能耗 +40%）。**与 #28 routing / KV-sweep 闭环**：上游策略价值在 4-ep 饱和 regime 才显现。✅ feeding 门禁已补（batched bounded，gate 放宽 ≥2 endpoint）：2-ep 真上限 **79,488** tok/s（策略 63–71%，缺口=active-work 准入节流 W65536 把 inflight 压到 4–22 vs K256，**非饿死**；`model_wall≈operator_wall` 无 pipeline 瓶颈）；4-ep bounded **24,733 病态**（unthrottled thrash 小 KV，策略 160–202% 超过）→ **准入控制是吞吐杠杆、效应随 regime 反向**（2-ep 压住上限可放开 W、4-ep 防 thrash 应保留）。 |
| `prefix_cache_data_org_20260730/` | cache-on prefix-aware batching 消融（batch 粒度，3 臂）+ routing 报告交叉引用 | 12/12 ok；上游 batching 顺序 within 1.2% 中性。vLLM APC 覆盖上游 prefix 组织。 |
| `prefix_routing_agent_20260730/` | cache-on prefix-affinity routing 跨数据集消融（agent-trace，2-ep/7B，3 臂）+ 跨数据集合并分析 | 12/12 ok、0 incident；吞吐三臂 \|Δ\|<2% 中性，但 pala P50 −7.8%/SLO −3.8pp/goodput +17%（高 cache 压力，过饱和区间，吞吐 −1.9% 未过门禁）。 |
| `prefix_routing_concentrated_20260730/` | cache-on prefix-affinity routing 跨数据集消融（concentrated ShareGPT，2-ep/7B，3 臂） | 12/12 ok、0 incident；吞吐 \|Δ\|<1.2% 中性，pala 信号弱（cache 压力低于 agent）。自包含简表 + 指向 agent 报告的合并分析。 |
| `static_credit_prompt_length_screen_20260730/` | Short/long prompt下request K／active-work存在性筛选，含独立median/CV及等价臂审计。 | 48/48成功，long W65K有稳定正信号；short无压力等价臂分裂48.5%且高方差，叠加urllib/no-token-ID与非factorial设计，正式判决阻塞、dynamic GO/NO-GO inconclusive。保留机制审计身份，原后续为async等价臂gate复验。 |
| `dual_gpu_shared_vllm_formal_20260729_1135/` | 1/2/4-job independent-full、static-partition与endpoint-shared DRR对照。 | 36/36、0 incident，全局request/work精确上限、容量安全与公平门槛通过。2-job无收益；4-job aggregate throughput +9.57%、max P99 −22.52%，但逐repeat异质，需held-out复验，仅作高竞争候选，不作通用默认。 |
| `dual_gpu_slo_ewma_flush_formal_20260729/` | high／arrival-limited replay下fixed-50、queue-25/50与SLO-EWMA-25/50对照。 | 24/24成功，exactly-once与completion-lag审计通过；SLO-EWMA相对fixed吞吐−0.52%／+0.10%，各臂30s-SLO violation均0。25–50ms控制相对5.6–17.4s P99缺少一阶杠杆，未晋级。 |
| `dual_gpu_service_quantum_20260729/` | 固定work/pool的batch、512/1024/2048/4096 complete-row quantum及request完成粒度对照。 | 24/24成功，HOL／credit barrier缩短、credit-held约降16%，但吞吐最多+1.75%，未形成超过原5%门槛的稳态吞吐或SLO收益；没有fixed quantum晋级。 |
| `dual_gpu_actor_pool_shape_20260729/` | 固定work、slots及Ray CPU的1×256／2×128／4×64 actor拓扑对照。 | 12/12成功，多actor最多+2.00%，未达原预注册5%晋升门槛；该single-job同构endpoint配置保留1×256，不外推多Job隔离或故障迁移。 |
| `dual_gpu_active_work_saturation_20260729/` | 双4090、八档request-level per-endpoint active-work饱和曲线，各cap三次formal。 | 32/32成功；65K（65,536）为原预注册最小饱和点，98K／131K不再增加吞吐且P99／SLO更差。 |
| `dual_gpu_active_work_curve_20260728/` | 双4090、request-level per-endpoint predicted-token work的早期五档容量曲线，各三次formal，用于发现原上界仍在上升。 | 当时49K仅为knee candidate、65K为最高已测边界；容量选择已由2026-07-29的dual_gpu_active_work_saturation_20260729/八档扩展取代，同签名后续采用预注册最小饱和点65,536。原结果保留诊断与复现价值。 |
| `dual_gpu_request_replay_20260728/` | 双endpoint whole-submission barrier与request-level continuous replenishment的K-count对照，真实双4090、每臂三次formal。 | global K32≈per-endpoint K16，名义work-matched request K48≈batch K16；K64吞吐最高，但offered work约多33%，未隔离该增量，不能称最优或机制胜出。 |
| `shared_vllm_adaptive_admission_20260726/` | Shared-vLLM前台／后台static K8、static K16、AIMD及adaptive-flush对照，真实单GPU三次重复，含精确request/token记账。 | K8保护前台尾延迟，K16提高后台吞吐；AIMD饱和至K16且decrease为0，没有反馈增量。adaptive flush大多选择50ms、近似fixed-50，也无稳定增量。 |
| `adaptive_admission_controller_20260726/` | 真实单GPU：64-request gate、随机512-request static K8／AIMD／EWMA-AIMD／PID矩阵，以及AIMD对static K16机制对照。 | 重复结果显示动态策略相对K8的收益来自收敛至K≈16，AIMD与同上限static K16不可分辨，未证明超越强静态；shared-service保护仍未验证。 |
| `accelerated_arrival_flush_20260725/` | 单RTX5070 accelerated-arrival下immediate／fixed-timeout／queue-adaptive flush首轮真实筛选，含run、submission、flush及resource traces。 | fixed timeout减少submissions，但吞吐差异未达到统计可分辨；queue-adaptive未形成多行batch。 |
| `adaptive_flush_cross_rate_20260726/` | 约51.4／12.85 req/s到达率下512-request fixed-25／fixed-50／adaptive筛选。 | 测试范围内fixed-50最好或等价；adaptive不足以支持增加默认复杂度。 |
| `adaptive_flush_randomized_20260726/` | Natural-EOS gate、随机顺序512-request fixed-25／fixed-50／queue-adaptive重复，含实际output-token／finish追踪。 | fixed-50与adaptive均比fixed-25约高32% tokens/s，二者不可分辨；fixed-50为该轮最简候选。 |
| `adaptive_flush_window_20260725/` | 修正后的双窗口adaptive flush gates、1024-row probe及512-row重复，含plot-ready traces。 | 单GPU accelerated replay在512行有正向候选信号；固定policy-group顺序、16-token输出上限及缺少per-request E2E tails限制结论，后续更严格实验已收窄原判断，仍保留需跟进的候选身份。 |
| `joint_batching_submission_512_20260726/` | 18-cell token-budget×K_max×flush真实筛选，以及independent splice、joint candidate、fixed-50机制控制的随机重复验证。 | 原1% SLO gate下独立拼接与联合搜索不可分辨；fixed-50是该workload的最简候选。 |
| `local_vllm_qwen15b_baseline/` | 本地AI_COMPLETE早期基线：PostgreSQL→Daft→Ray→vLLM Qwen2.5-1.5B；含fixed batch、token-budget／K_max配置、synthetic smoke、ShareGPT／BurstGPT固定行batch扫描CSV及latency probe。 | 真实链路／本地PG rehearsal及固定行batch基础证据，部分早期消融缺少现代trace完整度；不是token-aware调度结果或PostgreSQL18.3内置平台结果。 |
| `operator_cost_estimation_20260726/` | 已有profile的离线二次分析：仅用204条real formal做23-feature decision-context LOO；历史all-phase结果已归档。 | CE5 candidate pairwise 0.800，macro／pooled／max regret 4.58%／0.62%／26.23%；row pairwise 0.684未过原门槛，不晋级。粗粒度估计可用，严格SLO精度不足。 |
| `output_aware_bfd_1024_20260726/` | 1024-row held-out BFD确认，对照同cost sequential与最强practical baseline。 | 负规模结果：该BFD未泛化；原报告要求进一步联合调优row cap。 |
| `output_aware_bfd_512_20260726/` | 已替代的修前512行失败运行：sequential／BFD row cap不一致及timeout审计。 | 计划24次中完成21次后超时；约束不一致，仅保留审计身份，排除性能结论。 |
| `output_aware_bfd_512_v2_20260726/` | 修后512-row sequential／BFD×output-cost六cell比较，18 formal runs及plot-ready summaries。 | BFD trace在512行有局部正向候选信号；n=3，trace metadata不是paired output oracle。 |
| `output_aware_bfd_gate_20260726/` | 已替代的修前64-row output-aware BFD gate。 | lifecycle／resource链路证据仍可审计；sequential与BFD row cap未配平，排除算法性能比较。 |
| `output_aware_bfd_gate_v2_20260726/` | 修后64-row真实组件gate：output-cost、sequential／BFD packing及request/resource traces、power、energy、MFU。 | 约束与trace通过，所有token-budget策略共享token及row cap；仅为基础设施验证。 |
| `prefix_aware_batching_20260726/` | 0/30/70/100% shared-prefix受控workload、源码语义审计及真实vLLM筛选。 | cache关闭时prefix-only grouping无稳定收益；该轮保留sequential token-budget默认。 |
| `request_lifecycle_gate_20260725/` | 真实64-prompt PostgreSQL→Daft→Arrow→Ray→vLLM生命周期gate，含seeded runner、SLO字段及显式request→submission身份。 | 身份、计时与SLO trace闭环；每策略仅一次、fixed后adaptive，属于基础设施验证，不支持策略性能结论。 |
| `row_cap_aware_packing_1024_20260726/` | 1024-row held-out sequential／classic-BFD／row-cap-first比较，含request SLO、energy及MFU。 | 吞吐约+1%，但SLO violation由50.39%升至88.67%；默认采用被否决，保留sequential。 |
| `row_cap_aware_packing_512_20260726/` | 经prefix-cache审计的512-row row-cap／token-budget／packing筛选及三次重复确认。 | row-cap-first仅有512行小幅局部信号，不足以覆盖1024行负结果；cache-enabled探索轮只保留为invalid-ordering审计证据。 |
| `row_cap_aware_packing_gate_20260726/` | 64-row真实PostgreSQL→Daft→Ray→vLLM gate：sequential、classic BFD及BFD-inspired row-cap-first placement。 | 6/6 runs通过，约束及request/resource/MFU invariants通过；只有一次formal repeat，仅属基础设施验证，不支持性能排名。 |
| `text_heldout_2048_20260726/` | Natural-EOS 2048-request held-out fixed-50／adaptive比较，含精确request及MFU审计。 | 单次screen中fixed-50吞吐+1.75%、P99改善2.61%；持续积压仍放大尾延迟。 |
| `vllm_cuda_graph_512_20260726/` | Matched eager／CUDA-Graph 64-request gates，每臂1次warm-up＋3次formal 512-request重复，含完整prompt/output/request/resource/MFU tracing。 | CUDA Graph相对eager E2E −71.76%、observed tokens/s +254.05%，作为该轮本地steady-state baseline；属于部署调优，不是上游调度贡献。 |

台账中已登记的正式目录均有各自的 `README.md`。失败、被替代或门禁目录仍保留，是为了记录事故和排除理由，不应从台账中删除。尚未形成独立报告的原始运行目录不计入该句。

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

## 6. 尚无证据覆盖的能力（2026-08-31）

以下条目只说明证据缺口，不构成实施顺序：

1. reference/optimized 第二 path identity、quality policy/evidence 与 fallback 尚无实现资格证据；
   当前 schema v2 已通过 golden/fixed-model reference，reference AI-work cost 只是已测试的显式
   engineering heuristic，真实模型仍只有单次 capability，没有质量或性能验证。
2. extension/core carrier audit、accepted-prefix、多在途、增量 SemLoom adapters 与 IMLane-like batch
   placement 尚无相应验证；Kalypso-like lineage/prefix lease 仍只是参考方向。
3. 图像 HSE static GPU 非劣、stage/CE5 在线动作和小规模 pgvector 检索质量尚未完成。
4. 五臂共同观测 rehearsal 已完成，但 formal、matched-solo/full-solo isolation 与跨层 scheduler capability
   仍未完成或未获授权。
5. 代价估计仍需独立时间段或新 workload 校准、预测区间和在线决策增量；现有 429-run 结果只支持
   marginal feasibility。
6. 文本 Shared-vLLM held-out、weighted/SLO、UCB reward 归因、多 endpoint 故障迁移和剩余 prefix
   隔离统一为 `parked-conditional`，不阻塞当前主线。

以下保存当时的镜像摘要与存放决定，已由[当前保存规则](AGENTS.md)和本台账的恢复核验记录替代；其中的“待迁出”与“不进git”不作为当前要求。

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

与§3重复的42条目录摘要已合并到§3，各条独有解释与负结果保留；以下只保留额外目录或来源说明。

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
