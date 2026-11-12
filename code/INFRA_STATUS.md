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

### SemBench 原生接入覆盖口径

下表随本页 Git 提交版本化，固定 SemBench `c814e380`。LOTUS 方法接点使用1.2.4，
官方论文的1.1.3及默认 approximate 另记；其他系统版本、模型和参数以各冻结运行回执为准。
“已接线”表示存在完整原查询通路；“软件检查”不等于目标模型通过；目标已验只属于对应
提交／输入／环境。不能把旧来源的目标证据授予当前组合，也不据这些格子计算性能覆盖率。

| 任务能力与官方代表 | LOTUS | Ray 原生 processor | Daft 原生 AI | IMLane 作者入口 | Kalypso 作者入口 |
|---|---|---|---|---|---|
| 普通文本 Filter／Map／Extract；Movie、MMQA、Medical、Cars Q1/Q4/Q10 及 E-Commerce Q1/Q3/Q5 | 已接线，软件检查；目标按冻结来源分列 | 已接线，软件检查；目标按冻结来源分列 | 已接线，软件检查；目标按冻结来源分列 | 各场景 Filter、Movie 数字 Map 已接；Cars/E-Commerce Filter 有软件检查，真实数据/SQL待验；原版字符串输出受限 | 按原字段的文本 Filter／Map 已接；五场景 Filter、Movie/E-Commerce Map 有软件检查，新入口作者模型待验；原版结构化输出未透传 |
| 文本 exact Join；Movie Q5/Q6/Q7、E-Commerce Q7 | 显式 resident 已接线，N×M／原关系软件检查；实际选列另记，1.2.4不保证多列指令全部入模 | 原生 processor 已接，原 SDK／软件图检查；真实模型待验，旧记录图未接 | 独立原生入口已接，driver packet／原 sink 软件检查；SDK环境与模型另验，旧记录图未接 | 原版 TINYINT prediction 上的 LOTUS Join 已接；8667样本真实SQL／模型已验，完整输入另验，非作者 Join 算法复现 | 原 CP＋Filter 已接；4f12样本作者模型已验，其他来源／输入另验 |
| 文本完整排名；Movie Q9 TopK profile | 原 quick TopK显式resident已接；cb754的完整256真实模型已验，原parser回退保留；其他来源另验 | 未接线 | 未接线 | 未接线 | 作者 insertion／merge full TopK 已接；11eff的8行及原全量256发生项分别通过真实模型，其余来源另验 |
| 图像单 Filter 与两次独立 Filter；Animals Q1/Q3/Q10、Q7 | 原 SDK 已接线 | 共用一／两阶段生命周期已接线；目标范围按原来源 | 未接线 | 未接线 | 未接线 |
| 原关系连接后图像 Filter 和最终 LIMIT；Cars Q3/Q8 | 原函数／原生服务／原评分已接线，软件检查；真实数据／VLM待验 | 未接线 | 未接线 | 未接线 | 未接线 |
| 图像 Filter／Map／Extract、描述／图像 Join 和图像 self-join；E-Commerce Q2/Q4/Q6/Q8/Q9/Q13 | 原函数及原生服务已接线，软件检查；真实资产／VLM待验，Q8多列意图受SDK选列限制 | 未接线 | 未接线 | 未接线 | 未接线 |
| 图像 Join 后对真实匹配结果 Extract；MMQA Q2b | 原完整查询已接线；空路径已实机执行，非空Extract与旧原答发生项关联仍有缺口 | 未接线 | 未接线 | 未接线 | 未接线 |
| 原分层 Aggregate；MMQA Q5 | 独立原 SDK 查询已接线，真实 sf25 软件回放通过 | 未接线 | 未接线 | 未接线 | 未接线 |
| 文本 Filter 后对实际保留行作图像 Extract；E-Commerce Q12 | 原函数及原生服务完整接线；临时数据软件验证，真实数据／VLM待验 | 未接线 | 未接线 | 未接线 | 未接线 |
| 多分支 Filter／Join 及分组 TopK；E-Commerce Q10/Q11/Q14 | 原完整函数已接线，软件原生图检查；多列 Join 受当前SDK选列限制，真实数据／VLM待验 | 未接线 | 未接线 | 未接线 | 未接线 |

数据状态独立记录：Movie／MMQA／Medical／Animals 已有具名冻结输入和运行证据，具体运行仍须
重绑实际文件；E-Commerce 当前只有临时 Parquet／图像软件差分，真实资产／目标运行待核。
未设置可选真实资产环境变量导致的测试 skip 只记“本次未验证”，不能据此记“缺数据”。
“原版阻塞”限于已固定接口或失败证据；“未接线”不能改记成整个原生系统不支持。
表外题目仍按官方实现和实际接点核定，不能因为原题文件可读就算已经接入。

剩余工作按共同边界闭合：已接线原函数处理真实中间关系，额外预检之外的完整查询计时按版本
记录，再用真实资产及代表查询验收。完整 SemBench 入口及已核活动服务器绑定走共享路径。
旧 Ray Q3 专项图已退出维护，原题 Q1–Q4 差分覆盖同一 prepared processor；
原生默认重试／caller runtime 的历史身份从[准确来源](../experiments/reproduction/native_baselines/README.md#retired-ray-q3-native-binding)恢复，
没有把不同生命周期做成等价别名。Daft 专项 worker 也已移除旧 Q3 schema／执行图，
其结果、关闭失败和部分构造失败用例迁到现行 prepared 路径；独立 HTTP profile 保留。
Animals 的 Ray 零模型捕获／processor门也已按原题元数据消费一遍或两遍完整输入，移除资格工具
写死Q7两遍的限制；四题7图软件样本与受影响28项检查通过。冻结60c875随后通过Linux实际processor门
及Q1官方sf64的64次真实调用，count=3、原误差0；冷source→sink包含原生启动，其他单遍题不转授。
原生LOTUS服务观察已按SDK输入顺序返回的completion ID关联HTTP尝试，解决重复输入不同答复的
逐发生项观测缺口；本机56通过／1资产未绑定跳过，冻结40c477的Linux57项全部通过，0模型。
旧Q2b的325次Join、空Extract、F1=0及103发生项原答关联缺口保留；不回填旧日志或授予非空第二阶段GPU资格。
非空Extract的参考阶段准备已复用原查询／SDK，官方正例只作显式软件Join夹具；捕获后即停止，
不产生查询结果／质量，不代替真实Join。组件模型诊断及其独立服务观测仍待完成。
IMLane prepared VARCHAR现将原call_id传入既有模型IPC，取消Q1消息内容必须互异的接线限制；
共享审计只按occurrence与原batch位置核对。82项不同软件检查通过，原生SQL／新kernel绑定仍待验，
没有修改DSO、C++ driver、消息、生成参数或旧模型结果。
其他已就绪项继续排队；不为每个格子另写执行器，不只积累软件通过数。
已核对的普通文本题主要是单个语义调用；E-Commerce Q12 的后续 Extract 明确包含图像，
已由完整原生 LOTUS 服务入口运行，不能当纯文本或假定其他后端已能执行它。

宿主资源边界不再仅凭 Python 进程存在判定冲突：原生任务、GPU 使用及不完整清单仍拒绝，
未分类 Python 保留观测，且阻止仅凭时间窗口归属残留信号量并清理。
`933157666` 已通过目标 22 项 CPU 检查和实际零模型 stage，外层与阶段边界使用同一策略，
资源／租约释放通过；[资格与迁移范围](src/baselines/image/ray_animals_q7/README.md#evidence-and-timing)
保留旧失败、不声明 CPU 无干扰或新 Ray Q7 模型资格。仍须显式迁移活动外层 owner 的 loader，
不能只更新 stage。已冻结 helper 仍拥有租约和清理职责，不按旧文件名删除。

共享题目准备层的原生 exact Join 现在绑定各自左右表和原指令字段，预算为 L×R；
quick TopK 绑定原指令引用列，保持 K=N、原 pivot 和 N(N−1)/2 比较上界。
`4e1f8a44` 已通过目标 26 项软件／原 SDK 检查，不授予新题目或模型性能资格。
Ray Q7 `d8308aac` 从冻结源读取 N、使用 2N 预算，保持两次原生生命周期和第一阶段必要释放；
目标原关系 N=1/7/13、capture、128 项 token/pixel 及 hook 门通过，模型为 0。
[同轮原件恢复说明](src/baselines/image/ray_animals_q7/README.md#evidence-and-timing)
保留收尾复制 socket／跟随链接造成的旧 failed；独立恢复不把它改成通过。
三候选的实际组合另通过 69 项受影响本机 CPU 回归，0 跳过、0 失败、0 模型调用；
执行和测试文件保留各已验候选的准确字节，不将组合检查改记成新的 Linux／GPU 成绩。

IMLane Filter 不再用历史输入行数作为配置白名单；完整原 occurrence 数仍须等于显式冻结预算。
现有 `freeze-config` 零模型入口从原题准备生成同 schema 的 SHA、实际 occurrence 数和首次观察的
原始列顺序；正式 Filter 复用共享 prepare 消费冻结顺序，不再重复 formatter 接线。
Filter 后端按原操作形态验收，不再另列题号／字段分支或 Medical 固定 scale；上下文和回执
使用本次实际生成上限。任务层继续拥有原题与后处理，原生 TINYINT 驱动、DSO、调度和 ACK 不变。
运输字段改为源索引／模型输入 cells，完整 messages/token 与原关系单独核验；新增源码不继承
历史 Linux SQL、GPU 或性能资格。零输入仍受现有正预算/driver 合同限制，明确留待支持。
生成配置不是 GPU 运行授权；本地原源码／SDK／tokenizer 检查不转授 Linux SQL 或模型资格。
prepared Q1/Filter 的薄 driver 构建身份补齐源文件及编入的 ACK header，旧记录保留历史资格，
当前重用需重链薄 driver 补齐身份。详见[边界说明](integrations/imlane_duckdb/README.md#filter-源规模与构建身份)。
这不改变原生算法或配置声明的采样、源列顺序、DSO 和运行额度；实际 cold/ready-source 时钟仍分列，
原任务形态检查在模型前、fresh source 在 ready 后。`a5353fc25` 已完成目标零模型
Q1 SQL 25、Filter Q3a 25／Movie Q2 120 的完整 sink、ACK 和退出，以及新旧构建记录门检；
新 freeze 对同 seed 原 formatter、旧冻结配置对历史参考的全消息／token／源位置分别相等。
两份既有 DSO 未重编，未运行新模型。source 检查后的外层未知 Python 拒绝及未归因租约消失
仍保留；后续只续未运行的两项比较并清洁退出，详见同一边界说明，不将该失败抹平。

通用 Filter `016a83529` 已另行通过目标 Movie Q2 完整 120 项原生软件 SQL、Arrow/IPC、
原 sink/ACK 和外层资源清理，未重编 DSO、模型调用为 0；其余任务仍按各自精确源码证据判断。
IMLane Q9 Map `0e80e7fa` 的原版 DSO 软件 SQL 256 项和独立重复 ID 样本 6 项通过，
完整消息、关系、sink/ACK/退出已核；运行 token 为软件占位，不能转授模型 token 或性能资格。
Kalypso Q9 Map `0afa54134` 在原 `05024` 环境通过 N=256／上限 512 与样本 N=6／上限 12
的 tokenizer/planner/request 门检，未执行模型。首次缺 Git 身份的部署失败保留，修正部署后通过。
这五项限定目标门检与一项失败共 363 份原件已校验，不代表所有题或整合树的新 GPU 实测。
三项合并后的 64 项受影响 CPU 回归全部通过、0 跳过；19 个执行／测试文件逐字匹配上述
已验来源。整合仅合并说明中的资格边界，没有新增模型调用，也没有把软件 token 认作真实 token。

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

LOTUS 原视觉查询与 MMQA Q5 聚合在完整原关系返回时记录内存 sink；维护版 v3 把本地 LM／tokenizer、
源码校验／模块绑定和输入身份预检放在起点前，真实数据／图像读取、原生连续算子和完整后处理仍计入。
Animals 的额外像素身份审计在 sink 后复核，不取代原 ImageArray 解码；具名官方 sf64 身份仍固定64条，
软件 fixture 不再受64条上限。原 SDK、HTTP 软件答复与受控时钟检查覆盖两次独立 Filter、Join→Extract、
真实中间关系和聚合分层；Q5 原 sf25 资产使用软件 tokenizer／生成回放，不证明目标模型的分层请求数。
目标环境验证待完成；旧 `query_seconds` 和历史 v2 时长保持。

[Kalypso 原生客户端](src/baselines/text/frameworks/kalypso_native/README.md#内存结果边界)新增完整原关系
就绪时间，维护版公共报告从显式 `source_started_ns` 到内存 sink 计算 `source_to_sink_seconds`；
旧 caller 起点另列，不回填历史值。trace 审计、结果编码、
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

文本预检按原查询实际产生的算子检查后端能力，不再按 Movie Q5/Q6/Q7 题号判断 Join。
普通 Filter／Map／Extract、显式 LOTUS resident 的 Join／TopK、Ray 原生 processor 和独立 Daft 原生
exact Join 按各入口分列；prepared 请求入口对 TopK／Aggregate 等不支持算子在读取 stage 前明确拒绝。
这不等于任意多算子查询已经接通，
原项目的查询／系统职责及与本地执行比较的区别见[接入职责](src/baselines/README.md#sembench-原项目与本地接入职责)。
共享普通算子绑定新增 Map `suffix`、Filter／Map 的 instruction keyword 及 Extract 的
`input_cols/output_cols` keyword，保留同一准备、parser 和关系恢复；真实 SDK 的消息与结果差分
覆盖命名列、嵌套对象、重复 index 及碰撞字段。E-Commerce Q1／Q3／Q5 现补齐原 Parquet 读取、原查询
及 TOML gold SQL／官方 F1 或 adjusted Rand index 评价，继续使用相同后端接点；本地临时数据的原 SDK、
记录链与 prepared 差分已覆盖完整关系和失败边界，真实资产／目标模型未验。
Cars Q1／Q4／Q10 也已绑定原 CSV、原函数及原评分，共用同一 Filter／Extract 和后端接点；
full_data 标签只供评价，既有 sample 的尺度只选择原路径，不充当模型调用数。
软件对照保留重复投诉及原 Q4 加权查询／去重车辆金标的差异，真实 Cars 资产与目标环境待验。
IMLane Cars Q1/Q4、E-Commerce Q1 复用同一原版 TINYINT Filter、owner 和完整 sink ACK，
仅增加原场景文件绑定；原 formatter/tokenizer/Arrow kernel、重复发生项和空结果软件检查通过。
Cars scale 只选原 CSV，不等于模型行数；Extract/Map/Join 的尚未接线状态不由 Filter 扩展而改变。
Cars 新增[原CSV准备](src/baselines/README.md#cars-original-csv-preparation)：已有四表元数据可直接复用
官方分层抽样／投影和完整标签，不把图像／音频 payload 下载作为文本查询准备前提。
临时表与真实CLI已有软件检查；真实元数据、媒体和各题目标资格仍分别核定，不生成替代金标。
Cars Q3／Q8 也复用原生视觉服务，保留完整图像发生项及最后 LIMIT；原 car_id／image_id 标签筛选与
评分独立于计时，原图像路径不在执行时改写。固定 LOTUS tree 缺 Q2/Q5/Q6/Q7/Q9 查询文件，
该官方实现缺口不等于其他系统不支持；不补造音频查询来宣称覆盖。
共享原生 Join 接收多列 `:left/:right` 引用及 positional／keyword 指令，E-Commerce Q7 复用
同一 resident、原价格选择、N×N self-join、ID 拼接与 F1，没有单独执行器或候选预筛。
IMLane 也通过独立 `text_join` schema 将同一原消息和完整 pair 发生项交原版 TINYINT prediction；
复用既有 owner/SQL/ACK，由原 SDK 恢复 Join／原题 sink，原始答复和实际选列在 sink 后审计。
原版 DSO/kernel/C++ 未改。冻结 `8667d526c` 已通过[真实 Linux SQL 三门](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#imlane-boolean-join-sql)：
完整64条软件答复、8×22中间关系及 sink ACK，缺行／重复 ID 在任何 AI query／provider 请求前拒绝；
复用既有 patched 构建的 `52bf5763` DSO，0模型，资源与租约闭合。随后同一冻结来源完成64次
真实请求、完整8×22关系／空sink及逐条消息／token核对，原F1=0；单样本资格不外推全量，零候选仍是薄 driver 接线限制。
已核当前 LOTUS1.2.4 accessor 实际每侧只选一个 Series；此前多列软件检查证明与原 SDK 一致，
不证明全部声明字段进入模型。现在 `native_join_inputs` 记录真实选列／遗漏字段，保留原生行为；
完整多列指令载荷在该原生接口中不能表示，不能仅记作待跑模型。四个独立进程的hash seed检查还确认
实际所选字段会变化；截至上游`aa81339c`，Join与列解析文件仍与1.2.4逐字相同，升级这两处不能解决。
固定源码与软件观测见[原生选列限制](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#lotus-native-join-projection)。
上游1.1.3也有每侧一个Series及同类选列限制；不能把版本回退当成修复。Ray／Daft exact Join 复用
同一原 SDK 消息／parser／关系和原题，保留该限制；原消息准备及 sink 重建的适配成本实际计时，
后置消息核对／选列披露不进 sink。调用预算从真实 L×R 取得，未开放 TopK 或混合算子图。
Movie 的 exact 是显式比较 profile，官方 runner 默认 approximate，两者及未逐题核实的论文策略不混同。
四个文本场景现于任务准备阶段绑定官方函数，实际查询仍重新扫描数据；源码 Git 校验和模块加载
不再每次嵌入执行区间。原题、原参数及已有独立调用入口保持，原 SDK／prepared／resident 软件回归通过；
这不扩展单算子桥的多算子能力，也不转授新的目标模型或完整公平计时资格。

共享 Movie source 已移除三列投影，完整保留官方读取的 CSV 关系；只有算子引用字段进入模型消息。
原始11列样本的 Join 中间关系恢复22列，未引用元数据不再丢失。受影响131项软件检查为128通过、
3跳过；Daft／Ray 各64条历史真实答复经原完整 CSV 回放，消息、本地冻结 token 数组、完整中间关系、
最终 sink／评价相同，新增模型调用0。旧 `c39d96347` 模型运行仍属于三列投影来源，耗时不回填到
修正路径。冻结 `4f12b556e` 随后通过 Linux 完整源关系门及原作者 Movie Q5 样本8×8的64次
真实请求、完整22列中间关系／原 sink／评价重放和物理清理／租约释放；原件已恢复并核验。
原生8 token Filter与判定保持；实际消息摘要、CPU完整token数组已核，GPU实际token数组、usage、
价格与kernel计数未观测。资格只属于该来源／样本，不转授全量、其他后端或后续TopK组合树。

LOTUS resident／视觉／MMQA 聚合、Ray／Daft prepared、IMLane Filter 和 Kalypso Movie Filter／Map 使用
`query_clock_version=sembench.source_to_sink.v3`：共享文本输入身份校验在起点前和 sink 后执行，
完整查询真实读取及最终关系处理仍计入；旧宽时钟和 v2 不回填。LOTUS 可显式
`model_initialization=before_queries`，调用原 owner 初始化后执行 `ready_first/ready_repeat`，没有额外模型预热；
默认仍在首次查询内启动。已就绪路径记录实际就绪观测，Ray／Daft 仍是包含启动的原生单冷路径。
维护版 IMLane Filter 与 Kalypso Movie Filter／Map 也已移出额外预检：前者在起点前准备源码、
审计 token、预算及上下文检查，区间内保留执行消息、native SQL 和原完整 sink；后者前置
manifest／源码绑定，仍新鲜读取原 Reviews CSV、生成作者必需输入并执行原关系。
Kalypso 另保留 caller→sink，不改旧 receipt 或估算扣除。受影响软件检查通过，v3 目标代表查询待验；
其余专用／私有入口的旧时钟按
[时钟范围](src/baselines/README.md#完整查询时钟与仍未统一的路径)分列。
服务器冻结 `d55e9229` 的112项CPU检查通过；IMLane Q3a完成25次请求、原F1为0.9167，
5.4116秒保留为含文件校验的旧区间。Kalypso在prompt目录冲突处失败，请求0；这是运行绑定问题，
不代表原生不支持。Join的8行64对只完成CPU及真实renderer核对；E-Commerce真实数据在已查位置缺失，
多列原题和全量目标运行仍未验证。上述结果不转授本批v3或性能排名资格。

Kalypso Movie Q8/Q9/Q10 现由同一个 Map 接点绑定原题，保留作者提示、动态输出上限、
length→8192 重试、原 parser 和完整关系后处理。Q10 另绑定原 Movies 评价文件；调用数从原题选择取得，
不设固定样本量。候选 `e5ea4842f` 的既有 48 项 CPU 检查与 4000 条保存答复回放为软件证据；
交接后的隔离组合另通过 22 项零模型回归，覆盖三题的小规模原查询／评价及 CLI 交接。
冻结候选 `e5ea4842f` 已由独立服务器会话通过 Linux 作者 SDK／planner／request-object 及完整 token
检查，合计2376条，模型调用0、未初始化 CUDA。
当前分支随后修改了共享查询及 Map 字段绑定，整合后的目标环境验证仍为 pending，不转授该候选或旧 GPU 结果；
查询终点仍是完整内存关系，审计、评价、编码、持久化和关闭位于该终点之后。

同一 Kalypso Map driver 现在按原题引用字段绑定，不限定 Movie 题号／`reviewText`／数字结果；
新的 `sembench-map` spec 直接指定原 scenario/query，旧 Movie 名称仍使用它。E-Commerce Q5 的
完整多字段描述、原分类列与 ARI 已有本机原查询／SDK／作者模板软件检查；结合 Movie/owner/计时
回归共 56 项不同检查通过，0 跳过、0 模型。真实 E-Commerce 数据未齐，作者 planner／模型待验；
Extract 的结构化输出、连续算子及其他未接线能力不因此获得支持。
来源、输入表示与边界见[Map 字段绑定](src/baselines/text/frameworks/kalypso_native/README.md#按原题字段复用-map)。

Kalypso 的 `sembench-filter` 复用相同原题／字段输入和 CPU 交接，只有一个 Filter driver；
Movie、MMQA、Medical、Cars、E-Commerce 原题均由作者普通 Filter 判定，再回到原关系及评分。
原 `max_tokens=8`、不含 false 即通过、无 retry、作者调度保持，不冒用 LOTUS parser；N 个实际
occurrence 需要 N 次有限预算。五类来源九个原题、全否与 sink 后失败、本机原作者方法体和预算
边界检查通过；结合上述 Map、owner 与计时共 63 项不同软件检查通过，0 跳过、0 模型。
旧 Movie／私有入口的结果保留各自身份；新入口的作者 planner／模型仍待验。
见[Filter 字段绑定](src/baselines/text/frameworks/kalypso_native/README.md#按原题字段复用-filter)。

Kalypso `sembench-join` 把原左右表交给作者 `CartesianProduct → SemFilter`，一个 Join driver
复用现行 predicate/source/sink 生命周期；预算为全部 L×R，保留作者8 token、原判定和调度。
只补原 CP 未传递的配对 `idx` 元数据及原 `:left/:right` 指令字段，不改候选展开／准入。
原 SDK 只恢复关系，不再调用模型；Kalypso 模型输入包含全部声明字段，不借用 LOTUS SDK 的单列投影。
Movie Q5/Q6/Q7 与 E-Commerce Q7 的原结果／评分、2×3重复输入和原 planner／本地真实 tokenizer
软件门检通过，连同受影响旧路径共68项不同检查，0 skip、0真实模型。新作者环境／模型仍 pending；
这不代表任意连续算子或 E-Commerce 多分支查询已在 Kalypso 完成。
见[原生 Join 绑定](src/baselines/text/frameworks/kalypso_native/README.md#原生-join-组合与配对身份)。

Kalypso 客户端另移除与原关系规模无关的固定32MiB响应上限，完整读取后仍做原有数量／身份检查。
33MiB本地回归先复现旧失败再通过，原HTTP/JSON/sink与失败边界通过；结合上述接线共69项不同
软件检查最终通过，0真实模型。外部 owner 预算、超时与资源监督不变，旧源码结果不回填。

Kalypso 的[原生完整排名](src/baselines/text/frameworks/kalypso_native/README.md#原生完整排名-topk)
复用维护 CLI、原查询和字段交接，把完整 source 一次交作者 `SemTopK(k=N)`；保留其 insertion／merge、
5 token A/B 判定、blocking plan 与原调度，不把它命名为 LOTUS quick 算法。预算为完整比较上界，
实际调用另记；空／单行不虚构请求。原字段、重复发生项、7／8行算法切换、完整原结果与评分、
原 planner／本地 tokenizer context 门及失败路径有软件检查。冻结 `11eff9cd7` 已通过 Linux5项软件
及原作者0／1／8行CPU门（8行56个有向比较完整token），模型调用0、清理通过，原件已核。
同一冻结来源又完成原CSV样本8行的16次真实比较（上界28），原排名／分数／评价重放、实际消息及
generate API输入token数组关联、清理／租约均通过；质量按原结果保留，不代表全256行或其他后端。
另以原2000×11 CSV实际选出的256发生项独立通过65280方向CPU门和1508次实际原生比较；
重复review ID保留，完整原rank-to-score／评价、输入token关联和清理通过。
全量资格及原生质量见[验收报告](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md)，不转授其他系统或组合树。
实际output token数组、价格和纯GPU kernel计数仍未观测；API usage不当作GPU实际工作量。
连同受影响 Map／Filter／Join、owner、HTTP／时钟及原题共114项不同本机检查为111通过、
3跳过（独立Daft SDK及未设置的MMQA／Medical可选资产变量），0真实模型。

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

[Ray Animals Filter](src/baselines/image/ray_animals_q7/README.md)现让 Q1/Q3/Q10 复用同一单遍 worker，
Q7 复用两遍 worker；原题及原评价决定结果，不新写题目执行器。N=1/7/13 的原消息、parser、
完整结果／评价、单遍错误收尾与旧 Q7 生命周期已有 CPU 检查；单遍的目标 SDK／GPU 待验。
Q7 仍保留第一 stage 的完整关闭，最终原 City
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
