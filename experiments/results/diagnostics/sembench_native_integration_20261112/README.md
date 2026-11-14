# SemBench 原生 baseline 接入的分路径验收

本报告维护 `integration/baseline-general-20261112` 后续接入的验收身份，保留原 SemBench
`c814e3807e72d4cf876b852b17e77f3cc94575c2` 的查询与评价。它不是全套 baseline 完成报告，
也不把不同原生算法、冷暖条件或后端的单次样本排成性能榜。
当前实现范围见[能力表](../../../../code/INFRA_STATUS.md#current-refactor-status)，
执行与计时规则见[原生接线说明](../../../../code/src/baselines/README.md#sembench-native-query-timing)。

## 完整 Movie source 与 Kalypso Join

源码 `4f12b556e3911497bb15fd21047aa70f71265a5d`、code tree
`b6ba58486d675d7457c42e4494fc168201ae6472` 移除共享 Movie 读取的三列投影，保留原 CSV 全关系；
只有原算子引用的列进入模型消息。11列样本恢复22列 Join 关系。131项受影响软件检查为128通过、3跳过；
跳过项不能当作通过。Daft／Ray 各64条历史实际答复的完整源回放保持消息、冻结输入 token、
完整关系和最终 sink／评分，新增模型调用0；旧三列来源的模型耗时不回填。

固定 Kalypso 作者 `05024f027f932c0e398852e38302be30b9015481`，运行
`baseline-4f12-movie-fullcsv-cpu-20261113-r1` 及其 GPU 子批次，先通过完整关系门，
再执行 Movie Q5 原序8×8样本。原作者 CartesianProduct→SemFilter、8-token 输出上限及原生调度保留。
实际64次请求全部成功，未知0、重试0；得到20×22中间关系、10行原 sink，原评价为
precision=1、recall=0.8、F1=0.888888888888889。
模型就绪后完整 source→sink 为2.350992505秒，模型启动40.220577725秒、额外预检8.198883225秒另计。
本次为单样本、单冷轮；原生 prefix cache 开启、结果缓存关闭。完整消息及CPU token参考已核，
GPU实际输入／输出 token 数组、usage、金额和kernel计数未观测。物理清理、租约释放通过。
不转授 full256×256、其他后端或后续组合树。

## Kalypso 原作者完整排名

源码 `11eff9cd7b8b4f24138be2ea8095225abc13e752`、code tree
`4ae252a7fc0fa358905696d4ffd648b425dd3b03` 使用同一原作者来源的 `SemTopK(k=N)`：
N<8用插入，否则递归归并；5-token A/B比较、并发20和作者默认引擎保留。
它不同于 LOTUS quick TopK。原 Q9 TopK 分支继续拥有完整排名到评分值的变换及评价。

`baseline-11eff-kalypso-topk-cpu-20261113-r1` 的5项软件检查和原作者0／1／8行门通过；
8行检查覆盖56个有向比较的完整消息、token及上下文，模型调用0。
随后 `baseline-11eff-kalypso-topk-gpu8-20261113-r1` 在同一8行11列原 CSV 上执行一次真实模型资格。
永久预留上界28次，实际16次成功比较，未知／重试／额外预热均为0；未消费额度不退款。
完整排名为 `[6,1,5,3,4,7,2,0]`，原 rank-to-score 与评价经保存答复重放通过，
Spearman=0.60616080621109、Kendall=0.49135381491199537。
模型就绪后完整查询1.041000821秒，模型启动41.215229964秒另计。

16次实际 generate API 输入 token 数组与 CPU 参考一致；API逻辑用量为prompt3954、completion32，
total3986。它们不是缓存未命中量或GPU kernel工作量；输出token数组、金额、纯kernel计数未知。
原生prefix cache开启、结果缓存关闭，物理清理与租约释放通过。此资格只覆盖8行功能样本；
8行资格不转授full256；其独立验收如下，其他后端TopK仍需接线／验证。

### 原 Movie 全量256发生项

同一 `11eff9cd7` 和原作者来源，原完整2000×11 CSV的SHA-256为
`ad5def7df4ece425c741c82d73687b8cb4a61952e1580c0427879cbef3033aaf`。
原Q9选择256×11，128个重复review ID保留。运行
`baseline-11eff-topk-full256-cpu-20261113-r1` 先检查全部65280个有向比较，最长原prompt333tokens，
context32768、额外输出上限5；模型调用0、CUDA未初始化。实际模型预算未在此CPU门创建。

独立运行 `baseline-11eff-topk-full256-gpu-20261113-r1` 永久预留比较上界32640，
实际1508次开始和成功终态，未知／重试／额外预热均为0。256个源位置各出现一次，
原rank-to-score、完整sink和原评价重放通过：Spearman=0.5516305717402453、
Kendall=0.4201608841569373。单次模型就绪后source→内存sink为31.57879594秒，
启动40.210957441秒、额外源码预检8.610074455秒另计；controller→sink为82.264354239秒。
这些是实测不同事件区间，不用相减制造统一时间。

1508条实际消息及generate API输入token数组与CPU参考关联通过；API报告逻辑prompt355007、
completion3025、total358032。输出token数组、金额和纯GPU工作／kernel计数仍未知。
原作者默认引擎、5-token比较、prefix cache开启／结果缓存关闭，物理清理与租约释放通过。
这是一轮该完整Movie输入的功能资格，不能作为跨系统排名或其他组合题的资格。

## LOTUS quick 的完整 Movie 排名

固定`cb75415be49b0bdbd333bde1114115c9fd06aa44`、code tree
`cb15d8ec32e3ca3734ed6e316aaff61811aec7ea`的
`baseline-cb754-lotus-topk-full256-20261113-r1`使用上述原完整CSV与原Q9的256个发生项。
LOTUS1.2.4、原quick／K=N／pivot／parser／rank-to-score及评价保持，输出上限8192，无额外预热／重试。
先通过12项软件检查、32640最坏比较软件IPC与65280个有向比较的实际vLLM0.25.1 renderer门；
完整消息／token／context32768核对，最长prompt326tokens，模型0、CUDA未初始化。
CPU资格峰值RSS6118539264字节、新增证据盘1511399424字节；这是全部有向pair资格展开的开销，
不是生产TopK内存成绩。CPU与真实预算分离，没有重置旧TopK8额度。

随后一次真实查询完成3355次native generate API开始／唯一成功终态，unknown／retry／额外预热0。
永久预留上界32640，未用额度不退款；完整实际消息、输入／输出token数组、原RNG及保存答复重放、
256行完整排名／原sink／原评价通过，物理资源与租约清理通过。
原Spearman=0.6630149296210235、Kendall=0.5156059574917722。
原始答复中Neither1283次、None13次共1296次走**原parser默认True／Document 1回退**；
不重采样、不改parser，不把回退答复当规范二选一输出。

模型就绪后新鲜source→完整内存sink为42.366610336秒，启动31.005879033356905秒另计；
outer controller→sink113.2394963秒。它是ready-first，非独立warm重复或机器冷态。
实际原生配置为BF16、context32768、max_num_seqs128、max_num_batched_tokens2048、
gpu_memory_utilization0.92、LM并发64，测量中未调参；这些是本次冻结值，不称vLLM默认。
原生prefix cache开启、结果缓存关闭，全部实际重复值及原生回退保留。
API逻辑prompt769185、completion10762、total779947；金额、first engine accept、纯GPU／kernel时间未知。
单轮只授予该来源／完整输入功能资格，不与Kalypso不同算法作性能排名。

<a id="imlane-boolean-join-sql"></a>
## LOTUS exact Join over IMLane prediction 的真实 SQL

源码 `8667d526c83f26679ec7efb3dfea8d52584e68cd`、code tree
`986a62fa65f5ff98ea7c7c1ccaadeff9f16759b0` 扩展既有零模型 SQL 资格入口的
`--saved-reference`，复用同一 Boolean 任务、真实 DSO／驱动／UDF／Arrow／Unix IPC／完整 sink ACK。
只有模型进程由显式软件服务替代。保存参考绑定每个 occurrence 的消息、token、答复、完整关系、
最终 sink／评价；同消息不同答复不合并。没有新增 native SQL 或逐题执行器。

运行 `baseline-8667-imlane-join-sql-cpu-20261113-r1`，Movie Q5 原序8×8，源11列，使用64条
历史实际答复作为软件 fixture。参考 SHA-256为
`1d4ef86443cf6e30e8408ef9db5544d1a9604a8193cbfb552d13aefe5a215fff`；
原答复重放不是本次模型质量，最终空表也不能代替完整中间关系检查。

| 门 | 实际 native AI SQL | 软件答复 | 验收 |
|---|---:|---:|---|
| complete | 1 | 64 | 完整8×22中间关系、原 sink／评价、ACK后退出 |
| missing-row | 0 | 0 | 原准备完成后删一输入行，在provider前明确拒绝 |
| duplicate-id | 0 | 0 | 原准备完成后制造重复occurrence，在provider前明确拒绝 |

三门全部通过，模型调用0，物理资源／SHM／租约清理通过。复用既有 DSO
`52bf5763aab6f35b76746a9fc15b75e27bac1534315d1a20926eb4a78c2badaf`，
其原构建身份为 `author_build_mode=patched`；未重编DSO、重链接driver或重跑旧链接探针。
逻辑 Join 仍属于 LOTUS exact，IMLane拥有 prediction与原生传输；不能称为复现原作者 Join 算法。
真实模型质量、性能与其他输入范围不由这三道零模型门授予。

随后 `baseline-8667-imlane-join-gpu64-20261113-r1` 在同一冻结来源与8×8完整源上执行一次真实模型资格。
原生成配置为max_tokens8192、temperature0、top_p1、seed0、stop[]、零惩罚／repetition1；
Qwen2.5-7B-Instruct BF16由独立vLLM0.25.1原生owner执行，driver LOTUS1.2.4与Arrow16.1作者ABI环境隔离。
64次真实generate API调用和成功答复，未知／重试／预热0，完整原消息／renderer输入token逐条匹配。
完整8×22关系、最终空sink、原评价、ACK和资源／租约清理通过，DSO未重编、driver未重链接。
原precision=0、recall=0、F1=0，保留实际质量，不用软件参考替换真实答复。
就绪后source→内存sink4.433729632秒，worker进入至sink67.183766429秒，模型启动53.8795217750594秒，
host controller→sink82.733766769秒，分别保留；单样本不代表fullJoin或性能对比。
API逻辑用量prompt14704、completion4062、total18766；输出token数组、金额、物理GPU生成／kernel计数未知。

## 旧 Ray Q3 专项图退出维护

当前生产入口已调用共享 `execute_prepared`，旧 `execute_q3` 只剩测试调用；本次删除该专项
Filter／COUNT图及callbacks，保留原题任务层。Movie Q1–Q4 使用真实官方函数、LOTUS SDK及同一个
软件 Ray processor 做消息／完整 sink 差分，重复业务ID继续按 occurrence 关联。
现用 prepared 函数、默认参数、原生图、预算及关闭实现逐字不变；HTTP profile 保留其不同服务拓扑。
旧 API 的默认重试／caller runtime 不能冒充新图的隔离零重试，恢复方式及差异见
[历史入口](../../../reproduction/native_baselines/README.md#retired-ray-q3-native-binding)。

81项不同受影响软件检查为78通过、3跳过，模型调用0。首轮新增四题差分误把参考helper的
`(messages, options)`批列表当作平铺消息，断言失败；修正夹具展开方式后四题全部通过，
首轮失败及修复日志保留。3项跳过为独立Daft SDK和未绑定的两个可选真实数据环境变量，
不据此判断资产缺失。此清理不新增任何模型／性能资格，也不承担项目整体历史清理。

## Daft Q3 worker 迁移与原生图生命周期

当前原题driver一直使用prepared worker；旧Q3分支另做tokenizer、源读取、Filter／COUNT，
本次迁移其所有现行worker／测试调用并删除该分支与配置schema。prepared表达式、owner、trace和
关闭实现逐字不变；原Q1–Q4和Join由任务层完成原sink。13项worker／期限／失败检查、
1项包含五道原查询的driver差分及实际Daft0.7.21 SDK的12项检查全部通过，共26项、0模型。
SDK使用已存在的Mac arm64二进制，仅在独立测试进程指定SDK路径，没有安装或改动driver环境。
首轮一个旧Mock未提供prepared结果的native_api字段，修正fixture后通过；失败日志保留。
准确恢复来源及旧执行边界见[退役记录](../../../reproduction/native_baselines/README.md#retired-daft-q3-native-binding)。

另以[可执行诊断](../../../../code/tests/baselines/text/probe_daft_native_lifecycle.py)保留真实
Daft表达式、Rust前缀算子、LocalVLLMExecutor、submit／poll及collect，只替换vLLM依赖。
同一prompt表达式分别处理两个不同的2行DataFrame，实际构造执行器／软件模型各2个，
软件generate共4次；第二次collect新增一个构造。线程／事件循环关闭，真实模型及GPU调用0。
明确的软件资源profile为gpus_per_actor=0、concurrency=1，不将它称为模型性能配置。
[原始CPU回执](daft_lifecycle_mac.json)保存平台、二进制／Python源码摘要和完整两轮结果，
诊断源SHA为 `16a8389dc11decf1c179afdcec86a8d9159ab53a50448862bdea9c39653a4a00`。
这证明该原生本地路径的两次collect未复用模型；不能外推所有Daft入口，也不代表多算子不可实现。
后续接线必须保留实际模型构造／冷启动成本，不能用直接executor或自造池冒充同一原生图。

`baseline-daft-native-lifecycle-20261113-r1` 随后在实际Linux Daft0.7.21验证同一图路径，
使用已冻结的旧诊断源SHA `e95a5000e9bba043df9e999d2e8e6aeeee2c24f70a52a01ba66f3d242d2daaea`，
上下文来源为 `431573e08`，只绑定该脚本及其标准库依赖，不授予整个HEAD资格。
两执行器／两软件模型、4次软件generate和完整关系、事件循环／线程／资源／租约清理通过，
真实模型、Ray runtime和GPU调用0。Mac后续诊断只增加平台和扩展库摘要，不回写Linux旧源身份。

## Ray 图像单遍查询的资格工具

已有生产绑定支持Animals Q1／Q3／Q10一遍、Q7两遍，但CPU捕获和原生processor门仍固定Q7／2N，
阻断了单遍查询目标验收。本次让捕获接受原查询身份，processor只消费其已捕获的原题元数据，
按真实Filter遍数和完整调用量检查发生项；不增加逐题执行器，也不改生产模型／原生图／参数。
捕获保留完整图像消息与RGB核对，使用明确软件答复，乱序结果经现有binding关联；
再直接执行同一原查询／原SDK核对完整sink，不自写各题COUNT、City集合或排序结果。

四道原题分别使用7图软件manifest检查通过，连同旧绑定、预算、完整sink和失败／关闭路径共28项，
0模型、0跳过。首轮一个旧真实CPU子进程测试被沙箱sysctl进程树权限拒绝；未改生产代码，
在允许进程检查后仅重跑该项通过，失败原件保留。真实Ray图像／token／pixel processor与单遍VLM
仍由服务器单独验收，不能从旧Q7的两遍模型资格推定通过。

随后冻结`60c875f9fc6b5b4c3a45421dc46ca3164345ee11`、code tree
`d628b1ac0e75085c0a9a92b54fcd87e849d2ea58`的
`baseline-60c875-daft-rayq1-cpu-20261113-r1`通过Linux Daft14＋12项、无skip，
以及Q1官方sf64的实际Ray2.56.1／vLLM0.22像素、完整token与第65次预算拒绝门，0模型。
没有重复e95两次collect诊断。

同一来源的`ray-animals-q1-60c875-native-gpu64-20261113-r1`随后完成一次真实冷查询：
64次generate API开始／成功终态，unknown、retry、额外预热0；原完整COUNT为3，原relative_error=0。
实际wire消息、请求到原occurrence关联、pre-MM及展开后的完整输入token数组、输出token数组核对通过。
GPU像素张量、纯kernel／GPU工作、first engine accept与金额未观测。
source→完整内存sink为152.202383049秒，**包含原生模型启动**；controller→sink154.334707351秒。
物理清理和租约释放通过；这不转授Q3／Q10、其他来源或ready-first计时，也不是跨系统性能结论。

<a id="native-service-duplicate-response-correlation"></a>
## 原生服务重复输入的答复关联

Q2b实机审计暴露相同请求内容对应不同措辞时，仅凭HTTP日志不能恢复每个原occurrence的答复。
本次在既有 `NativeServiceCalls` 的LM观察范围内记录原 `_process_uncached_messages` 返回的
completion ID顺序，原函数、参数、结果对象和SDK并发方式保持；不向请求插入ID或改模型提示。
记录仅保留ID／计数，不保留额外base64图像副本。查询返回后用唯一completion ID关联实际HTTP尝试，
写入 `native_sdk_response` 事件的LM调用号、uncached位置和HTTP请求号，关联／写盘在sink之外。
缺失或重复ID、结果槽不完整时明确报告不可用，不按输入文本或网络完成顺序配对；
全局请求终态和逐发生项关联分别披露，不改写旧日志缺失信息。

57项不同检查为56通过、1跳过，模型调用0。新增实际LOTUS SDK用例覆盖相同输入、不同答复、
HTTP完成顺序相反时的原结果／ID关联，以及并发LM调用、缺失／重复ID和方法恢复。
完整视觉及分层Aggregate使用实际SDK／原题／软件HTTP检查消息字节、sink、解析与失败边界；
受控时钟证明新增关联不进入完整结果区间，原异常与已有结果在观察失败时仍保留。
跳过只因可选真实sf25 MMQA目录未绑定。新的Linux／模型资格另验，不重跑已完成Q2b来补旧证据。

冻结`40c477f2f9b9ad59f6901f38228bc3681c540300`、code tree
`e2e0fe99ca1658072c10a6590a68714fcdc3b3a9`的
`baseline-40c477-service-IDs-cpu-20261113-r1`随后在Linux原LOTUS SDK绑定真实sf25文本资产，
四模块57／57、0skip、0模型；原日志及资源／租约清理通过。它不修补旧Q2b日志，也不新增VLM资格。

<a id="mmqa-q2b-empty-model-path"></a>
## MMQA Q2b 的空路径模型结果与第二阶段缺口

`lotus-mmqa-q2b-8667-native-vlm-20261113-r1`固定`8667d526c`及上述code tree
`986a62fa65f5ff98ea7c7c1ccaadeff9f16759b0`，完成325次Join POST、成功终态，Extract实际0次。
完整结果为空，原precision／recall／F1均0；ready后source→内存sink45.744609236秒，
模型启动65.08640497秒另计。原生Join实际选中了该题全部声明列，不能把本项质量归因于下文多列限制。

36组相同输入中的103个occurrence原始措辞不能由旧HTTP日志唯一恢复。服务器后置CPU审计确认
这些组的所有原生Boolean解析成员资格在任意组内置换下相同，所以空Join／空Extract／原评价可复核；
这不恢复逐发生项原答关联。首轮严格审计失败保留，未新增模型重跑，不能记作全部观测通过。
空Extract保留维护wrapper的空schema兼容身份，不声称是未修改作者空输入报错行为。

非空Join→Extract已有原查询／原SDK／软件HTTP的完整消息、JSON参数、重复图像和失败用例。
补充部分匹配用例：2个同像素图像发生项×3行表（重复ID及Track保留）产生6个原Join候选，
明确软件答复只保留4个；原Extract只处理这4行，排除的Track不进入结果，重复ID／图像不去重，
实际软件尝试10而非预算上界12，completion ID关联覆盖全部发生项。该模块28项通过、0skip、0模型，
未改变原查询／parser／提示或生产接线；原325次模型请求未重复。
真实非空第二阶段仍未验证。本轮没有发现可据此修改parser／提示的接线错误，不能预填匹配行或
替换真实答复来制造非空。质量负结果及第二阶段缺口分别保留，后续新代表输入须独立计划／冻结；
不为补齐覆盖而重复这325次请求。

只读追查原`MESSAGES-PRIVATE.json`及`native-http.jsonl`后确认该输入为13行表×25图。
官方金标使用的`117d500aaa630023c4038b8268b309c0.png`在原图序第4项，实际发送的PNG可视确认是
清晰的Santa Anita Park标志，原文件摘要为`67e82252780057e1f0d1e2fff8b0a4600dab7ef634573b51b772c08eda93755d`。
该图与Santa Anita Park文本的相同输入组共有5次实际请求：4个答复明确以False收尾，另1个在推理中
给出false，原parser均判False；部分答复读出了正确场所名仍否定匹配。
这排除了该金标图缺失这一解释，不能因此修改原parser／提示，也不能把5个重复请求的措辞强行分配给ID。
其余图像／模型能力不由这一项观察推广，额外模型请求0。

[参考Extract阶段准备](../../../../code/tests/baselines/image/prepare_mmqa_extract_stage.py)随后
复用同一原查询与SDK，用官方正例作**显式软件Join夹具**，在原Extract完成消息格式化后停止；
不生成Extract答复，不返回完整查询、不计算质量，也不改写旧运行。私有artifact固定实际输入、
官方参考摘要、全部重复发生项、原消息与JSON生成参数，供后续独立组件诊断绑定。
临时输入的10个Join软件答复保留5个正例，捕获5个相同图像Extract请求；缺正例时在模型／网络前拒绝，
SDK方法在正常／失败路径均恢复。14项不同软件检查通过、0模型；首轮一项测试误断言SDK的token字段
为`max_tokens`，实际原SDK使用`max_completion_tokens`，修正断言后新增两项通过，失败日志保留。
这项准备工具尚未完成真实Extract调用；将来组件通过也不授予原Join→Extract整体模型资格。

冻结`0755df1c99cc23867d514dbf47e512d6de98e52a`、code tree
`b48e6843016ad7f077972bc8a6d4c390f1d4b509`的
`baseline-0755-mmqa-extract-preparation-cpu-20261113-r1`随后完成Linux原题14项及c982新增部分匹配1项，
共15／15、0skip／0模型。原真实sf25与旧Q2b输入身份相同，原SDK实际消费325个显式Join软件答复，
捕获5个重复图像Extract消息；未完成查询、质量为空、未绑定真实模型／服务配置，未创建GPU预算。
原件及全部payload核验，资源／租约清理通过；仍不能记为真实非空Extract已通过。

## IMLane prepared 请求的 occurrence 交接

原VARCHAR prepared kernel已有call_id列，却没有把ID交给模型provider；Q1入口因而要求所有消息内容互异。
现将原ID交给provider已有的`occurrence_ids`本地回执字段；RPC按原batch ID／位置匹配，未新增请求字段。
消息／生成／原生SQL／调度保持，取消这一内容限制。
共享IPC审计只按occurrence、原batch位置、消息、token及实际答复核对，Filter／Join调用同步迁移；
删除旧消息摘要关联路径、`by_occurrence`开关及无用导入，不保留两套当前关联机制。
旧8667离线audit脚本属于其原件与固定源码，不修改历史脚本；新衍生审计使用现有无开关签名。

82项不同受影响软件检查通过、0模型。已有重复消息不同答复／乱序、缺失／重复／外来ID、位置、
消息、token、参数及关闭失败用例继续使用，prepared UDF另确认原call_id确实交给provider。
首轮IPC模块遇到沙箱AF_UNIX bind权限错误，仅在允许本地Unix socket后重跑该模块21项通过，
生产代码未为此改变，失败日志保留。真实作者SQL／当前kernel绑定及模型资格仍待验；
DSO及C++ driver源码未改，旧构建记录不能手改摘要冒充重新绑定，不重复既有25次模型。

随后417457的Linux首轮执行85次、去重82项：81通过，断连用例的诊断断言失败；同源码单项复核也失败，
没有继续重复。该用例等待软件generate drain后立即读stderr，但原连接还会等待work／disconnect任务及
writer关闭，最后才打印诊断；drain不是连接处理完成屏障。测试现观察原`connection`返回再核原诊断，
保留abort／drain／日志／复用全部断言，不改生产关闭代码、不加固定sleep。
本机受影响IPC模块21项通过、0模型。冻结3d9fb8fe的Linux单项随后1／1通过、0.021秒、0模型；
SQL／kernel／provider／RPC生产字节保持417来源，原失败保留，不能说417457整批通过。

417的当前kernel重新绑定及真实VARCHAR SQL／IPC／ACK也已通过：相同消息的两条不同call_id保留
不同软件答复，DSO不重建；common14单独通过，均0模型。两轮资源／租约清理通过。
独立核验归档及417的1327、3d9的1198个payload全部字节／SHA256；按运行标识从服务器协调者取原件：

| 运行标识 | 归档字节数 | SHA256 |
|---|---:|---|
| baseline-417457-imlane-occurrence-cpu-20261113-r1 | 3434623 | `f32898b4f1877526ba63a5fbee76abc4c2ebd14e306a7c9e18415c709e2ecd1e` |
| baseline-3d9fb8-imlane-disconnect-cpu-20261113-r1 | 3224191 | `6f19527046531c530c10adad95de308bc2af937d96c9c25b158e51a4d0dca8d6` |

## LOTUS quick over IMLane VARCHAR prediction

[新接点](../../../../code/integrations/imlane_duckdb/README.md#lotus-quick-topk-与-varchar-prediction)
复用原查询／quick／消息／parser／排名／评分，将每批LM消息交既有作者SQL VARCHAR prediction。
一个模型服务贯穿查询，每批SQL scope启动／关闭计入完整查询；不称作者TopK算法或数据库常驻。
原LM执行usage计数未使用，实际usage来自完整IPC审计，价格未知不填零。

新增6项软件检查使用临时Reviews输入及原题：重复内容／业务ID、SQL乱序输出、原parser回退和
0／1／5行按原发生项保留；真实Arrow与provider回执执行，SQL进程／模型是夹具。
受控时钟确认每批SQL关闭包含在sink内，token／RNG答复回放和评价在sink后。
审计／关闭失败保留完整结果；prediction批次失败不冒充完整前端结果。
首轮4项失败源于夹具漏了SQL投影的call_id，按真实C++投影补齐后6项通过，失败输出保留。
原Q1生命周期、原quick及prepared UDF／构建的21项既有检查通过；按测试ID去重，本批27项通过、0模型。
随后冻结11f745f720c8605a21e87e5b549d5b02134b93f4（code tree
5597983c41776a78a6c89c83707216c36e7c851d）通过9项Linux检查、0skip。
原Movie Q9前8个发生项的完整11列派生输入，经实际vLLM0.25.1 renderer核对56个有向完整消息／token及
8192输出context门，未初始化CUDA；7批真实作者VARCHAR SQL／IPC分别为7／6／5／4／3／2／1条，
共28个软件答复。一个软件ModelService贯穿查询，每批SQL启动／关闭计入完整sink；
原RNG、消息、发生项、采样、排名、关系和评价回放一致，ACK／作者关闭及宿主资源清理通过。
复用417的DSO／薄driver，无重建／重链；0模型、无GPU预算，软件usage不能当真实模型成本。
仅为该冻结来源的8行CPU接线资格；下述ecbd已另完成8行真实比较，完整256仍待验，旧Q1或LOTUS成绩不转授。

## MMQA Q2b 参考 Extract 组件的真实验证

输入继续使用0755捕获的5条原SDK消息：官方正例Join仅为明确软件夹具，不重跑旧325次真实Join。
首轮CPU门的私有harness误把三字段金标整行与二元(ID,image)比较，0请求、未进入renderer，失败完整保留。
单行修正只投影金标前两列；新轮只补失败门及未执行renderer，不重跑已通过的11f SQL／IPC。
5条原消息、JSON参数及输入身份与0755一致，实际VLM renderer的完整展开token／context门通过、CUDA未初始化。

后续单轮独立组件沿原8667已核VLM／processor／BF16及容量签名，永久上限5次；
5次真实POST全成功、未知／重试／额外预热为0，原始答复与原Extract parser结果均为`{"color":"Blue"}`。
原SDK返回slot、completion ID与HTTP attempt关联通过，保留全部重复参考发生项。
组件“消息→原解析关系”1.332686583秒，服务启动78.093255054秒分列；不是完整源→sink或独立暖查询成绩。
价格、输出token ID、首次engine accept与kernel时间未观测，不填零。进程／socket／SHM及租约清理通过，
原生tracker警告仍保留在原件中。组件没有完整查询质量，旧Q2b的Join全False、Extract0、F1=0及103原答关联缺口不变；
实际Join→非空Extract端到端资格仍未完成。

三份归档及全部payload已独立核字节／SHA256，按运行标识从服务器协调者取原件：

| 运行标识 | payload数 | 归档字节数 | SHA256 |
|---|---:|---:|---|
| baseline-11f745-imlane-topk-cpu-20261113-r1 | 1379 | 3496153 | `7d5e0fb673776165013935d70bc1af7712ca12b046bff77965e6c258b044444c` |
| lotus-mmqa-extract-component-11f-20261113-r1 | 35 | 39883 | `af6f16e3ead97ae103703349b997eed8a5029adfb20823c912c5d99946f6de22` |
| lotus-mmqa-extract-component-11f-20261113-r2 | 89 | 118103 | `055c637399f5e6c291209152e78cdae88cb4238cac1e68f438b7bef696d8f4b4` |

## 原题 Map／Extract 复用 VARCHAR 接点与计时

[同一任务入口](../../../../code/integrations/imlane_duckdb/README.md#原题-mapextract-与-varchar-prediction)
不再由Q1专属关系类承担执行。原任务决定Map／Extract、输入发生项及JSON约束；既有owner／SQL／
ACK／parser／完整查询与评分复用。当前Q1模板及离线helper迁移，删除旧`Q1Relation`；
旧schema仅保留其原配置承诺，进入同一实现。Filter／Join／TopK保持原接点，未扩成混合图框架。

额外文件身份与参考token准备移到计时前，绑定原查询时预检、在sink后复核；
真实源重读、原提示、必要CSV／投影、parser及完整原关系仍实测，不估算扣除。
原Extract拥有JSON参数，不依赖配置猜测。旧Q1各冻结模型成绩继续用其原时钟身份。

12项软件检查通过、0模型：临时输入分别运行Movie Q8、MMQA Q1／Q4、Medical Q10、E-Commerce Q3，
与原SDK的完整结果及消息相同；保留重复业务ID、非连续索引与原JSON额外字段行为。
错误预算／Filter误用在模型前拒绝；原Q1 owner／失败／时钟及TopK回归通过。
新Map／Extract任务及新计时的Linux作者SQL／实际模型仍未验证，不继承旧Q1或其他后端资格。

## ecbd 原题 Map／Extract SQL 与八行 TopK 模型验收

冻结`ecbd60b5469a9103ec7bab69fb80733ba6b8ffe4`，code tree
`6fda2713870f3675feb4a75b65f352f0002763f5`。12／12 Linux CPU、0skip；原Movie Q8选中120项Map和
MMQA Q4实际24项Extract走真实作者VARCHAR SQL／Arrow／IPC／ACK、真实renderer和软件答复，
发生项、原关系及原SDK参考检查通过。Extract原JSON由任务补齐。DSO／薄driver／kernel未改，0模型。
Map首轮在原题参考准备门失败，尚未进入SQL／renderer／模型：私有callback误把sem_map的DataFrame按TopK(frame,stats)拆包。
生产代码未改，仅修正该harness后补Map软件门，实际Map SQL执行一次；原失败保留，不能写成第一次全通过。

同一原Q9前8项、完整11列派生输入随后只运行一轮真实TopK。1个原生模型服务跨4批作者SQL，
批次为7／2／3／1，共13个成功终态，unknown／retry／额外warmup／原parser回退均0；
永久预留28，未用15不退款，单元关闭。保留原RNG，实际13条完整消息／展开token／采样／请求ID／原文关联、
8行原排名／完整sink及原答重放通过；原文为6条Document 1、7条Document 2。
Spearman为0.6667768868321989，Kendall为0.49135381491199537，保留该样本观察，不称质量阈值达标。

就绪后新鲜source→完整sink为15.848765164秒，包含4批SQL启动／关闭；服务启动31.183271148242056秒另计，
worker-entry→sink为56.357690154秒。不是完整256、独立暖轮、作者TopK算法或跨系统性能排名。
价格、输出token ID、kernel、首次engine accept及独立物理generate计数未观测，不填零；
进程／GPU／socket／SHM及租约清理通过。旧模型结果没有重跑或改记为本次来源。

两份归档及全部payload已独立核字节／SHA256，按运行标识向服务器协调者取原件：

| 运行标识 | payload数 | 归档字节数 | SHA256 |
|---|---:|---:|---|
| baseline-ecbd60-imlane-prepared-text-cpu-20261113-r1 | 1425 | 3894995 | `7a2645c34768f5626d445a75d1d73f5a2cb9ded842c6839d859b4a44f362d9f8` |
| baseline-ecbd60-imlane-topk8-gpu-20261113-r1 | 147 | 183851 | `927bb75a96fc81e0703b0b7fc4d1ec7099c0040a4ac1949399e7916b003c35f1` |

## 原版 DOUBLE Map 的完整结果时钟

[既有Q9入口](../../../../code/integrations/imlane_duckdb/README.md#movie-q9原版-double-map-的唯一运行入口)
保留原DSO／SQL／数字转换／参数与预算；把原题预检和诊断快照同必需CSV／原消息物化分开。
模型ready及额外身份检查后实际启动新时钟，真实原查询源重读及最终关系保持；全局ready在完整sink时
由共用窗口发布，随后才做原答回放、prepared摘要、评价及最终关闭。原`complete_query_seconds`宽区间保留，
新增v3 `source_to_sink_seconds`，不是从旧时长相减。共用就绪窗口从VARCHAR入口移到既有生命周期模块，
没有第二套通知实现；原调用名和就绪schema保持。

48项受影响软件检查在HEAD加本批改动的独立干净导出中通过，明确排除三文件草稿，0模型。
受控时间包含90秒模型启动、7秒预检、额外身份／审计／持久化／关闭，同时仅把2秒原输入物化＋3秒完整原查询sink
记为5秒v3区间；原宽区间113秒另存。原1／7／17项、重复业务ID、数字回退、完整关系／评价、永久预算与失败边界保持。
首轮新增窗口测试插入位置错误，误带相邻用例断言，修正测试后干净导出48／48通过，原日志保留。
本批本地未运行新模型；原Q9历史成绩不继承新时钟。后续034dc目标SQL验收如下。

<a id="imlane-034dc-validation"></a>
## 034dc 的 DOUBLE SQL 与 VARCHAR Map／Extract 目标验收

冻结`034dc93a666b4e5b916ecbc8714c45a7a29e6bb8`，code tree
`c764b3beb3f0fcda8cb29055182e979d406657af`。48／48项Linux检查通过，0跳过／0模型。
原版DOUBLE Q9只执行一次原生SQL／Arrow／IPC／ACK，原2000行中256个发生项、128个业务reviewId
和完整11列sink保留；明确软件常量“3”产生256次终态，模型调用0。
根READY与完整sink时间一致且先于原答审计，诊断快照在sink后，预算关闭后拒绝额外尝试。
首轮私有验收在SQL及sink完成后，把数值reviewId与运输字符串比较而断言失败；原失败保留。
随后只按实际列名／类型离线核已保存结果和原评价，未重跑SQL、未改生产代码或DSO／driver。
软件时长和常量答复质量仅作边界检查，不是模型性能／质量，DOUBLE新时钟模型资格仍待验。

同一冻结来源的VARCHAR工程变体，使用既有Qwen2.5-7B-Instruct／BF16绑定和原8192输出上限，
两个独立新预算分别硬限120／24，各一冷轮、无额外预热／重试。原Map为普通文本，
原Extract继续由任务指定JSON输出。原题输入、真实SQL／IPC／ACK、逐发生项答复和完整sink都保留。

| 原查询 | 成功请求／上限 | ready后真实源→完整sink（秒） | 模型启动另计（秒） | 原质量 |
|---|---:|---:|---:|---|
| Movie Q8 Map | 120／120 | 4.267836332 | 29.729959546 | relative_error=0.04043126684636118 |
| MMQA Q4 Extract | 24／24 | 4.337518803 | 29.874565830 | precision=0.5142857142857142，recall=0.46153846153846156，F1=0.48648648648648646 |

两题各一个模型实例，unknown／重试／额外暖机均0；完整源／token／参数门和原答回放通过。
接入会话另核原始worker中的144个唯一call_id及request_id、预算1..N、完整token数组长度、
SQL逐call_id原文答复、JSON响应、实际源／sink时间戳、关闭和物理资源／SHM／socket／租约。
原答回放只验证原SDK解析／完整关系／评价，不再发模型或SQL。价格、output token IDs、kernel和
独立物理generate计数仍未知；API usage不当作GPU work。Extract执行完整不表示质量达标，
本轮不作性能排名，也不授予新Medical、TopK、DOUBLE模型或后续组合树资格。

两包全部1476个payload与归档／清单SHA由接入会话重新核验。私有原件按运行标识向服务器协调者获取：

| 运行标识 | payload数 | 归档字节数 | SHA256 |
|---|---:|---:|---|
| baseline-034dc-imlane-ready-cpu-20261113-r1 | 1329 | 4370955 | `a2596117f6a03982268c3a32c2d86fcbe3e6005cd4486ac1527749be68744b22` |
| baseline-034dc-imlane-varchar-model-20261113-r1 | 147 | 515386 | `9bdbf66b2d7fd0fd4185feee7d5638386af8879f5b645c9acdec3506a6b76fe8` |

## Ray Animals 四题的完整结果时钟

既有一／两阶段Filter入口共用`sembench.source_to_sink.v3`，预检／像素身份、原函数绑定与LOTUS
客户端初始化在真实manifest扫描前完成；原生ImageArray仍实际读图／解码，原查询的提示、parser、
关系后处理与评价不变。Q7的第一阶段关闭及必要物理清理仍计入，最终阶段仍等完整sink才释放。
新`source_started_monotonic_ns`与原sink事件直接形成时长；早期入口事件和实测宽区间
`query_entry_to_sink_seconds`独立保留，不从历史成绩估算扣除。

干净候选导出排除三文件草稿后，35项不同本地软件检查通过、0模型／0原生Ray runtime。
四题受控时钟把7秒预检、11秒输入绑定、13秒源码绑定和90秒LOTUS客户端构造放在起点前；
单遍真实manifest读取2秒＋原生软件执行3秒共5秒，两遍另含第二次执行3秒及必要阶段间清理5秒共13秒。
最终关闭23秒、逐消息摘要与评价均在sink后；原题N=1／7／13消息、默认parser回退、完整关系及评价差分保持。
首轮34项通过，CPU-child一项因macOS沙箱禁止进程树读取失败；获准单项复核后通过，原失败日志保留。
日志运行身份为`ray-animals-query-clock-r1-clean`及`ray-animals-query-clock-r2-child`，可向接入会话取脱敏原件。
这些软件秒数只表征边界，不是性能数据；目标新时钟尚待验收，旧Q1／Q7模型成绩保留原口径。

## Medical 单图／双图 Filter 原生接线

固定SemBench `c814e380`的`files/medical/query/lotus/Q3.py`、`Q8.py`和`Q9.py`
直接交既有LOTUS视觉服务入口。任务绑定只读取实际CSV／图像并计数原首算子关系，复用原查询、
LM／HTTP／预算／原答关联／完整sink／评分和v3时钟；没有逐题执行器或新调度。
工程参照沿用[系统架构§8.7–8.8](../../../plans/系统架构.md#company-engineering-reference)已固定的
算子语义与模型通信分工，本批没有访问或移入公司材料。

Q3／Q8保留患者条件、重复连接行、完整Filter之后的LIMIT 5／100。Q9保留两种图像的患者连接，
每个发生项在同一原消息中携带皮肤和X-ray图，调用预算为N而非2N；不按患者ID或图片值合并请求。
额外身份审计在完整时钟之外，原CSV读取／ImageArray／PNG编码和原关系处理仍在时钟内。
原MedicalEvaluator生成Q3的Series金标后，通过其自身缓存读取接口取得官方CSV的列形状；
其余金标生成、采样、度量和失败行为不改。Q8临时金标不足100且无命中时，完整空sink后仍保留原采样异常。

干净候选导出明确排除取消的三文件草稿。两模块43项一次通过；金标读取改为原缓存接口后，
重验三题差分及9项既有Medical文本检查，合计52项不同检查通过、0跳过／0模型。
软件原查询发生项7／104／11全部消费；原LIMIT结果分别5／100／11行，双图11发生项仍只有11个软件POST。
消息及完整关系直接与原SDK差分，原评分一致；另覆盖缺图／不支持音频预检、源像素变化、
原小金标评价失败和实际scale路径。原症状Filter／Extract、重复标签和缺字段失败保持。
Cars／Medical的CSV读取也共用同一段服务编排，随后7项受影响检查通过。
日志运行身份为`medical-visual-native-r1-clean`、`medical-visual-native-r2-text-and-gold`及
`medical-visual-native-r3-shared-csv`，
可向接入会话取脱敏原件；临时患者／图像只作软件fixture，真实资产、目标renderer及VLM仍待核。

## 78d／db3fe 的目标软件验收和资产范围

78d冻结`78d192af5ef07ffd098224c77c14b7e74f77b112`。35／35项Linux检查通过、0跳过，
随后原sf64的Q1／Q3／Q7／Q10完整捕获分别有64／64／128／64个软件答复。
四题原消息／PNG像素／发生项及完整sink与直接原SDK一致，源、sink、审计与评价时间戳顺序保持；
模型调用、Ray runtime和GPU预算均0。私有测试清单重复方法名、预检日志名冲突两次失败在实际测试前发生，
原件保留，只有后续正常测试体计通过；不改历史模型成绩或授予新v3模型时钟资格。

db3fe冻结`db3feadc2b520e9335871e072f727e49e4c31c39`。52／52项Linux软件检查通过、0跳过／0模型。
已有Medical CSV及原作者checkout的限定只读查找未取得非空原视觉输入：sf20的Q3原连接为0行，
Q8／Q9及其他已查规模缺原图／所需资产。没有运行processor或完整token／context门，没有下载或造数据。
零行只能说明该原查询切片为空，不授予视觉资格；限定目录未找到资产也不能推断整个服务器都没有。
图像准备和真实VLM仍pending，不能记成原生系统不支持。

两包全部payload、清单与归档摘要由接入会话独立核验，并读取四份捕获与Medical原输入检查。
私有原件按运行标识向服务器协调会话获取：

| 运行标识 | payload数 | 归档字节数 | SHA256 |
|---|---:|---:|---|
| baseline-78d192-ray-animals-clock-cpu-20261113-r1 | 1279 | 412800418 | `d69df19963def0d4a437bcc698b6e21921f899336dc368eb86996bd18c724ed2` |
| baseline-db3fea-medical-image-cpu-20261113-r1 | 1206 | 3388445 | `6e3737d980fcc09b5b44c453a397d8da25aeffba769aec34b1495b9e60d5874b` |

<a id="ray-mmqa-image-join"></a>
## Ray 单次图像 Join 的原生接线

MMQA Q2a／Q7复用已有`SemBenchTextQuery`任务绑定、prepared Join和Ray LLM processor，
显式启用其图像stage；原脚本、ZS_COT策略、原PNG格式、左右输入顺序、L×R发生项及原评分保留。
普通无歧义字段名由各自原表识别，`:left/:right`仍保留原标签；不按提示文本或图片值去重。
SDK的多列选列限制照实报告。任务图像快照在源前及sink后核对，worker核对原检查快照后才领取预算；
真实数据／图像读取、准备和冷Ray／模型启动保留在v3区间内，完整原sink后才审计、评价和关闭。
其余后端及Q2b的Join→Extract没有借单Join获得支持，也未增加逐题执行器。

90项不同本地软件检查通过、0模型／0真实Ray runtime，干净导出排除取消的三文件草稿。
新图像关系10／6个发生项分别直接与原SDK消息／CoT／结果／评价差分；软件Ray图验证完整sink先于关闭、
原native图像flag、精确预算和未知终态为空，图片变化在预算领取前停止。既有文本Join／F/M/E、
原生owner及错误边界同时覆盖。真实native processor／完整token及VLM仍待目标验收，
软件时钟和评分不作新模型成绩。

首轮暴露旧桥拒绝原题无后缀字段、旧测试夹具漏新输入状态及检查顺序的问题；修正后受影响44项通过。
补充测试选择器误选一个helper，原11项测试已通过但进程失败；纠正选择器后11项复核通过，失败保留。
之后组合门暴露Ray替身用`patch.dict(sys.modules)`恢复整个模块表，移除了刚加载的igraph评分依赖，
导致后续原评价重新初始化失败。改用既有`module_entries`只恢复指定SDK模块，旧机制直接删除；
35项组合复核通过，不改评分器／模型／解析或隐藏原失败。该测试修复单独提交`f281d1b79`。
输入状态只由图像快照表示，移除重复模式状态后，42项受影响组合检查通过。
日志身份为`ray-image-join-r1-clean`、`r2-source-shape`、`r3-full-driver-and-text`、
`r4-fixed-selector`、`r5-capability-state`、`r6-worker-identity`、`r7-sdk-module-scope`及`r8-single-input-state`，
均保留在接入会话私有证据中；重复复核不累加为新测试数。

<a id="lotus-native-join-projection"></a>
## LOTUS 多列 Join 的原生选列限制

来源为已安装LOTUS1.2.4源码、固定上游及四个独立进程的软件运行，紧凑观测见
[原生选列记录](lotus_join_projection_cpu.json)。原accessor在多个`:left/:right`引用中每侧只留下
最后一个列名，core只接收一个Series；`parse_cols`先转set，所选列还受进程hash seed影响。
同一1×1临时输入声明左右各两列，seed0选择b/c、seed1选择b/d、seed2和3选择a/c；
每次真实消息仅含选中列值，两列遗漏。每进程只用一个显式False软件答复，模型调用0。

截至固定上游[`aa81339c`](https://github.com/lotus-data/lotus/tree/aa81339c813e828bc2e4f51d4209a853c81ce7a2)，
[`sem_join.py`](https://github.com/lotus-data/lotus/blob/aa81339c813e828bc2e4f51d4209a853c81ce7a2/lotus/sem_ops/sem_join.py)
和[`nl_expression.py`](https://github.com/lotus-data/lotus/blob/aa81339c813e828bc2e4f51d4209a853c81ce7a2/lotus/nl_expression.py)
的Git blob分别为`05be2ba10ba40afe1467554b5976b025f06190b0`和
`0e5739a51a633f6948a7c60ecc7db41970bbc955`，与已安装1.2.4相同。
因此完整多列载荷在这条原生路径中不能表示，升级到该来源也不解决；不能把它写成只缺模型验证。
现有LOTUS及复用其Join消息的Ray／Daft／IMLane保留原生行为，`native_join_inputs`继续披露遗漏。
不通过拼接列值、改写原指令或静默补丁冒充官方完整多列语义；这是该接口的限制，不是整个系统无Join能力。

## TopK 软件门移除样本与字段限制

现有[同owner软件入口](../../../../code/tests/baselines/text/replay_lotus_topk.py)改为从原查询取得
字段、N与N(N−1)/2比较上界，保留原quick、原parser、完整排名／sink／评价及既有IPC预算。
全部有向比较用于目标renderer与完整token／context门，不再把8行、28次或`reviewText`当作入口要求。
重复文本可共用明确的Document 1软件答复，发生项仍由原文档位置绑定，不用响应摘要假装来源唯一。
0／1／5行完整原查询分别产生0／0／10次软件IPC，0／1不构造模型；关闭的预算单位拒绝额外尝试。
多字段相同文本的0／1／3／9行还覆盖全部有向pair，目标renderer在context不足时拒绝并关闭执行器。

12项不同受影响检查通过，模型调用0；首轮空原查询不发出算子，暴露资格脚本访问`None.records`，
修正后仅重跑受影响三项通过，原失败保留。原生产TopK、随机pivot及生成配置未改。
这些是软件入口资格，不授予原完整256的模型／性能资格；已有8行真实记录保持原身份，不重复请求。

## Cars 与 E-Commerce 真实资产缺口

`baseline-cars-ecomm-data-checks-20261113-r1` 的Cars独立官方full_data.zip检查限定512MiB／15分钟，
仅请求原四表元数据，不下载图音。实际连接超时、0字节、未取得HTTP响应，按约停止且未重试；
sf64原抽样器未运行。这是本次访问阻塞，不能证明所有位置都没有资产。

E-Commerce只处理此前保留的完整成员缓存，未新增网络请求、未重置原下载额度或截止时间。
恢复清单记录4498个CRC完整成员：styles.csv、images.csv及4496个JSON，共94605481字节；
还缺39950个JSON，不能生成官方sf64，更不生成替代数据冒充原输入。
本会话独立核验的是检查／恢复脚本、记录和清单归档，未重新读取服务器上的全部恢复数据；
成员CRC与恢复事实由保留的原执行记录支持。原下载和未完成尾部继续保留。

<a id="full-query-runner-boundary"></a>
## 完整原题与查询级 owner 的公共边界

本次工程修改针对共享接点只允许一次语义调用的限制，不增加逐题执行器。任务提供
`bind_execution(multiple=True)`的实际算子callback与`bind_original_execution()`的完整原函数入口；
前者返回完整结果和按原执行顺序排列的报告，后者由原LOTUS SDK自行决定实际LM批次、后续输入和最终关系。
默认单算子合同仅保留给尚未迁移的记录图，不由此声称所有后端支持混合查询。

LOTUS＋Ray已使用完整原函数及一个query级runtime，每个实际LM批次仍由Ray原生processor
拥有chat template／tokenization／batching／模型actor。JSON Extract显式映射到原生structured outputs，
首个非空批次才启动runtime，完整原sink后统一关闭；不声明跨批模型复用或物理GPU退出。
LOTUS视觉服务和Ray任务共用同一视觉原题对象，保留原函数及评分，删除被替代的MMQA cached reader
及Ray“准备首阶段再重跑原查询恢复结果”的编排。历史单批回放入口迁移到新occurrence ID，
原中间关系的复核在软件回放中完成，不重复原生执行／预算请求。

隔离HEAD导出后仅覆盖本次源码，明确排除已取消Daft动作的三文件草稿；12模块共191项不同
软件检查为188通过、3跳过。跳过分别为独立Daft SDK与未绑定的真实Medical、MMQA资产，
不解释为数据不存在。官方MMQA Q2b的临时2图／5表行保留原PNG／CoT／JSON：实际10个Join
答复后分别走5行非空Extract和空第二阶段，完整消息／原结果／原评分与直接SDK软件参考一致。
Movie Q9 quick的8行软件输入保留28次比较、7批及原rank-to-score；相同消息不合并发生项。
另覆盖中途失败、主异常与关闭失败、sink后审计失败保留结果、预算关闭、原先单算子／驻留路径及原服务时钟。

首轮44项中的三项失败是旧夹具仍替换已绑定默认parser及使用过严的相同mock时刻先后断言；
改为真实parser回调故障与非倒退事件断言后，扩大整合检查通过。首轮输出保留，不改成全通过。
独立源码复核另发现参数映射问题：原Filter的`logprobs=False`与
[vLLM0.22的关闭表示None](https://github.com/vllm-project/vllm/blob/v0.22.0/vllm/sampling_params.py#L220)
不同；接点只把明确False转为None，保留数值0。显式generation与原Extract的等价JSON object约束
合并一次，真实冲突仍拒绝；原operator kwargs和有效／原生参数分开留存，未测这些差异的GPU开销。
原SDK参数红失败、首轮夹具对duplicate index原生标签写回的错误预期及未绑定checkout的跳过均保留，
没有改原parser或索引行为来消除失败。
本地验证模型／GPU／真实外部服务请求0；软件检查不是目标Ray／vLLM多批资格，旧dde单批真机成绩不转授当前源码。
LOTUS＋Ray／Daft与不经LOTUS执行的原生系统路径按语义和执行归属分列，不重复统计baseline；
当前Ray完整查询报告使用`semloom.sembench_text_result.v2`：`native.batches`记录各次原生预测，
查询级`events`记录owner关闭；原始答复只存`native-predictions.json`，查询摘要和请求文件按call ID关联，
不再嵌套复制预测。默认旧记录图的v1和历史单批文件不改写，回放调用者已迁移新结构；
v2及单副本输出的受影响40项另通过，属于上述191个用例，不累加通过数。
整体接入仍有[当前缺口](../../../../code/INFRA_STATUS.md#current-refactor-status)。

<a id="lotus-ray-native-aggregate"></a>
## LOTUS＋Ray 原分层聚合与计时范围修正

MMQA Q5接到既有`SemBenchTextQuery → OriginalLotusQuery → native_ray_sembench`完整查询循环，
实际LM批次继续交`ray_data_sembench_native.query_scope`。没有新增Q5 runner或复制聚合算法。
首层预检`leaf_plan`、work描述和本地counter加载收回原聚合任务模块，原服务调用同一实现；
旧`_leaf_plan`已删除，无转发壳，服务配置AST和默认保持。原函数、真实`sem_agg`、实际摘要决定的后续
层次、原token计数与评分保留，LM新增context/tokenizer选项的默认不影响其他任务。

预检绑定tokenizer JSON／SHA、目录、声明revision与Ray engine context；实际引擎加载仍明确未验证。
动态总请求数保持`None`，首层给minimum；显式engine上限在新批次前限制，永久预算在真实generate前消费。
源读取在实测区间内，额外绑定／身份和原评价在外。单层、紧凑多层、长摘要增加归并层及错误actor四组
服务↔Ray软件差分，提示序列、运行中`LM.count_tokens`序列、批形状、完整结果和原评分相同。
预算耗尽、归并失败及关闭同时失败保留primary和已有答复，最后由同一query owner关闭。

完整调用链审查同时修正了旧计时声明：`ray_native_observer`在finished output yield前同步写terminal事件，
发布完整文本／token IDs，setup也逐事件写JSONL，均在实际source→sink内。当前因此采用独立
`lotus_ray.source_to_sink.v1`，明确包含这些I/O及必要预算预扣；排除的是driver后置审计、结果写盘、评价及shutdown。
早期完整查询代码沿用的统一v3标签并未排除上述内部成本，现撤回该可比性声明；冻结原字段、数字和失败不改写，
也不事后相减。后续可靠留存／flush接点仍待闭合，本批不为改标签重造协议。

对[固定Ray2.56.1原生输出](https://raw.githubusercontent.com/ray-project/ray/ray-2.56.1/python/ray/llm/_internal/batch/stages/vllm_engine_stage.py)
的只读核查确认，公开行已有`prompt_token_ids/generated_tokens`及计数，但`request_id`仅replica内唯一；
`tokenized_prompt`是被消费的输入字段，不能冒称实际返回token。公开行缺`finish_reason`，默认
[DetokenizeUDF](https://raw.githubusercontent.com/ray-project/ray/ray-2.56.1/python/ray/llm/_internal/batch/stages/tokenize_stage.py)
还会覆盖文本，不能等同observer保留的原始模型文本；整批materialize失败也不返回已完成的部分行。
因此不以公开字段“成功可取”推断当前失败证据可无损删除，也未为此增加重复payload副本。

独立候选158个不同软件方法为154通过、4个资产／独立Daft SDK未绑定跳过；早期harness拦截系统字体、
只读git show和明确RecordedFixtureEngine子进程的失败原文保留，收窄guard后补验，不修改产品迎合guard。
整合后的29模块共346个不同用例最终为340通过、6跳过：真实Q5／MMQA／Medical输入、独立Daft SDK，
以及两项未绑定本地tokenizer的原作者token门。首轮两个旧Join夹具绕过构造器、缺新增`aggregate_task=None`，
修复夹具后仅重跑两项；另绑定既有05024 Git对象补验12项，保留两项tokenizer跳过，不将其解释为缺数据。
其余328项通过结果保留，计数按用例ID去重，不累计各批次数。模型／GPU／真实外部服务请求0；
实际Ray SDK、目标tokenizer／模型加载、完整Q5模型质量及物理清理仍待目标环境验证。

<a id="ray-query-concurrency"></a>
## 原生分组调用的并发身份与 runtime

固定LOTUS1.2.4的`SemTopKDataframe`在groupby分支使用`ThreadPoolExecutor.map`共用LM。
源码核对及受控交错复现两个共同缺口：QueryLM先读`len(batches)`、复制消息后才追加，
可产生相同批次ID；Ray在`init`返回前发布runtime，其他线程可能先读取尚未建立的版本字段。
现在只锁批次登记及runtime初始化／关闭，模型预测和原生图仍并行；失败初始化不重试，
失败批报告保留实际`input_call_ids`，完成和报错关联不依靠并发追加顺序。

首次五项复现为1通过、3失败、1错误；修后新五项和旧Ray API十项15／15通过。
整合后全新导出另验七个受影响模块105项，102通过、3项独立Daft SDK或真实输入未绑定跳过；
保留前述重叠计数，不累加。Barrier验证预测确实重叠、单次init／close、唯一ID、
失败和正常兄弟批独立关联及原参数；不将软件Ray替身当作真实SDK／模型或物理清理资格。
另只读核对固定Ray2.56.1的Processor、ActorPoolMapOperator、StreamingExecutor与vLLM stage：
每次Dataset执行有独立原生pool，公开Processor没有可借用既有engine/session的句柄。
复用Python对象／runtime不是跨独立LM批次复用模型的证据，`concurrency`也不是查询全局上限。
正常耗尽后的graceful shutdown／UDF析构与GPU进程真正消失不同；多GPU的原生placement group
释放还需目标状态核验。后续验收分别记录批次构造／PID、资源排队、完整sink和退出，不增加自造池或新调度。

<a id="lotus-ray-original-visual"></a>
## LOTUS＋Ray 的完整视觉原题

同一`SemBenchTextQuery → OriginalLotusQuery → native_ray_sembench`路径绑定既有视觉Source，
不再为新模态先捕获单个语义算子。Animals使用manifest及digest，Cars／Medical／E-Commerce保留原数据根；
真实CSV／Parquet读取、ImageArray解码／PNG格式化、实际中间关系、全部原算子及最终关系后处理都在source→sink内。
外层身份预检、设置／模块绑定在前，验证、评价、结果／driver证据写盘与runtime关闭在后；
actor内同步observer I/O仍按独立Ray v1披露。原groupby线程通过既有`native_query_settings`取得LM，未改并发算法。

临时原schema数据差分覆盖Animals四题、Cars两题、Medical三题；E-Commerce的图像Filter／Map／Extract、
非空多字段描述Join、图像self-join、4→3实际survivor Extract、多分支Join及分组TopK。
空Join、原空Extract的KeyError、小Medical gold与空groupby的原ValueError也保留。
多列Join仍披露原SDK实际选列，不补造完整字段语义；Q10／Q11各中间分支为空的组合尚未逐项覆盖。
既有MMQA typed/image_join、历史保存答复回放和相关公有API仍有实际CPU资格调用，未新增兼容壳或误删。

独立审查发现新入口会接受全局JSON／logprobs等覆盖，先留三项参数失败复现，再与原服务的标准生成合同对齐：
Extract拥有自己的JSON请求，全局选项不改变各原算子。另同步输入检查器的接线状态和Animals的CLI／supervisor／worker参数。
初始组合197个方法中，Animals两项subtest因原gold SQL并列值无ORDER BY而行序不同失败；它们属于同一方法。
只修正夹具比较无序金标的方式，保留原SQL、结果和评价，定向重跑该方法的四题；
再新增一个方法补验五种未覆视觉调用形状。最终按方法ID去重198项为194通过、4项环境／真实输入未绑定跳过，
并非重复运行次数之和，首次失败均保留。生产源与测试导出一致，未重跑模型。
真实原始数据、Ray SDK／VLM／processor加载、完整多批模型质量、构造次数、资源排队及物理清理仍需目标验收。

<a id="lotus-service-query-lifecycle"></a>
## LOTUS 外部服务的同一查询生命周期

视觉／多模态完整原题与MMQA Q5聚合改为同一`sembench_lotus_query` worker／CLI，
原查询对象收拢到`baselines.sembench_lotus.OriginalLotusQuery`，与LOTUS＋Ray任务共同使用。
原函数、算子、数据读取、实际后续关系、评分及两种既有配置schema保持；Q5的tokenizer预检和
原层次聚合仍有自己的明确分支，没有变成逐题执行器或新增查询语言。
该次重构保持完整sink事件及各自旧`query_seconds`／`elapsed_seconds`区间。
后续独立源码审查确认，HTTP observer在原send前计算请求JSON／内容指纹，NativeServiceCalls同步做
inline-image审计hash，BufferedEvents.record同步编码和入队，后台writer也可能与查询重叠。
因此当前服务时钟更正为`lotus_service.source_to_sink.v1`，明确包含上述观测和必要额度预扣，
排除的是额外预检、后置身份核对、结果编码、原评分和最终drain；不再据旧v3名称宣称全部审计／写盘已移出。
事件起止与数值计算未改，历史原件和模型成绩不重标或相减；这只修正观测口径，内部审计闭合仍待完成。
更正后全新导出的五个受影响模块65项软件检查为64通过、1项真实sf25输入未绑定跳过；
原HTTP MockTransport／SDK／预算／原结果和失败留证检查保持，实际模型／GPU／外部服务请求0。
此复核与此前组合检查有重叠，不累计为新的覆盖数量。

删除旧两个CLI／worker及旧视觉原题对象前，七个完整原查询软件差分覆盖Animals Q7、MMQA Q2b、
E-Commerce Q12、Cars Q3、Medical Q9，以及Q5单请求和多层长摘要。
原消息／有效采样参数、原生LM批形状、完整结果和原评价一致；三个受控时钟profile的旧区间也一致。
十二个无效配置保持原异常类型／文字，两个有效配置归一结果相同。删除后全新隔离导出中，
105项检查为104通过、1个未绑定真实sf25输入跳过；不累加为新增105个用例或整个分支资格。
本次模型／GPU／真实外部网络请求均为0。

在整合IMLane Q3和此次任务对象迁移后，全新HEAD导出覆盖全部选定源码并移除旧模块，
16个受影响模块共245个不同软件用例：241通过、4跳过；四项为独立Daft SDK及三份本次未绑定的
真实输入，不作为缺数据结论。该组合检查包括上述105项及先前公共边界／IMLane检查，不能把各批计数相加。
已取消Daft动作的三个未提交草稿未进入导出，未新增对应文件；末尾空行修正不改执行AST，其余源码与被测导出逐字一致。

现行导入、CLI、测试与文档已迁移；已核活动仓库外工具和本地执行进程没有旧调用，
冻结实验包及原失败记录保持。[准确恢复来源](../../../reproduction/native_baselines/README.md#retired-lotus-service-query-runners)
指向删除前的`1c321afef`。新共用入口的实际服务、tokenizer／processor、模型质量及资源退出仍按准确来源另验，
不能把该重构或此前冻结成绩视为新源码目标通过。

<a id="kalypso-empty-frontier"></a>
## Kalypso 原题零发生项

固定作者05024和原SemBench查询，原生CSV source可以为空；旧接入却要求正数逻辑／物理额度，
后验owner集合比较又将零API事件误判为model UID变化，CPU最大prompt统计也无法处理空列表。
现在允许有限零额度，仍在generate前阻止任何超额调用；观察器安装时创建真正的空trace，
逐事件owner检查保留，没有构造假的成功终态。无请求的最大prompt、模型／采样观察保持`None`。

三个原题空frontier回归先失败，修后Filter／Map／Join的原关系和预算通过；临时原schema数据经
现行CPU输入冻结、固定作者方法／规划器及显式软件KV依赖完成空计划，未加载权重或CUDA。
受影响11模块82个不同方法最终80通过、2个未绑定真实tokenizer门跳过。
首次扩展CPU夹具漏提供`vllm.kalypso.context.RETRY_TASK`，仅补夹具后定向重跑该方法；首次输出保留，
其余79项通过保持，不累计重复次数。既有正数输入、原生length retry、错误／身份与sink留证检查保持。
真实服务器、非空模型和新计时资格不由这组无模型结果授予。

<a id="kalypso-system-runner-review"></a>
## Kalypso 系统 runner 与完整结果保留

基于独立候选`19d91e339f2f09b5d6bc5c071959429f9a2e7e98`收拢已有文本Filter／Map／Join／TopK：
一个`KalypsoRunner`拥有查询生命周期，四个handler保留各自原生调用、parser、Map重试及TopK insertion／merge。
原四个Driver类及维护调用已迁移删除，[准确恢复来源](../../../reproduction/native_baselines/README.md#retired-kalypso-operator-drivers)
保留。旧Movie Q1–Q4入口和作者服务owner继续按原职责存在；当前仍是已接单算子，不能称完整多阶段接入完成。

整合前独立审查发现新循环把结果编码放在trace／source审计之后，导致已完成的Map结果在后验失败时丢失；
旧Map原顺序会保留结果。最小修补只把`result_encoding`放在完整sink后、审计前，不改变请求、原生策略或计时起止。
两个新增用例使用实际`query_evidence`／persist，先复现缺`result.json`，修后保存完整原关系、原异常及失败状态，
没有成功quality，不接管外部模型关闭。最初5项中2错误的输出与修补后5项全过分别保留。

固定公共依赖`613d930bc`的私有隔离导出中，四算子39项为37通过、2个可选tokenizer未绑定跳过；
补充owner／内存sink／CPU输入交接23项全部通过，共62个不同用例，60通过、2跳过，5项定向检查包含其中。
新Kalypso生产／测试源码逐字等于该导出；原owner工作树未改。模型／GPU／外部服务启动均为0，
原来的作者环境／真实模型成绩仍只属于其冻结来源。

对仍维护的Movie Q1–Q4另核实：旧profile使用manifest的有限prefix、原样`cfg.native_instruction`、
扁平结果和`row_audit`，新field-bound Filter使用原题文件与字段绑定，两者不能按同题号静默替换。
它们是同一Kalypso系统的前端profile，不另计系统baseline。旧`NativeQueryDriver.query`也有已完成sink
但trace失败时缺结果的遗留行为；另一个最小修补仅把原结果编码移到sink之后、trace之前。
旧提示／prefix／schema／分派／owner不变，23项相关CPU全过，受控source→sink仍为29秒，
首次12项的1个顺序断言失败和2个缺结果错误保留。此23项与前述62项有重叠，不累加为新全树通过数，
也不授予新模型／性能资格。

<a id="native-imlane-q3-cf220"></a>
## 独立原生 IMLane Movie Q3 的查询时钟

整合来源`cf2208960225cb64e8aba3b338d028bda5ec33ef`的薄driver、worker和软件检查，
复用现有作者prediction SQL／Python UDF／独立模型服务；不另建第二套Q3执行器。
该路径的题目合同不导入LOTUS，明确参考1.2.4 non-CoT规则；它不是作者未公开的论文Q7 UDF复现，
也不与LOTUS over IMLane组合路径重复统计。cf220初次整合时三份生产／测试源码保持相同Git blob，
只更正README把DOUBLE Map误标为Q3时钟的文字；DOUBLE继续使用`sembench.source_to_sink.v3`，后续Python拆分另记如下。

模型与作者runtime就绪后，真实CSV读取、必要输入物化／交接、DuckDB扫描与普通谓词、
prediction及C++完整COUNT进入`imlane.q3.source_to_sink.v1`。额外COUNT／EXPLAIN在完整COUNT
时间戳后，worker冷区间另记；该冻结来源的输入packet摘要和元数据写入仍在当次主区间，
不是其他路径排除额外审计后的统一成绩，不作事后相减。
整合后的30项本地软件检查通过，0模型／原生SQL；Linux薄driver、SQL和模型事实来自下述协调者回执。

服务器协调会话报告该冻结来源的Linux22项及THREAD SQL／Unix IPC门通过：12行输入、8行prediction、
完整COUNT=4，0模型。随后同源真实2000行输入、120个发生项均有终态，unknown／重试0，
COUNT=7而原金标14，相对误差50%；ready-source至完整内存COUNT为3.816秒，
worker至COUNT为35.007秒，模型启动30.478秒，资源释放完成。模型为Qwen2.5-7B-Instruct BF16、
vLLM0.25.1，原生prefix cache开启、结果缓存关闭。
本会话随后逐一复核138份payload的字节／SHA、原2000行CSV及120个发生项，以保存的UDF／IPC
重新关联原消息和答复，重算原parser、COUNT与官方评分，结果一致；没有重跑SQL或模型。
120次API发生项与原账本／请求身份分列，独立物理调用、kernel、价格和输出token ID数组仍未知。
首轮私有审计误将API次数当作必须填入的物理计数而失败，修正私有断言后保持原未知项，失败记录保留。
不据此声称质量达标、整个IMLane runner或全部SemBench已通过。初次整合继承相同源码身份，
冻结原件不改记为后续组合或新时钟运行。

<a id="native-imlane-q3-input-audit-v2"></a>
### 后续 Python 输入物化与身份审计拆分

真实source读取／parse／投影与四列CSV写出、flush/fsync仍在查询内；SHA、额外统计和packet发布移到
COUNT后、owner清理后、`audit_result`前。身份来自本次实际读取字节与投影行，使用同一个writer重建
期望导出字节，再与实际`input.csv`核对；源或导出文件后续变化不能获得成功。现有`prepare_input`
仍即时导出并发布packet，软件fixture的调用和数据合同保持。失败也保留可发布的源证据，一次性发布失败
不覆盖原native错误，也不因清理再尝试不确定的同一写入。

这次只排除了outer source/packet层，Python时钟命名`imlane.q3.source_to_sink.v2`，原生COUNT marker v1单列。
沿完整调用链复核发现，成功Arrow batch在COUNT前仍同步发布UDF receipt和provider started/terminal，
并计算消息／请求／响应指纹；这些尚未后置。IPC、client关闭、失败terminal监测与额度预扣是必要carrier，
不能为计时整体挪走。因此没有采用最初候选的统一v3标签；相同起止事件也不能证明额外审计全排除。
成功审计的query-scope留存及COUNT后可靠flush/确认、实时失败控制与完整审计payload的分离仍待闭合。

现有输入／worker及直接native语义检查在隔离组合51项全过；12种临时CSV乘3种mode的36组旧／新
`input.csv`和`packet.json`逐字节相同。受控软件区间包含2秒读取、3秒导出及5秒完整COUNT，后置
身份／统计／发布注入的31秒未进入10秒查询值；这些是软件时钟表征，不是数据库或模型延迟。
首轮过严harness拦截了NumPy的只读`lscpu`探测，失败输出与放行准确探测后的通过结果分别保留。
本批C++、DSO、UDF、provider和模型协议未改，真实SQL／模型及保留raw bytes/rows的规模RSS仍待验证；
cf220的120次真实运行、质量误差和v1时长保持原样。

<a id="aggregate-system-runners"></a>
## 各系统 runner 的聚合与完整查询接线

基于`ce88968668d9ad7ac4dd8c7b06ae0070944c83fe`整合三条路径。固定SemBench c814e380、
LOTUS1.2.4、Kalypso05024；原题source／关系／评分保留，LOTUS组合与Kalypso原生算法分列，
没有新增逐题runner或另一套调度器。

| 路径 | 实际修改及生命周期 | 尚未授予的资格 |
|---|---|---|
| LOTUS＋Daft | 同一原函数／QueryLM接每个实际批次，沿既有独立原生prompt worker；下一批前关闭当前worker，最后完整sink后ACK才关闭最后worker。生产旧prepare／finish调用迁移删除，冻结回放保留原消息／token／发生项身份映射。 | 真实目标tokenizer／模型、多层GPU、物理清理；TopK取消项仍停止，字符串provider没有逐行图像carrier。 |
| LOTUS＋IMLane | 同一现有CLI接原Q5层次算法，复用SQLPredictions／VARCHAR／Arrow／provider／ACK；一个模型owner、每批真实SQL scope关闭后继续原算法。 | Linux真实SQL／DSO与模型；这是LOTUS over VARCHAR工程变体，不是作者聚合算法或论文Q7 UDF复现。 |
| Kalypso | 同一KalypsoRunner增加SemAgg handler，共享任务只绑定原完整Aggregate输入和最终投影；05024 SemAgg／BlockingExecutor决定分块、并发和摘要归并。 | 真实服务serializer／tokenizer／模型与物理退出；其他连续算子、结构化Extract、多模态尚未接线。 |

Daft和IMLane的真实摘要内容决定后续LOTUS层次批数，动态总数保持未知，配置提供有限永久预算；
Kalypso N>1每次至少归并两个context，调用上界N−1，N≤1保留作者零调用／原文透传。
原生idx=-1不是源lineage，不能据此补造所有源行关联。三条路径都不借用已完成的旧模型资格。

软件检查使用临时输入、实际原题／原评分与原SDK／作者方法，模型、KV及部分进程为明确fixture：

| 整合检查 | 运行方法数 | 通过／跳过 | 逐项结果 SHA-256 |
|---|---:|---|---|
| IMLane聚合／共享原题受影响回归 | 87 | 83／4 | `560226fcb232b3cc2ff49e2efdfb79ae7ba19a1475c387d013d917b1f91b4e2a` |
| Kalypso原聚合及旧系统handler | 90 | 88／2 | `4deabf3d46447f385a7d1bfab57cdd7b0c25e81bfca455057cf0d7f570be495a` |
| Daft完整查询／ACK及服务结果字段 | 60 | 60／0 | `21339c8a9f0a812f4bfc8026bb5341db39327e977471e0c130410b7208289093` |

三组有交集，去重为225个不同方法，219通过、6跳过，不能相加为237项资格；0真实模型／GPU。
Daft包含实际本地0.7.21 CPU图与软件模型、受限CPU子进程READY／ACK，IMLane包含实际Arrow／provider／
relation ACK但SQL进程是fixture，Kalypso执行固定Git方法体的SemAgg／BlockingExecutor／parse/build及
显式软件KV/API，均不等于完整目标环境。跳过是本次未绑定真实MMQA／Medical、单独SDK或tokenizer，
不能直接推出数据不存在。完整计划、逐项ID、脱敏输出、patch及源码摘要保留在同名私有交接中。

保留并修复的有效失败：Daft旧worker可在父query sink前关闭，新增明确release ACK；独立复核发现
按时sink后的final close被查询截止误拒，现及时ACK后使用单独cleanup额度，中间关闭仍消耗原查询期限。
Kalypso作者worker吞掉预算拒绝时可能只返回一份部分摘要，真实原方法软件反例先失败；现记录已有trace的
aggregate拒绝事件，后审计使run失败，保留result而不产成功quality，不改作者归并算法。
早期夹具的字段顺序／退出主异常、准确只读help子进程与guard路径错误均有原件，未据此重跑模型。

三条新路径分别使用`lotus_daft.source_to_sink.v1`、`lotus_imlane.source_to_sink.v1`、
`kalypso.source_to_sink.v1`直接记录fresh source→完整内存结果。原生必要交接／中间关闭保留，内部observer／
provider同步I/O仍在区间，各自冷暖及剩余成本明确；不贴统一v3、不减去估算。
LOTUS附着服务没有远端READY观察，旧startup=False／attached-ready文字改为未知；实际HTTP等待仍在区间。
原打点、旧时长与调用行为未变。其余未接线与原生限制继续见当前能力表，不宣称整个接入完成。

<a id="native-051-minimal-model"></a>
## 051639 最小真实模型回执

服务器协调者在准确`0516391a0e88ad0d22c49d110a0f5c91ec10bf69`完成103项CPU及四次真实请求：
独立IMLane Q3输入32行、3次请求、COUNT=0；LOTUS＋Ray原Q5 sf25源25行选16行，原单叶1次请求、
F1=1。均终态、无重试。Ray cold source→完整sink为70.494273555秒，IMLane ready区间2.4022秒，
不能据此互相比性能；多层／并发及新视觉GPU路径未验。

Ray原外层状态仍保留失败：私有审计把整数request_id与引擎字符串ID直接比较，原生产query已完成，
固定SDK的转换规则和保存原件离线核对通过；首次清理拒绝及后续准确清理补充各自保留。
本接入会话逐字复核1578份payload加清单与归档一致，1136个snapshot源Git blob等于准确提交；
没有重跑模型或改写旧失败。首次本地校验误把全部1136源文件当作code目录1117文件的计数断言失败，
按实际另19个追踪文件核完全部快照，失败原因保留。归档身份见下表，不授予本批新runner的模型资格。

<a id="ray-animals-q1-051639"></a>
## 051639 Ray Animals Q1 完整 sf64

`ray-animals-q1-051639-full-query-20261113-r1`在同一准确051639原查询／完整runner上执行一次：
官方sf64输入64张图，64次原生请求均终态，未知／重试／额外语义预热0；完整COUNT=3、金标3，
原评价相对误差0。该COUNT不能证明64个逐项分类全部正确，也不是Animals全部8718图或全套SemBench。

模型Qwen2-VL-2B-Instruct BF16，Ray2.56.1／vLLM0.22.0；实际配置为max_num_seqs4、
max_num_batched_tokens2048、context32768、GPU利用率配置0.8、prefix cache关闭，结果缓存关闭。
模型／processor／输入身份、64份完整消息、展开输入token、输出token及采样已在冻结原件关联，
API逻辑input81307／output128；金额、GPU像素tensor、首次engine接受和kernel时间未知。
保存答复重放原Filter、完整COUNT与原评价通过，不重复调用模型。

独立`lotus_ray.source_to_sink.v1`为119.966097066秒，包含新鲜源／图像读取、原SDK准备、
原生冷启动及必要pipeline退出、同步actor观察／容量回执I/O；最终Ray.shutdown、driver审计／评价和持久化在外。
该单冷数值不能与ready-first、旧152.202383049秒或其他时钟直接排位。多层聚合、并发和同owner真暖仍未验。
三段执行监督通过，现场无owned进程、GPU1MiB、原SHM未变，租约释放；不能由runtime关闭推断其他来源亦完成物理清理。

原Q5的私有ID审计失败与首次清理拒绝保留，本轮依照实际SDK转换及原清理规则通过，未修改生产Ray或放宽保护。
另有归档传输首次超时及恢复ACK迟到失败：完整包已按同账户原连接恢复、SHA与1431份payload加清单一致，
1136个源码Git blob也经本接入会话复核；执行结果未受影响，归档confirmation状态仍保持failed，未重跑query／模型。
首次本地复核沿用旧包source前缀，实际新包是source-snapshot，计数断言失败；检查确切成员后核完全部源码，原失败保留。
不把这轮资格转授新807224的聚合路径或其他视觉题。

<a id="imlane-zero-occurrences"></a>
## IMLane 合法零发生项的原查询

基于807224dd7复用既有原版TINYINT Filter／Join runner，删除Python非空拒绝并将C++参数体放开至N=0。
物理预算保留`max(1,N)`，SQL cardinality独立传实际N，不把正账本额度当行数；零请求不加载tokenizer、
不创建模型／provider。真正捕获的零行Filter／Join仍走必要header CSV／messages、原SQL／ACK／原完整sink。
原函数提前返回且operation=None、已知请求数0时只执行真实原函数，不造空Join／SQL，semantic_operator=None。
模型READY及ready-source字段不适用时为空，source→完整结果直接实测，正数旧流程保持。

整合独立38／38软件检查通过，无skip／真实模型／GPU／原版DSO执行：直接编译当前C++参数体并测
ASCII／溢出／缺参／重复／非负边界；真实Arrow零表在provider前返回；Movie Q3空Filter与E-Commerce Q7
零对经软件SQL结果和实际Python ACK完成原关系及评分；Movie Q4和空Movie Join原函数直接返回、无SQL。
另覆盖错误SQL cardinality的abort ACK、后置评价失败保留完整sink及旧Filter／Medical／Movie行为。
来源中的SDK多列Join选列限制继续披露，不由零调用检查授予非空消息资格。
逐项结果SHA为`f26227e115518796329fad275f714c1c3046035087e54bf1843c64d7bfd41243`，原r1夹具缺少resource_state及gold列的失败保留；修夹具未改变生产源。
ROOT仅把no-op的timing_scope文字更正为“所需SQL仅在实际到达时执行”，打点与行为未变。

原DSO／UDF／ACK未修改。当前薄driver源码身份检查自动拒绝旧构建记录，Linux必须重链后验真实0行SQL／
IPC及物理清理；本地参数体和软件SQL不能代替该目标资格。构建及原件入口复用既有说明，无第二套runner。

<a id="imlane-original-query"></a>
## IMLane 完整视觉原题与原生载体

基于3500ef740整合仍固定原SemBench／LOTUS1.2.4的同一native_imlane_sembench runner，
新增显式`semloom.imlane.lotus_query.v1` profile，复用原函数、QueryLM、已有VARCHAR SQLPredictions和模型owner，
不是作者未公开的论文runner复现。各原生系统算法与工程变体身份继续分开。

原实际LM批次分别写不可变provider配置与receipt目录，共用一个模型socket／永久unit；全局作者注册端点的
SQL／配置／关闭scope串行保护，原SDK批次和实际survivors不改。原Extract的JSON object仅在native采样边界
等价映射为structured_outputs，原SDK与实际native参数分别保存；新profile拒绝全局JSON／logprobs／repetition覆盖。
旧schema／默认保留，不把typed prepared单算子入口变成第二套完整查询runner。

依据固定[v0.25.1 renderer](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/renderers/base.py)、
[EngineInput](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/inputs/engine.py)及
[AsyncLLM.generate](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/engine/async_llm.py)，
原模型桥的token-only条件确会拒绝真实multimodal输入。修正后把同一完整EngineInput交generate，
不丢mm_kwargs/mm_hashes/mm_placeholders，也不dump图像tensor，仅观察类型与字段名。
文本参考token规则不放松；只有真正image_url行可缺独立参考，须关联原消息、native multimodal类型、
已展开输入token数组与usage，并明确独立processor参考未验证。这不是完整像素tensor／GPU证明。

整合后的110个不同软件方法单次全过、0skip，结果SHA为`467401c11ee04b4905ca9d6cba417ce78009d6c04cb1e6f96692a84242bc371e`。包括carrier49与旧文本／
Map／Extract／TopK、零输入、参数体和失败回执；这些与前文各批重叠，不累加为全套系统通过数。
新视觉差分仍用临时PNG／CSV／Parquet和明确软件renderer／generate／SQL，走实际Arrow、provider wire、
ModelWorker.batch/_row、原SDK及Python ACK：Animals Q1一批4、Q7两批各4完整输入；MMQA Q2b实际非空
4次Join后4次JSON Extract；E-Commerce Q12文本4→图像3，空路径保留原KeyError。
比较的是完整原函数／结果／原评分，实际坏input type、纯文本token被篡改、预算和后验失败保留原结果与失败状态。

首次复用fixture误改stdlib socket，真实asyncio自管pipe拿到SoftwareSocket而失败；只将mock限制到
provider模块socket命名空间后通过，原失败保留。provider单项最初沙箱bind拒绝与其后产品红／绿分开，
获准的本地Unix软件fixture不访问TCP、真实服务或GPU。ROOT首次交付清单字段名校验错误在patch已应用后失败，
核实际候选SHA后继续，未重复apply；首次无plan的launcher没有启动任何测试。两处私有harness失败均保留。

独立`lotus_imlane.source_to_sink.v1`从需要模型时READY后的fresh source到完整原内存sink，
含必要SQL／ACK／scope关闭、provider配置／指纹／回执I/O；事后token／关联／源审计、评分、最终模型关闭在外。
zero/no-op无READY前提，不伪造统一v3或减掉估算成本。初始110没有授予groupby（后续验证见下）／其他factory路由、
Linux真实SQL／VLM／物理清理均未授予资格；807的单叶真实结果只属于其旧冻结来源。

<a id="imlane-807-single-leaf"></a>
## 807224 原LOTUS聚合 over IMLane 工程版

`imlane-aggregate-807224-native-qualification-20261113-r1`固定807224dd7，31／31 LinuxCPU、
一次真实VARCHAR SQL软件门后执行原sf25 Q5单叶：1次真实请求终态，未知／重试／额外语义预热0，
输出Lizzy Caplan，原precision／recall／F1均1。它是LOTUS原SemAgg over VARCHAR-owned工程版，
不是未修改作者DSO或作者聚合算法。

Qwen2.5-7B-Instruct BF16、vLLM0.25.1；实际context32768、max_num_seqs128、batch tokens2048、
GPU利用率配置0.92、prefix cache开启、结果缓存关闭；原generation输出上限8192、seed0，未在线调参。
API逻辑prompt3442／completion5／total3447，金额、kernel和未独立观测项仍为空。
ready-source→完整sink为5.09760984秒，含启动worker→同sink为45.612627451秒，模型启动31.43684386秒；
必要SQL／ACK／scope关闭与同步receipt在v1区间，不与Ray冷区间排位或回填当前组合。

执行／资源／租约释放通过。1367份payload加清单与归档逐字节一致，1143个snapshot源Git blob由本会话
复核等于准确807提交，未重跑模型；本轮准备路径失败、stale console的051标签及ACK检查把纯文本当JSON的
错误原件保留，源码与断言实际都绑定807。当前冻结sf25输入只有单叶，不能据此授多层或并发资格，
更不授新完整视觉profile或整套SemBench通过。

<a id="imlane-branches-groupby"></a>
## IMLane 原分支 Join 与 groupby

基于4073f0a5e只补既有完整query测试，生产runner无需修改。两项新方法通过后，整合独立执行受其RNG／比较
helper影响的原题模块8／8通过、0skip，结果SHA为`bd45811b0e6a1c51f5426e466986bc649672249caca72dc2a3e50ddf254b1cf5`；前110与本8有重叠，不能相加为118项资格。

实际原Q10三批各5的Filter后两批各2的Join；原Q11四批各5的Filter后两分支与最终Join的批次为2／1／2。
完整结果、原评分及消息multiset与原服务软件参考一致；SDK未将多列声明全入模的事实继续披露。
原Q14运行真实SDK ThreadPoolExecutor的4个group线程，两实callback在fixture barrier相遇，未替换执行器或线程容量；
SQLPredictions作用域最大同时1，实际LM批次5／8／1／1／1／1、永久attempt1..17连续，provider路径／call_id唯一。
每批sink→ACK→driver退出及SQL scope关闭、完整原sink／最终模型关闭顺序通过。

两次对照前记录／回设实际NumPy RNG快照，不改生产算法。Q14结果为原query返回的id列且官方指标为F1；
按原GenericRunner不写pandas index的关系值／重复次数和原评分比较，实际原index仍保留，不拿线程结束顺序当语义。
Q10／Q11及所有旧case保持精确完整结果断言。组内模型错误经实际worker/provider逻辑触发ModelBatchError，
所有SQL scope关闭、最终模型只关闭一次，失败LM批次保留，不产完整sink或成功quality。
数据、renderer／generate与SQL仍是明确软件fixture；没有真实模型／SQL／GPU，不授予并发SQL、多Job或目标性能结论。

<a id="imlane-original-input-kinds"></a>
## IMLane 原题输入类别的软件覆盖

基于43d6fe0b1只补同一完整原题模块，生产代码未改。ROOT从干净Git导出叠加唯一测试文件，
独立运行整份模块12／12通过、0skip，结果SHA为`da60d9ac22c3ca338db4a10b2a8e444093604289e2c8bbfd25760f5f76a1b565`。
其中前8项与上一批重叠，不能累加为20或把旧110再加12；本批0真实模型／SQL／GPU。

使用既有临时输入及明确软件SQL／renderer／generate，原函数、SDK、Arrow、provider、ModelWorker和ACK实际执行。
新增14个原题case中，13个完整结果／评分与原服务软件参考一致；E-Commerce Q4保留5个重复发生项，
完整5行JSON Extract相同后，两边原GenericEvaluator的ARI均抛同一ValueError，按失败对照验收而非成功评分。

| 输入类别 | 实际观察及保留的原行为 |
|---|---|
| Animals Q3/Q10、MMQA Q2a/Q7 | 四题均实际4次请求；Animals原gold SQL无ORDER BY，只对并列金标比较multiset，原结果和指标断言保持。 |
| Cars Q3/Q8，整数scale7 | 原连接后分别14／15次请求，完整sink为10／2行，LIMIT不减少前置请求。 |
| Medical Q3/Q9，整数scale29 | 分别7／10次请求，Q9每个发生项保留两张图；原完整结果／评分相同。 |
| E-Commerce Q2/Q4/Q6/Q8/Q9/Q13 | 单算子、描述Join及图像self-join共用原factory；Q6实际2次，Q9保留全部25对及每对两图。Q8临时合法长描述使真实Join非空，原SDK多列选列限制不改。Q4原评分异常如上。 |
| Medical Q8完整输入与原失败 | 小gold在完整sink之后保留原采样ValueError；足量gold输入实际104次请求后保留原LIMIT100及原评分，不裁掉第101次之后输入。 |
| 空关系与缺图 | Medical Q9无匹配保留空sink；E-Commerce Q8实际0次、总数预期None，原runner仍保守分配1个owner，如实记录，不把未知写成0。缺Cars图在模型／SQL／输出创建前报相同FileNotFoundError。 |

测试helper仅把Path转为字符串，scale_factor保留整数，allowance原样传两侧；没有修改原数据定义、
原查询、原评分器或生产算法。首轮E-Commerce Q4原评分错误保留，随后加入完整sink／原异常／失败状态和关闭的共同对照，
没有删重复行或改gold凑成功。真实资产、Linux作者SQL／DSO／IPC／物理清理、VLM／独立processor参考及性能仍待验。

<a id="kalypso-3500-single-reduction"></a>
## Kalypso 原Q5 单次真实归约及环境缺口

`kalypso-aggregate-3500ef7-native-qualification-20261113-r1`固定3500ef740／作者05024，
20／20 CPU及实际作者planner／初始完整token门通过，原sf25源25行选16发生项；一次真实SemAgg归约终态，
未知／重试／额外语义预热0，原F1=1。它不同于LOTUS层次算法，只授予单层功能，不授多层／并发或真暖。

Qwen2.5-7B-Instruct BF16、原作者vLLM0.13.0rc4分支、原引擎defaults：context32768、max_num_seqs256、
batch tokens2048、GPU利用率配置0.9、prefix cache开启；SemAgg并发8、max_tokens8192，原seed42／
frequency0.5／repetition1.3保持，未为统一而换成LOTUS采样。逻辑APIprompt3552／completion5，金额及kernel未知。
独立kalypso.source_to_sink.v1的ready源到完整sink0.943170746秒，owner含启动57.489038759秒，
模型服务启动40.207198926秒、额外source preflight14.48238821秒分列，未扣减旧成绩，不与其他独立时钟排名。

首次私有owner调用CellBudgetLedger构造器时文件尚未初始化，0次调用且未创建model/client；按原接口
create＋reserve_unit后在新输出slot、相同15永久上界执行，源码／生成不变。不是生产SemAgg初始化缺陷，
不应添加“文件没有就自动创建／重置”回退。原失败、未消费14预留及全部终态证据保留。
另外HF offline已设置但私有owner缺local price-map配置／严格INET guard，LiteLLM价格表联网超时；
全离线资格明确失败，真实价格不可用，不以功能成功掩盖。后续owner应设已有LITELLM_LOCAL_MODEL_COST_MAP=True
及现有外网guard，不重复此模型运行来抹失败。

资源／租约清理通过。1368份payload加清单与归档逐字节核对，1144个source Git blob由本会话复核等于
准确3500提交，未重跑模型。冻结原件及失败不改成当前新组合或正式性能资格。

<a id="imlane-csv-record-envelope"></a>
## IMLane prepared CSV 的实际记录边界

协调回执`imlane-visual-q7-4073f0a-native-qualification-20261113-r1`固定4073f0a5e：
110CPU与128条原像素／native CPU消息门通过，但真实作者SQL在prediction前读取CSV失败；
默认maximum_line_size=2097152字节，小于实际2293907字节。软件generate及真实模型调用均0，完整sink未到达。
当时本会话只核对本地BLOCKER／HANDOFF／FREEZE及关键文件SHA；完整原件随后已无损恢复并逐件核验，
见[冻结Q7回执](#q7-ray-imlane-frozen-model)。首次私有guard失败、SQL失败和原预算保持，不能把旧4073记为修复后通过。

固定作者072f的7件CSV源码Git blob／SHA已独立核对：0不表示无限，行长比较无该特例，
原错误建议实际长度加1；reader buffer另有独立默认值。修复仅在现有sembench.cpp薄driver顺序测量CSV最大记录的真实字节数，
将该值加1传给maximum_line_size；按既有csv.writer双引号合同保留引号内LF，不解析字段／物化消息／缓存整文件，
也不按整文件大小放大buffer。报告input_csv保存实际record字节及reader值，无题号／图像或任意更大常量。

完整source→sink包含这次额外顺序扫描，原native_projection_seconds起止、prediction SQL、Arrow／UDF／IPC／ACK、
原图尺寸、原消息、原批次及模型配置不变，没有估算扣减。旧build record因源码不符仍被拒绝，须仅重链薄driver，保留原DSO身份。
同次窄范围只读核查未在后续uint64 wire帧／Arrow消息解码发现另一个固定字节cap；这不是实际大消息SQL／模型资格。

ROOT从干净aab09192f导出叠加唯一C++及测试文件，独立9／9通过、0skip，结果SHA为
`a650ee093a3421fb1bfae841092c28ef6d527854567477a360ecd416fd48aeda`。
测试编译实际标准C++ helper／reader表达式，以本机DuckDB2.0.0复现2293907字节记录的旧默认失败，再原样读回；
同时覆盖UTF-8、引号／多行ID、尾行、很多短行、空输入、坏CSV与既有3项build身份行为。
图像URI在此只是CSV字节fixture，不授予图像解码或VLM资格。首轮Mac ifstream将目录视为EOF的fixture假设失败保留；
随后要求测量报错或由原DuckDB拒绝非法source，未为测试添加生产兜底。

本地未编译完整Linux driver、未重编DSO、未执行原作者SQL／IPC／模型／GPU。
后续原测试会话已在a110完成薄driver重链、原SQL／IPC／ACK及sf64两阶段VLM，见[准确回执](#q7-ray-imlane-frozen-model)。
没有缩图、改源或拆批绕过；该冻结Q7的执行资格不授予其他题目或整套接入。

<a id="kalypso-query-scope-clock"></a>
## Kalypso 原题范围与独立时钟读取

固定05024的SemanticPlan支持多算子；当前项目仍按单handler绑定。固定c814的Movie Q4只调用一次
语义Filter，前置id筛选及ratio是普通关系处理，不能为凑多阶段改写原题。当前维护纯文本分支没有连续
两次受支持算子的实际需求；真实Q2b／Q12还缺图像／Extract接线。已复核22件原题、项目和作者Git blob／SHA：
作者semantic-query数据源只包装文本，planner无sem_extract、chat builder无JSON格式透传；同一底层vLLM却有图像／JSON API。
因此只记录本前端缺口，不宣称整个系统不支持，也不提前添加阶段框架。未来真实接线须保留原中间关系与阶段身份，
逐原生子查询不能冒称作者跨阶段pipeline／KV复用。

另复现一个现有共同读取缺陷：聚合runner已输出`kalypso.source_to_sink.v1`，公共query_timing却将其当未知版本，
真实耗时读成None。现在只将该准确版本加入已知receipt，保留其source起点与独立身份；其他未知版本仍不可用，
旧v2／v3规则与receipt保持。五handler及Movie records/prefix同时停止新写“统一v3”标签，使用现有独立Kalypso版本。
实际计时起止、提示／算法／采样／结果／服务关闭没有改变，查询内同步observer／指纹／回执仍披露，不能据此排位或改写历史成绩。

先以新增行为用例复现“None而非真实耗时”的失败，再从干净fc16导出叠加10个源／测试文件，执行8个受影响模块：
76项中74通过、2项因真实tokenizer未绑定跳过，0模型／真实服务／GPU。结果SHA为
`2c7dc492341dabed494c7bbad6056efc05af8d345a29137bfe74ee48cc57b102`；
覆盖五handler、records/prefix、完整sink后审计失败及旧版本／未知／非法时序，原缺陷输出保留。
这是时钟身份和读取修复，不增加多阶段、图像或新模型资格，冻结3500功能与离线失败仍按原版本解释。

<a id="independent-native-filter-runners"></a>
## 独立原生 Ray／Daft 的同类 Filter runner

实现冻结为`75837115092401284fb607532891b8160cd5e600`，配置模板与两系统runner同时提交。
依据用户要求，将独立原生系统与LOTUS＋后端分开：新`sembench_native_text.MovieFilterTask`只复用
固定c814的题目目标、原源数据和评分，不导入LOTUS SDK、formatter或parser。
它用项目独立JSON布尔提示／严格解析绑定Movie Q1–Q4；Q1全源、Q2–Q4先按原id筛选，
实际合格行均完成Filter，Q1/Q2再取源顺序head5，Q3完整COUNT，Q4保留原分母及空结果行为。
重复reviewId仍按源发生位置请求，标签只进入事后评分。原空gold的NaN质量不改成零误差。
这是“统一语义工程对照”（`semloom.native_text_filter.json_bool.v1`），不是作者原版或官方系统runner。
固定c814相关7件原源码Git blob已核对。

每个系统一个runner：`sembench_native_ray`调用现有公开Ray LLM processor；`sembench_native_daft`调用
内置vllm-prefix-caching图，复用既有隔离worker／监督／READY→sink→release ACK。
两者不复制逐题执行器，不改原生批处理／调度。Ray引用外层已预留永久预算，Daft在本查询私有目录
绑定有限预算；每次实际委托前扣除，失败不退还。显式生成参数须声明单输出和本任务JSON-object。
check／无执行标志run仅作CPU检查，不claim预算、创建输出或启动模型。

fresh CSV、任务消息、原生冷启动／预测、严格解析及完整内存sink在独立source→sink内；Daft还包含
实际chat template、packet及IPC，两者已披露的同步观测I/O仍在内。最终关闭、源身份核对、原评分和
driver结果持久化在后，不能与ready后查询直接排名。完整sink已完成而解析以外的清理、评分或写盘失败时，
保留结果、原异常、已消耗预算并撤下成功质量标签。审查补齐Ray后验写盘失败、Daft最终报告失败行为，
没有增加新的持久化重试或错误恢复框架。

隔离干净导出组合初轮46／46；新增Ray CLI与显式生成合同后，只复核受影响Ray模块10／10。
去重为48个不同方法全部通过、0skip／模型／GPU，不是56项；单独任务层12项和早期分任务检查不再累加。
去重来源摘要SHA-256为`b865397a39db3ebffa6d8c8e6fc2f5434faa92da1078fdf81883b32e7e02c132`。
覆盖四题原评分、重复／缺失／未知答复、空输入、完整sink与关闭顺序、失败留证及fresh进程禁止LOTUS导入。
Ray使用公开图软件替身，Daft使用真实子进程／ACK加软件图；它们不是目标框架整图或真实模型资格。
Daft首轮默认host监督受本机sysctl沙箱限制，模型0；原失败保留，fixture改用同现有测试的有界已知子进程，
生产supervisor不改。新语义的Linux原生框架／模型、质量、物理退出与性能均pending。
现有Ray HTTP Q3因外部模型所有权保留，元数据更正为LOTUS-compatible路径；其旧默认True parser
仍按原来源解释，不重复计入新独立语义baseline，也未借此次修改删除尚有用途的对照。

<a id="lotus-daft-attached-visual"></a>
## LOTUS＋Daft 的内置 OpenAI 图像接点

实现冻结为`f029bb5441f6138d76b749278aca979847d80f2e`，沿前一提交不改变独立语义runner。
固定Daft0.7.21的原生`OpenAIProvider`＋`prompt(list[Expression])`可表达原查询所需的
固定system＋单user内容数组。新接点只将文字／PNG data URI映射为有类型原生列，保留原内容和顺序；
原LOTUS查询、提示、parser、JSON阶段、评分及最终关系仍由同一完整query runner拥有。
这属于LOTUS＋Daft，不能改称上节独立原生语义路径。没有新增provider、Prompter或执行调度器。
每批沿现有worker／ACK；外部模型不由worker关闭，实际READY、启动、显式SDK client close均未观测，
worker退出只证明本地隔离进程结束。多轮、字符串user、额外detail和未验证消息形态不作静默转换；
原Q12混合文本阶段、分组并发及取消的TopK保持未接，不通过其他实现恢复取消动作。

先以真实Mac arm64 Daft0.7.21原生表达式／执行图加软件AsyncOpenAI，比较原始Animals单图4、
Medical双图10以及MMQA Q2b Join4→Extract4四批：消息／图像字节和完整原结果一致，
重复消息保留不同call_id，仅Extract透传JSON-object。没有访问模型或外部API。
该原生图证明摘要SHA-256为`da928e15fea79bc9d4f42b4721870f5ab624e915fb3ab083d6ea4fe067302d89`。
首轮guard拦截urllib3的IPv6能力探测socket构造，实际socket尚未创建、无bind／connect；原输出与源码定位保留，
未为绕过检查重跑原题。SDK软件client共创建6个、GC后均销毁，显式close为0，不能据此声称transport正常关闭。

HTTP观察复用现有observer，发送前永久占额，SDK重试仍由原生配置拥有；HTTP receipt只按原消息整批核对，
逐行结果关联由Daft call_id列拥有，不能声称重复消息的HTTP答复已经逐行绑定。
endpoint／key只引用环境变量名，不进入packet；外部profile拒绝同时声明本地engine构造。
独立验证五个受影响模块共47个不同方法：首轮46通过、1个fixture读取错误报告字段失败，
改用实际`physical_attempts_reserved`字段后只复核该项，通过；生产代码没有为此失败改动。
去重证明SHA-256为`bb93b971e74d22bf4fa173712d87f4d0b9da5db8b6ad0e87f1fb26730e88497d`。
覆盖旧字符串图／worker／完整查询回归、新真实Daft图、软件子进程HTTP观察、原Q7及非空Q2b完整sink／ACK／原评分，
无skip／模型／GPU。真实兼容服务、模型、并发和物理资源资格pending，不能借用旧本地vllm成绩。

<a id="q7-ray-imlane-frozen-model"></a>
## 冻结 Ray／IMLane Animals Q7 sf64 两阶段模型回执

原服务器协调会话分别提供`ray-animals-q7-4073f0a-full-query-20261113-r1`与
`imlane-visual-q7-a1105-native-qualification-20261113-r1`。本接入会话只恢复核验原件，没有复跑模型。
两条均为LOTUS完整原题和原评分，各64源行、两阶段共128次终态，unknown／retry为0，最终City precision／recall／F1均1。
这是原sf64，不是Animals全8718图、全部视觉题或新独立原生JSON语义runner资格。

| 冻结路径 | 完整查询区间 | 启动与真实执行条件 | 有效范围 |
|---|---:|---|---|
| LOTUS＋Ray，4073f0a5e | `lotus_ray.source_to_sink.v1` 229.928207925秒 | 包含原生懒启动；两个实际模型owner，vLLM0.22；不是同owner暖轮 | 两阶段原消息／终态／完整sink已验；原外层清理失败保留，精确资源恢复后监督通过 |
| LOTUS＋IMLane工程版，a1105b723 | `lotus_imlane.source_to_sink.v1` 97.176916763秒 | 模型就绪后fresh source；启动72.99360989872366秒另列；两批同一Qwen2-VL-2B-Instruct BF16，revision `895c3a49bc3fa70a340399125c650a463535e71c`，vLLM0.25.1 | 薄driver重链，原DSO未重编，Linux SQL／IPC／ACK及原图VLM通过，物理资源／租约清理完成 |

两者环境、模型生命周期和起点不同，不能按这两项数值排名。IMLane保留原max_tokens8192、temperature0、
top_p1、seed0、空stop与原惩罚参数；未使用事后估算扣减。CSV扫描仍在查询内，
两批最大记录分别2380577／2380579字节，对应reader2380578／2380580，未缩图或拆批绕过。
a110的9项Linux读取／build检查通过，旧build仍被拒绝；旧4073在prediction前CSV失败的0模型记录保持。

128条原消息及输入token数组跨路径相同，126条原始文本答复相同；第一阶段发生位置2／14的True／False不同，
最终City仍相同。没有从这一现象推断因果或逐项完全等价；IMLane输出token ID数组未观测，不填空数组或零。
两份无损参考恢复包绑定同一已核64图原消息参考，接入owner逐件核编码／恢复字节、清单、归档和准确Git源码：
Ray包1694个payload含Ray383及旧失败IMLane4073的1311；对应4073源码1146个blob。
a110包1492个payload、恢复2643558317字节，对应源码1147个blob。归档身份见下表；
模型结果只归各自冻结版本，不转授当前新增Daft接点或独立Ray／Daft语义runner。

<a id="imlane-shared-task-profile"></a>
## IMLane 共享任务配置与旧 Q1 分支退出

在`9486be0401b48e906aa0204f8c3d5f84a6abcd44`基础上复核现行调用：Q1模板已使用
`semloom.imlane.prepared_text.v1`，旧专用schema只在worker fixture和历史服务器qualification材料中保留。
移除生产的旧Q1 schema、默认Q1 task及固定25次／8192输出校验；仓库worker fixture迁移到显式共享task。
Map／Extract预算从“必须等于N”改为“有限且至少覆盖N”，实际请求、原消息、JSON阶段、评分、SQL／ACK及计时不变。
这是共享配置与上限语义修正，不是为另一题新增runner；预算大于N不产生额外调用，不足N仍在模型启动前拒绝。

干净9486导出只叠加生产模块和两份受影响测试，26个不同软件方法全部通过、0skip／模型／GPU／原DSO执行。
涵盖Map／Extract原SDK完整关系差分、新非25规模／预算余量、旧schema与缺task拒绝、完整sink后失败留证，
以及既有完整视觉／分支／groupby和单层／多层聚合的软件回归。
结果摘要SHA-256为`f31c8dd3c82272db0acb7e7f01b156b2a1ffbd60db300b46de74fea600847384`。
旧服务器helper属于冻结运行，未覆盖或重新执行；新候选必须使用显式task。
准确恢复树和迁移说明见[退役来源](../../../reproduction/native_baselines/README.md#retired-imlane-q1-schema)。
本批不修改build输入／DSO，不增加新的Linux SQL或模型资格。

<a id="independent-native-map-runners"></a>
## 独立原生文本 Map 复用同一系统 runner

从`e3ad235080bbb875eafbd632da31b93989a17ec0`继续扩展任务层，不增加执行器：
`movie_text_task`工厂保留原Q1–Q4 Filter，增加同一`MovieMapTask`的Q8分类与显式Q9评分Map。
Ray／Daft仍各用原runner、公开native graph、预算及原结果交接／关闭；配置只选择query及Q9所需`ranking="map"`。
Q9缺少ranking、选择TopK或给其他题ranking在执行前拒绝，未恢复取消的TopK动作。

固定c814的Q8原目标为taken_3完整情感分类计数；保留value_counts、缺类别补零及按scoreSentiment排序。
Q9 Map原目标为指定Ant-Man电影的完整reviewId／reviewScore关系；保留原顺序、重复发生项、
float转换、合法1–5小数与非数／越界默认3.0的普通后处理。空Q9仍返回原两列object类型表，不造模型请求。
项目提示与JSON解析独立：Q8单sentiment键，Q9单score键；坏JSON、重复／额外键失败，
已抽取score再进入原普通数值后处理。提示profile分别为`semloom.native_text_map.sentiment_json.v1`与
`semloom.native_text_map.score_json.v1`，不是原LOTUS提示／parser或作者原版算法。
原数据的scoreSentiment／originalScore只供官方评分；缺必要列在模型前失败，标签与ID不进入消息。
既有Filter提示／实现不改，原源快照、完整sink、失败留证、预算及计时仍保留。

两个runner的顶层身份收敛为`ray-native-project-json`／`daft-native-project-json`，具体语义仍取task的prompt_profile，
不将旧Boolean或LOTUS目标证据转授Map。只读复核另确认Ray零发生项不启动runtime／模型，
其ready字段改为不适用并记录原因，零调用不虚报模型就绪；源起止、结果与额度不变。

干净e3ad导出叠加六个源／测试文件，三个完整受影响模块45个不同方法全部通过、0skip／模型／GPU。
结果SHA-256为`3e48ef09cfdf07f735533bdaf6160620bd18d5fe73e00cf10ab4044ea7f13fd1`。
覆盖新Map原评分、补零／空表schema与dtype、合法小数／默认3、重复／缺失答复、坏JSON、缺金标列、
两系统完整sink／ACK／失败收尾、CLI分支及fresh进程禁止LOTUS导入；旧Filter回归包含在45项，不累加前批计数。
分任务初始新增API缺失的红输出、原SciPy评分`0.9999999999999999`与1精确断言失败保留；
后者只改fixture的浮点比较，原评分不变。分任务4项／22项与本组合重叠，不再相加。
没有运行实际Ray／Daft模型图或新模型；Map目标质量、物理清理、完整Q9输入与公平性能均pending。

<a id="independent-native-q10-runners"></a>
## 独立原生评分 Map 的完整分组结果

基于`7fb4adca49c0d6fcdfabf6787f31c1eef4aa9382`，同一`MovieMapTask`增加原Movie Q10：
全部Reviews物理行逐条评分，保留原1–5合法小数／默认3、pandas默认groupby(id)、Python sum/len及round(mean,2)。
重复reviewId不去重，缺id的行仍完成Map后才按原groupby丢出分组；空源保留object两列，
非空全空id保留原零列结果，不为统一schema修饰原行为。两系统沿原runner／原生图／预算／ACK，未增加Q10执行器。

配置显式给`movies_csv`，只供原Q10金标。两类task统一增加`prepare_evaluation()`：旧题无额外gold，
Q10在模型启动、预算claim及source计时前读取并校验Movies.csv的id/audienceScore，冻结原字节与DataFrame。
sink后原评价只使用这份快照，回执附gold字节SHA；路径后来变化不改变本轮gold，不二次读盘或静默回退。
构造不读输入，真实Reviews读取、全部评分、分组与完整内存sink仍在查询内；gold不进入消息或查询计时。
缺gold配置／文件／列在模型前失败，check也检查gold但不占预算／启动worker，未新增预检或缓存框架。

干净7fb4导出只叠六个源／测试文件，三个完整受影响模块56个不同方法全部通过、0skip／模型／GPU。
结果SHA-256为`5edd82baedd7ef7f9b9b79148ce2fa45bcd8e4a7c2e507c368d573b5e0938316`。
包括既有45项与新增Q10：多电影／乱序／重复发生项、null id、合法小数／默认3、两位舍入、原空表、
真实官方gold SQL／评分、gold冻结后路径变化、fresh进程禁止LOTUS导入及两系统完整sink／失败留证。
分任务29项／4项均包含在组合中，不累加。真实框架整图、完整Q10模型、质量／物理退出与公平性能pending，
不借用LOTUS＋后端的旧Q10成绩。

<a id="lotus-a1105-q8-q10-model"></a>
## a1105 原 LOTUS Q8 常驻两轮与 Animals Q10 完整查询

两项冻结在`a1105b72357e89da2f4609a3aaeb0ff7b8679237`、SemBench c814及LOTUS1.2.4，
属于原LOTUS语义，不能授予新独立JSON工程对照模型资格。本接入会话及只读核验任务没有复跑模型或评分；
两归档原字节SHA、全部payload／恢复字节与各1147个准确源码Git blob已核对。
紧凑私有核验摘要SHA-256为`f627f15945b220e9f60875f36e625aa9aa443d8419381ada64cd41fa95ccf4fc`，
归档身份见下表，按运行标识从原服务器协调会话获取。

| 冻结运行 | 真实工作与原结果 | 查询时钟与启动 | 缓存及资格 |
|---|---|---|---|
| `lotus-movie-q8-a1105-resident-qualification-20261113-r1` | 原2000×11源、每轮taken_3完整120次；两轮共240终态，unknown／retry／额外推理warmup为0；两轮NEG105／POS15，原gold106／14，原relative_error0.04043126684636118 | 原`sembench.source_to_sink.v3` ready-first1.941388724秒、同owner repeat0.713572857秒；启动53.797202093秒、关闭5.417008606秒另计 | result cache关闭，native prefix cache开启；同model进程／engine UID／birth，120条原答复及完整输出token数组逐项相同；一次首／暖功能资格，非正式排名 |
| `lotus-animals-q10-a1105-full-query-20261113-r1` | 官方sf64完整64行一次Image Filter，再原City／StationID分组／排序／head1；64推理POST成功、unknown／unfinished／retry／额外推理warmup为0；原precision／recall／F1均1 | 独立`lotus_service.source_to_sink.v1` ready-first35.544935968秒；启动66.080541646秒、tokenize／ready门5.980956013秒、controller→sink121.899902351秒另列 | result／prefix cache均关闭、图像processor cache0；未跑warm，不作跨系统性能比较或全Animals资格 |

Q8使用Qwen2.5-7B-Instruct BF16、revision `a09a35458c702b33eeacc393d103063234e8bc28`、vLLM0.25.1；
原生成8192 token、temperature0／top_p1／seed0等声明保留。每轮逻辑API usage为14206／360／14566。
两轮包含同一源位置79／116的重复消息，各自发生项均执行；按原SDK engine_request_id关联240条源、
native消息指纹、完整输入／输出token数组，不将source ID、transport ID或到达顺序当同一命名空间。
实际OMP线程为1，旧6c为8；相同engine容量不代表运行校准签名相同。原生MFU仅为scheduler解析估计，
kernel、纯GPU时长、成本及独立逐请求SamplingParams读回仍未观测。

Q8的v3标签不表示内部观测全部排除：冻结resident在原后处理前深拷贝中间relation，
F/M/E stage的完整binding.report统计也在原sink前；主要source回放、评分与落盘在后。
原数值不扣除估算成本，不把这两轮当跨路径统一计时或前缀缓存收益的正式结论。
首次CPU fixture误用transport到达顺序关联source，0真实模型、120软件尝试不退还；
只修私有映射后新预算软件两轮通过，原失败保留。

Animals Q10使用Qwen2-VL-2B-Instruct BF16、revision `895c3a49bc3fa70a340399125c650a463535e71c`、vLLM0.25.1，
原LM batch4及生成配置保留。64次额外tokenize POST是模型前CPU元数据检查，不算64次推理warmup。
完整input signatures、服务tokenizer数组、API usage及completionID发生项关联已核；output token ID数组、
GPU像素tensor、独立generate／kernel、真实SamplingParams读回、成本未观测，不填零。
原gold SQL有三组并列最大City／StationID且无ORDER BY，原查询head1选其中一组，
c814原评价允许该合法最大结果获得F1=1；不把它解读为逐图全部正确或漏掉金标。
首次CPU旧私有helper导入失败、首次audit对无序gold做整dict比较失败保持；
后续仅修私有核对，不改query／模型原答／金标／评分。两项模型、事后audit和归档阶段的owned进程、
GPU空闲、socket／SHM及租约清理均有实际通过回执。

<a id="native-join-resident-accounting"></a>
## 独立 exact pair Join 与 resident 统计后移

在`f83b2cc9ae292a202b36f0094fbcaf9e0ab3b25c`干净导出上整合两项互不覆盖的修改。
独立Ray／Daft的同一任务工厂增加`MovieJoinTask`，覆盖原Movie Q5同情感、Q6/Q7相反情感。
所有题都筛选原Ant-Man电影关系，先完成left-major／right-minor的N²有向配对，含self与重复ID；
只在全部预测后按True序列重建Join索引、移除相同reviewId，Q5/Q6再head10，Q7完整返回。
方向、重复与索引空洞保持；非空实际三列，空Q5为原五列object表、Q6/Q7为原零列表，
不按原docstring的错误列名或金标set去重改执行。原评分可无向化／去重，这不减少模型工作。

新`model_request_count(source)`只做N或N²整数运算，不构造消息、不计算SHA；两个run与check共用，
在消息展开前按本次鲜读源核有限预算。N=1仍需一次请求，不用N(N−1)、唯一文本或LIMIT代替。
pair消息只含左右reviewText，沿项目严格JSON keep布尔解析；原题源／关系／评分保留，
方法身份明确为`project exact Cartesian scalar pair predicate`，不是作者级Native Join或LOTUS Join。
实际生成、批处理及调度仍由原生API拥有，没有新executor／调度器或逐题runner。

LOTUS resident的F/M/E复用既有Join延迟记账模式：完整binding／LM／ready快照统计在full sink后只汇总一次；
真实请求身份、预算与READY观察时点不移。默认cold及其他borrowed stage调用保留同步合同。
原后处理失败也在owner close前记账；统计失败仍保存已完成结果，原异常及close阶段晚到成本继续留证，
私有binding／LM引用在序列化前移除，不跨下一query悬挂。
中间关系改为`copy(deep=False)`只冻结schema，不复制整表值缓冲区。
已查固定resident可达原题无既有值缓冲区的原地写；E-Commerce Q7真实原函数新增id列、
MMQA Q4对另造分组表rename，Movie评分构造新结果，均不需要深复制。真实Q7测试核旧schema与shares_memory，未改COW策略。
必要provider记账、轻快照与SDK本身工作仍在区间，不能称完全无观测成本；旧a110时长不扣减、不回填。

最终十个源／测试文件叠加后的五个完整模块115个不同软件方法全部通过、0skip／模型／GPU。
其中独立任务／Ray／Daft73项，resident stage／driver42项；包括既有回归，不与分任务40／6／42或前56项累加。
结果SHA-256为`af62265073aa4236cf6b4ece17cb565ec2d5f8342e468e803dc024bc0951fdc1`。
Join首个新fixture误读原metric键f1而非f1_score，修测试后原评分不变；resident首次参数顺序断言
在base也复现，只修为核所需CLI标志／真实data-dir，不改CLI迎合测试，首次输出均保留。
新Join模型与当前计时路径的Linux／模型／真实秒数尚未验，不转授7fb4 Q8或旧a110成绩。

后续完整原函数差分发现独立Filter Q1/Q2的全阴性空结果漏了movieId、reviewText两列；
现恢复原三列object空表，非空RangeIndex及其他查询不变。共同差分对固定c814完整原函数和
真实LOTUS SDK提供软件答复，覆盖Q1–Q10的空源、重复与索引、全阴性及无候选四类输入，
Q9显式选Map；40个子例核列、dtype、行、索引及重复全部一致。首次6个失败均为上述空表差异，原输出保留。
SDK只在该参考测试内部加载，独立生产路径与四项fresh-process禁LOTUS检查保持。
主线程在干净导出上复核任务层与两个runner共74项全部通过、0skip／模型／GPU；结果SHA-256为`b28a6a7f30188c1778e0239573d031423bae684f9629dc47ea00bbe4a54d97b8`。
这验证普通关系结果合同，不表示JSON提示等价于LOTUS或新增模型质量资格。

<a id="native-q8-7fb4-model"></a>
## 7fb4 独立原生 Ray／Daft Q8 真实模型

运行`native-ray-daft-q8-7fb4ad-qualification-20261113-r1`冻结在
`7fb4adca49c0d6fcdfabf6787f31c1eef4aa9382`。两边使用同一原2000×11 CSV与完整120个taken_3发生项，
119个唯一消息仍执行120次；项目提示profile为`semloom.native_text_map.sentiment_json.v1`，
无LOTUS提示／parser依赖，身份是“统一JSON工程对照”。各120实际native终态、unknown／retry／额外推理warmup为0，
永久预算均完整扣除并关闭；原始生成JSON文本及解析sentiment逐行120／120相同。
两边结果均NEG107／POS13，原gold106／14，官方relative_error0.04043126684636118；
额外逐行诊断正确113／120，计数误差不能当分类准确度。它与上节LOTUS Q8的105／15是不同语义来源，不能因相同误差合为同一结果。

| 冻结原生路径 | cold fresh source→完整内存sink | 原生版本与口径 |
|---|---:|---|
| Ray Data | 78.597339925秒，`native_ray.source_to_sink.v1` | Ray2.56.1／vLLM0.22.0；实际原生启动、必要读取、预算及同步observer写入在内 |
| Daft | 59.122346378秒，`native_daft.source_to_sink.v1` | Daft0.7.21／vLLM0.25.1；实际原生启动、chat template、packet／digest／IPC及native写入在内 |

两边均绑定Qwen2.5-7B-Instruct同一权重revision `a09a35458c702b33eeacc393d103063234e8bc28`，
BF16／context32768／prefix cache开启已有相应实际回执。模型从已核本地文件加载，原生revision实参仍为None，
不冒称远程revision参数被显式传入。声明生成n1／8192 token／temperature0／top_p1／seed0／空stop及原惩罚参数保留；
逻辑API usage两边均14315／1080／15395。各自原生processor／容量与线程观察范围保留，不能声称超出已测字段的严格配置身份。
Ray保存实际完整输入／输出token数组；Daft保存实际prompt SHA、token长度及输出SHA而未保存完整数组，
不能由原文相同推断两边token IDs完全相同。kernel、纯GPU时长、成本仍未知。
外层监督总时长、sink后关闭及审计单列，没有相减构造统一成绩；本轮没有warm或正式性能排名。

私有归档1669个payload与1158源码Git blob、3个c814参考源码及执行packet／bundle均逐件核验，
本接入会话没有复跑模型或评分。核验摘要SHA-256为`817b68f83153a7e5975d38a2885f091bdac8aa4a7ca6c896d30a4b7034c3b6c1`。
首次CPU五项未绑定／私有序列化、SDK builder误触runtime初始化保护、bundle及两次archive失败原件保持；
只复核缺项，最终45个不同CPU方法通过。query内部physical exit仍不明，独立host guard补证owned进程、
GPU空闲、socket／SHM及租约释放通过；这些结果不授予Join、Q10或后续候选整体模型资格。

<a id="native-q10-f83b-model"></a>
## f83b 独立原生 Ray／Daft Q10 全源分组

具名运行`native-ray-daft-q10-f83b2c-qualification-20261113-r1`固定在
`f83b2cc9ae292a202b36f0094fbcaf9e0ab3b25c`及原c814查询／评分，沿同一独立JSON评分Map。
Reviews原2000行／11列，所有2000发生项逐条推理，不以1864个不同消息去重，最后按原规则得到105组。
Movies原116行作为计时前冻结的gold，仅事后评分；原evaluator按有效ID交集105组评价，不能把116当源分组数。
两系统各2000次实际终态，永久账本closed、无退款、无未知／重试／额外查询预热；
实际原始答复逐组核完整均值／两位舍入，不重跑模型或评分。原Spearman为Ray
`0.4083424225290053`、Daft `0.4068781478274947`，Kendall分别`0.2773157705052932`、`0.27392553979454326`。
1973／2000原始JSON字符串相同，27条score及19组均值不同；不声称结果等价，也不外推其他题。

Ray的`native_ray.source_to_sink.v1`为`86.528661014`秒，Daft的`native_daft.source_to_sink.v1`
为`71.394418457`秒，均为鲜读到完整内存关系的单冷查询，包含各自原生模型启动。
Ray保留预算／同步observer写盘，Daft保留packet／digest／IPC／原生证据I/O；
sink后关闭分别`1.442937471`／`3.4895760491490364`秒，监督总时长分别`92.13058116380125`／`85.29243813920766`秒，
组件不相减制造暖成绩。当前每查询fresh模型owner，同owner暖轮入口尚未接，复用tokenizer或Python实例不能替代。
固定SDK生命周期核查：[Ray 2.56.1](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/data/_internal/execution/operators/actor_pool_map_operator.py)
为新Dataset执行建actor pool并在结束释放；[Daft 0.7.21](https://github.com/Eventual-Inc/Daft/blob/v0.7.21/src/daft-local-execution/src/streaming_sink/vllm.rs)
的本地VLLMSink新状态创建LocalVLLMExecutor，Session只保存provider配置，当前local路径并非Ray actor。
现profile未找到公开模型owner移交接口；重复collect缓存结果不算鲜读暖查询。
公开Ray Serve deployment或Daft附着OpenAI兼容服务可另立持续模型profile，但会改变执行拓扑，须重新接线与验证，不能继承本轮成绩。

Ray 2.56.1／vLLM 0.22.0与Daft 0.7.21／vLLM 0.25.1的默认容量差异保留；
模型绑定Qwen2.5-7B-Instruct BF16、完整权重manifest与`a09a35458c702b33eeacc393d103063234e8bc28`来源，
本地模型构造revision参数为None，不称远程revision参数已设。两边请求n=1、max_tokens=8192、temperature=0、
top_p=1、seed=0、stop=[]、presence/frequency/repetition为0/0/1及JSON object；结果缓存关闭、prefix开启，
原生初始化和内部预热保留，编译／磁盘缓存未清除。输入2000个原JSON／模板及CPU token数组已核；
Ray实际输入／输出token数组保存，Daft仅有实际prompt SHA／token长度／输出SHA，不能推其完整数组。
权重身份复用Q8完整SHA及未变的inode／size／mtime，小文件重新hash；本轮核验未重传、重hash或加载全部权重。
kernel／纯GPU成本及全actor线程实况未知；此处为统一JSON工程语义功能资格，不是作者原版或性能排名。

56项CPU通过，首次私有控制器r1/r2未执行、r3 SHA匹配后执行；错误SCP目标被自动审查拦截、
随后正确目标成功及额外inactive Ray socket清理均留在原件。最终owned进程、GPU空闲、SHM及租约闭合。
独立核验3407个payload、1158个f83b源码Git对象、4个c814参考、bundle及15个packet成员，
恢复字节一致；核验记录SHA-256为`989b03b7289f4f7333ba43f2360c8999d8ba16071a10e9913a97c073db5f26dd`。
原归档身份见下表，后续代码组合／其他题／暖轮／公平性能仍须自身验证。

<a id="native-mmqa-text-extract"></a>
## 独立原生文本 Extract 复用系统 runner

共同配置工厂默认movie，新增mmqa后按具体任务构造器接收data_dir，Movie的reviews_csv／movies_csv／ranking
继续原合同，不为每题复制执行器。独立`MMQATextExtractTask`Q1/Q4只发送原text列与本项目JSON指令；
单个director／genres字段接受str或null，null明确物化为字符串`None`。提示、严格JSON解析和物化政策是
工程对照，不导入或复制LOTUS SDK／formatter／parser，不声称作者Extract算法。

Q1先对ben_piazza_text_data全部行Extract，再与ben_piazza表left merge、筛Role=Bob Whitewood，
保留原director Series的name／index／dtype及重复、未匹配NaN。Ray改用任务编码合同，
不在查询内转DataFrame；报告保存Series的kind／name／index／data／dtype。
后置原评价复用既有`sembench_mmqa.evaluate_original`桥，在sink外to_frame，保持官方写CSV／读回的列形状。
Q4保留原24片名集合谓词和重复发生项，完整genres逗号切分／strip／展开／默认groupby与title连接，
返回原genre／movies_in_genre列，24是题目条件而非调用数上限。
缺数据／列、无效JSON或发生项不完整均失败；原Q1空text缺director、Q4空frontier缺genre的KeyError及
原NaN评价失败保持，不制造成功空表。gold来自固定checkout的受核查询JSON，模型消息不含标签。

完整鲜读源、Extract与必要merge／筛选／展开／groupby保留在原source→sink，
源身份审计、原评分、结果编码写盘与最终关闭位置沿各系统既有合同，不重估／扣减历史时间。
Daft编码失败继续保存失败报告，有主异常则保留；已完成的内存Series可从异常query_result定位，
不以兜底转型覆盖原结果。Movie既有编码和默认配置兼容，没有新调度、缓存或重试机制。

任务层新增10项与工厂1项、两个runner的最小5项与fresh隔离2项分别通过，分任务计数不累加。
软件参考直接执行固定c814完整原函数与真实LOTUS SDK的受控答复，五场景覆盖Q1重复／null／未匹配、
空table／空text及Q4全部原24片名／重复／null与空frontier；原结果或异常一致。
原评分的重复fixture可得到F1>1，按原实现保留，不作为模型质量成绩。
主线程在608e20干净导出叠加八个源／测试文件，四个完整模块90项全部通过、0skip／模型／GPU，
包括原Movie回归与已有runner测试，不与前74或分任务计数累加。结果SHA-256为`6170c38bf9fd7df4290c77e3bd994185ad184f23a922535e2309a6cf5ad3f9f9`。
真实MMQA Extract模型／质量／物理清理仍待验证，不借用旧LOTUS或Movie模型资格。

<a id="native-q1-f596-model"></a>
## f596 独立原生 Q1 的完整推理与最终结果

`native-ray-daft-q1-f596dd-qualification-20261113-r1`固定
`f596dda765b32bf5e6ca7508b2958d06c48ce442`和c814，原Reviews 2000行／11列，重复发生项保留。
两个原生系统各提交／终态2000、不同call_id及永久预算sequence均完整，closed无退款、未知／重试／额外查询预热为0。
保存的完整预测与实际原生输出逐项对应，最后才按原源顺序取五条；两个head5结果相同，原precision／recall／F1均为1。
这只评价最终五条。额外全2000分类诊断正确Ray 1597、Daft 1593，kept为1108／1106；
不能把head5 F1当作全量分类准确率或两系统逐行等价。这里读取保存的评分，不重跑模型、解析分类或评分。

Ray `native_ray.source_to_sink.v1`为`97.108214333`秒，Daft `native_daft.source_to_sink.v1`为
`66.858933088`秒，纳秒事件差已核；都含原生启动与各自observer或packet／digest／IPC成本。
Daft预检／tokenizer加载在外，两者sink后的关闭、审计、评分／持久化不回填区间。
Ray sink先于shutdown、Daft sink先于ACK释放再关闭均已核；外层最终owned进程、GPU空闲、socket／SHM及租约清理通过，
driver内部physical exit字段仍为None。原生Ray 2.56.1／vLLM 0.22.0和Daft 0.7.21／vLLM 0.25.1及容量差异保留，
prefix／持久编译缓存与原生内部初始化未清除；不称硬件冷、同owner暖、matched性能排名或纯GPU／成本测量。

3464个payload及归档3465个常规成员（另一个为清单）逐件字节／SHA匹配，1158个f596源码blob、
4个c814原查询／评价／gold参考与预算SQLite及关联事件已核。具体首次失败回执在该具名原件中未定位：
6份host FINAL均passed／failure=null，泛化“失败回执保留”标记不能替代具名证据；此缺口保持pending，
不编造首次失败，也不宣称全历史无失败。核验记录SHA-256为
`1041924c070eb2ca0633eff328b50a1df6012b6eb24f4a1e63481eecce35bd40`。
这是原JSON工程对照的Q1单冷功能资格，不授予后续Q1/Q2空表修复、MMQA或新图像路径目标资格。

<a id="native-animals-json-filter"></a>
## 独立原生 Animals Filter 复用 Ray runner

同一任务工厂增加manifest源，单个`AnimalsFilterTask`覆盖原Q1/Q3/Q7/Q10；Ray保持一个系统runner，
以原生PrepareImageStage承载独立JSON keep消息的文本＋PNG内容数组，不导入LOTUS提示或解析。
Q1/Q3/Q10是完整N行zebra Filter后原COUNT、City或City／StationID分组取最大；
Q7的zebra和impala都处理同一完整N行，真实读图／解码／RGB／PNG各做一遍，共2N独立请求，
分别取城市集合后交集排序，空city表保持。2N平铺在一个原生图的批调度不是LOTUS两次顺序LM批次的复现，工程身份分列。

准备接口复用原AnimalsDataset的manifest角色／身份、原图及RGB额外核验，冻结Species供后置原SQL评分；
查询内仍鲜读manifest和各次实际图像bytes、RGB转换／PNG编码，不resize、不拿标签入模。
post-sink核当前身份及实际消费bytes快照，变图后恢复也不能把实际错误输入冒充原件。
重复图片仍是独立发生项，不以64或唯一图片数限制算法；当前官方数据身份仍为原sf64，65项软件fixture不代表全8718图已接或验。

Ray按实际image_url消息要求有效的既有prepare_image_stage，保留原生bool／dict.enabled默认语义；
未启用时在predict前失败，不以模板占位符假装传入像素。配置必须真实绑定VLM及匹配tokenizer／processor；
本批不添加wrapper尚未支持的新MM stage字段。Daft当前本地vllm-prefix字符串profile在check明确拒绝content-parts，
run在任何模板渲染／worker启动前检查实际整批非文本parts，拒绝图像而不丢弃／flatten，
允许纯文本内容数组原样交tokenizer渲染，未为图片边界额外限制文本输入；
另一个LOTUS＋Daft附着服务profile不为本组合授予视觉资格。

完整原函数／真实SDK软件参考核原结果、评分及RGB→PNG消息；Q7第一遍全阴性仍保留第二遍完整N请求，
两遍实际读取快照均检查。首轮fixture未初始化、第二轮原Q10无ORDER BY并列gold导致元数据顺序断言失败均保留，
只修新fixture，不改原查询／评分。最终组合114项结果见[下节](#native-mmqa-filter-family)；新Ray VLM、processor像素／token及物理清理、warm／公平性能仍待验。

<a id="native-mmqa-filter-family"></a>
## MMQA 完整文本 Filter 家族

同一MMQA任务模块新增一个`MMQATextFilterTask`和小型家族工厂，共用原有两系统runner、
独立JSON keep提示骨架／严格关联、源快照和官方评价。Q3a–g完整读取lizzy_caplan_text_data的title＋text，
条件分别为comedy、science fiction、romance、horror、heist、romantic comedy、biographical comedy，
不能改复合genre为“或”，也没有Q4的24-title预选。Q6a–c完整读取tampa_international_airport的
Airlines＋Destinations，条件分别为Frankfurt、Germany、Europe；即使原gold相同，每题仍独立处理实际全部N行。

完整Filter后只投影原title或Airlines列，不做head／去重／reset_index，source dtype、原索引、重复与空值保持，
空源／全阴性也是原列的空DataFrame。模型evidence的缺值明确转空字符串，原输出源值不变；
每变体只在sink后读自己的固定JSON并用原评分，重复导致recall/F1>1的原行为不钳制。
现有Extract类、源快照类及旧Extract测试逐字未改，未复制逐题执行器或新增调度／缓存层。

私有候选MMQA完整模块18项通过、0skip，其中旧Extract10项、新Filter8项；
一套差分方法实际跑10题×四场景=40子例，调用完整c814原函数及真实SDK软件答复，
比较空源、全False、全True、混合索引的完整列／dtype／index／重复／null。
全部十变体fresh进程经任务工厂／真实原评分，并禁止LOTUS／semantic_methods／实验与测试依赖及网络。
root在8d071干净导出整合十个源／测试文件，任务、工厂与两系统消费者五个完整模块114项全部通过，
0skip／模型／GPU。该数包含旧Movie／Extract与分任务检查，不与前90项、11项、18项或消费者6项累加；
结果SHA-256为`2dcbf8e7f62421f8628cf57232817fd4c11bc109f86873c2b68e61bbb50e8c62`。整合时按实际image模态与非text parts建立拒绝边界，
纯文本数组仍原样交tokenizer渲染；整批拒绝检查先于任何模板调用，配置与真实消息分别检查。
真实模型与物理清理仍待各自目标验收。

<a id="native-q9map-608e20-model"></a>
## 608e20 独立原生 Q9 Map

具名`native-ray-daft-q9map-608e20-qualification-20261113-r1`固定
`608e20bebd404efbd480a58f71fccbbf6f7ef4c5`及c814，两配置明确ranking=map。
同原Reviews 2000行／11列筛选Ant-Man实际256发生项，保留源序256行完整评分结果；
其中只有128个不同reviewId，原evaluator字典对重复ID采用最后一项，不能因此把推理缩成128次。
两个永久账本各256、完整终态、closed无退款／未知／重试，额外查询预热为0。
252条原始答复相同、四处分数不同，不称结果等价；原Spearman Ray `0.734180233896955`、
Daft `0.7185159186346288`，Kendall分别`0.6347995726167764`、`0.6167664658392769`。

单冷source→完整sink为Ray `77.972277759`、Daft `56.684655174`秒，包含原生启动与已披露的内部I/O，
关闭／审计／评分后置。Ray 2.56.1／vLLM 0.22.0与Daft 0.7.21／vLLM 0.25.1的版本、容量及缓存差异保持，
无同owner暖、硬件冷或性能排名。Ray实际token数组与Daft prompt SHA／token长度／输出SHA均核，
不补写Daft未保存的完整token数组。六组FINAL均passed／failure=null、GPU空闲／租约与SHM保护已核，
本轮未找到失败模型轮，不据此宣称全历史无失败；预启动控制脚本保留原件。
1751个payload、1158个608e20源码Git对象、四个c814原参考与实际逐条关联／预算核验通过，
核验记录SHA-256为`77bdbab611708dd3391ac52620f88cdbbdf24a0f3f526c17db990bab26079ef5`。
未重跑模型或评分；此为独立JSON Map，不是TopK、LOTUS或当前任务组合的资格。

<a id="native-mmqa-q1-8d071-model"></a>
## 8d071 独立原生 MMQA Q1 sf25

`native-ray-daft-mmqa-q1-8d0710-qualification-20261113-r1`固定
`8d071066f80fd004fc5e82a372a95b930bb53dae`及c814，输入原20行表与25行／5列文本，
两系统各25个完整发生项／终态，永久账本25/25、sequence与摘要唯一、closed，无退款或重跑。
完整原结果保留Series：name=director、dtype=object、index=[8]、唯一值Michael Ritchie。
只有后置评分桥转换列形状，原最终查询F1=1；不是25条抽取均正确的质量结论。
保存的实际输出、Series及原评价分别核验，未重跑模型、答案解析或评分器。

Ray单冷`77.047351423`、Daft单冷`57.154012258`秒，均由source／sink事件差直接支持且含原生启动；
实际sink先于close／ACK，审计／评分后置。版本／容量、原生内部初始化、prefix／持久编译缓存与预检口径保留，
不授同owner暖、硬件冷或跨系统正式排名。
Ray首次query已completed、25次已终态，但外层清理报告semaphore身份或holder unresolved、租约未释放；
随后EXACT-CLEANUP按精确name／inode／uid清理32字节对象，恢复时holders及scan_errors为空，0模型／无退款／不改原helper。
首次具体holder、scan-error及分配库原因仍未知，不能把后验恢复归因为某个库；Daft首次外清理passed。
最终精确清理及SHM保护回执保留，不把原失败轮改写成首次正常收尾。

1516个payload、1517个归档常规成员、1160个源码Git对象、四个c814参考及两份bundle／frozen ref均核，
核验记录SHA-256为`6f76cf87d408afcfa89ad3a9fab91fbbd558770a634d7c8aef2b4ec86809897d`。
原归档见下表；只授8d071工程JSON Q1 sf25单冷功能与实际清理范围，不扩展到新Filter、Q4、视觉或warm。

<a id="daft-native-mixed-messages"></a>
## LOTUS＋Daft 文本到图像的实际混合链

原Q12第一批Filter产生user字符串，随后只对实际survivors作含图片的JSON Extract。
Daft0.7.21 builtin OpenAIPrompter固定以内容数组发送，现用无SDK依赖的effective_openai_messages明确描述
system＋single-user的str→单text part编码；旧数组不变、不修改原QueryLM消息或packet、不增加自定义provider／Prompter。
URL被原生错误识别为媒体及PNG边界沿既有列构造一次验证；未增加查询内per-occurrence审计hash向量。

完整sink后按effective SHA Counter核HTTP、保存batch_index／call_id及原始／effective SHA映射；
原始SDK提示、普通关系、parser和逐阶段JSON参数保持。重复发生项不去重，HTTP答复仍只按整批关联，
不能由该映射宣称重复消息的逐行HTTP响应已绑定。
metadata明记daft.openai.system_single_user.parts.v1及target_renderer_equivalence_verified=None；
文本和角色无损包装不证明目标renderer／token序列或模型质量等价，实际素材／VLM资格仍待验。

完整原函数／worker软件矩阵包含Animals两Filter4→4、MMQA非空Join→Extract4→4及EcommQ12 Filter4→Extract3，
核原结果／原评分、原QueryLM消息、JSON与最后sink后ACK；未编码的raw文本HTTP即使产出完整结果也在后置audit失败，
保结果、移除成功quality。原Q12空survivor在4次软件HTTP后保留原KeyError，不启动Extract worker。
实际Daft0.7.21 native图＋软件OpenAI API另验字符串、内容数组、重复及原四视觉batch，旧prefix Map／sampling函数逐字未改。

主线程在0d0cc干净导出整合四个源／测试文件，五个完整模块52项全部通过、0skip／模型／GPU，
包含旧回归，不与分任务9／5项或前47项累加；结果SHA-256为`558fcf872a7f38a6a936676564df992aa61203f2e0262fc2b6fd8d48acbf476b`。
首次私有用例导入LiteLLM尝试远程cost-map且DNS失败，后续设置本地cost-map；额外零INET尝试哨兵在功能5/5之后
因urllib3本地IPv6能力probe被拦而失败，具体输出及窄定位保留。最终根组合用本地cost-map；
实际native API为软件transport，未启动服务／模型，不把首轮说成从无网络尝试。
only-visual入口与TopK取消guard、原生重试、模型所有权／worker关闭和计时边界均未更改。

<a id="native-medical-text-family"></a>
## 独立原生 Medical 文本任务家族

MedicalTextTask复用同一独立任务协议及Ray／Daft runner，接固定c814的Q1／Q4 Filter和Q10 Extract，
不导入LOTUS SDK／提示／解析。scale_factor只选原CSV文件，默认原11112，无算法行数上限。
Q1对全部symptoms判定后返回原patient_id Series；Q4在source区间完成patient与symptoms的原普通inner join，
对所有连接发生项判定后计算重复加权年龄平均；Q10全量诊断后投影patient_id／text_diagnosis。
金标不入模，模型／扣额／source计时前冻结原patient labels；Q10评分的小写原地修改只作用sink副本，
完整内存结果保持模型原文。缺失症状按独立JSON合同转空文本，JSON null按既有Extract合同物化为字符串None，
原空Series／NaN均值／空Extract缺列KeyError保留，不改写为成功空表。它是工程JSON对照，非作者原版语义方法。

主线程在2bac6e184干净导出整合Medical、任务工厂和下节Serve接点，七个完整模块139项全通过，
0skip／模型／GPU，包含旧Movie、MMQA、Animals及两系统消费者回归；结果SHA-256为
`b62a7bb65da6865640d888321286f34d3d5d685bf19ba4206200b632e22baeb0`，不与分任务计数累加。
Medical模块包含真实原函数／SDK的受控回复差分，以及fresh禁LOTUS导入后的原评分。
另六条实际Medical任务→原Ray driver或Daft prepared worker／ACK的软件流程，覆盖每系统4／6／4发生项，
核完整结果及评分一致、诊断大写sink不被评分污染；证据SHA-256为
`ae6009bb5493f93ea3b977f72b1bfdf8ab9e6d2c429a8ab3f1028cc77cfb4c4e`。
这六条是实际driver协议上的软件kernel，未启动原生SDK／模型，不授予Medical真实模型或性能资格。
恢复前的独立任务只有测试、无生产实现，原目录与失败输出保留；本轮未丢弃或覆盖该稿。

<a id="native-ray-serve-session"></a>
## 独立原生 Ray 的 Serve 会话 profile

以下保留初始v1软件接点；默认caller现已由[隔离v2](#native-ray-serve-isolated-caller)替代，原软件结果不授予v2目标资格。

固定Ray2.56.1公开LLMConfig／build_llm_deployment及ServeDeploymentProcessorConfig，
在同一sembench_native_ray runner新增显式serve-session及单独schema，由owned deployment承担模型生命周期，
Data processor承担批处理／调度；不复制任务逻辑，不改offline run_query、原采样或图像stage接点。
JSON structured_outputs在chat边界显式转换为response_format，stream=False接完整非流式答复；
call_id与实际chat response.id／native request_id在sink后核对，不猜测去除ID后缀。

一个已预留有限预算覆盖部署启动的实际请求及两遍完整fresh source查询，扣额不退还，未知或超额立即停止后续轮。
每轮ready后的source→native graph→strict task parse→完整内存sink直接计时，部署启动及最后关闭另列；
原生同步预算／观测仍在内，身份核验、source审计、评分和写盘在sink后。失败保留已完成sink及首个异常。
只关闭本次新建application及其隔离Ray连接，不执行全局serve.shutdown，也不接管已有同名application。

Python调用可接既有host identity_snapshot，当场观测node／replica actor、PID／创建时间及非空实际模型子进程PID／创建时间，
同时核模型构造事件在两轮前后不变。缺字段、陈旧观测或无探针均不授予warm；CLI没有该探针，warm_qualified=None。
软件owner fixture能检验判断逻辑，不能证明真实GPU模型复用；target_native_model_qualified始终留待目标验收，
物理GPU／SHM退出由外层监督确认。Ray Serve是不同执行拓扑，不能给offline历史成绩追授暖轮，Daft暖轮仍未接。

两模块38项受控生命周期检查纳入上节139项组合，不另累加；覆盖共享owner、fresh source、实际预算区间、
startup请求、unknown、身份变更／缺失、创建／关闭错误和完整结果保留。首次用例暴露遗漏JSON格式转换，修正后冻结；
fresh用例导入错误及失败输出同样保留。源码已核固定Ray公开API和vLLM单prompt ID规则；
本轮无Ray Serve SDK部署／模型／GPU／服务安装，实际版本兼容、warm身份及物理退出仍pending。

<a id="native-mmqa-q4-8d071-model"></a>
## 8d071 独立原生 MMQA Q4 sf25

具名native-ray-daft-mmqa-q4-8d0710-qualification-20261113-r1冻结
8d071066f80fd004fc5e82a372a95b930bb53dae及c814原题，实际25行源按原24片名条件选择24发生项，
各系统24次、共48真实终态，无unknown／retry／额外query warm-up；完整19组结果及24条原JSON两系统一致。
原位置7／15返回JSON null，按已声明合同形成字符串None组；保留原大小写／genre别名，不事后修答案。
保存的原precision／recall／F1均为0.6153846153846154，执行完整不表示全部抽取正确。

Ray source→sink为75.204570546秒，Daft为56.933395667秒；各自单冷含新owner及内部I/O，
未测同owner暖轮，版本／容量／缓存不同，不作性能排名。原生模型、预算、实际关联和最终物理清理回执核验。
首次Daft sdk-r2因私有版本断言被错误改成0.24.1而在0模型时失败；r3仅恢复实际0.25.1断言后完成该门，
未改模型／算法／依赖，首轮失败保存。独立核验器首次误将raw messages与prompt packet比较，修正核验对象后通过，原件未动。

1512个payload／1160个源码blob／四个官方参考及归档／本地副本已独立核hash；
私有核验结果SHA-256为`04ffb48d43e7841b59ef126fcb2008f1ab0f33bdf65618a3740e93bdab31b5cb`。
未重跑模型或评分，不转授当前新Filter、Medical、视觉或Serve会话资格；Daft实际完整token数组仍未知。

<a id="native-q7-q6a-0d0cc-model"></a>
## 0d0cc 独立 Ray 图像Q7与Daft文本Q6a

两项均冻结0d0cc150d016619a3e38d224f64d73721abcf6e1及c814官方参考，仍为项目JSON工程对照。
具名native-ray-animals-q7-0d0cc1-qualification-20261113-r1使用原sf64 manifest、64图两条件共128请求，
全部终态、实际输入／输出token数组、发生项关联及永久预算相符。完整城市交集Mombasa／Nakuru，
保存原precision=1、recall=2/3、F1=0.8，缺Kisumu；不能借旧LOTUS Q7的F1=1覆盖此质量差异。
source→完整sink为163.652111592秒，包含新owner初始化、真实图像处理及已披露同步I/O，无真暖轮。

具名native-daft-mmqa-q6a-0d0cc1-qualification-20261113-r1对机场CSV全部25行Filter，25请求全部终态，
实际prompt SHA／token长度／输出SHA及预算相符。完整投影保留原索引6的Discover Airlines，保存F1=1，
不表示全部25行分类准确率；source→sink为33.289591545秒，含初始化／packet等内部I/O，未测同owner暖轮。
Daft实际完整token ID数组未保存，保持未知。两条执行路径和任务不同，不作横向排名或转授新Serve候选。

两者归档／本地payload／Git源码／官方参考及清理回执已核验，GPU／所属进程／租约／SHM检查通过；
核验只读，永久预算字节未变，未重跑模型或评分。Ray恢复包复用旧Q7图像字节作无损运输，未复用旧模型结果，
128个实际PNG／RGB消息载荷逐项核验；原JPEG本机未重新散列，保留归档身份与服务器检查证据。
最初scp文件名被拒在0模型时发生；核验器首次错误期待SDK prompt字段的失败也保留，修正核验对象未改原件。
本机两份详证SHA-256分别为`8103b51c15698408737e16a5d2d987a8042b3f877d9cccf00fb7319ba2e5d30c`、
`5850641377bb8002af1912c312712bf738afa546929a17fa1cb69bb6c8e57080`。

<a id="daft-empty-native-successor"></a>
## LOTUS＋Daft 正常空批次的后置审计

服务器冻结6c75efdf的零模型Q2b探针令Join全False，原查询已完成内存空结果，后置HTTP审计却读取
空Extract批次不存在的metadata，抛KeyError。该批次按既有predict合同直接返回空predictions，不创建原生worker，
因此审计现在直接跳过无请求批次；没有虚构native metadata或改变原查询／parser／计时。
完整原结果与运行成功分开，首轮failed_zero_model_probe及其已完成sink保留，不重写为原轮成功。
该空sink验收对象是现固定image/sembench_mmqa._native_query_api的exact＋空Extract schema兼容接线：
它已有零行Extract补output_cols的行为；固定raw c814配LOTUS1.2.4零答复在投影处可抛KeyError。
HTTP审计修复没有补列或改该既有合同，不把此空sink称为未修改raw SDK空关系行为；后续独立JSON多阶段将保留具名合同。

新增受控Q2b回归先复现同一KeyError；修复后按原函数／原评分核完整空sink，LM发生项为4→0，
只有一个软件worker、四次软件HTTP终态，无第二worker／模型请求，最后ACK在sink之后。
原Q12空survivor的原KeyError仍保持失败，不用统一空表掩盖不同原题行为。
干净导出五个完整模块53项全通过、0skip／模型／GPU，含真实Daft0.7.21图＋软件API旧回归，结果SHA-256为
`3d4a778a4918d7dd0ba5e911cddb542009b86ba0f82c9564659a27c965cdeac6`。这只关闭空批次正常语义缺口，Q2b实际VLM／非空Extract及性能资格仍另验。

<a id="native-cars-ecommerce-text"></a>
## 独立 Cars 与 E-Commerce 文本任务复用现有系统 runner

CarsTextTask接c814原Q1/Q4/Q10，scale_factor默认原157376，只选择真实sample CSV路径，无行数上限。
Q1全部投诉Filter后返回原car_id Series，无无关car表依赖；Q4/Q10在查询内完成cars.merge(complaints,inner)，
对全部连接发生项推理，保重复／NaN普通连接。Q4固定`2026 - mean(year)`及投诉加权，
原gold却按engine问题car IDs筛full车辆求平均的差异明确保留。Q10完整car_id／problem_category不因评分改写。
金标预先冻结，原sample complaint_id／car_id成员筛选使用本轮source快照，重复sample不人为扩gold，原长度错误保持。

EcommerceTextTask接原Q1 Filter／Q3 Extract／Q5 Map；title和整个nested productDescriptors入模，
ID及articleType／brandName等gold不入模。Q5原Apparel／subCategory谓词留在真实Parquet读取后的查询内，
Q1原id Series，Q3原index标签赋值的重复标签后写行为，Q5原逐位置Map投影保持；JSON null物化None字符串。
prepare_evaluation提前冻结同一Parquet字节，真实查询fresh读源；sink后核同源快照，原TOML SQL与指标在冻结gold临时目录执行，
不重写scorer。原空Extract／空谓词边界及重复ID引起的原ARI错误保留，不用去重或fallback遮盖。
E-Commerce Q7及图像／混合题未由这批实现，真实资产缺口不是原生API不支持。

主线程在4793ecf75 clean导出整合六个源／测试文件，九个完整模块159项全通过，0skip／模型／GPU，结果SHA-256为
`4f99793e38187d872479fce955c0ff338b6e0c032bc93321c043daf3d5c21a4a`；不与先前149／139或分任务计数累加。
Cars含3题×5原完整函数／SDK软件差分，E-Commerce含3题×4差分；fresh禁LOTUS导入后的原评分也通过，
两runner源码未因场景新增而复制。另各六条实际两driver软件流程验证完整结果／评分、预算和Daft ACK，
Cars各5／4／4发生项、E-Commerce各6／6／3，消费者证据SHA-256分别为
`4fb8c5e867ae7f30d35be9c4597fea8d8a3d4de2cdcea66da4a4b9b3eabb4d5d`、
`229e80d4c4f707a242c47e4c4fa3d0cd2c0a8005ed09189da26a0a7513983a5e`。它们没有运行原生模型，不授予真实数据完备性、质量或性能资格。

Cars首次fixture在两张gold表都放同名private_label，原无suffix join正确报列冲突，修fixture列名后通过；
E-Commerce首次缺gold列fixture含空struct，PyArrow拒绝写Parquet，只改有child的descriptor fixture。
两个首轮失败、missing-module红及核验命令状态错误均保留，未改生产逻辑迎合测试。

<a id="native-ecommerce-exact-pair"></a>
## 独立 E-Commerce exact pair 接入同一任务

在009232c04的scalar任务上增量接Q7，仍是一个EcommerceTextTask及原两个系统runner。
真实Parquet读取后按原price<=500筛选，保原索引；完整N²有向pair包含self与重复ID，先用轻量count核有限额度，
再构造完整title＋nested descriptor的JSON布尔请求。原生API负责推理／批处理，工程任务不引入Core调度。
这是项目exact pair对照，不称LOTUS／作者Join方法；原LOTUS1.2.4每侧语义选列限制及approximate策略不由此修写。

True预测先转换成原索引标签pair，再沿原普通join回连两张源表、reset_index，最后两侧id.astype(str)拼接。
三条frontier若索引为[9,2,9]，九个全True模型发生项会回连成25条结果，不能简化成九条或去重。
原空Series、浮点／null ID字符串、price null排除、原F1及gold快照边界保留；未改scalar既有执行与评分。

主线程在干净009232c04导出核三文件增量，四完整模块111项全通过、0skip／模型／GPU，包含同两runner旧回归，
SHA-256为`99636258cb7b068bb03696291ff3ddfebf2374470ee1bb229ca4c44290d29cdc`，不与先前159或分任务16累加。
新模块包含Q7七种原完整函数／真实SDK软件差分及fresh禁LOTUS原F1检查。
两条实际任务→driver软件流程各九个发生项、完整25行结果／原评分相等，永久预算及Daft ACK通过，
证据SHA-256为`6ecc3283a8b91b06b9999b44f1606ea4fc6edb8e7f2c6c5ba4b3aafd2eca69e1`。
复制的私有helper首次报告标签错误写成six-flow，实际计数是一题／两系统；仅更正派生metadata，原报告保留，未重跑模型或评分。
本轮原生SDK／模型及真实E-Commerce资产未授资格，原来源配额／完整输入缺口保持。

<a id="native-daft-attached-session"></a>
## 独立原生 Daft 附加服务的连续查询

同一sembench_native_daft runner新增attached-session动作及独立schema，直接复用中性native_daft_query的messages packet、
builtin OpenAI graph、HTTP observer、监督与ACK；未导入LOTUS语义、未新增provider／server／执行调度框架。
四个offline函数configuration／check／_render／run_query逐字保持；新profile拓扑和旧offline冷成绩分开，不追授暖轮。

两次fresh任务读取与完整sink各自直接计时，一个永久有限HTTP ledger贯穿两轮，真实POST及SDK重试都扣额，
不退款；unknown／missing terminal／预算不足／client或ACK失败／已知owner变化时停止下一轮。
每轮worker持有最后结果直到driver形成完整内存sink后ACK，才退出；外部服务不属于该worker的清理进程集合。
packet、native graph／HTTP及同步观测、IPC在query内，最后客户端退出、原评分、消息审计及写盘在sink后，
service startup／ready在driver中未测则为None。有效消息SHA与HTTP消息＋输出multiset后置核对，重复发生项仍保留，
逐行关系由native call_id投影关联，不宣称重复HTTP的逐发生项一一映射。

每批native OpenAI prompt只支持静态system及同内容布局；字符串显式转单text part数组，目标renderer／token等价保持未知。
Python接口可接既有外层host probe，观测服务及非空实际模型子进程PID／创建时间，时间戳须落在本次同步观测区间。
四次完整身份相同且两轮有HTTP工作才给同owner复用标记；无probe／陈旧／仅endpoint为None，CLI没有probe。
相同PID不能证明实际权重、cache状态或进程内重构，target_native_model_qualified仍为False，真实暖轮／性能待目标验证。

主线程在66f0bdaac clean导出整合两文件及配置模板，四完整模块120项全通过、0skip／模型／GPU，
结果SHA-256为`4487688e541a93bc29cc5b22e84db6a10a9fe77ebbcd7d597b9247700e14d809`，包含旧offline、任务工厂、E-Commerce配对与Ray回归，不与此前111或分任务29累加。
新增九项使用真实中性worker subprocess／ACK＋软件HTTPX，核fresh源4→5、共享预算、503重试、unknown即使有sink也阻断、
缺旧身份／owner变化、gold缺失、有限额度、主异常与完整raw结果保留，fresh进程禁LOTUS／engine仍用原官方评分。
首轮软件child／fresh脚本漏json import的红已保留，只修fixture；最初JSONL存在性期待也更正。
既有可选native trace writer会吞掉含monotonic_ns的HTTP事件，因此本profile明确只使用完整worker.json.native_api原件及prepared消息，
不使用或宣称完整JSONL，也未为此扩写共享观测工程。真实Daft SDK graph／外部服务／模型尚未在本批运行。

<a id="native-query-batches-visual-families"></a>
## 独立原生 runner 的查询级依赖批次与图文任务

来源为固定c814原题源码、LOTUS1.2.4软件原函数差分及当前项目实现；工程JSON提示不复现原作者算法。
现有Ray offline／Serve和Daft offline／attached-session沿同一个native_query_batches(task, source)协议：
任务yield实际请求、send整批预测、return完整关系；原生系统仍拥有图执行、模型提交和调度。
既有单批任务直接适配，没有题目专用执行器或查询DSL。已知首次计数在消息构造前检查，
依赖总量未知为None，实际后继批次在提交前检查永久余额；不会用最大请求数冒充实际调用。
Ray在一个查询owner内推进批次，Daft复用既有逐批worker／IPC／ACK；空批不创建worker，
最后完整sink在末批ACK之前，前序必要退出仍计入查询。普通关系、真实读数和图像处理保持在查询内。
本次替代runner旧单批执行主体，既有文本入口／profile保留有效用途；没有长期保留第二套逐题执行器。

| 同一任务工厂场景 | 8cd初始接入 | 当时范围及后续 |
|---|---|---|
| Animals | Q7按原两遍完整source分为N／N Filter，其余Q1/Q3/Q10保持一批 | 新分批组合模型／Daft图像资格另验；0d0cc模型成绩属于旧平铺源码 |
| Medical | Q3／Q8完整单图Filter后原LIMIT；Q9普通三表merge后双图Filter | 新JSON视觉组合、真实原图绑定／VLM待验 |
| Cars | Q3普通merge＋Manual＋完整Filter＋原head10；Q8完整Filter后原head100 | 其他原题未接；真实资产／VLM待验 |
| E-Commerce | Q2 Filter、Q4 Extract、Q6 Map、Q8/Q9 exact pair、Q12文本Filter→实际幸存图片Extract、Q13图文Filter | Q10/Q11当时未接，后续见[依赖Join](#native-ecommerce-dependent-outfits)；Q14及真实资产／模型待验 |
| MMQA | Q2a／Q7完整视觉exact pair；Q2b真实Join匹配→颜色Extract | 8cd独立JSON Q2b三轮已真机非空执行，F1均0，见[原件分析](#native-json-q2b-8cd-nonempty)；不转授LOTUS系列；Q5原生归约未接 |

这些场景任务由Ray和Daft同一配置工厂选择；Ray显式打开prepare_image_stage，Daft图文显式选择attached-session。
Daft本地字符串profile仍拒绝图片，不静默换语义或换执行拓扑。原生API只提供生成，不等同作者级Join／Aggregate／TopK。
Q8完整标题／描述及图片是本项目工程输入；原LOTUS1.2.4 Join accessor只取一侧单列的限制未被本路径“修复”。
图像仅对真实需要的源／幸存行读取，RGB／PNG无resize；Q9双图按明确标签关联，原SDK列顺序不作为新算法等价证明。
E-Commerce Q4保重复索引后写覆盖，Q8/Q9保全部配对、原标签回连扩张、后置ID过滤及原字符串投影；
Q12保模型生成ID／字符串，不用金标修复。MMQA保os.listdir顺序和Q7原实现未筛Europe的差异。
原SDK空Extract不建输出列：新独立Q2b保留缺color的KeyError，现有LOTUS＋后端空schema兼容适配明确为另一合同，
不借它宣称未修改原SDK，也不把空Join的结果当非空后继验证。

26cc5ec9b干净导出加14份准确文件，12完整模块212项通过、0skip／模型／GPU；
MMQA两文件及E-Commerce Q8/Q9增量后仅重验三个受影响模块61项，全通过、0skip。
两次结果SHA-256分别为`b94d731ff8e12decb1332ccc9ec0fe7fc5f54b97dbc10c3bc3b8454e693222c4`、
`4a443807ee4b8de2b6be333428d34de51cffde308e5c0eae0e0a9d21a4dac8f6`，有重叠，不累加。
覆盖旧文本／图像／两runner、严格发生项、依赖预算阻断、空批、主错误／完整sink保留、最终ACK和fresh无LOTUS进程。
原函数与真实SDK软件差分覆盖完整关系、PNG证据和原评分，不使用模型服务。

另用实际场景任务连接两runner壳：Medical Q9为9，Cars Q3为4，E-Commerce Q12为5→3，MMQA Q2b为10→10，
E-Commerce Q8为12、Q9为36请求；每例Ray一遍、Daft附加会话两遍，共18条完整软件查询流程。
Daft真实子进程／IPC／ACK及HTTP记账、Ray录制processor与任务结果一致；无真实Daft／Ray SDK、服务、VLM或GPU资格。
证据SHA-256为`3d351eaa015fa6aae994705693fb4c8a5d6331cad3a59ddd8b13f29ec192308d`、
`be7810e74663a3c25b808abc50ba65478889f2520c6bac4e49ebff10cc8bb9ee`。
前两次消费者helper漏checkout／图像开关的失败保留，仅修本地验证配置，没有更改生产语义。

<a id="native-ecommerce-dependent-outfits"></a>
## E-Commerce 多分支 Filter 与依赖 Join

8cd108a83后在同一个EcommerceVisualTask接Q10/Q11，不新增系统runner：Q10原色彩／price<=1000预筛后，
三次完整Filter得到footwear／bottomwear／topwear，再执行两次真实依赖Join；Q11三次完整Filter和price<=500后的accessories Filter，
先footwear×accessories、bottomwear×topwear，再对实际两侧结果作最终Join，原四ID顺序不变。
每个分支都按原顺序独立执行，不因前一Join为空而跳过Q11另一独立Join；原普通merge、重复源行、标签回连、
冒号改名和最终字符串ID保留。工程JSON提供完整标题／描述和图像，不冒称原LOTUS多列选择行为或作者Join算法。

初始计数是实际必需的全部独立Filter请求，后继总量保持None；每个真实配对批次是普通sized iterable，
原生runner先检查len再展开消息／解码配对图片，避免真实后继关系扩大时在额度拒绝前构造N×M消息。
没有预设题号规模、任意行数限制、最大值冒充实际调用或额外计划框架；空配对不读图、不创建native worker。
原生调度、永久预算与ACK机制保持；原先单批主体已由现查询级路径替代，保留的字符串profile仍用于有效纯文本任务。

四个受影响完整模块135项通过、0skip／模型／GPU；包含两题的非空、首Join为空、Filter全False原函数／SDK软件差分，
完整原关系和原评分相同，Q11独立第二Join的实际工作保持。
三个原生入口的新增拒绝用例用一百万对的未展开迭代器证明在构造消息前停止；这里只验证有限预算边界，不是规模性能结果。
第一轮测试错误预期原SDK不进入零大小batch_completion已保留，改正软件断言后通过；生产代码未为其增加空分支。
另用两原生runner壳实际接Q10的6／6／6／2／2批次和Q11的6／6／6／5／2／1／2批次，
每例Ray一遍、Daft附加服务两遍，共六条完整软件查询流程，结果和原评分一致；不授予真实E-Commerce资产／VLM或正式性能资格。
整合软件结果SHA-256为`5894d38665bf4943d1b4719f14ae9fef92a3b09d0e7bf323fbb7149d257fe681`，
六条消费者结果为`117df4f52692da228d17790d1da12fee861de9c8e6626e22c0f74f1097278185`。

<a id="daft-6c75-4793-606f-model"></a>
## Daft 三条具名模型回执及 Q2b 标签更正

本节只读复核服务器原件，未重跑模型；新多阶段代码不继承这些冻结来源的资格。

| 冻结来源／真实路径 | 工作与质量 | source→完整sink与边界 |
|---|---|---|
| 6c75efdf3，LOTUS＋Daft Animals Q7 sf64 | 两次原生Filter共128真实终态，原城市交集F1=1 | 87.953249591秒，外部服务启动66.064980013秒另列；不是独立暖查询 |
| 4793ecf75，LOTUS＋Daft MMQA Q2b sf25 | 25图片×13表行＝325真实Join，空结果P/R/F1=0，Extract 0 | 50.373891034秒，外部服务启动65.085848104秒另列；完整空分支执行，不是质量通过 |
| 606f8b34a，独立JSON Daft Movie Q8 | 两轮fresh 2000行各120真实终态，逐行预测相同，计数相对误差各4.0431266846% | 5.594704559／5.988286006秒，外部服务启动55.07258258秒另列；含客户端worker／packet／IPC和同步观测 |

4793 Q2b是LOTUS＋Daft，不是独立原生Daft；之前笼统“原生Daft Q2b”的交接标签更正，调用与质量事实不变。
其已有空schema兼容适配已披露，不能称raw SDK零Extract行为。325条原始回复经纯LOTUS1.2.4解析均False，
其中299条明确Answer: False、26条缺Answer标记但正文含false，默认解析回退0；全部stop，未见输出长度终止或拒答。
旧8667原始路径是LOTUS原LM→OpenAI/vLLM，不是Ray；相同325候选／原消息、模型revision及七份模型文件摘要已核。
旧回复为306条明确False＋19条无标记false，回退0；本复核未找到独立Ray Q2b原件，不能串用Q2a／Q7结果。
五个官方金标匹配都在这325候选里，故不能归因于候选遗漏；完整runtime输入／输出token和全部生成参数读回未采，
重复消息HTTP逐发生项不能独立一一关联，不能据此完全排除接线差异或把原因全归模型。
非空Extract仍待验；可用一个原图片与原金标表行做最多Join＋Extract两请求的具名诊断，
必须由真实Join决定是否进入Extract，False保留为失败观察，不预置匹配或改评分。

606f四次新鲜服务／实际模型进程PID及birth观测一致、日志仅一次EngineCore初始化，支持该冻结配置下同模型owner复用，
每轮客户端worker另建。prefix cache开启，不能称无cache正式性能对照；同PID不证明全部cache／权重状态。
原生call_id完整关联及HTTP多重集合核对通过，四个重复消息发生项的独立HTTP一一关联和实际生成token读回仍缺。
run报告内部target_native_model_qualified=False不是自我授证；外层实际SDK／模型资格仅限本次Q8。
首次bundle／验证脚本ref假设失败、退出semaphore警告、旧Q2b首轮审计失败均保留，资源清理闭合。
4793外网INET guard激活缺独立证明，不能称全程离线；本地只读核验没有网络或模型调用。

三条详细复核SHA-256依次为`0c439b9cf1a3cc57076fc0e278a214a78c30fa7c73cf3fa677e27adf6a742ad8`、
`6fb68d5750f3f74134e25e9a0f356b439c5064444a893d2b9ecaa9a1083fb103`、
`bf7d504daef3fd09480e6522cda26d74ac0de257701923a6851e07b33e0f2488`。
金标候选交集复核SHA-256为`4f610db316d7d1d29ac4e527299306e783d1918476566b0d0e34a1e172d8a7bd`。
不同语义、冷暖与计时范围不互排，不把Q8复用结果转授新视觉／多阶段或Ray Serve。

<a id="medical-q10-606f-model"></a>
## 606f Medical Q10 完整源 Extract

只读核验固定606f8b34a、原SemBench c814和两份服务器归档，不重跑模型／评分。
原文件后缀scale11112及11112行金标不等于模型输入行数：真实症状源1200行，每轮全部执行，完整sink为1200×2诊断关系。
三份生产源码与本地冻结Git逐字相同；Ray2628份、Daft1348份声明payload逐件与归档核字节／SHA，
全部原生预测按物理源位置重建到完整诊断列，JSON null按已公布合同物化为字符串None，不静默重试或改金标。

| 路径 | 真实请求与原F1 | 实测时钟 |
|---|---|---|
| 独立JSON Ray，单冷 | 1200终态，F1=0.3044844036 | source→完整sink 80.886512009秒，含原生懒启动 |
| 独立JSON Daft，服务就绪后首次 | 1200终态，F1=0.3032953430 | 34.222731663秒，服务启动42.058288603秒另列 |
| 同Daft实际模型owner重复 | 1200终态，F1=0.3139864316 | 34.653651831秒，同外部模型owner，客户端worker另建 |

Daft两轮1193条解析结果相同、七条不同，按物理发生位置保留；原答案差异／低质量不被最终执行成功覆盖。
prefix cache开启、结果缓存关闭，无额外推理预热／整题重试；native/API终态与永久计费、资源清理按原件核。
这是统一JSON工程对照，不是作者原版算法；Ray冷与Daftready首／重复口径不能横向排位，不能转授图像或新依赖查询资格。
Daft实际generate输入token数组、输出ID、重复HTTP一一关联等未知点仍保留；后续独立JSON Q2b见[非空原件](#native-json-q2b-8cd-nonempty)，不转授LOTUS原题链或Ray Serve。
复核只重算原始预测→sink投影，质量引用归档保存的原评价，未重算金标评分。

原件复核SHA-256：`fa3f029e3d8e77fbd7f64a95320fe7c5bbc9c9206cf0fb861f52981d5014e248`。

<a id="original-query-minimum-budget"></a>
## 原题准入下界与 LOTUS＋Daft 分支查询

对既有LOTUS＋Daft完整原题桥进行软件检查，Q10/Q11已能沿同一个原函数／LM边界执行全部实际阶段，
19／25请求分别得到原完整结果和原评分，末批ACK在完整sink后；没有因此新增执行器或改写原题。
原覆盖表把“未验”写作“未接线”，现更正，grouped TopK仍未接且取消动作保持停止。
这与独立JSON Q10/Q11是两个语义身份；原SDK只选一侧单列的限制保留，不能互授算法或目标资格。

另零模型复现原Q12预算误拒：真实四行Filter及三行幸存Extract只需七次，但原服务、LOTUS＋Ray校验／执行和
LOTUS＋Daft把八次理论最大值当最低准入。当前从既有描述读取minimum，固定工作量仍读expected，
MMQA source及共享描述显式传递其首Join下界；四个原入口不再拒绝真实能完成的较紧额度。
后续实际调用／原生重试仍永久扣额，耗尽不重试整题或增加调用；未取消有限预算、预置匹配或改变质量／计时。
Q12七次真实软件API非空链，以及MMQA Q2b仅候选额度的空链均完整通过；小于已知必要量仍在模型前拒绝。

初始新增三方法中，原Daft两题的软件检查已通过，另外两项准确复现最大值误拒；失败原件保留。
修改后八完整模块139项首轮138通过、唯一失败为旧测试仍强制预留最大后继的断言。
只修该测试合同并加强MMQA空／非空紧预算用例，受影响完整模块22项复核全过、0skip；
其余七模块117方法源码／测试不变，最终139个不同方法全部有当前通过证据，不累加重复22项或抹除原失败。
范围包含Movie旧原题／prepared、视觉、Q5多层聚合、真实CPU子进程／ACK和预算失败关闭，均0模型／GPU；
Daft／Ray推理与HTTP回复是软件fixture，不是目标SDK、VLM或性能资格。
最终方法清单及分范围复核SHA-256为`ce43235ca8b0b29768444f5bc5a2d4e5dc3b1986b44c4cd8fcba19270055314e`。

<a id="native-ray-serve-isolated-caller"></a>
## Ray Serve v2：原生调用环境与任务／评分环境分离

606f目标环境检查发现driver有LOTUS／DuckDB等评分依赖但没有vLLM／SGLang协议，原生环境有Ray2.56.1／vLLM0.22.0，
但没有评分依赖。旧默认caller在driver导入Serve请求类型即失败，不能因此判系统不支持或通过混装环境解决。
现同一系统runner直接调用native_ray_serve_process，model_python中的一个持续caller持有原NativeServeSession，
driver只读真实source、推进任务依赖批次和评完整结果；只共享项目源码路径，不借另一venv的包路径。
Data processor与Serve仍拥有提交／批次／调度；没有新增语义runner、后台模型池或查询DSL。

每个实际batch通过独占私有JSON包交接，回复后恢复原任务；正常结束与首异常均关闭本次owned app，
原records supervisor负责有界进程树退出，不关闭其他app或全局Serve。启动／会话／清理配置是显式例值，
清理在总会话时限内预留，不为每阶段重复扣除；失败终态保留原错误及close错误。
图片包按实际文件大小读取，去掉通用8MiB JSON默认值对合法图像的误限，无输入行数／题号上限。
必要IPC、原生同步预算／观测留在source→完整内存sink，后置审计／评分／结果写盘在外；
采用native_ray_serve.source_to_sink.v2，不扣减或改写旧成绩，caller与deployment启动分别报告。

默认CLI在原生构造事件记录实际model actor context及其PID／birth，再当场读取真正EngineCore后代的PID／birth；
不把它称作独立观测的Serve replica或cache／权重身份。前后构造事件与身份须一致，两轮必须实际有模型工作；
陈旧／缺失观测、PID复用或空查询均不授予warm。此身份hook只对Serve启用，不增加offline观测调用。
目标SDK／模型资格仍未知，软件身份只是检查判断逻辑；物理GPU／SHM退出仍须服务器确认。

f26fc3f6e干净导出加七份准确文件，三完整模块55项通过、0skip／模型／GPU，包含真实隔离CPU child、
两次fresh查询、最终ACK／close、启动／执行异常、大图像包、PID复用、空查询及SDK配置检查fixture。
此前68项整合含旧Ray prepared回归，不与55项重复累加；首次误列不存在测试模块的53通过＋1导入错误保留，修正计划后完成整合。
最终55项结果SHA-256为`58b3d54b448b413d408047c629eb33a91ab32ccc2bf4e961a4d625c2e756c7fe`。

服务器应先用model_python执行--check-sdk，只导入并构造原生LLM／processor／request配置，确认未启动Ray／CUDA／模型；
仍需核实际响应ID与AsyncLLM请求ID的关联。通过后再安排有限两轮真实Movie查询及同模型身份／退出核验。
本批未安装依赖、启动Serve、访问GPU或重复既有模型请求；v1失败保留，目标兼容、真实暖轮和物理退出pending。
调用与三个超时字段见[现行入口](../../../../code/scripts/README.md#native-ray-serve-session)。

<a id="native-json-q2b-8cd-nonempty"></a>
## 8cd 独立 JSON MMQA Q2b：非空执行与零质量

两份服务器原件冻结8cd108a83909b35f6b583f5f08dd6c7ebe94c9ca、SemBench c814，保留13表行×25张实际图片的325个配对。
这是独立JSON工程语义，不是LOTUS原提示／算法；4793的LOTUS＋Daft空Join回执仍是另一条路径。

| 路径 | 真实Join→Extract及完整结果 | 原F1 | 原source→sink |
|---|---:|---:|---|
| 独立Ray，单冷 | 325→6，6行ID／image／color | 0 | 198.625074238秒，含原生懒启动 |
| 独立Daft，服务就绪后首次 | 325→5，5行 | 0 | 41.516931146秒，服务启动另列 |
| 同Daft实际模型owner重复 | 325→8，8行 | 0 | 40.614414924秒，同owner，客户端worker另建 |

共994次真实终态、unknown为0；支持该冻结输入的实际非空二阶段执行，不能说质量通过、全规模通过或横向排名。
三轮全部五个金标配对都出现在325个候选中，原始JSON均明确False，没有一个金标配对进入Extract。
被保留的19个匹配均属于另外两张赛马场照片；SHA绑定的金标图片是蓝色Santa Anita Park标志，另两张为场景照片。
原SemBench与本工程提示都要求识别该赛马场的logo，本次没有更改原题、提示、阈值、解析或金标来获得匹配。
原始答复严格解析后，按真实输入顺序回放任务，三份完整关系逐项等于已存sink；没有发现Boolean回退吞掉True。
因此已定位到Join判定与金标不符，而不能仅凭本复核证明模型内部视觉tensor或全部生成配置正确；颜色阶段不能恢复漏掉的配对。

只读复核两份完整归档字节／SHA、选定源码blob与8cd Git一致、25张原图与manifest一致，
全部真实答复及完整请求包SHA精确回放；不声称独立重核所有3264个payload，完整manifest计数由协调会话另存。
Mac路径别名造成的本地顺序fixture失败、相同RGB但PNG编码字节不同、误猜Daft包布局的三次复核失败均保留；
只修私有回放helper，使用归档PNG且核25张解码RGB与原图逐项相等，再核完整包字节，不改生产数据或补造匹配。
本次模型／网络／评分重跑均0；复核SHA-256为`6a52e9edd0f8e673d97ee8e0b254c01e3ffc220a7380bc87ba998dc25a66ffc2`。
LOTUS＋后端的原语义非空Join→Extract资格仍独立pending，Ray Serve v2也不借用本轮成绩。

<a id="native-medical-q4-8cd-model"></a>
## 8cd 独立 JSON Medical Q4 完整普通 Join 与均值

三轮均读取11112个patient与1200个symptom原源，原普通Join得到1200个发生项，再执行JSON Filter及保留重复权重的年龄均值。
固定同一8cd源码，原生Ray冷启动与Daft外部服务就绪后两轮分列；不是作者原版或原LOTUS提示／解析。

| 路径 | 真实请求／保留发生项 | 完整均值 | 原MAPE | source→完整sink |
|---|---:|---:|---:|---:|
| Ray，单冷 | 1200／129 | 59.8992248062 | 6.4951220634% | 80.785331415秒，含原生启动 |
| Daft，就绪后首次 | 1200／130 | 59.6692307692 | 6.8541511564% | 34.585681993秒 |
| Daft，同模型owner重复 | 1200／129 | 59.8992248062 | 6.4951220634% | 32.593405434秒 |

Daft服务启动44.065334279秒另列，prefix cache开启、结果缓存关闭；三轮无未知、重试或额外推理预热，不作性能排名。
Ray原查询与结果已完成，但首轮外层清理失败；后续精确恢复未重跑模型，原失败和原因判别记录不足均保留，不能重写成首轮全过。
本接入会话重新核两份归档及3969个声明payload字节／SHA、场景任务与系统runner的冻结Git blob，
用全部真实Boolean按原patient普通Join位置重建保留关系及均值，结果与三份完整sink相同；质量取保存的原评分，没有重算金标或调用模型。
这证明具名配置下执行闭合，非质量门槛通过，也不转授视觉、其余Medical题或新Serve拓扑。
只读复核SHA-256为`2a7d3afda8fee7d3ab1fa7513396106308ff3c12dedf324d660d9cabab0222c4`。

<a id="kalypso-text-extract-map"></a>
## Kalypso 文本 Extract-via-Map 与原版图像边界

固定作者05024f027f932c0e398852e38302be30b9015481九个Git blob，核interface／reader／context、OpName、
Map、prompt、planner、executor与KV manager。原版没有SemExtract，data_path的CSV／JSON内容只转为文字context；
KV估计仅用tokenizer，没有图像processor／视觉token展开。原chat请求底层能接内容数组，不替代semantic-query的实际图像carrier。
这是固定源码结论：原版前端未提供该支持，本项目图像工程适配也尚未实现，不能混同为缺数据或只缺一个题目开关。
原planner支持多算子，此限制不等于整个Kalypso不支持多阶段；Q2b／Q12的实际图像后继路径仍未接通。
源码复核SHA-256为`d38cd2a8df3e551bf623a6e2a7ea24e8266236e54f9ed1e7d778c18621ee1920`。

现同一个KalypsoRunner新增sembench-extract-map明确工程profile，复用作者SemMap调用、KV／长度／ratio估计、
采样、length到8192重试、原transport／trace／owner；只从原Extract输出字段构造JSON任务指令并恢复关系。
没有新SemExtract、查询DSL、逐题执行器或第二个runner；原Map入口仍拒绝把Extract换名冒用。
LOTUS1.2.4 extract_postprocess明确只作兼容后处理，保actual keys、str(value)、NULL、额外字段、重名覆盖和标签写入；
原题读数、普通关系、完整结果和原评分保留，未向native请求透传response_format，报告标engineering variant。
原stage的JSON格式是任务元数据，不是已确认的原生API参数；实际作者SDK／模型／renderer与资源清理仍pending。
输入准备、必需CSV／指令表、nativeMap、解析及完整sink仍计入原独立时钟，后置审计／评分／写盘在外，旧时长不改写。
原Map内联关系恢复已移动为共享方法，新的Extract handler只覆盖该方法及身份，没有遗留两套传输／审计主体。

7aa61cba8干净导出加九份准确文件，十三完整模块95个方法：93通过、2个既有Join／TopK tokenizer门因未绑定而跳过，
0模型／GPU。新增七方法覆盖五个真实原题软件完整流程（MMQA Q1／Q4、Medical Q10、E-Commerce Q3、Cars Q10）、
原评分、显式profile与2N上界、冻结输入、原始null／键／列覆盖／缺键及空Extract错误；
直接运行固定作者Map的模板／估计器／方法，API与KV依赖为软件fixture，不能称完整目标SDK验收。
另用真实作者reader及parse_ops证明图片JSON仍是文字、sem_extract被拒绝，避免用底层vLLM能力虚报前端资格。
最终软件结果SHA-256为`a5839eef76e72337f2478f13a6f94e0579512892549ae99013f42f8bee1f9ee7`。

首次五方法暴露三个fixture错误：Cars误选Filter Q1、Medical原评分修改传入结果大小写、假模块名称被author scope覆盖。
修正场景／在评分前冻结预期sink／使用确切模块键后通过；没有更改原题或评分。
首轮整合还有旧prepare包装器不接kind参数的九个subtest错误，透传kwargs后原受控source→sink时长检查通过；失败日志均保留。
这些不是新的模型结果，后续由既有服务器协调者先验实际原生CPU规划／完整prompt，再做有界两轮代表text Extract。
图像输入和图像Extract不从本次纯文本profile取得资格；取消的Daft TopK及三份保护草稿未改。

<a id="ray-serve-native-hook-repair"></a>
## Ray Serve 原生启动 hook 冲突与 callback 修复

7aa61固定候选在服务器通过55项CPU、原生配置／请求及120条独立JSON Movie Q8消息的原生模板／完整token门，
但三次原生初始化均在生成前失败，实际generate为0，没有查询或暖轮结果。vLLM EngineCore的ray.init
收到Ray Serve的worker_process_setup_hook，同时继承本项目observer的内部hook标记，Ray一致性检查拒绝两者冲突。
242次预算预留已关闭、不退还，原失败、重启尝试和资源释放证据保留，不改写成通过。

修复删除Serve adapter的export_setup_func_module接线，使用原生LLMConfig.callback_config指定
NativeObserverCallback；其继承默认CallbackBase，在on_after_node_init调用父方法后安装原有AsyncLLM观察器。
节点初始化、原生worker hook、执行器、预算及模型构造／请求观测保留，不改引擎或禁用观察器。
SDK检查也构造相同callback配置。依据是[固定Ray LLMConfig](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/llm/_internal/serve/core/configs/llm_config.py)、
[原生callback](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/llm/_internal/common/callbacks/base.py)及
[构造顺序](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/llm/_internal/serve/engines/vllm/vllm_engine.py)。
七个官方Git blob与目标失败包的实际hook一致性函数分别核对；未把官网版本验证当作目标安装环境通过。

e67cc干净导出加五份准确源码／测试，四完整模块70个不同方法全部通过、0skip／模型／GPU，
覆盖callback先后顺序、原生context／hook保留、安装失败传播、原observer预算及完整查询／关闭旧路径。
官方真实CallbackBase分派和目标一致性函数的零模型回放通过，旧冲突可复现、新合同无冲突。
CPU结果SHA-256为`5c652cae4da744d6170a2e0ce4869facf936d49d4e80b9327801b8cc6590f086`，
源码回放为`6e6cc9c80188bed078971ca19aa8190e0119521d3bcb0758f6eae53170f11f8c`。
修复后的目标SDK callback配置／完整启动、响应关联、同owner两轮及物理清理仍待服务器验收；无新模型调用，不授予暖轮资格。

<a id="lotus-kalypso-text-model-20261114"></a>
## 原 LOTUS Q6a 与 Kalypso Extract 工程任务首／重复轮

两份具名回执使用SemBench c814的完整官方sf25，各25×2真实请求、全部终态、unknown／额外预热／重试为0；
各自模型owner保持不变，原完整关系／parser／评分回放通过，不能把后置F1等同逐项全对。

| 系统与准确候选 | 原题／策略 | ready首次／重复source→sink | 原质量／边界 |
|---|---|---|---|
| 原LOTUS f26fc3f6e | MMQA Q6a原Filter与机场投影 | 0.884563632／0.212812939秒 | P／R／F1均1；结果缓存关、prefix开，模型启动32.093948773秒另列 |
| Kalypso e67ccfcb7 | MMQA Q1，显式Extract-via-SemMap工程任务 | 1.202610809／0.409334223秒 | P／R／F1均1；作者SemMap策略、原Extract parser及关系评分，非作者原生Extract |

Kalypso原caller→内存sink为9.731334692／0.440251780秒，模型服务启动40.208845454秒另列，
保留与独立source时钟的差异；JSON只是工程指令、未透传原生格式约束。CPU完整token数组与每次实际API消息hash已核，
运行期模型token IDs／usage未独立观测，不以CPU计数填充运行期字段。LOTUS运行期token／输出长度审计已核。
首次LOTUS私有绑定误用Airlines/Text列而非原Airlines/Destinations，失败保留，仅修复并重跑受影响的零模型门。
两运行物理资源释放与租约关闭通过；不作跨系统性能排名，不转授其他题目、后端或Kalypso图像支持。

本会话仅只读核验三包全部4255份payload、3546件对应Git源码blob和归档哈希，未重复模型。
完整核验SHA-256为`6750610debacdac3b28712bb8045b42ec7611420fedce9a1f170004188c980ba`。

## 原件恢复与独立核验

下列私有归档由服务器协调会话《统一安排服务器 GPU 测试》按运行标识提供，完整清单、配置、
source binding、日志和观测保留在归档内；公开仓库不复制真实机器路径、模型回复或runtime配置。
本接入会话重新核对每个payload及清单的字节数／SHA，未重复执行模型。

| 归档身份 | 字节数 | payload数 | SHA-256 |
|---|---:|---:|---|
| 4f12 CPU | 见原清单 | 91 | `0c5a7feb696868ba9ba3eb802f5a3229e77fce9f3e9dbed353034c1098b1717e` |
| 4f12 GPU | 见原清单 | 111 | `b7ae2399c9b0278a549069a60cbe2a96e0b27d95078b421b7e77f01a32b49695` |
| 11eff CPU | 123286 | 102 | `9234bcc5e0185aeea54684b21ded6ac54768c7a16c0b993fcf1d056c6002fc36` |
| 11eff GPU8 | 652933 | 220 | `d310d0585d086d823c923c7dcbb32888bc1b614f20a0084555f8ffe9a89e21de` |
| 8667 SQL CPU | 342404 | 177 | `ac98ad437e07f71486442606720d0622134c0d833e0ff1c1304f1130b1487b73` |
| 11eff full256 CPU | 15750581 | 60 | `161175a54e3c5c92959394ceb36e46130774c15ca1c8611ea288047cb2e73fb8` |
| 11eff full256 GPU | 1170008 | 109 | `0756f60790a1626e8e31c0a3ebbcc6fa85abdb700b279863c6950ae7e5ab6565` |
| 8667 model64 | 242405 | 148 | `8f0fdc209c4bf4e6b088011b8971c40af8d713403101b1e84e4b4bf49c78a237` |
| e95 Daft Linux CPU | 27635 | 33 | `63ca0761aae0d0464c6ad0d02db07d13d7ec1f0cbd41ce38799122ada6ee2f96` |
| Cars／E-Commerce检查 | 388677 | 63 | `61cc26fd6cec44c6a6bfb76eb4fad28fb088182c9c236ed98a0fa5c14e7e9b54` |
| E-Commerce恢复记录 | 259761 | 22 | `e39f956c1d2641942dbbaca547258a48fea4d398800a2f7eeffd8c5cf80785e1` |
| 60c875 Linux CPU | 85531127 | 1284 | `df2d87b0add4023028c4415ab8814370b7164c66ea67b06c9c939317b8fdece7` |
| 60c875 Ray Q1 GPU64 | 82065595 | 216 | `c752bb34fede696daee37b1e661d8c3ba063dea7b0e02d1dce4ea0914ccd56b9` |
| 40c477 Linux CPU | 3391607 | 1209 | `f009e8dd5d1cceba72ad7e97faf50765edf544730623795ea6b9c89ff63f94f9` |
| 8667 MMQA Q2b GPU | 59392914 | 94 | `7d733719a54b8de5eb560c3b9096d4247a82cbfb73d8ca38537ac3d0a9cd4840` |
| cb754 LOTUS full256 CPU＋GPU | 105729722 | 1269 | `8f7fb25b14800c0076c0cfcd3a3b22a50357bdd4149ef8d73ade435e0e3c8bd5` |
| 0755 Extract阶段准备CPU | 3295145 | 1205 | `8b89d4b52fed0f5a7ba934ce5838c7f065a6338bf6d6b509dfe96479ea84b715` |
| cf220原生IMLane Q3模型 | 779015 | 138 | `304bdd0206a3d1e1c5da8a74987a660021d14bd3210a648d01a5c35a02a97ae4` |
| 051639最小CPU＋模型 | 3930503 | 1578 | `a6d9e929568bad9f9f8e74ddcb9d3cdf406f06aa6d76147a4872834b4ed572e0` |
| 051639 Ray Animals Q1 sf64 | 84905964 | 1431 | `8ce429180056911dc8e0d7e2ce67a0eda19a045fd85dc7cfc27e7818dad4fa02` |
| 807224 IMLane Q5单叶 | 3741666 | 1367 | `e648fc1de0bcee4f4aad4cf0ca678f8b32bb7c12e498ac7dc6684c1a702361b0` |
| 3500 Kalypso Q5单层 | 3129361 | 1368 | `3549da07180f31519880da6412f7dacec24547ddad33f300dfbc4de54bc320f0` |
| 4073 Ray Q7及旧失败IMLane参考恢复包 | 3541250 | 1694 | `461c67847a7f867a8da420401350e1d23a8a8f677330c427ceef1b5927d81a27` |
| a1105 IMLane Q7参考恢复包 | 3683997 | 1492 | `0cae1fa63714d42c8ac671c0cec38de62038b5e07681df60470240ecc6560c2c` |
| a1105 LOTUS Movie Q8首／暖轮 | 4256954 | 1315 | `5a4c447d64fa58ce56a66abb584640bcfe0020bb604d5811d7ad728e873017b4` |
| a1105 LOTUS Animals Q10参考恢复包 | 3110267 | 1370 | `e45b1773729537c2a97155837a4d601388a5cca0e9735b3dd604e86fdcc4efcc` |
| 7fb4 独立原生Ray／Daft Movie Q8 | 3738751 | 1669 | `e1e87b4b76bf6e8c763440ca61c06b0335a21f5720068ca3e0ae5a21cf535659` |
| f83b 独立原生Ray／Daft Movie Q10 | 6977259 | 3407 | `91775f68b25259c7b5f27b2e3678d29d51d50f3240e5dc9ed1510af653e65b90` |
| f596 独立原生Ray／Daft Movie Q1 | 7036724 | 3464 | `af47bc3acf443b5ed3032223a05115862321a7833e024b2f3179adcd9aa25a54` |
| 608e20 独立原生Ray／Daft Movie Q9 Map | 3874724 | 1751 | `0ec63e7473afb647c7bd4b3b3aef13b7d0f967d0bca6c1a9695816a8be32750a` |
| 8d071 独立原生Ray／Daft MMQA Q1 sf25 | 3264095 | 1516 | `62a01b273b0aebb9d09fcf47f359401e4b412deff1617c8d9bb24adcbbda6638` |
| 8d071 独立原生Ray／Daft MMQA Q4 sf25 | 3238620 | 1512 | `75ebd31de446926e21239e08f327b728c65e3cdec35b567ac7630e71816f2999` |
| 0d0cc 独立原生Ray Animals Q7 sf64恢复包 | 3356087 | 1579 | `bef43365126e7f61f56f45cb48a958acfdd13512dbf05fdc78c0f8784b40a8b2` |
| 0d0cc 独立原生Daft MMQA Q6a sf25 | 2983339 | 1289 | `6a1d26d0b8dac40fd815bafd5dbd6809eb115b524b42bbcc21f88a31324e6c49` |
| 6c75 LOTUS＋Daft Animals Q7 sf64 | 3361273 | 1482 | `34e53d7269e3efa1c3105b2c21e5c62cf536b49abd8859581b2f8dbe64eb10b1` |
| 4793 LOTUS＋Daft MMQA Q2b sf25 | 240934848 | 1425 | `1d87ce019eec252036af50850ce5d28b3b3f118ffefde23bf9cec6ca598324c4` |
| 606f 独立JSON Daft Movie Q8同owner两轮 | 3790208 | 1482 | `b1aee233c72bdbcce806fd6a1ab70437bf3dff3700b97071735335d089b35930` |
| 606f 独立JSON Ray Medical Q10完整1200行 | 4447628 | 2628 | `e5dcff3e78389d7579d9986f8fd1107e48d44a9c438af5232cf240859e71c4cd` |
| 606f 独立JSON Daft Medical Q10同owner两轮 | 7076616 | 1348 | `6c105a256431036c47720bc70f457d51b6aa9542f9c082069b285734a75c6780` |
| 8cd 独立JSON Ray MMQA Q2b非空 | 183867006 | 1846 | `24213d8d97114011c2173d12b5210ccd13c5a9753b512fdb5676cc788dfa6786` |
| 8cd 独立JSON Daft MMQA Q2b首／同owner重复 | 600396690 | 1418 | `bccf8e0c801ef7b9c5a7bc2ab92d989cc26965e17fe6fc90f22d98de4a14d3b0` |
| 8cd 独立JSON Ray Medical Q4 | 4429887 | 2614 | `358c12ad7eae7a08418b543458bb83515a0d1baebe534b3936bbed0388001a2f` |
| 8cd 独立JSON Daft Medical Q4首／同owner重复 | 7638119 | 1355 | `3b270486497b772ebcfb570941a334cfcdfd042a2954db22b952d8b2cb1d1a7f` |

| f26fc 原LOTUS MMQA Q6a首／重复 | 3257623 | 1377 | `4f273596c42b815c243501ed04f7b0012dd6154b2370f834f236114e516f6bfc` |
| e67cc Kalypso MMQA Q1 Extract工程首／重复 | 3325383 | 1420 | `e30c45647708de3f1ef19c754a266f3ae51d0778b9d4441d452891db8699e07d` |
| 7aa61 Ray Serve Q8启动失败／0生成 | 3533532 | 1458 | `99570e54a00fd70c99258899fb9f5169722380c4ceb06559a8428c7062a3c18b` |

payload数不包括每包另存的一份清单。已完成的资格不重复请求；缺数据、SDK接线限制和未运行模型
分别记录在[当前状态](../../../../code/INFRA_STATUS.md)，不由本报告扩展支持范围。
