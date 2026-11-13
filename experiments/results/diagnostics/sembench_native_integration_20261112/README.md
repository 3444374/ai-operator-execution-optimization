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

payload数不包括每包另存的一份清单。已完成的资格不重复请求；缺数据、SDK接线限制和未运行模型
分别记录在[当前状态](../../../../code/INFRA_STATUS.md)，不由本报告扩展支持范围。
