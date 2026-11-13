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
Map首轮已执行SQL，但私有参考callback误把sem_map的DataFrame按TopK(frame,stats)拆包；
生产代码未改，仅修正该harness后补Map软件门，原失败保留。不能把参考脚本失败改成第一次全通过。

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
本批未运行新模型；原Q9历史成绩不继承新时钟，目标SQL验收仍待进行。

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

payload数不包括每包另存的一份清单。已完成的资格不重复请求；缺数据、SDK接线限制和未运行模型
分别记录在[当前状态](../../../../code/INFRA_STATUS.md)，不由本报告扩展支持范围。
