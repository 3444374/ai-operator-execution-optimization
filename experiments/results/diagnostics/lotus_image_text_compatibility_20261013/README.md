# LOTUS图文执行接点与Sema输入核查

早先切片只验证固定前端能力和同方法执行接点，没有真实模型、GPU或Ray启动。
后文追加真实Daft/Ray及localhost软件fixture；[审查修补](#stage-review)纠正旧RSS与关联取证边界，
最新[块尾部诊断](#stage-block-tail)在原配额下未发现准备lease拒绝，未扩策略或GPU窗口。
没有真实推理或PG-E2E性能资格。
独立研究原型为`92bcd05a`；通用LOTUS接线为`607b9f88`，只改`lotus/sdk.py`、`lotus/batch.py`并增加独立测试。
研究假设和静态缓存反例仍见[阶段设计](../../../../docs/research/heterogeneous_ai_dataflow_execution_model_20260811.md#frontend-stage-audit)，
这次接线不采用准备缓存，也没有新增调度或性能结论。

## 实际配置与结果

运行标识`semloom-stage-20261013.z3x6arlg`；既有Python3.12.3、LOTUS1.2.4、LiteLLM1.95.0、OpenAI2.50.0、pandas2.3.3、Pillow12.2.0。
使用两个CPU（24、25），没有安装依赖、下载权重或操作既有模型服务。所有响应来自拦截、MockTransport、本地回调或localhost替身。
首次将测试亲和范围用于整机预检，因2个CPU低于机器profile的10槽要求而失败，原件保留；按整机128槽重新观察通过，实际测试仍只有两个CPU。
开始/就绪wall time未单独采集；unittest运行时长保留在日志，仅作复现信息，不作查询性能值。

| 检查 | 实际结果与范围 |
|---|---|
| 新图文profile | 9/9通过，无跳过；单图文Map每臂4个逻辑调用，Filter→Map每臂4＋2个；完整JSON值、内容列表顺序、原parser/usage、行序与清理一致 |
| SDK错误 | 每臂4个429模拟调用，原生LM与SemLoom错误类型/状态一致；没有真实HTTP模型请求 |
| 初次旧文本回归 | 54项中50通过，4项失败或错误；在原`92bcd05`的SDK/batch及同一环境复现相同4项，原件保持；接续诊断及修后检查见下文 |
| 固定Sema | 二进制`v0.0.1 8c2d3bd2`、SHA-256 `15a534667a668a152524a8756fe5d66c0fa9c9822e73d26029e1a71693a56ec3`；函数目录没有image/video/vision项。两条普通VARCHAR语义投影分别给PNG路径和data URI，2次localhost调用均为字符串content，没有image_url列表 |
| 已有模型配置 | 已配置根目录的4个config只见Qwen2ForCausalLM、MistralForCausalLM和CLIPModel；CLIP是视觉embedding模型，本次没有核实可服务该图文查询的生成式VLM |

4项旧失败分别是：row-limit旧错误文字断言；不完整JSON响应期待ValueError；Unicode用量用例拿到InternalServerError而非usage；Wait用例patch已不存在的`batch.lotus_response`。
首次回归包遗漏测试helper导致额外导入错误，补齐公开helper后重新核对；初次与修后记录分开保留。
Sema只测试了公开普通语义投影的VARCHAR入口，不由函数目录或两条探针推断闭源二进制绝无其他未公开接口。
论文中的视频出现在相关工作，不是本产物的视觉能力证明：[Sema原文§8](https://arxiv.org/html/2603.11622v1#S8)。

## 接口与证据

`LotusBatchExecutor(..., message_profile="lotus-image-text-v1")`显式接受固定LOTUS生成的PNG data URI图文列表，
额外核对`utils.py`、`dtype_extensions/image.py`和`sem_filter.py`，整个图文批次在打开flow前使用原SDK检查。
默认text、原native LM、生成参数、完整请求及响应重放继续使用原路径；profile字段不进入模型JSON。
配对比较完整JSON值和有序内容，不声称不同HTTP编码器的原始wire字节相同。
image消息仍使用现有完整调用的request-count工作量，不代表视觉token/CPU decode代价已校准，也不声明模型有视觉能力。
视频/音频、任意URL、两Map继续执行中的图像状态、PG图文接入及Daft/Ray图文替换仍未验证。

[测试源码](../../../../code/tests/semantic_methods/test_lotus_multimodal.py)、[接口说明](../../../../code/src/semantic_methods/lotus/README.md#image-text-profile)、
[验证元数据](verification.json)与[脱敏日志](raw/evidence.tar.gz)可复核；压缩包含9份必要观测和`member-identities.json`的私有/公开双摘要。
私有原件按上述运行标识定位。源码上传首次被自动审批拒绝；逐blob证明369文件来自已公开`92bcd05`、其余3文件属于已授权接线、372文件隐私扫描通过后，重新提交获准。未绕过拒绝或移出其他私有材料。
后续真实视觉模型、任务质量、阶段画像与性能比较另需主任务协调能力、来源、预算及共同基线；640/15分钟只是旧建议，本轮未启动该窗口。

<a id="four-failure-diagnosis"></a>
## 四项失败诊断与修后验证

修复提交`aa3338ce`；接续`repair-1`在同一固定SDK、同一两个CPU上使用模拟响应，55项文本＋11项图文共66项全部通过，无跳过；
UTC检查范围05:00:21.709–05:01:27.067。该范围是验证进程wall time，不是查询性能。
本地对应检查有27项通过、39项因缺固定依赖跳过；最初将纯profile用例直接调用batch时缺OpenAI，
随后把纯验证和实SDK零提交用例分开，不在本地补装依赖。真实模型/GPU/Ray启动仍均为0。

| 初次失败 | 差分依据与处理 |
|---|---|
| row-limit文字 | 现有`LotusCallLimits.preflight_rows`已使用“selected executor capacity”；只更新断言，额外核对target rows=2、capacity=1及零HTTP/零资源 |
| 畸形JSON期待ValueError | 当前执行接点把原SDK的OpenAIError子类作为响应对象返回给原LM，LM再抛出；使用原生LM作为异常类型对照，完整status/body仍保留，byte-limit的ValueError断言仍独立覆盖，未改parser或生产错误路径 |
| surrogate输入用量 | **真实输入有效性缺口**：原生LM对Python字符串`\ud83d\ude00`发送0次，报InternalServerError，cause链含UnicodeEncodeError；旧prepare的JSON副本把这两个码点合成`😀`并提交，回放原输入才失败。现在在JSON副本前检查UTF-8编码，移除suffix规范化fallback；有效标量`😀`继续原路径 |
| Wait过时patch | `batch.lotus_response`已不是实际接点；patch当前`parse_lotus_response`并调用真实SDK解释路径，转换前仍检查held_tasks=1、result_bytes=8192、active_requests=0及LEASED，完成后资源归零；未改生产生命周期 |

编码预检覆盖纯文本、图文text部分和data URI字符串；非法输入直接抛UnicodeEncodeError，
不声称与原SDK包装异常类型相同。文本启用`prevalidate_batch=True`时、图文强制整批预检时，
首行及非法后缀在任何flow前拒绝。新增实SDK文本/图文负例确认0个HTTP请求，合法中文和emoji正例保留。
源码只修改`sdk.py`，测试修改adapter/wait并扩展独立multimodal测试；不修改公共HTTP、计数器、调度核心或其他owner文件。
公开HTTPX cookie隔离`aaa88e28`与本问题的SDK序列化、返回类型及测试接点没有关联；本分支未吸收，留给集成owner协调。

原`raw/evidence.tar.gz`和verification的初次失败字段保持；
[接续日志](raw/repair-evidence.tar.gz)包含7份测试日志、测试/源码身份、差分诊断、模型/图像只读检查，
共12份观测加双摘要清单。归档SHA-256为`74ebe07755a4006ab0d33aed4afd0fce26e9d342353818c95d817117c7273b9b`。

<a id="real-image-prerequisites"></a>
## 最小真实图文验证的现有条件

只读检查范围为既有配置模型根目录的4个top-level config及两个明确的数据根目录；不推断机器其他目录绝无VLM。
已有Qwen2.5-1.5B/7B-Instruct和Mistral-7B-Instruct-v0.3配置是纯文本生成架构；
CLIP有vision config和processor，用于embedding，不能承担本图文生成Map。只数现有weight文件，
没有重新散列大型权重、import模型、启动服务或下载新文件；未核实可用生成式VLM及其processor/revision。

既有COCO val2017目录现有5000个JPEG，12个历史CLIP资产全部仍存在，逐字节SHA-256与历史manifest一致。
在这两个根目录深度3内未找到instances/captions标注文件，图文任务gold仍须单独核实。
这12张图经既有Pillow12.2.0做RGB/PNG/base64准备的表示范围如下，源文件保持：

| 表示 | 最小–最大字节数 |
|---|---|
| 原JPEG encoded bytes | 62,406–204,829 |
| RGB逻辑字节数（宽×高×3） | 562,500–983,040 |
| PNG | 202,453–653,891 |
| PNG data URI | 269,962–871,878 |

12个data URI单独都小于当前默认1MiB请求上限；完整prompt/JSON仍需逐请求测量，
原JPEG的256KiB输入上限不能代替RGB、PNG、base64或同存副本的额度。
RGB逻辑字节数不是allocator峰值，未采集准备时长、pinned/device内存或视觉token。
151次历史CLIP forward与数据库清理证据仍只证明受限embedding路径，不扩展为图文VLM或性能资格。

建议先安排**至多38次真实调用、10分钟的独立接点/质量窗口**，本轮未启动或申请：

1. 主任务先核实已有/另授权的生成式VLM完整weight、processor/revision、image_url服务能力、GPU/服务归属与预检；协调baseline owner空闲窗口，固定模型/参数/超时/零重试。
2. 在既有manifest中预先选4张可审定gold的图，加2个复用行、每行独立文本，冻结6行顺序、摘要及质量判定；不按模型输出成败换图，不把CLIP标签当生成任务gold。先以0模型调用核对原LOTUS消息、完整body大小和原parser。
3. 原生LOTUS与本图文接点各跑单Map6次，再各跑Filter→Map最多6＋6次；共享prompt、输入、原formatter/parser与参数，最多36次正式调用，加至多2次服务预热。超时、错误、结果形状或行关联不符即保留原件并停止，不扩展新数据或服务。
4. 此窗口只判真实视觉能力、任务质量、同方法完整输入/输出及生命周期。准备复用原型尚未接入LOTUS formatter；后续才比较原native、强静态有界stage/同信息缓存与资产租约候选，分别测CPU准备、序列化/移动、队列、GPU服务、聚合、真实驻留及完整查询，不能从66项模拟检查推导速度收益。

该预算先于§11.4的扩大阶段画像建议。具身轨迹可在未来把同一资产标识用于多个任务共享的已选帧，
帧筛选若改变任务输入须由算子语义/数据规范授权；本轮不引入机器人数据、视频API或新选择器。


<a id="stage-a-demand"></a>
## 阶段A：实际条件需求与释放边界

实现提交`27815f3e`；本切片只扩独立`PreparedAssetWindow`，不改NativeCall、PG协议、公共HTTP或调度核心。
有来源的可选metadata引用既有WorkDescriptor/StageWork；成本、局部性、主阶段区间与校准来源不另造接口。
`ready_after/resolve_stage`只接受原阶段实际输出，未就绪的Map不prepare或提交；snapshot可选记录缓存payload字节秒。
source版本来自历史manifest并重新散列核验，原图尺寸来自header，变换签名包含固定LOTUS源码摘要/Pillow版本。
工作量是RGB逻辑字节和每行1个完整调用，没有GPU服务/视觉token成本校准。

固定SDK最终21项（20资产/合同＋1实际LOTUS查询诊断）全部通过，无跳过；CPU24/25，依赖沿用既有环境。
本地20项资产、3项WorkDescriptor、4项stage、8项broker、2项原源码反例共37项通过；实SDK查询在本地缺固定环境而跳过，远端执行。
首次最小上传没有测试目录package标识，带top-level discovery的两项在加载前失败，改成对应独立目录discover后通过；原失败不覆盖。
随后去除初稿重复的源字节字段，改引用WorkDescriptor，并增加身份去重的物理表示观察；各阶段源码身份与原值分别保留。

12张既有COCO内容/文件均唯一，没有复制行或新下载。实际原Filter→Map方法以相同图文消息、参数、parser及原LM执行；
替身仅按图像宽高返回方向，Filter保留7行，Map直到完整Filter返回才有合法需求。它不是VLM语义质量验证。
8个配置每次均19逻辑调用，完整请求正文哈希/顺序、行关联、输出及usage一致，均提交9,747,951字节HTTP正文。
这只是拦截正文长度，不是网络wire或H2D字节。共152个SDK替身调用/每轮，真实模型/GPU/Ray启动均0。

| 配置 | 缓存范围 | prepare次数 | 另复制URI字节 | 缓存payload字节秒 | 模拟query wall秒 |
|---|---|---:|---:|---:|---:|
| native | 原cache，无新额度 | 19 | 0 | 未采集 | 2.840734 |
| static_uri | 4 MiB | 17 | 0 | 1963566.086 | 2.122156 |
| static_bytes | 4 MiB | 17 | 9736050 | 1950505.078 | 2.086052 |
| consumer_bytes | 4 MiB | 17 | 9736050 | 1932794.027 | 2.071406 |
| native | 原cache，无新额度 | 19 | 0 | 未采集 | 2.217901 |
| static_uri | 16 MiB | 12 | 0 | 5003074.131 | 1.515097 |
| static_bytes | 16 MiB | 12 | 9736050 | 4982682.521 | 1.510543 |
| consumer_bytes | 16 MiB | 12 | 9736050 | 4966075.094 | 1.507238 |

4/16MiB只限制自有/静态共享缓存及声明的准备scratch，不限制原生ImageArray或全查询host内存。
上表各一次，顺序执行；元数据获取另记录，未纳入该query wall。没有真实推理、数据库写回或完整E2E，不能据此作速度排名。
URI静态是强控制：直接共享原字符串；bytes静态与消费者方案使用相同表示、同序、同信息、同scratch额度与last-use。
它们的prepare顺序也一致。17/12次准备相对原19次的减少完全由普通共享缓存解释，消费者组织没有额外减少工作或正文移动。

**输入读取、模型结束、结果消费不能合并。** bytes exporter复制为独立字符串后结束其真实读取，但Filter消息仍持有
6,196,536字节URI，Map为3,539,514字节；请求池返回前后都相同，原SemLoom SDK回放也读取原messages。
bytes方案另外复制9,736,050字节URI；字符串静态缓存与消息共享对象，不重复计物理内存。
Filter边界已知图像对象的Python字节数为原生/URI静态6,197,028，bytes方案4MiB时7,674,768、16MiB时12,393,960。
仅按对象身份去重已知图像表示，不包含所有临时buffer、请求、进程RSS或pinned/device；cache字节秒不是全物理驻留字节秒。
lease归还没有伪造模型/设备结束或结果消费回执；结果仍由原LM/算子持有。

**本轮判定：A没有超强静态的组织机会或净收益证据。** 保留先前4→2反例，暂不推进多副本B或真实模型窗口。
现有数据库图像入口只核对SemanticImagePlan与已有WorkDescriptor引用，没有PG共享准备接线/E2E证明。
下一切片沿现有数据库入口，必须由真实语义查询提供资产版本、合法后继及取消/消费事实，再证明可控准备/模型关键阶段；
不将纯文件转换强归SemLoom，不以本LOTUS fixture替代数据库证据，不扩DROID/视频/新数据平台。
原始文献边界与后续假设见[现有阶段设计§11.6](../../../../docs/research/heterogeneous_ai_dataflow_execution_model_20260811.md#stage-demand-lifetime)。

[独立模块](../../../../code/src/data/materializers/prepared_assets.py)、[资产检查](../../../../code/tests/data/test_prepared_assets.py)、
[实际查询fixture](../../../../code/tests/modalities/image/test_lotus_stage_demand.py)、[验证元数据](verification.json)及
[全部阶段原值/失败](raw/stage-demand-evidence.tar.gz)可复核；18份观测加私有/公开双摘要清单。
归档SHA-256 `abb224268b7694b79b3cdf11b88e63fa4b5cc4fc58084e9f3fc1c2d8a6ed5623`，私有原件按原run标识和stage-a-1/2/3定位。

<a id="stage-a-daft-ray"></a>
## 阶段A的真实Daft/Ray软件接线验证

本节是历史首轮，数字及原件保留。旧fixture跨轮持有完整请求正文，且同向Map输出不能独立验证行关联；
RSS与关联结论的修正及新资格见[审查修补](#stage-review)。

复用现有`LotusBatchExecutor(image-text)`→`NativeTaskSession/SessionEngine`→
`MapInputPreparation/iter_payload_batches`（Daft native分批）→`RayMapTransport`（Ray HTTP actor）→原SDK重放。
解码仍由原LOTUS前端完成，`physical=None`不包含Daft/Ray；PG typed CLIP是另一条既有CPU/GPU stage路径。
本轮只补软件fixture，生产默认与执行框架均不改。

固定12张旧图、原Filter→Map提示/parser、4MiB同表示共享准备额度及相同后继信息，C=2、held=4。
强静态bytes与消费者bytes各2次交错重复，另各1次已知429关闭，至多100个localhost替身POST。
沿用已有Linux环境，独立GPU=0 Ray集群；CPU24–27、600秒外部硬截止、4GiB采样RSS停限，失败不扩矩阵或策略。
主任务wall从本次专属源核验/元数据准备到结果消费结束；另报已ready查询与关闭时间。
图片准备、SDK编码/解析、Daft/put及Ray等待用原始区间保存，重叠累计时间不相加为主任务wall。
50ms进程RSS与声明cache/Arrow字节分别记录，RSS加总不等于唯一物理驻留；缓存读取结束不推出URI/模型/结果消费结束。
该路径的分层须准确解释：LOTUS的ImageArray/fetch_image在前端准备URI，原formatter产生完整消息；
Core拥有准入、工作额度与结果租约；Daft把Core已接受的有限payload窗口转成Arrow批，Ray拥有对象与HTTP actor执行。
`MapInputPreparation`复用既有broker管理JSON/Arrow输入，未把图像decode移动到Ray，也不是Ray Data原生整查询调度图。
Daft Native完整调用baseline由其自身graph调度，与SemLoom内部Daft materializer是不同角色。

源码以`b8b9b865`为基底，两份fixture按[源码摘要](stage-ray-verification.json)冻结，生产代码未变。
沿用Python3.12.3、LOTUS1.2.4、Daft0.7.21、Ray2.56.1、Arrow24.0.0、Pillow12.2.0，既有preflight通过，无安装。
实际窗口T0为08:43:51.478632 UTC，原硬截止08:53:51.478632；最终监督在08:50:54.235634退出。
前两次配置拒绝分别为ready额度大于object额度、API base误含`/chat`，均0请求；只修fixture，原失败保留。
早期LiteLLM导入/零请求probe曾尝试远端价格元数据并超时，最终轮启用其已有`LITELLM_LOCAL_MODEL_COST_MAP=True`。
整个窗口共3次CPU Ray cluster启动，最终1项完整实依赖用例通过、0跳过；共76成功＋24个429 localhost POST，模型/GPU均0。
主进程监督跟踪自有PID＋birth；08:53:06 UTC独立确认10个采样身份无存活、共同host lease已释放、GPU为空。
独立检查先误用缺psutil的系统Python，再改用已有driver完成；没有修改环境。

| 顺序／臂 | 图片prepare | URI复制字节 | 主任务秒 | ready查询秒 | driver采样峰值MiB | 同时RSS加总峰值MiB |
|---|---:|---:|---:|---:|---:|---:|
| 1 static_bytes | 17 | 9736050 | 4.714388 | 4.698094 | 1178.16 | 2317.26 |
| 2 consumer_bytes | 17 | 9736050 | 2.708299 | 2.693082 | 1202.75 | 2351.04 |
| 3 consumer_bytes | 17 | 9736050 | 2.834861 | 2.822550 | 1218.75 | 2368.62 |
| 4 static_bytes | 17 | 9736050 | 2.723388 | 2.713941 | 1231.00 | 2382.39 |

四次均为12唯一源／7后继／19完整请求；原请求JSON、prepared顺序、四臂输出、parser与usage一致。
这不是独立行关联oracle。HTTP到达异序也不能代替完成异序取证；每次准备payload总字节9,747,951。
原始HTTP wire、H2D、pinned/device驻留未测。
两次429流程各12完整请求、12次图片prepare，Map未执行；原异常status=429，消费者需求取消后缓存归零。
六流程在消费边界Core held/input/result/request/work及broker encoded/ready/model/object均归零，关闭回执为true。

主任务从本次源SHA/header元数据核验起，到Filter/Map结果转成原行输出并消费结束，包含图片准备和准入/结果等待；
ready查询起点在`sem_filter`前，关闭另报。共同Ray/actor ready为8.462728秒，最终fixture整体25.441468秒；
监督含解释器导入及退出为42.374281秒，不能用25.44秒替代原600秒窗口累计预算。
各stage原始区间在归档：首个static的图片prepare累计2.103919秒、SDK prepare0.313750、SDK parse1.188710、
Daft/Arrow payload0.979224、put0.090394、Ray await0.613691；这些重叠区间的累计数不能相加为主任务时间。
未单独加入预热轮；首个Daft/SDK解释冷导入明显，四样本不作速度排名，也不删除冷样本或据此另扩重复。

RSS用已有ProcessSampler每50ms观测driver及其自有Ray子进程，采样从backend绑定到关闭；源元数据前缀未采样。
子区间峰值可能漏采；各进程RSS有共享页面，表中同时加总不是唯一物理内存。driver RSS随顺序上升，两臂均未显示驻留优势。
4MiB仅约束辅助共享缓存＋声明scratch；broker的encoded/ready/object声明额度与进程RSS分别解释，均不是host物理硬限。
cache exporter读取结束时reader=0，但SDK重放回调仍持有Filter URI6,196,536字节、Map URI3,539,514字节；
Core result额度仍保留（回调所见4MiB是容量reservation，不是响应实际内存），消费后的归零由另一条结果租约负责。
Ray对象在其成员获得确认终态后释放，不能用缓存释放代替模型终态或结果消费；未知远端错误仍按既有合同保留责任，
本轮实际错误只覆盖已知HTTP429，没有杀actor或证明任意未知失败可干净关闭。

**判定：完整Daft/Ray接线成立，消费者准备没有超强静态的准备、复制或驻留收益证据。** 不增加策略或真实模型窗口。
现有配置已核实的生成模型是文本Qwen/Mistral，CLIP只适用embedding；原151次CLIP forward不提供本Filter→Map的VLM资格。
如以后主会话确认已有生成式VLM与质量参照，可先协调两臂各19调用、总38调用／10分钟的接口检查；本轮未启动或申请该窗口。
数据库图像入口、Ray CPU/GPU tensor阶段、PG结果消费、多节点、帧/token成本校准及全物理驻留均未由本fixture验证。

[实际fixture](../../../../code/tests/modalities/image/test_lotus_stage_ray.py)复用原静态控制、历史资产与现有执行器；
[聚合与定义](stage-ray-verification.json)、[原区间/RSS/失败/源码身份](raw/stage-ray-evidence.tar.gz)保留32份观测及双摘要清单。
归档SHA-256 `8ec9cb89fc6b306976bbd06a1b09d511e60891f93f2296bf20ce4cf90018939c`；
原件按`semloom-stage-ray-20261013-hzw91ilu`定位，前两次失败不覆盖，原历史归档保持。

<a id="stage-review"></a>

## 审查修补与重新取得软件资格

基底`9673682c`。只修现有图文预检与阶段fixture，不新增调度器、结果缓存或模型窗口。
[源码与聚合](stage-review-verification.json)按十份源码SHA冻结；[原件](raw/stage-review-evidence.tar.gz)
保留60个成员及private/public双摘要，SHA-256 `63557209531122688d5bb755deeabaed2b1af6335d034f3b0f84d0b374b94beb`。

- 第5行超请求容量、累计输入容量或固定模型配置时，旧实现已有1个flow、4条backend提交；
  修后held=2/4的六个子例均零flow、零提交、零解析。图文逐行完整转换后检查并丢弃后缀，只留首行；
  取消/到期覆盖检查，合法后缀仍按原有界块重做转换。纯文本默认及轻预检没有升级。
- 原来SHA/header核验后重新按路径转换，文件替换可使旧key用于新内容。现在同一不可变encoded bytes
  完成SHA、header与原LOTUS PIL分支转换；超源字节界在decode前拒绝，多帧仍拒绝。固定12源额外驻留
  1,612,536字节，明确独立于4MiB辅助准备缓存。三类PIL输入与固定原fetch_image的离线对拍一致。
- 旧HTTP helper跨轮留存100份完整body，其中URI合计51,337,272字节；这不是实际RSS量，
  但使旧跨轮RSS不能作为缓存驻留优势的证据。新专属fixture只保存身份、摘要、长度、状态和时点；
  handler结束前清除body及其parts引用，嵌套body弱引用回归证明前轮对象不再被helper持有。
- 同向输出互换仍可能保持四臂一致。新独立预期指定索引`[10,30]`与唯一输出
  `['Answer: keep-a','Answer: keep-c']`。gate在sequence 1被实际Core交付后才放开0；
  Filter交付`[1,0,2,3]`、Map交付`[1,0]`，HTTP完成时点同样先1后0；逐行对应与原序恢复通过。
- 混合200/429在原LM用4个内存SDK回复取得异常参照，再经实际Ray的4个HTTP回复核对。
  先交付429时慢行仍占C/W；不提前退款，最终原SDK异常类型/status及完整请求一致，确认终态后关闭归零。
  parser/消费错误、取消和actor未知的五个廉价生产Core/transport回归通过；未知成员保留C/W/held各1及块责任，
  关闭不伪造确认。这是seam故障注入，未在实际Ray杀actor。独立审查另复现并修好fixture清理异常覆盖
  第一异常、跳过落盘的问题；第一异常和关闭错误分别保存。

本地相关55项＋HTTP弱引用1项通过，0跳过；新增部分使用seam，不称为本地实SDK/Ray资格。
沿用Linux既有driver及LOTUS1.2.4、Daft0.7.21、Ray2.56.1、Arrow24.0.0、Pillow12.2.0，
只读preflight通过，无安装/下载。实环境83项旧SDK＋新增回归通过，0跳过，20个localhost HTTPX POST＋2个弱引用POST。
独立代码审查的初次HTTP用例因其sandbox禁bind未执行；本线本地和Linux均实际通过该项。

窗口`semloom-stage-review-20261013-qm1wafys`：T0 10:19:38.142090 UTC／北京时间18:19:38，
原硬截止10:29:38.142090 UTC／18:29:38。CPU24–27、600秒、4GiB采样RSS停限、同一共同host lease。
首次实Ray在19个POST后因新oracle误删原Map的`Answer:`前缀失败；原parser不改，失败及源码身份保留。
恢复仍在原期限和110阶段POST总额内：四成功臂76＋唯一关联6＋混合429的4＝86，累计105。
为守原预算，本轮未重复两次全429；此前两次24请求证据仍在旧归档。两轮共2个CPU Ray集群，模型/GPU均0。
恢复监督于10:24:22.592105 UTC／18:24:22退出；10:25:35.394309 UTC／18:25:35独立新driver
核对26个采样PID＋birth全部无存活，共同lease不存在、GPU计算进程为空。此后无新增服务器负载。

| 顺序／臂 | prepare | URI复制字节 | 主任务秒 | driver峰值MiB | 同时RSS加总峰值MiB |
|---|---:|---:|---:|---:|---:|
| static_bytes 0 | 17 | 9736050 | 5.379852 | 1181.83 | 2322.36 |
| consumer_bytes 0 | 17 | 9736050 | 2.732684 | 1202.04 | 2350.38 |
| consumer_bytes 1 | 17 | 9736050 | 2.739025 | 1208.96 | 2360.28 |
| static_bytes 1 | 17 | 9736050 | 2.840349 | 1213.76 | 2365.17 |

各臂实际准入/准备事件均核对C≤2、W≤2、held≤4、input/result/object声明≤4MiB，
Daft仅借用Core已接受、仍持准备责任的原任务；模型许可之后仍受C/W约束，消费边界归零。
同一准备缓存、source快照和原输入表示下，两臂工作量仍相同；冷导入、固定顺序、采样共享页面及SDK前端驻留
继续存在，表中wall/RSS不作性能排名。source准备前缀不在各臂RSS采样内，外层窗口采样另存。

**原负结果保持：消费者准备未减少prepare或URI复制；驻留与速度没有收益资格。**
图像解码仍在LOTUS整批消息构建前端，未受Core held/C/W统一控制；4MiB是辅助缓存或声明额度，不是host物理硬限。
本轮不证明实际VLM质量/性能、pinned/device/H2D、PG消费或多节点能力。未来轨迹帧选择可复用内容版本与实际消费者回执，
收益仍须面对同信息强静态有界流水和原生stage pipeline；没有新机器人数据或视频/音频实现。

<a id="stage-block-tail"></a>

## 多行Arrow块尾部的有界诊断

通用预检修复`7d83c2a6`的吸收边界及未覆盖项已交集成负责人；本节不把研究策略并入main。
假设：已确认快成员仍随同块慢成员持有ready work，可能挡住同Job的合法下一块准备，即使HTTP C/W有余。
必要条件同时包括编码输入已接受、FIFO头合法、CPU准备槽空、bytes可承接下一块保守reservation、
三层容量/flow允许以及权威成员终态。强对照先用同配额固定小块；本轮没有实际work退款实现。

源码依据：`map_preparation._settle`先移除成员，只有最后成员才调用broker完成/失败并清引用；
`stage_broker.lease_prepare`预留下一块ready bytes/work，`complete_prepare`再从保守估值改成实际buffer大小。
因此旧块尾部的物理buffer仍需保留，下一块2倍ready估值是另一项独立reservation。
`prepare_inputs`已预检model_inflight≥held_tasks，当前二者均4，未把它误判为请求C或准备瓶颈。
已有修后trace只有总量与完成，缺少拒绝原因/块成员/Job-session视图，不能从它推出尾部阻塞。

[可选观测](../../../../code/tests/modalities/image/stage_block_trace.py)只用于测试：记录原enqueue/lease_prepare/
成员settle，Core owner在advance/release/wait外、持准备锁时复制global/Job/session额度及原records标量。
拒绝发生时只记broker视图，不伪称同步Core快照；统计仅含有FIFO候选时的broker lease_prepare拒绝，
未分类try_prepare的非阻塞锁/encoded借用返回0。尾部ready容量只在前者检查；成员权威终态按原settle记录，未知结果不计退款。
owner检查FIFO头、flow状态与三层C/W资格，反事实保留整块bytes、只扣已确认成员work。
这不执行router/credit/organizer选择；`before_wait`是前一progress提示，未测实际等待时长。
后续成员终态不会改写早期快照，没有保存请求body或宣称URI物理消失。

6个新廉价回归＋15个旧准备回归共21通过，0跳过。合法tight work=2的seam场景确实复现尾部work挡准备，
同配额单行块可消除；另以ready bytes=2712核对旧buffer＋下一块实际上界2038可容纳、
保守2倍reservation相加2720超限的独立bytes问题。未知成员/disabled flow不成为可退款机会。
初次bytes fixture额度设得过小，导致首块本身只能单行；另一初次断言误把启用状态的历史机会当成disabled机会，
两次失败日志保留。独立审查还发现wait关键字参数被观测器截断，已在任何新POST前修复并补透明透传回归。

实际软件仅使用原12图片、原Filter全批屏障→Map、强静态资产准备臂；保持C=2/W=2/held=4、
encoded/ready/object=4MiB、ready work=4、prepare_inflight=1、model_inflight=4。
保留原batch_rows=2，对照固定1；各19 localhost POST，总38，完整请求、prepare顺序、输出/parser/usage一致。
两臂都prepare17次、复制URI9,736,050字节，未继续消费者缓存算法。source快照各1,612,536字节。

| batch_rows | Arrow块 | owner检查点 | 有尾部的检查点 | 实际prepare lease拒绝 | 单独尾部work容量机会 |
|---|---:|---:|---:|---:|---:|
| 原2 | 14 | 787 | 31 | 0 | 0 |
| 小块1 | 19 | 199 | 0 | 0 | 0 |

原2中20个检查点同时有尾部和FIFO编码头：1个bytes/work均可容纳；19个已有prepare_inflight=1。
后者包含暂时work超额和bytes/work同时超额的谓词，但不是独立尾部work拒绝；CPU准备槽已忙，
实际worker可再次lease时原broker均成功，没有返回一次容量拒绝。例：旧尾部1,352,666 bytes/work2、
下一块reservation1,010,278/work1、C/W各占1；bytes合计2,362,944及work3均低于原上限，可正常准备。
即使某点反事实扣work后能容纳，也不能把它写成已解除CPU忙或下一块bytes reservation。

| 原始阶段累计秒（不可相加为主任务wall） | 原2 | 小块1 |
|---|---:|---:|
| 前端fetch/decode/RGB-PNG-base64准备 | 1.808297 | 1.779177 |
| 全批validate（包含下行SDK转换） | 0.159368 | 0.160479 |
| validate内SDK行转换 | 0.148517 | 0.149148 |
| 块payload准备（包含首次Daft import） | 2.791505 | 0.432255 |

两臂decode输入的encoded字节累计2,355,880是按17次准备加权；每臂源快照读取1,612,536字节，
外层取路径另核验一次同源。这些加权decode字节不是额外文件I/O；
RGB名义展开13,570,440，不当作测得物理tensor驻留。缓存miss的URI准备9,143,678、全部URI导出9,736,050。
全批validate完整JSON累计9,747,951，有限块中的SDK后缀重复转换8,677,875（首行复用），不是第二次HTTP。
Arrow backing buffer累计9,748,519／9,748,559，差40 bytes来自5个额外块的8-byte offset；不是RSS/H2D。
主任务wall 5.555500／2.709387秒含源准备到原行消费；固定顺序、首轮冷import和观测开销均保留，不作速度排名。

只读preflight通过，沿用原driver/LOTUS/Daft/Ray/Arrow/Pillow，无安装/下载。
窗口`semloom-stage-tail-20261013-1ud8qzw2`在同共同host lease下CPU24–27、600秒/4GiB采样停限：
11:14:31.201582 UTC／北京时间19:14:31开始，原截止11:24:31.201582；11:15:03.499593 UTC／19:15:03完成。
实际6个component检查＋1个完整软件用例通过，0跳过；1个CPU Ray集群、38 POST、模型/GPU均0。
11:16:12.046766 UTC／19:16:12独立新driver核对13个PID＋birth无存活、共同lease不存在、GPU空。

**本切片没有尾部work独立阻塞信号，停止退款策略与GPU配对扩展。** 紧work场景可以存在，但固定小块已能消除，
不能当作原配置瓶颈或新算法收益。不同work额度、真实VLM慢尾/质量及GPU/pinned/device/PG边界仍未验证。
[聚合](stage-tail-verification.json)及[29成员原件与双摘要](raw/stage-tail-evidence.tar.gz)保留参数、所有阶段时点、
逐成员authority、拒绝与一致容量原件；归档SHA-256 `e66e7a4f3925fb839825f83335aca1321691caae581ec9dad986d551f5e0b16e`。
