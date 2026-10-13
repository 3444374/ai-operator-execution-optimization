# LOTUS图文执行接点与Sema输入核查

本轮只验证固定前端能力和同方法执行接点，没有真实模型、GPU或Ray启动。
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
