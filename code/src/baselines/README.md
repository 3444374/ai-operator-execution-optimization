# 独立对照与公共语义

本目录保留原生系统的执行图、作者桥接和对照任务。LOTUS 方法、模型传输、结果合同、观测和通用文件工具
放在所属共享层，SemLoom 与对照分别依赖这些组件；生产主线不导入 baseline 实现。

<a id="text-execution-boundaries"></a>
## 文本执行边界

| 对照 | 真实入口／实现 | 谁拥有执行 | 当前限制 |
|---|---|---|---|
| 原生 LOTUS | [lotus_native_local.py](text/frameworks/lotus_native_local.py)，普通 Movie session 的 `backend=lotus-native` | 原算子、`LM.__call__`、`_process_uncached_messages`、LiteLLM `batch_completion`；公共 provider 只接本地模型 IPC | 新非 HTTP 路径只有 CPU 资格，官方完整 Q123 依赖及实机待验 |
| Daft | [daft_lotus_records.py](text/frameworks/daft_lotus_records.py) | `read_csv`、原生 class／batch UDF 与模型 class 生命周期 | 明确的隔离配置；可选[单线程 session](text/frameworks/daft_lotus_session.py)单独比较 |
| Ray Data | [ray_data_lotus_records.py](text/frameworks/ray_data_lotus_records.py) | Dataset、`map_batches`、ActorPoolStrategy 与原生图 | runtime 复用不等于跨查询模型复用 |
| IMLane | [resident.py](../../integrations/imlane_records/resident.py)、[隔离入口](../experiments/imlane_movie_formal.py) | 作者 CPython Filter 桥接与作者执行；共享方法／模型内核 | 原生作者能力和有限桥接合同保留，不等于任意 DAG |
| Kalypso | [kalypso_native](text/frameworks/kalypso_native/README.md) | 固定上游作者服务和原生默认 | 独立语义系统；作者的 HTTP 服务边界不是退役 Movie HTTP 模型 owner |

SemLoom Core 的实现位于 `execution_provider/adapters/lotus_records_core.py` 和有限链适配，
它是项目方法，被比较时单独命名，不列作原生 LOTUS 后端。
普通单 Filter、有限链／cascade、图像查询不能互授支持范围和 GPU 资格。

## 共享层和参数

- LOTUS formatter／parser／方法与[结果身份合同](../semantic_methods/lotus/record_results.py)归 `semantic_methods/lotus/`。
- 模型进程、阶段清理和 IPC 归 `serving/backends/`；
  [原生有限链 worker](../serving/backends/lotus_chain_worker.py)提交完整方法批次，不引入 Core 的 token 分组。
  Core 的[专用子类](../execution_provider/method_workers/lotus_chain_batch.py)持有 prepared batching／准备复用；
  Ray、Daft、IMLane 的原生图不导入该子类。
- [脱敏](../infrastructure/redact.py)、[私有文件和摘要](../infrastructure/private_artifacts.py)归通用层。
- [CLIP batch workers](../modalities/image/clip_batch_workers.py)归图像公共层；
  [Daft](image/frameworks/daft_clip.py)和[Ray 图](image/frameworks/ray_data.py)保留原生图所有权。

原生 LOTUS 的 `native_lm_max_batch_size` 是原 LM 客户端的并发批设置，必须显式声明；
它不是 vLLM 内部批大小，也不是 Core 的 C／R。模型引擎未指定的参数交引擎解析，实际值从 owner ready 回执取得。
原生 Ray／Daft／IMLane 的[同步模型入口](text/frameworks/sync_reference_engine.py)与
[有限链入口](text/frameworks/reference_chain_batch.py)继续选择原生 `LLM.chat`，不导入 Core 执行或调度。
`max_model_len`、`max_num_seqs`、`max_num_batched_tokens` 省略／null 时不传给 vLLM；
显式正整数原样传入。原 4096／8／4096 隐式补值及链、cascade、IMLane 中的上下文重写已移除。
模型 ready 后按实际上下文检查输出上限，显式上下文仍可在分配模型前检查。
这三项是模型容量配置，与 LOTUS 客户端 `native_lm_max_batch_size`、原生图 `model_batch_rows`、
模型生命周期的调用总额 `max_model_calls` 分别记录；省略不代表无限容量或已找到最优值。

`text/movie_records.py`、`movie_tasks.py` 和 `sembench_movie.py` 是 SemBench 工作负载／原评价适配，
由实验入口使用，不是 Core 的策略依赖。原生 LOTUS 的结果缓存在本次矩阵查询中显式关闭，
原缓存实现仍在 LOTUS；模型驻留、KV／prefix cache 和调度状态复用须分别记录。

补充的普通 Movie Q4 继续使用既有 Filter 图，保留完整 taken_3 关系与原 positivity ratio，
不提前停止。原查询／评价适配可加载 Q8、Q10，Map 已通过下述单算子接线的 CPU 差分；
Q10 的评价依赖原 `Movies.csv` audienceScore，不能从评论标签构造替代金标。
[共享上游加载](text/sembench_source.py)只校验所选公开 checkout 并加载原模块；
[MMQA 文本适配](text/sembench_mmqa.py)保留 Q1、Q3a–g、Q4、Q6a–c 的原函数与每题金标。
其准备函数只对现成作者 CSV 调用原文本生成方法，不下载或生成图片。sf25 实测模型输入规模分别为
25、七题各25、24、三题各25，共299；这是预计调用量，不是模型执行结果。
MMQA Q5 的 sem_agg 与视觉 Join 不从这些文本内核取得支持资格。

## 执行依赖审查与可测范围

本轮从 PR90 `7f0b6d42` 的实际调用链核对。下表的“可测”表示已维护的入口和可运行的 CPU fixture，
不是已经完成新服务器、模型、质量或性能验证。普通 Movie task 现覆盖 Q1／Q2／Q3／Q4 Filter；
有限链、cascade、CLIP 检索不自动代表其他 SemBench 查询已接通。

| 路径 | 算法与执行归属 | 当前可测范围／待验证点 |
|---|---|---|
| 原生 LOTUS | 原 SemBench 查询、LOTUS accessor／LM 和 LiteLLM batch；模型 provider 只接公共 IPC | 普通 Movie Q1／Q2／Q3／Q4、显式 LM max_batch_size、可选驻留；原 checkout／评价依赖和真实模型需单独绑定 |
| Ray Data | 原生 Dataset／map_batches／ActorPoolStrategy，LOTUS 只拥有方法语义 | 普通 Movie、有限单模型链与独立 cascade 接线；runtime reuse 不表示模型 reuse，实际 Ray 图／进程清理和模型另验 |
| Daft | 原生 class／batch UDF／扫描图和类生命周期 | 普通 Movie、有限单模型链与 cascade；单线程驻留是单独 profile；max_active_batches 不冒充已施加的 Core 上限 |
| IMLane | 固定作者代码、CPython UDF 和作者执行 future，桥接明确使用有限块与单 worker | 普通 Filter、有限链／cascade 的协议 fixture；真实桥接需匹配作者构建、Arrow16.1／Python3.12 ABI 与 Linux 环境，不表示任意作者 DAG |
| Kalypso | 作者服务、原生默认、原生 instruction 和 parser；客户端一次提交完整查询 | 独立 Movie Q1／Q2／Q3 及 Q4 的原 Filter/ratio 组合；当前绑定限既有 BF16 7B 身份，作者环境／worker／有效容量与完整查询另验，不借用 LOTUS 语义质量资格 |
| Ray／Daft 图像 | 原生 Dataset 或 class UDF，公共 CLIP 编码内核 | CLIP RM／index／search 的 CPU 合同；不授予 Q2b 独立视觉接入或新 GPU 资格 |

共享的 LOTUS formatter／parser、记录身份、生成配置、模型内核／IPC、失败回执及观测属于一致基础。
SemLoom 的任务／会话调度、credit、路由、准入、token 分组和准备复用不进入原生图。
Ray 的 ActorPoolStrategy 参数、Daft 的原生缓冲、IMLane 的有限桥接块以及累计模型调用额度
分别保留原归属；它们不是 SemLoom 的在途额度或反馈控制。

[真实依赖检查](../../tests/baselines/test_execution_independence.py)禁止加载
`execution_provider`、`scheduling` 和 `research`，覆盖五类文本入口、有限链／cascade、图像入口与启用的计时。
另用实际 LOTUS 方法和 CPU 响应执行原生完整批次，验证结果身份、整批调用、用量和关闭；
该检查不启动原生框架集群、作者服务或模型。Core 的分组、取消／失败回执及准备复用由原用例继续覆盖。

### 保留的边界问题记录

| 现象／根因 | 处理 | 验证边界 |
|---|---|---|
| 有限链原生图经 ReferenceFusedChainBatch 导入 Core 目录中的 worker；同一类含可选 token 分组与准备复用。 | 无调度 kernel 移到 serving，实际分组与复用留在 Core 子类；三种原生图使用无 Core 的基类。 | 禁止 Core 导入时可完成原生链处理；Core 分组和失败／unknown 原断言继续检查。 |
| 启用详细计时会从 ray_map_transport 取得时钟身份，间接加载 Core transport／调度模块。 | 不改原 Linux 哈希／不可用回退，时钟函数移到[公共观测层](../observability/clock.py)，两边直接调用。 | 在禁止 Core 导入时创建、记录并关闭 Timing；原 transport 的时钟不可用检查保持。 |

两项均是源码与可复现依赖边界问题；本次没有据此宣称查询加速。首次拒绝导入的失败记录与修复回归分别保留。

<a id="additional-sembench-text"></a>
## 补充 SemBench 文本查询

[工作负载](text/sembench_text.py)直接执行固定 SemBench 的原函数，原函数仍拥有普通筛选、
关系关联、Map 评分的原回退和最后汇总。唯一普通 Filter／Map／Extract 在其 accessor 边界交给
[既有原生记录图](text/frameworks/sembench_text.py)或[原生 LOTUS](text/frameworks/lotus_native_stage.py)。
不运行上游构造器的隐含暖机、下载或近似策略。入口及配置见
[脚本说明](../../scripts/README.md#additional-sembench-text-entry)。

原生 LOTUS 保留 accessor、LM、LiteLLM 原批调用与解析，provider 复用现有本地模型 IPC。
它观察原 LM 的真实消息，不额外格式化预遍。旧 ledger 的 `lotus-filter-v1` 是保留的身份协议，
Map／Extract 的真实类型另由 stage 和有限阶段 ID 映射记录，不把它们当 Filter 算法。
Ray／Daft／IMLane 使用原来已有的有限记录图和单个阶段；它们与旧普通 Filter 图是分别命名的执行布局。
原 Q1/Q2/Q3 及 Q4 的普通 records、session、矩阵入口继续保留。

下表是现成 Movie2000、原 MMQA generator 的 sf25／seed42 单次完整执行预计调用量，
不含暖机、重试或重复。Movie 金标 `Movies.csv` 为116行、105个唯一电影 ID，覆盖评论中的全部105部电影；
仍由原 ranking evaluator 处理 ID 与有效分值，不修改原金标或自行去重。

| 数据集 | 题号 | 原方法 | 实际模型输入行数／预计调用 |
|---|---|---|---:|
| Movie | Q4 | taken_3 正面比例，Filter 后用完整分母 | 120 |
| Movie | Q8 | 原 POSITIVE／NEGATIVE Map 后计数 | 120 |
| Movie | Q10 | 全评论 Map 打分、原无效评分回退及电影汇总 | 2000 |
| MMQA | Q1 | 整张描述表 Extract 后与演员表关联 | 25 |
| MMQA | Q3a | 喜剧 Filter | 25 |
| MMQA | Q3b | 科幻 Filter | 25 |
| MMQA | Q3c | 爱情 Filter | 25 |
| MMQA | Q3d | 恐怖 Filter | 25 |
| MMQA | Q3e | 劫盗 Filter | 25 |
| MMQA | Q3f | 浪漫喜剧 Filter | 25 |
| MMQA | Q3g | 传记喜剧 Filter | 25 |
| MMQA | Q4 | 原目标片名预筛，再 Extract 类型及原分组 | 24 |
| MMQA | Q6a | 法兰克福航线 Filter | 25 |
| MMQA | Q6b | 德国航线 Filter | 25 |
| MMQA | Q6c | 欧洲航线 Filter | 25 |

每个 baseline 的 MMQA 十二题合计299次，Movie 三题合计2240次。
sf25 机场关系含一个缺失 `Destinations`：输入 CSV／原 DataFrame 仍为 NaN，记录 wire 仅保留
原 DEFAULT formatter 的 `nan` 文字呈现。真实25行的原 SDK、记录方法和原生内存传输已逐消息相等；
没有填目的地、删除行或扩大到其他序列化格式。更换规模／资产须由 `check` 重新计算，不能沿用常数24／299。

| 对照 | 本轮接线范围 | 未验证／留空原因 |
|---|---|---|
| 原生 LOTUS | 上述十五题，原 accessor／LM 路径 | CPU 原查询差分通过；新的实际模型与完整生命周期未跑 |
| Ray Data | 上述十五题，既有 Dataset／actor 有限图 | SDK／分发合同 CPU 通过；新图的实际 Ray／模型未跑 |
| Daft | 上述十五题，既有 native class／batch 有限图 | SDK／分发合同 CPU 通过；实际 Daft／模型未跑 |
| IMLane | 上述十五题的原作者有限记录链 | 作者构建、Arrow ABI、显式有限容量及实际启动仍需绑定／验证 |
| Kalypso | 旧 Movie Filter 路径可组合 Q4 ratio，CPU 辅助按显式任务清单检查 | 当前原生绑定没有 Movie Map 或 MMQA；不新增算法补齐，相关项留空 |

这些是 CPU 接线范围，不是模型质量或性能成绩。原查询／原 accessor、共享链和 NativeStage 的
软件回复差分与全部十五题 CLI fixture 均运行；真实模型逐行一致性仍待实机检查。
原 aggregate 分数不能排除相抵的分类错误，保留原调用文本和旧普通 Filter 的逐行审计供核对。
Q1 的原 Series 仅在原评价入口按官方 CSV 往返转换成同名 DataFrame；Extract 保留实际额外／碰撞 key
和原 Index label 写入，不补未返回的声明字段。后处理失败仍保留已完成调用与关闭回执。

本地组合为 LOTUS1.2.4／LiteLLM1.95.1／OpenAI2.50.0／pandas2.3.3／Arrow24；
它不冒充服务器 Linux LiteLLM1.95.0 的精确软件资格。物理 GPU 退出仍由现有外部 host supervisor 核查。

本轮保留的可复现边界：

| 现象与根因 | 处理与验证 |
|---|---|
| MMQA 原 runner 的 `src.runner` 与 SemLoom 已加载的 `src` 冲突，首轮无法导入。 | 仅在原模块加载期映射同一已验证上游 runner，随后恢复；十五题原查询／方法／accessor 差分及别名恢复检查通过。 |
| sf25 Q6 有 NaN，最初的纯字符串接纳误拒绝原 SDK 可序列化输入。 | 在文本 wire 边界保留原 DEFAULT cell 呈现；真实三题各25行完整消息与结果顺序相同，原 CSV 不变。 |
| Extract 成功后原关系后处理可能因 `{}` 缺列失败，旧的新接线未把已完成算子报告附给该异常。 | 保留原 KeyError 并转交已知调用与关闭证据；不补字段把失败变成成功。 |
| Movie 适配扩展使五份历史投影从维护版读取旧摘要失败。 | 保存单份准确原源并补五条来源／格式映射；原3010条摘要与投影 ID 不变，完整 materialize 通过。 |

### 图像查询准备范围

| 原查询 | 现成资产与预计工作 | 当前接线限制 |
|---|---|---|
| Animals Q1/Q3/Q10 | 官方 sf64 共64图、42,725,857编码字节；各64个图像输入 | 现用原查询 helper 仅 Q1/Q7，Q3/Q10 上游存在；当前视觉 provider 需另验 |
| Animals Q7 | 同一64图，两遍原 Filter，约128个输入 | 不能用 CLIP 检索替代 VLM Filter |
| MMQA Q2a | 原生成切片25图＋13行表，精确 Join325候选对 | 新非 HTTP 视觉 provider 尚未接通 |
| MMQA Q2b | 同一切片，Join 后按匹配对 Extract，最多650次 | preset 单次 Extract 不是完整 Join→Extract 通过 |
| E-Commerce／Cars 图像 | 原实现存在；本轮已查目录未定位成套数据 | 数据和每条 baseline 视觉路径待确认 |

90个现有文件（64 Animals图、25 MMQA图及表文件）的当前摘要与清单相符。
Qwen2-VL-2B-Instruct 的配置、processor、索引及两个权重分片齐全，权重合计4,418,050,768字节；
本轮未重新哈希整份权重或加载模型。历史身份为 revision `895c3a49bc3fa70a340399125c650a463535e71c`、BF16。
历史 Animals 的 `max_pixels=1003520` 与 MMQA／原 processor 的 `12845056` 不同，须冻结为明确的比较配置。
Animals Q3/Q10 在此64图有三个并列最大项，保留原评价对并列项的接受行为，不强求某一精确结果帧。
Ray／Daft 的现有 CLIP 检索、LOTUS SDK 的 image-text profile，均不能自动授予上述 VLM 查询资格；
IMLane／Kalypso 当前视觉绑定未验证。原 LOTUS 明确不支持的音频题留空，其他系统未验证能力另记，均不计作零分。

## 已退役和暂停

旧 `native_calls` prepared-HTTP 图、`*_pg_http`、Daft prompt／Ray HTTP 对照、Movie HTTP owner、
PG 查询矩阵与其专用脚本／测试／配置已从现用树移除。PG 后续仍可恢复；
准确来源、SHA 与范围见[归档索引](../../../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md)。
原生 Q2b 位于独立实验分支，旧路径仍曾使用 HTTP；本次 Movie 非 HTTP 适配没有完成其视觉 provider 替换。
不删除其历史对照，也不声称它已随此清理恢复。

SAOR 调度算法与离线研究材料保留；依赖退役 HTTP 的历史启动胶水隔离恢复。
公开结果／来源门槛见[历史复现](../../../experiments/reproduction/native_baselines/README.md)。


原生本地文本对照共享中性 EngineSpec：dtype、显存比例及三项 vLLM 容量未指定时均保留省略，
显式合法值仍原样传递；没有改作者图、LM 客户端批大小、提示或生成设置。
ready 的精度／显存比例来自实际原生配置，缺测保持未知；CPU 构造替身不是实际 vLLM、质量或性能资格。
[当前配置修正记录](../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#native-defaults-capacity-combination)
保留独立 source、所选测试和未完成范围，历史具名投影继续读取准确旧参数。


本地 LOTUS provider 仍由原 LM 声明并发，并按原单项大小及控制槽公式构造传输额度；
现在一次构造明确请求数，不再先建立内部8再覆盖。显式较小传输额度保持，原算子和客户端调度不改。
SDK 的 R=None 不给 Daft／LOTUS 增加无用要求，Ray 可交其原生策略；不表示这些原生默认已经实机重测。
