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
| Movie | Q9 map | Ant-Man 评论的原 rubric Map，保留原无效值回退3.0 | 当前 packet 为256，实际 CSV 由 check 核算 |
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

每个 baseline 的 MMQA 十二题合计299次，先前 Movie Q4/Q8/Q10 三题合计2240次。
Q9 新入口明确选择原 `ranking=map`，逐条评分与 TopK 排名不是同一算法。现有 packet 的 Ant-Man
切片为256行、128个唯一 reviewId；保留全部256个 occurrence，不能按 reviewId 去重。
该 packet 与先前 `Reviews.csv` 的编码字节摘要不同，运行前须用 `check` 冻结实际输入与调用数，
不能把两份来源直接合成同一精确输入签名。
sf25 机场关系含一个缺失 `Destinations`：输入 CSV／原 DataFrame 仍为 NaN，记录 wire 仅保留
原 DEFAULT formatter 的 `nan` 文字呈现。真实25行的原 SDK、记录方法和原生内存传输已逐消息相等；
没有填目的地、删除行或扩大到其他序列化格式。更换规模／资产须由 `check` 重新计算，不能沿用常数24／299。

| 对照 | 本轮接线范围 | 未验证／留空原因 |
|---|---|---|
| 原生 LOTUS | 上述十六题，原 accessor／LM 路径 | CPU 原查询差分通过；新的实际模型与完整生命周期未跑 |
| Ray Data | 上述十六题，既有 Dataset／actor 有限图 | SDK／分发合同 CPU 通过；新图的实际 Ray／模型未跑 |
| Daft | 上述十六题，既有 native class／batch 有限图 | SDK／分发合同 CPU 通过；实际 Daft／模型未跑 |
| IMLane | 上述十六题的原作者有限记录链 | 作者构建、Arrow ABI、显式有限容量及实际启动仍需绑定／验证 |
| Kalypso | 旧 Movie Filter 路径可组合 Q4 ratio，CPU 辅助按显式任务清单检查 | 当前原生绑定没有 Movie Map 或 MMQA；不新增算法补齐，相关项留空 |

这些是 CPU 接线范围，不是模型质量或性能成绩。原查询／原 accessor、共享链和 NativeStage 的
软件回复差分与全部十六题 CLI fixture 均运行；真实模型逐行一致性仍待实机检查。
原 aggregate 分数不能排除相抵的分类错误，保留原调用文本和旧普通 Filter 的逐行审计供核对。
Q1 的原 Series 仅在原评价入口按官方 CSV 往返转换成同名 DataFrame；Extract 保留实际额外／碰撞 key
和原 Index label 写入，不补未返回的声明字段。后处理失败仍保留已完成调用与关闭回执。

本地组合为 LOTUS1.2.4／LiteLLM1.95.1／OpenAI2.50.0／pandas2.3.3／Arrow24；
它不冒充服务器 Linux LiteLLM1.95.0 的精确软件资格。物理 GPU 退出仍由现有外部 host supervisor 核查。

本轮保留的可复现边界：

| 现象与根因 | 处理与验证 |
|---|---|
| Daft source DataFrame 提前选择 Native runner，随后显式线程配置被当成外部覆盖而拒绝，发生在模型启动前。 | 在创建 DataFrame 前调用同一个 Native 配置入口；省略时保持 Daft 默认，已有 runner 仍不被暗中重配。原失败及零模型复现保留，原生图／核不改。 |
| MMQA 原 runner 的 `src.runner` 与 SemLoom 已加载的 `src` 冲突，首轮无法导入。 | 仅在原模块加载期映射同一已验证上游 runner，随后恢复；十六题原查询／方法／accessor 差分及别名恢复检查通过。 |
| sf25 Q6 有 NaN，最初的纯字符串接纳误拒绝原 SDK 可序列化输入。 | 在文本 wire 边界保留原 DEFAULT cell 呈现；真实三题各25行完整消息与结果顺序相同，原 CSV 不变。 |
| Extract 成功后原关系后处理可能因 `{}` 缺列失败，旧的新接线未把已完成算子报告附给该异常。 | 保留原 KeyError 并转交已知调用与关闭证据；不补字段把失败变成成功。 |
| Movie 适配扩展使五份历史投影从维护版读取旧摘要失败。 | 保存单份准确原源并补五条来源／格式映射；原3010条摘要与投影 ID 不变，完整 materialize 通过。 |

<a id="sembench-image-preparation"></a>
### 图像查询准备范围

[Animals 原查询](image/sembench_animals.py)接通 Q1/Q3/Q7/Q10 的原函数、金标 SQL 和原评价；
[MMQA 原视觉查询](image/sembench_mmqa.py)直接调用完整 Q2a/Q2b/Q7，固定 exact policy 和原 `zs-cot`，
不运行构造器、不预设匹配，不在两阶段之间手工去重图片；比较配置显式关闭原生缓存。
[CPU 检查入口](../../scripts/experiments/check_sembench_image_query.py)只读取实际文件／RGB 像素摘要和计算单遍预算，
结果统一为空且标记待验证；它不申请额度、不初始化 framework 或模型，也没有运行 GPU 的选项。
命令见[脚本说明](../../scripts/README.md#sembench-image-preparation)。

| 原查询 | 现成资产与单遍模型请求预算 | 当前接线限制 |
|---|---|---|
| Animals Q1/Q3/Q10 | 官方 sf64 共64图、42,725,857编码字节；各64次 Filter | 原查询／原评价 CPU 接通；原生 SDK 服务入口已接，模型服务待实机验证 |
| Animals Q7 | 同一64图，两遍完整原 Filter，共128次 | 两遍原模型输入都保留，不能替为 CLIP 检索 |
| MMQA Q2a | 原生成切片25图＋13行表，精确 Join325候选对 | 原生 Join／评价 CPU 接通；原生 SDK 服务入口已接，模型服务待实机验证 |
| MMQA Q2b | 同一切片，Join325次，后续 Extract 为实际匹配行数，最多再325次 | CPU 完整 Join→Extract 通过；preset 单次 Extract 仍不构成完整模型资格 |
| MMQA Q7 | sf25 机场25行与原25图，组合后625候选对 | 原源码无自然问题的欧洲预筛，保留并单列该差异；原生服务入口已接，实际服务待绑定／验证 |
| E-Commerce／Cars 图像 | 原实现存在；此前已查目录未定位成套数据 | 数据缺口与视觉 provider 缺口分别记录，不能记零分 |

上述预算不含暖机、重试或重复；Animals 四题与 MMQA Q2a/Q2b 共970到1295次，Q2b 的匹配数由真正的 Join 决定。
Q7 需把已由原 generator 准备的机场表与同一图像切片绑定到一份输入目录，额外625次；
当前两组资产不在同一准备目录，不能把可组合视为已经在服务器运行。
90个现有文件（64 Animals图、25 MMQA图及表文件）的既有只读摘要检查相符；本轮没有重新复制图像或加载权重。
Qwen2-VL-2B-Instruct 的配置、processor、索引及两个权重分片齐全，权重合计4,418,050,768字节；
已有 revision `895c3a49bc3fa70a340399125c650a463535e71c`、BF16 身份不代替新 provider 的实际加载资格。
历史 Animals 的 `max_pixels=1003520` 与 MMQA／原 processor 的 `12845056` 不同，须显式冻结为可比较配置。
Animals Q3/Q10 在现有64图有三个并列最大项；原评价接受其中一个合法第一，不强求某一结果帧或同时输出全部第一。

当前 `serving/backends/vllm_protocol.py` 的记录模型通道只接文本，sync 引擎也按 token-only 渲染；
LOTUS 的 `IMAGE_TEXT_PROFILE` 没有当前生产调用者，其旧 prepared-call seam 也不接 Extract 的 `json_object`。
[原生 LOTUS 视觉运行入口](../../scripts/experiments/run_sembench_lotus_image_query.py)已将这些原查询直接接到原 LM SDK 的 OpenAI-compatible 服务。
原生 SDK 保留 PNG 消息、并发批调用、`zs-cot` 和 Extract `json_object`，不用 prepared-call seam 或文本 IPC。
本地多模态 IPC owner 仍未接通；本次服务入口的真实模型、processor／generation 与物理服务关闭待机器验证。
原生 LOTUS 也可由原 LM SDK 直接访问独立官方 VLM 兼容服务，复用现有 `native_http_observer.observe_native_httpx`
观察完整原请求及每次尝试；这不是已退役的 SemLoom HTTP 代理，也不需要改变算子。该服务的配置、模型身份与生命周期仍待机器绑定。入口读取已有预算的预留单元，每次真正的 HTTP POST 先消费，
不分配或退回额度；Q2b 按完整 Join＋最多同等数目的 Extract 预留。原失败与已发送单元不得重置。
服务启动和 GPU 归外部 host owner，入口仅监督查询子进程；该计时为已有服务上的完整查询，不能混入文本冷模型启动成绩。
私有审计保留原 POST 的内容摘要和 PNG URI 摘要，避免在事件队列复制整幅 base64 图像；原请求字节不变。
生成配置未给出的值按原 SemBench 8192 token／无 stop 解析，显式值保持，不继承短文本 Filter 的8 token／换行截断。
Ray／Daft 已有 CLIP 图不构成上述 VLM Filter／Join／Extract 支持；IMLane／Kalypso 当前视觉绑定未建立，
不由此推断整个上游系统永远不支持视觉。原 LOTUS 明确不支持的音频题留空，其余未验证能力另记。

CPU 视觉夹具使用真实 ImageArray、原 PNG 格式化、原 Filter／Join／Extract 解析与关系结果。
MMQA 同一图片的多个匹配行仍逐个 Extract，JSON response_format 保留原值。
原 Q2b 在无匹配或 Extract 返回 `{}` 时可能因 `color` 缺列抛出 KeyError；测试保留原异常，
不补字段把失败变为成功。这些软件回复不构成 VLM 质量或性能结果。

### 后续 Join、TopK 与缺失数据

Movie Q5/Q6/Q7 的原 exact Join 入口和原评价已可直接选择；现有单个 F/M/E worker 不因此支持 Join。
现有 Ant-Man256行的每题是65536个候选对，模型之后才剔除相同 reviewId 的512对；
Q5/Q6 的 LIMIT10 也在完整 Join 之后，不能预裁剪、去重或按十次调用申请预算。
原 LOTUS1.2.4 会先 materialize 全部 pair 的 prompt，须单独验证主机内存、启动／查询期限与原生模型 owner；
三题单遍预算196608次，暂未申请或运行。

Q9 原 TopK 仍须显式选择 `ranking=topk`，不能借用本次 Map 的256次资格。
其 `quick` 路径随机选 pivot；完整 N=256 排序的候选比较数上界32640，精确次数依赖 pivot 和模型判断，
不能把 SDK safe-mode 估算当实际消耗或额度上界。LM seed 不设置 NumPy pivot RNG。
MMQA Q5 使用原分层 `sem_agg`；调用数依赖上下文分块与中间生成长度，现有单 F/M/E 链未接，不能按输入行数猜次数。
其余纯文本候选为 E-Commerce Q1/Q3/Q5、Cars Q1/Q4/Q10、Medical Q1/Q4/Q10。
三组当前没有已核实的成套输入；先定位原 Parquet／CSV，再复用原 generator 的采样入口。
Cars／Medical 的普通准备会顺带下载图音素材，文本准备不可盲跑整套 setup；有完整原 CSV 时调用原采样，不重写采样。
E-Commerce 的嵌套 `productDescriptors` 保留原 DEFAULT 的整对象字符串，不能抽取一个 description 字段代替；
E-Commerce Q3／Cars Q10 的 keyword Extract、E-Commerce Q5 的 Map suffix 需要单独绑定，当前有限 hook 尚未接这些参数。
这些属于待接线／待数据，不是系统缺少原算法。E-Commerce、Cars 和 Medical 的其他原实现按各自文件与能力准备；
此前限定目录中未找到的数据只记待定位，不推断全服务器没有，也不下载或改写算子凑结果。

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
