# 独立对照与公共语义

本目录保留原生系统的执行图、作者桥接和对照任务。LOTUS 方法、模型传输、结果合同、观测和通用文件工具
放在所属共享层，SemLoom 与对照分别依赖这些组件；生产主线不导入 baseline 实现。

<a id="text-execution-boundaries"></a>
## 文本执行边界

| 对照 | 真实入口／实现 | 谁拥有执行 | 当前限制 |
|---|---|---|---|
| 原生 LOTUS | [lotus_native_local.py](text/frameworks/lotus_native_local.py)，`backend=lotus-native` | 原算子、`LM.__call__`、`_process_uncached_messages`、LiteLLM `batch_completion`；公共 provider 只接本地模型 IPC | 冻结MMQA Q3a/Q4已真实冷暖通过；正式resident迁移的CPU与目标资格[分别记录](../../../experiments/results/diagnostics/lotus_mmqa_q3a_native_20261110/README.md)，不外推完整Movie Q123 |
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
  [Daft](image/frameworks/daft_clip.py)保留通用原生图所有权；绑定PG读取的[原Ray图](../../../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md#pg-image-isolation)独立归档，公共CLIP workers继续维护。

旧的链结果、链worker、同步／分阶段模型进程和阶段清理共五个baseline转发模块已删除。
当前IMLane及测试直接从`semantic_methods`或`serving`的实际所属模块导入；这些实现和原生调度不改。
旧模块直接导入及引用旧模块名的pickle停止支持；当前类／状态序列化保留，历史实验按
[准确来源恢复](../../../experiments/reproduction/native_baselines/README.md#retired-baseline-worker-forwarders)。

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

IMLane 的[有限传输声明](../../integrations/imlane_records/transport_limits.py)由实际工作推导：
Filter／cascade 每个原生查询为 N 个调用记录加一条关闭记录；链以 `model_batch_rows` 打包，
每批一条 Arrow 记录，再加一条关闭记录；驻留会话按已声明的 `query_calls` 推导每查询最多
`max(N)+2` 条记录、全会话 `Q+2` 个原生查询 ID。模型额度仍独立计数且不退款。
单个 CPython／模型 owner、同步模型方法、重叠查询拒绝和 `max_pending` grant／ready 等值要求保留。
owner 配置／context 可选 `transport_limits`，仅覆盖 `max_input_bytes`、`max_output_bytes`、
`max_command_bytes`、`max_response_bytes`；默认分别为 64 KiB、1 MiB、128 KiB、8 MiB。
输入／输出及两份 64 KiB IPC 余量须容于作者 128 MiB Lane；待办／保留缓冲及管道声明须容于
owner 的 RSS／SHM 额度。完整进程内存仍由原采样器检查，声明不是模型吞吐或硬件资格。
Movie source 使用 manifest 的显式 `max_rows`，CSV／字段／manifest 字节保护保留。
本次桥接 CLI／ready 新增完整声明，须[重新构建](../../integrations/imlane_records/build.py)并保留新来源回执；
历史二进制和结果不改写。[CPU 边界测试](../../tests/execution_provider/test_imlane_transport_limits.py)
不启动作者运行时或模型，也不授予新的实机资格。

[真实依赖检查](../../tests/baselines/test_execution_independence.py)禁止加载
`execution_provider`、`scheduling` 和 `research`，覆盖五类文本入口、有限链／cascade、图像入口与启用的计时。
另用实际 LOTUS 方法和 CPU 响应执行原生完整批次，验证结果身份、整批调用、用量和关闭；
该检查不启动原生框架集群、作者服务或模型。Core 的分组、取消／失败回执及准备复用由原用例继续覆盖。

[LOTUS＋IMLane完整原题profile](../../integrations/imlane_duckdb/README.md#lotus-original-query)
复用同一CLI、原函数和QueryLM，实际后续批次通过既有VARCHAR／SQL／ACK；原生multimodal EngineInput不转为文本。
每批原SDK／native采样、文本参考与图像证据分列，目标SQL／VLM资格按冻结来源核对。
完整原题现支持[同模型双查询](../../integrations/imlane_duckdb/README.md#lotus-imlane-model-session)，此次80项软件通过、实际模型复用待验；
它与不经LOTUS的原生Movie Q3、TINYINT profile不能重复计算成更多系统。

### 保留的边界问题记录

| 现象／根因 | 处理 | 验证边界 |
|---|---|---|
| 有限链原生图经 ReferenceFusedChainBatch 导入 Core 目录中的 worker；同一类含可选 token 分组与准备复用。 | 无调度 kernel 移到 serving，实际分组与复用留在 Core 子类；三种原生图使用无 Core 的基类。 | 禁止 Core 导入时可完成原生链处理；Core 分组和失败／unknown 原断言继续检查。 |
| 启用详细计时会从 ray_map_transport 取得时钟身份，间接加载 Core transport／调度模块。 | 不改原 Linux 哈希／不可用回退，时钟函数移到[公共观测层](../observability/clock.py)，两边直接调用。 | 在禁止 Core 导入时创建、记录并关闭 Timing；原 transport 的时钟不可用检查保持。 |

两项均是源码与可复现依赖边界问题；本次没有据此宣称查询加速。首次拒绝导入的失败记录与修复回归分别保留。

<a id="direct-native-movie-q3"></a>
## 直接原生 AI 的 Movie Q3 准备

以上 LOTUS 方法＋框架执行图继续保留；本节另准备项目 workload binding，直接使用原生 AI API，
不称为官方 SemBench runner。范围、作者配置依据、有限调用量和执行前条件见
[原生基线校准计划](../../../experiments/plans/原生基线校准.md)。

- [中立 Q3 合同](text/sembench_movie_native.py)读取完整原 CSV，按源位置保留 occurrence_id，标签只用于后置评价。
  `q3_messages/q3_decision` 与 LOTUS1.2.4 普通 Qwen Filter 做 CPU 差分，运行时不导入 LOTUS／Core。
  `evaluate_q3`消费同一份保存的预测，核对原生COUNT、调用原评价器，并报告FP／FN及解析失败default=True。
- [Daft 原生 AI](text/frameworks/daft_sembench_native.py)当前复用
  [原题 prepared 查询](#daft-native-prepared-sembench)，使用原生 `prompt(vllm-prefix-caching)`，每个实际LM批次一次collect；
  tokenizer与完整原题／sink由任务层拥有。旧专项 `execute_q3` 及其worker schema 已退役，
  [历史来源](../../../experiments/reproduction/native_baselines/README.md#retired-daft-q3-native-binding)保留。
  native runner不重设线程；只接受确实生效的prefix／buffer／morsel选项，拒绝仅distributed生效的actor参数。
  首测返回120条但进程收尾失败，原质量和失败保留在[诊断报告](../../../experiments/results/diagnostics/native_sembench_q3_20261106/README.md)。
  独立进程用[公共工厂／shutdown作用域](text/frameworks/daft_native_owner.py)关闭实际创建引擎；
  `on_result_ready`在关闭前发布结果，异常保留已物化预测。67a4已一次120条正常收尾，质量七个FN仍保留。
- [Ray Data LLM](text/frameworks/ray_data_sembench_native.py)当前维护
  [原题 prepared 查询](#ray-native-prepared-sembench)，Q3 与其他普通文本查询复用同一原生 processor；
  原题函数拥有选择、parser和完整 sink。旧专项 `execute_q3` 图已移除，
  [历史来源及默认重试／runtime差异](../../../experiments/reproduction/native_baselines/README.md#retired-ray-q3-native-binding)保留。
  Ray2.56.1发布extra仍要求0.22.0；旧67a4实际执行120条但SHM残留导致生命周期失败，物理attempt总数未证实。
- [Ray公开HTTP诊断profile](text/frameworks/ray_data_http_sembench_native.py)因校准对照仍有用途而保留原HTTP处理器默认与Q3语义，
  外部caller拥有独立模型服务；query／Ray driver断开与物理进程／服务关闭分开核对，不声称修复旧local actor。
  35个替身方法及6996真实Ray＋独立软件服务的正常／错误两例通过；120＋1次实际POST、显式关闭和SHM检查通过。真实模型服务仍待验。
- [DuckDB＋作者IMLane](../../integrations/imlane_duckdb/README.md)在准确798e通过真实shared库／薄driver、
  DBEnd函数alias、Arrow16.1及PROCESS／THREAD软件CPU链路和外层清理，2000→120软件COUNT14；后续[模型provider／服务owner／worker](../../integrations/imlane_duckdb/README.md#本地ipc模型接线)已接，准确e420真实作者＋软件HTTP正常／错误CPU闭环完成，GPU仍待验；
  当前本地IPC项目适配共享一个AsyncLLM，作者执行保持，124项CPU通过；完整作者＋软件IPC及真实模型仍单列待验。原Q7模型客户端未公开，不称论文客户端复现；
  固定来源中的SCHEDULE Filter为空实现，不能靠注册该kind声称完成接入。现有records桥不取代该路径。
  [MMQA Q1 工程版](../../integrations/imlane_duckdb/README.md#mmqa-q1-工程版接线)单独绑定已验
  varchar-owned DSO，保留原 JSON Extract／完整 sink；25 条 CPU 保存答复回放不授予原作者或新 GPU 资格。

原公开Movie2000输入的Q3为120个模型输入、金标14正面／106负面；本地用金标固定回复通过原评价器，
只证明数据、结果形状和评价接线，不是模型质量结果。首批两个新API各一次冷查询共240个逻辑请求；
第三条接线通过后再加120。67a4单次复测不作稳定性能，实际retry／资源／完整退出和三轮交错比较按计划另验。
Daft复测使用[新worker／监督入口](../experiments/native_daft_query.py)，另追加120且保留原消耗；
外部调用方继续负责host租约、永久额度、preflight、离线评价和最终物理GPU／SHM检查。

<a id="daft-native-prepared-sembench"></a>
### LOTUS＋Daft 原生 AI 的完整查询入口

完整 SemBench 调用走本节和下节的共享查询入口；旧 Daft `execute_q3`、独立tokenizer／Filter／COUNT图
和 `semloom.native_daft_q3.v1` 配置已删除。原题 Q1–Q4 与 Join 通过同一 prepared packet及原 sink；
现有监督器保留结果READY、错误原件、永久预算和关闭期限，不再在worker中复制Q3语义。
历史专项图与新图的执行边界不同，不将退役操作称作生命周期完全等价。

[原 worker](../experiments/native_daft_query.py)的 `sembench` 动作现在调用
[统一的单查询接线](../experiments/native_daft_sembench.py)，复用原监督器、Daft
`prompt(provider="vllm-prefix-caching")`、原生 `LocalVLLMExecutor` 和实际模型 owner。
driver 与模型 Python 依赖保持隔离；共享 LOTUS 完整提示与 parser，
[原 SemBench 查询](text/sembench_text.py)继续拥有选择、连接、分组、最终 sink 和 evaluator。
它与上方 native class／batch UDF 接线是不同执行路径，不借用后者的资格。

```bash
PYTHONPATH=code "$DRIVER_PYTHON" -m src.experiments.native_daft_query sembench \
  --config "$FROZEN_CONFIG" --output-dir "$NEW_PRIVATE_OUTPUT" \
  --model-python "$DAFT_NATIVE_PYTHON"
```

[配置模板](../../configs/sembench_daft_native.example.json)以 MMQA Q3a 单冷25条为例。
已有单 F/M/E 工作负载绑定包括 Movie Q1/Q2/Q3/Q4/Q8/Q9-map/Q10、MMQA Q1/Q3a–g/Q4/Q6a–c、
Medical Q1/Q4/Q10；这是参数化接线范围，**不是22题均已完成当前资格**。
Extract 的 JSON-object 由原任务决定，普通 Filter/Map 不强加 JSON 约束。
已知请求数／聚合首层最小数超出 `max_model_calls` 时在模型启动前拒绝；已有 SQLite 预算在每次原生
`generate` 前扣除，失败不退还，耗尽后不委托第 N+1 次。没有额外预热或整轮自动重试。

完整原函数现在经`bind_original_execution()`和`make_query_lm()`实际产生各LM批次，
不再走生产单算子的`prepare/finish`重建。MMQA Q5也使用同一runner：配置追加`token_counter`
（本地`huggingface_tokenizer_file`的path／sha256／tokenizer_id／tokenizer_revision）和
`native_lm_max_ctx_len`；本地模型profile继续核对`engine_kwargs.max_model_len`及所声明tokenizer。
外部服务profile保持空`engine_kwargs`，使用同一声明式context／SHA绑定本地counter；远端context／tokenizer匹配未验证，不宣称模型owner或实际加载已核。
原LOTUS拥有分块、token计数、摘要依赖、parser和最终`_output`；动态总数保持未知，有限预算按实际调用扣除。
现有字符串prompt provider尚无逐行图像carrier；图像使用下述独立profile。TopK取消项保持停止，分组并发尚未由该路径授予支持。
附加服务的既有顺序查询桥亦能执行E-Commerce Q10/Q11原分支Filter／Join，19／25请求的软件原题、完整结果／评分与末批ACK已核，保原SDK多列选择限制；不为两题另建执行器。
原服务及LOTUS＋Ray／Daft按已知minimum准入，maximum只描述上界；实际幸存行／后继请求逐次受同一永久额度约束，
不是要求预留每种理论后继；紧预算非空／空路径与目标待验范围见[验证](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#original-query-minimum-budget)。

另有[外部OpenAI兼容服务模板](../../configs/sembench_daft_native_openai.example.json)，沿同一原题runner与worker：
Daft0.7.21内置`OpenAIProvider`和`prompt(list[Expression])`拥有执行，不增加自定义provider或Prompter。
接可选静态system＋单user字符串或内容数组；builtin将字符串显式编码为单text part，数组原内容保持，
文本与PNG data URI还原为有类型的原生列，固定布局、图像字节及顺序按实际编码核对。
原QueryLM消息／packet不改，完整sink后保存call_id对应原始／effective SHA并按后者核HTTP。
不把该API说成任意raw conversation接口；多轮、额外图像detail和未验证形态仍拒绝。
原Q12文本Filter→实际survivors图像Extract已复用同一runner，软件原题4→3／原评分及空survivor原KeyError通过；
这是明确的native编码差异，目标renderer／token等价仍为None待验，不因相同文本声称输入tokens相同。
原Extract的JSON-object只在该阶段透传，沿chat-completions返回原文本，仍交原LOTUS parser。
纯文本F／M／E／Join及Q5单层／摘要依赖多层现沿同一桥接，删除仅允许visual的错误入口条件；
八条原题软件流程及聚合回归的[范围与失败记录](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#lotus-daft-attached-text)分列，不借独立JSON或旧视觉模型资格。
Medical评价在共享边界复制结果给原评分器，避免其大小写规范化改写完整sink；独立工程任务不再重复复制结果，原评价算法与分数保持。
`native_openai`只引用endpoint／key环境变量名；禁止同时声明本地模型构造，外部模型由调用方拥有。
原生SDK重试策略照配置，HTTP发送前占永久预算；driver仅在完整sink后核对实际请求／响应。
结果行关联由原生Daft的call_id列承担；重复消息的HTTP答复目前仅按整批关联，不虚报逐行HTTP绑定。
未观测远端READY／启动或显式SDK client close；隔离worker在release ACK后退出，不能据此证明远端模型退出。
真实Daft0.7.21图＋软件API已验证Animals单图、Medical双图和Q2b非空Join→Extract的消息等价；
完整worker／ACK与原题的软件验证另列[报告](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#lotus-daft-attached-visual)。
[混合文本／图像接线](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#daft-native-mixed-messages)核真实native图与后置双身份审计，不新增provider／执行器或恢复TopK。
没有真实模型／服务器资格，也不借用本地字符串profile的旧成绩。

每批原生READY后，driver将预测交回原题；随后原题产生下一批或完整sink，显式ACK才允许当前worker关闭。
中间worker必须先关闭再启动下一批，最后worker在完整sink之后关闭；不存在自建模型池或跨批复用声明。
原worker未开启`wait_for_result_release`时保留旧合同。结果改为v2，`native-lm-batches.json`保留
全部实际批次；顶层`native_api/model_close_receipt`仅是最后批次的便利字段，不能当整查询调用总数。

`source_start_ns → sink_ready_ns`直接记为独立`lotus_daft.source_to_sink.v1`，包含fresh source、
原SDK提示与算法、packet／API观察写盘、IPC、原生冷启动及必要的中间关闭；最终关闭、事后身份核对、
评价和driver持久化在外。不追认旧v3或统一冷暖成绩，不从旧时长扣减。最终按时sink后仍有独立cleanup额度，
每个中间关闭继续消耗原查询绝对截止时间。真实目标tokenizer、模型、多批物理清理仍待验。

本地实际 Daft0.7.21 CPU 表达式/执行器、Q3a/Q4保存答复全关系回放、预算及清理结果见
[资格报告](../../../experiments/results/diagnostics/daft_native_semantics_20261110/README.md)。
`ffbc352ac` 的 Q3a25/Q4 24 已完成目标原生参数、真实模型和物理清理资格；后续已把消息摘要及
关系序列化移至 sink 后，旧49条时长不追认为该新边界。`48a6b42d0` 的 Movie Q1/Q2/Q3 已分别
完成 2000/120/120 次真实单冷、完整原 sink 和逐条审计，零 unknown、零重试，物理清理完成。
这些成绩绑定原提交；当前整合仅重跑受影响 CPU 回归。其余题按各自冻结回执判断，同 owner 暖轮
仍未取得资格。保留每个系统自己的依赖版本，不以迁移到统一 vLLM 为前提。

<a id="independent-native-text"></a>
### 独立原生 Ray／Daft 任务

[独立任务层](text/sembench_native_text.py)定义Movie Filter／Map的源、条件、最终关系和原官方评分，
同类算子共用项目JSON提示／解析，不导入LOTUS SDK、提示构造器或解析代码。
同一配置工厂的`scenario=mmqa`接[文本Extract任务](text/sembench_native_mmqa.py)Q1/Q4；
同模块的一个Filter类接Q3a–g、Q6a–c完整家族，分别对全部lizzy文本的title＋text或机场表的Airlines＋Destinations推理，
再投影原title／Airlines列，保留dtype、索引、重复和空值，不套Q4的片名条件或合并Q6a/b推理。
缺省scenario仍为movie，不另建系统runner。Q1先抽取全部文本行的director，沿原left join及Role筛选返回
带原索引的Series；Q4沿原片名集合筛选、genres逗号展开及groupby生成完整genre／movies_in_genre表。
仅text进入项目JSON消息，重复发生项分别推理，模型输出JSON null显式物化为字符串`None`；
空输入缺列等原关系错误保持。Series完整sink不为写盘转形，原评分桥只在后置评价转成原CSV往返的列形状。
Q1/Q2在全部合格源行完成Filter后取原顺序前五项；Q3为COUNT，Q4为原分母上的比例。
Q5–Q7共用exact JSON pair任务：先完成含self／重复ID的N²有向配对，按True序列恢复Join索引后再去等ID，
Q5/Q6最后head10，Q7完整；方向、重复与索引空洞保留，原Q5五列／Q6Q7零列空结果不统一改写。
Q8保留taken_3分类计数、补零及排序；Q9须显式选择Map，保留完整评分关系、合法小数及原数值默认3的后处理。
Q10复用同一评分Map处理全部物理源行，沿原pandas分组、Python平均与两位舍入；空源及全空id的原不同结果形态保留。
无效JSON结构仍按项目语义合同失败；Q8/Q9各有独立提示身份，均不冒称原LOTUS formatter／parser。
重复reviewId按发生位置分别请求，不用ID去重或LIMIT提前减少语义工作。
这是“统一语义工程对照”，不是作者原版或官方SemBench系统runner。

同一工厂的`scenario=medical`接[Medical文本任务](text/sembench_native_medical.py)Q1/Q4/Q10；
实际scale只选择原CSV文件，Q4普通patient/symptoms内连接及重复加权平均保留，Q1返回原Series，Q10保留完整诊断关系。
金标在模型前冻结，真实源读取／普通Join／全部推理／完整sink在查询内；评分使用sink副本，避免原Q10评分的小写转换污染完整结果。
Medical原函数／SDK软件差分与同两runner消费者范围见[接线记录](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-medical-text-family)；Q10已有[606f完整源模型回执](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#medical-q10-606f-model)，Q1/Q4及新图像组合另验。

同一工厂的`scenario=cars`接[Cars任务](text/sembench_native_cars.py)Q1/Q4/Q10，保原CSV采样路径、普通merge、
Series／完整诊断关系及Q4的`2026 - mean(year)`；query按投诉加权而gold按车辆计算的原差异不改。
`scenario=ecomm`接[E-Commerce任务](text/sembench_native_ecommerce.py)Q1/Q3/Q5，保完整nested descriptor、
原Apparel谓词和index-label Extract后写行为；gold预先冻结、查询fresh读真实Parquet，原SQL／指标在sink后执行。
E-Commerce Q7复用同一任务的exact pair分支，原price<=500后N²含self／重复ID请求，
原index标签普通回连可扩张结果，最后才原字符串ID拼接；同类Movie pair的系统执行与前置计数接口直接复用。
这是项目exact工程方法，未复现原作者Join策略，见[配对范围](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-ecommerce-exact-pair)。
它们只扩现有任务工厂，不新增系统runner；真实资产／模型与软件范围分列，见[软件范围](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-cars-ecommerce-text)。

每个系统一个runner：[Ray](../experiments/sembench_native_ray.py)使用公开LLM processor／原生AsyncLLM，
[Daft](../experiments/sembench_native_daft.py)使用内置`vllm-prefix-caching`图和既有隔离worker／ACK关闭。
两者只共享任务工厂、评分和观测工具；新增Map／Extract无需新增执行器，系统仍拥有批处理、调度及模型执行，没有逐题runner。
所核固定版本公开AI接点提供生成／结构输出，未提供对应作者级语义Join／Aggregate／TopK算法；
这里的pair predicate明确为项目exact工程方法，不把普通join/groupby/sort加prompt称作作者原算法。
配置与CPU检查／显式运行命令见[脚本入口](../../scripts/README.md#independent-native-text-runners)。
同一Ray runner的显式`serve-session` v2通过[原生caller](../experiments/native_ray_serve_process.py)
在model_python环境持续持有[owned Serve adapter](text/frameworks/ray_native_serve_session.py)，以Ray2.56.1公开deployment／Data processor执行两次fresh查询。
driver只持有任务、原评分及必要批次IPC，不导入vLLM协议或混入另一环境的包；caller／部署启动、查询和关闭分别记录。
原生构造事件绑定的model actor及实际EngineCore进程PID／birth须经当场观测，且两轮实际有模型请求；缺证据时warm=None。
观察器通过[原生初始化callback](text/frameworks/ray_native_serve_observer.py)在模型构造前安装，保留Ray Serve原worker hook及节点初始化；
旧自定义hook造成7aa61真实启动冲突、0生成；修复后751b Q8已完成240真实终态和同owner首／重复，
原外层清理失败经确切恢复，仍不记全程通过；见[具名回执](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#ray-serve-q8-751b-model)，其他组合另验。
该拓扑不继承offline成绩，见[失败与修复](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#ray-serve-native-hook-repair)及[隔离接线](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-ray-serve-isolated-caller)。
同一Daft runner另接[attached-session模板](../../configs/sembench_native_daft_attached_session.example.json)，
复用builtin OpenAI messages graph、既有worker／ACK及永久HTTP预算，两次fresh完整查询使用各自客户端worker。
外部模型服务由调用者拥有，driver不创建或关闭；每个实际批次保留静态system／content layout限制，任务依赖由查询层推进，Animals Q7拆为两次完整Filter批次。
HTTP原件／完整输出及新鲜服务和模型子进程身份在sink后核；缺probe或只复用endpoint时warm=None。
初始软件范围见[接点验收](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-daft-attached-session)；606f Movie Q8已有具名同模型owner实测，新多阶段／视觉组合与实际token读回仍另验，见[后续回执](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#daft-6c75-4793-606f-model)。
生成参数和预算必须显式声明，JSON约束来自本工程对照合同，不能套给LOTUS原Filter。
Q10另显式给Movies.csv供原评分；两系统调用同一任务准备接口，在source计时和模型启动前冻结gold，
sink后评价只使用快照及其字节摘要，不让gold进入模型消息或真实查询输入。缺数据在模型前失败，无额外gold缓存或重试层。

独立时钟`native_ray.source_to_sink.v1`／`native_daft.source_to_sink.v1`包含fresh CSV、项目消息、
原生冷启动／预测、严格解析及完整内存sink；各自同步观测／packet I/O仍披露，最终关闭、源审计、
原评分及结果持久化在后。失败保留已完成结果及原异常，不留成功质量标签。
[独立软件验证](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#independent-native-filter-runners)只授予其零模型范围；
后续[7fb4 Q8](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-q8-7fb4-model)已完成两系统120次真实模型，
[f83b Q10](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-q10-f83b-model)完成各2000次及完整105组；两项均核独立物理清理。
后续[Q9 Map](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-q9map-608e20-model)与
[MMQA Q1 sf25](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-mmqa-q1-8d071-model)另按冻结版本核，
后者Ray首次外清理失败及精确恢复保留；当前新族、其余同模型重复组合与正式性能仍分别待验。
[后续Map组合验证](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#independent-native-map-runners)保留具体提示身份、原后处理、完整空表及失败来源，不与前批计数累加。
[Q10分组与gold边界](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#independent-native-q10-runners)复用同一原生图，不增加题目专用runner。
[Join与计时组合](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-join-resident-accounting)记录完整配对、提前预算检查和resident统计后移；只授予其实际软件范围。

同一工厂还绑定[Animals图像Filter](image/sembench_native_animals.py)Q1/Q3/Q7/Q10，Ray沿现公开processor的
`prepare_image_stage`接PNG内容数组；模型／tokenizer／processor须实际支持视觉，不从文本模型取得资格。
原manifest身份与Species金标在额外预检冻结，真实manifest读、每次图像读取／RGB转换／PNG编码仍在查询内；
Q7的zebra与impala两次Filter都处理完整N行，保留两遍实际图像处理，当前query_batches依次交出N／N请求，
最终才取城市交集排序；旧0d0cc单图平铺批次成绩只归其冻结版本，当前分批组合尚待模型验收。
其余三题沿原COUNT或城市／站点分组取最大，金标只供sink后原SQL／评分。重复图片分别请求，未增加64行执行上限。
现官方输入身份仍为既有sf64 manifest，其他规模软件fixture不代表全8718图目录已接入或验证。
当前Daft本地字符串profile在check及模板渲染前明确拒绝图像等非文本内容，纯文本数组原样交tokenizer渲染，不静默丢图；
已有LOTUS＋Daft外部服务profile仍为另一身份，不能转授这里。
代码及软件范围见[图像接线](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-animals-json-filter)，目标VLM／物理清理及性能仍待验。
现有Ray外部HTTP Q3继续保留其外部模型所有权，身份更正为LOTUS-compatible工程路径，不能重复统计为独立原生语义baseline。

<a id="native-query-batches-visual-families"></a>
### 独立任务的完整查询批次与图文接线

任务用普通Python生成器query_batches(source)交出实际请求，接收整批预测，最后return完整原关系；
[native_query_batches](text/sembench_native_text.py)让既有单批任务沿同一路径执行。
[Ray runner](../experiments/sembench_native_ray.py)及[Daft runner](../experiments/sembench_native_daft.py)拥有原生提交、预算、终态和关闭；
任务没有执行调度器、后端分支或逐题runner。下一阶段仅从实际前序输出生成；首次已知计数在构造消息前检查，
后续实际批次在消息展开及原生提交前检查余量：依赖配对用普通sized iterable，先len再读取配对图片／构造消息；没有执行策略或DSL。未知总量保留None，不用最大值伪作实际请求。
空批不创建原生worker；末批完整sink在最终ACK／释放前形成，必要的中间worker退出仍在查询内。
原source／关系后处理及严格JSON解析保留，评分、额外身份审计和结果写盘在完整sink后。

同一工厂新增四个场景任务模块，各自拥有真实源／普通关系／工程提示和原评分，不复制执行器：

| 任务模块 | 当前绑定 | 保留的原关系与尚未接项 |
|---|---|---|
| [Medical图像](image/sembench_native_medical.py) | Q3／Q8／Q9 | patient与图像普通merge、完整单图／双图Filter后LIMIT；原scale只选择文件 |
| [Cars图像](image/sembench_native_cars.py) | Q3／Q8 | 普通merge、Q3 Manual谓词、完整Filter后原投影／LIMIT；其余图像算法未接 |
| [E-Commerce图文](image/sembench_native_ecommerce.py) | Q2／Q4／Q6／Q8／Q9／Q10／Q11／Q12／Q13 | Q4重复索引后写；Q8/Q9完整pair；Q10/Q11独立Filter分支→真实依赖配对；Q12文本→图片Extract；Q14 TopK未接 |
| [MMQA图文](image/sembench_native_mmqa.py) | Q2a／Q2b／Q7 | 原os.listdir顺序、所有有向候选、Q7无Europe预筛；Q2b仅真实Join匹配进入颜色Extract |

均为项目独立JSON工程语义，不导入LOTUS SDK／提示／解析，不能称作者算法。
E-Commerce Q8显式提供完整标题／描述和图片；原LOTUS1.2.4 Join accessor只选一侧单列的限制仍保留在原路径，
本工程输入不据此宣称模型算法相同。原生生成API没有自动提供作者级Join／Aggregate／TopK。
Q4重复Parquet索引按原Extract标签后写覆盖，Q8/Q9标签回连可扩张关系，物理发生项和模型生成ID不按gold修正。
MMQA独立Q2b零Extract保留原SDK缺color列的KeyError；此前LOTUS＋后端补空schema的旧合同已退役，冻结结果仍按原来源解释。

Ray需显式prepare_image_stage及真实VLM；Daft图文使用显式attached-session，offline字符串profile保持原拒绝规则。
各场景的真实资产／模型资格与新增软件范围分列，见[完整验收与缺口](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-query-batches-visual-families)。

<a id="ray-native-prepared-sembench"></a>
### LOTUS＋Ray 原生执行的完整查询 sink

现有 `run_sembench_text_query.py` 使用
[显式 `ray_native` 配置](../../configs/sembench_ray_native.example.json)时，调用
[原生 processor 接点](text/frameworks/ray_data_sembench_native.py)的 `query_scope`／`owner.predict`。
Ray Data LLM processor 拥有图、chat template、tokenization、batching、模型 actor 和调度；
[完整查询预测接点](text/sembench_prepared.py)让原 LOTUS SDK 实际构造消息、解析并继续执行固定 SemBench 原函数。
不调用 SemLoom Core、原自写记录链或 HTTP 替代模型。
这条路径的身份是 **LOTUS＋Ray 原生执行**；它不是另一个独立 Ray 语义 baseline。
同一个查询 owner 接受原算法实际产生的多批预测，不按题号或固定阶段数复制执行器。
所查Ray2.56.1的[Processor](https://raw.githubusercontent.com/ray-project/ray/ray-2.56.1/python/ray/llm/_internal/batch/processor/base.py)
每次Dataset调用建立新图；[原生actor pool](https://raw.githubusercontent.com/ray-project/ray/ray-2.56.1/python/ray/data/_internal/execution/operators/actor_pool_map_operator.py)
属于物理算子。分组线程可同时建立多个processor，`concurrency`是每个processor的配置，
不是查询总上限；共用runtime不保证跨批模型复用。完整sink、actor终止与GPU子进程退出须分别观测，
多GPU还须核原生placement group释放，不能仅凭丢弃pool引用判定已清理。
Movie Q9 quick TopK 和 MMQA Q2b Join→Extract 已通过下述完整原题软件差分；
MMQA Q5层次聚合另按[对应tokenizer／context条件](#sembench-mmqa-aggregation)接入，实际模型资格分别核验。

同一入口可显式运行MMQA原Q2a／Q2b／Q7：配置`scenario=mmqa`及所选题，
在`ray_native.processor_options`中设置`prepare_image_stage=true`，绑定实际VLM／processor环境。
LOTUS和独立Ray入口现共用原生stage启用判断，也接受`{}`或`{"enabled": true}`及原生配置对象；
dict按Ray2.56.1的bool解析规则处理，显式禁用仍拒绝图像。配置原样交SDK，不改图像处理或查询时钟，
见[入口反例与验证](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#ray-image-stage-binding)。
共享任务读取原CSV和`images/`，保留原目录顺序、左右关系及L×R预算；原函数仍拥有Join和最后投影。
普通无歧义列名及`:left/:right`引用均按原字段绑定；原`ReasoningStrategy.ZS_COT`在准备和结果恢复时保留。
完整原PNG消息直接交Ray原生图像准备、chat template、tokenization和vLLM actor，不把图片路径当普通文本。
像素／编码身份在源起点前和sink后核对，worker也核对已检查的图像快照；实际读图和PNG准备仍在完整区间内。
Q2b由原函数把实际Join结果交给原Extract，JSON约束显式映射为原生vLLM `structured_outputs`；
空匹配不预造非空第二阶段，预算上界为两倍L×R、实际扣除按原生请求发生。
该能力不转授普通记录图、其他后端或新的多批目标资格。旧90项及dde单批来源保持原身份，
当前公共边界的目标processor／完整token／VLM与跨批清理仍待验；必要原生启动计入查询。

[查询编排](../experiments/native_ray_sembench.py)重新读取已核输入，按 occurrence `call_id` 接收完整预测。
每次原LM调用交给原生 `materialize().take_all()`，原SDK收到答复后继续解析和后续算子；
整次原函数返回后才 `ray.shutdown()`。一个runtime不表示跨批模型actor或模型实例复用。
原查询返回的内存结果立即标记 `sink_ready_ns`。`source_to_sink_seconds` 包含本轮新鲜 source/prepare、
Ray/模型启动及原关系计算；driver消息摘要、关系序列化、评价、结果持久化和关闭在sink后另记事件。
当前时钟为`lotus_ray.source_to_sink.v1`，起点位于输入身份／配置核对、预算领取之后；准备的是已绑定输入，
原查询开始时真实读取CSV，图像在实际算子格式化时读取，不先准备首阶段再重跑原函数恢复关系。
driver完整文件摘要和逐消息摘要在sink后核对；原始批次／答复按独立call_id保存，价格／用量以真实backend回执为准。
actor保留原生API调用前的预算预扣，删除重复逐请求成功事件、输出／token副本及其批间文件检查。
已静态核对服务器Ray2.56.1：原生processor只返回finished输出，否则抛错，默认错误不继续；adapter不再重包原迭代器。
完整API结果及call ID覆盖仍检查，预算预留不表示引擎接受或GPU工作量。目标SDK执行与新模型计时仍待验，
早期统一v3声明不恢复，冻结旧时长不改写或相减，见[原生完成契约](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#ray-native-completion-owner)。
该完整查询结果schema为`semloom.sembench_text_result.v2`；`native.batches`保留实际预测批次，
query级`events`保留最后关闭，旧单批报告与默认记录图v1不回填或改记。
原始预测只存`native-predictions.json`；查询摘要保留call ID／消息摘要，
`native-lm-batches.json`保存原请求和原／有效参数，不再嵌套复制同一批预测。
`query_seconds` 仍是包含审计和 runtime close 的较宽区间，不能代替 sink 指标。
最早host/controller入口未在此worker观察，保持空值。`native_observation`记录实际API预留／完整结果数，
不再产生首提交／末终态／unknown事件；独立Serve只按需保留模型构造身份事件，不把缺少独立逐请求观察填成零。

`profile` 是本地独占 `ray.init` 参数；`ray_native.processor_options` 保留未覆盖的原生默认。
引擎显式设置由 `engine` 与 `ray_native.engine_kwargs` 一致组合，冲突拒绝。
模型 actor 的 `py_executable` 使用声明的原生环境；不套用项目模型桥的固定 vLLM 版本。
Ray 2.56.1 / 官方 vLLM 0.22 可作为目标组合，实际版本由 actor 回执核验。
actor setup hook 复用已核 Q7 的版本相关接点；SDK、跨解释器加载和 tokenizer 资格绑定实际运行版本。

调用方先预留永久 SQLite 的单次单位，worker 领取后由原生 `AsyncLLM.generate` 前的
[预算观察器](text/frameworks/ray_native_observer.py)逐次扣除；相同提示不去重，失败不返还。
模型 stage 明确设置 `max_restarts=0` / `max_task_retries=0`，不重跑整查询。
单位计数或完整原生API结果不匹配时不能报成功。原 parser、后置审计、close 或持久化失败均保留失败状态
及已有答复；已观察到 sink 不会因此擦除。外层继续使用既有 host 租约、资源监督和物理清理，
`ray.shutdown` 的 driver 断开本身不证明模型进程、SHM 或 GPU 已释放。
`startup_timeout_s + query_timeout_s + 30` 是既有监督器的 worker 总窗口，包含其退出预留；
不是独立 host 全流程期限。此入口只实现单次冷查询，不声称同模型owner暖轮。

公共任务的`bind_execution(multiple=True)`按原函数的真实算子顺序调用
`execute(operation, invoke)`，返回`(完整结果, 算子报告列表)`；callback返回含`frame`的报告，
TopK另含`native_stats`。首算子保留旧记录ID，后续记录加`operator-N`区分，后续输入来自真实中间关系。
`bind_original_execution()`则不截获算子，直接运行完整原函数；`make_query_lm`按实际LM批次使用
`query_id-batch-N-row-M`关联原生预测。默认`bind_execution()`仍服务尚未迁移的单算子记录图，
不假定它们自动具备多阶段能力。结果写盘、额外审计和评价均在完整sink之后。
原生groupby线程共享LM时，批次号在登记锁内分配，编号表示登记顺序；每批原生报告保留
`input_call_ids`，不按两个报告列表的追加顺序猜关联。Ray首次初始化及关闭由同一runtime锁保护，
初始化失败保留原异常而不重试；锁不包住预测，不改变原算法的并发执行。

[LOTUS原题对象](sembench_lotus.py)由原生LOTUS服务与上述Ray查询任务共同使用，
接收已有输入快照，拥有原函数、真实后续关系、输入复核和原评分，不拥有模型／预算／时钟。
外部服务上的视觉／多模态查询和MMQA Q5共用[同一查询生命周期](../experiments/sembench_lotus_query.py)；
Q5保留原层次聚合及tokenizer预检，不再独立复制查询worker。旧入口从[准确来源](../../../experiments/reproduction/native_baselines/README.md#retired-lotus-service-query-runners)恢复。
同一Ray完整查询入口也按既有Source的视觉能力绑定Animals、Cars、Medical和E-Commerce，
使用该原题对象读取真实CSV／Parquet／图像，继续执行真实后续关系及原算子，不先造单算子结果。
配置显式启用`ray_native.processor_options.prepare_image_stage=true`；Animals给`manifest`／
`manifest_digest`且省略`--data-dir`，其他场景仍给原数据根目录。Extract自己的JSON请求保持，
全局格式／logprobs等不覆盖原算子；未知总量保持`None`，有限`engine.max_model_calls`和永久预算约束实际调用。
该服务查询的当前时钟为`lotus_service.source_to_sink.v1`：HTTP额度预扣、请求指纹、
图像审计hash和事件编码／入队仍在区间内，后台写盘可能重叠；最终drain、后置审计、
结果编码与评价在sink后。它不是统一v3口径；旧字段／数字保持原件，不事后扣除观测成本。
MMQA不再另设cached CSV reader来准备首阶段后重跑恢复；被替代的`load_data`接线已移除。
typed callback中的`operation.frame`保留原dtype；记录字段不是原生图像payload的替身。

CPU 回放使用 `python -m tests.baselines.text.replay_ray_sembench --help`，Ray API 与模型为明确的软件替身。
Movie Q10 的 2,000 条真实保存答复保留 1,864 个不同提示及 136 个重复提示 occurrence；
完整消息/token、原 Map 关系、105 电影 sink 和原 evaluator 与冻结参考一致，2,001 次申请被拒绝。
这些答复来自历史 Daft Q10，只用于接线等价，不是新 Ray 质量或性能。受控时钟证明后置摘要、
序列化和关闭不进入 sink 区间；成功、解析/审计/关闭失败、空输入和乱序关联均有软件检查。
`ac4440328` 另已完成目标原生 Q10 full2000、Q8 120 和 Q9 Map256：实际 token／答复、
完整原 sink、永久预算及物理清理逐项核对，零 unknown、零重试；Q10 保留原105组普通聚合。
Q9 Map 不代表 TopK。上述是该冻结提交的独立单冷成绩，当前整合仅做受影响 CPU 回归，
其余题与同 owner 暖轮仍按各自回执判断。

旧的无 `ray_native` 配置继续作为明确标注的历史记录图入口；其耗时不纳入新口径。
该记录图仍有独立调用者，未被本次prepared接线完整替代。旧Q3专项API已按前述恢复说明退役；
其余记录图在完成对应调用迁移与验证后再退出，不删除原始成功/失败证据或活跃公共清理helper。

<a id="lotus-ray-serve-ready-repeat"></a>
#### 模型就绪首轮与同模型重复

同一[完整查询runner](../experiments/native_ray_sembench.py)现支持
[Serve配置](../../configs/sembench_lotus_ray_serve_session.example.json)，沿原`check/run`入口选择
`lifecycle=serve-session`。先建立一个原生Ray Serve deployment，再两次调用原LOTUS完整查询函数，
每轮fresh source、实际依赖批次、parser／关系后处理和完整内存sink保持；没有逐题runner或替换成JSON任务。
原生chat请求保留每算子采样：普通Filter／Map／TopK不强加JSON，Extract自己的JSON-object按native API同义映射。
独立JSON入口仍要求其原JSON格式，两个语义身份不合并。固定Ray2.56.1，实际vLLM／模型环境另核。

现有[隔离caller／会话生命周期](../experiments/native_ray_serve_process.py)由两条路径复用；
同一个永久unit覆盖启动及两轮实际调用，每轮用attempt差量与native批次offset核对，末轮后统一关闭。
`round-N/query.json`仅由会话层发布，原LOTUS批次／预测分别保存；原冷路径仍独立拥有unit／runtime及报告。
原groupby线程共享单个顺序IPC caller，因此不同LM批次顺序提交，各批内部仍由原生Serve调度；
不能称与offline并发形态完全相同。锁取得后和caller接受命令前核已有期限，stop后不再接新工作。

`ray_native.serve.session_timeout_s`覆盖模型启动、两轮查询及清理，另有启动和清理保留；
本profile不支持单查询deadline，显式`resources.query_timeout_s`会在check拒绝，不留下无效限制。
`serve`的processor选项仅batch_size／concurrency；图像经原LOTUS真实PNG内容与Serve chat carrier，
不用offline的prepare_image_stage。真实renderer／token及VLM条件须按目标SDK验证，不能从软件消息相同外推。

每轮`lotus_ray_serve.source_to_sink.v1`从原数据读取到完整原结果；模型启动、后置审计／评价／写盘和最终关闭分列，
必要API预算、batch IPC和模型执行留在内。caller启动包含deployment启动，不能相加。
warm要求两轮实际非空、无新增constructor、当场model actor和EngineCore PID／birth均保持；
缺证据／零请求留空，prefix cache开启不证明命中。每轮原RNG、真实工作量和质量分别保留。
43e原Q8及8b73原MMQA Q4均已真实执行同模型两轮，Q4低质量与外层SHM清理失败及恢复保留，见[原件与范围](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#lotus-ray-mmqa-q4-8b73-model)。
会话在自有隔离runtime中以原生`serve.shutdown()`关闭应用和Serve系统组件，再`ray.shutdown()`断连；
配置／build失败也走同一关停路径，外来runtime／同名应用不被关停。删除原逐应用delete及冗余status读取。
38项相关软件检查通过、0模型；原生API返回不证明物理GPU／SHM清理完成，仍由目标监督核对。其他任务／renderer与正式匹配比较另验。

<a id="lotus-native-resident-sembench"></a>
### 原生 LOTUS 的显式 resident 文本查询

现有 `run_sembench_text_query.py` 默认仍执行 isolated 单查询。选择
[resident 配置](../../configs/sembench_lotus_native.example.json)后，同一原生 LOTUS owner 执行冷查询和
指定数量的新鲜暖查询；每轮重新读取 source/prepare，经原 accessor、LM、LiteLLM 和本地原生模型 IPC
到完整原 sink。`native_lm_max_batch_size=64` 是显式实验 profile，不是作者默认；SemLoom Core 不参与调度。

`resident.model_initialization` 默认 `inside_first_query`，保留首次查询内惰性启动和 `cold/warm` 标签。
显式 `before_queries` 先调用同一原生 owner 的 `start()`，随后执行 `ready_first/ready_repeat`；
不增加模型查询预热，也不更改原生引擎初始化。启动、实际 ready 和 source 起点分别记录；
`model_ready_before_source` 及其观测时间区分两种条件，不能仅凭 first/repeat 标签推断缓存冷热。

```bash
PYTHONHASHSEED=3 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=code "$DRIVER_PYTHON" -B \
  code/scripts/experiments/run_sembench_text_query.py check \
  --config "$RUN_CONFIG" --sembench-checkout "$SEMBENCH_CHECKOUT" --data-dir "$DATA_DIR"
```

经唯一 host 协调者安排后，同一入口使用 `run --execute-model --output-dir "$NEW_PRIVATE_OUTPUT"`。
外层继续提供 host 租约、CPU/GPU 限额、空闲预检和最终物理清理；本入口不再创建一套 host 控制器。
配置绑定所选当前 commit/code tree、输入文件与有序发生项摘要、完整模型/分片和 tokenizer 文件摘要，
以及实际 generation/resources/profile；真实路径保存在仓库外。模型文件列表应列全实际权重、索引和配置，
不能只填写示例的单个占位项。`occurrences_sha256` 由现有原查询的 `occurrence_digest` 产生。
永久 SQLite 的各轮单位须由调用方预留且未被领取；worker 逐轮领取，失败不退还，结束后关闭该单位。

resident F/M/E现显式延迟完整binding／LM统计，完整query sink之后、owner关闭之前只汇总一次；
默认cold和其他borrowed stage调用不变。原后处理或后验统计失败仍保留原异常、已完成结果与晚到成本。
已核固定可达原题未原地改写返回关系的既有值缓冲区，浅快照隔开新增列／重命名并保留原中间schema，免除整表值复制；
没有修改pandas COW。真实请求身份／预算、原SDK工作与轻快照仍在区间，旧a110成绩不回填或扣减，
具体范围见[计时收尾](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-join-resident-accounting)。

原 stage 可借用 owner，成功或失败均由外层生命周期负责最终关闭；原 isolated 调用仍自行关闭。
source→完整 sink 与最早脚本事件→sink、监督器 Popen 前事件→sink 分列；模型实际 ready、首原生提交、
末确认终态、snapshot/审计、评价、查询持久化和最终 close 分列。adapter-ready 不冒充模型 ready，
引擎首次接受和 kernel 时间留空。每轮持久化终点在 `session.json`，该文件本身的写入终点见
`session-persistence.json`；不能从文件写入前的快照宣称已落盘完成。
示例 `timeout_s=600` 已含监督器的30秒退出预留；正常 owner close 在 worker 内，外层 host 清理另计，
不能称完整 host 流程已被这600秒覆盖。结果缓存关闭，模型 KV 和磁盘缓存按真实配置记录，不伪称全冷。

完整软件回放使用 `python -m tests.baselines.text.replay_lotus_resident`，保存答复按完整消息身份绑定，
由真实原生 accessor/LM/LiteLLM/IPC执行，具体资格和已完成的冻结 GPU 结果见
[报告](../../../experiments/results/diagnostics/lotus_mmqa_q3a_native_20261110/README.md)。
完成后的 session 报告按实际文件大小读取，配置文件仍保持默认大小限制，并拒绝符号链接和非有限数。
`50eae2e04` 已完成 Movie Q10 冷暖各2000次及独立离线审计；长度修复没有改变原查询或计时边界。
当前整合不重跑该模型资格，不将它改记为整合分支的实测。

<a id="lotus-movie-q9-topk"></a>
### Movie Q9 作者 TopK 候选

同一 CLI 显式设置 `scenario=movie`、`query=9`、`ranking=topk`、`backend=lotus-native` 和
`lifecycle=resident`，调用原 SemBench `quick`、`K=N` 分支。省略 `ranking` 时 Q9 仍使用原 Map。
TopK 不进入 F/M/E 记录图；原 LOTUS 拥有随机 pivot、比较批次、答案解析和排序。
比较绑定左右 source occurrence，再由原 LM 的有序返回关联终态，不能按并发抵达顺序或相同文本去重。

使用现有 resident 配置模板；单冷资格设置 `resident.repetitions=1`，只预留一个冷单位。
每轮额度覆盖 `N*(N-1)/2` 比较上界，可保留有限余量；模型和永久总上限分别覆盖全部轮次，不再要求与实际工作量相等。
8 条样本最多 28 个模型请求，实际比较数可更少；两轮不可共用一份 28 次预算。
结果缓存关闭，`num_retries=max_retries=0`；每次实际 IPC 提交前由原预算守卫收费。
完整原查询返回即记 sink，再按保存的实际 RNG 状态、比较答复重放原算法并核对完整关系和原 sink，
随后评价、持久化和关闭。重放不发模型请求；审计失败保留失败与原生答复。

CPU 软件资格使用 `python -m tests.baselines.text.replay_lotus_topk replay`。
输入字段、发生项数和最坏比较量从原查询取得，不限8行或`reviewText`。相同文本仍是独立发生项，
软件常量答复可共享，原文档位置与比较身份不按消息摘要合并；空原查询及单行不启动软件模型。
该模块的 `renderer` 子命令只核目标 vLLM 原生消息渲染/token和prompt加原输出上限的context，
不加载引擎或权重。完整输入可使用同一入口，但仍需独立冻结数据、资源上限与目标验收。
TopK 原消息含文本列表，不能直接把 Qwen tokenizer 的 TypeError 用自定义提示绕过。
本机 source-derived token 参考与目标原生 renderer 验收分开；软件回归见
[原 quick 比较测试](../../tests/baselines/text/test_lotus_native_topk.py)，renderer 回执路径修复见
[入口测试](../../tests/baselines/text/test_lotus_topk_renderer_receipt.py)。
原候选 `2338d4c49` 和回执修复 `c9cadb1df` 的资格按各自冻结记录判断；当前整合未运行目标 renderer 或模型。
既有 Q9 Map 的 512 次冷暖不授予 TopK 质量或性能资格。

<a id="additional-sembench-text"></a>
## 补充 SemBench 文本查询

[工作负载](text/sembench_text.py)直接执行固定 SemBench 的原函数，原函数仍拥有普通筛选、
关系关联、Map 评分的原回退和最后汇总。唯一普通 Filter／Map／Extract 在其 accessor 边界交给
[既有原生记录图](text/frameworks/sembench_text.py)或[原生 LOTUS](text/frameworks/lotus_native_stage.py)。
不运行上游构造器的隐含暖机、下载或近似策略。入口及配置见
[脚本说明](../../scripts/README.md#additional-sembench-text-entry)。
原查询函数在任务准备阶段校验并绑定一次，执行阶段继续读取实际源文件；不把重复 Git 校验、
模块加载算作原查询工作，也不以预检 DataFrame 替代真实查询扫描。Movie／MMQA／Medical 的
已有独立调用入口继续复用同一个绑定函数，保留其现有调用方和行为。

共享 Movie source 保留原 `Reviews.csv` 的全部关系列，与官方 `load_data` 一致；只由原算子声明的
字段进入模型消息。评价标签留在关系中不表示发送给模型，Join 的中间关系也保留未引用列。
旧三列投影会改变完整中间关系及其物化成本，现已移除。原查询／后端受影响软件检查及保存的
Daft／Ray 各64条真实答复回放用于核对完整关系，不重用旧投影路径的耗时。

原生 LOTUS 保留 accessor、LM、LiteLLM 原批调用与解析，provider 复用现有本地模型 IPC。
它观察原 LM 的真实消息，不额外格式化预遍。旧 ledger 的 `lotus-filter-v1` 是保留的身份协议，
Map／Extract 的真实类型另由 stage 和有限阶段 ID 映射记录，不把它们当 Filter 算法。
无 `ray_native` 的 Ray／旧 Daft／IMLane 使用原来已有的有限记录图和单个阶段；
新的 [Ray 原生完整查询](#ray-native-prepared-sembench)及 Daft AI 入口另行明确选择。
它们与旧普通 Filter 图是分别命名的执行布局。
原 Q1/Q2/Q3 及 Q4 的普通 records、session、矩阵入口继续保留。
正式 `SemBenchTextQuery` 也已开放 Movie Q1/Q2/Q3，Daft 原生 AI 使用同一 prepared 入口，
不再被题号白名单拒绝；这属于项目查询绑定缺口，不是 Daft 不支持 Filter。
Q1/Q2 保留作者带引号的完整指令，Q3 使用作者另一条原指令，不混用提示。
先执行全部原 Filter 输入，再由作者原函数做 head(5)／COUNT；重复 reviewId 仍是独立 occurrence。
全阴性时 Q1/Q2 保留作者空表的三列，Q3 返回单行零，不修饰成另一种 schema。
原 accessor、完整消息、完整 token、sink 和 evaluator 的零模型回归不授予真实模型或性能资格。

**旧 isolated 计时不能直接进入统一 source→内存 sink 比较。**
[旧单算子入口](../experiments/sembench_text_query.py)的 `query_seconds` 等待完整 operator 返回，
其中 Ray／Daft 记录流在 EOF 内关闭，IMLane 作者阶段也使用 `close_on_eof=True`；
审计文件回收、模型／作者清理及 Ray runtime 断开可能已在原关系汇总之前完成。
此字段保留原始含义；新的 `source_to_sink_seconds` 明确留空并附未观测原因，不以改名、减去
关闭用时或历史结果回填制造统一口径。这包括旧 Movie Q10，不能与已有独立 sink 边界的
[Daft 原生 AI](#daft-native-prepared-sembench)及[LOTUS resident](#lotus-native-resident-sembench)时间混用。
需要补测的原生路径须在完整原关系交付时观察独立终点，并将事后审计、评价、持久化和关闭另计；
不修改原作者算法或调度来补非空结果。现有成功、失败、质量及 cleanup 证据保持不变。

下表是现成 Movie2000、原 MMQA generator 的 sf25／seed42 单次完整执行预计调用量，
不含暖机、重试或重复。Movie 金标 `Movies.csv` 为116行、105个唯一电影 ID，覆盖评论中的全部105部电影；
仍由原 ranking evaluator 处理 ID 与有效分值，不修改原金标或自行去重。

| 数据集 | 题号 | 原方法 | 实际模型输入行数／预计调用 |
|---|---|---|---:|
| Movie | Q1 | 完整评论 Filter 后原 head(5)，不提前停止或去重 | 2000 |
| Movie | Q2 | taken_3 完整 Filter 后原 head(5)，不提前停止或去重 | 120 |
| Movie | Q3 | taken_3 完整 Filter 后原 COUNT | 120 |
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
| 原生 LOTUS | 上述十九题，原 accessor／LM 路径 | 原查询差分与 resident 分开验证；实际模型范围按冻结提交判断 |
| Ray Data | 上述十九题的有限记录图；显式原生 processor 另见上文 | 旧图无独立 sink 边界；原生 processor 不继承旧图时长 |
| Daft | 上述十九题的有限记录图；原生 AI 另见上文 | 旧图无独立 sink 边界；原生 AI 不继承旧图时长 |
| IMLane | 有限记录链的语义合同与独立 DuckDB 作者入口分别核验；原生 TINYINT Filter／DOUBLE Map 已有具名真实运行 | 原 `072f8257` 的 VARCHAR 返回通路存在根／子 Arrow schema 错配；不把十九题合同 fixture 或 `398ba063` 工程补丁算作作者原版全部支持 |
| Kalypso | 专门作者 CLI 含五场景原题文本 Filter、普通 Map、原 CP＋Filter Join 及原生完整排名 TopK；旧 Movie／私有入口分别保留 | 本统一文本 CLI 未暴露作者服务；新字段／配对／排名绑定按源码分别验证，作者环境／模型另验。TopK 保留作者 insertion／merge，区别于 LOTUS quick；原 `05024f02` 无原生Extract／JSON格式透传；现有[文本Extract-via-Map](text/frameworks/kalypso_native/README.md#text-extract-via-map)明确为工程适配，复用同一runner，仅e67cc MMQA Q1已有具名模型回执，其他组合另验；Filter／Map／文本Extract可由同一owner顺序运行，原预算／算法保留，混合类型序列模型待验；图像carrier仍未实现 |

这些是 CPU 接线范围，不是模型质量或性能成绩。原查询／原 accessor、共享链和 NativeStage 的
软件回复差分与原十六题 CLI fixture 均运行；真实模型逐行一致性按对应源码和运行回执判断。
原 aggregate 分数不能排除相抵的分类错误，保留原调用文本和旧普通 Filter 的逐行审计供核对。
MMQA Q1 的原 Series 仅在原评价入口按官方 CSV 往返转换成同名 DataFrame；Extract 保留实际额外／碰撞 key
和原 Index label 写入，不补未返回的声明字段。后处理失败仍保留已完成调用与关闭回执。

本地组合为 LOTUS1.2.4／LiteLLM1.95.1／OpenAI2.50.0／pandas2.3.3／Arrow24；
它不冒充服务器 Linux LiteLLM1.95.0 的精确软件资格。物理 GPU 退出仍由现有外部 host supervisor 核查。

<a id="sembench-medical-text"></a>
### Medical 原文本查询

[Medical 适配](text/sembench_medical.py)直接调用原 Q1／Q4／Q10 脚本、原金标生成和原评价：
Q1 为症状 Filter，Q4 为原患者关联后的 Filter／平均年龄，Q10 为原疾病字段 Extract。
原提示中的研究用途说明、Series／DataFrame 结果及无匹配时的 NaN 行为保留。
准备入口只复制已存在的三份原 CSV，不采样、不运行图音 setup；原评价只绑定所需文本和标签关系。
原评价源码使用 Python 3.12 的 f-string 语法，运行 driver 需要 Python 3.12。

固定上游完整资产有11112名患者、1200条症状及对应标签；三题各1200次调用，每个 baseline 共3600次，
四种普通 F/M/E 合同的计划额度合计14400次，不表示四种原版均支持或全部通过；IMLane 的 VARCHAR
限制同上。Kalypso 未由本统一 CLI 暴露，私有作者入口按其独立绑定／回执判断，不能据此 CLI 判为
整个系统不支持 Medical。`scale_factor` 表示患者规模，不能直接当模型输入行数：
原 `_25` 资产有25名患者、仅1条症状，且没有 Q1 的 allergy 或 Q4 的 acne 金标，只适合诊断。
该诊断切片的 Q1 空集对空集可被原评价记为1，Q4 保留 NaN；不能把这些结果当作模型质量。
原 Q10 指令的 `DIMORPHIC HEMORRHOID` 与原标签复数拼写也不被适配器改写或归一化。
`check` 读取原关系并核算实际调用数；尽量匹配模型、可公开配置的生成项和输入身份。
各系统保留原生依赖及必要策略，不能匹配的实际参数／提示／parser 差异单列，不强改作者算法。
补充文本入口的配置省略生成项时均采用原 LM 的8192 token／无 stop，显式值保留。
这改变了旧补充 Movie／MMQA 的省略项解析；显式8 token／换行 stop 的旧配置和普通 Filter 类型默认不变，旧结果不改写。

### Medical 原图像查询

同一[Medical任务绑定](text/sembench_medical.py)把原Q3／Q8／Q9脚本接到既有
[LOTUS视觉服务入口](../experiments/sembench_lotus_query.py)，不另建执行器。
配置使用`scenario=medical`、所选题及`scale_factor`（默认11112），`data_dir`为原场景根目录。
保留`data/patient_data[_N].csv`、原题所需`image_x_ray_data[_N].csv`／`image_skin_data[_N].csv`
及实际图像；`patient_data_with_labels[_N].csv`只用于原评价，标签不进入模型消息。

Q3／Q8先按原患者条件连接全部图像并Filter，之后才取原LIMIT 5／100；重复患者和图像不去重。
Q9保留两表与患者的原连接，每个发生项把皮肤与X-ray两张图一起交原生Filter，预算按发生项N，
不是按图片数2N。预检在原首算子边界取得实际行数和图像身份；真实CSV读取、图像编码和完整关系
保留在上述服务源→sink区间内，服务启动由外部owner另记。

原MedicalEvaluator直接生成／读取金标和评分；Q3生成的Series金标通过原缓存读取接口恢复官方CSV形状，
不重写评分。Q3／Q8的小金标无法满足原评价采样5／100时，保留原失败，不补标签或放宽限制。
当前52项不同本地软件检查通过，覆盖三题原消息／完整结果／评价和既有文本路径；
真实图像资产、目标renderer与VLM资格仍待核，不能由临时图像或软件HTTP答复转授。
音频题及固定上游未实现的脚本不由此取得支持。

### Cars 原文本查询

[Cars 绑定](text/sembench_cars.py)直接执行官方 Q1／Q4／Q10，复用同一个文本任务、Filter／Extract、
prepared、原生 LOTUS 及已有后端入口；没有新的题目执行器。设置 `scenario=cars` 和所选题，
`scale_factor` 默认157376，仅选择 `data/sf_N/` 下原 CSV；模型调用量从真实投诉／车辆连接关系取得。
数据目录为作者 Cars 场景根，须保留 `data/full_data/` 金标 CSV。Q1 不要求车辆／图像／音频表，
Q4／Q10 只额外读取车辆表；完整标签不进入模型消息，也不在执行区间扫描。

Q1 保留 `car_id` Series，Q4 保留原 `2026 - joined.year.mean()`，Q10 保留完整多分类关系与重复车辆行。
原 CarsEvaluator 的标签筛选、gold 生成和指标直接复用，gold 缓存写入新临时目录；
评分输入共用[原 CSV 往返](text/sembench_evaluation.py)，不向上游 checkout 写文件。
Q4 查询按投诉连接后的行加权，而作者 gold 按车辆选择求平均；该差异保留，不能为了得分去重查询行。
Q10 继续用作者 macro precision／recall／F1，不做类别修正、答案截断或按车辆 ID 合并模型调用。
原 SDK、记录链、原生 LM 接点及 prepared 软件对照覆盖三题；真实资产和模型资格仍待验证。
IMLane 原版 TINYINT Filter 也复用同一任务绑定接 Cars Q1/Q4；现有 native SQL、owner、预算及 sink ACK
不变，配置见[Filter 源规模与构建身份](../../integrations/imlane_duckdb/README.md#filter-源规模与构建身份)。
Q10 的字符串 Extract 仍不属于该原版 TINYINT 接点，不借用工程版 VARCHAR 的资格。
文本入口不处理图音；Q3／Q8 使用下述同一原生视觉服务。

<a id="cars-original-csv-preparation"></a>
#### Cars 原 CSV 准备

[准备脚本](../../scripts/data/prepare_sembench_cars_inputs.py)从现成官方 `full_data` 的四份 CSV 调用
固定 c814 的原 `scale_down_data(seed=42)`，保留原分层抽样和输出投影，不触发上游 `prepare_data`
附带的全部图像／音频下载。四表元数据仍是抽样必需项，不以空图音表替代；完整标签逐字节保留。

```bash
"$DRIVER_PYTHON" code/scripts/data/prepare_sembench_cars_inputs.py \
  --sembench-checkout "$SEMBENCH_CHECKOUT" --source-dir "$CARS_FULL_CSV_DIR" \
  --output-dir "$NEW_CARS_SCENARIO_DIR" --scale-factor "$SCALE_FACTOR"
```

输出含 `data/full_data/`、原 `data/sf_N/` 和来源／输出摘要；大于原车辆数的 scale 按作者改为实际车辆数。
小规模抽样的作者最少分层数量也保持，不自行截成 N 行；实际各表行数另记，模型调用量仍由原题决定。
图音路径原样保留，manifest 明确未检查媒体内容；CSV 准备不授予 Q3／Q8 或真实模型资格。
临时元数据上的原抽样、四表完整标签／投影、三条文本原函数及真实 CLI 检查属于零模型软件证据。

### Cars 原图像查询

同一 [Cars 任务绑定](text/sembench_cars.py)运行原 Q3／Q8，复用[原生视觉服务](../experiments/sembench_lotus_query.py)
的 LM、预算、观测和完整 sink。设置 `scenario=cars`、所选题和可选 `scale_factor`（默认157376），
`--data-dir` 仍指向作者场景根。原函数读取 `data/sf_N/` 的车辆及 `image_car_data_N.csv`，
按原 `image_path` 读取图像；迁移数据路径须在正式运行前固定，执行器不改写这些路径。
评价另读 `data/full_data/car_data_full.csv` 和 `image_data_full.csv`，不要求文本／音频表。

Q3 的 Manual 普通筛选、完整图像 Filter 和最后 `head(10)`，Q8 的完整 Filter 和最后 `head(100)`
均由官方函数拥有；预算按实际连接后的图像发生项计算，重复车辆／图像不去重，不提前 LIMIT。
原 CarsEvaluator 按采样 image_id／car_id 选择完整标签，并保留其命中相关的金标采样和 F1；
gold 缓存只写临时目录。源码、CSV／图像及金标身份校验位于查询区间两侧，真实 CSV 读取、
ImageArray 解码／编码和关系处理仍计入。临时资产／HTTP 软件检查不授予真实 Cars 或 VLM 资格。
固定 LOTUS tree 只有 Q1/Q3/Q4/Q8/Q10 五份查询文件；Q2/Q5/Q6/Q7/Q9 缺少原实现，
不能为凑全题数自行添加音频算法，也不据此推断其他系统不支持。

### E-Commerce 原文本查询

[E-Commerce 绑定](text/sembench_ecomm.py)直接调用官方 Q1／Q3／Q5／Q7 的 `run(data_dir)`：Q1 保留原 Filter，Q3 对完整
`styles_details.parquet` 做品牌 Extract，Q5 由原普通谓词筛选 Apparel 后做命名列 Map。
嵌套描述字段按原 DEFAULT formatter 完整呈现，不摘取子字段或自行改写提示；模型消息只使用
原题声明的 `productDisplayName` 与 `productDescriptors`，标签不进入消息。
Q1／Q3／Q5 复用上述普通 Filter／Extract／Map、prepared、原生 LOTUS 和既有后端接点，没有独立执行器。
源固定检查也覆盖 `files/ecomm/queries`；查询真实重读 Parquet，不以预检快照替代。
Q7 则由同一个原生 LOTUS resident Join 接点运行：保留原 `price <= 500` 选择、两侧各两列描述、
全部 N×N 有序候选对及原 `id` 拼接，包含自身和对称项；不套用 Movie 的后置 ID 排除。
Join 的 positional／keyword 参数共用普通绑定，任务可声明多列；实际入模字段由原 SDK 决定。
当前 LOTUS1.2.4 accessor 每侧只选一个 Series，新增 `native_join_inputs` 记录实际选列、dtype、
发生项数及遗漏引用，不向原生 prompt 补字段。已有多列差分通过仅证明与该 SDK 的行为一致。
此入口仍显式选择 exact，
官方 runner 的 approximate 不被声称为已复现；Ray／Daft 原生 prepared Join 见下述共享接点。
上游 [v1.1.3](https://github.com/lotus-data/lotus/blob/v1.1.3/lotus/sem_ops/sem_join.py) 和
[v1.2.4](https://github.com/lotus-data/lotus/blob/v1.2.4/lotus/sem_ops/sem_join.py) 都保留每侧一个
Series 的接口与覆盖式选列；回退论文版本不等于补齐多字段，不向原版静默注入字段或改写提示。

评价直接执行原 TOML 的 gold SQL，再调用官方 EcommEvaluator 所用的 `GenericEvaluator`，
Q3／Q5 保留 adjusted Rand index 的分组评分语义，Q1／Q7 保留原 F1；不把分组评分改成品牌名称
精确匹配，也不向上游树写 gold 缓存。评价在内存中执行原 `to_csv(index=False) → read_csv`，
保留 Series 形状和 dtype／NA 推断；例如 Q14 的字符串 ID 会像官方评分输入一样读回整数，原查询输出不变。
本地临时 Parquet 的原 SDK／记录链／prepared 差分覆盖完整消息、普通选择、原投影、乱序答复、
空输入和文件变化；这些是零模型软件检查。真实 E-Commerce 数据、目标 SDK 和模型运行仍待验证，
IMLane Q1 已接同一原版 TINYINT Filter；Q3/Q5 的字符串输出仍未由该接点接线，原版 VARCHAR
限制及 Kalypso 未由此 CLI 接入的边界继续适用。

### E-Commerce 原完整多模态查询

同一[E-Commerce 任务绑定](text/sembench_ecomm.py)也绑定官方 Q2／Q4／Q6／Q8–Q14，使用现有
[LOTUS 视觉服务入口](../experiments/sembench_lotus_query.py)，设置 `scenario=ecomm` 和所选 `query`
并提供作者数据目录。Q12 原函数继续拥有 Parquet 读取、普通选择／合并、文本 Filter、对实际保留行的
图像 Extract，以及最终 JSON Series；没有复制题目执行器，也不通过单算子 prepared 桥执行。
Q2／Q13 的图像 Filter、Q4 的 Extract、Q6 的 Map、Q8 的描述／图像 Join 和 Q9 的图像 self-join
使用相同入口；原选择、重复项、自身排除和投影均留在官方函数中。
原生 LM、HTTP 观测、永久预算、完整 sink 和原 TOML gold SQL／评分共用已有实现。

预检调用原函数到首个语义边界即停止，记录算子及左右输入规模，保留所有发生项（包括重复商品／图像映射），
不模拟中间结果。首个 Join 的下界为真实左右规模之积，其他首算子为其输入规模。
Q12 首阶段 N 项，后续只处理实际保留的 M 项；预算按最多2N预留，实际收费N+M，
不把上界当成真实调用量。运行时重新读取两份 Parquet，真实图像由原 ImageArray 读取／编码；
源码绑定及文件／像素身份校验位于上述服务查询区间外。原题在空结果或缺失 `generated_json` 时的 KeyError
保留，不补造输出列或金标。Q2 的 styles 标签文件仅供评价，不加入查询读取或模型消息。
Q4 重复 ID 导致的原 adjusted Rand index 长度错误同样保留，不去重改写原结果来修评分。
临时数据软件检查与原始数据／真实 VLM 资格分列。

Q10／Q11 的多分支 Filter／Join、Q14 的分组 TopK 也直接运行原函数；真实中间关系决定下游调用量，
预检只提供首个算子的下界，不强制按后续最坏笛卡尔积预留。调用方须给明确的永久尝试额度，
耗尽后停止并保留失败；没有把未知总量记为零或预检样本量。多列 Join 的实际选列限制同上，
不能把完整原函数可运行称为全部声明输入语义已满足。
分组 TopK 使用官方 `settings.configure` 让原生线程池取得当前 LM，查询结束恢复全局设置；
没有串行化原生线程池。并发 LM 调用不能用全局请求增量作逐阶段归因，重叠时该值留空并说明，
完整查询仍核全部原生请求、成功终态和预算。此入口用于单查询隔离进程，不声明同进程多查询并发。
数据准备须使用固定作者生成过程：包含43条指定注入行，`scale_factor > 43`，随机种子12345600；
不能仅截取同样行数来替代作者采样。实际规模以生成结果和输入预检为准，数据缺失不能记为系统不支持。

<a id="sembench-mmqa-aggregation"></a>
### MMQA Q5 原分层聚合

[Q5 适配](text/sembench_mmqa_aggregate.py)保留 `_execute_q5` 的片名筛选、原 `sem_agg` 层次和 `_output` 评价，
[同一原生服务入口](../../scripts/experiments/run_sembench_lotus_query.py)直接使用 LOTUS LM 与外部文本服务。
它不经过仅接单个 F/M/E 的记录入口。原 sf25 的25行中，Q5 选16行，正文12403个 UTF-8 字节；
Q4 的24行不是该查询规模。只有原 SDK 首层恰好生成一个请求时，总量才精确为1；
多首层分块的总量还依赖真实摘要长度，`check` 不把它猜成固定次数或逐行16次。

预检使用本地 tokenizer JSON 和原 LM 计数，记录声明的服务上下文、完整首层消息和计数来源，零模型／零额度消耗；
本地 tokenizer 身份不代替服务端验证。运行使用既有持久预算逐 POST 消费，保留原分层归并、失败尝试和原 JSON 评价。
该路径计时基于已就绪的外部服务，模型启动／退出另由宿主核查，不能与普通文本冷启动成绩混算。
上述服务`source_to_sink_seconds` 从预检、LM／tokenizer 和原函数绑定完成后、
实际原查询读取前到完整聚合结果返回，排除重复源码 Git 校验／模块加载、observer 收尾、评价、
结果编码和持久化；旧 `query_seconds` 保持原边界。原 sf25 保存输入的软件 SDK 回放不证明目标 tokenizer
的首层划分，真实调用上限仍需上述目标预检，不能直接使用软件 fixture 的单请求数。
LOTUS＋Ray也复用同一原函数、`leaf_plan`和本地counter加载：`SemBenchTextQuery`绑定原聚合任务，
`make_query_lm`只接收显式context/tokenizer选项，实际LM批次交给既有Ray query owner，不另建Q5 runner。
原摘要决定后续层次，总次数未知时保持`None`，预检使用原首层推导的minimum；显式engine上限和永久预算封顶。
counter的JSON／SHA、目录和revision只核对声明的Ray模型／tokenizer及context，实际加载仍待目标验证。
单层、多层、长摘要新增归并层和错误actor的软件差分保留原提示、token计数序列、完整结果及评分。
该路径包含原生冷启动和API预算预扣，使用上述独立Ray时钟；服务ready后的LOTUS成绩不能直接与它排位。
其他后端聚合接点及各自验收见[当前系统表](../../INFRA_STATUS.md#各系统-runner-收尾状态)，不借单层成绩授予多层资格。
配置与命令见[脚本说明](../../scripts/README.md#sembench-native-mmqa-aggregate)。

本轮保留的可复现边界：

| 现象与根因 | 处理与验证 |
|---|---|
| Daft source DataFrame 提前选择 Native runner，随后显式线程配置被当成外部覆盖而拒绝，发生在模型启动前。 | 在创建 DataFrame 前调用同一个 Native 配置入口；省略时保持 Daft 默认，已有 runner 仍不被暗中重配。原失败及零模型复现保留，原生图／核不改。 |
| MMQA 原 runner 的 `src.runner` 与 SemLoom 已加载的 `src` 冲突，首轮无法导入。 | 仅在原模块加载期映射同一已验证上游 runner，随后恢复；十六题原查询／方法／accessor 差分及别名恢复检查通过。 |
| MMQA Q2a/Q2b/Q7 的原 `"zs-cot"` 字符串与 LOTUS1.2.4 的枚举不相等，静默生成非 CoT 提示。旧软件回复检查未覆盖策略分支。 | 在独立查询 worker 调用原 Join 的边界转为 `ReasoningStrategy.ZS_COT`，正常及异常返回均恢复 accessor；不改原查询源码、候选对、顺序、Extract 或解析器。回归检查实际原生 API 参数与 HTTP 消息中的完整 CoT 提示，后续 `d061df8c` 实测确认该提示。 |
| Q2b 空 Join 后，LOTUS Extract 仅从实际回复字典创建新列，零行结果没有声明的 `color`，原最终投影报错。 | 旧补列适配已删除，当前保留原KeyError，零行Extract无模型请求；历史空表仅按其当时合同保存，不能当当前原生成功结果。 |
| sf25 Q6 有 NaN，最初的纯字符串接纳误拒绝原 SDK 可序列化输入。 | 在文本 wire 边界保留原 DEFAULT cell 呈现；真实三题各25行完整消息与结果顺序相同，原 CSV 不变。 |
| 非空 Extract 成功后原关系后处理可能因 `{}` 缺列失败，旧的新接线未把已完成算子报告附给该异常。 | 保留原 KeyError 并转交已知调用与关闭证据；不补字段把失败变成成功。 |
| Movie 适配扩展使五份历史投影从维护版读取旧摘要失败。 | 保存单份准确原源并补五条来源／格式映射；原3010条摘要与投影 ID 不变，完整 materialize 通过。 |

<a id="sembench-image-preparation"></a>
### 图像查询准备范围

[Animals 原查询](image/sembench_animals.py)接通 Q1/Q3/Q7/Q10 的原函数、金标 SQL 和原评价；
[MMQA 原视觉查询](image/sembench_mmqa.py)直接调用完整 Q2a/Q2b/Q7，固定 exact policy 和原 `zs-cot`，
不运行构造器、不预设匹配，不在两阶段之间手工去重图片；比较配置显式关闭原生缓存。
原 `zs-cot` 在当前 SDK 接口处显式映射为 `ReasoningStrategy.ZS_COT`，由 LOTUS 生成 zero-shot CoT 提示；
输入报告同时记录上游拼写和原生枚举，避免把静默退回普通提示视为原策略已生效。
[CPU 检查入口](../../scripts/experiments/check_sembench_image_query.py)只读取实际文件／RGB 像素摘要和计算单遍预算，
结果统一为空且标记待验证；它不申请额度、不初始化 framework 或模型，也没有运行 GPU 的选项。
命令见[脚本说明](../../scripts/README.md#sembench-image-preparation)。

| 原查询 | 现成资产与单遍模型请求预算 | 当前接线限制 |
|---|---|---|
| Animals Q1/Q3/Q10 | 官方 sf64 共64图、42,725,857编码字节；各64次 Filter | 原查询／原评价 CPU 接通；原生 SDK 服务入口已接，模型服务待实机验证 |
| Animals Q7 | 同一64图，两遍完整原 Filter，共128次 | 两遍原模型输入都保留，不能替为 CLIP 检索 |
| MMQA Q2a | 原生成切片25图＋13行表，精确 Join325候选对 | 原生 Join／评价 CPU 接通；原生 SDK 服务入口已接，模型服务待实机验证 |
| MMQA Q2b | 同一切片，Join325次，后续 Extract 为实际匹配行数，最多再325次 | CPU完整非空Join→Extract及原空Extract缺列错误已验；d061原失败与离线评价按来源单列，preset单次Extract不构成完整模型资格 |
| MMQA Q7 | sf25 机场25行与原25图，组合后625候选对 | 原源码无自然问题的欧洲预筛，保留并单列该差异；原生服务入口已接，实际服务待绑定／验证 |
| E-Commerce 图像／多模态 | 首算子真实发生项及图像身份按原函数计算；原件待准备 | 原生服务入口已有软件检查，真实资产／VLM待验，不能记零分 |
| Cars Q3／Q8 图像 | 原车辆／图像 CSV 的真实连接规模；原件待定位／准备 | 原函数／服务／评价已接线，真实数据／VLM待验 |
| Medical Q3／Q8／Q9 图像 | 原患者／图像表连接后的一次Filter；Q9每发生项含两图 | 原脚本／服务／评价已接线，软件差分通过；真实图像／renderer／VLM待核 |
| Cars 其他图音 | 固定 LOTUS tree 缺 Q2/Q5/Q6/Q7/Q9 文件 | 官方实现缺口单列，不编造查询，不代表其他系统不支持 |

上述预算不含暖机、重试或重复；Animals 四题与 MMQA Q2a/Q2b 共970到1295次，Q2b 的匹配数由真正的 Join 决定。
Q7 需把已由原 generator 准备的机场表与同一图像切片绑定到一份输入目录，额外625次；
当前两组资产不在同一准备目录，不能把可组合视为已经在服务器运行。
90个现有文件（64 Animals图、25 MMQA图及表文件）的既有只读摘要检查相符；本轮没有重新复制图像或加载权重。
Qwen2-VL-2B-Instruct 的配置、processor、索引及两个权重分片齐全，权重合计4,418,050,768字节；
已有 revision `895c3a49bc3fa70a340399125c650a463535e71c`、BF16 身份不代替新 provider 的实际加载资格。
历史 Animals 的 `max_pixels=1003520` 与 MMQA／原 processor 的 `12845056` 不同，须显式冻结为可比较配置。
Animals Q3/Q10 在现有64图有三个并列最大项；原评价接受其中一个合法第一，不强求某一结果帧或同时输出全部第一。

当前 `serving/backends/vllm_protocol.py` 的记录模型通道只接文本，sync 引擎也按 token-only 渲染；
[原生 LOTUS 视觉运行入口](../../scripts/experiments/run_sembench_lotus_query.py)已将这些原查询直接接到原 LM SDK 的 OpenAI-compatible 服务。
原生 SDK 保留 PNG 消息、并发批调用、`zs-cot` 和 Extract `json_object`，不用 prepared-call seam 或文本 IPC。
本地多模态 IPC owner 仍未接通；本次服务入口的真实模型、processor／generation 与物理服务关闭待机器验证。
原生 LOTUS 也可由原 LM SDK 直接访问独立官方 VLM 兼容服务，复用现有 `native_http_observer.observe_native_httpx`
观察完整原请求及每次尝试；这不是已退役的 SemLoom HTTP 代理，也不需要改变算子。该服务的配置、模型身份与生命周期仍待机器绑定。入口读取已有预算的预留单元，每次真正的 HTTP POST 先消费，
不分配或退回额度；Q2b 按完整 Join＋最多同等数目的 Extract 预留。原失败与已发送单元不得重置。
服务启动和 GPU 归外部 host owner，入口仅监督查询子进程；该计时为已有服务上的完整查询，不能混入文本冷模型启动成绩。
私有审计保留原 POST 的内容摘要和 PNG URI 摘要，避免在事件队列复制整幅 base64 图像；原请求字节不变。
LOTUS服务仅保留已读HTTP响应bytes原引用、状态和真实时间，额外响应JSON解析／审计编码在完整sink后或失败finally执行；
不会缓存请求／图片全文，也不替换SDK响应。`http_response`行可能后于`http_finished`追加，关联使用attempt／ID和事件本身时间。
默认Daft等HTTP调用保持原行为；请求指纹、图片审计及请求／结束事件仍计入查询，
见[后置范围与软件检查](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#lotus-response-audit-after-sink)。
共享服务观察还保留原 LOTUS `_process_uncached_messages` 按输入顺序返回的 completion ID，
在完整查询之后用唯一ID关联HTTP尝试，写入 `native_sdk_response` 的LM调用号／发生项位置／请求号。
相同输入可以有不同答复，不按消息摘要或HTTP完成顺序猜对应；缺失／重复ID记录为不可用。
查询报告的 `native_sdk_response_audit` 披露覆盖数与原因；旧无序日志不能事后补成逐发生项全验。
原SDK函数、请求、参数和调度保持，关联及写盘不进入sink区间；观察器退出后恢复原方法。
生成配置未给出的值按原 SemBench 8192 token／无 stop 解析，显式值保持，不继承短文本 Filter 的8 token／换行截断。
[Ray Animals Filter](image/ray_animals_q7/README.md)复用原生 Ray LLM processor、worker、预算和收尾，
按原任务元数据执行 Q1/Q3/Q10 的一遍 Filter 或 Q7 的两遍 Filter及原 City交集。
保留历史 module 路径；`RUN-BOUND.json` 的 `query` 默认为7，单遍预算为N，双遍为2N。
新增单遍路径已有 N=1/7/13 的完整原查询／消息／评价及生命周期 CPU 检查，实际 SDK／GPU 尚待验证；
下述64图资格只属于各自冻结的 Q7 来源。Q7 顺序执行两遍64条 Filter和原 City交集；
两次各自启动/关闭，整体共享128次额度，不能标为同模型 warm。默认示例使用 Ray2.56.1／vLLM0.22.0；
原128条模型资格与当前清理接口的Linux CPU资格分列，其他环境和完整公平计时另验。
当前 Q7 保留第一 stage 的物理退出及清理，在最后 stage 预测原子发布后执行原 City 交集；
完整内存 sink 就绪后 ACK，再完成最后 runtime 的公开关闭和宿主清理。第一 stage 的释放仍属于
该双 worker profile 的必要流程。19 项 CPU 及 128 条保存答复身份／parser 回放通过，完整原 City／
evaluator 和目标 Ray／GPU 新终点后来已由 `909f842c` 完成独立资格；后续失败诊断/退出码
改动与本组合树仍只按各自 CPU 证据解释，不从公共 runtime 存活推断内部模型 actor 驻留。
旧结果包含最后关闭，不能减去估计清理时间回填新口径。

Ray／Daft 的 CLIP 图仍是独立嵌入路径；Q7 的资格不扩大为其他 VLM Join／Extract 支持。
视觉预检分别报告Animals的Ray VLM Filter、MMQA Q2a／Q7的显式Ray图像Join及其余未接线范围；
预检不授予模型资格，Q2b后续Extract不由单Join扩展取得支持。
已安装Daft0.7.21源码中，当前`vllm-prefix-caching`接点使用单个文本表达式，`prompt()`明确拒绝
多消息列表、system_message及return_format；现用prepared路径已将原消息渲染为完整文本，不受此列表限制。
原Extract的JSON约束继续经sampling_params的structured_outputs传递，不依赖return_format。
该SDK的OpenAI provider另支持图像bytes／File等原生输入。这不等于当前已接视觉任务，也不能把
某个provider的接口限制扩成整个Daft不支持视觉；选择其他provider还须验证原消息／PNG、重试／预算、发生项关联和完整生命周期。
LOTUS＋IMLane的同一[完整原题profile](../../integrations/imlane_duckdb/README.md#同一-runner-的完整-lotus-原题)已接视觉消息和原生multimodal载体，
单图／双图、Join后Extract及实际survivors有软件差分，真实Linux SQL／VLM另验；Kalypso当前文本前端尚未接视觉载体。
这些接点范围不代表整个上游系统的视觉能力边界。
原 LOTUS 明确不支持的音频题留空，其余未验证能力另记。

CPU 视觉夹具使用真实 ImageArray、原 PNG 格式化、原 Filter／Join／Extract 解析与关系结果。
MMQA 同一图片的多个匹配行仍逐个 Extract，JSON response_format 保留原值。
非空第二阶段的独立准备工具为
[`tests.baselines.image.prepare_mmqa_extract_stage`](../../tests/baselines/image/prepare_mmqa_extract_stage.py)，
参数为`--sembench-checkout`、`--data-dir`和`--output-dir`。它只用官方正例作为明确软件Join夹具，
让原查询／SDK生成Extract消息即停止，0模型；不返回完整查询或评分，不替换真实Join结果。
消息仅写私有artifact，缺少正例时拒绝。后续独立Extract模型诊断不授予Q2b端到端资格。
Q2b无匹配时，原零行Extract不创建`color`，最终投影保留原KeyError，额外POST为0；
非空Extract返回`{}`缺列也保留原错误。不再包装Extract补列，原Join策略适配正常及异常均恢复。
[86项原生边界检查与真实输入计划](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#mmqa-native-empty-and-real-gates)分开记录软件、模型质量和数据限制。
`d061df8c` 实测失败的保存记录为325个 Join 成功响应、Extract 输入0行／POST 0次；
据此重建带 `ID/image_id/color` schema 的空表，原评价器离线得到 precision／recall／F1 均为0。
原失败记录及46.740秒失败耗时保留，离线评价不补记正常 API 返回或成功端到端计时。
这些软件回复检查与离线恢复不授予修复版本新的 VLM 质量或性能资格。

<a id="lotus-movie-exact-join"></a>
### Movie Q5/Q6/Q7 原生 exact Join

同一文本 CLI 显式选择 `backend=lotus-native`、`lifecycle=resident`、`scenario=movie` 和 `query=5/6/7`，
调用[一条共享 Join 接点](text/frameworks/lotus_native_join.py)。原 `sem_join` 拥有全部有序对、formatter、
LM/LiteLLM batching、Boolean parser 和关系构造；现有 owner/预算/监督继续复用，默认 F/M/E、TopK 不变。
pair 按左右 source position 关联，原 SDK 有序返回绑定相同消息的不同 occurrence，不能去重。
完整原查询返回后才转移逐对 receipt 报告，再做原 Join/最终 sink 重放、评价与持久化。

#### Ray／Daft 复用原 exact Join

Ray／Daft维护入口均使用[完整原题LM边界](text/sembench_prepared.py)：原`sem_join`实际产生完整消息，
Ray原生processor消费消息、Daft原生AI消费声明tokenizer的完整chat template，各自拥有batching／调度。
原SDK继续拥有Boolean parser、Join关系构造和原查询后处理；自身排除、对称项、重复值及LIMIT不预裁剪。
适用于Movie Q5/Q6/Q7和E-Commerce Q7，不另建逐题执行器。多列指令仍受原SDK选列限制，
实际入模字段以保存的原LM消息为准。原[typed Join准备与完成](text/sembench_prepared_join.py)仍供
IMLane prepared接点及冻结答复身份回放使用，不能删除仍有实际调用的合同。

Ray通过已有`ray_native`配置选择该能力；Daft使用`semloom.native_daft_sembench.v1`入口，
普通记录图不借用其资格。完整比较预算保留L×R，失败／缺失／重复答复不补默认值或重新请求。
source→sink包含原提示、原生后端和原SDK解析／关系，额外消息核对及driver持久化在sink后，
每条路径内部observer成本及冷暖身份按各自独立时钟记录，不能称相同成本。
原题／SDK、软件图、跨进程packet与保存答复差分范围见[分路径报告](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#aggregate-system-runners)；
真实模型和物理清理仍须按准确来源验收。

IMLane 复用同一原生 Join 接点，以独立 `text_join` schema 把完整 pair 发生项交原版 TINYINT
prediction，实际成员关系交原 SDK／原题恢复完整 sink，之后审计原始答复、评价并关闭。
原版 SQL、DSO、kernel、模型 owner 与 ACK 不变；这是 LOTUS Join 的执行接入，不是作者 Join 算法复现。
配置及零候选接线限制见[IMLane 接点](../../integrations/imlane_duckdb/README.md#lotus-exact-join-与原版-imlane-prediction)。
软件 Arrow／原 SDK 检查不授予该组合 Linux SQL／模型资格；旧 Filter 配置不会自动取得 Join 支持。

共享准备层不再把自 Join 或 `reviewText` 写成后端能力限制：普通 exact inner Join
按原指令绑定各自左右表及显式字段，完整预算为 L×R，并在执行前核对右表未变；
quick TopK 使用原指令引用列，保持原 K=N、pivot 和比较算法。
`4e1f8a44` 的目标 26 项零模型检查通过，包括矩形 Join、重复 occurrence、完整关系及原任务回归。
这不开放新的任务题号、Join 策略或任意 DAG，也不继承旧题的 GPU／性能资格；
同轮原件及归档失败恢复见[记录](image/ray_animals_q7/README.md#evidence-and-timing)。

首份 [CPU 资格](../../../experiments/results/diagnostics/lotus_movie_join_native_20261111/README.md)
只用原 Ant-Man 顺序前 8 条：每题 64 对，Q5/Q6/Q7 分别完成软件 IPC 回放。
配置复用 [resident 模板](../../configs/sembench_lotus_native.example.json)，单冷设置 `resident.repetitions=1`，
输入声明改为该份 `Reviews.csv`，原生模型与永久预算分别绑定每题 64 次；CPU 命令见[脚本说明](../../scripts/README.md#additional-sembench-text-entry)。
这些是合成答复，不是新模型质量、性能或目标物理清理资格。

完整 Ant-Man 256 条的每题是 65,536 个候选对，模型之后才剔除相同 reviewId 的 512 对；
Q5/Q6 的 `head(10)` 也在完整 Join 之后，不能预裁剪或按 10 次调用申请预算。
原 LOTUS 1.2.4 会先 materialize 全部 pair prompt。完整输入的主机内存、期限和目标模型资格仍待核；
三题完整单遍上限 196,608 次没有因此释放。

### 后续 TopK 与缺失数据

Q9 原 TopK 仍须显式选择 `ranking=topk`，不能借用本次 Map 的256次资格。
其 `quick` 路径随机选 pivot；完整 N=256 排序的候选比较数上界32640，精确次数依赖 pivot 和模型判断，
不能把 SDK safe-mode 估算当实际消耗或额度上界。LM seed 不设置 NumPy pivot RNG。
MMQA Q5 使用[独立原生聚合入口](#sembench-mmqa-aggregation)；现有单 F/M/E 链不接聚合，不能按输入行数猜次数。
E-Commerce Q1/Q3/Q5 和 Cars Q1/Q4/Q10 的原文本任务已接线，目前没有已核实的目标成套输入。
Medical 已核实上游自带的完整文本资产，按上方原文本入口直接复制；Cars／Medical 的普通准备会顺带下载图音素材，
文本准备不可盲跑整套 setup。需要其他规模时选择现成原 CSV 或原采样方法，不另写采样。
E-Commerce 的嵌套 `productDescriptors` 保留原 DEFAULT 的整对象字符串，不能抽取一个 description 字段代替；
共享文本绑定现支持 keyword Extract 和 Map suffix：E-Commerce Q3／Cars Q10 的
`input_cols/output_cols` 与 E-Commerce Q5 的 `suffix` 不再需要专门执行接点，
它们仍需绑定各场景的真实输入、原查询及评价，不能据此宣布这些题已取得完整运行资格。
这些属于待数据／目标验证，不是系统缺少原算法。其他原实现按各自文件与能力准备；
此前限定目录中未找到的数据只记待定位，不推断全服务器没有；下载遵守官方来源与资源计划，不能改写算子凑结果。

## SemBench 原项目与本地接入职责

SemBench 论文评测 LOTUS、Palimpzest、ThalamusDB 和 BigQuery；仓库另有 CAESURA、FlockMTL
接入，不能把源码中存在的入口都当成论文已报告的系统。论文主实验使用远程 Gemini 2.5 Flash，
不是本项目的本地 vLLM／GPU 实验；系统版本、模型、策略和启动计时分别保留实际身份。
来源见[论文 §5.1–5.2](https://arxiv.org/html/2511.01716v2#S5)。

固定 `c814e380` 的[公共 runner](https://github.com/SemBench/SemBench/blob/c814e3807e72d4cf876b852b17e77f3cc94575c2/src/runner/generic_runner.py)
共用查询发现、运行、结果和指标接口；系统 runner 负责原生调用及系统配置。
LOTUS／Palimpzest 的逐题方法或脚本定义查询，ThalamusDB／BigQuery 主要执行对应 SQL。
逐题保留选择、指令、关系后处理和评价是任务定义，不应为每题复制模型提交、预算、监督或关闭实现。
多算子查询由原查询顺序及后端能力处理，不能凭题号或固定行数决定通用执行行为。

本地 Ray／Daft 的 prepared 路径共享 LOTUS 方法，后端拥有各自原生执行图；IMLane／Kalypso
按作者接点分别核对。它们不是上述论文四系统的完整复现，也不强行共用系统提示模板。
固定官方 `GenericLotusRunner` 的默认 `policy` 是 `approximate`；本项目 Movie 调用原题时
显式设为 `exact`，两者必须分别命名。论文系统设置描述了优化 Join，但各题实际 policy
本轮未逐项核实，不据此推断所有论文题或官方默认都走 exact。
原远程 Gemini 的并发与输出长度是来源事实，不是本地模型的推荐容量；本地仍需显式绑定实际参数，
校准和请求上限按具体机器、模型、服务及 workload 核对。
文本入口现按捕获到的算子检查能力：普通 Filter／Map／Extract 使用既有接点，Join 使用显式
LOTUS resident或Ray原生processor，TopK可走原生LOTUS resident或上述Ray完整查询；独立Daft入口也接exact Join。
prepared 请求入口拒绝其余算子，不在缺少 stage 时以属性异常退出。
实际接线继续按后端能力区分；Ray完整查询现已接真实后续关系和MMQA原分层聚合，其他后端不能据此自动取得资格。
同一普通算子的 positional／keyword 参数由一个共享绑定处理，Map 的命名输出列按原 `suffix`
写入；普通 Extract 保留声明字段及原 JSON 解析，quotes/raw 等未支持的选项仍明确拒绝。

<a id="sembench-native-query-timing"></a>
### 完整查询时钟与仍未统一的路径

各路径同时保留worker进入、预检结束和sink的实测时间戳。部分LOTUS／旧Ray Animals／Daft／IMLane Filter及历史Kalypso路径
仍按原来源保留`sembench.source_to_sink.v3`字段；它只说明该版本的外层事件，不证明内部审计I/O全已排除。
完整LOTUS＋Ray当前使用独立`lotus_ray.source_to_sink.v1`，LOTUS服务使用独立`lotus_service.source_to_sink.v1`，
原生IMLane Q3使用独立v2，当前Kalypso五handler及Movie records/prefix使用独立`kalypso.source_to_sink.v1`并各自披露残余；
字段或版本相同均不自动授予跨系统可比性。

<a id="native-runner-clock-boundaries"></a>
当前源码的完整查询起止如下；LOTUS算法、独立JSON工程方法和作者原生系统保持各自身份。
共同起点是真实源读取，终点是原查询或任务完成内存结果；真实准备和关系后处理留在区间内，最终评分／归档在外，必要packet／READY交接仍在内。

| 执行profile | 查询区间内 | 启动／结束与仍待验证的条件 |
|---|---|---|
| LOTUS resident | fresh source→原accessor／LM／parser→完整原题结果 | 默认首次惰性模型加载包含；显式before_queries准备另计，按实际owner观测区分冷暖；统计／写盘／最终close在外 |
| LOTUS外部服务 | source／实际读图及PNG准备→完整原题结果；真实HTTP等待、POST预算、请求／图像指纹和请求／结束事件仍在内 | 额外响应解析／编码、关联／评分／flush在sink后；后台写盘仍可重叠，服务启动／READY由外部owner另证 |
| LOTUS＋Ray offline | fresh source→原SDK语义及实际多批native graph→完整原函数结果；API额度与惰性runtime／模型启动在内 | 原函数返回后audit／ray.shutdown；逐请求成功旁录已删，不声称跨批模型复用，新候选目标运行待验 |
| LOTUS＋Ray Serve | fresh source→原LOTUS实际消息／算法／多批native Serve→完整原函数结果；API预算与批次IPC在内 | 模型READY及启动在前，后置审计／评价／写盘和最后关闭在后；groupby批次顺序交接；43e Q8与8b73 Q4同owner执行已验；Q4外层SHM失败／恢复保留，当前Serve关停修复的物理效果待验 |
| 独立Ray offline／Serve | fresh source→工程消息／实际图像准备→真实依赖批次及完整任务结果；额度与必要IPC在内 | offline包含惰性启动；Serve deployment启动另计，源前／sink后观测真实模型身份，响应关联和最终close在外 |
| LOTUS＋Daft／独立Daft local | fresh source→提示／packet与摘要→各批原生图→完整原题或工程任务结果；完整READY交接及中间批ACK／退出在内 | 各批模型冷启动仍在内；末批完整sink后ACK／close。原生后台错误可能只到外层timeout，失败日志和READY须保留 |
| Daft attached的两种语义入口 | fresh source／消息和真实PNG编码→builtin OpenAI graph→完整结果；packet／HTTP／完整READY与中间批交接在内 | 外部模型启动另证；末批sink后ACK／client退出。完整READY的API快照复用，原始HTTP审计仍在区间，不能借用local或其他语义的暖轮 |
| LOTUS＋IMLane完整原题 | fresh source→必要packet／SQL／UDF／IPC及实际后续关系→完整原函数结果；SQL scope／ACK及provider回执在内 | 模型READY在前；评价／最终模型关闭在外；provider同步审计仍有区间内成本；默认一查询；显式model-session同模型两查询已接，逐轮source→sink与包含前轮审计的会话总区间分开，实际SQL／模型资格待验 |
| IMLane独立Q3 | fresh source及必要输入／packet摘要→作者SQL／UDF／IPC→C++完整COUNT内存结果 | 模型和作者coordinator TCP ready在前；driver中的DuckDB构造／DBEnd Setup仍在内。Python报告读回在后；真实worker-entry与query-window区间分列 |
| Kalypso五handler／Movie records-prefix | fresh source及作者必需输入→原生执行／默认策略／完整关系后处理→完整结果；uniform回执在内 | 规划／预检在前，评分／写盘在后；外部启动和模型READY需实际owner证据；ratio旁录副本已删，图像carrier仍无原生支持 |

Daft失败报告引用原有`supervision/worker.log`及已保存`result-ready.json`，不以文件存在证明按时sink、ACK或关闭。
原生失败仅写既有stderr的脱敏短原因和异常链，成功不增加写盘；`add_request`collector包装及误导的`accepted`汇总已删除。
该快照在callback／ACK／关闭之前已持久化，是当前失败交接所需成本；不为移出计时另建补证据阶段，
见[源码依据与验证](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#daft-native-failure-handoff)。
上表闭合实际起止和成本归属，仍不授予不同冷暖、诊断成本或语义profile的正式横向排名。

<a id="native-query-comparison-scopes"></a>
#### 配置能回答的比较问题

下表是源码支持的比较范围，实际资格仍按固定版本回执核对。同算法执行比较须绑定同题／输入／完整sink、
模型和tokenizer身份、生成／缓存／资源条件及实际启动归属；不同原生算法保留各自策略，另报质量和完整系统代价。

| 比较范围 | 已有配置 | 边界与真实缺口 |
|---|---|---|
| 含模型初始化的source→sink | LOTUS resident的`inside_first_query`首次非空查询；LOTUS＋Ray offline；Daft local | 各自真实native初始化留在查询。Ray每次LM预测新建processor、Daft每批新worker，重复轮次或同runtime不证明同模型warm；比较完整物理路径，不能把多实例成本称为单纯传输差异 |
| 模型已就绪后的首次完整查询 | resident的`before_queries`；有外部READY证据的LOTUS服务／Daft attached；明确先READY的IMLane／Kalypso；LOTUS＋Ray及独立Ray的`serve-session` | 仅双方实际支持的相同任务／语义可比较；resident当前配置不能补称视觉／聚合ready路径。Q3先起coordinator，LOTUS＋IMLane原题逐批起scope，初始化归属仍不同，不能只凭ready字段混排 |
| 同模型owner重复查询 | 已验身份的resident；LOTUS＋Ray／独立Ray的`serve-session`；独立Daft `attached-session`及有相应外部证据的原题查询；LOTUS＋IMLane的`model-session`已接、目标待验 | 必须真实执行非空请求，并核新鲜进程／birth／constructor与owner不变。序列cold/warm标签、端点相同、零请求、不同题顺序执行均不授予该题warm资格；prefix cache开启也不证明实际命中 |
| 包含启动的生命周期总时长 | 各自真实worker／caller／服务启动事件→同一完整sink | 这些起点不同，不能跨字段直接排位。Serve deployment启动嵌在caller启动内，不能相加；外部服务attach不是模型启动。Q3真实worker入口不含外层CLI／模块导入，旧同名字段实际起于QueryWindow，保留旧值 |

HTTP额外canonical JSON重编码／SHA已删除；真实payload字节SHA仍同时供预算与事件，
LOTUS记录的审计body可在查询后核对，不能把byte SHA换名成旧values SHA。Daft attached只存messages SHA／model等记录，
不据此声称保存了完整body或可重建旧canonical字段。实际SDK发送、参数、body读取和响应关联保持。
尚在查询内的HTTP解析／图像指纹／事件编码、IMLane provider回执和Kalypso uniform记录仍逐项披露；
其中有现用审计／失败消费者，不能以删记录或扣估算时间取代可靠留存。必要数据、packet、IPC、阶段间关闭不移出查询。
LOTUS＋Ray的同算法ready／warm配置已实现，43e原Q8及8b73原Q4两轮已取得真实模型执行回执；Q4质量／清理限制保留，其他任务另验，独立JSON Serve不替代其语义身份。
其余实际模型／缓存／启动条件及真实资产未绑定项仍按所属覆盖表留空，不由本次软件检查授予性能资格。

<a id="native-cache-and-request-order"></a>
#### 缓存与请求顺序

比较记录同时保留prefix cache请求配置、实际可确认的启用状态、命中证据和结果缓存设置；
省略配置只表示使用原生默认，不能填成False。没有实际状态／命中计数时分别记unknown／unavailable，
不把“同一模型”“cache开”或两轮原文相同解释成已命中。按固定运行回执核值，不把下表当全局统一开关。

| 系统／profile | prefix cache配置与状态来源 | 结果缓存与请求顺序 |
|---|---|---|
| LOTUS resident／外部服务 | 本地engine声明及实际owner配置；外部服务由其启动配置／日志另证，缺观察留空 | LOTUS原题profile关闭SDK结果cache；保存原source及LM批次顺序，SDK／服务可原生重排 |
| LOTUS＋Ray offline／Serve | engine_kwargs与实际原生配置／启动日志分列。43e Q8与8b73 Q4均请求并确认开启，命中量未知 | 两题结果cache均关闭；ready-first再same-owner-repeat，每轮一批、逻辑选中顺序分别row0..119／0..23，轮间call_id不同。实际engine接受顺序未观测 |
| 独立Ray JSON | 各自offline／Serve的engine_kwargs及owner记录，不能借LOTUS那轮状态 | 项目任务无结果cache；保留完整依赖批次、call_id和已观测API关联，原生调度顺序未观测时留空 |
| LOTUS＋Daft local／attached | 内置provider／engine配置与实际worker状态；attached的远端模型另证，不能借local provider名称授予状态 | LOTUS SDK结果cache关闭；每批新client／worker，记录批次及原生行关联，HTTP相同消息不强配唯一发生项或顺序 |
| 独立Daft JSON | 同样分清local provider配置、worker状态和attached远端服务状态 | 项目任务无结果cache；实际依赖批次／顺序与远端模型owner分开，每批client更换不等于模型更换 |
| IMLane原SQL／LOTUS工程profile | ModelService配置与模型ready日志；未观测的原生默认不补False | LOTUS完整原题关闭SDK结果cache，原SQL专属配置按各回执。保存Arrow／call_id／SQL批次与provider序列；model-session两轮用不同query_id、共用连续attempt。接受顺序不能由最终COUNT推断 |
| Kalypso | loaded-model的native_engine_configuration.cache及作者引擎声明，实际worker未采到则留空 | 适配层不加结果cache，作者机制保持；按各回执记录设置。保留source idx、native API sequence与原server调度，不能用源顺序替代已发生的原生顺序 |

43e Q8与8b73 Q4的两轮时差只作各运行观察，不归因prefix cache；原件已有信息足够更新上述状态，不新增缓存实验。

共享文本任务的 `bind_execution()` 在计时前核输入身份，执行时真实读取原 CSV／Parquet 并运行原题，
sink 后再核文件摘要；独立 `execute()` 仍保留组合校验入口。LOTUS 视觉和 MMQA 聚合也在
本地客户端、输入／叶级预检及原函数绑定后开始，不将 Git 校验和模块加载混入完整查询。
Animals 像素身份审计在区间两侧，原生 ImageArray 的真实图像读取／解码仍在区间内；
MMQA 保留真实 Join 结果及其后续 Extract，聚合保留实际摘要长度决定的原分层归并。
Ray Animals四题也在manifest／像素及源码预检、原函数绑定和LOTUS客户端初始化后开始真实读取，
原生模型启动、一遍或两遍图像Filter、必要阶段间清理及完整原sink保留；原入口至sink宽时钟另存，
新边界本地软件已验、目标验收待进行，旧模型时长不回填。
软件 fixture 不再套用64条上限；具名官方 sf64 输入仍必须匹配其64条生成身份。
Ray／Daft prepared 路径在配置／输入核对和预算准备后开始，包含必要的消息准备、原生执行和
原查询的真实重读／后处理。禁止从历史宽区间估算扣除预检时间；旧 JSON 按原口径保留。

文本`check.query_clock_version`只声明执行接点的边界：原生Ray用`lotus_ray.source_to_sink.v1`，
惰性runtime／模型启动计入、最终关闭在sink后；LOTUS resident用v3，模型是否已就绪按具体phase观测。
旧isolated无独立关闭前边界，保持None，其query_seconds含adapter审计／关闭；预检不产实测成绩，
见[原题完整Q3与复现核对](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#query-clock-preflight-and-repro-freeze)。

Ray离线／Serve直接使用原生API完成／错误约定，非末批只核本批额度差，不再旁录成功事件或读回其文件。
Daft worker不再重复同步写optional事件sidecar，原生执行记录与必要结果／ACK保持。
LOTUS＋Ray及独立Ray离线入口也不再逐请求复制／落盘完整token与output，返回predictions保持；
报告`native_output_artifacts=False`。Serve的完整响应ID关联仍在sink后核对，来源明确为非流式API答复，
不是独立engine-output证据；旧终态事件方案及冻结原件按历史版本保留，
当前接口／软件范围见[原生完成契约](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#ray-native-completion-owner)。
这些实际开销修正不等于所有时钟已对齐，见[验证与剩余边界](../../../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-query-audit-work)。

这些版本化起点不等于所有运行配置已经可直接排名：Ray／Daft 单冷和默认 LOTUS resident 的首次惰性
加载仍在区间内；显式 LOTUS `before_queries`、IMLane Filter 和 Kalypso 使用已就绪模型，
另记实际 ready 观测。模型启动条件必须匹配并独立记录，不能借同一字段名
把不同 profile 混算。旧 `query_seconds`／外层生命周期区间仍可能包含预检、审计和关闭。
IMLane Filter 现在把源码／输入、tokenizer 与审计 token、预算和实际上下文检查放在起点之前，
执行消息和必需 CSV、原生 SQL、原查询实际扫描及完整 sink 留在区间内；worker 进入到 sink
另计预检和模型启动。Kalypso 维护版 Filter 把 manifest／来源核对前置，区间内新鲜扫描原
Reviews CSV，并生成作者必需输入；Map 也在 `make_query` 前置后开始原查询实际读取。
Kalypso 由 receipt 的 `source_started_ns` 和显式版本计算，外层 caller→sink 另存
`caller_to_memory_sink_seconds`；旧 receipt 保留旧值，不从宽区间估算扣除成本。
旧 v2 共享文本路径仍含执行内文件摘要核对，旧 LOTUS 首次查询还含模型启动；原值保留，不能回填为 v3。
IMLane原版DOUBLE Q9 Map也已区分ready后的v3源→完整sink与原含启动宽区间，
输入诊断快照及身份检查移出时钟，全局ready在审计前发布；034dc的原版256项软件SQL门通过，
新DOUBLE时钟模型资格仍待验。该来源VARCHAR Map／Extract另完成144次真实请求，原质量单列。
其余IMLane专用入口、Kalypso未迁移私有查询和旧时钟仍按实际来源解释。
默认 isolated 记录图继续明确留空统一 sink 指标；多算子、Join、TopK 支持以实际后端接点核对，
不以时钟字段、软件夹具或逐题计划代替执行能力。

## 已退役和暂停

旧矩阵专用的 `common/gate.py` 及其四项专测已退役；现用入口没有调用该实现。
原文件和测试的[准确恢复来源](../../../experiments/reproduction/native_baselines/README.md#retired-profiler-csv-and-matrix-checks)保留。
当前原生对照的来源记录、请求身份及结果汇总／校验继续由公共模块维护。

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
