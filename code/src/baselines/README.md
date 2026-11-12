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
- [Daft 原生 AI](text/frameworks/daft_sembench_native.py)的 `execute_q3(rows, model=..., tokenizer=..., sampling_params=...)`
  使用原生 `prompt(vllm-prefix-caching)`、一次 collect及原生筛选／计数；tokenizer由调用方提供匹配的本地资产。
  native runner不重设线程；只接受确实生效的prefix／buffer／morsel选项，拒绝仅distributed生效的actor参数。
  首测返回120条但进程收尾失败，原质量和失败保留在[诊断报告](../../../experiments/results/diagnostics/native_sembench_q3_20261106/README.md)。
  独立进程用[公共工厂／shutdown作用域](text/frameworks/daft_native_owner.py)关闭实际创建引擎；
  `on_result_ready`在关闭前发布结果，异常保留已物化预测。67a4已一次120条正常收尾，质量七个FN仍保留。
- [Ray Data LLM](text/frameworks/ray_data_sembench_native.py)的 `execute_q3(rows, model=..., sampling_params=...)`
  使用公开processor和原生异步vLLM，完整cohort先做Dataset.filter，一次materialize后审计和原生count。
  处理器默认、原生重试、实际可得usage与runtime归属分开记录；现有0.25.1守卫CPU API通过，
  Ray2.56.1发布extra仍要求0.22.0；67a4候选已实际执行120条，SHM残留使完整生命周期失败，物理attempt总数未证实。
- [Ray公开HTTP profile](text/frameworks/ray_data_http_sembench_native.py)保留原HTTP处理器默认与Q3语义，
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
### Daft 原生 AI 的完整文本查询入口

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
支持原单 F/M/E 工作负载入口的 Movie Q1/Q2/Q3/Q4/Q8/Q9-map/Q10、MMQA Q1/Q3a–g/Q4/Q6a–c、
Medical Q1/Q4/Q10；这是参数化接线范围，**不是22题均已完成当前资格**。
Extract 的 JSON-object 由原任务决定，普通 Filter/Map 不强加 JSON 约束。
输入完整关系超出 `max_model_calls` 时在模型启动前拒绝；已有 SQLite 预算在每次原生
`generate` 前扣除，失败不退还，耗尽后不委托第 N+1 次。没有额外预热或整轮自动重试。

原始预测发布后，监督器立即调用原 parser/完整查询 sink；driver 不先等待 worker 关闭。
`source_start_ns → sink_ready_ns` 含 fresh CSV、prepare、模型启动、原生执行及完整原查询，
实际 IPC/polling 包含在内；原 sink 后才记录持久化、评价，模型 close/进程退出另列，
close 可能与 driver 后处理重叠。严格 host controller 入口仍未知，不能把该区间追认为
包含 host 导入、预检和租约的统一 cold 总时钟。native generate/add/final 为 API 证据，
引擎首次接受和 kernel 时间没有独立观测。此入口只声明单冷，不伪造同 owner 暖轮。

本地实际 Daft0.7.21 CPU 表达式/执行器、Q3a/Q4保存答复全关系回放、预算及清理结果见
[资格报告](../../../experiments/results/diagnostics/daft_native_semantics_20261110/README.md)。
`ffbc352ac` 的 Q3a25/Q4 24 已完成目标原生参数、真实模型和物理清理资格；后续已把消息摘要及
关系序列化移至 sink 后，旧49条时长不追认为该新边界。`48a6b42d0` 的 Movie Q1/Q2/Q3 已分别
完成 2000/120/120 次真实单冷、完整原 sink 和逐条审计，零 unknown、零重试，物理清理完成。
这些成绩绑定原提交；当前整合仅重跑受影响 CPU 回归。其余题按各自冻结回执判断，同 owner 暖轮
仍未取得资格。保留每个系统自己的依赖版本，不以迁移到统一 vLLM 为前提。

<a id="ray-native-prepared-sembench"></a>
### Ray 原生完整查询的内存 sink

现有 `run_sembench_text_query.py` 使用
[显式 `ray_native` 配置](../../configs/sembench_ray_native.example.json)时，调用
[原生 processor 接点](text/frameworks/ray_data_sembench_native.py)的 `execute_prepared`。
Ray Data LLM processor 拥有图、chat template、tokenization、batching、模型 actor 和调度；
[准备及完成接口](text/sembench_prepared.py)复用原 LOTUS 消息/parser 与固定 SemBench 原查询。
不调用 SemLoom Core、原自写记录链或 HTTP 替代模型。

[查询编排](../experiments/native_ray_sembench.py)重新读取已核输入，按 occurrence `call_id` 接收完整预测。
`materialize().take_all()` 后，在 `ray.shutdown()` 前执行原 parser 和完整关系查询；
原查询返回的内存结果立即标记 `sink_ready_ns`。`source_to_sink_seconds` 包含本轮新鲜 source/prepare、
Ray/模型启动及原关系计算；消息摘要、关系序列化、评价、持久化和关闭在 sink 后另记事件。
`query_seconds` 仍是包含审计和 runtime close 的较宽区间，不能代替 sink 指标。
最早 host/controller 入口未在此 worker 观察，保持空值；AsyncLLM 首提交、末确认终态与构造返回事件
保存在 `native-events-*.jsonl` 并在查询报告汇总，不能称为引擎接受或 GPU kernel 时间。

`profile` 是本地独占 `ray.init` 参数；`ray_native.processor_options` 保留未覆盖的原生默认。
引擎显式设置由 `engine` 与 `ray_native.engine_kwargs` 一致组合，冲突拒绝。
模型 actor 的 `py_executable` 使用声明的原生环境；不套用项目模型桥的固定 vLLM 版本。
Ray 2.56.1 / 官方 vLLM 0.22 可作为目标组合，实际版本由 actor 回执核验。
actor setup hook 复用已核 Q7 的版本相关接点；SDK、跨解释器加载和 tokenizer 资格绑定实际运行版本。

调用方先预留永久 SQLite 的单次单位，worker 领取后由原生 `AsyncLLM.generate` 前的
[预算观察器](text/frameworks/ray_native_observer.py)逐次扣除；相同提示不去重，失败不返还。
模型 stage 明确设置 `max_restarts=0` / `max_task_retries=0`，不重跑整查询。
单位计数和原生终态不完整时不能报成功。原 parser、后置审计、close 或持久化失败均保留失败状态
及已有答复；已观察到 sink 不会因此擦除。外层继续使用既有 host 租约、资源监督和物理清理，
`ray.shutdown` 的 driver 断开本身不证明模型进程、SHM 或 GPU 已释放。
`startup_timeout_s + query_timeout_s + 30` 是既有监督器的 worker 总窗口，包含其退出预留；
不是独立 host 全流程期限。此入口只实现单冷，不声称同 owner 暖轮。

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
该入口及 `execute_q3` 各有现行调用者，本次不删除。完成目标资格及调用方配置迁移后，
再退出被替代的完整文本记录图；不删除原始成功/失败证据或活跃公共清理 helper。

<a id="lotus-native-resident-sembench"></a>
### 原生 LOTUS 的显式 resident 文本查询

现有 `run_sembench_text_query.py` 默认仍执行 isolated 单查询。选择
[resident 配置](../../configs/sembench_lotus_native.example.json)后，同一原生 LOTUS owner 执行冷查询和
指定数量的新鲜暖查询；每轮重新读取 source/prepare，经原 accessor、LM、LiteLLM 和本地原生模型 IPC
到完整原 sink。`native_lm_max_batch_size=64` 是显式实验 profile，不是作者默认；SemLoom Core 不参与调度。

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
每轮预算为 `N*(N-1)/2` 上界，`engine.max_model_calls` 和永久总预算为该上界乘轮数。
8 条样本最多 28 个模型请求，实际比较数可更少；两轮不可共用一份 28 次预算。
结果缓存关闭，`num_retries=max_retries=0`；每次实际 IPC 提交前由原预算守卫收费。
完整原查询返回即记 sink，再按保存的实际 RNG 状态、比较答复重放原算法并核对完整关系和原 sink，
随后评价、持久化和关闭。重放不发模型请求；审计失败保留失败与原生答复。

CPU 软件资格使用 `python -m tests.baselines.text.replay_lotus_topk replay`。
该模块的 `renderer` 子命令只核目标 vLLM 原生消息渲染/token，不加载引擎或权重。
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
| Kalypso | 正式目录含 Movie Filter 和共享 Q8/Q9/Q10 Map 接点；另有 MMQA Q3a–g／Q6a–c 和 Medical Q1/Q4 的具名私有原生运行 | 本统一文本 CLI 未暴露作者服务；新接点的私有迁移已做保存响应回放，目标运行绑定待核，旧结果不自动取得新内存 sink 口径。原 `05024f02` 未透传结构化输出，MMQA Q1／Q4 Extract 留空 |

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
补充文本入口的 Movie／MMQA／Medical 配置省略生成项时均采用原 LM 的8192 token／无 stop，显式值保留。
这改变了旧补充 Movie／MMQA 的省略项解析；显式8 token／换行 stop 的旧配置和普通 Filter 类型默认不变，旧结果不改写。

<a id="sembench-mmqa-aggregation"></a>
### MMQA Q5 原分层聚合

[Q5 适配](text/sembench_mmqa_aggregate.py)保留 `_execute_q5` 的片名筛选、原 `sem_agg` 层次和 `_output` 评价，
独立[原生入口](../../scripts/experiments/run_sembench_lotus_mmqa_aggregate.py)直接使用 LOTUS LM 与外部文本服务。
它不经过仅接单个 F/M/E 的记录入口。原 sf25 的25行中，Q5 选16行，正文12403个 UTF-8 字节；
Q4 的24行不是该查询规模。只有原 SDK 首层恰好生成一个请求时，总量才精确为1；
多首层分块的总量还依赖真实摘要长度，`check` 不把它猜成固定次数或逐行16次。

预检使用本地 tokenizer JSON 和原 LM 计数，记录声明的服务上下文、完整首层消息和计数来源，零模型／零额度消耗；
本地 tokenizer 身份不代替服务端验证。运行使用既有持久预算逐 POST 消费，保留原分层归并、失败尝试和原 JSON 评价。
该路径计时基于已就绪的外部服务，模型启动／退出另由宿主核查，不能与普通文本冷启动成绩混算。
`source_to_sink_seconds` 从本地 LM／tokenizer 准备前到完整原聚合结果返回，排除 observer 收尾、评价、
结果编码和持久化；旧 `query_seconds` 保持原边界。原 sf25 保存输入的软件 SDK 回放不证明目标 tokenizer
的首层划分，真实调用上限仍需上述目标预检，不能直接使用软件 fixture 的单请求数。
当前仅原生 LOTUS 聚合已接线，另外四种 baseline 的聚合项留空，不增加替代算法。
配置与命令见[脚本说明](../../scripts/README.md#sembench-native-mmqa-aggregate)。

本轮保留的可复现边界：

| 现象与根因 | 处理与验证 |
|---|---|
| Daft source DataFrame 提前选择 Native runner，随后显式线程配置被当成外部覆盖而拒绝，发生在模型启动前。 | 在创建 DataFrame 前调用同一个 Native 配置入口；省略时保持 Daft 默认，已有 runner 仍不被暗中重配。原失败及零模型复现保留，原生图／核不改。 |
| MMQA 原 runner 的 `src.runner` 与 SemLoom 已加载的 `src` 冲突，首轮无法导入。 | 仅在原模块加载期映射同一已验证上游 runner，随后恢复；十六题原查询／方法／accessor 差分及别名恢复检查通过。 |
| MMQA Q2a/Q2b/Q7 的原 `"zs-cot"` 字符串与 LOTUS1.2.4 的枚举不相等，静默生成非 CoT 提示。旧软件回复检查未覆盖策略分支。 | 在独立查询 worker 调用原 Join 的边界转为 `ReasoningStrategy.ZS_COT`，正常及异常返回均恢复 accessor；不改原查询源码、候选对、顺序、Extract 或解析器。回归检查实际原生 API 参数与 HTTP 消息中的完整 CoT 提示，后续 `d061df8c` 实测确认该提示。 |
| Q2b 空 Join 后，LOTUS Extract 仅从实际回复字典创建新列，零行结果没有声明的 `color`，原最终投影报错。 | 仍执行原零行 Extract，在返回边界保留声明列；不填值、不补行、不发送 Extract 请求。保存的零行调用记录可重建空表并用原评价器离线评价，原失败与失败耗时继续保留。 |
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
| MMQA Q2b | 同一切片，Join325次，后续 Extract 为实际匹配行数，最多再325次 | CPU 完整 Join→Extract 及零行 schema 通过；d061实测的空结果返回失败与离线评价单列，preset 单次 Extract 不构成完整模型资格 |
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
[原生 LOTUS 视觉运行入口](../../scripts/experiments/run_sembench_lotus_image_query.py)已将这些原查询直接接到原 LM SDK 的 OpenAI-compatible 服务。
原生 SDK 保留 PNG 消息、并发批调用、`zs-cot` 和 Extract `json_object`，不用 prepared-call seam 或文本 IPC。
本地多模态 IPC owner 仍未接通；本次服务入口的真实模型、processor／generation 与物理服务关闭待机器验证。
原生 LOTUS 也可由原 LM SDK 直接访问独立官方 VLM 兼容服务，复用现有 `native_http_observer.observe_native_httpx`
观察完整原请求及每次尝试；这不是已退役的 SemLoom HTTP 代理，也不需要改变算子。该服务的配置、模型身份与生命周期仍待机器绑定。入口读取已有预算的预留单元，每次真正的 HTTP POST 先消费，
不分配或退回额度；Q2b 按完整 Join＋最多同等数目的 Extract 预留。原失败与已发送单元不得重置。
服务启动和 GPU 归外部 host owner，入口仅监督查询子进程；该计时为已有服务上的完整查询，不能混入文本冷模型启动成绩。
私有审计保留原 POST 的内容摘要和 PNG URI 摘要，避免在事件队列复制整幅 base64 图像；原请求字节不变。
生成配置未给出的值按原 SemBench 8192 token／无 stop 解析，显式值保持，不继承短文本 Filter 的8 token／换行截断。
[Ray Animals Q7](image/ray_animals_q7/README.md)使用原生 Ray LLM processor，顺序执行两遍64条 Filter和原 City交集；
两次各自启动/关闭，整体共享128次额度，不能标为同模型 warm。默认示例使用 Ray2.56.1／vLLM0.22.0；
原128条模型资格与当前清理接口的Linux CPU资格分列，其他环境和完整公平计时另验。
当前 Q7 保留第一 stage 的物理退出及清理，在最后 stage 预测原子发布后执行原 City 交集；
完整内存 sink 就绪后 ACK，再完成最后 runtime 的公开关闭和宿主清理。第一 stage 的释放仍属于
该双 worker profile 的必要流程。19 项 CPU 及 128 条保存答复身份／parser 回放通过，完整原 City／
evaluator 和目标 Ray／GPU 新终点后来已由 `909f842c` 完成独立资格；后续失败诊断/退出码
改动与本组合树仍只按各自 CPU 证据解释，不从公共 runtime 存活推断内部模型 actor 驻留。
旧结果包含最后关闭，不能减去估计清理时间回填新口径。

Ray／Daft 的 CLIP 图仍是独立嵌入路径；Q7 的资格不扩大为其他 VLM Join／Extract 支持。
IMLane／Kalypso 当前视觉绑定未建立，不由此推断整个上游系统永远不支持视觉。
原 LOTUS 明确不支持的音频题留空，其余未验证能力另记。

CPU 视觉夹具使用真实 ImageArray、原 PNG 格式化、原 Filter／Join／Extract 解析与关系结果。
MMQA 同一图片的多个匹配行仍逐个 Extract，JSON response_format 保留原值。
Q2b 无匹配时，原零行 Extract 返回后保留声明的 `color` 列，最终结果仍为零行，额外 POST 为0；
非空 Extract 返回 `{}` 缺列时仍抛出原 KeyError。正常及异常结束均恢复原 Join／Extract accessor。
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
其余纯文本候选为 E-Commerce Q1/Q3/Q5、Cars Q1/Q4/Q10，目前没有已核实的目标成套输入。
Medical 已核实上游自带的完整文本资产，按上方原文本入口直接复制；Cars／Medical 的普通准备会顺带下载图音素材，
文本准备不可盲跑整套 setup。需要其他规模时选择现成原 CSV 或原采样方法，不另写采样。
E-Commerce 的嵌套 `productDescriptors` 保留原 DEFAULT 的整对象字符串，不能抽取一个 description 字段代替；
E-Commerce Q3／Cars Q10 的 keyword Extract、E-Commerce Q5 的 Map suffix 需要单独绑定，当前有限 hook 尚未接这些参数。
这些属于待接线／待数据，不是系统缺少原算法。E-Commerce、Cars 和 Medical 的其他原实现按各自文件与能力准备；
此前限定目录中未找到的数据只记待定位，不推断全服务器没有，也不下载或改写算子凑结果。

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
