# Kalypso 原生 SemBench 适配

本目录源自2026-10-25 r3适配。维护版将客户端接入中性生命周期与公共计时，并将新运行绑定改为作者CLI默认解析；旧运行的固定容量配置和成绩保留历史身份。本目录没有注册新的默认后端，也不包含作者服务、模型或数据。作者源码固定为`05024f027f932c0e398852e38302be30b9015481`，原生引擎属于vLLM0.13.0rc4分支。

## 内存结果边界

`NativeQueryDriver.query()` 在原生 HTTP 响应解析、发生项索引／模型／返回数量校验，以及原
`MovieTask.result_frame()` 的完整投影或聚合完成后，立即记录
`backend_report.memory_sink_timing.memory_sink_ready_ns`，随后发布 `memory_sink_ready` 事件。
统一查询时间从外层 `query_start_ns` 到这个客户端单调时钟；不从旧 `answer_ready_s` 扣除估计开销。
作者 HTTP handler 会等待完整 `processor.execute()` 后才返回所有存活 context，客户端只恢复
原生 `idx` 对应的发生项，不改变 Filter parser、调用预算、HTTP 模型路径、pin 或 KV 策略。

`semloom.kalypso_memory_sink.v1` 的 `phases` 分别记录 source／必需输入、HTTP＋JSON 解析、
原关系生成、trace 审计、结果编码、评价和审计持久化的起止时间、秒数及状态。
`native-input.csv` 是作者服务实际消费的执行输入，仍在 source 内写入；request／occurrences、
response body／HTTP clock、决策和质量文件在 sink 之后保存。独立写入失败会继续保存其余证据，
同时令查询失败；原始查询异常不会被后续写入异常替换。

trace 完整发生项、request ID 和 model UID 核对在 sink 后执行，审计失败保留已观测的 sink，
但最终查询仍失败，不能据此产生合格成绩。旧 `result_ready` 仍位于审计和编码完成之后；历史
时长不改写。`HTTP-clock.json` 新版 `semloom.kalypso_http_clock.v2` 只计响应读取／关闭，明确
排除 JSON 解析和文件持久化。

现有私有 Map／MMQA 查询重载 `query()` 并继承 `post()`，后者默认仍同步保存 HTTP 证据。
这些入口未迁移时不能使用本次内存 sink 资格；只在迁移的 query 内开启延迟持久化。退役该兼容
行为的条件是所有实际调用者都采用显式内存 sink，并在新私有副本完成回归，不能修改冻结包。
此目录的 Filter／Q9 Map 绑定不是 Kalypso 已完成题目的全集；既有 Movie Map 和 MMQA 私有运行另外保留
其原生入口与历史结果。本次[受控时钟测试](../../../../../tests/baselines/text/test_kalypso_memory_sink.py)
仅验证本地顺序、输出和失败证据，不授予新的作者环境或 GPU 资格。

私有查询的迁移复用 `NativeQueryDriver.query_evidence()`，它仅统一延迟证据写入、阶段计时和
异常回执，不运行查询、服务或模型。`response_map_texts()` 校验完整 Map 返回并恢复原顺序，
随后仍须调用该查询已有的 parser；只有原 `query.execute()` 的完整投影／聚合返回后才能记录
sink。Filter 使用 `response_decisions()` 恢复发生项，再交回原查询完成关系计算。逐行 prompt
表被作者 hook 实际读取时也属于必需执行输入，仍在 sink 前写入。

Q10 Map 与 MMQA Q3b–g 的现有私有入口已有独立副本迁移补丁，保留 `make_query`、`prepare`、
`reconcile` 和 owner／CLI 编排。前者的两次 2000 行原 CPU 响应、后者的十二次归档真实 HTTP
响应已做本地原查询／parser／评价器差分回放；新结果及评价与各自原件一致。后者旧 CPU fixture
缺少 `model_name`，不能通过新边界的必要校验，失败记录保留；没有补造字段。
回放中的 `original_query` 阶段包含输入、HTTP、parser 和原关系计算，和嵌套阶段不可直接相加。
这些是新私有副本的 CPU 候选；未部署、未运行新的 GPU 查询，其余私有题入口仍须各自迁移。

`adapter.py`只负责输入、完整发生项核验、源序恢复和观测接入；作者服务继续拥有Filter执行、生成设置、批处理和调度。原生parser以结果不含`false`判通过，与LOTUS不同。`native_server.py`保留实际worker模型元数据及物理设备旁录；客户端关闭不代表服务退出。`cpu_native.py`需要已有作者环境和模型tokenizer；本次没有安装或启动它们。

attached比较流程与CLI维护在`src/experiments/kalypso_query_lifecycle.py`，查询脚本`run_kalypso_sembench_query.py`直接调用该模块。`adapter.py`保留作者服务客户端，不导入实验编排。旧`adapter.run_attached`与原模块CLI可从Git提交`5b25197dafc03d6ae7fa48e7c0f7460122e85e51`的完整树恢复。

原程序以独立的`adapter`和`native_server`模块名解析worker扩展。以后在私有运行副本执行时，将本目录加入该进程的`PYTHONPATH`并提供明确配置；不要只改扩展名而声称继承原worker测试。`load_dependencies`还需要具名历史源码及对应packet依赖，新私有运行绑定尚未完成。

`qualification.py`仅提取成功r3控制器的采样参数审计比较：将`stop=None`与`stop=[]`视作作者规范化后的等价值，其他键和值仍逐项比较。它不改写实际生成参数。旧端口TIME_WAIT修复保留在原运行来源中，尚未移植通用控制器；不能据此声称本目录已经提供完整的启动/关闭入口。

独立CPU回归覆盖默认物理UUID读取、设备漂移、负例与未知终态、预算停止及原异常保留；未重跑完整原评价器或真实worker。历史运行报告和私有封包尚未随此候选发布；[具名复现说明](../../../../../../experiments/reproduction/native_baselines/README.md)区分当前维护代码、历史来源及仍需新建的运行绑定。

## 正式目录入口候选

- 作者服务：`PYTHONPATH=code python code/scripts/experiments/run_kalypso_native_server.py --help`。真实服务仅在显式启用的私有config下启动；`--cpu-check`只导入作者模块并解析参数，不启动引擎。
- 已挂载服务的SemBench查询：`run_kalypso_sembench_query.py --config ... --unit ... --output ...`。客户端不拥有服务器退出，外层owner仍须单独监督/记账/核验物理清理。
- 原生CPU/token/planner检查：隐藏CUDA后使用 `check_kalypso_native_cpu.py --config ... --output ...`，需要作者固定隔离环境。

attached比较编排使用实验层的 `native_query_lifecycle` 和公共 `native_query_timing`；ready中的`model_startup_scope`明确模型属于外部GPU owner，客户端attach没有测量模型初始化。没有修改作者Filter/生成/parser/调度，Kalypso仍非LOTUS backend。此目录迁移后的CLI仅做本地无模型smoke和CPU替身回归，尚未重新绑定作者环境、配置/资产/worker路径或转授旧r3 GPU资格。私有cfg的baseline_source必须指向本次明确代码树，dependency_sha256必须重新封包，禁止冒用旧锁。

## 新运行使用作者默认值

新私有绑定必须设置 `native_launch: "author_defaults"` 并移除整个 `engine` 对象。旧配置会明确拒绝，不能在原运行目录重新执行。服务只传模型、localhost端口和现有模型身份观察扩展；不传序列/每轮token额度、显存比例、context、dtype、attention、chunked prefill、prefix caching或engine seed覆盖，也不把推导的原生默认数字写回包装器。模型/数据/物理设备及完整查询额度仍由外部owner明确绑定；没有把SemLoom的R、CB、B或扫描器接入作者服务。

`native-launch.json`记录实际argv、原生parser读出的值及来源。CPU解析中的None保持未解析含义；`cpu_native`仅使用作者`AsyncEngineArgs.create_model_config()`读取模型context/dtype并验证完整prompt+原输出cap，不创建EngineConfig、不探测GPU容量、不加载权重。CPU计划中的6GiB仍只是数值fixture，不能作为服务实际KV预算。

实际worker在原`load_model`返回后，将其已有`vllm_config`的model/scheduler/cache/parallel/attention/compilation字段写入`loaded-model.json.native_engine_configuration`。新客户端同时核验启动回执与model UID/设备/有效容量；缺失值不替换成预想默认值。此观察时点可能早于KV分配和图捕获结束，最终分配与启动阶段还须保留作者原生日志。KV metrics采样比例也不再覆盖，缺计数或采样值按实际来源解释。

作者README的Llama70B/TP4示例与当前既有7B/单卡绑定有区别。新运行让原生CLI按该绑定解析默认值，并披露这个环境选择；不能称完整论文配置复现或已达到硬件平台。两种历史运行与新默认运行分别报告，不改写原结果。

本地回归覆盖参数省略/旧覆盖拒绝、固定作者源码检查（含`vllm/config`及`arg_utils`）、实际值只读回执、三处标准CLI、输入完整发生项与质量记录，以及原观察异常/unknown与历史投影。真实作者环境CPU解析、worker初始化和完整SemBench新运行尚待验证；本地替身不授予GPU资格。

外层既有owner可调用`src.experiments.kalypso_query_lifecycle.owner_cli_bindings(cfg)`取得server/CPU/query命令前缀、Git模块PYTHONPATH和实际旁录模块路径；`native_cpu_evidence`必须指向新CPU检查结果，不能继续匹配旧参考目录。该函数只返回命令数据，不启动进程、取得lease、调整超时或接管清理。owner继续在原timeout前缀、进程出生身份监督、TIME_WAIT端口检查与finally物理清理内调用这些命令；源文件仍写死私有模块路径时，需要显式替换相应接点。历史owner的campaign根、已过期限和账本身份不能重放成新运行，本接线不修改或放宽这些保护。

## 外置 owner 辅助的退役引用迁移

服务器零模型预检发现，外置 `q3_host → q3_packet.read` 仍导入退役的
`sembench_records_formal`；修正这条导入后，host 顶层还有退役资源采样器的引用。
普通 JSON 读写改用现行 `src.infrastructure.private_artifacts`，脱敏改用
`src.infrastructure.redact`。仍被使用的 58 行资源观察器迁到
`src.observability.process_resources.supervised`，原算法语法树不变，未恢复旧 HTTP／固定 campaign。

[导入迁移补丁](../../../../../../deploy/autodl/kalypso_owner_imports.patch)只修改
`q3_packet.py`、`q3_host.py`、`host_helpers.py` 的普通导入，覆盖延迟读取报告的接点。
在**新的私有 packet 副本**、冻结其新摘要之前执行：

```sh
patch --batch --forward -p1 -d "$NEW_PRIVATE_PACKET" -i "$VERIFIED_REPO/deploy/autodl/kalypso_owner_imports.patch"
```

新 packet 的源码、CPU evidence、账本与时限仍按本次运行绑定；不修改历史副本、冻结摘要或旧成绩。
补丁不改变作者服务、提示、生成、parser、容量、CPU placement 或清理规则。
实际辅助源码的隔离副本已完成 CPU 导入检查：禁止 Core／调度、退役模块、模型库、子进程与网络，
仍能加载 host、读取原 scope 配置并取得公共监督 API。此检查不启动作者服务，也不提供 GPU 资格。


## 每次查询的支持声明

attached入口把自己的后端标识和模型复用能力随调用交给`native_query_lifecycle.run_sequence`。
不再临时改写共享模块的支持集合；Movie默认支持、查询时钟和关闭处理保持。
声明来自适配代码，没有新增CLI／JSON配置项，也不改变作者Filter、生成或服务调度。
[复现、回归与源码](../../../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#native-lifecycle-call-support)
记录观察器构造失败和两个会话交错退出的CPU反例；它们不代表真实作者服务并发或模型资格。


## Movie Q9 Map 维护版接线

`run_kalypso_sembench_query.py` 的专属 `movie-q9` unit 现在选择 `movie_map.MapDriver`；
其余已支持的 Movie Q1–Q4 仍走原 Filter driver。Q9 使用原 SemBench c814 的
`ranking="map"` 数字评分、完整排序关系和原评价器，不调用 TopK，也不宣布支持其他私有题。
私有 `map_query.py`、`map_server.py` 不再作为新 Q9 运行入口；历史副本及原结果保持不变。

新配置除了现有 owner／模型／输入绑定，还必须有：

- 每题 `task="movie-q9"`、`expected_calls=256`、`max_native_calls=512`；两题 unit 的
  `call_cap=1024`，分别记录实际物理次数。外层 owner 仍验证总预算及逐题预算之和。
- `sembench` 指向固定 c814 原查询／评价器，`data_dir` 指向完整 Movie CSV；
  `source_data_sha256` 至少绑定完整 `Reviews.csv`，可同时记录 `Movies.csv`。
- `map_prompt_root` 指向新私有空目录。每次查询重新扫描原输入，选出全部 256 个 Ant-Man
  发生项，写入作者实际消费的 CSV 和逐发生项 prompt 表。重复业务 ID 不去重。
- `native_launch="author_defaults"` 且没有 `engine`；保持原作者 05024 checkout、同一 maintained
  source、依赖 SHA、worker 观察模块、新 CPU evidence 和新 owner 的时限／账本绑定。

`map_observation.py` 迁入已经验证的 Q9 字段格式化和被动观察：精确 rubric／review 嵌入原作者
Map prompt，原 `_planned_max_tokens`、`estimate_tokens`、ratio update/reset、pin/KV、采样和执行器
不改写。作者首请求输出额度动态估计；仅 `finish_reason=length` 且额度小于 8192 时，原算法
重入一次并使用 8192。第二次仍为 length 也不会再重试。因此 256 是逻辑发生项，512 是物理
上界；HTTP 和外层 lifecycle 都不自动重试。通用 SemBench 的无重试元数据不代表禁用此作者分支。

完整原查询关系返回即记录 memory sink，随后才执行 API start/sampling/end 身份审计、source
快照、原评价器、编码、持久化和客户端 close。逐请求序号、请求 ID、idx、model UID、采样与
原生重试分支均核对。审计失败可保留已观察 sink，但查询失败，不能产生合格成绩。
`source_to_sink_seconds` 是 ready 服务后的新鲜逐题查询时间；`startup_inclusive_cold_seconds`
在没有本次外部 owner 启动事件时留空，不把 attach 当模型加载，也不拆分／减算旧批次时钟。

标准 CPU 入口对专属 Q9 unit 调用 `movie_map_cpu.check`，使用原 tokenizer／planner／request
对象核对完整 prompt 和重试 cap 的 context 长度，保存每发生项消息摘要及 token IDs；不创建引擎。
该入口仍需要**已经存在的作者隔离环境**，不能用本机 source-method fixture 代替目标运行资格。

本次本地 CPU 检查包含既有 Filter 回归、新 Q9 事件／预算负例、完整 256×2 保存响应与原评价器
回放，以及真实维护版 query CLI 的受控时钟检查。全部 256 条原消息和 tokenizer IDs 与原归档
一致；原方法体在显式 API/KV 替身下覆盖全 256 条 length→8192→length，最多两次，观察 hook
保持原 generator 对象。它们不是新模型分数、全作者模块导入、目标 planner 或 GPU 资格。
共享目录的名字不能作为版本证据：名含 05024 的本地资产实际还可能是 9e919 工程变体，必须
核对 Git HEAD 和源码摘要；本次方法核对从原 05024 Git 对象取字节，不修改共享资产。

零模型维护回归：

```sh
CUDA_VISIBLE_DEVICES='' PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=code "$DRIVER_PYTHON" -B -m unittest \
  tests.baselines.text.test_kalypso_movie_map \
  tests.baselines.text.test_kalypso_native_observation \
  tests.baselines.text.test_kalypso_native_launch \
  tests.baselines.text.test_kalypso_memory_sink \
  tests.experiments.test_kalypso_owner_cli_binding
```

目标原生 CPU 命令仍是 `check_kalypso_native_cpu.py --config "$NEW_CONFIG" --output "$NEW_CPU_EVIDENCE"`；
服务只由既有 owner 通过 `owner_cli_bindings` 返回的 maintained CLI 启动。新 Linux owner/source/
native CPU 与实际 GPU／完整 sink／物理清理资格仍待验证，不复用归档 LAUNCH、期限或模型成绩。
