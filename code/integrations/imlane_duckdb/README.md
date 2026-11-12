# DuckDB＋作者IMLane的原生Movie Q3

这是项目的原生API工作负载绑定，使用作者修改版DuckDB、DBEnd-C、coordinator与CPython executor。
它不调用records协议或SemLoom执行／调度，不声称复现作者未公开的论文Q7 UDF。
数据、消息、解析及评价复用[中立Q3合同](../../src/baselines/text/sembench_movie_native.py)。

| 文件 | 职责 |
|---|---|
| [q3.cpp](q3.cpp) | 完整四列CSV→原生普通筛选→PROCESS或THREAD prediction→单次COUNT；保存完整EXPLAIN与结果，不在本地C++回调执行算法 |
| [q3_udf.py](q3_udf.py) | Arrow `c0..c3`→一次batch-provider调用→无NULL、同序、单chunk int8；保存逐occurrence原答复 |
| [model_provider.py](model_provider.py) | 一次作者batch→一次Unix socket RPC，原消息／原文／usage及失败回执；不拥有模型或引入SemLoom调度 |
| [model_service.py](model_service.py) | 独立单模型IPC进程及birth匹配关闭；实际native容量／socket身份与统一期限，外层核对GPU与SHM |
| [model_ipc_worker.py](model_ipc_worker.py) | 一个原生AsyncLLM，多客户端／batch异步提交，原生continuous batching及abort／drain |
| [model_wire.py](model_wire.py) | 长度前缀JSON、严格UTF8、整批绝对期限，不加载Core／模型 |
| [runtime.py](runtime.py) | 仅拥有作者coordinator／executor；复用已有纯进程归属与清理设施，不创建records客户端 |
| [build.py](build.py) | 隔离导出固定作者源码、记录版本／缺源恢复和便携配置，另行编译薄driver；不安装软件或改canonical前缀 |
| [missing_sources.json](missing_sources.json) | 三件必要构建源的准确官方提交、Git blob、SHA256；没有每批源码版本门槛 |
| [qualify_varchar.py](qualify_varchar.py)、[C++ driver](varchar_qualification.cpp)、[fixture](varchar_fixture.py) | 零模型 VARCHAR 的真实 DSO／PROCESS／THREAD 资格入口；原作者与 schema-child 工程版分开记录 |
| [native_imlane_relation.py](../../src/experiments/native_imlane_relation.py)、[relation_handoff.hpp](relation_handoff.hpp) | 在完整原查询 sink 就绪前持有 native driver／DuckDB result；ACK后退出、审计与作者关闭分开 |
| [sembench.cpp](sembench.cpp)、[sembench_udf.py](sembench_udf.py) | 工程版 VARCHAR 原生投影：call_id／messages_json 入，原模型文本出；不实现替代 parser |
| [native_imlane_sembench.py](../../src/experiments/native_imlane_sembench.py) | MMQA Q1 原题准备／完整 sink，复用既有作者、模型 owner 与监督器；25 次物理预算 |
| [native_imlane_filters.py](../../src/experiments/native_imlane_filters.py) | 显式单题 Movie Q1（2000 次）／Q2–Q4（120 次）、MMQA Q3a–g／Q6a–c（25 次）或 Medical Q1/Q4（1200 次）；原版 TINYINT Filter、完整原查询 sink 后 ACK |

输入四VARCHAR依次为`occurrence_id/id/reviewId/reviewText`，作者转换器按位置命名`c0..c3`。
输出为TINYINT，SQL比较`=1`；不走尚未资格的位图Arrow Boolean转换。完整cohort先进入DuckDB；
作者planner将普通Filter放在prediction的child，运行时再以实际UDF回执确认只有eligible行。
COUNT与回执消费同一次模型执行，后置审计不会重跑AI图。

## 构建

固定作者来源为`IM-DM4AI/IMLane0@072f8257db80569e42c9f5727d839587df529652`。
原根`build*`规则误排除两个optimizer文件和mbedtls头。仅使用官方
`duckdb/duckdb@5ebd8ca697099859239ba85e0de07dac7e1b7c04`的三件准确字节恢复；
既有56份不同文件均保留，不把整个作者fork说成该上游树。来源与实测状态见
[准备报告](../../../experiments/results/diagnostics/imlane_duckdb_q3_20261106/README.md)。

先将manifest三件文件从该官方提交按相对路径放入仓外`UPSTREAM_SOURCE`，然后：

```sh
PYTHONPATH=code python code/integrations/imlane_duckdb/build.py prepare \
  --root "$IMLANE_AUTHOR_ROOT" --workspace "$DUCKDB_WORKSPACE" \
  --upstream-source "$UPSTREAM_SOURCE" --jobs 4
```

`prepare`只写新的私有源码副本和明确命令。复查后运行打印的configure／build命令；
数据盘、编译CPU／内存／时间与已有host租约按runtime runbook约束。
保留C++17、实际作者Python、fmt前缀和现有SDK依赖头目录；不通过安装普通pip DuckDB替代它。
无可核对release tag时LibraryVersion如实为unknown，不能填写论文0.10.1。

`prepare --schema-child` 是显式工程变体：仅在新导出的隔离源码内，把
`ConverArrowAarrayToVector(child_array, &schema, out)` 改为相应的 `schema.children[0]`。
原始 header 必须匹配固定 SHA256；默认保留原作者调用，`prepare.json` 记录两种身份。
该一行修补不处理 NULL、slice offset 或长字符串所有权，也不代表新库已编译或 SQL 已通过。
不得把现有工程修补 checkout 的 HEAD 标成原作者提交。

```sh
PYTHONPATH=code python code/integrations/imlane_duckdb/build.py driver \
  --root "$IMLANE_AUTHOR_ROOT" --duckdb-source "$DUCKDB_SOURCE" \
  --duckdb-build "$DUCKDB_BUILD" --output "$PRIVATE_BIN/q3"
```

## 零模型CPU检查

[实验入口](../../src/experiments/native_imlane_query.py)只允许明确的软件fixture。
在外部host租约、环境预检和资源supervisor内执行worker；不要在已有coordinator上注册／热替换函数。
作者默认端口7086必须空闲；省略executor／DuckDB线程配置时保留作者默认，不注入单线程限制。

```sh
PYTHONPATH=code python -m src.experiments.native_imlane_query prepare-fixture \
  --reviews-csv "$REVIEWS_CSV" --output "$PRIVATE_PACKET"

PYTHONPATH=code python -m src.experiments.native_imlane_query worker \
  --packet "$PRIVATE_PACKET" --output "$NEW_PRIVATE_RUN" \
  --author-root "$IMLANE_AUTHOR_ROOT" --method-python "$IMLANE_METHOD_PYTHON" \
  --driver "$PRIVATE_BIN/q3" --kind process
```

THREAD用新的coordinator／executor／输出目录独立检查。`audit_result(packet,query_json,receipts,checkout)`
在driver环境读取保存结果与回执并调用原评价。worker进程／namespace通过，不替代外部GPU、SHM和完整质量核查。
空结果、错误及清理失败保留原件；软件COUNT14只验证接线，不是模型质量。

准确798e的真实PROCESS／THREAD软件运行均已通过：2000→120、一次COUNT／batch-provider、
Arrow16.1往返及外层进程／SHM清理。作者默认4 executor共需4×128MiB Lane和8个控制队列；
实测队列逻辑8592B、各占3页，合计512MiB＋96KiB，不能套用只计Lane的512MiB预算。
DuckDB默认16线程仍在显式8核亲和性内，未注入单线程。完整来源和边界见上述准备报告，
特殊空选择／错误的真实二进制路径和模型服务生命周期仍未资格。

## 本地IPC模型接线

[模型worker](../../src/experiments/native_imlane_model_query.py)与原软件fixture入口分开，
前者明确拥有一个外部vLLM服务；作者coordinator／4个CPython executor不加载4份GPU模型。
数据库→executor仍用作者ArrowLane／共享内存；UDF→单个AsyncLLM改用本地AF_UNIX socket。
论文Q7仅明确远程vLLM，没有已核实的HTTP硬要求；本接线是项目原生API绑定，不冒充论文Q7 runner。
不复用SemLoom的单父进程AsyncEngineProcess或C/R调度，也不使用会串行跨batch的同步LLM.chat。

私有模型配置如下；用现有资产替换占位路径。模型socket由owner在新私有短目录创建，作者7086仍须空闲：

```json
{
  "schema": "semloom.imlane.duckdb.q3.model_run.v2",
  "provider_mode": "model",
  "reviews_csv": "<original-Reviews.csv>",
  "model_service": {
    "model_python": "<existing-vllm-python>",
    "model": "<local-Qwen2.5-7B-Instruct-directory>",
    "served_model_name": "sembench-qwen25-7b",
    "dtype": "bfloat16",
    "engine_options": {}
  },
  "sampling_params": {
    "temperature": 0, "top_p": 1, "max_tokens": 8192, "n": 1,
    "stop": null, "presence_penalty": 0, "frequency_penalty": 0,
    "repetition_penalty": 1
  },
  "request_timeout_s": 1200
}
```

worker入口：

```sh
PYTHONPATH=code python -m src.experiments.native_imlane_model_query \
  --config "$PRIVATE_MODEL_CONFIG" --output "$NEW_PRIVATE_RUN" \
  --author-root "$IMLANE_AUTHOR_ROOT" --method-python "$IMLANE_METHOD_PYTHON" \
  --driver "$PRIVATE_BIN/q3" --sembench-checkout "$SEMBENCH_CHECKOUT" --kind thread
```

模型服务直接使用vLLM0.25.1的`AsyncLLM`、`render_chat_async`与每行`generate`；
`generation_config="vllm"`避免模型repo默认覆盖共同SamplingParams。这是项目对照条件，
`engine_options`直接交给原生AsyncEngineArgs，不注入并发8、4096容量、worker或额外队列门槛；
固定model／dtype／generation_config不可由该字典重复覆盖。实际容量在ready回读，不以请求值冒充生效值。

一个作者batch发一个RPC，同batch每行及不同executor的batch在同一event loop并行进入原生continuous batching；
服务不重新合批、不分配SemLoom credit、不重试。默认4 executor不表示真实Q3一定有4个同时工作的batch；
此前120行只有1个作者batch，不为制造并行手动切成四块。
socket父目录0700、socket0600，短路径避免AF_UNIX长度限制，且只清理本次相同inode的socket。
JSON帧带8字节长度前缀，没有换行Reader的64KiB隐式界限或额外8MiB门槛。
断线、期限或取消须abort并drain本连接自有模型请求；服务关闭先停止accept，再关闭原生engine。

逐行保存原模型文本／usage／finish reason及native generate调用状态；调用数不冒充物理模型执行数，
未收到完整terminal时仍unknown。length保留原文供原parser评价，不补答案或以False掩盖失败。
IPC减少TCP／HTTP协议、每行HTTP请求和HTTPX依赖，但仍有进程切换、JSON与socket复制；
未做匹配传输测量前不声称速度收益或零拷贝。HTTP诊断源码可从e420a0abbc02782a919340a3a83179f077f2598a恢复，
[必要软件证据](../../../experiments/results/diagnostics/imlane_duckdb_q3_20261106/http_software_cpu.json)保留；真实HTTP模型测试暂停。

完整2000源物化计入冷scope，普通筛选仍由DuckDB完成，正常逻辑量120。
审计用同worker的调用起止区间连接UDF与IPC批次，再逐position核对occurrence，重复消息不靠全局Counter掩盖交换。
已发布自有回执按实际文件大小读取，不套用小配置JSON的8MiB界限；整体RSS／磁盘仍由外层监督。

新driver在析构前写Linux `CLOCK_MONOTONIC`结果时钟，运行模型worker前须重建；旧798e二进制没有此字段。
结果出现后一个120秒窗口覆盖native析构、作者关闭与服务关闭，末尾8秒留给外层强制停止。
无结果失败时另写cleanup-start标记，让parent按首次失败固定期限监督；marker写失败仍继续回收并保留原异常。
每次IPC调用在连接／发送／读取时使用同机绝对期限，服务每行生成也受该期限约束。
引擎原生关闭可用整个剩余graceful窗口，不能把末尾8秒误作正常shutdown仅8秒；child TERM fallback／KILL均保留失败。
作者既有close仍有分段等待，因此必须用本模块`supervised_run(command, output, supervision_dir, monitor=...)`
绑定READY期限；supervision目录与worker输出分开。调用方持有host租约，并将`ResourceMonitor.finish().passed`
及GPU退出检查纳入最终成功，不能仅凭worker completed或socket返回。构建／新接线资格见准备报告，
真实模型、物理GPU关闭、质量与性能仍待后续实测。

软件IPC CPU验证必须显式`provider_mode=software_ipc`并提供无模型测试factory；普通CLI拒绝该mode，
不能因写了software而意外启动GPU。CPU验证与实际vLLM／GPU资格分开记录。
执行前范围见[原生基线校准](../../../experiments/plans/原生基线校准.md)。

当前124项相关CPU通过，含真实Unix socket四客户端barrier／129行及截止边界；真实作者＋软件IPC闭环按校准计划单列，不将软件结果当模型资格。

## VARCHAR 独立 CPU 资格

这组入口只使用固定字符串，绝不启动模型。Linux 上分别保存原作者与 schema-child 工程版的
隔离 configure／build 日志，再用已有作者 Python／Arrow 16.1 ABI 编译同一 qualification driver：

```sh
PYTHONPATH=code python -m integrations.imlane_duckdb.qualify_varchar build \
  --author-root "$IMLANE_AUTHOR_ROOT" --method-python "$IMLANE_METHOD_PYTHON" \
  --duckdb-source "$DUCKDB_SOURCE" --duckdb-build "$DUCKDB_BUILD" \
  --variant schema-child-engineering --output "$NEW_PRIVATE_BUILD"

PYTHONPATH=code python -m integrations.imlane_duckdb.qualify_varchar run \
  --build-record "$NEW_PRIVATE_BUILD/build.json" --mode converter --case ownership \
  --output "$NEW_PRIVATE_RUN"

PYTHONPATH=code python -m integrations.imlane_duckdb.qualify_varchar run \
  --build-record "$NEW_PRIVATE_BUILD/build.json" --mode sql --case strings --kind process \
  --author-root "$IMLANE_AUTHOR_ROOT" --method-python "$IMLANE_METHOD_PYTHON" \
  --output "$NEW_PRIVATE_SQL_RUN"
```

原版使用 `--variant original-author`，来源与二进制不可混用。每种变体分别执行
`strings/empty/null/offset/ownership` 五个 case；SQL 再分别执行 `process/thread`，每次新建
输出目录与 coordinator namespace。现有外层 host lease、资源监督与总期限覆盖整个 worker，
确认7086空闲；本入口仅限一项构建120秒、一项driver60秒，作者启动和关闭沿用既有 owner，
不能把driver期限误当整个worker的期限。父监督应留足启动／回收时间并检查最终进程与SHM。
Mac 直接拒绝 native 运行，不安装或伪造 Linux 依赖。

converter 只有声明，driver 不编译其实现；构建记录必须证明 `nm` 中该符号为 `U`，运行时
核实 `dladdr` 的真实 DSO 路径及 SHA256。源码variant声明本身仍不代替隔离库构建日志。
NULL 是真正 Arrow validity，offset 使用非零 slice；长串先检查 DuckDB 是否借用 Arrow
buffer。若确认借用，释放后不解引用悬垂指针，所有权 case 失败；只有不借用时才验证释放后值。
SQL 保存实际完整关系；完整关系复制后、校验／文件输出／native析构前取结果时钟，进程退出及
作者 namespace 清理另记。失败、崩溃或超时不能由回执或正确的中间值改判为通过。
本入口新增的是可执行资格程序；Linux native 编译、各项 SQL 与退出仍须真实运行后才填写结果。

`prepare --varchar-owned` 是独立的 `varchar-owned-engineering` 工程变体，与
`--schema-child` 互斥。它在原作者新导出副本上应用
[精确补丁](patches/varchar_owned.patch)：配对 child schema，在 VARCHAR 支路导入
Arrow validity、使用 element offset，并通过 DuckDB `StringVector::AddStringOrBlob`
复制到输出 vector 的 string heap；固定宽度支路字节不变。它保留原来的 u/U/z/Z/vu
接受分支，不改变作者调度或查询算法。旧 `schema-child-engineering` 仍只有一行参数修正。

新变体使用既有 `qualify_varchar build --variant varchar-owned-engineering` 及同一组
converter／PROCESS／THREAD case；须另建 library、probe 和结果目录，不能复用另一个变体
的 DSO。源码 hash、编译日志、实际解析到的 DSO 和真实 SQL 结果分别保存，工程版通过不能
写入原作者资格。未通过、尚未执行或语义不支持的项继续留空。

本地回归入口是 `test_imlane_varchar_owned.py`，需设置
`SEMLOOM_IMLANE_AUTHOR_CHECKOUT` 为包含固定作者 commit 的已有仓库。它从真实原始
header 应用补丁，用作者 DuckDB headers 做 C++ 语法检查，再将同一 converter body 与
明确标注的 storage test double 编译，消费真实 PyArrow C Data。核心 UTF8 用例覆盖 NULL、
跨 bitmap 字节的 slice offset、释放 Arrow 后的 8192 字节长串、非 NULL 空字符串的
0 字节 value buffer，以及输出 validity 重用。此检查不加载原作者 DSO，不代替 Linux SQL
或字符串 heap 的真实运行资格；其他格式的本地值契约尚未扩测。

## 完整关系 sink 与原生结果交接

已有标量 COUNT 的 C++ 就绪点无需调整。Filter／DOUBLE Map 的原关系后处理需要在作者关闭前完成；
等待 driver 退出、退出 `DuckDBScope` 后才执行原 title／group sink 的旧时间只能保持原口径。
不能从旧记录减去估计的关闭时间，也不能把 projection 就绪时钟改名为完整查询就绪。

`run_relation_query` 复用现有 `DuckDBScope`、owner、`QueryWindow` 与外层资源监督。
它在作者 scope 内读取原子发布的 `query.json`，从 native relation 执行原 sink、记完整结果时钟，
然后发布 ACK、等待 driver 实际退出，最后做回执审计与作者关闭。`finish_mmqa_filters` 绑定
MMQA Q3a–g 的原 title sink 和 Q6a–c 的原 Airlines sink，`finish_movie_scores` 绑定 Movie Q9 原完整评分投影和 Movie Q10 原分组 sink；它们分别消费
`accepted_occurrences` 与 `mapped_occurrences.score`，不从模型回执重建结果。
Filter 成员由 native 决定，原输入 occurrence 顺序决定恢复顺序；不按 native 返回顺序、title
排序或去重。服务 `check()` 只检查存活与期限，逐请求 terminal／receipt 遍历放到 `audit()`。
联合 Q3b–g 的150行执行只产生整批时钟，不能当作六个独立逐题延迟；逐题运行使用单题 binding。

### MMQA 原版 Filter 的单题入口

[配置模板](../../configs/sembench_imlane_mmqa_filter.example.json)的 `query_id` 只选一道
`3a`–`3g` 或 `6a`–`6c`，每次完整保留 25 个 occurrence。`prompt_columns` 显式冻结原 LOTUS
实际列顺序，经过已有 `resolved_input_columns` 接口序列化；不随 Python hash seed 改变提示，
不改谓词、parser 或原完整关系。Q3 的 `title/text` 与 Q6 的 `Airlines/Destinations` 仅在 native
传输列中使用共同别名。无额外预热、无自动重试，本入口一次冷查询，不将重启称为暖轮。

```bash
PYTHONPATH=code "$DRIVER_PYTHON" -B code/integrations/imlane_duckdb/build.py filter-driver \
  --root "$AUTHOR_ROOT" --duckdb-source "$ORIGINAL_DUCKDB_SOURCE" \
  --duckdb-build "$ORIGINAL_DUCKDB_BUILD" --duckdb-sha256 "$ORIGINAL_DSO_SHA256" \
  --output "$RUN_DIR/bin/mmqa-filter"
PYTHONPATH=code "$DRIVER_PYTHON" -B -m src.experiments.native_imlane_filters check \
  --config "$CONFIG" --output "$RUN_DIR/check"
PYTHONPATH=code "$DRIVER_PYTHON" -B -m src.experiments.native_imlane_filters run \
  --config "$CONFIG" --output "$RUN_DIR/run" --build-record "$RUN_DIR/bin/mmqa-filter.build.json" \
  --author-root "$AUTHOR_ROOT" --method-python "$METHOD_PYTHON" --kind thread --execute-model
```

`filter-driver` 仅链接已有原版 DSO 并记录实际加载路径；不会重建 DuckDB。实际运行继续由既有
宿主资源监督和唯一协调者获取租约，禁止直接脱离该外层启动。模型 ready 后重新读取 source、
准备完整提示，原生预测关系经原 sink 完成后立即记录 `ready_source_to_sink_seconds`，再 ACK、
driver 退出、审计、评价、持久化和模型关闭。`cold_worker_entry_to_sink_seconds` 另含 worker
入口后的预检及模型启动；外层 `cold_supervised_runner_to_sink_seconds` 从既有 supervisor 的
Popen 前起点计时。它们不包含更早的宿主导入／预检／取租约；未知最早入口保持空白。

本次 CPU 保存答复回放覆盖 Q3b–g、Q6a–c 共 225 条，真实 Arrow／Unix IPC、原消息和完整 CPU
token、原关系／sink／评价、预算第 26 次拒绝及 ACK 时序。author SQL、模型与引擎为明确软件
fixture，不能据此授予新原生 SQL 或 GPU 时长。协调器已报告 `d176cbb2` 九道独立25次的
目标执行；Q3e 外层 guard 失败保留，逐题范围见[隔离组合报告](../../../experiments/results/diagnostics/baseline_validated_integration_20261111/README.md)。旧联合包保留为历史原件，其总时长不能拆为逐题值；正式替代验证后
迁移旧私有启动引用，唯一原始成功／失败证据不删除。Q1 的 VARCHAR 工程变体保持独立。

Python scope 尚在不代表 C++ runtime 尚在。适配的 driver 必须在完整关系发布之前创建
`RelationHandoff`、将其 protocol 与本轮唯一 `sink-release.ack` 路径写入 `sink_handoff`，并在
发布之后、其 DuckDB result／connection／database 离开作用域之前调用有界 `Wait`。例如：

```cpp
semloom_imlane::RelationHandoff handoff(release_path, native_ready_ns);
report["sink_handoff"] = Json{{"protocol", semloom_imlane::RELATION_ACK_PROTOCOL},
                              {"release_path", release_path}};
publish(descriptor, pending, output, report);
handoff.Wait(std::chrono::seconds(120));
```

ACK用独立新私有路径，已存在ACK不能作为新完成。正常 sink 完成后发布 `complete`；sink失败时
发布 `abort` 并等待原进程回收，再走既有 owner 关闭。没有 ACK、错误 ACK、早退、退出失败和
回执审计失败都不能判为成功。接点分别保留 source、native relation、full sink、ACK发布与driver
退出观察事件。source→sink 包括必要的原生关系发布／传输，额外保存、回执审计与关闭在其后。
冷暖共用模型的 worker 应保留整体服务期限，并给每次 native query 一个既有 `QueryWindow`；
不能在冷查询 sink 后提前启动整个常驻模型的最终关闭期限。

调用方提供已有 workload 的 `command`／`kernel`、原完整 `sink(native)`、后置
`audit(native, result)` 和服务 `check()`。需要重新编译带 ACK 的薄driver并将worker接到本入口；
未携带匹配 `sink_handoff` 的旧driver会被拒绝。历史packet原字节保留，只在新副本应用适配。
失败异常携带 `query_report` 和已经得到的 `query_result`；worker 在 `finally` 调用
`persist_relation_observation` 保存报告及完整原关系，次生写失败只给主异常追加说明。
若 sink 已完成而 driver 提前退出，保留 `full_sink_observed_monotonic_ns` 和完整关系，
标记 `native_alive_after_sink=false`，不接纳 `result_ready_monotonic_ns` 或成功资格。
本地 C++ publisher检查与保存关系回放不代表真实 DuckDB／Linux 进程生命周期已完成资格。

## Medical Q1 原版 Filter 接线

[Q1 配置](../../configs/sembench_imlane_medical_q1.example.json)选择完整 1200 条原 symptoms，
沿用同一原版 TINYINT Filter。原 allergy prompt、true-before-false/default-true parser、
patient_id Series（原索引、顺序、重复值与 dtype）及原 evaluator 不变；原标签只用于评价。
审计按原结果类型比较 Series，不为通过测试改成 DataFrame。

完整历史 cold/warm 各 1200 条保存答复已通过共享 CPU Arrow／Unix IPC／原生 provider
预算回放，并逐条核对历史 CPU 完整 token 数组。重复消息的不同回复按 occurrence 保留，
包括 raw Answer: False 但原 parser 判 True 的记录。该回放的 SQL/模型是 fixture，
不授予新的模型质量、性能或 Linux DSO 资格；旧 GPU 只有 token 长度，也不升级为完整 token 资格。

Linux 零模型门复用 [现有 gate](../../tests/baselines/text/qualify_imlane_movie_filter.py)，
Medical Q1 的 `--original-run` 指向冻结原件 `model-output/run/cold`，依次单独执行
`--case complete`、`missing-row`、`duplicate-id`。完整门必须真实执行原 DSO/SQL/Arrow/IPC，
核对 1200 次、原 Series 序列化／质量和 sink→ACK；两个反例须在 prediction 前失败且零请求。
复用当前 build 时必须匹配 driver_source/kernel/relation_handoff.hpp；缺少 ACK header 身份的旧记录
仍保留为原版本历史证据，不能声明当前头文件已编入。只重链薄 driver 生成新记录，不重编 DSO。
冻结源码 `70a153810cbe901ae3d4ee24c9af5cbe9c966c5c` 已独立通过上述Linux门，并完成一次
cold1200/hardcap1200、warmup0、retry0；1200条实际完整消息/token逐条一致，完整原Series
结果40行，precision=0.5、recall=0.4、F1=0.4444444444444445。完整sink、ACK和退出／资源
清理通过不表示质量满分；历史旧GPU只有token长度的限制不变。
原件包 `imlane-medical-q1-70a-1111-r1-originals.tar.gz` 的SHA256为
`fdc5f863bc386c6ed28defbcac85b8440e83f9d20c435c3d95c1286f76b899d3`；仓库外
`imlane-medical-q1-binding-20261111/target-results-r1/LOCAL-AUDIT.json` 汇总逐条核验，
原件内 `run-r1/cpu-host-r2/FINAL.json`、`run-r1/gpu-host/FINAL.json` 保留原源码和退出资格。
上述成绩仅归70a原冻结源；当前组合未作目标执行。Q9维护CLI的独立零模型门状态见下节，
它也不代表当前组合或新GPU成绩。
本机未链接Linux DSO，不由CPU预演或旧v4目标结果转授资格。实际冷起点与ready后完整sink
事件各自计时，禁止估减旧时长。

## Medical Q4 原版 Filter 接线

同一 [单题入口](../../src/experiments/native_imlane_filters.py) 以
[Medical 配置](../../configs/sembench_imlane_medical_filter.example.json) 显式选择 Q4，
固定患者 source scale 11112、原 join 后 1200 个发生项和 1200 次物理调用上限。
原 SemBench c814 的 patients INNER JOIN symptoms、Filter、age.mean() 和单行
average_acne_age 结果均保留；空集合、重复行、NULL age 和原 pandas join 语义不变。
仍使用原作者 TINYINT DSO `52bf5763aab6f35b76746a9fc15b75e27bac1534315d1a20926eb4a78c2badaf`，
不借用 Q1 的 VARCHAR 工程变体，不改作者算法、parser、调度或批大小。

1200 个发生项含 1153 条唯一消息、43 个重复组、47 个额外重复发生项。
共享 provider 的可选 occurrence_ids 只记录到本地回执，与原有 batch position 绑定，
不进入模型消息或 request。原始回复按 occurrence 对应，不去重、不按到达顺序匹配，
不把最后一个重复回复覆盖到其他行。解析继续 true-before-false/default-true。
薄 driver 必须显式 `--expected-rows N`，只接受正十进制 int64；无题目白名单或默认25。
随后以实际 SQL count 与 DISTINCT occurrence_id 检查完整输入，仍在 prediction 前失败。

计时沿用真实双起点：ready 后 fresh source/prompt/token 准备和必要传输到完整内存
join/filter/mean sink；另保留含模型启动的 cold worker／supervisor 起点。完整 sink 先于
ACK、回执审计、评价、持久化和最后 owner close，后置失败仍使运行失败并保留 sink 观察。
第 1201 次请求在 generate 前拒绝，失败／未知不退款，不额外预热或自动重试。

[CPU 回放](../../tests/baselines/text/replay_imlane_medical_filter.py) 使用历史 cold/warm 各
1200 条原保存答复、全部原消息和本地真实 tokenizer 的完整 token 数组，逐发生项保留重复回复。
真实 Arrow／Unix IPC、完整关系／原 sink／evaluator、乱序批次、1200/1201 预算、完整输入下
后置审计失败与 ACK 时钟已验证。历史 GPU 回执没有完整 token 数组，因此不授予其逐 token
一致资格。模型和作者 SQL 是软件 fixture，这不是新模型质量或性能。协调器另报告
`6a31cc38` 冷轮1200次及完整 sink 通过，按[隔离组合报告](../../../experiments/results/diagnostics/baseline_validated_integration_20261111/README.md)与本地组合 CPU 分列。旧 cold/warm 时长不得通过减估关闭来修复。

## Movie Q1–Q4 原版 Filter 接线

[单题入口](../../src/experiments/native_imlane_filters.py)的 Movie schema 复用现有 TINYINT
Arrow kernel、ModelService、provider、原版 DSO、物理预算和完整 sink ACK，不新增 controller
或修改作者 runtime／算法。使用[Q2 配置模板](../../configs/sembench_imlane_movie_filter.example.json)。
原 SemBench `c814e380` 拥有 CSV 输入、普通选择、逐题 prompt/parser、最终关系和 evaluator：
Q1 全 2000 个发生项后取前 5；Q2 对 taken_3 的 120 个发生项后取前 5；Q3 同 120 个计数；
Q4 同 120 个计算比例。Q2 引号、Q3 无末尾句点、Q4 有末尾句点保持原文。
普通选择位于原 SemBench source，选中的所有发生项进入作者 native Filter，不提前 head5。

每题独立新 source→完整原内存 sink，先记录 ready／cold 实际时钟再 ACK；审计、评价、
持久化和 owner close 在后，沿用上述单题入口。发生项使用完整 call_id，不按重复消息去重。
零模型检查用 `native_imlane_filters check`；首个目标门只选 Q2 全 120 行，不能自动放行
Q1 2000 次。既有 `run` CLI、host lease、进程/SHM/GPU 清理条件和原始 DSO hash 不变。
必须重链当前薄 driver，并核对当前 driver 源码和 kernel 身份；不重建 DSO 或安装替代 DuckDB。

[CPU 回放](../../tests/baselines/text/replay_imlane_movie_filters.py)验证 Q1 的 2000 条、Q2/Q4
cold/warm 各 120 条原 native 保存答复，以及 Q3 的 120 条 `168bc` LOTUS-compatible
project resident bridge 保存答复，共 2600 条。Q3 明确保留 bridge 来源，不冒充作者默认；
其旧 `3c80` native 尝试在 generate 前停止，不能声称已有答复。全部逐发生项检查原消息／
完整 CPU token、原 Filter 关系、head/count/ratio/evaluator、N+1 generate 前拒绝、乱序和
后置失败保留时钟。Q3 还与旧完整 prompt token 数组匹配；其他历史 GPU 的完整 token
资格不由 CPU 推导。76 项局部测试通过，空结果保留原 Q1/Q2 的三列 schema；共享 Medical
cold/warm 2400 条回放作为回归。这些 CPU 回放不授予 Linux DSO/SQL、目标模型或性能资格；
后续精确 `5c69f037` 的 Q2 目标验证另列如下。

### Movie Filter 原生 CLI 修复

`9507f9799` 的 Q2 目标尝试揭示薄 driver 尚有旧 `25/1200` 白名单：源准备通过，模型已
启动约 52.728 秒，但 CLI 在 SQL 前拒绝120，实际 generate=0、预算 consumed=0、无 sink。
旧失败和已领取预算保留；该启动时间不是查询时长。此前76项测试与保存回复回放未覆盖
真实 native CLI，不能作为 Linux SQL 资格，也不能沿用旧薄 driver。

修复删除白名单和默认25，保留必填正 int64、实际 source count／唯一 occurrence 与显式期望
一致的合同。`verified_build` 在任何 ModelService 启动前核当前 `driver_source` SHA；旧
二进制即使 kernel/DSO 不变也会前置拒绝。作者算法、DSO、kernel、预算及 sink 不变。

[编译执行测试](../../tests/baselines/text/test_imlane_filter_arguments.py) 直接从生产 cpp
取同一参数函数进行 C++ 编译／实际进程执行，覆盖25/120/1200/2000、非枚举7、非法／缺失／
重复／非ASCII／溢出参数，并验证旧 driver 在模型实例创建前拒绝。不是全文 driver/SQL 测试。
Mac 不支持该 Linux 原作者 DSO 链接；后续 Q1–Q4 已分别完成目标原生 SQL 门。
[Linux零模型门](../../tests/baselines/text/qualify_imlane_movie_filter.py)
保留真正 driver／Arrow／SQL／完整原 sink／ACK，只在既有 ModelService.spawn 注入
[软件服务](../../tests/baselines/text/imlane_filter_software_service.py)，禁止模型导入。
保存答复按 started receipt 的 batch_id、position、occurrence_id 关联，保留完整 token，
不按消息去重。先执行 Q2 cold 的全120条，再分别检查 missing-row／duplicate-id 在SQL
计数后、任何 IPC/generate 前失败。外层原 host lease 和进程／SHM 资源监督仍必需。

### Q2 的精确目标验证

实测源码为 `5c69f037cce5ffaca8f0c5012275c6242d31045d`，实测 code tree 为
`c7ecabffae680af039ba29a1286a9910bad2bda5`。Linux 新薄 driver 链接既有原版 `52bf5763`
DSO，源码／kernel 摘要和实际加载身份核对通过，未重建作者 DSO。零模型门完成完整120行
原生 SQL／Arrow／保存答复／原 sink，以及缺行、重复ID两项在任何 generate 前拒绝。

随后仅 Q2 完成一次真实冷轮：120个发生项、119个不同消息，重复消息对应的发生项仍分别执行；
120次请求全部成功，完整原消息与运行时渲染 token 数组逐发生项匹配。完整 Filter 先于原 head5，
原评价 precision／recall／F1 均为1；完整内存 sink 先于 ACK、审计、持久化和清理。
本轮 warm-up=0、整轮重试=0，进程、socket、SHM、GPU 空闲与租约释放均通过。

运行标识为 `imlane-movie-q2-5c69-1111-r1`；245件原件按该标识从运行交接获取，归档 SHA256 为
`73ba1236ae4325dd90092004247f2c46ad7922ce9a6da78bb8f62de6f600c4b4`。
`9507` 的零 generate 失败与后续私有 CPU guard 失败分别保留，不改成通过，也不并入成功轮。
这份 Q2 记录保持独立，Q1/Q3/Q4 的后续运行另列如下。本轮不支持 warm、重复稳定性
或跨系统性能排名；后续测试入口与文档同步不改变上述实测生产源码身份。

### Q1/Q3/Q4 的独立目标补充

三题均使用上列 `5c69f037` 生产源码和既有原版 `52bf5763` DSO；各自先完成完整原生 SQL
软件答复门，再独立启动新模型执行一次冷轮。Q1 为2000个发生项，Q3/Q4 各120个，实际请求
全部成功，完整原消息与运行时 token 数组逐发生项匹配；不按重复消息去重，不提前截取 sink。
各题均先形成完整原内存 sink，再 ACK／审计／持久化／关闭；进程、socket、SHM、GPU空闲
和租约释放通过。每题 warm-up=0、整轮重试=0，未据此认证 warm 或重复稳定性。

原评价保留实际结果：Q1 的 head5 precision／recall／F1 均为1；Q3 COUNT=7、MAPE=50%；
Q4 ratio=`0.058333333333333334`、MAPE=`50.00000021287373%`。后两题执行与接线完成，
质量偏差仍在，不能记作全题质量通过，也不改变原 parser／算法来修饰结果。
Q3 的旧 `168bc` project resident bridge 只供软件 CPU 答复引用；本次真实模型结果独立归属，
不冒充旧作者默认运行，也不把新模型实测记为 bridge 回放。

沿用[同一零模型门](../../tests/baselines/text/qualify_imlane_movie_filter.py)，由原配置取得
题号与完整数量，不再只允许 Q2／硬编码120。Q3/Q4 实际目标测试脚本为 `53a9671`，Q1 为
`5aafdd5`；后者仅让两份新完成且归本次测试拥有的 worker／software-service 回执按各自
文件大小读取；Q1 目标已使用该脚本完成。其他输入限制、生产代码与全局读取上限未改，
未增加 fixture 缓存、另一套入口或重新运行模型。

三题运行标识分别为 `imlane-movie-q1-5c69-1111-r1`、`imlane-movie-q3-5c69-1111-r1`、
`imlane-movie-q4-5c69-1111-r1`。359件原件按标识从运行交接获取，归档 SHA256 为
`f1dfadf3fac4e7e69803ace879225a550bb42bae5295560b62a69ce6899f6757`。
先前失败、私有 CPU guard 修正与各次运行保持原身份，不覆盖或合并成一份成功成绩。

## MMQA Q1 工程版接线

`0f562cf05eca1ad48d2db2fedeb0313f05356806` 的 `varchar-owned-engineering` 已在 Linux
通过 15 项真实 DSO／converter／PROCESS／THREAD 零模型检查；DSO SHA256 为
`5a172f77e9c46fae28f96251d20a022fda472749c1e858b2b2c25262eafa5fdd`。
作者 `072f8257` 的 4 过／11 失败记录不变，旧 schema-child 未构建。以下 Q1 是该工程变体的
新任务绑定，不继承其他系统或旧版本的模型成绩；不为不支持的图像等任务增加替代算法。

固定原 SemBench `c814e380` Q1：25 条 text occurrence 全部进入原 Extract formatter，
原 JSON 生成约束经 `GenerationSpec.kwargs()` 转为 vLLM `StructuredOutputsParams(json_object=True)`；
其余 generation 原样传入，`n=1`。作者 DuckDB 拥有预测图、batch 和 THREAD／PROCESS 调度；
薄 Arrow kernel 仅一次调用既有 IPC provider，VARCHAR 原样承载 JSON 文本，不作数字编码或预解析。
完整 native 投影按 call_id 还原，原 LOTUS parser／index updates／Title–title left join／
`Role == 'Bob Whitewood'`／director Series 和原 evaluator 保持。无模型答复去重或提前筛选。

使用[配置模板](../../configs/sembench_imlane_mmqa_q1.example.json)，真实路径和机器容量在仓外绑定。
`engine_options={}` 表示使用该原生服务的默认配置；实际容量／缓存从 owner ready 回执读取，
不能把模板当作目标环境已一致。既有 Linux DSO 不重建，仅编译新薄 driver 并做实际动态链接探针：

```sh
PYTHONPATH=code "$DRIVER_PYTHON" -B code/integrations/imlane_duckdb/build.py sembench-driver \
  --root "$IMLANE_AUTHOR_ROOT" --duckdb-source "$OWNED_DUCKDB_SOURCE" \
  --duckdb-build "$OWNED_DUCKDB_BUILD" --duckdb-sha256 "$VERIFIED_DSO_SHA256" \
  --output "$FRESH_DRIVER"

PYTHONPATH=code "$DRIVER_PYTHON" -B -m src.experiments.native_imlane_sembench check \
  --config "$Q1_CONFIG" --output "$FRESH_CHECK"
```

模型运行仅由既有唯一服务器协调者持有 host lease／资源监督后执行：

```sh
PYTHONPATH=code "$DRIVER_PYTHON" -B -m src.experiments.native_imlane_sembench run \
  --config "$Q1_CONFIG" --output "$FRESH_RUN" --build-record "$FRESH_DRIVER.build.json" \
  --author-root "$IMLANE_AUTHOR_ROOT" --method-python "$IMLANE_METHOD_PYTHON" \
  --kind thread --execute-model
```

本批仅一冷轮，最多 25 次原生 `generate`，额外预热／自动重试为 0。永久 SQLite 单元先整体
保留 25，再由模型 worker 在每次实际 `generate` 前消费；第 26 次拒绝，失败／未知不退款。
发生项和响应按完整消息身份、批内位置及 native call_id 核对，不按异步到达次序配答案。
25 个冻结 prompt 全部不同；其他输入若破坏该相关性前提会显式拒绝。

两套真实时钟分别保留：`ready_source_to_sink_seconds`（兼容字段 `source_to_sink_seconds`）
从模型 ready 后的新鲜 CSV／prompt/token 准备开始，包含必需输入／关系传递、原 parser 和完整内存
sink；`cold_worker_entry_to_sink_seconds` 还包含 worker 预检和模型启动。外层 `timing.json` 再从
既有 supervisor 的 Popen 前 `started_monotonic` 计算含启动的冷总区间。严格 host controller
入口未知；不可将 runner 启动边界当作包含 host 预检／取租约。比较时选择双方相同的实际起点，
不只选择较短值，也不从旧时长减估启动。回执审计、评价、结果持久化和最后关闭都在 sink 后；
第一必要 native 输入／结果 IPC 仍在查询内，并非零 I/O。

完整查询与启动共用 1200 秒上限，sink／失败后关闭最多 120 秒；既有监督器的 8 秒 grace 包含
在其 1320 秒总范围内，不额外相加。driver 的 ACK 等待最多 120 秒，外层仍可按原期限终止。
Q1 结果先保存为原 Series，持久化时转成单列关系，不改变内存查询语义。

本机真实 Unix IPC／Arrow 的 25 条保存答复回放验证消息、完整 token、每源原文、完整 Extract 关系、
原 sink／evaluator 和 25/26 预算；模型与作者 SQL 是软件替身。协调器另报告 `73ee255e` 的
目标25次及完整 sink 完成，准确来源见[隔离组合报告](../../../experiments/results/diagnostics/baseline_validated_integration_20261111/README.md)，不继承到本组合树。Mac 缺原作者 Boost 头，
不通过安装普通 DuckDB 或重建已验 DSO 绕过。旧参考 F1 不记作本次 IMLane 模型质量。

## 外层运行资源与回执

资源观察覆盖模型与作者全过程，不能把作者默认lane／queue分配当总SHM上限。
实际3c模型启动后漏计4096B的原失败及[控制器补丁](../../../experiments/results/diagnostics/imlane_duckdb_q3_20261106/README.md#真实模型启动后的shm范围错误与控制器修复)
已保留；修复按启动时/dev/shm物理可用空间设置观察边界，保留用量／残留统计，
不设逐模型页配额，不改原生引擎／作者算法。初始与终态ledger使用不同新文件，
失败的回执保存不能跳过身份校验的lease释放。此修复未重跑模型。

## Movie Q9：原版 DOUBLE Map 的唯一运行入口

维护入口为 `PYTHONPATH=code python -m src.experiments.native_imlane_movie_scores
--config PRIVATE_CONFIG --output FRESH_PRIVATE_OUTPUT`（在同一行执行）。
外层继续负责主机 lease、资源 preflight、进程监督及永久预算的创建/独占 claim；入口不另建调度器。
仅单次 cold256、warmup0、retry0。输入256个occurrence/128个reviewId，保留重复和原顺序；不排序、不做Q10平均。

- [q9.cpp](q9.cpp)：原SQL DOUBLE投影，在完整关系sink完成的ACK前持有结果。
- [q9_udf.py](q9_udf.py)：原数字解析及Q9专属Arrow/消息边界，调用既有provider。
- [sembench_movie_q9.py](../../src/baselines/text/sembench_movie_q9.py)：原题source/消息与计时后的receipt审计。
- [native_imlane_movie_scores.py](../../src/experiments/native_imlane_movie_scores.py)：唯一正式CLI，调用已有完整sink、owner和永久预算。

配置沿用模型配置、sampling_params、data_dir、sembench_checkout、model_name、author_root、method_python、
request_timeout_s、query_seconds、cleanup_seconds、run_id、provider_mode，另外显式提供
input_identity（私有原输入与消息SHA清单路径）、build_record（薄driver构建记录）、request_budget。
request_budget为既有已claim描述符：path、budget_id、limit=256、unit_id。非新鲜单位拒绝，模型owner每次generate前永久扣费。
机器路径、原数据/token参考及结果不进入源码。运行时要求原DSO
`52bf5763aab6f35b76746a9fc15b75e27bac1534315d1a20926eb4a78c2badaf`。

薄driver构建用 `build.py movie-scores-driver --root AUTHOR_ROOT --duckdb-source DUCKDB_SOURCE
--duckdb-build DUCKDB_BUILD --output FRESH_DRIVER`（同一行）；只链接既有原DSO，不重新构建作者库。
原source开始到完整内存sink、service-start到sink、model-ready到sink分别直接观测；ACK后driver退出、审计和关闭仍须成功。
这些观测不由历史耗时相减推算。

本机复核入口为[Arrow/IPC回放](../../tests/baselines/text/replay_imlane_movie_scores.py)和
[消息/token回放](../../tests/baselines/text/replay_imlane_movie_score_tokens.py)，通过环境变量SEMLOOM_Q9_FIXTURE指定私有冻结参考。
UnixIPC回放使用软件engine；token回放使用本地原tokenizer，不声称模型性能。
私有v4是已执行历史证据，不再作为新包的生产worker来源；未来包必须调用此维护CLI。
迁移后的本机测试不替代Linux完整CLI/DSO资格，旧v4实测也不自动成为新CLI的目标实测。

Q9构建记录绑定当前q9.cpp、q9_udf.py、relation_handoff.hpp以及实际binary/DSO字节；入口拒绝旧源码构建和被修改的产物。

维护源码 `4c08b57b9a2e54768c8c475226c870f5a0a950e0` 的独立Linux零模型门已通过并完成本机原件核验：
正式CLI执行完整SQL256，完整关系／逐occurrence／永久预算256／sink→ACK→退出及资源清理通过；
实际存活driver加载原52bf DSO，三个stage退出成功，SHM不变且lease释放。运行token是软件placeholder，
不授予新GPU token或性能资格。私有v4冻结包的真实模型256次仍只归原包，不作为本CLI新增GPU结果。
1173份原件包 `imlane-q9-maintained-4c08-1112-r1-evidence.tar.gz` 的SHA256为
`fe608fe2254d25c9241bb90b1984b085067c9244200044ffae94ab5055b2521a`，汇总见仓库外
`imlane-q9-maintained-linux-gate-20261112/RESULT.json`。当前Medical＋Q9组合只有本机回归和源码适用性
核对，未作整体目标／GPU执行；各组件的原失败证据与独立成绩保持原身份。

### Filter 源规模与构建身份

`native_imlane_filters.configuration` 不再把 Movie/MMQA/Medical 的历史 2000/120/25/1200 行数当作
执行能力白名单。显式 `max_model_calls` 仍由现有 `AttemptBudget` 验证为正整数；
`prepare_query` 必须从冻结 SHA 对应的原题源码读取完整 occurrence，并验证数量等于该预算，
不截断、不补行、不重试。支持题号、原 8192-token 生成参数及 Medical scale 11112 保持。
接受其他输入规模不授权额外运行，目标运行仍只使用已批准的冻结配置和持久预算。

`prompt_columns` 保存原 LOTUS 观察到的源列顺序，而非渲染 header。原 `StageSpec.bind`
已经从原题 instruction 解析列名，准备阶段核对相同列集合，再应用显式冻结顺序；
不按展示大小写映射、不排序、不跨进程重新推断顺序。当前入口仍要求先记录该原始顺序，
未新增配置生成框架。历史 Q3a 冻结包与证据不改。

prepared Q1 和 Filter 构建记录现在同时绑定当前 driver 源码、kernel 与编入的
`relation_handoff.hpp`，在 model owner 启动前验证。旧构建缺字段时明确要求重链薄 driver；
不将旧 binary 认定为损坏，不回写历史成绩，也不因此要求重跑所有 GPU 查询。
CPU 边界回归见 `tests.experiments.test_imlane_build_binding`、
`tests.experiments.test_native_imlane_filters` 和原源码 suite 的
`test_imlane_filter_budget_binds_actual_original_source_not_fixed_sizes`。

本轮限定 CPU 回归 33 项通过、0 跳过，包括原 SemBench c814 的 11 行 Movie Q1 源码与
LOTUS formatter/预算一致性。tokenizer 为软件替身，build/DSO 为不执行的身份 fixture；
真实 CLI 参数解析和构建身份检查执行。未运行 native Linux SQL、模型或 GPU，不授予目标资格。
