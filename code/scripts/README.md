# SemLoom 脚本入口

## 原生语义系统单查询

`baselines/run_semantic_system_query.py`调用LOTUS原生`sem_map`、Daft内置`prompt`、
DuckDB社区AI函数或Sema作者二进制；先核对[具体方案](../../experiments/plans/语义系统对照.md)、环境与已授权清单。
原始输入关系由既有数据库输入工具预先安装，连接串来自`SEMLOOM_QUERY_DSN`。
示例中的文件均为仓库外已准备资产，预算须已创建，输出目录须尚不存在：

```sh
PYTHONPATH=code python code/scripts/baselines/run_semantic_system_query.py \
  --role lotus-map --manifest /path/to/private/manifest.json --table semantic_inputs \
  --model /path/to/private/model.json --budget /path/to/private/budget.sqlite \
  --budget-id authorized-comparison --max-attempts 100 --unit-id unique-query \
  --concurrency 4 --tokenizer /path/to/model/tokenizer.json \
  --output /path/to/new-private-query
```

其他`--role`值为`daft-prompt`、`duckdb-ai`、`sema-map`；Sema另需`--sema-binary`，
`--num-threads`控制其原生线程或Daft runner，实际HTTP峰值单独记录。
本入口只执行一个查询；模型服务、PG、共享累计预算、CPU设置与重复清单由调用方管理。
原生SDK与透明代理可能同时保留多组连接，入口先检查`3×源行数+128`的保守文件描述符额度。
检查只读，额度由调用方为所属进程准备；512行补测使用8192，具体环境与失败见[主报告](../../experiments/results/postgresql/semantic_system_comparison_20261008/README.md)。
完整执行耗时含原生库准备及输入装配；模型启动和事后质量评价另列，不自动开始下一查询。

<a id="native-adapter-query"></a>

## 原生方法与SemLoom成对查询

`baselines/run_native_adapter_query.py`执行有限外部文本输入的单查询，输入JSONL每行仅含唯一`row_id`和原始`text`。
`--arm`包括共同固定Map的原生Daft／Ray／SemLoom、LOTUS单Map与两Map三种推进、DuckDB成对执行和Sema三种请求路径。
具体名字与适用范围见[接口说明](../src/experiments/postgresql/README.md#native-adapter-query)及[工程报告](../../experiments/results/postgresql/native_adapter_integration_20261009/README.md)。
预算须已创建；配置与输出均在仓库外，输出目录须尚不存在：

```sh
PYTHONPATH=code python code/scripts/baselines/run_native_adapter_query.py \
  --arm fixed-map-semloom --input /path/to/private/raw-input.jsonl \
  --plan /path/to/private/plan.json --model /path/to/private/model.json \
  --budget /path/to/private/budget.sqlite --budget-id authorized-comparison \
  --max-attempts 100 --unit-id unique-query --options /path/to/private/native-options.json \
  --ray-physical /path/to/private/ray-map.json --ray-temp-root /path/to/private/ray-scratch \
  --references /path/to/private/reference-outputs.json --allowed-output true --allowed-output false \
  --output /path/to/new-private-query
```

两Map另传`--stages`，Sema传已核对的`--sema-binary`，DuckDB传已编译的`--duckdb-library`；LOTUS可传`--tokenizer`。
SemLoom主路径要求Daft payload与真实Ray执行，完整合法任务必须装入所声明窗口；本地诊断另有独立名字。
模型POST由共同观察代理逐次持久计数，worker身份标签只用于观察，不重复计费；失败与清理摘要保留。
调用方管理模型服务、整体期限、CPU与完整运行清单。本入口不自动启动服务或下一查询。

`baselines/run_persistent_native_adapter_query.py`接收明确的`--schedule`列表，在一个常驻进程中顺序执行查询。
每项包含`arm`、唯一`unit_id`、原始JSONL的`input`和`phase`；`phase`为`qualification`、`warmup`或`measurement`，
可另给该输入的`references`文件。保持一个arm的清单可使用独立Ray集群；一次最多三个arm的共享部署另记实际可用CPU。
计划、模型、累计账本和原生参数仍由调用方提供，Ray临时目录须新建且路径短。

```sh
PYTHONPATH=code python3 code/scripts/baselines/run_persistent_native_adapter_query.py \
  --schedule /path/to/private/arm-schedule.json \
  --plan /path/to/private/plan.json --model /path/to/private/model.json \
  --budget /path/to/private/budget.sqlite --budget-id authorized-comparison \
  --max-attempts 100 --options /path/to/private/native-options.json \
  --ray-physical /path/to/private/ray-map.json --ray-temp-root /path/to/new-short-ray-root \
  --tokenizer /path/to/private/tokenizer.json --output /path/to/new-private-arm-output
```

`lotus-method-semloom-local-diagnostic`使用同一个Core和本地完整响应传输，`physical=None`，没有Daft／Ray。
Ray集群、SemLoom执行实例及LOTUS LM跨查询保留；当前查询的输入读取、格式化、图构建、物化和消费继续计时。
每查询独立归还结果／任务流和账本单元，首次错误停止后续查询；最终owner统一关闭组件。
`startup.json`记录启动，`query-<序号>/persistent-query.json`分别给出release、实际提交和EOF，
以SHA-256对应原`summary.json`；同目录`runtime-resources.json`保存查询前后Ray额度，warm-up标签不进入测量分组。
完整接口和已验证／待验证部分见[常驻检查](../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#persistent-fixture)。

## 数据就绪后的查询计时

`PYTHONPATH=code python -m src.experiments.postgresql.ready_semantic_query`接收
`--config --manifest --model --budget --budget-id --max-attempts --output`；连接串仍从环境变量读取。
配置支持PG、PG-source direct、原生Ray Data和Daft Native Map；PG还必须提供`--pg-log`。
调用方拥有Ray集群时在配置中给出`ray_address`；自建原生Ray运行时可提供仓库外`--ray-temp-root`。
入口要求已安装原始输入表和新输出目录，准备可复用组件后记录SELECT／原生API入口至消费结束。
SQL扫描、原生reader连接、提示与查询图构造保留在查询内；准备零模型调用由统一代理核对。
原生系统在上方单查询命令中增加`--timing-mode ready`，先加载原始输入并准备组件，默认仍为`application`。
调用方负责单元进程期限与模型服务；原生API的请求超时、取消和停止范围见[接口说明](../src/experiments/postgresql/README.md#ready-query-timing)。

## gateway多查询干扰诊断

`profiling/gateway_isolation_probe.py --output /path/to/new-private-probe`运行真实UDS登记/消息槽、共享Core与Map循环，
用厂商表、API和独立worker替身检查状态准备、payload准备、慢消费与同步observer干扰。
固定40个case、1,088次模拟调用，单次20秒、整体5分钟，首次错误保留并停；HTTP、模型与PG均为0。
使用现有Python标准库，设置`PYTHONPATH=code`；输出在Git外且尚不存在，不安装依赖。
`--replay /path/to/restored-suite --output /path/to/new-private-analysis`从原始事件复算主指标。
kernel peer为fixture，真实平台/SQL收益不据此推断；[主报告](../../experiments/results/diagnostics/gateway_isolation_20261004/README.md)保存完整设置和取舍。

## 同机共享预付计数原型

`profiling/mapped_request_probe.py --output /path/to/new-private-probe`用现有SQLite单元预付与本地共享映射，
比较1／4进程逐条领取；固定16,384次组件领取、每次30秒及整体5分钟，首次错误保存并停。
`profiling/mapped_core_probe.py`以同样的`--output`参数比较真实Core／Map循环中的观察方式，
厂商表和worker仍为替身，固定8,224次模拟调用、每次60秒及整体5分钟。两者HTTP／模型／PG均为0。
设置`PYTHONPATH=code`，输出在Git外且尚不存在；不安装依赖、不运行厂商SDK。
候选仅接本地诊断API，正式查询配置与计费不变，实际范围及后续服务器条件见[报告](../../experiments/results/diagnostics/mapped_request_budget_20261003/README.md)。
2026-10-04已补观察器的descriptor调用范围与Linux/Ray无模型检查；这是程序API，没有新增正式CLI计数模式。
实际命令、原始源码及有限配置保存在[服务器补充](../../experiments/results/diagnostics/mapped_request_budget_20261003/README.md#server-observer)，
完整消费未获净收益，默认保持。共享服务器归档的只读复算入口为
`experiments/results/diagnostics/gateway_isolation_20261004/raw/analyze_server.py <archive> <new-output>`。

## 本地Core／Map观察成本诊断

`profiling/map_observation_probe.py --output /path/to/new-private-probe`运行真实Core、异步backend和Map传输循环，
用有限表与独立线程worker替身比较持久／内存计数和内存事件／`BufferedEvents`投影写入。
固定20次执行、16,448次模拟调用、每次60秒与整体5分钟，首次错误保存后停止；HTTP、PG与模型均为0。
只用现有Python标准库，不运行Ray/Daft/Arrow、不安装依赖；设置`PYTHONPATH=code`，输出须在Git外且尚不存在。
最终计数、全部事件与原始账本保留，范围和取舍见[主报告](../../experiments/results/diagnostics/map_observation_isolation_20261003/README.md)。
内存臂仅作诊断，真实计费与生产默认保持。

## 本地持久记账组件对照

`profiling/request_budget_batch_probe.py --output /path/to/new-private-probe`比较逐行与4/16/64条每事务。
使用现有Python标准库及新建仓库外SQLite，固定16,384条组件预留、每次60秒及整体5分钟；
无HTTP、PG或模型调用，原件与首次失败保留，已有目录拒绝覆盖。运行时设置`PYTHONPATH=code`。
身份、全部重复和限制见[结果报告](../../experiments/results/diagnostics/request_budget_batch_20261003/README.md)。

`QueryConfig.remote_budget_mode=batched`及observer的`--remote-budget-mode batched`仅用于已声明Ray Map传输的实验查询。
CLI同时要求shared cell budget；批量事务最多16条已就绪记录，默认同步保持。`remote_request_batch`单列事务时间，
逐行`remote_request_guard`只报告等待与完成身份，不能将同一个批量事务重复累加到各行。
真实模型对照未取得完整查询收益，该参数仅作显式诊断，不替换同步默认。

## 实验结果恢复

历史结果的`raw/storage-manifest.jsonl`使用`semloom.evidence_storage.v2`时，清单把每个原始路径
映射到直接保留的文件或共享归档成员。相同字节只存一份，原始运行身份和目录关系仍由路径及原元数据记录。
归档可能被其他结果引用，取用时保留完整仓库；原有可直接阅读的数据和脚本仍留在报告所列位置。

读取归档数据或运行依赖原始路径的旧分析脚本前，先把清单登记的数据与脚本恢复到新建的仓库外目录：

```sh
PYTHONPATH=code python3 code/scripts/analysis/restore_result_evidence.py \
  experiments/results/data_organization_comparison/raw/storage-manifest.jsonl \
  --output /path/to/new-private-result
```

该入口使用Python标准库；输出原始路径、权限及字节，逐项核对字节数、SHA-256和清单总数。
持续维护的Markdown报告与说明保留在仓库阅读，不复制为第二套报告。
输出必须尚不存在且位于Git目录外；坏归档、校验不符、重复或跨目录写入路径会在创建输出前拒绝。
这项操作只恢复已有文件，模型请求为0；其他存储版本继续使用对应报告的恢复方式。

跨机器环境入口：`environment/manage_environment.py`。它按
`deploy/runtime/profiles/*.json` 与 `deploy/runtime/assets.json` 只读检查机器、Python
能力、模型和数据；安装/下载是独立显式子命令。它不导入 PostgreSQL workload，也不
替代各实验 runner 的正确性门禁。完整流程见 `deploy/runtime/README.md`。

## 普通 PG 结果发送缓冲诊断

`experiments/pg_result_buffer_probe.py --describe` 只输出配置，不连接数据库。
实际运行需先核对隔离PG18.3与runtime preflight，再设置外部`SEMLOOM_TEST_PG_DSN`并指定新的产物目录：

```bash
PYTHONPATH=code python3 code/scripts/experiments/pg_result_buffer_probe.py --dsn-env SEMLOOM_TEST_PG_DSN --output /path/to/new-buffer-probe
```

128行、每行5ms，8/128/128/8字节有效载荷，四条查询各15秒statement timeout；无模型调用。
使用scalar临时函数的逐行服务端标记和客户端接收时间；不使用会全量收集的PL/pgSQL返回集合。
PG18.3四次普通SQL诊断已完成；每次查询独立写checkpoint，结束时只创建一次最终summary。
[服务器结果、首轮写入失败与修订](../../experiments/results/scheduling/capacity_wait_server_20260914/README.md)。

## 文件定位

当前数据库原始输入与公共查询入口见[下节](#数据库原始输入与公共查询)，包含准备、安装和有期限的单查询执行。
PG Map查询配置支持`pg_total_budget=true`与独立`pg_staging_bytes`，由总字节控制实际留存行数；
逐算子内存观测及真实诊断见[总字节预算报告](../../experiments/results/postgresql/pg_window_budget_20260910/README.md)。
PG Map还可提供`organization_config`与该文件的`organization_sha256`，选择下述token工作量组织配置；
实际消息、token usage、候选前缀、分组及出站顺序均由查询评价器核对。

脚本按职责分为七组：

| 子目录 | 只负责 |
|---|---|
| `data/` | workload 导入 |
| `services/` | 本地调试服务与受审计的模型服务启动器 |
| `baselines/` | 原生 baseline/gate 薄入口 |
| `profiling/` | 数据链路画像与机制诊断 |
| `experiments/` | 场景、矩阵和多 job 正式编排 |
| `analysis/` | 离线汇总、代价估计和 calibration 选择 |
| `environment/` | 机器、依赖、资产与仓库安全检查 |

入口脚本只解析参数并调用 `src/`；不得因为移动目录而复制生产逻辑。历史结果目录里的
raw manifest 保留执行时旧路径作为不可变证据，README 中的复现命令使用当前新路径。

## 数据库原始输入与公共查询

当前系统比较见[四路径计划](../../experiments/plans/completed/文本Map对比_20260930.md)。
主表选择 `profile=main`（SemLoom Daft/Ray、direct、原生Ray、原生Daft），
本地执行消融选择 `local-ablation`；旧记录默认 `legacy-four-paths`，评价不能更换profile。
`python3 -m src.experiments.postgresql.text_map_candidates --table TABLE --rows 512
--ray-address ADDRESS --transport-path FILE --transport-sha256 SHA` 生成12组主表候选；
整条命令在一行执行。该输出不是可执行阶段，也不会创建额度或启动服务。
原生Daft是官方SQL reader与异步batch UDF执行图，工作函数只调用同语义HTTP，不称内置prompt。

单阶段执行入口为 `python3 -m src.experiments.postgresql.text_map_campaign SPECIFICATION`，
加 `--preflight` 只检查本地文件和清单，不连接数据库或模型。执行使用已有查询监督器，要求外部准备好的
服务、不可变源表、数据库环境变量和全新有限账本；文件采用 `semloom.text_map_campaign.v1`。
候选、轮次顺序、输入/安装/环境/模型摘要、服务签名、POST 总数和最长时间必须明确。
失败保留记录并停止；下一阶段由已授权的外层清单显式启动。
每次worker结束后，实际配置摘要和查询编号必须与下发候选相同，首次不一致即停止。
PG单查询另存`gateway-preparation.json`；gateway库加载及Ray首批处理观测见
[查询入口说明](../src/experiments/postgresql/README.md)，真实阶段时间尚待目标环境采集。
原生Ray的 `ray_async_batches_per_actor` 默认1；actor数×异步批次数不得超过HTTP允许上限。
批行数不等于HTTP并发，读取块数量也会影响实际供给；以请求轨迹确认，不只看配置值。
`database_queries.py run` 的原生 Ray 配置可设置 `ray_address="127.0.0.1:6379"` 连接已启动的
单节点服务，同时省略 `--ray-temp-root`；服务的实际 CPU、GPU 与对象存储总额度必须匹配配置。
PG＋SemLoom Ray 继续使用独立传输文件；该参数不向原生系统注入 SemLoom 策略。

完成查询后的离线汇总：

```sh
PYTHONPATH=code python3 -m src.experiments.postgresql.text_map_comparison \
  /path/to/comparison.json --output /path/to/new-selection.json
```

输入 JSON 使用 `schema=semloom.text_map_comparison.v1`、`stage=tuning` 或 `evaluation`、
`repeats`（3–30）以及 `candidates` 列表。每个候选给出唯一 `id`、`role`、`warmup` 与 `measured`；
两种记录列表的每一项均为 `{"path":"/path/to/unit/summary.json","sha256":"<SHA256>"}`。
四个角色为 `pg-http`、`pg-daft-ray`、`pg-source-direct`、`ray-data`。至少一次预热，测量次数等于 repeats，
所有候选完整保留；相同中位数按候选声明顺序选择。评价另给 `tuning_result`，同样使用 path/sha256，
且每个角色只能使用一个已选配置。输入与输出放仓库外，输出不覆盖已有文件。
此工具只检查已有证据；不启动查询，不推定容量平台、质量等价或数据无重叠。

M1 的方法实验入口使用[显式阶段配置示例](../configs/m1_screening.example.json)，先执行只读检查：

```sh
PYTHONPATH=code python -m src.experiments.postgresql.m1_campaign /path/to/stage.json --preflight
```

示例仅演示 128 行 fixture、6 组×4 轮的结构；数字不是下一轮真实配置或额度。缺少真实文件时拒绝，
不能原样执行。替换输入 manifest SHA 后，`max_posts` 必须等于该 split 行数×组数×轮次（含预热）；
所有组使用同一 `resources`，`orders` 每轮必须各包含全部组一次，判定参数在 `selection_policy` 中显式给出。
`screening` 只做请求数和路径筛查；`strategy-tuning` 单独调 W；`evaluation` 使用独立输入；
`observation` 才允许混合日志模式。旧无 schema 的 44,544 次矩阵在访问 PG 前被拒绝。

token 组另提供 `work_limit`；顶层提供固定 `context_tokens`、`model_revision`、`tokenizer_path`、
`tokenizer_fingerprint` 及各 split 的 `work_sha256`。分词身份与预计算 work 来自既有准备，预检不加载模型。
非筛查阶段给出 `reference_group`；工作量调参对请求数参照，所有 token 组保持同一保护 C。
调参另需 `screening_source`/`screening_sha256` 指向已完成的同签名、同存储筛查报告；PG 须有平台候选，
请求数参照采用其选定 C，token 的保护 C 来自该筛查，并同时保留该 C 的 request 对照。
独立评价/观测还要求 `selection_source` 与 `selection_sha256`，指向调参后审阅保存的 JSON：
`status=ready_for_independent_evaluation`，其 `evaluation_design` 完整包含选定 `groups`、`resources`、
`selection_policy`、`service_signature`、`manifest_sha256`。保留调参报告引用和选择理由；不从评价结果生成该文件。

真实恢复时才去掉 `--preflight`。调用者提供已运行的隔离 PG/模型、已有且未使用的阶段账本、有限期限和外部进程监督；
本模块不启动服务、不初始化额度。它检查账本剩余期限不超过声明阶段时长，每查询仍有期限；
外部监督负责卡在准备或清理中的进程。阶段结束不启动下一阶段，失败保留证据且不重试。
[研究问题、选点与解释规则](../../experiments/plans/数据组织.md#m1-throughput-platform)。


`experiments/database_queries.py`提供`prepare-movie`、`prepare-squad`、`install`、`run`。
完整输入、标签、模型配置、额度SQLite和输出目录均放仓库外；连接串只从环境变量读取。
源表安装独立计时；查询时间包含PG源读取、消息构造和原生SQL规划/探测。

```sh
PYTHONPATH=code python3 code/scripts/experiments/database_queries.py prepare-movie \
  --csv /path/to/Reviews.csv --sembench-checkout /path/to/pinned-sembench \
  --provenance /path/to/source-provenance.json --max-rows 2000 --output /path/to/new-prepared
PYTHONPATH=code python3 code/scripts/experiments/database_queries.py prepare-squad \
  --source /path/to/dev-v1.1.json --selection /path/to/qualified-selection.json \
  --split tuning --max-rows 2000 --output /path/to/new-squad-prepared
PYTHONPATH=code python3 code/scripts/experiments/database_queries.py install \
  --manifest /path/to/new-prepared/manifest.json --table query_reviews \
  --dsn-env SEMLOOM_QUERY_DSN --output /path/to/new-installation
PYTHONPATH=code python3 code/scripts/experiments/database_queries.py run \
  --config /path/to/query.json --manifest /path/to/new-prepared/manifest.json \
  --model /path/to/fixed-model.json --budget /path/to/authorized-budget.sqlite \
  --budget-id authorized-query-campaign --max-attempts 2000 \
  --dsn-env SEMLOOM_QUERY_DSN --pg-log /path/to/pg.log --output /path/to/new-short-output
```

最后一条仅为参数示例，不授予模型运行额度。`run`不创建额度账本，不退款或自动重试；实际上限必须
与已有授权账本完全一致。最小query.json为
`{"unit_id":"map-pg-1","arm":"pg","task":"map","table":"query_reviews","max_posts":2000}`。
arm可取pg、pg-source-direct、ray-data、lotus；task为map或movie-q1/q2/q3。direct/Ray只接Map，
LOTUS只接原Movie。C、window、字节和期限使用`QueryConfig`内显式字段，正式运行前另行选定配置。
PG须加`--pg-log`；Movie须加`--sembench-checkout`，LOTUS可加`--tokenizer`；Ray须加新的短
`--ray-temp-root`。Ray runtime由该单元独占，禁止连接现有共享runtime。

Movie schema v2分别存执行行号与原始reviewId，重复原始ID不去重；原任务评价仍用原字段。
`run`启动独立worker并保存`unit/`和`supervisor.json`；准备期限120秒、query使用配置期限，
结束后评价/清理期限120秒，TERM/KILL清理间隔10秒。worker失败保留部分输出并退出非零。
原生数据物化、语义差异、当前测试与真实验证待执行项见[报告](../../experiments/results/postgresql/database_queries_20260910/README.md)。

## SQuAD PG Map 数据准备与离线评价

`baselines/squad_pg_map_pilot.py` 只准备输入或评价已有结果，不连接数据库、不发送模型请求。
复用原 SQuAD importer 的官方文件哈希/行数检查与解析器，保留旧 workload 的单消息语义；
新身份 `squad_v11_pg_map_v1` 明确采用 Map 的 system/user 双消息。按完整 context 分组后，
生成互不重叠的 tuning/evaluation 子集、`manifest.json` 与只有 ID/input_text 的两份 CSV。
真实 source/model/SQL 资格与额度见[当前计划](../../experiments/plans/archive/数据组织历史方案_20260927.md#当前-pg-单-map-数据执行切片)。

```sh
python3 code/scripts/baselines/squad_pg_map_pilot.py prepare \
  --source /path/to/dev-v1.1.json --rows-per-split 64 --output-dir /path/to/new-output
python3 code/scripts/baselines/squad_pg_map_pilot.py evaluate \
  --manifest /path/to/new-output/manifest.json --split tuning \
  --predictions /path/to/predictions.jsonl --output /path/to/new-quality.json
```

预测 JSONL 每行为 `{"source_example_id":"...","prediction":"..."}`；失败可使用 `null`。
重复/未知 ID 和清单内容变化会被拒绝；缺失/NULL 仍进入 EM/F1 分母，退出码为 1。
EM/F1 为答案完全匹配率与词重叠 F1，均采用百分数；完整关联不表示答案正确。
输出目录/报告已存在则拒绝覆盖。完整数据与预测留在仓库外；CSV 只用于明确有行数上限的小样本，
不能由此声称大规模流式输入或有界客户端内存已经实现。

准备目录现在必须位于 Git checkout 之外，目录权限为 0700，私有文件为 0600；写入后重新读取
JSON/CSV 核对完整文本。公开证据脱敏不能用于准备输入。

`baselines/map_workload_tools.py` 提供 `sharegpt`、`tokens`、`summary` 三个离线子命令。
ShareGPT 只保留首个 human 的原文，不自动选择任务、裁剪或规范化空白；选样范围是 UTF-8 字节数。
源文件 SHA256 必须事先核对，最多读取 1 GiB JSON 文件，属于有规模上限的准备工具。

```sh
python3 code/scripts/baselines/map_workload_tools.py sharegpt \
  --source /path/to/source.json --source-sha256 "$SOURCE_SHA256" \
  --count 16 --minimum-bytes 256 --maximum-bytes 2048 --output-dir /path/to/new-private-workload
python3 code/scripts/baselines/map_workload_tools.py tokens \
  --manifest /path/to/new-private-workload/manifest.json \
  --instruction-file /path/to/instruction.txt --output-tokens 128 \
  --tokenizer /path/to/local-tokenizer --model-id "$MODEL_ID" --model-revision "$MODEL_REVISION" \
  --context-tokens 4096 --output /path/to/new-private-workload/context.json
python3 code/scripts/baselines/map_workload_tools.py summary \
  --manifest /path/to/new-private-workload/manifest.json --output /path/to/public-summary.json
```

这些参数仅演示接口，不代表 ShareGPT 的提示或预算已验证。`tokens` 要求已有本地 Transformers
和 tokenizer，禁止下载；SQuAD 改用 `--split tuning` 或 `evaluation`，从清单读取指令与预算。
计数使用完整 chat template 的 token IDs，任何行的输入 tokens 加输出预算超出上下文时退出 1。
报告记录实际模板与 tokenizer 文件哈希；调用者声明的 revision 仍须与真实服务核对。
`summary` 只输出独立 schema 的计数与哈希，不能作为 workload 重新读入。

查询记录 API 位于 `src/experiments/postgresql/map_query_recording.py`。`record_pg_query` 接收
调用者已有的 connection、SQL、新私有目录和结果行数/字节上限；调用者负责有限 statement timeout、
请求账本、事务和服务生命周期。先保存 `started.json`，消费时追加 `results.jsonl`，成功或异常均
尝试保存 `execution.json`，之后才写独立 `evaluation.json`。异常继续抛出，部分结果不可当作成功结果。
记录包含写盘开销；磁盘故障/进程被强制终止时无法保证终点落盘，也不因此证明客户端或 PG RSS 有界。
`verify_text_roundtrip` 可核对准备值与 PG 读回值；`verify_map_completions` 需要实际 producer 序号，
按 ID、payload digest、输出、模型与 stop 检查，不从无 ORDER BY 的 SQL 推断输入顺序。

`choice_gateway_observer` 的请求、完成与核心事件新增 `monotonic_ns`（观测时单机单调时钟，纳秒），
可同本机 SQL 计时关联；它不是服务内部阶段时钟，也不能跨机器直接相减。

观测器可用 `--private-events /path/to/new-private-events.jsonl` 另存完整原文。`--events` 仍为
脱敏事件，采用字段值脱敏后 JSON 序列化；公开前仍需隐私审查，脱敏器不保证移除任意个人信息。
可选 `--expected-request-hashes` 接收 `expected_requests.expected_request_manifest(bodies)` 生成的
完整请求值哈希及次数清单，必须配合已有 durable ledger。JSON 转义/字段顺序不影响值哈希；文本、
生成选项或次数漂移在 HTTP 发送前拒绝。拒绝仍消耗已预留的 1 次账本额度，真实 POST 为 0，不退款或重试。
退出时还有预期请求未出现则返回 1。哈希清单应由独立核对的预期消息构建；它不替代源数据/PG 读回检查。

## 单 Map 容量单元

`baselines/map_capacity.py` 提供 `prepare`、`budget`、`cell`。它复用固定 Map 消息、HTTP transport、
PG v6 和评分组件；direct 是独立有界客户端诊断，不调用 SessionEngine/组织器，不是原生系统 baseline。
模型/PG 服务启动、机器与模型身份、缓存初始状态、扫描选点由调用者按当前计划负责。

```sh
python3 code/scripts/baselines/map_capacity.py prepare \
  --source /path/to/dev-v1.1.json --tokenizer /path/to/local-model \
  --model-revision "$MODEL_REVISION" --context-limit 4096 \
  --rows-per-split 5000 --seed 20260910 --output /path/to/new-private-samples
python3 code/scripts/baselines/map_capacity.py budget \
  --budget-id "$BUDGET_ID" --requests "$REQUEST_LIMIT" --seconds "$TIME_LIMIT" \
  --output /path/to/new-private-budget.sqlite
python3 code/scripts/baselines/map_capacity.py cell \
  --config /path/to/cell.json --manifest /path/to/new-private-samples/natural.json \
  --fixed-model /path/to/fixed-model.json --budget-file /path/to/new-private-budget.sqlite \
  --budget-id "$BUDGET_ID" --requests "$REQUEST_LIMIT" --output /path/to/new-short-cell-root \
  --pg-dsn-env MAP_CAPACITY_PG_DSN --pg-log /path/to/actual-postgres-log
```

`CellConfig` 字段为 `unit_id,arm,split,rows,queries,concurrency,window,input_bytes,result_bytes,pg_window_bytes`；
`arm=direct|pg`。可选 `statement_timeout_ms`、`flush_rows`、`event_mode=compact-buffered|qualification`、
`producer_trace=true|false`。窗口 L 与活跃请求 C、三类字节预算分别指定。`queries` 是多条独立 SQL/
direct 查询，逐条记录 JCT，不能用其总时长冒充单查询稳态。自然/分层样本分别保留完整 token 画像、
种子和 context 分区，旧前缀选样入口不变。所有规模都有上限；准备值和评价字典仍随声明行数增长。

另可独立设置 `event_content=compact|full` 与 `event_write_mode=synchronous|buffered`。
未指定时兼容旧模式：direct qualification 是 compact＋synchronous，PG qualification 是
full＋synchronous，compact-buffered 为 compact＋buffered。完整事件仅在私有目录保存，PG 的公开
伴随文件保留紧凑计数／哈希。独立 observer CLI 使用 `--event-content` 和 `--event-write-mode`；
full 必须另给 `--private-events`。旧 observer qualification 未指定私有伴随文件时仍为 credential-redacted，
它不移除任意业务文本，因此该输出也必须位于 Git 之外。

单元开始前从 SQLite 账本持久预留 `rows*queries`；只允许一个进程领取一次，HTTP 前扣减不读盘。
崩溃、未发送和不确定结果都不退款，领取后禁止重新领取/复制到其他进程。旧 v1 小账本保留原格式，
声明规模超过 64 KiB 可容纳范围时提前拒绝，追加也先检查上限。不能单纯增大旧账本额度做容量实验。

PG runner 读取实际 server log 中默认关闭的 `semloom_pg.test_map_binding_id_column` 观测。
该列必须是本次 Map 输出中的唯一、直接投影 text 列；通过已有 tuple binding 找到 child 中的值，
不依赖 child 的列名。验证用 PG 的 before-offer 行 ID/stream/sequence/digest 与 accepted 记录，
再对照 gateway 请求侧事件、可信 socket peer PID、完成及 SQL 返回。缺失日志或字段直接失败。
`producer_trace=false` 仅用于观测扰动诊断，报告明确没有独立 producer 资格，不能替代开启时的检查。

`record_execution`/`record_async_execution` 保留旧 elapsed，另记 release、首/末接收、EOF/错误终止、
结果持久化与 stream 清理；主 JCT 为 query terminal 减 release，仍含消费与在线记录干扰。
`received_rows` 与 `recorded_rows` 分开；`evaluate_recording(...,mode='stream')` 在查询之后消费文件并核对
行数/哈希。当前 SQuAD evaluator 仍保留有规模上限的 ID/预测字典，RSS 与查询阶段分开采样。
外部服务、PG 连接和事务的最终清理时间由其所有者另记。

`semloom.query_timing.v3` 在流上下文退出前保存执行错误和终点，独立保存
`query_error/cleanup_error/recording_error`；后续错误不覆盖第一原因。`statement_timeout_ms` 同时作为
两臂查询期限：PG 设置 statement timeout 并用有限客户端取消兜底，direct 使用异步期限。
PG 容量 API 必须传入专用、空闲、autocommit 连接；通用 recorder 不接管事务。
runner为gateway子进程显式传递自身的绝对源码目录，不依赖调用者设置PYTHONPATH或保持仓库工作目录。
准备／执行／评价失败时仍保存可取得的单元证据，未知资源状态不写成零。

真实 PG＋本地 HTTP 的 opt-in 检查入口为
`python -m unittest tests.experiments.test_pg_query_execution_integration`；设置
`SEMLOOM_TEST_PG_DSN`、`SEMLOOM_TEST_PG_LOG`、全新私有 `SEMLOOM_TEST_ARTIFACT_ROOT`。
PG 与 runner 使用同一测试用户；测试不启动模型或替调用者销毁 cluster。

容量事件模式使用字节/条数有上限的队列，后台分批写入，收尾 drain/fsync；满队列或写盘失败使运行失败，
不丢弃事件继续计性能。`--cell-budget`、`--unit-id`、`--event-mode compact-buffered` 和
`--observer-summary` 也可直接交给 `choice_gateway_observer`。紧凑事件保存请求/输出哈希与资源计数，
原始 SQL 结果保存在私有记录中。逻辑 result bytes 是责任预留，不等于实际结果大小或进程 RSS。
`analysis/sample_capacity_service.py` 对已启动的本地服务采集 running/waiting、KV cache、usage、
服务进程 RSS 与 GPU 状态；请求均为 metrics GET，不发送模型推理。其采样误差与失败单列。

## PostgreSQL semantic execution-provider gateway

`services/run_execution_provider_gateway.py` 是外部 semantic execution-provider 的 canonical CLI。
同步 recording wire v2、exact wire v3、choice wire v4、deterministic golden adapter、fixed OpenAI-compatible
adapter 和 UDS server 的实现位于 `src/execution_provider/`。从仓库根运行 recording profile：

```bash
python3 code/scripts/services/run_execution_provider_gateway.py \
  --socket /absolute/path/semloom-recording.sock
```

上述公共 CLI 可从任意工作目录启动，无需额外设置 `PYTHONPATH`。TAP 已迁移至该入口；旧
extension CLI/import 别名已删除。Python 调用方直接导入 `src.execution_provider` 中的对应模块。
golden profile 使用 `--golden-fixture`，fixed profile 使用仓库外 `--fixed-model-config`；endpoint、
model、timeout 和 bearer-token 环境变量名不进入仓库。

默认同步模式下，`--max-connections`（默认8）限制同时存活会话，`--max-active-requests`（默认1）独立限制活跃或
远端终态未知的模型请求；没有待执行任务队列。连接满时新连接关闭，请求满时返回
`MODEL_REQUEST_REJECTED`。每会话仍同步逐项执行，空闲会话不占模型请求名额。
`--frame-timeout-ms`（默认120000）限制一个完整帧的读取时间，帧头与帧体共用期限。
这些值是可配置的工程默认，不绑定具体机器或模型吞吐。未知远端终态不会因断连而自动退还名额；
确认服务端请求已结束后，通过重启gateway恢复。系统DNS解析线程可能持续到解析自身返回。
请求观测事件携带`session_id`与`task`，可关联相同payload的并发请求/完成；
旧独占会话资源测量器仍不能用于并发资源归因。

### 图像 provider

同一 `services/run_execution_provider_gateway.py` 增加 `--image-config`，与文本模型配置互斥。
先将[配置示例](../configs/image_provider.example.json)复制到仓库外，填入已核对且已缓存的模型与
processor revision。示例容量只说明字段与有限资源，不代表已经校准。模型加载使用
`local_files_only=True`，不会自动下载；真实模型运行仍按专项计划另行确定资源与请求额度。

```bash
python3 code/scripts/services/run_execution_provider_gateway.py \
  --socket "$IMAGE_GATEWAY_SOCKET" \
  --image-config "$IMAGE_SERVICE_CONFIG" \
  --image-ray-address "$IMAGE_RAY_ADDRESS" \
  --max-active-jobs 2 --max-held-tasks 4 --max-active-requests 2
```

`staged` 模式必须明确指定已有 Ray 实例的地址，不接受 `auto` 或 `local`。
CPU actor 负责解码、resize 和 normalize，模型 actor 只接收准备后的张量；每个模型 actor 使用
Ray 分配的一张 GPU。任务不重试、actor 不重启。启动等待使用配置中的 `timeout_ms`。
`reference` 模式不使用 Ray：配置改为 `mode: reference`，去掉 Ray 地址并使用
`--max-active-requests 1`；GPU 可见性由仓库外运行环境指定。

PG 显式安装或升级至扩展 `0.3.0` 后，使用 `ai_semantic.embed(image, options)`，
选择匹配的 `image-reference` 或 `image-staged` profile。七个 options 字段与配置的 `plan` 完全一致。
图像是 Map 的关系行为，复用原有 PG 窗口、权限、快照和事务处理；数据传输使用独立的 wire v7。

各连接的 `MethodDriver` 以已声明的输入、状态、结果最大值计算固定行预留；全服务使用一个既有
`MethodBudgetPool` 分配这些预留。`method_capacity`、`method_allocated`、`method_used` 与 Core 的
`usage`、`image_stages` 分开记录。Core 结果交给方法后可释放其槽，最终向量仍计入方法预算，
发送完成后才释放。编码字节、准备后张量及 CPU/model 名额另由 stage broker 计费；
解码暂存受单图像像素上限和 CPU actor 数量控制，这些字段不是整个服务的 RSS 上限。

受控集成入口为 `tests.execution_provider.test_image_pg_integration`；只有明确提供隔离环境的
`SEMLOOM_IMAGE_PG_DSN`、`SEMLOOM_IMAGE_TEST_ROOT` 和短路径 `SEMLOOM_IMAGE_RAY_TEMP` 才执行。
它启动仅有 CPU 资源的私有 Ray 实例，使用真实 PNG 解码与明确标识的零权重模型；
不加载真实 CLIP，也不分配 GPU。完整检查及尚未验证项见[结果记录](../../experiments/results/postgresql/image_stage_execution_check_20260920/README.md)。

### 生成 Map 的增量核心接入

启动同一服务级 Engine 的增量路径（默认一个活动Job）：

```sh
python3 code/scripts/services/run_execution_provider_gateway.py \
  --socket /path/to/provider.sock --fixed-model-config /path/to/fixed-model.json \
  --incremental-map --max-held-tasks 2 --max-active-requests 2
```

PG多在途路径选择
`SET semloom_pg.provider_execution_profile='incremental-map'`；PG的 `provider_window_tasks`
默认2且不得超过网关公布的接纳任务数，`provider_window_bytes`默认8MiB。
网关用 `--max-held-tasks` 设置接纳任务数，用 `--max-active-requests` 设置后端并发；
`--input-buffer-bytes` 和 `--result-buffer-bytes` 分别限制输入与结果预留空间。
未指定新参数时保留原默认值：接纳数等于请求容量，字节预算按单任务上限乘接纳数计算。
任务窗口可以大于后端并发，字节预算不足时通过接纳结果施加背压；这些预算不是GPU显存测量值。
PG按字节预算检查窗口存储，不再限定为64项。该路径使用v6接纳/结果协议，
仅支持受限生成Map；EXPLAIN展示实际输入窗口，复杂表达式退回窗口1。
[验证记录](../../experiments/results/postgresql/async_window_20260908/README.md)包含同一PG查询的真实并发、取消与回收。

单Job生成Map可额外传`--organization-config /path/to/organization.json`。配置字段为：

| 字段 | 含义 |
|---|---|
| `mode` | `rows`按输入序固定行数组；`work`按输入序累计工作量；`length`在有限候选前缀内按工作量升序排列 |
| `window_rows` / `batch_rows` | 每次最多查看的已接纳候选数 / 每组最多行数，前者不能超过接纳任务容量 |
| `batch_work` / `active_work` | 组织分组目标 / 执行中工作量总上限，单位为token；超过分组目标的完整单行独占一组 |
| `model_id` / `model_revision` / `serving_revision` | 固定模型、权重版本和服务版本，组成估计身份 |
| `tokenizer_path` / `tokenizer_sha256` | 本地tokenizer目录与内容指纹；通过`map_organization.tokenizer_fingerprint()`计算，启动前后核对 |
| `context_tokens` | 完整输入加生成预算的context上限；超限拒绝，不修改或截断出站消息 |

工作量由完整规范消息的实际token数加`max_tokens`计算，输出部分是预算上界，不是预测真实输出长度。
三种控制共用计数方法及既有活跃请求/工作量/字节限制。每组仍展开为独立单行请求；分组不会插入
整组完成等待，因此两个输入序控制可能产生相同的实际提交顺序。此配置只支持单Job Map v6，
不适用于Filter/query-job或原生Ray/LOTUS。tokenizer常驻内存和预处理时间须随进程RSS与准备事件报告，
不包含在核心payload字节账本内。连续无限到达下的公平性不由该静态排序保证。

网关默认不使用单次模型超时限制排队或结果等待；模型HTTP仍有独立超时，PG取消与socket超时仍生效。
嵌入式入口 `server.main(incremental_execution_factory=...)` 可传入执行组装函数；
默认 `build_fixed_model_execution` 接受已有 `SessionPolicies`、工作量描述、阶段超时配置及`choose_flow`跨Job选择函数。
同一组装函数还可注入`allocate_job(engine)`，返回`JobBudget`。默认资源策略静态均分存储与
执行上限，不借用其它Job空闲份额；Engine校验总登记量、计费和释放，网关不计算份额。

多个独立PG查询共享Engine时，在上述命令追加`--max-active-jobs 2 --max-held-tasks 4`，
保留`--max-active-requests 2`。默认每Job可接纳2项、执行1项；`--max-connections`限制含
待登记连接在内的socket总量。总任务及字节容量须足以让每Job容纳至少一项最大请求与结果。
接纳数与执行请求数可独立配置；增加连接数不会增加执行容量。

`incremental-map`模式仍按一个连接登记一个Job。Linux上可设置
`semloom_pg.provider_execution_profile='query-job'`，让同查询普通Filter与Map共享Job。
它使用同一个gateway命令，新增独立版本的登记握手；Map窗口为1，choice Filter尚不支持。
例如`--max-active-jobs 3 --max-held-tasks 6 --max-active-requests 2 --max-connections 12`
可以为每个双算子查询保留两份任务空间。默认按Job总份额分割流存储，不能靠增加算子扩大预算；
实际字节预算也必须覆盖所有流的最大输入/结果。控制连接结束才结束查询Job；节点结束只关闭该流。
`--once`计数的是物理连接，不适用于需要控制与算子连接的query-job模式。

共享计算模式追加`--job-compute-policy shared`（默认`equal-share`）。它复用Core统一责任表和Job/flow轮转，
各Job可使用全部空闲计算容量，仍分别保留存储；每Job存储必须容纳服务声明的全部并发请求。
例如4查询、4请求、每Job8行可设`--max-active-jobs 4 --max-active-requests 4 --max-held-tasks 32
--max-connections 16`；默认输入/结果字节配置按这32行预留。PG管理员可开启
`semloom_pg.enable_query_job_window=on`并设`provider_window_tasks=4`，使query-job生成Map使用窗口4；
两flow查询各分得4行，Filter→Map仍遵守原有child安全预读判断。当前token组织配置仍限单Job Map。
实施与验证范围见[工作包E](../../experiments/plans/archive/数据组织历史方案_20260927.md#work-package-e)。
查询登记还在总连接上限内保留一个有期限的握手入口：双流查询至少需要4个socket名额。
`ConnectionCapacity`统一记录查询承诺与standalone占用，未打开的流名额不能借给其它工作。
帧期限从已登记流的首字节开始计算；帧间空闲由PG查询寿命管理。未登记与语义open仍有期限，
控制连接仅等待EOF，额外数据直接拒绝。
见[详细设计](../../experiments/plans/数据库接入.md#query-job)及
[验证记录](../../experiments/results/scheduling/query_job_20260909/README.md)。
独立生产器可用`engine.register_job(label, budget)`取得能力句柄，并以`engine.open(..., job=handle)`
打开多个流；由控制线程调用`engine.advance()`，各流`advance()`只交付结果。字符串标签不能加入Job。
[多Job设计](../../experiments/plans/增量执行.md#multi-query)说明资源归属、错误范围和待实现项。

v5过渡桥接 `--incremental-map-window-one` 已移除。单行窗口使用同一条v6路径：
`--incremental-map --max-held-tasks 1 --max-active-requests 1`，数据库同时设置
`provider_execution_profile='incremental-map'` 和 `provider_window_tasks=1`。
旧CLI/PG名称会明确报错；同步语义参考与wire v5仍保留。
[迁移验证](../../experiments/results/scheduling/bridge_retirement_20260908/README.md)记录旧桥接对照及最终版本的真实模型检查。
取消后保留未确认的远端占用；其它Job仅使用剩余容量，不能通过新建Engine重置额度。
观测CLI继续使用同一持久请求账本，记录提交、终态、排空和传输关闭，header不进入日志。

<a id="optional-daft-ray-map"></a>
### 可选Daft Native与Ray Core文本传输

默认HTTP路径继续保留。在增量网关参数后追加`--map-transport-config "$MAP_TRANSPORT_CONFIG"`，
可使用已存在的Ray集群。该JSON文件留在仓库外，由实际环境提供以下字段：

| 字段 | 示例或要求 |
|---|---|
| `address` | 明确的Ray集群GCS地址；本地示例为`127.0.0.1:6379`，不接受`auto`或`local` |
| `workers` | `1`；不得超过`--max-active-requests`，每个worker预留1个Ray CPU slot |
| `batch_rows` | `2`；不得超过活动请求上限，不等待凑满批次 |
| `window_bytes` | `2097152`；有限输入窗口的Arrow数据字节，至少容纳一个1MiB协议请求和24字节行键/偏移 |
| `object_bytes` | `4194304`；Ray对象底层Arrow buffer预留，至少大于窗口8字节 |
| `payload_backend` | 默认`daft`，可显式选`arrow`直接构成已选行的独立批次；只改变物理分批 |

PG仍选择`incremental-map`或已有`query-job`，设置对应输入窗口。网关先接收PG已选择的规范输入，
Daft Native只处理当前有限窗口；Ray Core actor按对象引用和行位置发出独立HTTP请求。
同一批次中快结果可先返回，PG按原协议恢复行关联。嵌入式调用若已连接Ray，声明的GCS地址必须与当前连接一致。当前不同时指定`--organization-config`；
`--job-compute-policy shared`和现有多Job机制可继续使用。它不自动启用PG优化器的backend选择。

`QueryConfig`新增可选`map_transport_config`与`map_transport_sha256`，用于完整观测的PG Map验证。
查询runner核对文件身份，并把配置副本写入各运行目录。`map_payload_backend`声明分批选择，
必须与物理配置`payload_backend`一致；默认均为`daft`。Arrow/Ray诊断不进入原Daft/Ray比较。
正式请求仍使用已有预算化实验入口；
`choice_gateway_observer`会在Ray RPC前持久预扣，不能只观察gateway进程里的HTTP。
可选`--remote-budget-mode threaded`仅用于有durable ledger的Ray Map，将原reserve交给单个I/O线程，
完成后再核对/记录请求并允许RPC；默认`synchronous`保留。查询配置使用`remote_budget_mode`选择，
模式进入配置摘要。取消等待已排队事务完成，不退还已提交额度；线程在事件writer关闭前排空。
worker的本地耗时和对象占用分别记录；没有对齐跨节点时钟时，HTTP活跃时间线明确不可用。

窗口与对象额度不等于整个进程RSS或Ray对象存储的总大小。首次SQL查询还可能包含Daft及HTTP客户端的惰性准备。
已验证单机受控路径，实际范围与失败记录见[本轮报告](../../experiments/results/postgresql/incremental_transport_20260927/README.md)；
追加[真实模型检查](../../experiments/results/postgresql/transport_real_20260927/README.md)完成12条SQL、4144次请求和资源清理。
图像继续使用上文已有typed阶段路径；新二进制批次的图像字节检查不等于完成了新的图像PG传输协议。

## SemMap resource measurement

`experiments/run_semmap_resource_checks.py` creates a new, exclusively owned result directory before
preflight and compilation. Existing directories are refused without writing into them. It compiles the
source-managed C client; the obsolete `--client` argument has been removed.

```bash
python code/scripts/experiments/run_semmap_resource_checks.py \
  --repo /path/to/repository --root /path/to/new-artifact-directory \
  --prefix /path/to/postgresql-18.3-install --commit <source-commit> --diagnostic
```

`--pg-user` selects the isolated cluster's OS owner and initial database role; `--pg-port` selects its
Unix socket port number. Defaults remain `postgres` and `55446`. The streaming client takes connection
values from the running cluster, so changing machines does not require editing Python or C code.

The diagnostic performs an actual 1×100 fixture workload with 100000-byte input and 65536-byte output.
It sends no real model requests. Run the runtime preflight first; keep the artifact/socket path short enough
for AF_UNIX. Each phase preserves baseline, operation, cleanup, session events and its own report/hash list.
Case and run reports use the same assessment; diagnostic qualification is always `not_evaluated` (exit 2).
Formal results use exit 0 for all required phases/cases passing, 1 for valid failed checks, 2 for incomplete
measurement, and 3 for runner/preflight failure. Interrupts preserve available evidence and propagate.
Fault/recovery connections use the observer's optional fixture barrier that waits up to five seconds for both endpoints
to be observed before releasing the handshake. Pressure timing is unchanged; these fault timings are not performance evidence.
See the [Map contract](../../experiments/plans/生成算子.md) for thresholds and
current authorization. Formal 3×2000 remains unavailable until a valid current diagnostic and separate authorization.

The fixed-model observer can serve Filter or Map using the same implementation:

```bash
PYTHONPATH=code python -m src.experiments.choice_gateway_observer \
  --events /path/to/new-http-events.jsonl --session-events /path/to/new-session-events.jsonl \
  --ledger /path/to/existing-attempt-ledger.jsonl \
  --budget-id <approved-budget-id> --max-attempts <approved-total-limit> -- \
  --socket /path/to/provider.sock --fixed-model-config /path/to/fixed-model.json
```

Supply both budget options together. Without them the legacy choice identity and total limit of 100 apply.
The existing ledger must match the expected identity and limit; opening it never restores spent attempts.
Endpoint, model ID and timeout come from the fixed-model JSON. This is a reusable observation entry point,
not authorization for new requests or a complete PG experiment. Dated scripts under results are retained
only to explain their original runs; future checks must not import their machine settings or budget code.

## Choice resource qualification tools

`experiments/run_choice_resource_checks.py` 是内部、仅限 Linux 的 fixture 资源验证入口。它使用指定的
PG18.3 安装创建独立集群，测旧/新配置、取消恢复和阻塞 DNS；不启动或调用真实模型。
运行前遵循 runtime preflight，并使用新的仓库外产物目录，采样与判定条件见
[choice 专项计划 C.5](../../experiments/plans/completed/选择算子接入_20260902.md#c5-对照请求预算与资源保证)。

```bash
python code/scripts/experiments/run_choice_resource_checks.py \
  --repo /path/to/repository --root /path/to/new-artifact-directory \
  --prefix /path/to/postgresql-18.3-install
```

实验侧共享 `src/experiments/attempt_ledger.py`，在 POST 前持久预留尝试，失败不退款；
choice 入口显式提供默认的 100 次预算，其他运行由外部配置提供预算身份与上限。
已有 ledger 不重新初始化。`choice_gateway_observer.py` 只在独立验证进程中记录实际请求/完成，
不记录认证头，也不改变生产 gateway 或 PG port。真实 smoke 必须复用同一外部 ledger；这些工具
本身不证明真实服务支持 choice 或通过模型质量验证。

`experiments/run_choice_service_checks.py` 以独立 PG18.3 集群执行预先登记的 14 次 old/choice 请求与
两个 NULL 对照。它要求已有持久 ledger；真实模式核对 live service、模型文件及继承参数，fixture 模式
必须显式指定。记录实际 HTTP JSON、raw completion、SQLSTATE、EXPLAIN 行数/usage，以及前后身份；
仅删除 choice 字段后仍有值或类型差异则拒绝通过。程序不启动模型、不创建真实预算、不读取 held-out，
也不评定标签质量；先按同一 [C.5 计划](../../experiments/plans/completed/选择算子接入_20260902.md#c5-对照请求预算与资源保证)
完成 preflight、模型服务与账本核验，再使用 `--help` 中的路径参数运行。

## Exact SemFilter reference calibration

`analysis/build_semfilter_reference_calibration.py` 只读取离线 training/held-out 观测，
生成 planner 显式选择的 JSON artifact；它不连接 PostgreSQL 或模型服务。artifact 必须匹配
semantic/physical、模型角色、provider、workload 与服务身份；缺失或失配时继续使用未校准的
exact reference，不产生第二物理路径。接口和使用条件见
[extension 说明](../postgres/semloom_pg/README.md)及[真实采集记录](../../experiments/results/postgresql/semfilter_reference_calibration_20260901/README.md)。

## 其他历史脚本

| 工作 | 主要脚本与事实入口 |
|---|---|
| SAOR 机制与原生系统检查 | `analysis/audit_saor_*.py`、`analysis/summarize_saor_*.py`、`experiments/run_saor_*.py`；当前状态及各检查的作用见[SAOR 模块](../src/experiments/saor/README.md)和[实验状态](../../experiments/plans/archive/进度汇总_20261001.md) |
| 图像与多 Job 画像 | `experiments/run_image_*.py`、`profiling/profile_clip_*.py`、`analysis/summarize_image_*.py`；身份与可比性见[baseline 说明](../src/baselines/README.md)和[图像结果](../../experiments/results/README.md) |
| 开题实验汇总 | `analysis/summarize_opening_*.py`、`baselines/opening_database_e2e_matrix.py`；按[开题材料](../../docs/archive/opening/README.md)和对应结果报告读取，不从汇总脚本推断新结论 |

这些脚本的早期机器命令与当时的完成状态保存在 Git 版本 `d32366de` 的本文件中。
新的运行参数从目标计划、运行手册和脚本接口重新核对。

## 早期外部执行脚本

以下脚本服务于外部文本画像、baseline 或历史实验。它们的运行身份以对应结果报告为准；
参数从脚本的 `--help`、配置模板和目标 runbook 核对，不从旧机器的命令示例复制。
`profiling/postgres_ai_operator_profile.py` 的 PostgreSQL 读取由外部 runner 管理，不能作为
PostgreSQL 内置语义算子的验证结果。

| 用途 | 入口与说明 |
|---|---|
| 外部画像、Daft 组织与指标 | `profiling/postgres_ai_operator_profile.py`、`profiling/daft_text_organizer_smoke.py`；模块职责见[观测说明](../src/observability/README.md)，结果见[动机画像](../../experiments/results/motivation/README.md) |
| 单作业场景编排 | `experiments/run_ai_operator_scenarios.py`；按固定种子保存次序、成功记录和失败事件，恢复时重新核对配置与既有记录 |
| 共享服务多 Job | `experiments/run_shared_vllm_experiment.py`；配置与运行步骤见[AutoDL 手册](../../deploy/autodl/README.md)，模块职责见[共享服务说明](../src/experiments/shared_vllm/README.md) |
| 官方 baseline 与 SQuAD 能力检查 | `baselines/run_official_baseline.py`、`baselines/squad_capability_gate.py`、`baselines/squad_database_e2e_runner.py`；系统职责见[baseline 说明](../src/baselines/README.md)，每次结果见[可行性记录](../../experiments/results/diagnostics/README.md) |
| 工作量准备与代价分析 | `data/import_ai_complete_workload.py`、`analysis/estimate_operator_cost.py`；输入筛选、模型与机器身份以目标计划和结果报告为准 |
| 状态变化实验 | `data/prepare_phase_change_workload.py`、`experiments/run_phase_change.py`、`analysis/audit_phase_change.py`；只从[目标手册](../../deploy/autodl/phase_change_state_aware_RUNBOOK.md)选择步骤，历史实现见[SAOR 说明](../src/experiments/saor/README.md) |

增量导入 ShareGPT/BurstGPT 时，`--source-row-offset` 在全部筛选后才生效；追加前应以相同
原始文件、tokenizer 与筛选条件验证既有前缀，再使用 `--append-only`。仅改变
`start_doc_id` 会重编号已有提示，不能选出新的输入行。

早期逐参数清单和机器专用命令保存在 Git 版本 `d32366de` 的本文件中；历史结论、
原始运行及失败记录仍在各[结果目录](../../experiments/results/README.md)。
