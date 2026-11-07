# 原生基线与具名 Core 历史复现

本页保存各次准确源码的恢复方法；实现现状见[代码状态](../../../code/INFRA_STATUS.md#current-refactor-status)。
较早小节的失败记录按当时来源保留，不代表后续版本仍处于同一状态。

<a id="operator-cost-row-estimator-source"></a>
## CSV-row estimator source

Historical CSV-row methods now live in the existing offline operator-cost directory. Their algorithm bodies are unchanged; LightGBM and public Ridge mathematics remain in planning. The three estimator behavior tests follow the CSV methods, while four numerical metric tests stay in planning. The old CSV-class imports and the unused Ridge convenience re-export have no forwarding aliases.

The exact public source from `4df3ccd7a9067bc7d80dbc5db93487be9e9622ff` is saved once as
[estimators.py.gz](historical_sources/4df3ccd7/code/src/planning/costs/estimators.py.gz).
Its decoded SHA-256 is `8e3f6949bc915af40a77e44068849a8f64cb3f8108df5c90509bcf2ae30f812e`. Five projections add only their matching source override and gzip-format record. Original file expectations, projection identities, source revisions, regression/capacity mappings and existing CLI source mappings are unchanged. The original test file is not a projection input and can be restored by its path from that same public Git revision. No unpublished author commit is needed for recovery.

<a id="operator-cost-feature-schema-source"></a>
## Operator-cost profile schema source

The unchanged 23-field `FEATURE_NAMES` tuple now lives beside `feature_vector` in the
[offline profile module](../../../code/src/experiments/operator_costs/README.md). The CLI imports that module directly;
Ridge fitting, splits, predictions and metrics remain in `planning/costs/regression.py`.

The exact source from `24a2b75b2ef664f8467007b072ee44ed13e0faa4` is saved once as
[regression.py.gz](historical_sources/24a2b75b/code/src/planning/costs/regression.py.gz).
Five projections add only the matching source and gzip-format entries; their expected file hashes, projection identities,
source revisions and existing mappings remain unchanged. The old CLI mappings continue to read their existing sources.
The former schema import has no forwarding alias. Tests are not projection inputs; recover their old paths from the same
exact Git revision when restoring that API. Existing CSV data, field order and result artifacts are unchanged.

<a id="retired-positive-B-token-packing-bypass"></a>
## 旧正值 B 旁路与 CPU 消融来源

`experimental_skip_token_packing` 已从当前构造、Movie／Session 配置和调用中移除。
当前 rolling 省略 `prepared_batching` 即使用 NoB；显式正值 B、请求／字节额度、上下文、取消和 unknown 责任继续保留。

旧正值 B 加旁路不是 NoB 配置或身份。准确公开 `ab088849022f79dd4ce9f97ca7029113e826ab46` 保留
[原组织器](https://github.com/3444374/ai-operator-execution-optimization/blob/ab088849022f79dd4ce9f97ca7029113e826ab46/code/src/execution_provider/adapters/lotus_chain_token_batches.py)、
[原校准身份](https://github.com/3444374/ai-operator-execution-optimization/blob/ab088849022f79dd4ce9f97ca7029113e826ab46/code/src/experiments/calibration/core.py)、
[原 CPU 消融与微基准](https://github.com/3444374/ai-operator-execution-optimization/blob/ab088849022f79dd4ce9f97ca7029113e826ab46/code/tests/execution_provider/test_lotus_token_packing_bypass.py)及
[原 Movie 专测](https://github.com/3444374/ai-operator-execution-optimization/blob/ab088849022f79dd4ce9f97ca7029113e826ab46/code/tests/experiments/test_movie_token_packing_bypass.py)。
旧 `python -m tests.execution_provider.test_lotus_token_packing_bypass --microbenchmark` 只在对应旧树运行，当前不保留该命令。
原结果和旧身份保持，不重新标为 NoB，也不把旧通过记录用于新配置。

当前[组织测试与共享 helper](../../../code/tests/execution_provider/test_prepared_request_organization.py)保留实际 NoB／B、字节、拒绝和 unknown 检查，
[Movie 用例](../../../code/tests/experiments/test_movie_prepared_requests.py)保留当前准备与 resident 行为；需要真实 tokenizer 的部分仍须在正常测试环境验证。
本次涉及的二十五条具名投影来源均已有独立旧源码，原清单、预期摘要、身份和路由保持，未新增来源映射。

<a id="retired-local-embedding-helper"></a>
## 旧文本嵌入 HTTP 辅助服务来源

`local_embedding_server.py` 仅服务于早期 PostgreSQL／GPU 嵌入画像，已移出当前服务脚本。
当前本地 IPC、typed 图像执行及第三方原生服务不使用它；旧 GPU 结果与原始命令保持。
准确公开 `af63c0e56dab8cbd3f13649375bffa8f6eb92422` 的
[162 行原件](historical_sources/af63c0e5/code/scripts/services/local_embedding_server.py.gz)保存一次。
五份投影各增加该文件的来源和 gzip 格式记录，原预期摘要、投影身份、来源版本和已有映射保持。
复查旧画像时先按本页恢复对应准确源码，不用旧服务命令解释当前运行方式。

<a id="semfilter-reference-calibration-sources"></a>
## SemFilter 参考校准归离线分析前的来源

校准实现逐字迁入[离线算子代价目录](../../../code/src/experiments/operator_costs/README.md)，
CLI 直接导入真实模块，原命令、参数、数学算法和 PostgreSQL 产物身份保持。
构建器已校验完整结果，CLI 不再对同一未修改对象重复校验；专测随用途迁入 `tests/experiments`。
原 `planning.semfilter_reference_calibration` 没有转发文件。

准确公开 `a39cec7404e92f0828bcbbe5d10b90b8d9652888` 的
[原校准模块](historical_sources/a39cec74/code/src/planning/semfilter_reference_calibration.py.gz)和
[原 CLI](historical_sources/a39cec74/code/scripts/analysis/build_semfilter_reference_calibration.py.gz)各保存一次。
五份投影只增加十条来源及 gzip 格式记录，原预期摘要、投影身份、来源版本及已有映射保持。
专测不在投影列表中，可从同一公开树恢复
[原测试和 fixture](https://github.com/3444374/ai-operator-execution-optimization/blob/a39cec7404e92f0828bcbbe5d10b90b8d9652888/code/tests/planning/test_semfilter_reference_calibration.py)。
旧 raw 采集、秩检查复现、失败报告和数值记录保持，按各自准确源码运行，不混用当前目录与历史脚本。
658 行实现只改变所属目录，不计作删除了算法代码。

<a id="text-organization-research-sources"></a>
## 文本组织原型研究归位前的来源

Arrow／Daft 文本组织原型逐字迁入[独立研究目录](../../../code/src/research/data_organization/README.md)，
原配置、排序、装箱和指标函数保持；两份专测随真实用途迁移，旧 `data.materializers.text` 不再提供导入。
二进制物化继续直接使用共享 Daft runtime，当前 prepared 组批没有因此接入该原型的排序或装箱策略。

准确公开 `81dfc7273efce377be386543562fad577b83f53e` 的
[文本原件](historical_sources/81dfc727/code/src/data/materializers/text.py.gz)与
[原包说明](historical_sources/81dfc727/code/src/data/materializers/__init__.py.gz)各保存一次。
五份投影只增加十条来源及 gzip 格式记录，全部原预期摘要、投影身份、来源版本和已有映射保持；
先前二进制物化的五条读取继续使用同目录原件。
两份专测不在投影文件列表中，从同一准确公开树按旧路径恢复，不重复保存副本。
历史结果、标量 packing／cost 组件及输出成本汇总脚本均保留。

<a id="payload-daft-runtime-source"></a>
## 二进制物化直接使用共享 Daft runtime 时的来源

`data/materializers/payloads.py` 直接从 `infrastructure.daft_runtime` 导入已有运行配置函数，
不再经文本组织原型转借；完整输入、字节表示、批次和已有 native 线程配置保持。
五份投影改读[准确 81df 原件](historical_sources/81dfc727/code/src/data/materializers/payloads.py.gz)，
只增加五条来源及 gzip 格式记录，原预期摘要、投影身份、来源版本和已有映射保持。
文本组织研究原型和其排序、packing、cost 组件未随本次导入整理删除。

<a id="retired-profiler-application"></a>
## 旧 PG 画像应用的来源

旧 PG profiler 的 Ray 提交、调度组装、Arrow 封装、参数配置和 CSV schema 已退出维护目录；
最后仅含说明文字的包文件一并退役，不保留旧导入空壳。
原 CSV 写入退役后，专用的请求轨迹模型及 `MonotonicEpochClock` 也不再提供当前导入。
两份专测随对应实现退役，架构测试继续检查实际 Core 默认导入。
共享的 `SubmissionLifecycleEvent`、模型及 Runtime Ray 适配保持；
当前观测仍使用 `observability.clock.monotonic_clock_domain`，它不负责构造旧请求轨迹。

准确公开版本 `a0ce689bfe564438c25ecdc537f73aabb6550724` 的
[配置原件](historical_sources/a0ce689b/code/src/observability/profiling/config.py.gz)、
[CSV 字段原件](historical_sources/a0ce689b/code/src/observability/profiling/schema.py.gz)与
[请求轨迹原件](historical_sources/a0ce689b/code/src/scheduling/core/lifecycle.py.gz)各保存一次，
另保存准确的[包说明原件](historical_sources/a0ce689b/code/src/observability/profiling/__init__.py.gz)。
五份投影只为这四份原件增加二十条来源及 gzip 格式记录，原预期摘要、投影身份、来源版本和已有映射保持。
`ray.py` 的五条读取继续使用原有 `9652108d` 压缩件；两个后拆出的 helper 与两份专测未被这些投影读取，
可从[准确 a0ce 完整树](https://github.com/3444374/ai-operator-execution-optimization/tree/a0ce689bfe564438c25ecdc537f73aabb6550724)按原路径恢复，不重复保存压缩副本。

历史 CSV 字段、原始观测和研究计划继续按各自来源解释；源码恢复不增加新的运行结果。
现行指标使用[对照规范](../../plans/reference/对照规范.md)、[报告核对](../../plans/reference/报告核对.md)和实际采集器，
不再以旧画像应用的总字段表解释当前每条执行路径。

<a id="retired-profiler-csv-and-matrix-checks"></a>
## 旧 PG profiler CSV 与矩阵校验的来源

`observability/profiling/traces.py` 的八个写入／指纹函数只由已归档 PG profiler 和七项专测调用；
`baselines/common/gate.py` 的校验只由旧矩阵入口和四项专测调用。两份实现及两份专测退出维护目录，
当时的 Ray SDK 保留；其后续退役见[旧画像应用来源](#retired-profiler-application)。
metrics、原生观测、对照来源记录和公共结果校验继续由各自现用模块负责。

准确公开版本 `a0ce689bfe564438c25ecdc537f73aabb6550724` 的
[CSV 原件](historical_sources/a0ce689b/code/src/observability/profiling/traces.py.gz)与
[矩阵校验原件](historical_sources/a0ce689b/code/src/baselines/common/gate.py.gz)各保存一次。
五份投影只增加十条来源及 gzip 格式记录，原预期摘要、投影身份、来源版本和既有映射不改。
两份专测未被这些投影读取，可从同一公开版本恢复
[CSV／指纹专测](https://github.com/3444374/ai-operator-execution-optimization/blob/a0ce689bfe564438c25ecdc537f73aabb6550724/code/tests/observability/test_completion_evidence_trace.py)与
[矩阵校验专测](https://github.com/3444374/ai-operator-execution-optimization/blob/a0ce689bfe564438c25ecdc537f73aabb6550724/code/tests/baselines/text/test_official_baseline_gate.py)。
原调用方按[旧 PG 与矩阵恢复说明](../paused_pg_and_retired_http_20261105/README.md)恢复，不与当前运行代码混用；
原始结果、性能记录和旧 schema 身份均保留。

<a id="map-codec-import-sources"></a>
## Map 编码器直接导入前的来源

维护代码直接使用 `execution_provider/wire/map_codec.py`，删除了仅重新导出的 `wire/v5.py`。
协议版本 5、Map 会话、编码算法与校验行为保持。
五份投影的旧 `v5.py` 已有独立压缩副本，继续使用原来源。
调用方 `golden.py` 与 `semantic_session.py` 的准确 `6707371f64b7b9f7f0e388f1c1458b0606156f75` 字节另存为
[golden 原件](historical_sources/6707371f/code/src/execution_provider/adapters/golden.py.gz)与
[会话原件](historical_sources/6707371f/code/src/execution_provider/adapters/semantic_session.py.gz)。
只增加十条来源与 gzip 格式记录，全部预期摘要、投影身份及原来源版本保持。

<a id="offline-cost-analysis-sources"></a>
## 离线代价分析拆分前的来源

三个当前命令改为直接调用[离线模块](../../../code/src/experiments/operator_costs/README.md)。
准确`6f15358ea8836567bf9f26c860c0fd71c85eab25`的[full原件](historical_sources/6f15358e/code/scripts/analysis/compare_cost_estimators_full.py.gz)
和[context-LOO原件](historical_sources/6f15358e/code/scripts/analysis/compare_cost_estimators_contextloo.py.gz)保留各自完整字节。
五份投影增加十条读取／gzip映射，原expected、投影身份及已有bfb的Ridge脚本来源保持；四十条相关来源读取通过，未重跑完整物化或旧模型实验。
[数值对照与现行参数](../../results/diagnostics/records_runtime_refactor_20261104/README.md#offline-cost-responsibilities)与历史命令分别解释。

<a id="image-clip-script-source"></a>
## CLIP 命令拆分前的来源

旧 `run_image_clip_e2e.py` 的19个定义迁入所属实验模块后，五份历史投影继续读取原完整脚本。
准确 `5b25197dafc03d6ae7fa48e7c0f7460122e85e51` 的原件以
[单份gzip](historical_sources/5b25197d/code/scripts/experiments/run_image_clip_e2e.py.gz)保存；
五条来源和格式映射只更换读取位置，全部预期摘要、投影身份及已有映射保持。
首次五条摘要不匹配与修正后的五条读取通过分别保留，没有重跑整套物化或模型。
[职责、原字节与检查](../../results/diagnostics/records_runtime_refactor_20261104/README.md#image-clip-module-owners)
说明当前命令和历史脚本的使用方式。

<a id="basic-multiquery-sources"></a>
## 基础多查询与研究拆分前的源码

当前主线保留固定份额与普通轮转，原共享计算选项和候选窗口迁入研究适配。
五份具名投影仍需原 `image_execution.py`、`ready_window.py` 与 `session_jobs.py`：
首次受影响路径核查发现15条摘要不匹配，准确公开版本
`8891c41b85e322fc83fb9fede4d27e466fb6654e` 的三份字节均与原预期相符。
原件按各自路径压缩保存在 `historical_sources/8891c41b/`，仅增加15条来源及格式记录，
全部预期摘要、投影身份、原版本和已有映射保持；当前导入不加载这些历史副本。
修正后60条受影响来源读取通过，没有重跑完整3,010条物化或模型。
[原错误、原件摘要与复核](../../results/diagnostics/records_runtime_refactor_20261104/README.md#basic-multquery-integrated)
与当前多查询代码分别解释。

<a id="retired-baseline-worker-forwarders"></a>
## 已由公共实现替代的五个baseline转发模块

当前维护代码删除`daft_engine_process.py`、`lotus_cascade_process.py`、`lotus_chain_batch.py`、
`lotus_chain_engine_process.py`和`lotus_chain_results.py`五个薄转发文件；它们不再提供直接导入或旧pickle名称。
原生执行图、当前序列化、模型进程和结果处理继续使用实际所属的`serving/backends`及`semantic_methods/lotus`实现。
五份具名投影的25条相关源码读取已全部走既有override，原字节与预期摘要保持，不需要新增副本或映射。
这55行转发本身可从准确`608a32ddfc846a5512acd33225913363846a0d7c`的
`code/src/baselines/text/frameworks/`回查；历史实验仍按MANIFEST恢复当时真正实现，不能拿转发替代旧实现。
[现用调用迁移与CPU范围](../../results/diagnostics/records_runtime_refactor_20261104/README.md#baseline-worker-forwarders-retirement)
记录删除的实际影响及未运行项。

## SAOR 与多任务研究迁移的原源码

SAOR／共享额度迁入[独立研究包](../../../code/src/research/multitask/README.md)后，
四份旧策略／适配、四份研究消费者和一份原同步调度测试不再从当前路径取历史字节。
九份准确 `b08936d2808999a3d9da7ca47e8a940207d99184` 原件按各自路径以 gzip 保存在
`historical_sources/b08936d2/`，五份投影仅增加 45 条来源／格式映射；
原 3,010 条文件 SHA、投影身份、来源版本和已有映射均未改。

迁移前后逐项核对仍为原十处不匹配，无新增不匹配；不宣称五份完整树已恢复成功。
具体检查与首次发现的路径缺项由[整理记录](../../results/diagnostics/records_runtime_refactor_20261104/README.md#saor-multitask-isolation)
保留。归档源码只用于隔离历史恢复，当前导入不加载它们。

<a id="retired-profiler-parser"></a>
## 旧 profiler 参数解析的准确来源

旧 PostgreSQL profiler 执行入口退役后，维护代码没有调用 `observability/profiling/cli.py` 的
八个解析函数。该文件现退出维护目录；准确
`46795c0fae03ab07db15b01552884b831ee2a7c1` 原源保存在
[单份 gzip](historical_sources/46795c0f/code/src/observability/profiling/cli.py.gz)。
五份投影各增加一条来源和 gzip 格式记录，原文件 SHA、投影身份及其他来源保持。
原解析参数与原工厂只在恢复出的同版本树使用；维护的 Ray SDK 仍接受调用方构造的接纳对象，
AIMD、批反馈及原生 vLLM 的 `--scheduling-policy fcfs` 保留。
旧参数表可从上述准确 Git 版本的 `docs/执行配置与优化开关.md#diagnostics` 读取。

近期 session 行为测试扩展产生的五条历史来源漏映射另行更正：
[原测试单份 gzip](historical_sources/73e686aa/code/tests/scheduling/test_incremental_session.py.gz)
来自 `73e686aad55772535555e0b8bc7dbce078a727ce`，其 SHA 正是五份投影原预期值。
只补来源与格式记录，维护版新增行为测试及原投影预期不变。
这项错误更正与 parser 退役分别保存来源，不表示其他历史来源或模型环境已经恢复。
上述 parser 与测试整理时，3,010 条来源中有 3,000 条匹配，十处差异位于 `task_info.py` 和
`submission_control/__init__.py`。后续[原生基线来源修复](#baseline-source-recovery)已补齐准确原件；
最新 HTTP 观测退役组合逐项读取 3,010 条来源均匹配。旧失败和各阶段数字保留其原版本，不代表模型环境已恢复。

<a id="baseline-source-recovery"></a>
## 原生 baseline 依赖边界拆分的历史来源

时钟身份从 Core transport 抽出后，五份历史投影仍须取得原 `ray_map_transport.py` 字节。
准确 `7f0b6d42` 原件保存为[单份 gzip](historical_sources/7f0b6d42/code/src/execution_provider/adapters/ray_map_transport.py.gz)，
解压 SHA256 为 `d58de2fdfae52162713e0429d60b427471699acf1645eae9690c64fa0f94431e`。
五份投影只增加来源／gzip 格式记录，原 3010 个 expected SHA、投影 ID、来源版本及已有映射保持。
首次五项 materialize 的摘要失配保留为失败；修复不把当前生产时钟或 worker 写入历史投影。

完整恢复继续检查时，还确认基底 `7f0b6d42` 已有十条来源失配，分别是五份投影的
`task_info.py` 与 `submission_control/__init__.py`。准确旧源来自
`070cda9fe93ca23cbbf8602bc4158d547e4d0fe8` 与 `b59ce7ee938791a3a0396e08f10567b493ac4b4c`，
分别保存于 [task info 原件](historical_sources/070cda9f/code/src/scheduling/core/task_info.py.gz)和
[原包导出](historical_sources/b59ce7ee/code/src/scheduling/submission_control/__init__.py.gz)。
只增加十条来源／格式映射，未改当前调度源码或旧预期。两次恢复失败和修正后的检查分别保留。

## SemBench 查询扩展的原适配来源

Movie Q4/Q8/Q10 与 MMQA 接线扩展后，五份投影不能继续从维护版 `sembench_movie.py` 取旧字节。
首次完整 materialize 的五条摘要失配已保留；准确
`0677c7a3ef98f946d3b00772169ec636e6f1a309` 原适配保存为
[共享 gzip](historical_sources/0677c7a3/code/src/baselines/text/sembench_movie.py.gz)，解压摘要为
`d21f3933a83429b5b870abc352a268a3c3241a20e7d0bc405e34cfac275f823d`。
只追加五条来源／格式映射，原3010条 expected SHA、五个投影 ID、原来源版本及其他映射保持。
维护版使用新的原查询／评价接线，历史恢复继续使用原适配；不混写原模型结果。

## IMLane workload 边界修改前的来源

维护版按有限查询声明推导桥接容量后，六份原 workload／UDF／owner 源码从准确
`448e6a9c3004ad77a085f0b19dd5778212bfdfea` 保存至
[共享历史目录](historical_sources/448e6a9c/code/integrations/imlane_records/)。
五份投影只追加30条来源映射；原3010条 expected SHA、五个投影ID、来源版本和既有映射不改。
首次检查的30条摘要失配与修正后完整核对分别保留；历史投影使用原16／4096／64合同，
当前运行时使用新的显式声明，旧结果和模型成本不重算。
修正后五份完整代码树实际物化并逐文件核对，3010条全部匹配；未启动模型或运行原实验。

## 重复源码的共享保存

五份投影中内容相同的源码改由 `MANIFEST.json` 指向同一精确副本，删除 88 份重复文件，
减少 1,376,156 字节。3010 条原预期 SHA、五个投影 ID、来源版本和其他映射不变；
只调整重复文件读取位置，恢复继续逐项核验，不改变任何原始结果或当前运行策略。
旧部署与实验声明另见[配方归档](../legacy_recipes_20261105/README.md)。

LOTUS 职责迁移后，`image_source.py` 和 `maps.py` 的原字节分别保存在
[图像来源](historical_sources/1f92fce5/code/src/semantic_methods/lotus/image_source.py.gz)与
[Map 来源](historical_sources/1f92fce5/code/src/semantic_methods/lotus/maps.py.gz)，来自准确
`1f92fce59897fb7e633087ed09ddc50068cd8fd8`。五份投影仅补十条来源／gzip 映射，
原 3010 条文件摘要和投影身份保持。图像来源继续独立记录，固定两 Map 原型的后续处理见[退役说明](#retired-lotus-http-methods)。

## 2026-11-04 的恢复记录（历史）

五份完整代码投影现已按原逐文件 SHA 全部恢复：3010 条来源不匹配为 0。
原十处问题对应两份缺少恢复映射的文件；从原仓库历史找到准确字节后，增加十条共享映射，
原预期摘要、投影 ID、原始来源版本及既有映射不改。准确来源分别为
`e8eb34555ad39d575a5d86b54260eca0e72cf701` 的资源观测文件与
`219094151c59346f106c77545730f6941e15821b` 的 session policy 文件。
51 项 CPU 检查全部成功，包含五份完整投影实际构造及每个输出文件摘要核对；
没有模型、真实 Ray、GPU、下载或服务访问。环境、数据、模型质量和性能资格不由此补全。
[修复记录](../../results/diagnostics/records_runtime_refactor_20261104/README.md#历史源码缺失映射修复)
保留原失败和准确源证明；下方各历史阶段的失败数字仍按当时状态解释。

同步参数职责迁移另为三份旧文件保留准确 129c5555 原件，由五份投影按需共享十一条新增映射；
原逐文件 SHA、投影身份、来源版本和既有映射保持。新增压缩原件位于
[historical_sources/129c5555/](historical_sources/129c5555/)，维护版不导入它们。
完整构造仍逐项核对 3010 条来源；[本轮记录](../../results/diagnostics/records_runtime_refactor_20261104/README.md#sync-default-ownership)
保留首次组合漏映射的错误与修正后的证据，不把当前默认行为写入历史投影。

<a id="retired-ucb"></a>
## 未接入运行入口的 UCB 原型

当前生产目录移除 `scheduling/submission_control/ucb.py` 及公共 facade 的四项 UCB 导出。
It had only CPU policy tests and no maintained profiler reward epoch or execution caller. The shared AIMD implementation remains in explicit batch feedback; later admission retirements are listed below.
准确 bfb 原件保存在 [historical_sources/bfb9100e/](historical_sources/bfb9100e/)：
UCB 实现、原 `scheduling/__init__.py` 和完整原测试文件均可按 SHA-256 核对。
五份历史投影各增加两条 override，共十条；原逐文件摘要、投影身份和既有映射不变。
原五项 UCB 行为测试随准确历史源码保存；维护套件不再载入该退役算法重跑。
完整历史仍有原十处不匹配；本次保留恢复材料不把它们改报通过。
[当前整理记录](../../results/diagnostics/records_runtime_refactor_20261104/README.md)分别说明删除范围、整体结构与剩余项。

The later PID numeric-input revision and its exact recovery source are listed under [PID recovery](#retired-pid).
The original bfb source and five historical mappings remain unchanged.

上下文解析候选另保存准确`826ab3a1`的`experiments/lotus_records_chain.py`、
`experiments/lotus_records_cascade.py`与CPU异步引擎fixture三份旧源，五个variant仅追加15条来源映射。
原逐文件摘要、投影标识、来源版本和已有映射保持；完整3010条绑定仍有原10处差异，本次新增0。
候选源码和CPU检查见[主记录](../../results/diagnostics/records_config_refactor_20261103/README.md)，
保存文件只供历史还原，维护路径不导入它们。

取消收尾修订为五份历史投影共用的 `test_async_backend.py` 保留准确 `5ff54bfe` 原件，只追加五条来源映射。
维护测试覆盖剩余 deadline 与零等待，历史测试、逐文件摘要及投影身份保持原状；不改历史结果或关闭既有失配。

异步默认序列参数修正为共享的`vllm_engine.py`和`experiments/calibration/core.py`保留准确
`5ff54bfe`原件，五份投影只追加十条override；原逐文件SHA、投影ID、来源及旧显式配置不变。
这些副本只供历史还原，维护版异步路径不导入它们；未指定参数交vLLM解析不追认为旧实验设置。

独立 HTTP 配置修订将十份受影响的共享旧源按准确 `5ff54bfe` 字节保存在
[历史来源](historical_sources/5ff54bfe/)；五份投影追加 50 条来源映射，包含组织化 PG 的准备函数。
原逐文件摘要、投影标识、来源版本及已有映射保持，维护运行时不导入这些副本。
本次新增绑定不匹配已清零；`resources.py` 与 `session_policy.py` 的基线既有十处不匹配仍单列，
完整还原尚未通过，未修改其 expected 摘要或原检查。实际检查范围见
[HTTP 证据记录](../../results/EXPERIMENT_EVIDENCE_REGISTRY.md#http-call-configuration-cpu)。

资源归属候选修改了维护版 `experiments/calibration/core.py` 的 CPU 配额类型检查。
五份投影共享[准确 5ff54bfe 原件](historical_sources/5ff54bfe/code/src/experiments/calibration/core.py)，只追加五条来源映射；
原逐文件 SHA、投影 ID、来源版本和既有映射保持。旧投影仍使用原整核校准检查，维护代码继续使用小数配额检查；
本次保留旧字节不修改历史结果或扩大实机资格。检查状态见[CPU 记录](../../../code/tests/records_cpu.md#resource-allocation-cpu)。

组合时资源归属所需的校准旧源与异步序列修正共用同一份原件和五条映射；复用既有映射，不重复新增或计数。

未接入维护执行入口的 `scheduling/runtime/saor_pipeline.py` 与纯 CPU 微基准
`experiments/saor/control_benchmark.py` 已退出 `code/`，其实现、专属测试及原 runtime 包导出
按准确 `54ff2324` 字节保存在 `historical_sources/54ff2324/`。
五份投影各追加三条 override，原逐文件 SHA、投影 ID 和已有路由不变；两个专属测试只作历史来源保留，
不增加原投影文件清单。维护运行时不导入此归档，仍使用活的 Core、stage broker 和各自适配器；
归档原型不代表另一算法的等价替换或查询性能改善。

维护代码的模型进程导入直接指向 `serving/backends/`；旧 baseline 模块继续保留 import／pickle 兼容。
本次将四份受影响的旧调用方按准确 `6511b950` 字节保存在
`historical_sources/6511b950/`，五份投影只追加16条 override；原逐文件 SHA、投影 ID 及已有路由不变。
三个兼容模块与归档副本均不删除，历史还原树继续执行其原字节。

供给反馈与资源观测的维护改动另将四份共享旧源按准确`6a8a2d7b`字节保存在[历史来源](historical_sources/6a8a2d7b/code/src/)：`gpu_allocation.py`、`metrics/vllm.py`、`records_telemetry.py`与`stage_work.py`。
五份投影只补20条override；原逐文件SHA、投影ID、来源版本及已有映射保持。静态副本只供显式历史还原，当前维护运行时不导入这些文件；历史实测结果与观测边界不变。

异步token批提交改变了维护版`session.py`、`session_contract.py`和`async_backend.py`。
本地组合将这三份共享旧源按准确`2212e331`字节保存在`historical_sources/2212e331/`，五份历史投影只增加15条override。
原逐文件SHA、投影ID、来源及已有映射保持；快照仅供旧投影还原，维护运行时不导入这些副本。

无行数上限修正改变了维护版`batching.py`；五份历史投影共享[原f3文件](historical_sources/f3c259ac/code/src/scheduling/organization/batching.py)的单份精确副本，只追加五条override。旧逐文件SHA、投影ID、来源及已有映射不变；当前组批运行时继续使用维护版无行数上限逻辑，不导入此副本。

token组批的纯回执校验抽取改变了维护版`engine.py`；五份历史投影共享[原a421文件](historical_sources/a421c9d4/code/src/semantic_methods/lotus/engine.py)的单份精确副本，只追加五条override。原逐文件SHA、投影ID、来源及已有映射不变；维护运行时不导入此副本。

动态查询入口更新另为十一份受影响旧文件各保留一个精确副本，由五种历史复现共享23条新增来源映射。旧哈希、投影ID、来源与已有映射不变；这些冻结副本仅供无Git源码包还原，不参与维护版执行。

可选逐记录查询的代码变化为五份受影响旧文件各保留一个精确副本，五种复现按需共享；只追加21条来源映射，原文件哈希、版本摘要及已有映射保持。

进程工具归位另将原`lotus_records_supervisor.py`逐字节保存到`historical_sources/`，五份投影只增加该文件的override路由；原expected SHA、projection ID与source revision均保持，历史树不依赖新的`infrastructure/owned_processes.py`。

同步模型进程和阶段清理归位另保存原`daft_engine_process.py`与`lotus_cascade_process.py`两份共享原件，五份投影增加对应十条override；摘要与投影身份保持，历史树继续使用原实现，不导入当前兼容入口或新的serving模块。

阶段生成IPC归位另保存原`lotus_chain_engine_process.py`，五份投影只追加对应override；链worker及其原生消费者沿用已保存原件，旧锁、投影ID和来源不变。

运行依赖归位同样为受影响的公共来源保留精确原件，仅追加必要override；原观测状态、额度类型、Map请求值及比较入口继续按各投影原字节还原，不借当前中立组件改写旧源码锁或资格。

共享结果模块迁移与e2bc汇合的本地候选将九份原公共来源保存在`historical_sources/`：五份结果合同/消费者旧文件、旧环境预检和Movie矩阵说明，以及旧RayHTTP与importer。MANIFEST相对45d共新增43条override，保留双方既有路由，原expected SHA、projection ID、source revision及既有variant override保持；还原树继续使用原字节，不导入当前shim或新模块。此目录属于历史复现，不是生产共享实现。


维护代码在 `code/`；这里保留五种**已实测来源的实际差异文件**和逐文件摘要，不仅保留 patch，也不把不同 Core 策略合成新默认。没有模型、数据、机器配置或原始日志。

```sh
PYTHONPATH=code python3 -B code/scripts/experiments/materialize_native_baseline.py list
PYTHONPATH=code python3 -B code/scripts/experiments/materialize_native_baseline.py prepare \
  --variant core_local_window_pairs --output-dir /tmp/semloom-named-reproduction
```

此命令只从维护代码与具名覆盖文件构造独立代码树，逐项验证来源；不运行模型、不扫描参数、不分配调用、不连接服务器。生成的 `REPRODUCTION.json` 明确这是代码投影，原内容指纹不是Git提交。每一种覆盖文件匹配旧冻结代码，包含其当时的观测边界；不得借维护代码后续观测修订追认历史MFU。

维护代码的Core原评价器整合测试读取显式`SEMBENCH_CHECKOUT`，缺少时只跳过该整合用例。五份历史投影通过各自override保留旧测试字节及不变的代码投影ID；历史相对packet路径只属于原私有封包，不是新目录的可移植运行保证。

维护版`FilterMethod`要求显式非空instruction，Movie提示仅留在任务配置。为保持历史来源逐字节可重建，
`compatibility/explicit_instruction/`保存受影响原文件的共享精确副本，manifest只增加对应override映射；
已有五份variant文件、`code_files_sha256`和代码投影ID不变，旧隐含默认只属于历史版本。

独立树真实入口是 `code/scripts/experiments/run_movie_records_matrix.py`。完整运行需要新的显式封包、输入/token/模型/环境绑定、账本与资源owner；旧私有packet不随此目录发布，不复用旧账本或未知调用。`--help`不运行模型；不以源码还原或CPU通过冒称新树已获GPU资格。

- `native_isolated_and_imlane`：Native、Ray isolated、Daft isolated和IMLane作者worker路径。
- `core_original_dynamic`：cfdc具名原动态；实际已测source79e8f103。
- `core_local_window_pairs`：ee39具名局部窗口配对；实际已测source5007e95b。
- `ray_runtime_reuse`：只复用runtime，每查询仍可创建原生模型。
- `daft_single_thread_session`：显式不同Daft原生单线程session配置。

Kalypso为独立作者原生服务，不是LOTUS后端；见 `code/src/baselines/text/frameworks/kalypso_native/` 的适配、命令和未绑定项。

历史variant中的README按原字节保留，其相对链接属于当时文档语境；差异覆盖目录不是完整源码/文档树。materialize只保证manifest列出的代码投影与摘要，不承诺未列历史文档链接可用。当前维护入口与CPU资产路径说明见[代码入口](../../../code/README.md)及[CPU绑定](../../../code/tests/records_cpu.md)。

上下文兼容修订另将四份IMLane原host/backend/worker源按准确b018字节保存到`historical_sources/b018d88a/`，
五份投影只追加20条对应override；旧expected SHA、projection ID、source revision及已有路由保持。
全部3010条绑定仍仅有原10处不匹配，新增0；本修订不将完整历史还原写成通过。

旧脚本整理为 `estimate_operator_cost.py` 与 `pgai_sql_operator_profile.py` 保存准确 bfb 原件，
位置为 `historical_sources/bfb9100e/code/scripts/`。五份投影各增加两条来源映射，原摘要、投影身份和已有映射保持。
维护版本移除无调用私有签名 helper，并将可选数据库驱动延迟到实际连接时导入；旧投影继续执行原字节。
相关 20 项 CPU 检查及七项 CLI 解析检查见[整理记录](../../results/diagnostics/records_runtime_refactor_20261104/README.md)。


<a id="retired-saor-launchers"></a>
## 历史 SAOR 启动脚本

三条只服务于旧 Project 机制、直接供给上限和供给间隙实验的启动脚本退出维护目录：

- `code/scripts/experiments/run_saor_project_mechanism.py`
- `code/scripts/experiments/run_saor_feeding_ceiling.py`
- `code/scripts/experiments/run_saor_feeding_gap_diagnostic.py`

准确 bfb 原件保存在 `historical_sources/bfb9100e/code/scripts/experiments/` 的 gzip 文件；
manifest 的 `source_formats` 记录压缩摘要、准确解码长度，原逐文件 SHA 与投影 ID 不改。
五份投影新增十五条来源映射。现用 materialize 在写出前核验压缩摘要、长度及原字节摘要，
拒绝损坏、截断、越位路径或未知编码；已有输出目录不修改。普通来源继续使用原复制方式。
也可使用标准 gzip 读取单份原件；解码后的字节与旧 Git 来源一致，不能据此宣称完整旧环境已恢复。

维护的离线结果分析、原始数据和 native baseline 保留。这三条旧命令只属于历史运行，
现行入口见[脚本说明](../../../code/scripts/README.md#current-entry-roles)。
相关 CPU 与三个恢复后 CLI 解析检查见[整理记录](../../results/diagnostics/records_runtime_refactor_20261104/README.md#历史启动脚本退出维护目录)。
3010 条来源仍有原十处不匹配，完整历史还原仍报告这些问题，不改报全部通过。


<a id="retired-oneoff-tools"></a>
## 一次性工具的准确历史来源

以下入口退出维护目录，准确原字节以 gzip 保存在 `historical_sources/4fec5472/` 的对应路径：

| 旧路径 | 当前处理 |
|---|---|
| `code/scripts/profiling/daft_text_organizer_smoke.py` | 生产组织器与维护行为测试保持；旧入门 CSV 探针只供明确历史恢复 |
| `code/scripts/baselines/time_duckdb_ai_baseline.py` | 原生 DuckDB adapter 与统一 baseline 入口保持；旧单分片外推探针仅作历史来源 |
| `code/scripts/data/build_opening_multijob_manifests.py` | 原短长 Job 选择算法保留准确来源；不把另一数据生成器当作等价替换 |

五份投影只新增十五条来源映射，原逐文件摘要、投影身份和既有映射不改。恢复时继续核对
压缩摘要、原长度和解码摘要；原始 manifest 与实验结果不变。维护生产路径不导入这些原件。
当前指引从[脚本入口](../../../code/scripts/README.md#current-entry-roles)查找；
[逐文件清单](../../results/diagnostics/records_runtime_refactor_20261104/raw/entry_inventory.json.gz)为静态调用与文档证据，
不能代替仓库外调用检查或真实模型资格。


<a id="retired-ack-source-replay"></a>
### 旧 ACK 语法树重放的同版本恢复

日常套件已移除 `code/tests/execution_provider/ack_order_fixture.py`、
`code/scripts/analysis/check_sync_cpu_evidence.py` 和一个专属重放测试。
它们以当前方法为模板反向改写一次等待，再核对旧语法树摘要；测试正常整理后摘要已不匹配，不能据此判断当前 ACK 行为。
当前直接事件 ACK 测试、固定公开 vLLM 方法检查和公开 CPU 副本恢复继续保留。

删除前完整字节来自 `5a19221be439d9b6d7027cef21766e4293ffb257`，文件长度／SHA在[本批记录](../../results/diagnostics/records_runtime_refactor_20261104/README.md#retired-ack-source-replay)保存。
旧演示应使用完整 `55d8776d50511898fe0459ddbee38289b154361e` 树；静态核对该树改写后的摘要仍为原 `8aa32770…`，本批没有重跑其 IPC 演示。

```sh
git worktree add --detach "$REPLAY_CHECKOUT" 55d8776d50511898fe0459ddbee38289b154361e
```

目标须为新目录；命令只恢复源码。原命令、依赖和原结果按该版本读取，不将旧 helper 拷回当前套件。
原[受控时序记录](../../results/diagnostics/records_runtime_refactor_20261104/raw/sync_ack_order_control.json)及失败、公开归档均未改写。

<a id="retired-stage-work-controller"></a>
### 旧工作量控制原型退出生产目录

`BoundedStageWorkController` 与 `WorkCreditDecision` 已从维护版 `stage_work.py` 移除；
当前执行入口没有使用这两项。准确原件继续保存在
[既有历史源码](historical_sources/6a8a2d7b/code/src/scheduling/submission_control/stage_work.py)，
五份投影的文件摘要、恢复映射和投影 ID 均不改。
原三项行为测试随历史来源保留，维护套件不再执行这个已退出生产的控制原型。
维护模块不再支持旧类型直接导入或任意旧 pickle 对象恢复；具名历史树保留原类路径与字节。
维护版供给／请求反馈与实际 stage broker 保留；显式历史 profile 随后按下节移出现用入口；
[逐项依据与检查](../../results/diagnostics/records_runtime_refactor_20261104/README.md#stage-work-retirement)
区分本次退役与仍需兼容的内容。


<a id="retired-request-feedback"></a>
## 旧请求反馈的 Git 恢复

维护代码已删除旧 `legacy_resource_feedback`、`delivered-work-scan`、`computed-work-local-pairs`、
`AscendingRequestController`、旧请求适配器、跨查询 hint 缓存及其专属测试。当前请求策略使用
`resource_feedback`；独立批反馈、共享工作量比较、资源采样和现代请求控制器保持。
旧机制不是当前运行器的兼容开关。需要重查原行为时使用准确完整 Git 树：

```sh
git worktree add --detach /tmp/semloom-request-history-9da 9da299e90f79fd0b71f2a4983a90884f8ba960b5
```

目标目录须为新目录；这里只恢复源码，不启动 Ray、模型或服务器。旧机制的配置、说明与测试在该树按原实现读取；退役时已做后续校正的测试按逐项清单的 `3fd3887e` 来源恢复。
不要将个别旧文件复制回维护树。原五份具名投影、已有逐文件摘要及恢复映射不改；它们与整个 9da 树用途不同。
[本批清单](../../results/diagnostics/records_runtime_refactor_20261104/README.md#legacy-request-retirement)
记录逐文件／方法摘要、保留的现代行为及未完成的真实运行检查。


<a id="ray-profiler-layout"></a>
### Ray 画像模块职责整理的历史来源

维护版将调度器组装移到 `ray_scheduler.py`，任务和 actor 提交保留在 `ray.py`，并删除无生产调用的旧参数包装。
五份具名投影继续使用[准确 965 原件](historical_sources/9652108d/code/src/observability/profiling/ray.py.gz)；
只新增五条来源映射及 gzip 格式记录，原逐文件摘要、投影 ID 和既有映射不改。
恢复出的旧 `ray.py` 包含其原函数，不导入新模块；本批前后五份历史树均逐文件重建通过。
[职责与检查](../../results/diagnostics/records_runtime_refactor_20261104/README.md#ray-profiler-layout)
不代表旧模型环境、真实 Ray 或性能已经验证。


<a id="scheduler-types-layout"></a>
### 调度类型整理的准确历史来源

维护版将七个配置、结果及提交接口类型移到 `scheduler_types.py`，原模块直接导入同一类型。
五份历史投影共用[原调度器](historical_sources/9652108d/code/src/scheduling/core/scheduler.py.gz)
和[原执行接点](historical_sources/9652108d/code/src/scheduling/runtime/execution.py.gz)两份准确 965 字节。
只新增十条来源映射及 gzip 格式记录，原摘要、投影身份和既有映射保持；旧树不依赖新类型文件。
[职责与检查](../../results/diagnostics/records_runtime_refactor_20261104/README.md#scheduler-types-layout)
记录改前／改后完整恢复，不增加真实 Ray 或模型资格。


<a id="neutral-shared-tool-imports"></a>
### 共用工具消费者的原始导入

当前执行与观测消费者直接使用已有 infrastructure 工具；五份具名投影仍恢复原 baseline 导入。
只有原先直接读取维护树的
[image worker identity](historical_sources/d559b942/code/src/modalities/image/worker_identity.py.gz)与
[process recorder](historical_sources/d559b942/code/src/observability/process_resources/recorder.py.gz)
需要保存准确 d559 原件，新增 10 条来源／gzip 格式记录。原投影身份和逐文件摘要不改，完整五树重建通过。
[首次元数据错误与修正](../../results/diagnostics/records_runtime_refactor_20261104/README.md#neutral-shared-tool-imports)
保留原检查结果；当前实现没有复制工具函数或增加旧路径包装。


<a id="movie-http-retirement"></a>
### 旧 Movie LOTUS HTTP 与固定 campaign 已退役

当前树删除旧 `lotus_native_local_model`、固定 `sembench_records_formal`、其资源采样器／脚本及专属测试。
Movie Session 不再接受旧 `lotus-native` backend；Animals 的同名选项、PG HTTP、Reference、Kalypso 与 SAOR 保持独立。
现用 Matrix 的 read／sha／validate_result 原函数归 `movie_matrix_support`，QueryClock 原类归
`observability.native_query_timing`，没有复制执行器或将旧 35 单元实验改成另一套实验。

需要重查原机制或此前 HTTP 参数修订时，使用准确 Git 源码：

```sh
git worktree add --detach /tmp/semloom-movie-http-history-646216fa 646216faa98e0a1a234c7fc74f9f3c0d216e96b2
```

目标须为新目录；此命令只恢复源码，不启动模型或服务器。原专属命令、38 个退役测试和结果说明在该树读取。
现有五份具名投影已经保有其准确旧 HTTP 实现；新增保存仍直接读取维护树的
[旧脚本](historical_sources/646216fa/code/scripts/experiments/run_sembench_records_formal.py.gz)、
[旧资源采样器](historical_sources/646216fa/code/src/experiments/sembench_records_formal_resources.py.gz)和
[原 Matrix 测试](historical_sources/646216fa/code/tests/experiments/test_movie_records_matrix.py.gz)，
只增加 15 条来源与 gzip 格式记录，原摘要和投影身份不改。
[本次组合记录](../../results/diagnostics/records_runtime_refactor_20261104/README.md#current-runtime-cleanup-composition)
分别解释删除、迁移、保留和仍待验证的内容。

## 现用 PG／HTTP 清理与原生 LOTUS 恢复（2026-11-05）

旧Movie HTTP和PG／HTTP实验入口继续退役；[逐文件归档](../paused_pg_and_retired_http_20261105/README.md)保留PG恢复来源。
本组合将原生LOTUS Movie以原LM＋本地模型IPC接回普通session，只有CPU资格，非旧HTTP owner复活；
Q2b视觉非HTTP替换未完成。历史原始ID／SHA保持，优先复用已公开的压缩旧源，不复制第二份同字节来源。


<a id="local-occupancy-projection"></a>
## 本地占用投影整理前的 topology 源码

当前实现将同步调度的纯本地占用投影放入已有 topology 模块，删除调度器的旧静态方法。
五份历史版本共用[准确原件](historical_sources/1d38e256/code/src/scheduling/core/topology.py.gz)，
来自 `1d38e256bf3d5abf52ed6cc9af9814362bf93297`；解码 691 字节，与原预期 SHA-256 相同。
只增加五条来源和相应 gzip 格式记录，原 3,010 个文件摘要、五个投影身份及其他映射保持。
历史调度器与测试继续读既有原件，不导入新函数；十处旧来源差异保留，完整恢复仍未通过。
[本批职责和检查](../../results/diagnostics/records_runtime_refactor_20261104/README.md#local-occupancy-projection)
不提供模型环境、真实 Ray 或查询性能结论。


<a id="retired-profiler-replay"></a>
## 旧 profiler 重放接线的准确原件

当前维护代码移除 replay.py 的八个旧参数接线函数，实际五个 Arrow 转换函数由 batch_envelopes.py 负责。
[完整原件](historical_sources/7bf1d92b/code/src/observability/profiling/replay.py.gz)来自准确
`7bf1d92baf2611925c608cfe76b4a766855bd77e`，30485 字节、845 行，与五份投影原预期摘要及此前维护字节相同。
只增加五条来源和 gzip 格式记录，原 3,010 个文件摘要、五个投影身份及其他映射保持；历史仍有十处旧差异。
原 PG profiler 调用方可从同一准确 Git 树恢复，源码和恢复说明见[原实验归档](../paused_pg_and_retired_http_20261105/README.md)。
维护 SDK 不再提供这些旧私有函数或参数；调用方提供的 typed replay_envelopes 与共享调度算法继续保留。
[当前改动和 CPU 检查](../../results/diagnostics/records_runtime_refactor_20261104/README.md#retired-profiler-replay)
不表示完整历史环境、模型或性能已恢复。


<a id="retired-ray-http-polling"></a>
## 旧 Ray HTTP metrics 轮询的来源

现用 `profiling/ray.py` 只保留 static 与调用方提供 admission 对象的提交；旧字典轮询及独占的
`RoundRobinSubmitter` 已删除，旧字典与旧类的仓外直接调用不再兼容。
`ray.py` 仍使用五份投影原有的准确 965 gzip 来源；原 expected 摘要和投影身份不变。
原 `ray_adapter.py` 为 17,434 字节，SHA-256 为
`ed88226891e5971d0904b31ca41d3fa684d381db23c5cfc94b8dfb134df37bef`，单份准确原件保存于
[历史 gzip](historical_sources/c5b49e85/code/src/scheduling/runtime/ray_adapter.py.gz)，五份投影仅增加该路径及 gzip 格式记录。
现用 actor worker 轮转和记账继续维护，不从历史文件导入。
[行为与完整恢复检查](../../results/diagnostics/records_runtime_refactor_20261104/README.md#retired-ray-http-polling)
记录最终 29 项 CPU 和五份共 3,010 文件的完整恢复；这不是历史模型环境验证。


## 当前引擎省略值与历史参数

当前 dtype／显存比例的省略行为、普通 async 的 R 入口及 typed IPC 默认已在维护版调整；
五份具名投影继续使用既有准确旧来源，没有把 None 写回旧文件或改写旧成绩。
[组合 CPU 与完整恢复](../../results/diagnostics/records_runtime_refactor_20261104/README.md#native-defaults-capacity-combination)
核对五份共 3,010 文件及原身份；这不表示历史模型环境已重演。


SDK 的 R 省略及底层 IPC 默认现已调整；五份投影继续使用准确旧源码。
`resident_udf.py` 的旧文件从已发布 0677 保存到[原件 gzip](historical_sources/0677c7a3/code/integrations/imlane_records/resident_udf.py.gz)，
manifest 为五份投影各增加一条旧源路由及对应格式，原 expected 摘要和投影身份保持。
[317 项组合检查](../../results/diagnostics/records_runtime_refactor_20261104/README.md#sdk-ipc-capacity-followup)含五份共3,010文件完整恢复；没有重演历史模型环境。

<a id="imlane-explicit-capacity-sources"></a>
## IMLane 显式容量接线前的准确源码

当前桥的 CLI、ready 与结果 carrier 改为使用同一显式请求数；五份具名版本继续使用旧代码。
六份准确旧源保存于 `historical_sources/9ae29579/code/integrations/imlane_records/`：
`client.py`、`imlane_records.cpp`、`method_environment.py`、`record_dbend_adapter.hpp`、`records_checks.cpp`、`records_host.py`。
来源为 `9ae2957953a8cca2cb7a63e375dc69a681022de6`，各 gzip 解码后仍匹配原 expected 摘要；
manifest 仅增加30条来源和30条格式记录，五份投影身份、原3,010文件摘要保持。
IPC 拆分涉及的四个历史文件继续使用已有19条准确路由，没有重复复制或新增同字节原件。
[组合源码及完整恢复](../../results/diagnostics/records_runtime_refactor_20261104/README.md#runtime-structure-and-bridge-capacity)
覆盖五份投影；旧模型环境与作者桥未运行。

<a id="retired-admission-variants"></a>
## 旧 EWMA／HOL 控制器恢复

无人使用的 EwmaAimdAdmissionController、HolAgeAimdAdmissionController 及 HolAgeAimdConfig 已退出当前导出，
八项专属测试随机制退役；通用 gate 的队首等待值传递检查继续保留，不再依赖旧策略。
五份具名投影的 adaptive／包导出继续使用原有准确来源，三定义可在这些旧文件中恢复，未新增副本或改写原摘要。
完整清理前代码和测试从[准确 cde87a55 Git 树](https://github.com/3444374/ai-operator-execution-optimization/tree/cde87a55cc4795e33aa07d07f53adc922d8ee71c)
在独立树恢复，不把旧策略重新加入当前搜索路径，也不自动恢复历史模型环境。
[本批源码、328项检查及原失败](../../results/diagnostics/records_runtime_refactor_20261104/README.md#feedback-duties-and-record-copies)
包含全部五份共3,010文件的完整恢复；现用批反馈、AIMD基类、其他平滑统计与观察字段保持。


<a id="retired-choice-observer-sources"></a>
## 旧 HTTP 观测清理后的来源（2026-11-06）

旧 Choice 观测模块和专属测试退出维护树；仍被使用的请求守卫迁入现有 AttemptLedger 模块，
当前 native HTTPX观察、mapped预算和PG provider不改行为。
删除清单、完整Git恢复与旧机制用途见[准确5539来源](../paused_pg_and_retired_http_20261105/README.md#retired-choice-http-observers)。

修改前以 `materialize_native_baseline.read_source` 读取全部30条受影响绑定，
其余文件已有原overrides或不在五份投影中；只有 `gateway_observer.py` 原先取维护树，须保存一份准确旧源：

- 来源：`5539a19ff85f9dd96524f10c8c1197b399a04fb8`。
- 原字节：4450；解压SHA256：`98ea566daeddad686d3c44d9cc4c00e214485ec178e71e6bda0beb79db1199e2`。
- 共享原件：[`gateway_observer.py.gz`](historical_sources/5539a19f/code/src/experiments/gateway_observer.py.gz)；五份投影只追加对应的5条来源／gzip映射。

原五个来源版本、投影ID、3010条expected和所有已有overrides／格式声明不改。
本批纯CPU检查已完整构造五树并逐文件核对3010份摘要，不执行历史模型或实验命令；
当前代码不导入这个压缩原件。旧结果、负结果与原失败继续按原身份解释。

<a id="retired-lotus-http-methods"></a>
## 旧 LOTUS HTTP 执行与固定两 Map 原型（2026-11-06）

维护目录删除孤立的 HTTP 请求准备、响应重放、固定两 Map 方法及其专用 driver／limits。
当前 records 使用原生模型 IPC，有限记录链、通用 MethodDriver、Filter cascade、图像原方法及基线执行保留。
[sdk.py](../../../code/src/semantic_methods/lotus/sdk.py)只记录已安装 SDK 的来源，继续支持
`text` 和 `lotus-image-text-v1`，供[原查询复算](../../results/diagnostics/main_queries_20261015/recompute.py)
与[原服务复算](../../results/diagnostics/main_service_auto_20261015/recompute.py)读取。
`source.py` 的 provider 来源观察另被当前 MMQA 聚合使用，没有随旧执行实现删除。

退役前源码位于完整 Git 树 `e5669213ea8b74e5adcd5040c5dfc4000fd3f8d2`：

| 文件 | 原字节数 | SHA-256 |
|---|---:|---|
| [sdk.py](https://github.com/3444374/ai-operator-execution-optimization/blob/e5669213ea8b74e5adcd5040c5dfc4000fd3f8d2/code/src/semantic_methods/lotus/sdk.py) | 16669 | `1f83e75775e7396ee30bc1125255eda32c52946eb53daf80ccf8294e1a5385e2` |
| [maps.py](https://github.com/3444374/ai-operator-execution-optimization/blob/e5669213ea8b74e5adcd5040c5dfc4000fd3f8d2/code/src/semantic_methods/lotus/maps.py) | 5832 | `a66442c2a7746cb58b5139e1fcbb98a5b6782c5c8da6625cf8a2d1b5b49cd702` |
| [driver.py](https://github.com/3444374/ai-operator-execution-optimization/blob/e5669213ea8b74e5adcd5040c5dfc4000fd3f8d2/code/src/semantic_methods/lotus/driver.py) | 4664 | `730b8eab338e7cac9394201c48653c95a7f13e9944c867f4b253798550a45ced` |
| [limits.py](https://github.com/3444374/ai-operator-execution-optimization/blob/e5669213ea8b74e5adcd5040c5dfc4000fd3f8d2/code/src/semantic_methods/lotus/limits.py) | 2824 | `cb26796e580f23ed0925aaceec2a6b64ef06876357ebbab61d07a65f26363af1` |

两 Map 研究源码从完整 e566 树恢复，不把旧模块复制到当前导入路径，也不据源码快照补记运行资格。
历史实验仍按其原结果指定的版本和环境复算。

删除前已实际调用 `materialize_native_baseline.read_source` 核对四模块的20条历史绑定。
SDK、driver 和 limits 继续取已有 `historical_sources/5ff54bfe/` 原件，Map 取已有
`historical_sources/1f92fce5/` gzip；均与原预期摘要一致，两份维护测试不在旧投影。
因此不增加压缩副本、不改 MANIFEST、原3010条摘要、投影身份、来源版本或既有读取位置。
本批14个现有 CPU 方法全部通过；其中[完整恢复用例](../../../code/tests/experiments/test_native_baseline_reproduction.py)
实际构造五份投影并逐文件核对3010份摘要，未运行旧模型或历史实验命令。

<a id="retired-fixed-local-probe-suites"></a>
## 本地固定测量套件的准确来源

`mapped_core_probe` 当时只编排 `map_observation_probe.run_case` 的固定两组对照，
`request_budget_batch_probe` 只测量固定规模的 SQLite 预留循环；两者没有当前生产调用。
这四份源码退出维护入口，不表示公共 Map／UDS provider、共享预算或观察方法退役。
当时保留的 `run_case` 随后与旧HTTP工厂一起退役，见[后续来源](#fixed-http-adapter-sources)。
公共 `reserve_many`、计数失败和unknown处理保持，
`mapped_request_probe` 的独立跨进程组件诊断也继续维护。

恢复旧固定矩阵应使用完整 Git 树
[`6ec2dde0e626c72f53b38a4f46222a4801f865f1`](https://github.com/3444374/ai-operator-execution-optimization/tree/6ec2dde0e626c72f53b38a4f46222a4801f865f1)
及其配套测试。不要将单个旧模块放回当前导入路径；恢复源码不代表已执行旧矩阵或重新取得模型结果。

| 删除路径的准确 Git 来源 | 字节 | SHA-256 |
|---|---:|---|
| [code/src/experiments/mapped_core_probe.py](https://github.com/3444374/ai-operator-execution-optimization/blob/6ec2dde0e626c72f53b38a4f46222a4801f865f1/code/src/experiments/mapped_core_probe.py) | 3593 | `4c39db5e14dd89aa0befca4d48b3b10fd0997c16718dc30e524f5a2aa2ac6d6b` |
| [code/src/experiments/request_budget_probe.py](https://github.com/3444374/ai-operator-execution-optimization/blob/6ec2dde0e626c72f53b38a4f46222a4801f865f1/code/src/experiments/request_budget_probe.py) | 5713 | `39d822c50b52459d6f06ff10338107d7853e6c8c69f0c4b49160fb92bcafde1c` |
| [code/scripts/profiling/mapped_core_probe.py](https://github.com/3444374/ai-operator-execution-optimization/blob/6ec2dde0e626c72f53b38a4f46222a4801f865f1/code/scripts/profiling/mapped_core_probe.py) | 166 | `46c9bdd86d781c9b8e8ea0b9800e50b1969e6b02c1e958b956366d8e224cda3d` |
| [code/scripts/profiling/request_budget_batch_probe.py](https://github.com/3444374/ai-operator-execution-optimization/blob/6ec2dde0e626c72f53b38a4f46222a4801f865f1/code/scripts/profiling/request_budget_batch_probe.py) | 166 | `c84315ad515813aad7532a5735e8cf881637e1cea03334f7fd96718a00dc0649` |

五份历史投影的两个源实现已从各自原有 `7bf1d92b` 来源读取，本次不改这些映射。
原来依赖当前文件的两个 CLI 分别保存为
[Core CLI 原件](historical_sources/6ec2dde0/code/scripts/profiling/mapped_core_probe.py.gz)和
[批记账 CLI 原件](historical_sources/6ec2dde0/code/scripts/profiling/request_budget_batch_probe.py.gz)。
只增加十条来源及 gzip 格式记录；原 3,010 条文件摘要、五个投影身份、原来源版本和已有映射保持。
旧测量数据、失败记录与离线复算脚本继续保留；本次不重跑旧性能测量。


<a id="dispatch-close-source"></a>
## 连接关闭整理前的派发源码

`session_dispatch.py` 的空输入／坏帧路径删除重复关闭，连接仍由原 `finally` 负责；版本派发、返回和错误处理不变。
同处不可达的 `return` 与固定端点适配器中未使用、未列入 `__all__` 的常量导入一并删除；实际 v3／v4／v5 会话和固定端点执行器保留。
派发原件来自完整 `6ec2dde0e626c72f53b38a4f46222a4801f865f1`，2,171字节，SHA-256 为
`79ece85a85eb7e04e635f35d0028d452842314400946f2d4ca0b2e62f002af48`，保存于
[压缩原件](historical_sources/6ec2dde0/code/src/execution_provider/session_dispatch.py.gz)。
五份投影仅追加该原件的5条读取及gzip格式路由，原文件摘要、身份、来源版本和已有映射保持；固定端点适配器继续读取原有5ff来源。

<a id="retired-native-system-matrix"></a>
## 旧 SAOR 矩阵执行与配置接线

旧启动器退役后，维护代码没有调用 `run_matched_system`、配置 loader／parser 或专属 preflight。
三个模块退出当前入口；SAOR 和多任务共享额度算法、14份研究资料、原始结果及离线汇总保持。
汇总所需的三个身份常量归入 `native_system_contract.py`，文件／JSON 摘要归入 `native_system_evidence.py`，
结果文件读取归入 `native_system_artifacts.py`，最终队列／额度核对归入 `native_system_validator.py`。
函数内容和摘要字节规则保持；没有为旧配置保留转发入口，也没有重新签发实验许可。

退役前的准确完整 Git 树为
[`87fdb2bc723a0b5fc8f743cc8faa62020a7c0864`](https://github.com/3444374/ai-operator-execution-optimization/tree/87fdb2bc723a0b5fc8f743cc8faa62020a7c0864)。
恢复旧机制应使用完整同版本树和其历史说明，不能将单个旧 runner 接到当前 provider。
该版本本身已经没有更早的启动脚本；原启动实验的完整接线从
[`7bf1d92baf2611925c608cfe76b4a766855bd77e`完整树](https://github.com/3444374/ai-operator-execution-optimization/tree/7bf1d92baf2611925c608cfe76b4a766855bd77e)
及[原恢复说明](../paused_pg_and_retired_http_20261105/README.md)读取，不与87fd单文件拼接。
本次不把旧许可模板或旧实验结果当作新的运行安排。

| 删除路径 | 字节 | SHA-256 |
|---|---:|---|
| [code/src/experiments/saor/native_system_matched.py](https://github.com/3444374/ai-operator-execution-optimization/blob/87fdb2bc723a0b5fc8f743cc8faa62020a7c0864/code/src/experiments/saor/native_system_matched.py) | 84734 | `39022e04da155c9be19ba758aa53c8176c853f606969f0b4795469745b057868` |
| [code/src/experiments/saor/native_system_parser.py](https://github.com/3444374/ai-operator-execution-optimization/blob/87fdb2bc723a0b5fc8f743cc8faa62020a7c0864/code/src/experiments/saor/native_system_parser.py) | 4861 | `0a6a4780b96e8039e33db830f65d0c983e6a9942e9d7e6f8665818da647817fc` |
| [code/src/experiments/saor/native_system_preflight.py](https://github.com/3444374/ai-operator-execution-optimization/blob/87fdb2bc723a0b5fc8f743cc8faa62020a7c0864/code/src/experiments/saor/native_system_preflight.py) | 14169 | `b64c79d0fd90fc2fb9ea49a83c83f5a694870f094a83b9eebacfd75a136eabe4` |

五份历史代码投影原已从 `7bf1d92b` 读取 matched、summary 和 evidence，本次不改已有来源。
parser、contract、artifacts、validator 和 preflight 的准确原件分别保存在
[`historical_sources/87fdb2bc/code/src/experiments/saor/`](historical_sources/87fdb2bc/code/src/experiments/saor/)。
五份投影只增加25条来源及gzip格式记录，原3,010条文件摘要、五个投影身份、来源版本和其他映射保持。

<a id="retired-shared-vllm-loader"></a>
## 共享 vLLM 旧加载与命令接线

当前离线分析和研究方法只使用 `config.py` 中的类型、字段默认值、容量运算和参数读取函数。
没有当前调用的21个 JSON 加载／命令函数、4个专用字段表和 `cli.py` 已退役；
共享额度、SAOR 算法、现用证据／指标函数以及图像的同名命令构造器保持。
不为旧 runner 或已不存在的包导出恢复转发入口，原始结果和14份研究资料不改。

此次准确原源码来自完整
[`128f64e052152fef2a5a3291517746661553cedf`](https://github.com/3444374/ai-operator-execution-optimization/tree/128f64e052152fef2a5a3291517746661553cedf)。
旧 `config.py` 可从该树直接读取；它在五份历史投影中继续使用已有 `b08936d2` 准确原件，
本次没有重复保存或修改已有映射。删除的 CLI 为 `3007` 字节，SHA-256 为
`d5d99efb9db4f8006cb52b96d08a7e25779545bb561eeeb06187b08d558906ca`，准确原件见
[cli.py.gz](historical_sources/128f64e0/code/src/experiments/shared_vllm/cli.py.gz)。
五份投影只增加该原件的5条来源及gzip格式记录，原3,010条文件摘要、投影身份和来源版本保持。

128f保存的是此次退役前的库文件，原 runner／direct control 启动器此前已经移出当前代码。
恢复更早的完整执行接线应使用
[`7bf1d92baf2611925c608cfe76b4a766855bd77e`完整树](https://github.com/3444374/ai-operator-execution-optimization/tree/7bf1d92baf2611925c608cfe76b4a766855bd77e)
和[原恢复说明](../paused_pg_and_retired_http_20261105/README.md)，不能将旧 loader 单文件接到当前执行器。

<a id="retired-shared-runner-support"></a>
## 旧共享运行器辅助和批量观察接线

旧共享运行器退出后，`shared_vllm/evidence.py` 和 `runtime.py` 没有当前执行入口，
后者只剩一项旧采样适配测试。两模块及该专属测试退出当前代码；公共主机／GPU读数、
现用指标、`preflight.wait_for_idle`、工作证据和 SAOR／共享额度算法保留。
`shared_vllm.config` 中只被该旧运行器使用的 `RunnerOptions`／`GroupRunIdentity` 一并退役；
现用场景 CLI 自己定义的同名 `RunnerOptions` 保持。

`BatchedRequestGuard` 原接于已退役的 Choice 观察器，现随专属测试退出当前代码。
公共 `reserve_many` 和普通请求守卫保留，[原批记账负结果](../../results/diagnostics/request_budget_batch_20261003/README.md)
及原始证据不改；本次没有重跑旧测量或改变请求、结果、unknown 的实际处理。

此次删除前的完整准确源码为
[`e86afedb91e5d913ebb2f988cbfd5f305b4c0482`](https://github.com/3444374/ai-operator-execution-optimization/tree/e86afedb91e5d913ebb2f988cbfd5f305b4c0482)。

| 删除路径 | 字节 | SHA-256 |
|---|---:|---|
| [code/src/experiments/shared_vllm/evidence.py](https://github.com/3444374/ai-operator-execution-optimization/blob/e86afedb91e5d913ebb2f988cbfd5f305b4c0482/code/src/experiments/shared_vllm/evidence.py) | 29389 | `c83e2f707d07b3ae7582bbe4690585da0f167023178c59d69c8a4bf35848adf8` |
| [code/src/experiments/shared_vllm/runtime.py](https://github.com/3444374/ai-operator-execution-optimization/blob/e86afedb91e5d913ebb2f988cbfd5f305b4c0482/code/src/experiments/shared_vllm/runtime.py) | 13188 | `5e03d4af0276076085250046505489b62b73efce14137c2e078ea590b3612c4c` |
| [code/src/experiments/batched_request_guard.py](https://github.com/3444374/ai-operator-execution-optimization/blob/e86afedb91e5d913ebb2f988cbfd5f305b4c0482/code/src/experiments/batched_request_guard.py) | 3833 | `3596b70e501e4738979e1f6e5ef6e4f9c5f8fef5eb74aecfb96b75c8dc57c4a2` |
| [code/tests/execution_provider/test_batched_request_guard.py](https://github.com/3444374/ai-operator-execution-optimization/blob/e86afedb91e5d913ebb2f988cbfd5f305b4c0482/code/tests/execution_provider/test_batched_request_guard.py) | 7464 | `ee929c5d89e3526a7e27904789a1586a39886b3b2d364344258643f2898132b0` |

五份代码投影原已从 `b08936d2` 读取 runtime 和 config；专属Guard测试与新增采样测试不在其原文件列表中。
本次只为 evidence 和 Guard 两份原件增加10条来源及gzip格式记录，保存于
[`historical_sources/e86afedb/code/src/experiments/`](historical_sources/e86afedb/code/src/experiments/)。
原3,010条文件摘要、五个投影身份、来源版本和已有映射保持。
恢复旧运行器的整套启动接线仍使用
[`7bf1d92baf2611925c608cfe76b4a766855bd77e`完整树](https://github.com/3444374/ai-operator-execution-optimization/tree/7bf1d92baf2611925c608cfe76b4a766855bd77e)
及[原恢复说明](../paused_pg_and_retired_http_20261105/README.md)，不将旧单文件接回当前执行器。


<a id="retired-observation-aliases"></a>
## 观测旧导入和分阶段采样归位

图像CLI、观察器和当前测试直接导入`src.observability`的时钟、事件、模型复用与请求身份。
`src.experiments.buffered_events`、`execution_timing`、`image_reuse`、`request_identity`四个转发路径删除，
`ProcessSampler`移至`src.observability.process_resources.phase_sampler`，旧`experiments.process_sampling`不再导出。
类逻辑、事件字段和采样默认保持；三份观测测试归入`tests/observability`。

准确退役前文件、旧测试及当时所有调用在
[e86afedb完整来源](https://github.com/3444374/ai-operator-execution-optimization/tree/e86afedb91e5d913ebb2f988cbfd5f305b4c0482)恢复。
本次未新增这些文件的副本或恢复映射：五份具名投影此前已读取各自准确原件，读取值保持。
旧直接导入不再兼容；需要旧API时使用完整隔离来源，不将转发壳加回当前搜索路径。
原图像和观测结果、失败记录以及尚在使用的HSE执行保持各自身份。

<a id="caller-query-duration-sources"></a>
## 整查询期限改动的准确旧来源

当前records默认不设整查询总期限；历史具名实验继续读取原源码和显式配置。
此次两份原文件保存在[87ab来源](historical_sources/87ab621f/)：IMLane的`resident_model.py`及原生LOTUS的`lotus_chain_native_relation.py`。
各自准确87ab字节与原预期摘要一致，五份投影只各增两条来源／格式映射，全部expected与投影身份保持。
完整旧接线从[87ab准确树](https://github.com/3444374/ai-operator-execution-optimization/tree/87ab621f0b788887459aadaeb6986942bd3c1a7a)恢复，
[本批171条读取与增量核对](../../results/diagnostics/records_runtime_refactor_20261104/README.md#caller-query-duration)与完整3010条物化分开记录。

<a id="fixed-http-launcher-source"></a>
## 旧 Fixed HTTP 入口退役时的来源读取

旧网关隔离诊断与研究服务入口的五份文件从当前树删除；
[完整f526恢复说明](../paused_pg_and_retired_http_20261105/README.md#retired-fixed-http-launchers)保存各自用途、字节数和摘要。
历史读取实际涉及10条绑定；五条同名CLI原先取当前文件，现改读
[准确f526原件](historical_sources/f526e6bd/code/scripts/profiling/gateway_isolation_probe.py.gz)。
其他五条继续读取已有原件；原expected、投影身份、版本和旧来源映射保持。
只复查受影响来源，不把本批检查写成整套3010物化或真实服务验证。

当前既有IPC生命周期用例改用未指定查询期限；旧五份投影仍需原测试字节，因此共用
[准确6fc旧测试](historical_sources/6fc10c21/code/tests/serving/test_async_engine_process.py.gz)。
只补五条来源／格式映射，旧expected和投影身份保持；后续准确e5的实际IPC结果见[服务器回执](../../results/diagnostics/records_runtime_refactor_20261104/README.md#single-query-ipc-e5-server)。


<a id="backend-package-import-source"></a>
## 后端包去除 HTTP 聚合导入时的源码

中性vLLM规范的导入不再自动加载本仓completion／embedding HTTP实现；
具体实现由真正需要它的调用方从所属模块显式导入，未增加惰性转发表。
五份历史投影中的 `code/src/serving/backends/__init__.py` 继续使用准确
[`e5e65fdb6db14f5f422ed4e3983e2206fbc75dfd` 完整树](https://github.com/3444374/ai-operator-execution-optimization/tree/e5e65fdb6db14f5f422ed4e3983e2206fbc75dfd)
的[原件](historical_sources/e5e65fdb/code/src/serving/backends/__init__.py.gz)。
只新增读取位置和压缩格式；原expected、来源版本、投影身份及已有映射不变。
本批仅检查五条读取与纯CPU导入／值行为，没有重跑3010物化或模型，也未删除全部HTTP实现。

<a id="hse-metrics-source"></a>
## HSE指标提取时的来源读取

现用HSE的调度／资源清理继续由`staged_execution.py`负责，指标累计及原结果装配移至`staged_metrics.py`。
五份历史投影继续读取[准确e5原执行文件](historical_sources/e5e65fdb/code/src/modalities/image/staged_execution.py.gz)，
原expected、来源版本、投影身份及已有映射保持；与同版包导入原件共用`e5e65fdb`目录。
[17项原CPU、完整输出与恢复](../../results/diagnostics/records_runtime_refactor_20261104/README.md#hse-metrics-responsibilities)
只覆盖本批实际职责提取，不代表真实Ray／模型或整套3010物化。

<a id="http-map-owner-sources"></a>
## HTTP Map owner退役后的来源读取

multiplexed_gateway、query_registration与connection_capacity退出当前树；图像使用自己的真实owner与帧读取实现。
旧gateway_runtime、image_gateway与framing的原字节和这三份删除源码共同保存在
[准确e5原件目录](historical_sources/e5e65fdb/code/src/execution_provider/)。
共六份gzip、三十条新增来源／格式记录，只有原先读取当前文件的条目改变读取位置。
五份投影共35条受影响绑定逐项保持，原expected、来源版本、投影ID和所有既有映射未改。
其余旧专测不在这五份原投影中，完整恢复见
[原源码／测试记录](../paused_pg_and_retired_http_20261105/README.md#retired-http-map-owner)。
本次只复查受影响来源，不把它写成3010条完整构造或真实图像服务验证。


<a id="fixed-http-adapter-sources"></a>
## Fixed HTTP工厂与旧Map协议的历史读取

维护路径删除固定HTTP配置、同步／异步传输、完整HTTP响应包装、Ray HTTP Map装配、准备／组织、
v6协议与旧观察probe。IncrementalExecution与NativeTask保留现有Core／Job／结果／取消能力，
不再从HTTP模块取得容器、默认结果容量或测试结果包装。

删除前45份相关文件逐字核对准确公开
[e5e65fdb完整来源](https://github.com/3444374/ai-operator-execution-optimization/tree/e5e65fdb6db14f5f422ed4e3983e2206fbc75dfd)。
五份投影共85条受影响read_source读取前后一致；九份原先取当前文件的源码各增加五条来源／格式记录，
保存于[准确e5原件目录](historical_sources/e5e65fdb/)。既有来源、原expected、投影ID与版本不改。
该目录同时保留此前Image owner的六份原件，不能把两批新增数量混计。
本批未重跑完整3010构造，原模型与实验结果不因源码恢复而新增资格。
旧专测和固定Map原型使用[完整恢复树及文件摘要](../paused_pg_and_retired_http_20261105/README.md#retired-fixed-http-adapters)，
不向维护路径加回旧CLI／导入。原诊断raw脚本、结果、失败和负结果继续保存。

<a id="movie-query-worker-sources"></a>
## Movie查询职责拆分时的来源读取

普通Movie参数入口、共享评价与普通后端worker分开维护，旧CLI私有导出不再保留。
当前Daft接线测试迁移了真实patch位置，五份历史投影中唯一原先读取当前测试的绑定改为
[准确b168原测试](historical_sources/b16838c6/code/tests/experiments/test_daft_session_wiring.py.gz)；
该gzip逐字对应`b16838c6f698560ffb1304834ab26c9b7717646f`，只增加一条来源／格式记录。
本批17条相关读取以及IMLane导入修正的5条读取分别核对原摘要，原expected与投影身份不改。
[具体职责、输出和检查范围](../../results/diagnostics/records_runtime_refactor_20261104/README.md#movie-query-worker-responsibilities)
不代表整套历史构造或真实模型执行。

<a id="arrival-replay-research-sources"></a>
## 到达回放、等待与B原型的历史来源

维护路径将ArrivalReplayBatcher与等待／B原型归入research/data_organization；生产组批类型仍在原模块。
旧调度包的研究导出退出，旧导入、pickle及测试可从[准确af63来源](https://github.com/3444374/ai-operator-execution-optimization/tree/af63c0e56dab8cbd3f13649375bffa8f6eb92422)恢复。
五份具名投影原先直接读取的两模块保存为
[flush原件](historical_sources/af63c0e5/code/src/scheduling/submission_control/flush.py.gz)和
[token_budget原件](historical_sources/af63c0e5/code/src/scheduling/organization/token_budget.py.gz)，只加十条来源／格式。
原batching及顶层包继续使用已有准确来源；原expected、投影身份及旧映射不改，二十条相关读源前后匹配。
研究算法继续维护，旧结果及失败保留，没有重跑完整历史树、模型或服务。

<a id="retired-pid"></a>
## PID admission prototype recovery

The standalone `PidConfig` and `PidAdmissionController` module and package exports are retired.
No maintained runner or configuration selected them; their old profiling consumer is already historical.
Current per-request feedback, shared AIMD batch feedback, generic admission gates and resource accounting remain.
This removes a Python policy option without introducing a replacement algorithm or compatibility import.

Recover the latest implementation, numeric-input corrections and policy tests from exact commit
[`a8028fc90f0c391cf412356a6e03fc2c97c4ed81`](https://github.com/3444374/ai-operator-execution-optimization/tree/a8028fc90f0c391cf412356a6e03fc2c97c4ed81):
[PID source](https://github.com/3444374/ai-operator-execution-optimization/blob/a8028fc90f0c391cf412356a6e03fc2c97c4ed81/code/src/scheduling/submission_control/pid.py),
[configuration tests](https://github.com/3444374/ai-operator-execution-optimization/blob/a8028fc90f0c391cf412356a6e03fc2c97c4ed81/code/tests/scheduling/test_pid_configuration.py) and
[PID behavior tests](https://github.com/3444374/ai-operator-execution-optimization/blob/a8028fc90f0c391cf412356a6e03fc2c97c4ed81/code/tests/scheduling/test_adaptive_admission.py).
Use that separate checkout to run the old policy; the maintained path provides no forwarding shell.

The five named baseline projections still use the unchanged original
[`bfb9100e` PID source](historical_sources/bfb9100e/code/src/scheduling/submission_control/pid.py),
SHA-256 `b9a4c4b4fc34f7a6d39959acc85c449ac276b8beb7884ebd7e776a2caa2a41e7`.
Their original expected hashes, projection identities and source mappings are unchanged. The existing
historical PID source check now lives with `HistoricalProjectionTests` in
[`test_native_baseline_reproduction.py`](../../../code/tests/experiments/test_native_baseline_reproduction.py);
its policy-execution tests are recovered with the retired implementation rather than rerun in the maintained suite.

<a id="retired-capacity-controller"></a>
## Offline capacity-step controller recovery

The standalone `BoundedCapacityController` and `CapacityDecision` module, two package exports and nine exclusive tests are retired.
Its former shared-vLLM runner and control benchmark are already historical; no maintained factory or script applies its selected arm.
This is not the current request feedback, batch feedback or Core resource account. The shared `CapacityArm` value and its public import remain,
including the existing research and trace-replay consumers.

The exact source from published `769a73074084a721e3342e505d0dd94b428f18b8` is stored once as
[`capacity.py.gz`](historical_sources/769a7307/code/src/scheduling/submission_control/capacity.py.gz).
It decodes to 7,963 bytes, SHA-256 `5ae611bb1e5f254a61cf33c09d8b6a51787d3ad8d8510bb3773727221895f02f`.
Each of the five named projections adds only this source override and its gzip format declaration.
Original expected hashes, projection identities, other sources and formats remain unchanged; the preserved source stays outside current imports.

Recover the complete original implementation and tests in a separate
[`769a73074084a721e3342e505d0dd94b428f18b8` checkout](https://github.com/3444374/ai-operator-execution-optimization/tree/769a73074084a721e3342e505d0dd94b428f18b8):
[`capacity.py`](https://github.com/3444374/ai-operator-execution-optimization/blob/769a73074084a721e3342e505d0dd94b428f18b8/code/src/scheduling/submission_control/capacity.py) and
[`test_capacity_controller.py`](https://github.com/3444374/ai-operator-execution-optimization/blob/769a73074084a721e3342e505d0dd94b428f18b8/code/tests/scheduling/test_capacity_controller.py).
Old results, failure records and research material retain their original interpretation; this retirement does not rerun those algorithms or model environments.
