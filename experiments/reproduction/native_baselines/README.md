# 原生基线与具名 Core 历史复现

## 当前源码恢复状态（2026-11-04）

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
它只具有 CPU 策略测试，没有 profiler reward epoch 或维护执行调用；现行 AIMD／EWMA／HOL-age／PID 入口保持。
准确 bfb 原件保存在 [historical_sources/bfb9100e/](historical_sources/bfb9100e/)：
UCB 实现、原 `scheduling/__init__.py` 和完整原测试文件均可按 SHA-256 核对。
五份历史投影各增加两条 override，共十条；原逐文件摘要、投影身份和既有映射不变。
原五项 UCB 测试从准确副本读取并在原算法上运行，当前运行时不导入该副本。
完整历史仍有原十处不匹配；本次保留恢复材料不把它们改报通过。
[当前整理记录](../../results/diagnostics/records_runtime_refactor_20261104/README.md)分别说明删除范围、整体结构与剩余项。

维护版 PID 现在拒绝小数／布尔请求窗口及布尔数值参数。准确 bfb 的 `pid.py` 另保存在同一历史目录，
五份投影只追加五条 override；原逐文件 SHA 与投影 ID 不变。旧配置的原解释由原源码恢复，
维护版有效配置的 16 条轨迹保持一致，检查见当前整理记录。

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


<a id="retired-stage-work-controller"></a>
### 旧工作量控制原型退出生产目录

`BoundedStageWorkController` 与 `WorkCreditDecision` 已从维护版 `stage_work.py` 移除；
当前执行入口没有使用这两项。准确原件继续保存在
[既有历史源码](historical_sources/6a8a2d7b/code/src/scheduling/submission_control/stage_work.py)，
五份投影的文件摘要、恢复映射和投影 ID 均不改。
原三项行为测试改为读取这份历史源码，不为当前执行保留兼容导出。
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
