# Kalypso 原生 SemBench 适配

本目录源自2026-10-25 r3适配。维护版将客户端接入中性生命周期与公共计时，并将新运行绑定改为作者CLI默认解析；旧运行的固定容量配置和成绩保留历史身份。本目录没有注册新的默认后端，也不包含作者服务、模型或数据。作者源码固定为`05024f027f932c0e398852e38302be30b9015481`，原生引擎属于vLLM0.13.0rc4分支。

`adapter.py`只负责输入、完整发生项核验、源序恢复和观测接入；作者服务继续拥有Filter执行、生成设置、批处理和调度。原生parser以结果不含`false`判通过，与LOTUS不同。`native_server.py`保留实际worker模型元数据及物理设备旁录；客户端关闭不代表服务退出。`cpu_native.py`需要已有作者环境和模型tokenizer；本次没有安装或启动它们。

attached比较流程维护在`src/experiments/kalypso_query_lifecycle.py`；`adapter.run_attached`和原模块CLI只作兼容转发。实际客户端不导入实验流程，调用方只有显式运行比较入口时才加载该编排。

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
