# Kalypso 原生 SemBench 适配

本目录从已完成的2026-10-25 r3适配选择三个文件：`adapter.py`、`native_server.py`、`cpu_native.py`。后两者保留来源字节；维护版`adapter.py`将客户端接入中性生命周期与公共计时，并明确外部模型启动归属。本目录没有注册新的默认后端，也不包含作者服务、模型或数据。作者源码固定为`05024f027f932c0e398852e38302be30b9015481`，原生引擎属于vLLM0.13.0rc4分支。

`adapter.py`只负责输入、完整发生项核验、源序恢复和观测接入；作者服务继续拥有Filter执行、生成设置、批处理和调度。原生parser以结果不含`false`判通过，与LOTUS不同。`native_server.py`保留实际worker模型元数据及物理设备旁录；客户端关闭不代表服务退出。`cpu_native.py`需要已有作者环境和模型tokenizer；本次没有安装或启动它们。

原程序以独立的`adapter`和`native_server`模块名解析worker扩展。以后在私有运行副本执行时，将本目录加入该进程的`PYTHONPATH`并提供明确配置；不要只改扩展名而声称继承原worker测试。`load_dependencies`还需要具名历史源码及对应packet依赖，新私有运行绑定尚未完成。

`qualification.py`仅提取成功r3控制器的采样参数审计比较：将`stop=None`与`stop=[]`视作作者规范化后的等价值，其他键和值仍逐项比较。它不改写实际生成参数。旧端口TIME_WAIT修复保留在原运行来源中，尚未移植通用控制器；不能据此声称本目录已经提供完整的启动/关闭入口。

独立CPU回归覆盖默认物理UUID读取、设备漂移、负例与未知终态、预算停止及原异常保留；未重跑完整原评价器或真实worker。历史运行报告和私有封包尚未随此候选发布；[具名复现说明](../../../../../../experiments/reproduction/native_baselines/README.md)区分当前维护代码、历史来源及仍需新建的运行绑定。

## 正式目录入口候选

- 作者服务：`PYTHONPATH=code python code/scripts/experiments/run_kalypso_native_server.py --help`。真实服务仅在显式启用的私有config下启动；`--cpu-check`保留原作者参数/导入顺序验证。
- 已挂载服务的SemBench查询：`run_kalypso_sembench_query.py --config ... --unit ... --output ...`。客户端不拥有服务器退出，外层owner仍须单独监督/记账/核验物理清理。
- 原生CPU/token/planner检查：隐藏CUDA后使用 `check_kalypso_native_cpu.py --config ... --output ...`，需要作者固定隔离环境。

新的客户端直接使用中性的 `native_query_lifecycle` 和公共 `native_query_timing`；ready中的`model_startup_scope`明确模型属于外部GPU owner，客户端attach没有测量模型初始化。没有修改作者Filter/生成/parser/调度，Kalypso仍非LOTUS backend。此目录迁移后的CLI仅做本地无模型smoke和CPU替身回归，尚未重新绑定作者环境、配置/资产/worker路径或转授旧r3 GPU资格。私有cfg的baseline_source必须指向本次明确代码树，dependency_sha256必须重新封包，禁止冒用旧锁。
