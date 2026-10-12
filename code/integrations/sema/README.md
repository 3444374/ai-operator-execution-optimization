# Sema后置原生执行检查

本目录保存Daft／Ray Data的实际库检查和IMLane完整HTTP调用的薄客户端。
原Sema仍拥有SQL、提示、解析及请求池；调用从HTTP抵达后交接，
实现与阶段定义见[Sema服务](../../src/execution_provider/adapters/sema_request_service.md)，
总设计见[语义系统方案](../../../experiments/plans/语义系统对照.md#sema-native-executors)。

## Daft和Ray Data

[verify_native.py](verify_native.py)执行有限完整请求及服务回送检查，不调用真实模型。
先按[运行时说明](../../../deploy/runtime/README.md)对独立driver环境检查`core,text`；
程序要求Daft0.7.21或Ray2.56.1，不安装依赖，不接管已存在的Ray会话。
Ray使用8CPU、零GPU、128MiB对象存储，拥有自己的短临时目录；只关闭本次启动的运行设施。

```sh
PYTHONPATH=code <driver-python> code/integrations/sema/verify_native.py \
  --backend daft --artifact-root /path/to/new-private-daft-check
PYTHONPATH=code <driver-python> code/integrations/sema/verify_native.py \
  --backend ray-data --ray-temp /path/to/short-ray-dir \
  --artifact-root /path/to/new-private-ray-check
```

`--scope graph`只检查原生图，`--scope service`只检查后置HTTP服务，默认两组均运行。
记录完整响应、出现身份、实际HTTP峰值、库源码摘要、取消和清理；短case未达到声明峰值时不称饱和。

## IMLane薄客户端

作者实现采用[固定提交](https://github.com/IM-DM4AI/IMLane0/tree/072f8257db80569e42c9f5727d839587df529652)。
共享作者构建由独立实施任务提供，本分支只链接Sema调用方，不复制作者库、依赖或虚拟环境。
需要已有构建目录中的`source/`、`prefix-patched/`与`artifacts/build-patched/build-identity.json`。
[build_imlane_http.py](build_imlane_http.py)核对来源、源码和二进制摘要，再链接
[C++ DBEnd客户端](imlane_http.cpp)，不会重建作者系统或覆盖既有客户端。

```sh
<imlane-python> code/integrations/sema/build_imlane_http.py \
  --root /path/to/checked-imlane-build \
  --output /path/to/new-private-bin/imlane_http
```

客户端通过作者任务线程池调用`ExecuteFunction`，输出复制到本次调用方持有的Arrow内存后回送。
[HTTP函数](imlane_http_udf.py)每次只执行一份原始完整调用，不合并/拆分提示，使用同步入口内的
`asyncio.run`完成HTTP，避免依赖当前Python3.12异步UDF事件循环问题；作者Lane和执行器仍管理执行。
作者库的Arrow16.1.0运行环境与查询driver的Arrow24保持隔离，HTTP函数所在解释器须支持aiohttp。

仓库外`--imlane-config`需声明`author_commit`、`bridge_binary`/`bridge_sha256`、
`udf_path`/`udf_sha256`、`gateway_port`、`work_root`、`namespace`及`native_task_threads`。
可选`env`只接受LD_LIBRARY_PATH、PATH、PYTHONPATH、TMPDIR和两项线程环境变量。
Coordinator须先在同一实例名称和loopback端口就绪；查询只拥有DBEnd子进程。
空输入与线程参数0可读取实际原生线程数，非空执行前必须与配置匹配，并且不超过声明请求容量。
取消后的远端工作写unavailable，客户端异常后禁止继续复用；正常结束需要全部响应、EOF和零退出码。

阶段、请求/查询P99和原始证据由[共同查询入口](../../src/experiments/postgresql/README.md#native-adapter-query)记录。
检查结果进入[同一主报告](../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#sema-native-executors)，
无模型检查不用于评价模型性能或Sema原池前调度。

## 三种原方法的IMLane模式

[完整调用接口](imlane_calls.md)复用上述DBEnd桥，明确区分作者CPython与作者Ray执行器。
公共`NativeCallExecutor('imlane', imlane_config=IMLaneCallConfig(...))`直接交接完整批次，
默认128行；显式2000行配置已通过两作者模式的原SemBench Q1软件检查。
三层共用有限行数、payload/wire、单项、待交付及累计响应额度，超额拒绝。
每查询一个DBEnd上下文，交付后补入下一调用并释放该输入/输出引用；聚合RSS没有32MiB保证。
LOTUS和DuckDB返回原SDK／C++解析，Sema保持原SQL及请求池后的有限HTTP快照。
统一CLI使用`--imlane-cpython-config`、`--imlane-ray-config`指定仓库外配置；
`--options`需覆盖探得的作者任务线程数，本次为6，检查配置使用并发额度8、作者执行器2。

[公共方法检查](verify_imlane_methods.py)在两种mode中检查真实LOTUS、DuckDB和Sema的完整消费及连续LOTUS查询。
它需要已有作者构建、原桥、已编译的`0.4.14-native3`扩展、固定Sema作者二进制和独立LOTUS解释器，
只访问localhost软件响应，不安装依赖或加载模型。来源和全部失败见
[本轮记录](../../../experiments/results/postgresql/native_executor_baselines_20261012/README.md)。
独立单卡及双卡模型轮分别检查LOTUS六条路径，各轮两种IMLane模式各24真实POST、实际2个worker通过；
作者Ray另有2个actor ID，Coordinator／Ray正常退出并核对共享内存0。Sema／DuckDB模型尚未运行，不能借用LOTUS结果。
错误或提前停止消费可能在DBEnd返回EOF前关闭调用方；同一Coordinator出现未确认记录后禁止新对象继续提交。
调用者核对并结束自己的Coordinator／Ray资源后，使用新的进程身份与实例名称开始下一次检查。
