# IMLane 独立构建与无模型检查

本目录保存公开 IMLane 的固定来源、隔离构建入口、最小配置补丁和 Arrow 数据验证程序。
研究对象为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
这里检查作者的 DBEnd（数据库调用入口）、Coordinator（执行协调进程）及 Python／Ray 执行器，
不构建随 artifact 提供的数据库副本。LOTUS、DuckDB AI 的提示、解析和算子策略接入另行验证。
总方案由[语义系统对照](../../../experiments/plans/语义系统对照.md)维护。

## 来源与依赖

作者实现固定在 [IM-DM4AI/IMLane0](https://github.com/IM-DM4AI/IMLane0/tree/072f8257db80569e42c9f5727d839587df529652)
提交 `072f8257db80569e42c9f5727d839587df529652`，许可证为 MIT（宽松开放源码许可）。
[来源清单](source_identity.json)记录提交、关键文件 SHA-256（内容摘要）、固定依赖和环境差异；
[作者许可](LICENSE.upstream)随最小补丁保存。作者数据库源码、依赖源码、虚拟环境、二进制和日志均留在仓库外。

| 组成 | 本轮固定版本／采用方式 |
|---|---|
| Python | 3.12；嵌入式解释器与验证虚拟环境使用同一基础安装 |
| Arrow C++／PyArrow | 16.1.0；两者来自同一 Python 轮子，构建入口核对版本并生成导入目标 |
| NumPy／Ray | 1.26.4／2.56.1；均在 IMLane 专属虚拟环境 |
| 资源采样 | psutil 7.0.0；只观察本次拥有的进程 |
| Boost | 1.84.0，核对官方归档摘要，只编译 program_options 和 system |
| libuv／fmt／msgpack／pybind11 | 1.50.0／11.1.4／cpp-7.0.0／2.13.0，使用清单中的精确提交 |
| nlohmann/json | 3.11.3，供作者附带的 nanoarrow 测试源使用 |

观察器先加载 psutil 7.0.0；Ray 2.56.1 另带 psutil 7.2.2，先加载 Ray 时会优先使用该副本。
运行库记录保存两种实际模块路径和加载次序，不将它们混称同一安装。

作者安装脚本引用 Python 3.8 轮子且包含不一致的下载／目录名称，本入口不直接执行该脚本。
独立依赖供应和对象库头文件路径由本目录 CMake 处理，保持作者核心执行实现。
这不是论文完整软件环境复现；Ray 版本和 Python 版本差异显式保留。

## 可重复流程

运行前按[运行时说明](../../../deploy/runtime/README.md)和目标平台规则保存只读环境报告。
使用仓库外 `AI_OPERATOR_ENV_FILE`，对本轮新增的 `imlane-runtime` 能力组执行
`manage_environment.py check`；机器能力与任务实际 CPU（处理器）位置分别记录。
本程序使用 Linux 的进程和共享内存接口，不发送模型请求、不下载模型或创建数据库实例。
验证进程通过编译器查找系统 C++ 运行库，避免基础 Conda 的旧库缺少 Arrow 所需符号；报告保存实际库摘要。

```bash
export IMLANE_WORK_ROOT=<external-data-disk>/imlane-validation
# 明确选择基础解释器，不使用模型服务环境。
python3 code/integrations/imlane/prepare.py \
  --root "$IMLANE_WORK_ROOT" --python <base-python> --jobs 8

# 先检查未修改作者源码；独立日志保留每次配置、编译与安装。
"$IMLANE_WORK_ROOT/venv/bin/python" code/integrations/imlane/build.py \
  --root "$IMLANE_WORK_ROOT" --mode pristine --jobs 8

# 明确应用本目录补丁，再构建配置后的作者执行器和验证程序。
"$IMLANE_WORK_ROOT/venv/bin/python" code/integrations/imlane/build.py \
  --root "$IMLANE_WORK_ROOT" --mode patched --jobs 8

CUDA_VISIBLE_DEVICES= OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 \
  "$IMLANE_WORK_ROOT/venv/bin/python" code/integrations/imlane/verify.py \
  --root "$IMLANE_WORK_ROOT" --runtime both --repeats 3
```

AutoDL 网络访问按最终域名选择代理：本入口仅在 GitHub 操作的子 shell 加载 `/etc/network_turbo`；
PyPI 和 Boost 归档访问取消代理。所有缓存、临时文件和输出写入显式指定的数据盘目录。
prepare 保存 Python 安装清单；build 核对源码提交、允许修改的文件、关键文件摘要与依赖提交，
禁止未登记的跟踪文件修改。相同源码的再编译复用构建目录，原日志不覆盖。
构建采用绝对库路径；移动安装目录后应按来源清单重新构建。

## 配置补丁与数据所有权

[补丁](imlane_fixture.patch)让测试实例拥有独立的共享内存／消息队列名称、端口和监听地址。
补丁使用零上下文表示，应用前核对固定源码摘要，再用 `git apply --unidiff-zero` 检查和应用。
`IMLANE_NAMESPACE` 只接受至多 64 个字母、数字、连字符或下划线；未设置时保持作者原名称。
`IMLANE_GATEWAY_HOST` 可显式指定监听地址；验证程序固定 `127.0.0.1`，未设置时保留作者默认。
`--gateway_port` 使用已有 `IMConfig.gateway_port`。这些选项不改变 Lane 选择或任务调度。
另有 Python 3.12 的嵌入式事件循环修订：作者执行器显式创建默认循环，继续使用原
`get_event_loop`／`run_until_complete`；自有循环在解释器结束前关闭并释放。
初始化异常转交原 `BackendException` 退出处理。未修改任务队列、Lane 选择和函数等待方式。
Ray 初始化还恢复此前的 Coordinator 信号处理器，正常和初始化报错路径都执行恢复，
让作者原退出流程继续处理工作线程和 Lane 归还。

[验证程序](fixture.cpp)实现作者公开的 `TableConverter` 接口，输入是 Arrow Table。
返回转换将输出复制到调用方拥有的 Arrow 缓冲区，因为原 DBEnd 在交回结果后释放 Lane 存储；
同一结果必须能在后续调用之后继续读取。本转换不替代作者的数据传输、Python 执行或 Ray 调度。
[确定性 UDF](fixture_udf.py)即用户定义函数，仅计算整数翻倍，保留输入列并附加实际工作进程编号。

验证使用作者 `ExecuteFunction`、`ExecuteFunctionPush`／`ExecuteFuncTryPull`、
`ExecuteAsyncFuncPush`／`ExecuteAsyncFuncPull`，并分别加载同步和异步 Python 函数。
覆盖重复标识／文本、位置对应、NULL、中文／换行／内含 NUL（零字节）的字符串、空表和输出类型。
每次进程调用还验证此前返回的输出在后续调用后仍然一致；连续三次调用复用同一 Coordinator。

Ray 由专属拥有者进程启动两 CPU、零 GPU（模型加速设备）的本机集群；作者 Coordinator 连接这一显式地址，
仍使用作者 Ray executor 和 actor（远程执行单元）包装。对象存储设置 128 MiB，共享内存核对上限 2 GiB，
验证采样合计 RSS（进程常驻内存）上限 8 GiB，单次验证期限 120 秒。
Ray 的短数据盘路径只作为独立临时目录入口，不停止其他 Ray 集群。

验证报告分别记录正常数据检查、作者退出码、是否强制终止、剩余进程和剩余共享内存。
只有正常数据检查与作者退出清理均通过，`passed` 才为真。
失败后清理只处理本次拥有的进程及已知资源名，原失败报告保留；强制清理不算作者检查通过。
现阶段未核验任意 UDF 异常、查询取消、超时后的结果错误传递和数据库跨批次异步推进，均为 pending。

## 检查入口与结果范围

Python 小检查使用：

```bash
PYTHONPATH=code "$IMLANE_WORK_ROOT/venv/bin/python" -m unittest \
  tests.integrations.test_imlane_fixture -v
```

它们核对来源拒绝策略及确定性 UDF 的输入／输出行为，不能代替实际 C++／Python／Ray 往返。
实际编译与往返结果、失败记录和恢复位置由父任务登记到实验报告及证据台账。
此目录不据 fixture 数字评价模型质量、端到端数据库性能或论文 batch-placement 收益。

未修改作者源码已完成全组件编译。配置补丁后的原 Python 执行器完成 12 个同步批次、60 行核对后，
在 Python 3.12 异步循环创建处停止；原 Ray 执行器的数据核对完成，退出时仍留下资源。
这些原件保留。事件循环和信号处理修订后的 Python／Ray 各完成一轮 105 行检查及正常退出，
Python 小检查 9 项全部通过。最终 Python／Ray 在各自同一 Coordinator 下分别完成三次调用，
共核对 630 行，正常退出与资源归还均通过。初始化注册异常注入还确认两个自有循环关闭并释放；
它不代表任意 UDF 错误已能传回数据库。逐次记录由同一父任务报告维护。
