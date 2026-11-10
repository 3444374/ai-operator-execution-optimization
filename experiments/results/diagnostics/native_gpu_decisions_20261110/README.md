# 原生 GPU 批决策的被动观察与诊断

本切片实现研究观察代码，不改变请求、调度选择或引擎容量。来源为 main
`70ee0970260a710efee73e5cc3db841abed75ead` 上的资源观察器及 vLLM 0.25.1 官方源码。
研究安排见 [PR #96](https://github.com/3444374/ai-operator-execution-optimization/pull/96) 中的数据组织计划；
本报告只记录当前代码与零模型验证，未运行服务器或真实 GPU。

## 实现与判断范围

在已按版本及源码摘要核对的 v1 worker 中，观察器临时包装原生共同前缀选择与图执行选择，
记录轮次、实际成员、公共前缀块、已计算 token、cascade attention 的选择／长度、CUDA Graph 模式、
原始／填充 token 数及 encoder 输入数。使用既有有上限缓冲；收尾恢复实例方法。
不读取 GPU tensor，不增加同步，不解释原始提示或响应。辅助模块从工具 checkout 加载，
不会因被测源码较旧而误读缺少新模块的路径。

[诊断程序](../../../../code/src/experiments/native_gpu_diagnostics.py)按运行、观察会话、PID 和执行轮次关联，
核对调度成员与实际决策成员；重复、矛盾、缺项及失败执行保留 unknown。
逐文件校验事件序列、关闭／flush 回执及缓冲丢失，保留文件摘要与行号。
干净的 sink 仅证明这些文件未记录丢失，不证明覆盖全部 worker、GPU 已完成或语义发生项已全部关联。

可输出的现象是公共块存在但 cascade 未选中，以及 native batch descriptor 的填充槽位。
两者均附下一项单因素干预；不把槽位计数解释为 GPU 时间，不把公共块计数解释为命中节省。
供给不足、计算／带宽受限、缓存节省和查询加速分别列出仍需的证据。

## 验证与来源

- `native_gpu_decisions_cpu_20261110_01`：12 个相关测试模块，229 项通过、无失败或跳过；
  覆盖原返回对象、异常、方法恢复、设备数据不读取、成员关联、事件缺项、既有 IPC／采集／取消路径。
- vLLM 官方 `GPUModelRunner._determine_batch_execution_and_padding` 从 SHA-256 固定文件提取，
  在 CPU 软件配置／dispatcher 下检查普通、cascade、encoder、强制 eager 四种分支。
  这是实际函数的 CPU 分支检查，不是原生引擎或 GPU 测试。
- 最后调整诊断后，7 项诊断检查及 1 项真实 sink 软件接线复查通过；CLI 使用软件事件完成输出、缺 footer 报告和文件权限检查。
  这些是定向复查，不与 229 项累加。
- 真实模型调用为 0。未采集 GPU 开销、kernel 时间、完整查询质量或性能结果。

运行元数据、模块清单和官方来源摘要见 [checks.json](checks.json)，
逐项输出保留于 [CPU 检查](raw/cpu-check-01.log.gz)、[首轮诊断复查](raw/cpu-analysis-final.log.gz)、[末轮诊断及接线复查](raw/cpu-analysis-final-02.log.gz)和
[CLI 软件事件检查](raw/cli-fixture.log.gz)。完整命令中的私有环境路径不进入 Git。

复查新模块可在已准备的 driver 环境运行：

```sh
PYTHONPATH=code "$DRIVER_PYTHON" -B -m unittest -v \
  tests.observability.test_native_gpu_decisions \
  tests.experiments.test_native_gpu_diagnostics \
  tests.experiments.test_record_resource_capture
```

可选源码检查由 `SEMLOOM_VLLM_RUNNER_SOURCE` 指向已下载的官方 `gpu_model_runner.py`；
测试先核对摘要，不自行下载、导入 vLLM 或访问模型。未提供时只跳过这一项，不能宣称源码分支检查已运行。
采集和分析命令见[观察器说明](../../../../code/scripts/profiling/record_observation/README.md#native-gpu-decisions)。

后续实机核对需要使用新工具身份重新通过既有资格流程，再比较观察 off/on 的输出、完整查询时间和资源开销。
身份桥、有限 kernel／搬运轨迹与单项组织干预分别补充；不会仅凭本次软件结果采用优化。
