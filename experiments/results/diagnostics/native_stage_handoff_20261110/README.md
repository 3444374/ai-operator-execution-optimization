# 原生文本阶段与 KV 交接

本报告对应[当前计划](../../../plans/数据组织.md#native-stage-handoff)与
[后端说明](../../../../code/src/serving/backends/README.md#native-text-prefilldecode-handoff)。

## 实现与来源

新增两个独立模型进程的文本 prefill/decode owner，使用 vLLM 0.25.1 原生 NixlConnector。
逐条完整请求在 P 处理输入后，由 D 使用原请求和原生成参数产生完整结果；保留两段物理回执，
不把 P 的一次性 token 拼到新 prompt。每段独立 GPU 可见性，单主机可指定不同设备。
默认路径不变；请求重叠、运行中 decode 状态迁移、视觉编码器分离与完整查询适配尚未实现。

工程参照为[固定版本代理](https://github.com/vllm-project/vllm/blob/v0.25.1/tests/v1/kv_connector/nixl_integration/toy_proxy_server.py)、
[原生 NIXL scheduler](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/distributed/kv_transfer/kv_connector/v1/nixl/pull_scheduler.py)
及[依赖](https://github.com/vllm-project/vllm/blob/v0.25.1/requirements/kv_connectors.txt)。
采用原生描述转发与失败策略，复用既有有限 IPC、进程所有权与清理；未修改上游模型计算或调度器。

## CPU 检查

- 系统 Python 首轮51项中，49通过、1跳过、1因未安装 pyarrow 的旧消费者导入失败；
  [失败摘要](raw/cpu-01-failure.txt.gz)保留原因。切换到已有隔离 driver，不在系统 Python 安装依赖。
- 完整 serving 检查首次194项有1错误、3跳过：[原日志](raw/cpu-02.log.gz)。旧 context IPC 测试
  漏传现行必需的 limits；补显式单请求容量后，[194项检查](raw/cpu-03.log.gz)中191通过、3跳过。
- 后续加入真实子进程 IPC、独立设备环境与原生配置构造检查，连同受影响 context 用例
  [20项通过](raw/cpu-04.log.gz)。不同源码状态的计数不相加。

以上没有执行模型。覆盖完整消息／生成、状态描述转发、结果关联、缺少状态、取消、部分启动失败、
两段使用量与逐进程关闭；测试替身不证明真实 KV 数据移动。

## 真实模型状态

指定服务器只读 preflight 已通过；当前仅一张 RTX4090。原生阶段验证 pending，
尚不能声称跨 GPU 传输通过、完整查询质量或性能改善。具体尝试、失败和结果将在本报告补记。
