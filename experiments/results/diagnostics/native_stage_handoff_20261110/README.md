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

## 单卡原生模型交接

`native-stage-r1b` 使用源码 `39844da2a622562017ad0c97d4d0f6d5be8abd5a`，
[来源](raw/native-stage-r1b--source.json.gz)、[配置与完整输入](raw/native-stage-r1b--config.json.gz)、
[全部原始结果](raw/native-stage-r1b--worker--report.json.gz)与[汇总](summary.json)均保留。
当前仅一张 RTX4090，两个阶段均使用 GPU0；这是合成文本请求的模型层功能检查，没有调用完整 LOTUS 查询或质量评价。

模型为本地 Qwen2.5-1.5B-Instruct，下载元数据的 revision 为
`989aa7980e4cf806f80c7fef2b1adb7bc71aa306`；[模型元数据](raw/final-inventory.json.gz)保存 JSON 文件摘要与下载记录，
本轮未重新计算大型权重文件摘要。vLLM 0.25.1、torch 2.11.0、NIXL 1.3.0。
[七份实际原生源码](native-source-verification.json)与官方标签逐字节相同。

driver 的26项、模型环境的17项只读检查通过；
[driver](raw/native-stage-r1b--driver-preflight.json.gz)／[模型](raw/native-stage-r1b--model-preflight.json.gz)分开保存。
按预览只新增 nixl、nixl-cu12、nixl-cu13 三个1.3.0包，未升级 torch／vLLM，未下载模型或数据。
两个实例分别使用0.25显存比例、单序列、上下文2048、2048批 token、BF16、eager和关闭前缀缓存；
完整生成规则相同：temperature=0、seed=0、最大输出16，无 workload 预热。原生初始化另记。

| 检查 | 实际结果 |
|---|---|
| 完整请求 | 4条；prompt长度154／154／162／154 token，均返回3个输出 token |
| 物理调用 | 共置4次，prefill4次，decode4次；累计12次，不把两段算作一次物理调用 |
| 输出一致性 | 4/4完整 prompt、输出 token、文本、结束原因一致 |
| 原生状态接收 | 4/4 NIXL接收完成；外部输入状态154／154／162／154 token |
| 生命周期 | 共置及两个阶段的API／进程关闭均确认；独立GPU进程列表为空，主机租约已释放 |
| 请求段计时 | 共置0.870103秒；分离1.893140秒。只有一次、含诊断和跨进程开销，不是完整查询或正式性能比较 |

交接证据来自[decode原日志](raw/native-stage-r1b--worker--split--decode.log.gz)的加载、接收完成和请求终态，
经 P 回传的 `remote_request_id` 关联到 D 的内部身份；不按内部ID后缀猜配。
[逐请求审计](native-transfer-audit.json)全部通过；审计实现是实机后补充的离线检查，
对保存原件复算通过，未因此增加模型调用。[两个反例检查](raw/cpu-06.log.gz)及
[最终13项相关检查](raw/cpu-07.log.gz)通过。原生统计补充的[18项检查](raw/cpu-05.log.gz)中17通过、1跳过。

**统计缺项。** 原生 Prometheus 的 NIXL传输直方图和计数读取均为0，但逐请求日志有四次接收完成。
对应源码 `LLMEngine.step` 只在 `outputs.outputs` 非空时记录统计；推断无输出的传输轮次可能未进入同步统计。
没有补写或把0改成4；原读数完整保留，传输字节和耗时记为 unavailable。
后续性能实验须补齐该项采集；本轮状态移动的判断由原生接收终态支持。

**失败保留。** 早先的控制脚本在系统Python导入psutil失败，尚未启动模型或占用调用额度；
改用已有driver解释器后执行 `native-stage-r1b`。[原错误](raw/remote_run_r1.py.log.gz)保留。
依赖探针曾读取不存在的 `nixl.__version__`；包本身已可导入，改用发行元数据核对版本，
[安装与探针原日志](raw/remote_nixl_install.py.log.gz)保留。这些准备错误不伪装成模型失败。

[监督](raw/native-stage-r1b--supervision--supervisor.json.gz)、[GPU采样](raw/native-stage-r1b--gpu-samples.json.gz)、
[最终GPU回执](raw/native-stage-r1b--gpu-after.json.gz)与[持久次数记录](raw/attempts.jsonl.gz)分别保存。
[证据清单](evidence_manifest.json)列出公开副本与私有原件摘要；原件保存在该运行的仓库外归档中，未删除失败或修改原数据。

## 可支持与尚未覆盖

本轮支持：在单卡、两个原生进程之间完成文本输入阶段与生成阶段交接，保持这四条请求的输出，并清理实际资源。
跨 GPU 的设备绑定代码已提供，但当前硬件没有第二张卡，不能据此声明跨卡实测通过。
运行中 decode 迁移、视觉编码器分离、请求重叠、动态路由、完整查询适配和性能收益仍未建立。
NIXL与阶段分离是复用的原生能力，不作为本项目的新颖性结论。
