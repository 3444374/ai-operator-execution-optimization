# 文本模型执行与部署

当前文本主线使用 SemBench 任务的真实输入、关系语义与评价器。
模型、生成参数、缓存和生命周期属于具体任务；环境检查按[平台入口](README.md)与[runtime](../runtime/README.md)执行。

| 路径 | 模型与执行归属 |
|---|---|
| SemLoom records | [Core 工厂](../../code/src/execution_provider/adapters/lotus_records_core.py)选择单次或 resident owner |
| 原生 LOTUS | 普通 [Movie session](../../code/src/experiments/movie_matrix.README.md)使用原 LOTUS LM 与本地模型 IPC |
| 其他对照 | 使用各自[原生入口](../../code/src/baselines/README.md)，由被测系统拥有图与调度 |
| 独立 HTTP 服务 | 任务显式需要时使用 [start_endpoints.sh](start_endpoints.sh)，先核对实际引擎参数 |

vLLM 拥有原生请求调度、continuous batching 和 KV 管理。应用 token 组批、供给并发、IPC 保护
与引擎序列／token 预算分别记录；不能从应用 batch 或逻辑容量推断 GPU 执行 batch。
Core 的实际 EngineSpec 与 native resolved 配置见[协议](../../code/src/serving/backends/vllm_protocol.py)，
记录模型／tokenizer 身份、dtype、上下文、显存比例、prefix cache 和显式覆盖。
省略参数按所用引擎解析，不把旧 K/W、强制 prefix cache 或固定双 endpoint 配方继承为默认。

冷启动、模型就绪后的首查询和同 owner 复用分开；模型复用不等于响应或策略缓存。
完整结果可用、质量评价和最终关闭分别计时。结束时核对未释放结果、进程、GPU 与租约，
保留失败与 unknown，不重放旧输出目录中的任务。

原文本指南、固定拓扑与历史命令的准确字节见[旧配方归档](../../experiments/reproduction/legacy_recipes_20261105/README.md)。
当前真实资格从[代码状态](../../code/INFRA_STATUS.md)和对应报告核对。
