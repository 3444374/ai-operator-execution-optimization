# 部署限定的资源 auto 配置：CPU 工程检查

基点为 `7a4c1846fa8c973225d2fe894ad4f0ab11deded1`，已核对它是本轮开始时
`integration/baselines` 的 HEAD。实现使用独立功能 worktree；没有修改 baseline、main 或其他会话文件。
本轮只做 CPU／graph double／合成进程身份检查及 localhost HTTP fixture，没有 GPU、真实模型、租机或性能实验。
独立审查及最终检查身份见 [qualification.json](qualification.json)。

## 硬编码与本轮范围

| 原位置 | 核查结果与修改 |
|---|---|
| LOTUS 图像 CLI | Ray 固定 GPU1／CPU4；新增可选 `--resource-config`，未传时保持原值 |
| `RayDataImageExecutor` | fused GPU pool=1；staged CPU/GPU pool=1；改为独立 worker 参数，仍由原生 Data 图调度 |
| `SemLoomImageExecutor` | CPU/GPU actor 各1；改为现有 `SemLoomRayWorkerPool` 多 handle，核对全部 CPU 预处理身份及 GPU UUID；复用原池 |
| HSE | prepare/model inflight=1；独立配置准备深度，opt-in model auto 取 `min(GPU workers, max_active_batches)`；ready 字节仍由原窗口决定 |
| Daft CLIP | 已有 `model_workers/gpus_per_worker`；CLI 贯通 model workers，scoped 配置仅允许每 worker 完整1卡，保留原 native UDF 生命周期 |
| Ray HTTP | `NativeGraphOptions` 新增 `ray_min_actors`，原生 `(min,max)` 按最大 actors 做 HTTP／CPU guard，关闭报告保存实际范围；旧通用/SQL入口也有可选 minimum |
| vLLM 启动器 | 新增可选 `--resource-config`，固定0.25.1单节点原生 MP、DP／显式TP、PP1；同一 HTTP 逻辑模型接口服务文本和图文 |
| Serve | 原单 `service_uid` 同时绑定服务与副本；本轮不开放 auto 或多副本，部署／副本身份、caller 并发及实际退出仍 pending |

这些是资源配置接线，不是新的统一调度平台或性能调优。原 LOTUS、PZ、Daft、Ray、vLLM 各自拥有原方法、
图或模型调度；不把 SemLoom router 注入其他 baseline。原生 LOTUS 单设备配置继续合法；IMLane 本地 CLIP 未接入。

## 资源边界与身份

[`resolve_gpu_allocation`](../../../../code/src/infrastructure/gpu_allocation.py)只查询显式允许集合。
来源依次为资源 JSON 的 `allowed_gpus`、部署 `GPU_IDS`、UUID形式的 `CUDA_VISIBLE_DEVICES`。
没有声明集合时拒绝 auto，`NVIDIA_VISIBLE_DEVICES=all` 也不会授予整机。
继承的 CUDA／容器 UUID mask 与声明集合取交集，交集外 UUID 在查询前排除。
NVIDIA 物理数字索引仅在没有继承 UUID mask 时支持；数字 CUDA／容器 ordinal、MIG、空集、重复物理 UUID、
超分配和歧义映射明确拒绝，要求部署改用完整 UUID，避免猜测 NVML 与 CUDA 编号的关系。

owner 在初始化自有 runtime 前冻结 `CUDA_VISIBLE_DEVICES` 为选中 UUID，结束后恢复本地环境。
scoped CLIP worker 在加载权重前要求只看到后端分配的一张卡，并校验实际 Torch GPU UUID。
SemLoom 额外拒绝多 handle 声称同一物理卡；全部 CPU readiness 必须匹配同一真实 transform-v2 身份与线程数。
Ray staged 每个批次携带并核对实际预处理身份；fused 每个构造器自行核对；Daft 每个构造器核对实际 slow processor 配置摘要。
默认复用／详细计时探针仍独立。opt-in 原始构造身份写入私有 `resource-workers/*.json`，包含 worker UID、PID、
Linux start ticks（可取得时）、实际 GPU UUID 或预处理身份；它只证明已成功构造的 worker，不能证明全部设备实际做过推理或已经退出。

报告分列 `resources_requested`、`resources_resolved`、配置文件摘要、worker／模型所有者及实际构造记录。
物理 UUID 不从 actor 数推断。显式资源模式的 Ray 图关闭 actor restart/task retry；未传资源配置的旧图保留原生重试默认。
未知工作仍拒复用旧 executor，不自动重发。
Daft 继续 `max_retries=0`。完整矩阵仍由原 collector 检查行 ID、shape、finite、无丢重和顺序。

## 使用同一查询切换部署

实际配置、路径与物理 UUID 继续放仓外 runtime env。资源集合由部署分配，下面 JSON 不按卡数维护分支。

本地 CLIP 的同一资源 JSON 可为：

```json
{"gpu_workers":"auto","cpu_workers":1,"ray_num_cpus":"auto","prepare_inflight":1,"model_inflight":"auto"}
```

在已有 `run_lotus_image_retrieval.py` 参数后增加 `--resource-config "$IMAGE_RESOURCES"`。
适用 `semloom_static/semloom_hse/ray_data/daft_native` 的 CUDA 路径；stock、direct、external（含 Serve）明确拒绝。
`ray_num_cpus=auto` 只计算已声明的 CPU stage＋GPU actor＋source/consumer 1 slot，并检查进程 CPU 分配；
这些是 Ray 逻辑 slots，不是 OS CPU 硬上限。fused／Daft 没有独立 CPU prepare pool，因此拒绝非默认 `cpu_workers`。
HSE 的准备深度与 GPU 数独立；static 的 `--max-active-batches` 必须覆盖 GPU worker 数，错误在启动模型前拒绝。
batch、窗口、字节上限与线程数仍须按 workload 显式设定，不从卡数推导最优值。

HTTP 模型服务的仓外 JSON 可为：

```json
{"gpu_count":"auto","tensor_parallel_size":1}
```

```sh
"$VLLM_PYTHON" code/scripts/services/launch_vllm_with_identity.py \
  --identity-output "$VLLM_IDENTITY" --port "$VLLM_PORT" \
  --resource-config "$VLLM_RESOURCES" -- \
  --model "$MODEL_PATH" --served-model-name "$MODEL_ID" --port "$VLLM_PORT"
```

同一服务 URL 与 LOTUS/PZ 查询保持；auto 用允许卡数除以显式 TP 得到 native DP，不能整除时报错。
CPU HTTP actors、PZ `max_workers/join_parallelism`、调用方 concurrency、batch 与 token 上限继续由各 baseline 配置，
不机械设置为 GPU 数。Ray 完整调用可在现有 `native_options` 中写 `ray_min_actors=1, ray_actors=4`；
活动 HTTP 上限按 `4 × ray_async_batches_per_actor` 检查，而不是 minimum，也不乘 batch rows。

启动器拒绝并行／rank／backend／device/YAML覆盖、短参数和下划线变体；再用官方 parser 且
`allow_abbrev=False` 核对实际 DP/TP/PP、单节点、LB、device、端口等字段，之后才写 sidecar 并 exec。
[`verify_model_replicas.py`](../../../../code/scripts/services/verify_model_replicas.py)复用原进程／listener核验，
对带资源身份的 native DP允许一个逻辑 endpoint 拥有多卡，保存全部 GPU owner PID/start ticks 与自有进程树。
rank→GPU 列表是启动请求；不按 PID 排序伪造实际 DP rank，资格报告明确只核对进程／设备所有权。

## 官方依据与工程决定

- [vLLM0.25.1 native DP](https://docs.vllm.ai/en/v0.25.1/serving/data_parallel_deployment/)：一个 endpoint、原生 internal LB，DP×TP决定 GPU 数；本轮只接单节点 MP。
- [固定 vLLM 参数](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/engine/arg_utils.py)与[FlexibleArgumentParser](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/utils/argparse_utils.py)：核对短参数、下划线和 YAML 展开，避免本地 guard 与实际 parser 不同。
- [Ray2.56.1 ProcessorConfig](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/llm/_internal/batch/processor/base.py)及[HTTP processor](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/llm/_internal/batch/processor/http_request_proc.py)：`(min,max)` 交给原生 actor pool；本轮只暴露原参数。
- UUID 范围交集、单卡可见性、独立准备深度与启动前容量拒绝属于保守工程决定，不认领研究方法或性能收益。

## 验证与未完成项

检查计划是：配置边界→1/2/4设备模拟→原池／原图完整输出→尾批／反序／失败／取消／下一查询→独立审查。
仅运行 CPU 和 localhost fixture，SDK／模型 opt-in 关闭；第一次环境失败、修复后重跑及摘要按独立 run ID 保留。
检查用例、完整结果及源码摘要以 [qualification.json](qualification.json) 为准；不把替身输出等同于原始 LOTUS/PZ 模型查询资格。

以下均为 **pending**：实际单卡新配置模型回归、真实双／多卡正确性和完整查询性能、Daft 原生每 worker GPU mask、
vLLM 各 DP rank 的真实创建／退出和 GPU ownership释放、Serve 多副本，以及各固定原生框架的实库复验。
现有 query supervisor 按 `(pid, create_time)` 追踪递归 children；持久外部模型服务不属于 query worker 的关闭范围，
此次只扩展模型进程身份观察，没有启动或停止任何服务，也没有把 shutdown 返回等同于物理退出。
旧 SAOR 五臂 `vllm_preflight` 的严格 TP1 sidecar 合同和 `start_endpoints.sh` 的逐卡服务方式未迁移到 native DP；
不能将新 sidecar 填入旧资格流程并认定通过。新资源配置不借用已有单／双卡旧来源证据。

完整查询原计时仍包含查询内原生建图、模型／processor重复初始化、传输、消费和结果检查；启动与最终清理另报。
本轮没有性能数值、可靠尾分位、在线扩缩或永久 worker 的新结论。

## 单卡后续准备

C 已把候选 `895daab3` 普通合入 baseline `7bac93ada893dd978b21d7935edd0bdb48d5980d`。
融合独审发现原版无条件关闭 Ray 图重试会改变旧默认；本功能分支精确接回 C 的两文件修复，
仅 `allowed_gpu_uuids` 显式模式传入零重试。定向资源／生命周期检查为 30 passed；
`895daab3` 的原始源码与 [CPU 资格摘要](qualification.json)保持原身份，不重标为融合或硬件结果。

后续单卡检查已经准备同一原生 LOTUS 图像 index/search 的 auto／explicit1 配对，
以及固定 vLLM0.25.1 官方 parser 的 CPU 检查；真实执行仍排在 main／PZ 之后。
配对按共享 host lease 独立申请，禁止自动恢复 stale lease；没有持锁等待开发，也没有启动模型。
