# 部署限定资源 auto 配置与单卡回归

当前模型检查固定源为 `d2fa8c9b83470057cd3bd10118ba7e44339695eb`。同一原生 LOTUS 图像
index/search 查询通过资源 JSON 切换 auto／explicit1；只在部署分配的 GPU UUID 集合内解析。
一张 RTX4090 上五条原生路径的完整输出、worker 身份与正常退出检查通过；其中四组额外精度环境验收通过，
HSE 的旧环境观察缺项使其外层验收仍失败。默认数学环境的首次数值失败保留，不能表述为全部配置通过。
实际范围、预算与完整观测摘要见 [单卡结果](single_gpu_qualification.json)。

基点 `7a4c1846fa8c973225d2fe894ad4f0ab11deded1` 已核实。独立功能 worktree 没有修改
baseline、main 或其他会话文件。895 初版 CPU 资格、88 后续准备与本次 d2 模型结果各保留原来源；
main 独立发布的服务资格另见[来源核对](main_service_provenance.json)。

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

## 当前单卡检查

实际 driver 为 Python3.12.3、Torch2.12.1+cu130、Transformers4.57.6、Ray2.56.1、Daft0.7.21、
LOTUS1.2.4、SentenceTransformers3.4.1、Pillow12.2.0。CLIP 固定 `openai/clip-vit-base-patch32`
revision `3d74acf9a28c67741b2f4f2ea7635f0aaf6f0268`，参数与 slow processor 内容摘要核对通过，输出 FP32／512维／L2归一。
实际物理 UUID 为 `GPU-b8905660-3863-5422-25f4-1426e61478d0`；auto 与 explicit1 都只分配这一张卡。

五组各执行 auto／explicit1 一次原生 `sem_index`＋`sem_search`：图库为 COCO 文档139、285、632三张原始JPEG，
batch上限2、K3、max_active_batches2；图片139查询重复两次，每臂5个图像行出现。没有丢弃 warm-up 或开启缓存。
输入文件、全量 RM 矩阵、原生持久化向量和 FAISS index、两次查询结果及所有构造回执都保留。
查询完整返回 `[139,632,285]`／位置`[0,2,1]`，自检索第一；独审重新计算完整内积和排名，
核对图库3×512、查询1×512、finite、归一、无丢重、尾批和重复查询可用。
原生持久化向量、FAISS payload 与采集矩阵逐位一致；这不是独立 embedding API 替代完整 LOTUS 查询。

| 路径／run ID | 原生输出、worker与退出 | 额外精度环境验收 | 配对图库最大绝对差 |
|---|---|---|---|
| SemLoom static／static-r2 | passed | passed | 1.7508864402770996e-7 |
| SemLoom HSE／hse-r1 | passed | **failed，explicit1 历史观测缺项** | 0，三份矩阵逐位同 |
| Ray Data fused／rayf-r1 | passed | passed | 0，三份矩阵逐位同 |
| Ray Data staged／rays-r1 | passed | passed | 0，三份矩阵逐位同 |
| Daft native／daft-r1 | passed | passed | 0，三份矩阵逐位同 |

这些执行计划显式使用仓外固定环境 `NVIDIA_TF32_OVERRIDE=0`，不改变生产代码或未配置旧路径的数学默认。
图库绝对误差≤1e-5、余弦≥0.99999、范数误差≤2e-5及原有分数／排名要求均未放宽。
全部已构造 GPU／预处理 worker 的 UUID、PID、Linux birth ticks、worker UID／model UID与预处理摘要核同；
每对单独正常取得、释放共享 host lease，`recover_stale=False`，没有占锁等待开发。
物理见证确认全部自有出生身份和 GPU owner 已退出，无应急信号；不把 `shutdown()` 返回等同于物理退出。
Daft 本轮仅证明实际单 UUID mask；多卡每 worker mask 仍待硬件。

HSE explicit1 原外层环境验收失败的 GPU owner 为 PID365682／ticks321535028。
旧观察器最后保存的 CUDA mask及两项TF32字段均空，但没有记录完整 `proc/environ` 长度；
无法证明它是退出窗口的空数据，还是非空环境中变量缺失，历史数据不能补造为通过。
其原生完整查询、实际 GPU UUID与物理退出证据独立通过；精度环境资格仍缺一臂。
之后只改仓外观察器：完整环境为空时记 unavailable并保留此前正证据；非空环境缺项或冲突则永久拒绝本次验收。
最后三组新观察均通过；Daft auto 的两次空环境读取作为 unavailable 保留，之前有正证据。
CPU序列检查覆盖 good→empty、only-empty、empty→good、冲突后恢复等情况，44项通过；没有重标 HSE 旧失败，也没有追加模型复测。

## 数值失败与有限诊断

固定 d2 的首个 `static-r1` 在默认数学环境完成两臂共10行，但图库最大绝对差
`1.182854175567627e-4`，原生返回分数跨配置最大差 `2.372264862060547e-5`，超过原1e-5阈值。
两臂 query 向量及重复结果逐位同、排名相同，实际模型／数据身份和正常退出通过；整对仍记数值失败。
auto 的图库实际2次 forward／3行，explicit1为3次／3行，Daft 原生分区／RecordBatch会改变实际 batch 形态。
输入文件与处理器身份一致；旧运行没有采集最终像素张量和完整 GPU 数学标志，因此不能声称旧像素已逐位观测或根因已隔离。
[失败摘要](single_gpu_static_failure.json)与73成员原件完整保留。

后续 `numeric-r2` 只用同一实际 actor、前两张图片做6行：default batch2、仅关闭cudnn TF32的batch2、
两个strict single。此次实际像素张量逐位相同；default与strict batch2输出逐位同，strict batch2／single
最大差 `1.7508864402770996e-7`。没有在同一 actor 测 default single，旧单图向量仍与新诊断不同，
故**未建立 TF32 根因证明**。新子进程的全局 override0属于另一个固定部署环境，之后的通过不修复或覆盖原默认失败。
数值、标志、像素摘要、NVML身份和正常退出见[6行诊断](numeric_diagnostic.json)。

[PyTorch2.12.1数值说明](https://github.com/pytorch/pytorch/blob/v2.12.1/docs/source/notes/numerical_accuracy.rst)
说明批量与切片计算可能不同；[固定环境变量文档](https://github.com/pytorch/pytorch/blob/v2.12.1/docs/source/cuda_environment_variables.rst)
定义 override0禁止TF32。[Daft0.7.21 into_batches](https://github.com/Eventual-Inc/Daft/blob/v0.7.21/daft/dataframe/dataframe.py#L3496)
将重分批标为 best effort。这些资料用于解释可行机制，不替代本轮缺失的因果测量。

## 计时与生命周期

以下是全部单次 strict 环境值，单位秒；HSE行仍带上述额外环境缺项。
`application_e2e_s`从源内容检查／解码到原生建库、全部查询与输出检查；eager owner／identity设置另在启动，
结果证据序列化另计。表中查询时间为`elapsed_s`，包含原生返回后的结果验证与记录；
`submitted_monotonic_ns→returned_monotonic_ns`仅到`sem_search`返回，终点在验证前，不能将两种边界混写。
启动列不包含所有路径的所有模型加载：Ray Data每RM调用重建CLIP，三次实际构造分别在图库、第一和第二查询内，
仍完整计入对应查询与application时间；独审用真实构造时刻逐一核对。不能把查询重复加载移到启动列扣掉。
SemLoom沿用原池，Daft沿用原生UDF生命周期，未强制为统一永久worker。

| 路径 | 配置 | 启动 | 原生建库 | 查询1／2 | application E2E | 清理 |
|---|---|---:|---:|---:|---:|---:|
| semloom_static | auto | 28.879090 | 4.854317 | 0.057246 / 0.055061 | 5.000270 | 1.454308 |
| semloom_static | explicit1 | 28.481998 | 4.884799 | 0.060737 / 0.059284 | 5.047926 | 1.459278 |
| semloom_hse | auto | 28.779556 | 5.463846 | 0.066475 / 0.050595 | 5.613858 | 1.459079 |
| semloom_hse | explicit1 | 27.886009 | 5.004808 | 0.064127 / 0.059438 | 5.160456 | 1.459895 |
| Ray fused | auto | 21.457259 | 11.425110 | 9.985427 / 9.850879 | 31.290894 | 1.215319 |
| Ray fused | explicit1 | 20.968196 | 11.521919 | 10.014067 / 10.171576 | 31.736831 | 1.463477 |
| Ray staged | auto | 21.441457 | 11.998381 | 10.410502 / 9.770466 | 32.216118 | 1.458674 |
| Ray staged | explicit1 | 20.955906 | 11.062907 | 9.378800 / 9.511137 | 29.981825 | 1.456061 |
| daft_native | auto | 12.443889 | 1.798332 | 0.027627 / 0.028546 | 1.883529 | 0.000769 |
| daft_native | explicit1 | 13.331526 | 1.797480 | 0.029374 / 0.026703 | 1.885756 | 0.000791 |

完整纳秒边界、实际模型构造 UID／PID／ticks、native lifecycle和所有原数值见[机器摘要](single_gpu_qualification.json)。
每配置只有一次workflow和两次闭环查询；没有性能优越性、可靠尾分位、饱和度、在线扩缩或最佳卡数的新结论。

## 预算、原件与独审

用户批准累计图像上限由50增至66，原10行失败不重置：默认失败10＋数值诊断6＋五组strict配对50＝**66/66**，剩余0。
首次88模型构造前失败、无模型UUID探针、numeric-r1正常lease拒绝和启动前包装器失败均0推理行，单独保留。
numeric-r1从未取得别人的活动lease；它的`lease_released=false`不是自有lease泄漏，未恢复、终止或修改外部工作。
自有12个模型HTTP POST计划未执行，实际0；main已独立完成服务闭包，来源见下一节。

四组本次无损原件均在服务器保留，并备份到Mac，逐成员字节数和SHA核验：

| Mac归档名 | 字节 | 成员 | SHA-256 |
|---|---:|---:|---|
| r3-static-r1-evidence.tar.gz | 139612 | 73 | `ea6be512bf2ada92750865caee370b2a26cdc93ee48a2e0af892c08ef617c965` |
| r3-numeric-r2-evidence.tar.gz | 740951 | 28 | `a02f8fc062cced72163b8c232d9556549e0d834bcaddb6c7bf662171ed872fe0` |
| r3-strict-static-hse-evidence.tar.gz | 839477 | 102 | `acf093be582c38762dd2247446e45d48cce7b60934228951999ded2215937515` |
| r3-strict-ray-daft-evidence.tar.gz | 920457 | 169 | `c1c8f9631b2d245bfcbd4199381497f39c2130bf509f9d26226ad4defaee9459` |

归档保留实际配置、完整CLI输出、输入JPEG、RM NPZ、原生index／vecs、模型／worker回执、进程树／NVML时间序列、
外层环境观测、监督器、helper源码与摘要。不同归档中已有证据成员复用同来源，不当作独立新运行。
部署runtime env、外部连接信息、模型权重与完整数据集不入Git。
按[机器摘要](single_gpu_qualification.json)的归档／成员清单摘要和run ID向项目owner取得仓外原件；
本地审计脚本及完整独审报告也已保留，无需重跑模型即可重算。

独立子代理先核73成员失败、28成员诊断、102成员static／HSE，再核169成员Ray／Daft，重新读取NPZ、受限NumPy原生vecs与FAISS payload、
计算内积／排名并核每个出生身份、实际GPU owner、正常物理退出和累计预算。没有用CPU替身增加模型行或硬件资格。
仓外控制器检查从26项加严后暴露清理问题，保留37pass／1fail原件，修后38项及新环境观察44项通过；
这些计数、生产UUID修复81项、895历史421项存在覆盖重叠，不相加。

## main 服务的独立来源

main owner已将仅4生产文件的可选vLLM服务auto闭包经[PR25](https://github.com/3444374/ai-operator-execution-optimization/pull/25)
发布到 `main@2041da67d883f54285952042482493a4a76a6ba1`。
实际模型资格源 `7b20feb507f64af27354b1d4c39c0606c7160725`；C核对这4文件与发布main逐字同。
原生LOTUS SDK auto／explicit1共12实际请求，TP1／DP1、单卡、unknown0，正常物理退出／lease释放及两轴独审由main线交付，
见[该源服务报告](https://github.com/3444374/ai-operator-execution-optimization/blob/2041da67d883f54285952042482493a4a76a6ba1/experiments/results/diagnostics/main_service_auto_20261015/README.md)。

本线只读核对handoff与4文件：launcher逐字同d2，gpu_allocation只有格式差异／AST同，
vllm_resources保留既有核心并新增资源sidecar验证，vllm_preflight已有main修订。
完整SHA与核对范围见[来源摘要](main_service_provenance.json)。本线没有重新审计main请求原件或远端Git对象，
不把其12请求重标为整个d2／本次CLIP资格；之后完整MovieQ2／Animals复验仍归main owner。
C集成时须保留已发布4文件与来源边界，不能从本分支整文件覆盖，PZ来源守卫也由C精确处理。

## 单卡后续准备

895初版的CPU／graph double／合成1、2、4设备身份／localhost HTTP fixture由[原始资格](qualification.json)拥有。
覆盖配置边界、原池／原图完整输出、尾批／反序／失败／取消／下一查询和unknown不安全重发拒绝；
未运行SDK／模型opt-in，不据此声称真实LOTUS/PZ多卡或性能。
C普通合入baseline `7bac93ada893dd978b21d7935edd0bdb48d5980d`后发现默认兼容问题；
本分支88精确接回两文件修复，显式`allowed_gpu_uuids`模式才关闭Ray图重试，未配置图保留旧默认。
同30项定向检查及独立复跑通过；895原源码／资格不重标为融合结果。

当时固定源 `88f9b08c0a1de6f4c0516b18a89d6aa4b909d3cf` 的[准备摘要](single_gpu_preparation.json)
包含driver／serving只读环境、缓存内容和vLLM0.25.1官方parser的DP1／TP1／PP1／MP检查，未创建engine。
原LOTUS／LiteLLM两次三行Map经6个localhost fixture POST，完整顺序／原解析及重复一致，模型请求0。
58成员准备归档171621字节，SHA `34d3a54b865a8ec09e81505ab4218ab0fc92f46b95b6e7c8830242240d35e3ff`；
Mac已逐成员核验，服务器原件保留。传输中断和system Python缺psutil等失败均保留，未改共享环境。
这些是历史准备事实，后续实际单卡状态以上文d2结果为准。

## 首次单卡失败与 UUID 表示修复

main／PZ正常释放后，固定88首个static auto在GPU actor构造身份检查处停止，未进入index/search，
无CLIP GPU模型构造回执或推理行；正常物理退出及lease释放均核验。另一次短正常lease无模型探针确认
实际Torch2.12.1的`torch._C._CUuuid`是完整裸UUID，NVML为同值加`GPU-`，直接字符串比较误拒合法设备。
[固定官方实现](https://github.com/pytorch/pytorch/blob/v2.12.1/torch/csrc/cuda/Module.cpp#L984)也定义该转换。
修复只在worker边界接受完整ASCII `8-4-4-4-12`、归一hex大小写后精确匹配；允许集合须为完整NVML UUID，
拒畸形、MIG、bytes及规范化重复，返回允许集合原值。CPU先3fail／12pass，修后五模块81pass，独立复跑同81并审查。
修复发布为d2后才重新固定源码做本次模型检查；[88失败与探针](single_gpu_first_attempt.json)保持原身份。

## 未完成项

真实双／多卡正确性与完整查询性能、Daft多卡每worker mask、vLLM DP>1各rank创建／退出与实际映射、
Serve逻辑服务／replica身份和多副本、Ray HTTP原生min/max实际扩缩仍pending。
HSE旧额外环境验收缺项未复测；真实GPU失败／取消注入后的下一查询仅有CPU合同检查，不能借正常GPU运行补齐。
IMLane本地CLIP仍未接入。新图文模型资格和main合并后完整LOTUS/PZ workload由各owner另行记录。
现有supervisor按PID／出生身份追踪自有children；持久外部模型服务不属于query worker关闭范围。
旧SAOR五臂preflight的严格TP1 sidecar／逐卡启动流程没有迁移到native DP，不能把新sidecar填入旧流程认定通过。
