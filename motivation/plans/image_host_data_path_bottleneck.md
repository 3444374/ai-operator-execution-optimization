# 图像 AI 算子系统木桶效应与 host data path 瓶颈判定实验

状态：预注册设计，等待 schema v2 gate 与 AutoDL 正式复测

适用 workload：PostgreSQL `BYTEA` → Daft → Ray → CLIP `AI_EMBED`

性质：动机/机制归因实验，不是策略有效性实验

## 1. 研究问题与判决

本实验不预设 PCIe 或“GPU 必须 100%”的结论，而是回答：在同一图像、模型语义、
物理资源和计时边界下，Daft Native/Ray、Ray Data、vLLM pooling 与项目静态流水线
分别由哪块短板限制，哪个系统能用更少 bubble 完成相同有效工作？主要阶段包括：

1. PostgreSQL/Daft 取数；
2. JPEG decode 与 processor；
3. Ray dependency、object store 或 host copy；
4. pageable/pinned host memory 到 GPU 的 H2D；
5. CLIP GPU forward；
6. embedding D2H 与结果返回。

最终必须给出一个可证伪判决：`CPU-preprocess-bound`、`framework/host-copy-bound`、
`PCIe/H2D-bound`、`GPU-compute-bound` 或 `mixed/undetermined`。如果证据不支持
PCIe，就把 PCIe 作为排除项，不为预设故事制造结论。

这里采用木桶模型：pipeline steady throughput 受最慢阶段的有效 service capacity
约束。某阶段“打满”只表示它先到容量上限；系统好坏则看相同 workload 下是否能
减少其他阶段等待和 GPU bubble，并转化为更高 E2E 吞吐/有效 MFU、更低 JCT/能耗，
而不是只比较单个 utilization 百分比。

## 2. 为什么现有 baseline 不能回答

现有 schema v1 的 operator-E2E 结果能证明阶段拆分比校准后的 fused Daft UDF
更快，但不能定位 PCIe：

- `batch_service` 包含 CPU ObjectRef 依赖等待、actor queue、host copy、H2D、
  forward、D2H 和返回；
- 只记录逻辑 tensor 字节，没有硬件 memcpy kind/bytes 或 CUDA stage 时间；
- `nvidia-smi` 500ms 采样会漏掉短 kernel，且单卡旧字段平均了另一张空闲卡；
- 没有 CPU per-core 利用率、busy-core 数、NUMA/PCIe 拓扑和 pinned/pageable 对照。

因此，`tensor_bytes / 某段墙钟` 不能称为 PCIe 带宽，低 GPU util 也不能单独证明
CPU 或传输饱和。

## 3. 表示阶梯：逐层加入真实开销

所有臂使用同一批 doc_id、同一 CLIP revision、float16 forward、512d float32
L2-normalized embedding，并做逐行 rounded digest 与抽样 cosine/max-abs 校验。

| 层 | 输入起点与执行路径 | 新增成本 | 回答的问题 |
|---|---|---|---|
| R0 compute ceiling | GPU-resident tensor → CLIP forward | 仅 forward/归一化 | GPU 计算平台期是多少 |
| R1 pinned H2D | pinned host FP16 tensor → H2D → forward | PCIe H2D | H2D 单独占多少，能否与计算重叠 |
| R2 pageable/Ray tensor | pageable NumPy/Ray object → host copy → H2D → forward | object-store view/copy + H2D | Ray/host-copy 的边际成本 |
| R3 in-memory JPEG | RAM 中 JPEG → decode/processor → R2 | CPU 预处理 | CPU prepare 的边际成本 |
| R4 DB operator E2E | PostgreSQL BYTEA → Daft lazy source → R3 | DB/Daft/source | 真实 operator E2E 的额外成本 |

pgvector sink 作为 R5 system-E2E 单独补充。它回答完整数据库作业时间，不参与
R0–R4 的 PCIe/CPU 机制判定，避免写回掩盖上游瓶颈。

R0/R1 是诊断 ceiling，不是项目需要击败的系统 baseline；R4 中必须同时包含 Daft
fused UDF、Daft/Ray Data staged pipeline 和冻结最佳 project pipeline。当前已完成的
Daft Native/Ray 数字仅属于 fused UDF 轨道，不能代替 staged 强 baseline。

## 3.1 主流执行栈对照，不预设谁有固定缺陷

| 轨道 | 正式对照 | 公平边界 |
|---|---|---|
| fused data-engine | Daft Native、Daft Ray `@daft.cls` 内 preprocess+forward | 校准 batch 与 fractional-GPU actor shape；用于隔离粗资源边界，不代表 Daft 最强形态 |
| staged Daft-on-Ray | Daft CPU decode/resize/processor → Daft GPU `@daft.cls` | 同一 lazy DataFrame、按算子资源声明、校准 batch/max-concurrency/actor shape |
| staged Ray Data | Ray Data CPU `map_batches` → GPU callable-class actor pool | 固定 batch/actor pool/in-flight；不把默认参数当强 baseline |
| model service | vLLM pooling CLIP | processor 在服务内，单列 service ceiling 与 DB→service E2E，不和 tensor-only micro track 混读 |
| proposed static | Daft source → Ray CPU preprocess → tensor-only GPU actor | 冻结静态 batch/active batches；动态策略全部关闭 |

每条轨道报告两组曲线：

1. **matched physical/resource budget**：相同 GPU 数、CPU core reservation、数据行、
   batch 与冷/热生命周期；这是判断系统效率的主比较；
2. **independently calibrated best-achievable**：每个系统独立调 actor/batch/in-flight，
   这是判断各系统可达到上限的补充，不能替代 matched-resource 结论。

现有官方系统本身已经提供 batch、actor pool、fractional GPU、in-flight/prefetch 等
机制，因此不能把“阶段失衡”写成它们的普遍固有缺陷。本实验只判定在本项目的
COCO/CLIP/2×4090 regime 下，哪个边界形成短板、项目是否改善。

### 3.2 会推翻项目动机的结果

- 调优后的 Daft/Ray Data 与 project 在 matched-resource 下差异小于 5%，且
  JCT、GPU duty、能耗均相近：阶段拆分只是实现选择，不能包装成系统贡献；
- project 只在多拿 CPU cores/source shards 时更快：只能报告资源换性能，不能声称
  同资源效率更高；
- GPU duty/effective MFU 提高但 P95/P99/JCT 或 SLO 明显恶化：不能晋级；
- 优势只存在于 CLIP ViT-B/32 小模型，换分辨率/模型或 workload 后消失：结论必须
  限定为该 regime，不能外推为数据库 AI 算子的普遍问题；
- Ray Data 官方 staged pipeline 已达到相同结果：本项目需要依靠后续数据库语义、
  multi-job shared credit 或状态感知策略证明增量，不能把 stage separation 当原创。

## 4. 两阶段实验，避免诊断同步污染 headline

### 4.1 阶段 A：低扰动正式曲线

- 先把总 work volume 增至每个 formal 点至少稳定运行 60 秒；优先导入至少 20K
  条不同的 COCO train 图像，避免只重复 5K val 图造成 OS/cache 混淆。若只能重复
  5K，必须标为 diagnostic。总行数只用于减少启停噪声，不把“行数更多”误当作
  “并发压力更大”；
- 避免直接跑完整笛卡尔积：先在单卡 R0 用 batch=`16, 64, 256` 找 compute ceiling，
  再在 R4 固定候选 batch，扫描 active batches=`1, 2, 4, 8, 16, 32`；只在 R0/R4
  差距最大的代表点补 R1–R3，最后把代表点扩到双卡；
- 吞吐平台预定义为连续两个更高压力点的 images/s 改善都小于 3%、CV≤5%，且没有
  OOM、无界排队或质量错误。以达到 97% 平台吞吐的最小 active work 作为该层
  `minimum saturation point`；
- 如果 R0 自身没有平台，先解决模型/batch 测量；如果 R0 有平台而 R4 始终明显低于
  R0，才进入逐层瓶颈定位；
- screening 可用 `1 warmup + 1 run` 淘汰明显非平台点；所有进入结论的候选每臂
  `1 warmup + 3 formal repeats`，相邻表示层交错、随机化顺序；
- 容量曲线之后，在选定压力点固定 CPU worker、source shard、actor 数和 active
  batches 做 matched-resource 表示层对照；资源相同曲线与各自独立最优曲线分开报告；
- 不启用逐 stage `cuda.synchronize()`，记录 operator E2E、first complete batch、
  images/s、CPU busy cores、active-card GPU util/memory、逻辑字节和质量；
- headline 使用 median，报告全部 repeats 和 CV。

阶段 A 给出真实吞吐/JCT；不能仅凭它称 PCIe 饱和。

这里“喂满 GPU”的操作定义是达到 R0 的吞吐/forward 平台，而不是要求
`nvidia-smi` 显示 100%。短 kernel、访存等待或采样周期都可能让 utilization 与实际
容量平台不一致。若增加并发只增加 queue/JCT 而吞吐不再上升，已经越过饱和点，不能
继续用更大的队列制造“更忙”的假象。

### 4.2 阶段 B：侵入式机制诊断

只对阶段 A 的代表点（默认 batch=64，以及曲线转折点）运行：

1. runner 的 `--detailed-stage-timing`，显式同步测 host copy、H2D、forward、D2H；
2. 用 CUDA events 复核 GPU H2D/forward/D2H device timeline；
3. 用 Nsight Systems/CUPTI 对一个短窗口记录实际 memcpy kind、bytes、duration、
   kernel gap 和 CPU thread timeline；若容器权限不允许，明确标记该证据缺失；
4. `nvidia-smi topo -m`、PCIe current/max generation/width、CPU NUMA 拓扑随
   manifest 保存；
5. pinned vs pageable、FP32 host tensor vs FP16 host tensor做诊断对照。

阶段 B 因同步会改变重叠行为，不能替代阶段 A 的 headline，只用于分解机制。

## 5. 变量与混淆控制

固定项：数据行集合、图像解码结果、模型/processor revision、输出维度、dtype、
batch、GPU 数、CPU 配额、actor 数、source shards、active batches、质量门槛和
计时边界。每个实验块只改变一个表示层或传输机制。

必须记录：

- 输入 JPEG bytes、host tensor bytes、device tensor bytes、output bytes；
- CPU preprocess、Ray dependency/completion、host copy、H2D、forward、D2H；
- host-wide per-core CPU、busy-core equivalent；该指标不能冒充 actor attribution；
- active-device GPU utilization、memory 与采样频率；低频 util 不能称 MFU；
- GPU power/clock、每卡估算能耗、`nvidia-smi topo -m`、PCIe current/max link
  generation/width 与 CPU NUMA 拓扑；
- pending batches、completion wall、actor service、preprocess 与
  `completion-preprocess-actor-service` 的未归因 wait；该 wait 只能定位候选 queue/
  framework gap，不能继续拆成 serialization 或调度器耗时；
- output rows/exactly-once、全维 sum、rounded digest，以及抽样逐行 cosine/max-abs。

不要把人为复制 payload 得到的大传输量作为 headline。若为了画机制曲线扩大 tensor，
必须标为 synthetic diagnostic；真实结论仍来自 COCO/CLIP，或后续更高分辨率/VLM/
视频等真实 workload。

GPUDirect Storage 不作为默认优化臂：它改变的是存储→GPU 路径，而当前 R3/R4 仍有
CPU JPEG decode/processor。只有先证明存储 I/O 位于关键路径，或同时加入 GPU decode
并单独记录语义/资源变化时，才增加 GDS/DALI/nvJPEG 对照。

## 6. 预注册 GO/NO-GO 门槛

### PCIe/H2D 路线 GO

同时满足：

1. H2D 位于关键路径，steady-state 中占 operator wall 至少 20%；
2. pinned memory、降低传输 dtype/bytes 或 H2D-forward overlap 中至少一项，在
   质量不退化时改善 E2E/JCT 或吞吐至少 5%；
3. 至少 2/3 formal repeats 同向；Nsight/CUDA events 与 runner 分段方向一致。

若 H2D 占比低于 10%，且上述传输改动均不足 5%，则 PCIe 优化 **NO-GO**。
10%–20% 或工具证据冲突时判 `mixed/undetermined`，不能选边。

### CPU preprocess 路线 GO

同时观察到 R2→R3 的主要增量、preprocess 位于关键路径、busy cores 接近所给 CPU
配额，并且增加 CPU workers/更快 decode 在质量一致时改善至少 5%。若 CPU 数增加
但吞吐不变，则需要继续检查 source、Ray 或 GPU 平台期，不能只凭阶段耗时下结论。

### Framework/host-copy 路线 GO

R1→R2 增量至少占 wall 20%，且减少 object-store copy/所有权转换或改变 actor
边界带来至少 5% 改善；否则不把 Ray serialization/host copy 包装成主瓶颈。

### GPU compute 路线 GO

R0 已到平台期，R1–R4 增量都小；增加 GPU 或改变模型 batch 明显移动平台期，而
CPU/传输改动不足 5%。GPU util 仅作旁证，不能替代 kernel/device timeline。

## 7. 输出表与结论模板

主表按表示层、batch、GPU 数报告：E2E、images/s、first output、CPU busy cores、
active GPU、stage critical-path 占比、实际/逻辑 bytes、质量。另给两张图：

1. R0→R4 累积时间瀑布图；
2. batch sweep 下 H2D、forward、CPU preprocess 的占比与吞吐平台图。

系统比较必须再给一张“有效工作效率”表：

| 类别 | 指标 |
|---|---|
| 用户结果 | images/s、JCT、first output、P50/P95/P99、质量 |
| GPU 占空比 | target kernel duty cycle、copy-engine time、bubble ratio |
| 计算效率 | active-window SM/Tensor utilization；只有 FLOP 计数方法经校准后才报告 E2E effective MFU |
| 资源效率 | images/GPU-s、images/J、CPU-core-s/image、峰值显存 |
| pipeline 木桶 | 各 stage service capacity、queue/backlog、critical-path 占比 |

同一模型下，吞吐更高会带来更高的相对 E2E effective MFU；但绝对 MFU 还需要统一的
per-image FLOP 计数。若 PyTorch profiler/分析工具不能完整覆盖算子，只报告
`estimated effective MFU` 并公开 FLOP 口径，不把 SM utilization 等同于 MFU。

结论必须使用以下句式之一：

- “在 COCO/CLIP/该硬件配置下，证据支持 X 是主要 feeding 限制；Y 已按门槛排除。”
- “当前为 mixed/undetermined，需要补 Z 计数器，不能声称 PCIe/CPU/GPU 饱和。”

## 8. 与后续研究实验的边界

本计划只证明为什么需要 stage separation、资源配比或 host data path 优化。
确定瓶颈后，策略实验才以冻结最佳静态 project pipeline 为强对照，测试 frame-work
credit、请求成形、多 job shared credit 等是否改善吞吐、JCT、P95/P99 或 SLO。
不能把 R0/R1 ceiling、Daft 系统 baseline 和策略 baseline 混为同一个对手。

## 9. 设计依据

- Daft 官方 GPU 指南：`https://docs.daft.ai/en/stable/custom-code/gpu/`。官方建议
  GPU workload 使用 `@daft.cls`、调 batch，并在单 invocation 不能饱和 GPU 时使用
  fractional GPU + `max_concurrency`；所以 baseline 必须先校准 actor shape。
- Ray Data 官方 `map_batches` 与 `ActorPoolStrategy`：
  `https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.map_batches.html`、
  `https://docs.ray.io/en/latest/data/api/doc/ray.data.ActorPoolStrategy.html`。官方已支持
  CPU/GPU staged transforms、fixed actor pool 和 per-actor in-flight；必须作为强 staged
  baseline，而不是只测一个默认配置。
- vLLM pooling 官方说明：`https://docs.vllm.ai/en/latest/models/pooling_models/`。
  pooling 支持 embedding/classification，但官方明确不保证相对 Transformers 的性能
  提升；它在本实验中是 service track，不是预设天花板。
