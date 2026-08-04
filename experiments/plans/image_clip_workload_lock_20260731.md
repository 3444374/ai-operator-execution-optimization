# 图像 AI_EMBED / AI_CLASSIFY Workload 锁定方案

日期：2026-07-31
状态：**🔴 首个 workload（2026-08-02 分类轨道升级）**。现有 COCO/CLIP
AI_EMBED 先作为执行链和 host-data-path 门禁；正式产品语义优先验证
AI_CLASSIFY。图像每行包含 JPEG bytes、
CPU decode/processor、约 600KB 的 FP32 pixel tensor 和 GPU forward，因此能把文本轨道
不明显的 host data path 与 CPU/GPU stage balance 变成可测变量。**这只是 workload
选择动机，不预设 DB read、Ray copy、PCIe 或 CPU 一定是 binding bottleneck**；具体
判决以 `motivation/plans/image_host_data_path_bottleneck.md` 的 R0→R4 实验为准。
CLIP 不绑死在冷启动旗舰上。详见
`research/daft_db_gpu_bridge_direction_scope_20260731.md` §10 + §10.1（benchmark 三层）。

baseline 的公共分层、原生性、证据等级和最低指标合同只以
`baseline_reference.md` 为准；本文只维护图像 workload、质量语义和运行矩阵。

> ✅ **2026-08-01 更新**：方向已锁 A+B（见 `experiment_status_and_gaps.md` §0）；**§6 go/no-go 门禁已过（GO，5K 规范跑显示 CPU preprocess 明显重于 GPU actor service）** → 下方「暂停 build」**已解除**，进入 path-B runner 建设期。该比例来自串行阶段计时，不等同于实测 GPU idle。详见 `motivation/results/gpu/image_clip_bottleneck_profile_20260801.md`。

关联：`research/daft_db_gpu_bridge_direction_scope_20260731.md`；`research/evaluation_metrics_survey_20260731.md` 附录 A.4 / B.4；`notes/communication_notes.md` §5；`code/INFRA_STATUS.md` §6–§7；`PROJECT_OUTLINE.md` §5.3（多模态泛化）。

> 本方案不改题目。它是把 PROJECT_OUTLINE §5.3 既定的"token-budget → frame-budget 多模态泛化"从 P2 提到当前优先级，作为**体现异构资源调度（学长反馈的第二种 bottleneck：重 CPU 数据准备 → GPU 等）的 flagship workload**，同时给课题加"数据库 AI 算子"定位的 workload 锚点。

> ⚠️ **2026-07-31 scoop 检索后更新（暂停 build）**：scoop 工作流（`notes/communication_notes.md` §5.5）判定项目 **partially-scooped**——prefix-aware 子切片已被 SOLO(ICML'26)/Liu 直接发表；"上游状态感知"一般声称被 llm-d/Preble 占据；Kalypso(2026-07) 重叠 framing；**Daft v0.6.9 已在同栈产品化 prefix bucketing + router，其 Future Work 明确列出"读 serving-engine cache metrics"= 本项目剩余切片**。本 workload 的 build ~~暂停~~ **已恢复**（2026-08-01：方向锁 A+B + §6 门禁过 GO，见文首 ✅ 更新与 `motivation/results/gpu/image_clip_bottleneck_profile_20260801.md`）。图像 workload 的价值现在主要在：① 多模态模态无关性（剩余可防御切片的一部分）；② 寻找"重 CPU 准备 regime"作为剩余切片**可能仍有显著收益**的最后机会。但它**不再是"体现异构调度"的纯技术展示**——必须先过 scoop 边界。

---

## 1. 为什么锁这个 workload（目的）

三条理由（来自学长反馈 + PolarDB 核查 + 项目证据）：

1. **体现异构资源调度**——学长判断当前纯文本单 job 场景"GPU 不慢、数据未到"，项目 feeding 95–98% 已证实。要让"CPU 准备 vs GPU 推理"的异构调度成为真实变量，workload 的**每行 CPU 准备成本必须重**。图像 AI_EMBED 是教科书级拆分：CPU 做 JPEG decode + resize + normalize（每图毫秒级，常重于 GPU CLIP forward），GPU 做 CLIP embedding。这正是 PolarDB Lakebase benchmark 用的形态（image 803,580 张）。
2. **多模态泛化验证（§5.3 既定路线）**——验证 token-budget → frame-budget、queue-adaptive flush → 完全复用的模态无关性。策略接口和中性 `cost_units` 已具备（INFRA_STATUS §6），只缺图像 source + CLIP workload。
3. **"数据库 AI 算子"定位锚点**——AI_EMBED/AI_CLASSIFY 是 Snowflake Cortex / PolarDB / Oracle 都有的正经数据库 AI 算子；写回 pgvector 是数据库 sink。采用它能强化"数据库 AI 算子"定位（对学长/导师/审稿人）。

**不 claim**：这个 workload 能保证项目策略赢。它只是让异构调度的问题真实存在；
能否赢 Daft/Ray Data 已有 staged overlap 和冻结最佳项目静态点，要按 §7 跑出来。

---

## 2. 架构决策（CLIP 如何服务——2026-08-01 能力校正）

CLIP 是 embedding 模型，不是生成式 LLM，但当前 vLLM 已通过 pooling runner
正式支持 `CLIPModel` / `SiglipModel` 的图像 embedding。因此，旧前提“CLIP 不能
复用 vLLM、CLIP 服务没有 batching”已经失效。真正需要区分的是**预处理在哪一层**：

| 选项 | 输入和预处理边界 | 角色 | 采纳 |
|---|---|---|---|
| **Daft built-in `embed_image`** | Daft `decode_image` → 内置 Transformers provider | Daft 自己拥有 batching/concurrency/backpressure 的主 native baseline | 256 图 gate 与逐行 parity 已通过；待独立 calibration/formal |
| **官方 803,580-row ResNet18 parity** | 固定 `Eventual-Inc/Daft@3f5bdd175b7de3dcdf35765e1ba604b5c1cb8e15` 的 `daft_main.py` / `ray_data_main.py` | 公开 file/object track 的 vendor-code baseline | pin/文件哈希已记录；待双 4090 原图 gate，仅做白名单环境/指标适配 |
| **Ray Data native API graph** | SQL source → CPU `map_batches` → GPU callable class | Ray Data 拥有调度/backpressure；workload UDF 不得包含项目策略 | 旧 gate 仅证明 adapter 可运行；移除项目 inflight 后重做 native gate |
| **项目自写 Daft fused/staged UDF** | 自写 `@daft.cls` 内核与阶段边界 | 暴露阶段耦合的 diagnostic reference，不是官方 baseline | 保留历史诊断，不进入主排名 |
| **vLLM pooling (`--runner pooling`)** | encoded image 进入服务，processor + pooling 在服务内部 | 与文本统一运维的成熟服务 baseline；官方说明 pooling 目前以功能便利为主，不保证优于 Transformers | 必跑服务 baseline；也是部署默认候选 |
| **常驻 Ray CLIP GPU actor** | Daft/Ray CPU worker 做 decode/resize/normalize，GPU actor 只收 typed tensor batch 并 forward | 直接复用现有 Ray actor pool/backpressure；保留“CPU 准备与 GPU 推理分离”的可归因主路径 | **ours 主路径** |
| Infinity / Ray Serve | encoded image 或服务内 preprocess，均自带 batching | 快速 smoke/补充 baseline；若使用必须冻结并记录隐藏 batching | 可选 |
| Triton | tensor-input、成熟 metrics/dynamic batching | 生产级上界；AutoDL 当前无 Docker，不能作为第一实现 | 容器环境 optional |

**决定**：主路径不是自写一个不受控的 FastAPI serving engine，而是通过统一
`ImageEmbeddingBackend` 接口接入**常驻 Ray GPU actor**。CPU worker 产出
preprocessed tensor，GPU actor 只执行 CLIP forward；项目已有 actor pool、credit、
backpressure 和 exactly-once 机制可以直接复用。vLLM pooling 作为统一部署默认候选
和强服务 baseline；旧 Daft fused/staged UDF 只是一条项目自写诊断臂。正式框架结论
必须来自 Daft 内置 AI Function、官方 ResNet18 parity 脚本和 Ray Data native graph。

Daft built-in 校准只扫描其公开原生 `batch_size`；GPU concurrency/actor placement 由
Daft 根据 Ray 集群 GPU 自行推断，不额外包项目 actor 或 admission。Transformers
provider 当前使用模型默认 dtype，runner 必须记录 `provider_default`，不得把命令行
`--dtype` 误记为已生效；公平比较同时报告相同默认精度控制和项目优化精度臂。

**为什么不能只选 vLLM pooling**：它会把本轮 profile 中最重的
decode/resize/normalize 移入服务端，无法直接验证“Daft/Ray 上游 CPU preprocess 与
GPU embed overlap”这一机制。它并非不好，而是回答另一个问题：一个成熟黑盒图像
服务的端到端上限。正式报告必须把两类预处理归属分别计时，不能混成同一 baseline。

**为什么 Ray GPU actor 是 AutoDL 上 ours 的默认 engine**：Daft/Ray 负责数据读取、
CPU 预处理、请求组织和提交；actor 仅常驻模型并执行 GPU forward，不再二次攒批。
AutoDL runbook 明确不使用 Docker，而 Triton 官方推荐 NGC 容器部署；为了跑 Triton
而在当前实例编译服务端会引入与研究问题无关的环境变量。Triton因此保留为有容器
能力环境中的生产上界，不阻塞首版方法验证。

**采用的官方/开源架构依据**：

- vLLM 官方 [Pooling Models](https://docs.vllm.ai/en/stable/models/pooling_models/)
  与 [vision embedding example](https://github.com/vllm-project/vllm/blob/main/examples/pooling/embed/vision_embedding_online.py)：
  CLIP 可用 `--runner pooling` 在线提供图像 embedding；官方同时说明 pooling 当前
  以功能便利为主，不保证优于直接 Transformers。
- Daft 官方 [embed_image](https://docs.daft.ai/en/stable/api/functions/embed_image/)：
  内置 AI Function 自行解析 provider 的 batch、concurrency、GPU 和 retry 配置；
  因而它是数据库输入轨道的 native baseline。`@daft.cls` CLIP 示例只能证明 API
  使用方式，项目重写后的 UDF 不再称官方 baseline。
- Daft 官方 [image-classification benchmark](https://github.com/Eventual-Inc/Daft/tree/3f5bdd175b7de3dcdf35765e1ba604b5c1cb8e15/benchmarking/ai/image_classification)：
  803,580 行（80,358 张 ImageNet 图重复 10 次）、ResNet18、batch 100；同目录同时
  提供 Daft 与 Ray Data 原始入口。正式复现保持两张执行图不变，只把 8 GPU worker
  改成当前物理 2 GPU，并外置采集指标。
- PolarDB 官方 [CPU/GPU 异构算子编排和调度](https://help.aliyun.com/en/polardb/polardb-for-postgresql/heterogeneous-operator-scheduling)：
  CPU decode/resize 与 GPU 类 UDF 可在同一 Daft-on-Ray pipeline 中按算子声明资源并
  流式重叠；因此不能用 fused `@daft.cls` 代表完整 Daft/PolarDB-style baseline。
- Ray 官方 [offline batch inference](https://docs.ray.io/en/latest/data/batch_inference.html)：
  重 CPU preprocess 与 GPU inference 应拆成两个 operation 以实现跨 batch overlap；
  这与 path-B 阶段划分一致，Ray Data 同时作为强 baseline。
- Triton 官方 [dynamic batching](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/batcher.html)
  与 [DALI backend](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/dali_backend/README.html)：
  支持成熟 batching、metrics 和 GPU 图像预处理。DALI 会改变预处理归属，只在独立
  upper-bound 臂开启。
- [Infinity](https://github.com/michaelfeil/infinity) 原生支持 CLIP 和 dynamic batching，
  可作快速服务 baseline，但不是主方法默认 engine。

**CLIP 无 KV cache / prefix** —— 所以 prefix-aware routing 这一支不适用图像；但 active-work / K_max / flush / queue-adaptive 全部适用。这也是有用的边界：说明哪些策略是模态无关的（flush/K_max），哪些是 LLM-only 的（prefix）。

---

## 3. 数据集

| 阶段 | 数据集 | 体量 | 体积 | 来源（公开） | AutoDL 磁盘 |
|---|---|---|---|---|---|
| Smoke（最小验证 §6） | **COCO 2017 val** | 5,000 图 | ~1 GB | `images.cocodataset.org/zips/val2017.zip` | ✅ 当前 7.4G 够 |
| 正式（小规模） | COCO 2017 train **精选子集** 或 ImageNet-1K 子集 | 10k–50k 图 | 3–8 GB | COCO train2017 / HF `imagenet-1k`（需 accept terms） | 需清盘或挂载数据盘 |
| 留出（held-out） | COCO 2017 test 或不重叠子集 | 5k–10k | ~1 GB | 同上 | ✅ |

**当前决定**：先下 COCO 2017 val（5K，smoke）。正式规模等 smoke 通过 §6 门禁 + 清盘后再定。ImageNet-1K 需 HF 协议同意，暂不用。

**PostgreSQL image 表设计**（写回 sink 也用 pgvector）：

```text
image_documents(workload_name TEXT, doc_id BIGINT, source_image_id TEXT,
                split TEXT, image BYTEA, image_bytes BIGINT, ...)
PRIMARY KEY (workload_name, doc_id)
image_embeddings(doc_id BIGINT, workload_name TEXT, model_revision TEXT,
                 processor_revision TEXT, normalized BOOL,
                 embedding vector(512), PRIMARY KEY (...))
```
- canonical DB→GPU 路径把 encoded image bytes 放入 PostgreSQL，才能真实计入 DB read；
  `source_uri` 路径只作“外部对象存储/共享文件系统”独立 baseline，不能与 BYTEA 轨道
  混跑。当前 COCO 5K profile 已使用 BYTEA。
- `doc_id` 不再由本次导入顺序生成；保留 COCO/source image ID、split 和 workload，
  以便 held-out、重导入和 recall@10 可复现。
- COCO train/val 的原始 ID 空间会重叠，因此身份必须是 `(workload_name, doc_id)`；
  禁止给某个 split 人工加偏移来维持错误的全局 `doc_id` 主键。source、correctness
  与未来 writeback/update 查询都必须携带 workload。
- embedding 维度：CLIP ViT-B/32 = 512；写回 `vector(512)` + HNSW 索引（deferred）。

---

## 4. Workload 参数化（对齐附录 B.4 的厂商共识）

按附录 B "按 (prep_cost, infer_cost) 参数化 workload" 的共识，图像 workload 的参数网格：

| 维度 | 取值 | 说明 |
|---|---|---|
| 解码分辨率 | 224×224 / 336×336 / 448×448 | CLIP 标准 224；升分辨率加 CPU decode 成本 |
| batch size（frame-budget） | 16 / 32 / 64 / 128 | 对齐 token-budget 离散档 |
| 并发（K） | 4 / 8 / 16 / 32 | 对齐现有 K_max 扫描 |
| 模型 | openai/clip-vit-base-patch32（512d, 默认）/ patch16 | 小模型本地易跑 |
| endpoint 数 | 1 / 2（双 4090 各起一个 CLIP service） | 对齐多 endpoint 拓扑 |

---

## 5. 指标（在项目标准指标集上加图像专属的）

**项目已有（复用，§7.5 + 附录 B.4）**：rows/s、images/s、operator JCT、first
output、可观测 batch P50/P95/P99、GPU util/能耗、**J/1K images**、
CPU-core-seconds/image、exactly-once 审计、control trace 和多 job Jain fairness。
只有具备逐图提交与完成时间、并显式冻结 image SLO 的实验才报告 SLO-compliant
images/s；不能把文本 request SLO/token goodput 直接改名后使用。

**图像 workload 新增的关键指标**：

| 指标 | 定义 | 为什么关键 |
|---|---|---|
| **CPU decode time / row**（mean/p95） | JPEG decode + resize + normalize 每图耗时 | 异构调度的第一维 |
| **GPU embed time / row**（mean/p95） | CLIP forward 每图耗时 | 异构调度的第二维 |
| **CPU/GPU 时间比** | decode_s / embed_s | **§6 门禁的核心判据**——>0.3 才算"重 CPU 准备"，异构调度有舞台 |
| **overlap 效率** | (decode_s + embed_s) / wall_s | 朴素 Daft overlap vs 项目调度的直接对比量 |
| **recall@10**（写回后） | pgvector HNSW top-10 vs brute-force ground-truth | 写回→检索产品化闭环质量证伪（附录 A.4 P1 缺口） |
| **index build time** | HNSW 构建墙钟 | pgvector 写回侧 baseline（厂商共识指标） |

**TTFT/TPOT 不适用** CLIP（非自回归生成）——这是诚实的边界，说明哪些 LLM 指标可迁移、哪些不可。

### 5.0 无法同规模长稳态时的三层证据

Daft built-in 在当前机器上受物化/object-store/spill 容量限制，无法和流式 Ray Data/
project 一起跑 60K×2 长稳态。正式材料不得用调小所有 arm 的方式掩盖 fast arm 只有
十几秒、尚未进入稳态的问题，改用三层互补证据：

1. **同规模兼容性门禁**：在 Daft 可完成的最大规模上跑三臂，比较 exactly-once、
   输出合同和描述性 images/s；fast arm 未满 60 秒时只标 gate/screening。
2. **长稳态两臂 formal**：Ray Data 与 project 在 60K unique×2、同 CPU/GPU、1+3
   交错下比较 JCT、images/s、first output 和单位资源成本。
3. **Daft 容量上界**：单列最大成功规模与更大规模的结构化 OutOfDisk/spill 失败，
   这是可扩展性结果，不把失败 arm 的缺失吞吐填成 0。

跨层联合表只比较已经归一化且每个 arm 独立达到平台的 `images/s`、images/J、
J/1K-images、CPU-core-seconds/image、images/CPU-core-second、GPU-seconds/image 和
host I/O bytes/image。绝对 JCT、总能耗、总 CPU 时间和 raw `first_output_s` 需要同规模
才能排名。`first_output_fraction_of_e2e` 可描述物化/流式结构，但仍是跨规模描述性信号。

### 5.1 AI_CLASSIFY 的质量与公开 benchmark 对齐

系统性能和模型质量必须分开报告；“同一实现 digest 一致”只能证明各执行 arm
算出了同一结果，不能替代分类质量。正式分类使用两条不混读的 workload：

| 轨道 | 数据/模型 | 直接对齐对象 | 质量指标 | 系统指标 |
|---|---|---|---|---|
| public-system parity | ImageNet-1K（先 val 50K，许可/空间允许再扩）+ ResNet18 | Ray Data/Daft 官方约 803,580 图 image-classification pipeline | top-1/top-5 accuracy、invalid/missing label | E2E/JCT、images/s、first output、repeat mean/std/CV、写出时间、stage/resource 指标 |
| database/generalization | COCO val + CLIP zero-shot multi-label | PolarDB `classify_image`/Snowflake image `AI_CLASSIFY` 的数据库算子形态 | mAP、micro/macro-F1、precision/recall@阈值、coverage、置信度校准 | 同上，并记录标签集合/描述/prompt 与每图类别数 |

COCO 不是单标签 ImageNet，不能对它报告误导性的 top-1/top-5；必须先导入 COCO
annotations 并冻结 label/prompt manifest。反过来，ImageNet/ResNet18 的单标签结果
也不能冒充 user-defined-category `AI_CLASSIFY` 的语义质量。

官方公开口径只作方法参考，不跨硬件比较 raw time：[PolarDB 2026 性能页](https://help.aliyun.com/zh/polardb/polardb-for-postgresql/daft-performance-benchmark)
在 8×24GB GPU 上报告约 803,580 图的总耗时，[Ray 2026 nightly](https://docs.ray.io/en/master/data/benchmark.html)
也使用约 800K ImageNet/ResNet18，
但两者公布的 Daft/Ray Data 排名方向相反。正式结论只来自本项目同硬件、同代码版本、
同数据和独立校准后的重跑。我们的结果至少比公开 headline 多报告 accuracy/quality、
尾延迟、CPU/GPU/内存/能耗、失败数和完整 timing boundary。

当前 COCO 表只有图像 bytes，尚无 annotations/captions；在 ground truth 导入前，
只允许报告 embedding 数值等价和系统门禁，不允许声称分类准确率或检索 recall。

---

## 6. 最小验证实验（fatal-flaw 门禁，必跑）

> ✅ **初始门禁结果（2026-08-01，GO）**：历史
> `CLIPProcessor(..., return_tensors="pt")` slow path 在实用 batch（≥16）的
> CPU 准备/GPU embed 比为 **13.8–18.3**；这证明值得建设 E2E runner，但不是方法
> 性能结论。子阶段补测只直接归因出 resize ~1.3ms，约 3.8ms 尚未细分；“GPU
> 空转 ~95%”已修正为由串行阶段时间推导的理论非-forward占比。当前代码改为
> `return_tensors="np" → ClipTensorActor`。该边界已在 `f3d17af` 上以四变体、6 个
> batch size、5+30 repeats 完成交错复测：tensor fast path 提升 1.14–1.22×，但
> CPU prepare 仍为 actor 的 13.8–31.2×；cosine=1/max_abs=0。详见
> `motivation/results/gpu/image_clip_preprocess_variants_20260801/`。

**目标**：在 all-in 搭完整 pipeline 前，用最小成本回答一个问题——**CPU decode 在我们的设置下是否真的足够重，让异构调度有真实变量？**

**步骤**（本地或 AutoDL 单 GPU，半天）：
1. 下 COCO val 5K + CLIP base（§3）。
2. 单进程跑：依次对 5K 图做 `PIL.decode + resize(224)` 和 `CLIP.encode`，分别计时。
3. 计算 decode_s/row、embed_s/row、比值。

**判据**：
- 比值 **> 0.3** → CPU 准备够重，异构调度有舞台，方案继续。
- 比值 **< 0.1** → CPU 准备太轻（CLIP forward 比 decode 慢太多），异构调度无变量，方案要重想（升分辨率、换更重的前处理、或换 VLM 生成式）。
- 中间（0.1–0.3）→ 边界，升分辨率或 batch 内交错 decode/embed 再测。

**这是 karpathy "先定义可验证目标、做最小实验" 的落地——用半天数据决定是否 all-in。**

**实现边界复测（✅ 已完成）**：使用
`code/scripts/profiling/profile_image_clip_preprocess_variants.py`，同一批图像内交错三条
processor 路径，保留 raw repeats；必须满足 embedding cosine gate，且不能故意保留
slow processor 制造策略空间。若生产/torchvision 路径令 CPU/GPU 比降到门禁以下，
应撤回“CPU preprocess 是主优化舞台”的外推，但仍保留历史 slow-path 结果。本次
fast path 未触发撤回条件，不过它只支持继续建设 E2E，不能证明调度收益。

---

## 7. 正式对照与晋级门禁

### 7.1 两层计时边界

图像 baseline 必须分两层，不能把 micro-profile 或只跑模型的数字称为数据库
端到端：

| 层 | 统一边界 | 回答的问题 | 当前脚本 |
|---|---|---|---|
| **operator E2E gate** | 每 query 的模型 worker 建立/执行开始 → 最后一批 embedding 返回；Ray 框架已启动，不含写回 | Daft Native/Ray 与拆阶段流水线谁更有效地执行同一 AI 算子？ | `code/scripts/experiments/run_image_clip_e2e.py` |
| **system E2E formal** | 同一 query/job 开始 → pgvector COPY 完成；索引延后统一构建 | 数据库作业实际多久完成，优化是否被写回抵消？ | operator gate 通过后接统一 sink |

operator gate 不是“只测 GPU”：它包含 DB read、JPEG decode、processor、CPU→GPU
transfer、CLIP forward、Daft/Ray 调度和结果 fan-in，只暂时排除 pgvector sink。三臂
必须使用同一张 `image_documents` 表、同一行集合、fast torchvision tensor processor、
模型 revision、dtype、batch size、两张 GPU 和 exactly-once/归一化审计。

### 7.2 第一轮最小矩阵（优先执行）

| 臂 | 唯一变化 | 角色 |
|---|---|---|
| `daft_builtin_embed` | Daft `decode_image` + 内置 `embed_image` | 正式 Daft native baseline；Daft 拥有隐藏 batching/backpressure |
| upstream `daft_main.py` | 官方 803,580-row ResNet18 脚本 | public parity 的 Daft vendor-code baseline |
| upstream `ray_data_main.py` | 同一官方目录中的 Ray Data 脚本 | public parity 的 Ray Data vendor-code baseline |
| `ray_data_staged` | Ray Data SQL + 两个官方 `map_batches` stage | database track native-API baseline；无项目 active-work/inflight |
| `project_ray` | Daft lazy source；Ray CPU preprocess actors 与 tensor-only GPU actors 有界重叠 | ours 当前主路径 |
| `daft_native/daft_ray/daft_staged` | 项目自写 Daft UDF | diagnostic only；formal 默认 fail closed |

执行顺序：先对 native arms 做 256 行 gate，再使用相同 PostgreSQL workload 进入正式
database track；官方 ResNet18 parity 在独立 ImageNet file/object track 运行。
每次 invocation 先执行 64 行 preflight/warmup，但 formal 必须新建模型 worker：Daft
每 query 天然重建 UDF actor，project-Ray 也显式销毁 warmup pool 后重建，避免生命周期
不对称。五臂按预注册随机块顺序交错，3 个 formal repeats。
headline 同时报告 operator E2E/JCT、images/s、first-output、GPU per-device util、
embedding checksum、最大 norm error 和 exactly-once，禁止只汇报吞吐。若 checksum/norm
或行集合不一致，性能结果无效。

指标执行以 `baseline_reference.md` 的“数据库 AI 算子评价指标合同”为最低合同。
当前 120K host-path matrix 仍是无 ground truth 的性能/瓶颈实验，因此必须明确写
`task_quality_not_evaluated`，不能用 embedding checksum/norm 声称分类准确率、检索
召回率或语义质量等价。进入正式 AI_CLASSIFY 时接入 ImageNet labels 并报告
top-1/top-5，或接入 COCO annotations 并报告 mAP、micro/macro-F1、precision/recall；
进入 AI_EMBED 检索时必须提供 queries/qrels 并报告 Recall@K、MRR/nDCG。正式 matrix
还必须结构化保存失败 run（success、error type、retry/timeout/OOM）以及 Ray
object-store peak/spill；system-E2E 排名需要所有 arm 接入相同 pgvector sink。

所有 Ray-backed staged graph 必须显式预留
`source reader slots + CPU preprocess actors（如有）+ GPU/model actor CPU slots`。2026-08-02
门禁复现了只分配 `4+2=6 CPU` 时 SQL reader 无 slot、0-row 永久等待的资源死锁；
runner 对 Ray Data native graph 和项目路径分别建立精确资源账本，并在
`ray.init` 前按 CPU affinity 拒绝物理超卖；cluster/host/declaration 分项写入 schema v8。
Ray `num_cpus` 只是准入资源，不是线程 quota；schema v8 同时冻结并记录每 worker
Torch intra-op/inter-op 线程，正式 matched-resource 默认 `1/1`。若 actor 实测线程
合同不一致则 fail closed，actor 数扫描与 per-actor thread 扫描分开报告。
正式 host-path 矩阵当前使用 schema v12：保留 v9 起分开的
`unique_images`、`dataset_passes` 与 processed `rows`，并增加输出合同、来源身份、
首输出结构信号和单位图片资源派生量。当前锁定 60K COCO train
唯一图、2 logical passes（120K processed rows）；pass-qualified execution ID 保留
exactly-once 门禁。重复 pass 只用于达到每 run 60 秒以上稳态，不得声称为更多唯一图。
Ray cluster 外的 Daft native source threads 单列为 external CPU，并计入 host 总预算。
source threads 与 preprocess actor 数分别配置，任何单因素扫描不得联动二者。
任何通过超卖或遗漏 source CPU 才能运行的点
不得进入正式 baseline；各 arm 的 Ray reservation 和 host 实测 CPU-core-seconds分开报告。

Daft SQL scan 在当前 PostgreSQL connector 下只有一个输入 partition，而 Daft 0.7.21
NativeRunner 的 `repartition/into_partitions` 均为 no-op。双 GPU baseline 因此在 source
端建立两个不重叠 PostgreSQL lazy shards，再 `daft.concat`；三臂统一使用同一 shards，
并从 per-device trace 确认两张卡均被激活。只声明 `max_concurrency=2` 但实际单卡执行
的结果无效。

Daft `@daft.cls` 支持 fractional GPU。one-actor-per-GPU 通过 gate 后仍须独立筛选
每 GPU 1/2/4 个 UDF actors（GPU share 1/0.5/0.25），让 combined preprocess+forward
baseline 也获得更多 CPU preprocessing concurrency。冻结最佳 Daft shape 后，ours
使用相同 PostgreSQL source-shard 数重跑；不能把未经 actor-shape 校准的 Daft 数字
作为 fused baseline。它不能替代 Daft-on-Ray staged pipeline 的独立 calibration。

**2026-08-01 历史 diagnostic 已完成**：项目自写的单卡 Daft Native UDF 最佳为 4 fused actors，双卡 Daft Ray
最佳为 4 fused actors；6/8 actors 已出现明显竞争退化。5000 图×3 formal 中，
project-Ray 相对最佳单卡 Native 为 1.296×，相对最佳双卡 Daft Ray 为 1.138×，
3/3 repeats 同向。该结果通过 operator gate，但仍缺 pgvector system-E2E、bounded
direct ceiling 和 CPU-budget-normalized curve；它不再作为 official/native baseline。报告：
`../../motivation/results/gpu/image_clip_native_baseline_20260801/README.md`。

### 7.3 完整对照臂

**对照臂**（对齐附录 B.4 + §7.5 干净合同；区分"直接 baseline"vs"Related Work"）：

| 臂 | 角色 |
|---|---|
| **bounded direct CLIP** | 物理上界（同协议绕过 Daft/Ray） |
| **Daft built-in `decode_image → embed_image`** | 正式 database-track native baseline；由 Daft provider 拥有调度 |
| **Daft 官方 ResNet18 script** | public file/object vendor-code baseline；固定 upstream commit |
| **项目自写 Daft fused/staged UDF** | 历史粗资源边界诊断，不进入官方主排名 |
| **PolarDB `classify_image` / Snowflake image `AI_CLASSIFY`** | 产品 SQL 语义参照；闭源服务不能在本机匹配硬件时只比较接口、质量口径和 timing boundary，不把厂商 raw time 与本项目排名 |
| **Ray Data 官方 ResNet18 script + native API graph** | 分别承担 public parity 与 PostgreSQL database track；均不注入项目 inflight/credit |
| **pgvector 直采 / 无组织串行** | 弱 baseline（仅诊断） |
| **ours：项目 scheduler（frame-budget + K_max + flush，观测 CLIP endpoint 队列）** | 主路径 |

> **直接 baseline**（必须跑 + 比数字，同杠杆=执行）：Daft built-in、官方
> ResNet18 parity、Ray Data native graph、naive、bounded direct；项目自写 Daft UDF
> 只作 diagnostic。产品 SQL 仅在能做到同硬件/同模型/同输入时进入数值排名。
> **Related Work**（只引用 + 定位，不比数字，不同杠杆=语义）：LOTUS / Palimpzest / Abacus（语义优化）、Cortex/Oracle（闭源）、Smart/GaussML（重写/实现）。
> baseline 的统一分层、准入和指标合同见 `baseline_reference.md`；图像方向的研究边界见
> `research/daft_db_gpu_bridge_direction_scope_20260731.md` §10.1。

**晋级门禁**（同项目 §7.5）：
1. 喂饱 GPU：bounded direct ≥ 95%（图像版 feeding 门禁；尚待补）。
2. ours 相对独立校准后的 **fused Daft Native/Ray**：operator-E2E images/s 已分别
   达到 +29.6%/+13.8%，但这只是 fused 对照；相对 Daft-on-Ray/Ray Data staged 的
   增量尚未知，system-E2E 也须在统一 pgvector sink 后重判。
3. stable：3 formal repeats CV 合理。
4. 质量不退化：embedding gate 用 digest/norm/检索 recall；ImageNet 分类用
   top-1/top-5；COCO multi-label 用 mAP 与 micro/macro-F1，禁止跨任务混用。

**只有 ours 在 matched-resource 和 independently calibrated 两种口径下，显著优于
Daft-on-Ray 或 Ray Data staged，同时再优于冻结最佳项目静态点，才能分别声称
“项目执行架构有增量”和“状态感知策略有增量”。** 只赢 fused Daft 不能声称优于
PolarDB Lakebase 异构流水线。

---

## 8. 范围缩减与触发条件

- **Smoke（§6）不过 → 不搭正式 pipeline**，方向重评（回 T2 大表 embedding 或 RAG）。
- AutoDL 磁盘 7.4G → smoke 用 COCO val 5K；正式前必须清盘或挂载数据盘。
- VLM 生成式（Qwen2.5-VL）标记 optional，不在首版。
- 多模态多 job 公平性为后续，首版单 job。

## 9. 待决与开放问题

1. **scoop 检索结果**（已启动工作流）——若发现已有"CLIP/图像 embedding 上游调度"论文，方案需调整定位。
2. **生产上界环境**——若后续获得可用 Docker/NGC 环境，再补 Triton
   PyTorch/ONNX/TensorRT；不得同时改变模型精度、processor 和 batching。
3. **正式数据集规模**——smoke 通过后定（COCO train 精选 / ImageNet 子集），受磁盘约束。
4. **是否同时上 T2 大表文本 embedding 作过渡**——可与图像并行，信号更早。

---

## 10. 下一步执行顺序

以下顺序取代早期“下载→smoke→build”清单。长时间本身不是目标；每一阶段只在前一
门禁通过后启动，避免用大矩阵掩盖语义或资源错误。

1. **当前代码 project static 复验（可立即运行）**：60K unique×2 logical passes，
   `8/16 CPU actors × active16/32`，固定 seed 交错 `1 warmup + 3 formal`，共 16 runs。
   每个 formal 的 steady-state proxy 必须≥60s、exactly-once、0 incident。该矩阵只冻结
   项目静态点，不与 Daft/Ray Data 横向排名。
2. **正式 baseline 公平性门禁**：Daft built-in 的 L2 normalization 必须在其 E2E
   计时边界内执行并显式记录 output contract；Ray Data/project 使用同一 normalized
   contract。逐行 parity、维度、finite、exactly-once 失败则停止。官方 ResNet18 另需
   upstream SHA256、adapter diff 和 ImageNet 数据许可/可用性门禁，不能拿 COCO 冒充。
3. **各 arm 独立 calibration**：Daft built-in 只扫公开 `batch_size`；Ray Data 扫官方
   batch/actor-pool 形状；project 使用步骤 1 的四点。每点至少 `1+2`，若候选与最优
   吞吐在 3% 内，选择更低 CPU/更简单的点；CV>10% 或 2 次重复方向冲突则补第 3 次，
   不按单次峰值选点。
4. **冻结后的长稳态 formal**：Daft built-in、Ray Data native、frozen project static
   和 bounded/direct ceiling 分别使用自己的 calibration 最优点；60K unique×至少
   2 passes，固定块顺序交错 `1+3`。同一结果表同时报告 E2E/JCT、images/s、first
   output、stage latency、CPU/GPU/显存/能耗、失败率和 embedding 质量；外部厂商 raw
   time 只作锚点，不与本机数字混排。
5. **system E2E 与方法实验**：只有 operator formal 有效后才接统一 pgvector sink，
   重跑 source→operator→writeback；再在 frozen project static 上做 frame/work budget、
   active-window、routing/feedback 和多 job 消融。动态策略只与 frozen static 比，不与
   弱默认或服务 ceiling 比。

预计步骤 1 约 0.5–1 小时；步骤 3 约 1–3 小时；步骤 4/5 各约 2–6 小时，具体以首个
完整 run 的实测时长外推。禁止为了“跑得久”增加无效 repeats；优先扩大 logical passes
使单 run 达到稳态，再保持 3 个交错正式重复。
