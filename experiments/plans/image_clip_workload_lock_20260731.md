# 图像 AI_EMBED (CLIP) Workload 锁定方案

日期：2026-07-31
状态：**🔴 首个 workload（2026-07-31 校正回升）**。学长反馈的核心判据：数据搬运瓶颈有两段——送 vLLM（拥挤）+ **DB 读出来 / CPU 搬到 GPU**（机会）；当前 prompt 文本每行 ~1KB、搬运太轻，瓶颈不显现。**图像 CLIP 每行 CPU→GPU 搬运 ~600KB（文本的 ~600×）+ JPEG decode+resize 重**，让 DB 读 + CPU→GPU 搬运瓶颈真正显现——这正是满足判据的首选 workload。**注意**：回升的理由是"让数据搬运瓶颈显现"，**与冷启动（机制，parked）无关**；CLIP 不绑死在冷启动旗舰上。详见 `research/daft_db_gpu_bridge_direction_scope_20260731.md` §10 + §10.1（benchmark 三层）。

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

**不 claim**：这个 workload 能保证项目策略赢。它只是让异构调度的问题真实存在；能否赢朴素 Daft overlap 要跑出来（见 §7 晋级门禁）。

---

## 2. 架构决策（CLIP 如何服务——2026-08-01 能力校正）

CLIP 是 embedding 模型，不是生成式 LLM，但当前 vLLM 已通过 pooling runner
正式支持 `CLIPModel` / `SiglipModel` 的图像 embedding。因此，旧前提“CLIP 不能
复用 vLLM、CLIP 服务没有 batching”已经失效。真正需要区分的是**预处理在哪一层**：

| 选项 | 输入和预处理边界 | 角色 | 采纳 |
|---|---|---|---|
| **Daft `@daft.cls(gpus=1)`** | Daft worker 内 decode/preprocess + CLIP forward | Daft Native 强 baseline；官方推荐的常驻 GPU UDF 形态 | 必跑 baseline |
| **vLLM pooling (`--runner pooling`)** | encoded image 进入服务，processor + pooling 在服务内部 | 与文本统一运维的成熟服务 baseline；官方说明 pooling 目前以功能便利为主，不保证优于 Transformers | 必跑服务 baseline；也是部署默认候选 |
| **常驻 Ray CLIP GPU actor** | Daft/Ray CPU worker 做 decode/resize/normalize，GPU actor 只收 typed tensor batch 并 forward | 直接复用现有 Ray actor pool/backpressure；保留“CPU 准备与 GPU 推理分离”的可归因主路径 | **ours 主路径** |
| Infinity / Ray Serve | encoded image 或服务内 preprocess，均自带 batching | 快速 smoke/补充 baseline；若使用必须冻结并记录隐藏 batching | 可选 |
| Triton | tensor-input、成熟 metrics/dynamic batching | 生产级上界；AutoDL 当前无 Docker，不能作为第一实现 | 容器环境 optional |

**决定**：主路径不是自写一个不受控的 FastAPI serving engine，而是通过统一
`ImageEmbeddingBackend` 接口接入**常驻 Ray GPU actor**。CPU worker 产出
preprocessed tensor，GPU actor 只执行 CLIP forward；项目已有 actor pool、credit、
backpressure 和 exactly-once 机械可以直接复用。vLLM pooling 作为统一部署默认候选
和强服务 baseline，Daft `@daft.cls` Native 仍是关键系统 baseline。

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
- Daft 官方 [Working with GPUs](https://docs.daft.ai/en/stable/custom-code/gpu/)：
  GPU UDF 使用 `@daft.cls` 常驻模型，且给出 CLIP image batch 示例；因此它是必须
  对照的原生实现，而不是本项目重复实现的模块。
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
image_documents(doc_id BIGINT PK, workload_name TEXT, source_image_id TEXT,
                split TEXT, image BYTEA, image_bytes BIGINT, ...)
image_embeddings(doc_id BIGINT, workload_name TEXT, model_revision TEXT,
                 processor_revision TEXT, normalized BOOL,
                 embedding vector(512), PRIMARY KEY (...))
```
- canonical DB→GPU 路径把 encoded image bytes 放入 PostgreSQL，才能真实计入 DB read；
  `source_uri` 路径只作“外部对象存储/共享文件系统”独立 baseline，不能与 BYTEA 轨道
  混跑。当前 COCO 5K profile 已使用 BYTEA。
- `doc_id` 不再由本次导入顺序生成；保留 COCO/source image ID、split 和 workload，
  以便 held-out、重导入和 recall@10 可复现。
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

**项目已有（复用，§7.5 + 附录 B.4）**：rows/s、images/s、request P50/P95/P99、SLO-goodput、GPU util/MFU、能耗 J/1k-tokens→改 **J/1k-images**、capacity_efficiency、exactly-once 审计、control trace、Jain fairness（多 job）。

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
`code/scripts/profile_image_clip_preprocess_variants.py`，同一批图像内交错三条
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
| **operator E2E gate** | 每 query 的模型 worker 建立/执行开始 → 最后一批 embedding 返回；Ray 框架已启动，不含写回 | Daft Native/Ray 与拆阶段流水线谁更有效地执行同一 AI 算子？ | `code/scripts/run_image_clip_e2e.py` |
| **system E2E formal** | 同一 query/job 开始 → pgvector COPY 完成；索引延后统一构建 | 数据库作业实际多久完成，优化是否被写回抵消？ | operator gate 通过后接统一 sink |

operator gate 不是“只测 GPU”：它包含 DB read、JPEG decode、processor、CPU→GPU
transfer、CLIP forward、Daft/Ray 调度和结果 fan-in，只暂时排除 pgvector sink。三臂
必须使用同一张 `image_documents` 表、同一行集合、fast torchvision tensor processor、
模型 revision、dtype、batch size、两张 GPU 和 exactly-once/归一化审计。

### 7.2 第一轮最小矩阵（优先执行）

| 臂 | 唯一变化 | 角色 |
|---|---|---|
| `daft_native` | `@daft.cls(gpus=1)` + Native runner，UDF 内完成 preprocess+forward | **关键强 baseline** |
| `daft_ray` | 同一 UDF，仅切换 Daft Ray runner | 执行器成本归因 |
| `project_ray` | Daft lazy source；Ray CPU preprocess actors 与 tensor-only GPU actors 有界重叠 | ours 当前主路径 |

执行顺序：先 256 行 gate，全部通过后使用 COCO val 5000 行，batch=64、2 GPU。
每次 invocation 先执行 64 行 preflight/warmup，但 formal 必须新建模型 worker：Daft
每 query 天然重建 UDF actor，project-Ray 也显式销毁 warmup pool 后重建，避免生命周期
不对称。三臂按 Latin-square 顺序交错，3 个 formal repeats。
headline 同时报告 operator E2E/JCT、images/s、first-output、GPU per-device util、
embedding checksum、最大 norm error 和 exactly-once，禁止只汇报吞吐。若 checksum/norm
或行集合不一致，性能结果无效。

Daft SQL scan 在当前 PostgreSQL connector 下可能只有一个输入 partition；双 GPU
baseline 必须显式 repartition 到 GPU worker 数，并从 per-device trace 确认两张卡
均被激活。只声明 `max_concurrency=2` 但实际单卡执行的结果无效。

### 7.3 完整对照臂

**对照臂**（对齐附录 B.4 + §7.5 干净合同；区分"直接 baseline"vs"Related Work"）：

| 臂 | 角色 |
|---|---|
| **bounded direct CLIP** | 物理上界（同协议绕过 Daft/Ray） |
| **Daft `@daft.cls` Native**（选项 A） | ⭐ 关键直接对照——PolarDB 式强 baseline（通用 overlap/backpressure） |
| **OceanBase AI_EMBED**（无 Daft/Ray，DB 原生算子） | 产品级核心 baseline——B1 门禁已过函数存在性（CE 4.5.0），当前 AutoDL 容器部署受阻、待可部署环境（见 `../results/oceanbase_b1_gate_20260731/`） |
| **Ray Data HTTP** | 框架归因 baseline |
| **pgvector 直采 / 无组织串行** | 弱 baseline（仅诊断） |
| **ours：项目 scheduler（frame-budget + K_max + flush，观测 CLIP endpoint 队列）** | 主路径 |

> **直接 baseline**（必须跑 + 比数字，同杠杆=执行）：Daft native / OceanBase / Ray Data / naive / bounded direct。
> **Related Work**（只引用 + 定位，不比数字，不同杠杆=语义）：LOTUS / Palimpzest / Abacus（语义优化）、Cortex/Oracle（闭源）、Smart/GaussML（重写/实现）。
> 完整矩阵见 `database_ai_operator_baseline_matrix_20260729.md` + `research/daft_db_gpu_bridge_direction_scope_20260731.md` §10.1。

**晋级门禁**（同项目 §7.5）：
1. 喂饱 GPU：bounded direct ≥ 95%（图像版 feeding 门禁）。
2. ours 相对 **Daft `@daft.cls` Native**（强 baseline，非串行 strawman）：images/s 或 SLO-goodput **>+5% 且 SLO 违约 <1%** 才晋级。
3. stable：3 formal repeats CV 合理。
4. recall@10 不退化（写回质量证伪）。

**只有 ours 显著优于 Daft Native**，才能声称"模型服务感知调度 > 通用 overlap"——这是项目相对 PolarDB Lakebase 的核心 claim。

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

1. ✅ 本方案文档（本文）。
2. 更新 `data/README.md` 加 COCO + CLIP 条目（exact URL + fetch 命令，遵循既有约定）。
3. AutoDL 下载 COCO val 5K + CLIP base（smoke 集，放得下 7.4G）。
4. 跑 §6 最小验证（CPU/GPU 时间比）——**这是 go/no-go 门禁**。
5. 门禁通过 → 写 CLIP embedding adapter + CPU decode pipeline（`code/src/`），复用 organizer/scheduler/tracing。
6. §7 正式对照（先 bounded direct + Daft Native + ours 三臂 smoke，再 formal）。
