# 图像 AI_EMBED (CLIP) Workload 锁定方案

日期：2026-07-31
状态：**🔴 首个 workload（2026-07-31 校正回升）**。学长反馈的核心判据：数据搬运瓶颈有两段——送 vLLM（拥挤）+ **DB 读出来 / CPU 搬到 GPU**（机会）；当前 prompt 文本每行 ~1KB、搬运太轻，瓶颈不显现。**图像 CLIP 每行 CPU→GPU 搬运 ~600KB（文本的 ~600×）+ JPEG decode+resize 重**，让 DB 读 + CPU→GPU 搬运瓶颈真正显现——这正是满足判据的首选 workload。**注意**：回升的理由是"让数据搬运瓶颈显现"，**与冷启动（机制，parked）无关**；CLIP 不绑死在冷启动旗舰上。详见 `research/daft_db_gpu_bridge_direction_scope_20260731.md` §10 + §10.1（benchmark 三层）。

> ✅ **2026-08-01 更新**：方向已锁 A+B（见 `experiment_status_and_gaps.md` §0）；**§6 go/no-go 门禁已过（GO，ratio 13–17，CPU preprocess 主导）** → 下方「暂停 build」**已解除**，进入 path-B runner 建设期。首跑 1024 图 / 50 iters（"试试"量级），**5K COCO val + 加长 redo 待跑**。详见 `motivation/results/gpu/image_clip_bottleneck_profile_20260801.md`。

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

## 2. 架构决策（CLIP 如何服务——必须先定）

CLIP 是 **embedding 模型，不是 vLLM 服务的生成式 LLM**。当前项目 pipeline 终点是"vLLM-compatible endpoint"，CLIP 不能直接复用 vLLM。三个选项：

| 选项 | 机制 | 是否复用项目调度机械 | 是否对照 PolarDB 模式 | 采纳 |
|---|---|---|---|---|
| **A. `@daft.cls(gpus=1)` GPU UDF** | decode + CLIP 在同一 Daft GPU worker，Daft morsel/backpressure 做 overlap | ❌ 绕过项目 Ray adapter + endpoint 层 | ✅ 正是 PolarDB/Daft 模式 | 作 **baseline** |
| **B. 独立 CLIP embedding HTTP endpoint**（FastAPI/Infinity）+ 上游 CPU decode | 上游 Ray worker 做 CPU decode（重），organizer 做 frame-budget，项目 scheduler（K_max/flush/credit，观测 CLIP endpoint 队列）→ Ray adapter → CLIP endpoint | ✅ 完整复用项目机械，只换 adapter | 🟡 项目独有的"模型服务感知上游"形态 | **主路径（ours）** |
| C. TEI / Infinity 现成服务 | 同 B 但用现成 serving 框架 | ✅（同 B） | ❌ 非数据库场景 | 备选 baseline |

**决定**：主路径选 **B**，baseline 含 **A**。

**为什么 B 是 ours**：项目的核心 claim 是"调度策略模态无关 + 模型服务状态感知"。B 完整复用现有 Ray adapter → HTTP endpoint → trace 管线（最小新代码——加一个 CLIP embedding adapter），并把 CPU decode 放在上游 Ray worker，让"decode（CPU）vs embed（GPU）"的 overlap 由**项目 scheduler** 控制——这正是模型服务感知异构调度的舞台。A（`@daft.cls` Native）作为 PolarDB 式强 baseline：Daft 自己做 overlap/backpressure，项目必须证明"再观测 endpoint 状态做请求成形"能比 A 更优或在多 job/高压下更稳。

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
images(id BIGINT PK, path TEXT, split TEXT, label TEXT, ...)
image_embeddings(id BIGINT PK, embedding vector(512), model TEXT, ...)
```
- 上游不把图像字节塞进 PG（大 value 慢）；PG 存 **path**（指向 autodl-tmp 上的解压图），Daft source 读 path → Ray worker 按 path 加载+decode。
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

> ✅ **结果（2026-08-01，已通过 GO）**：5K COCO val × 100 iters 正式跑，ratio = CPU 准备/GPU embed 在实用 batch（≥16）**13.8–18.3**（B=256 渐近 ~18），远超 0.3 门禁；p95 紧贴 p50。瓶颈 = CLIPProcessor resize+normalize（~5.2 ms/img），非 decode/transfer/pg_read(0.755 bulk)；B=128 串行下 GPU 空转 ~95%。结论与 1024 首跑一致。详见 `motivation/results/gpu/image_clip_bottleneck_profile_20260801.md`（脚本 `code/scripts/profile_image_clip_bottleneck.py` + `import_coco_images.py`）。

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

---

## 7. 正式对照与晋级门禁

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
2. **CLIP endpoint 选型**——自写 FastAPI（最灵活，复用项目 adapter 模式）vs Infinity/TEI（省事但非数据库场景）。倾向自写 FastAPI 跑 CLIP，保持 endpoint 行为可控、可观测队列。
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
