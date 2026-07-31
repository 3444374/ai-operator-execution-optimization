# MS MARCO 批 embedding workload 设计 + 执行计划

日期：2026-07-31
状态：**⏸ 降级为"文本轻对照"（2026-07-31 校正）**。学长判据：数据搬运瓶颈要在"DB 读 + CPU→GPU 搬运"段显现，需要每行 payload 重。**MS MARCO 仍是文本——token ID 紧凑（~1KB/行），DB 读 + CPU→GPU 搬运太轻，瓶颈不显现**，不满足判据。因此**不作首选 workload**，仅保留作"文本下数据搬运瓶颈不显现"的**边界对照**（与图像 CLIP 对比，证明文本 regime 下问题不存在）。首选 workload 改回图像 CLIP，见 `image_clip_workload_lock_20260731.md` + `research/daft_db_gpu_bridge_direction_scope_20260731.md` §10。
关联：`research/daft_db_gpu_bridge_direction_scope_20260731.md`（方向 scope）；`notes/communication_notes.md` §5（学长反馈）；`code/INFRA_STATUS.md`（现有管线）。

> 本 workload 是学长原则"先锁被认可场景"的落地。**机制（冷启动/调度策略）后面再叠加**——先把战场选在 MS MARCO 这个被认可的 benchmark 上跑通，有正指标再扩。不改题目。

---

## 1. 为什么是 MS MARCO（目的与定位）

按"被认可 + fit 条件 + 大数据 + 异构 CPU/GPU + 复用管线 + 机制无关"六条筛（scope §10），MS MARCO Passage 全胜：

- **被认可**：MS MARCO leaderboard（ACL'17，4478+ 引）；**BigVectorBench（VLDB'25）的 text 切片**——benchmark 名字可直接引用。
- **fit 18G 盘**：passages 生成 ~5.7G，放得下（AutoDL 现空闲 18G）。
- **大数据 + 异构**：8.8M 段文本，**CPU tokenize（8.8M 段，重）vs GPU embed**——学长要的"大数据 + CPU 准备 → GPU 等"教科书拆分。无需冷启动 regime。
- **复用文本管线**：项目现有 Daft+Ray 文本管线直接接，effort 最低（ unlike 图像要从头搭 decode pipeline）。
- **机制无关**：exercise 痛点①（data-volume sizing）+ ③（流式 model-service-blind）；冷启动②解封后可平滑升级到 BigVectorBench 多模态切片。

**不 claim**：MS MARCO 能保证任何机制赢。它只是让"大数据 + 异构"问题真实存在；能否赢 baseline 要跑。

## 2. 数据集

| 阶段 | 切片 | 体量 | 用途 |
|---|---|---|---|
| Smoke / go-no-go | MS MARCO 随机 50K 段 | ~50 MB | §6 CPU/GPU 比 gate + 管线打通 |
| Formal | 1M → 8.8M 全量 | ~5.7 GB | 正式对照 |
| Held-out | 不重叠 100K 段 | ~100 MB | 留出验证 |

来源：HuggingFace `microsoft/msmarco-passage`（公开）。PostgreSQL 表 `msmarco_passages(id BIGINT PK, pid TEXT, passage TEXT)`，Daft source 读 passage 列。

## 3. 算子 + 链路

- **算子**：AI_EMBED——默认 **BGE-base-en-v1.5**（1024d，~440 MB，业界标准 embedding 模型）；备选 BGE-large-en-v1.5（重 GPU，~1.3 GB）。
- **写回**：pgvector `vector(1024)` + HNSW deferred index（COPY 后建）。
- **embedding endpoint**：BGE **不是 vLLM 生成式模型**，走独立 HTTP endpoint（FastAPI 跑 BGE）——复用项目现有 Ray→HTTP→endpoint→trace 机械（与 AI_EMBED 文本预研的 custom endpoint 同模式，`motivation/results/gpu/`）。`@daft.cls` Native 作 baseline 不作主路径。

链路：
```
PostgreSQL msmarco_passages
  → DaftPostgresSource（读 passage）
  → Ray worker CPU tokenize（8.8M 段，重——异构第一维）
  → organizer（token-budget，复用现有策略层）
  → scheduler（K_max / flush / credit，观测 BGE endpoint 队列）
  → Ray adapter → BGE embedding endpoint（GPU embed——异构第二维）
  → pgvector 写回 + HNSW
```

## 4. 指标

**项目标准（复用 §7.5）**：embeddings/s、rows/s、tokens/s（input）、request P50/P95/P99、SLO-goodput、GPU util/MFU、J/1k-emb、exactly-once 审计、control trace。

**本 workload 关键新增**：

| 指标 | 定义 | 为什么关键 |
|---|---|---|
| **CPU tokenize_s/row**（mean/p95） | 8.8M 段分词每段耗时 | 异构第一维 |
| **GPU embed_s/row**（mean/p95） | BGE forward 每段耗时 | 异构第二维 |
| **CPU/GPU 时间比** | tokenize_s / embed_s | **§6 go/no-go 核心判据**——>0.3 才算"重 CPU 准备" |
| **recall@10**（写回后） | pgvector HNSW top-10 vs brute-force ground-truth | ANN-benchmarks 协议，写回→检索质量证伪 |
| **HNSW build time** | 索引构建墙钟 | pgvector 写回侧 baseline（厂商共识指标） |

## 5. Baseline（三层，对齐 §7.5）

| 臂 | 角色 |
|---|---|
| **bounded direct BGE** | 物理上界（绕过 Daft/Ray 直连 BGE 打满） |
| **项目静态配置**（static K / fixed flush / 无 adaptive） | ✅ **主 bar**——动态策略 > 此 >5% 才晋级（与文本主线同 5% 门禁） |
| **Daft `@daft.cls` Native + Ray Data** | 框架归因（model-service-blind 通用 overlap） |
| **OceanBase AI_EMBED**（无 Daft/Ray，DB 原生算子） | 产品级核心 baseline（B1 门禁已过函数存在性，部署待可部署环境） |
| pgvector 直采 / 串行 | 诊断，不当正式 baseline（§6 禁 strawman） |

> **直接 baseline**（同杠杆=执行，跑+比数字）vs **Related Work**（不同杠杆=语义，LOTUS/Cortex/Smart/GaussML 等只引用+定位）的区分见 `research/daft_db_gpu_bridge_direction_scope_20260731.md` §10.1。MS MARCO 本身作文本轻对照。

**主胜判 = 项目动态 vs 项目静态**（不是 vs Daft Native）。Daft Native 仅作 novelty 对照。

## 6. Go/No-Go 门禁（all-in 前必跑）

最小实验（单 GPU，半天）：对 50K 段 MS MARCO，分别计时 CPU tokenize（BGE tokenizer）和 GPU embed（BGE forward），算比值。
- **>0.3** → CPU 准备够重，异构调度有舞台，all-in。
- **<0.1** → CPU 准备太轻（BGE tokenize 比 embed 慢太少），换 BGE-large 或加预处理再测；若仍不行，方向重评。
- 0.1–0.3 → 边界，加批量 tokenize / 升模型再测。

## 7. 晋级门禁（正式对照）

1. **feeding ≥95% bounded**（embedding 版 feeding 门禁）。
2. **项目动态 vs 项目静态**：embeddings/s 或 SLO-goodput **>+5% 且 SLO 违约 <1%** 才晋级。
3. **3 formal repeats** CV 合理。
4. **recall@10 不退化**（写回质量证伪）。

## 8. 执行计划（step-by-step，含 go/no-go）

| 步 | 动作 | 产出 / 门禁 |
|---|---|---|
| 1 | AutoDL 下 MS MARCO（HF ~5.7G）+ BGE-base（~440M） | 数据 + 模型到位 |
| 2 | **Go/No-Go §6**：50K 段 CPU tokenize vs GPU embed 计时 | 比值 >0.3 才继续；否则重评 |
| 3 | 建 BGE embedding endpoint（FastAPI）+ 接项目 Ray→HTTP adapter | endpoint 可用、队列可观测 |
| 4 | 导入 MS MARCO 进 PostgreSQL（passages 表） | 表就绪 |
| 5 | Smoke 50K：管线打通 + 指标采集验证 | 端到端跑通、CSV 字段齐 |
| 6 | Baseline：bounded direct + **项目静态**（主 bar） | 两臂有数 |
| 7 | Ours：动态策略（K_max/flush/credit，观测 endpoint 队列） | 动态臂有数 |
| 8 | Formal 3 repeats 全量（或 1M 子集先）：全指标 + recall@10 | 正式结果 |
| 9 | **决策点**：动态 > 静态 >5%？✅ 贡献验证 → 扩多模态；❌ 边界刻画 → 写 negative result |
| 10 | （后续）加 image CLIP 切片（BigVectorBench image）→ 模态无关验证 | 升级到多模态 |
| 11 | （远期）+ audio 切片 → 三模态 → 冷启动 regime（若解封） | BigVectorBench 全套 |

**依赖**：2 是 3-8 的前置门禁；6 是 7 的对照前置；9 是 10 的决策前置。

## 9. 范围 / 降级

- §6 go/no-go 不过 → 不搭正式 pipeline，方向重评（回图像 CLIP 或重选 workload）。
- 8.8M 全量太慢 → 先 1M 子集跑通再扩。
- 写回 pgvector 索引构建若占主导 → 报 HNSW build time，不掩饰（写回是 §7.5 工程 baseline 非研究贡献）。
- 多模态（image/audio）作后续，不进首版。

## 10. 与多模态升级路径的关系

MS MARCO = **BigVectorBench 的 text 切片**。本 workload 跑通后：
- 加 COCO/ImageNet 子集 + CLIP embed → BigVectorBench image 切片（验证 token-budget→frame-budget 模态无关）。
- 加 LibriSpeech + audio embed → BigVectorBench audio 切片。
- 三模态 = 三个异构 embed 模型 → 冷启动 regime（若那时解封痛点②）。

**benchmark 名字始终是 BigVectorBench**，从 text 切片起步逐步扩多模态——场景认可度一路保持，论文叙事顺。