# 多模态（image/CLIP）推理服务引擎——部署与使用

> 本项目有**两条推理引擎 track**：① 文本（vLLM 生成式 LLM）② 多模态图像（CLIP embedding）。本篇是 **②**。
> ① 见 `deploy/autodl/README.md` §8（vLLM endpoint）。两条 track 的**共享平台 setup**（实例/venv/network_turbo/代码同步/模型下载方法/PG）也在 `deploy/autodl/README.md` §1–§7，本篇不重复，只写图像/CLIP 独有部分。
> 实验设计（测什么、go/no-go 门禁）在 `experiments/plans/image_clip_workload_lock_20260731.md`；本篇只讲"引擎是什么 + 在服务器上怎么部署/跑"。

## 1. 这个"模态"是什么

- **数据模态 = 图像**（不是文本）。workload 的每一行是**一张图**（JPEG 文件），不是一段 prompt 文本。
- **AI 算子 = `AI_EMBED`**：把每张图编码成一个**固定长度向量（embedding）**，写回 pgvector 做相似度检索。这**不是** `AI_COMPLETE`（文本生成）。
- **模型 = CLIP ViT-B/32**：OpenAI 的对比式视觉-语言模型。本项目中只用它的 **image encoder**——把一张图（224×224）压成一个 **512 维向量**。CLIP 也有 text encoder（图文匹配），本 workload 主要用 image 侧。

### 为什么从文本切到图像（核心动机）

文本 prompt 每行 ~1KB，**CPU→GPU 搬运太轻**，"DB 读 + CPU 搬到 GPU"这段数据搬运瓶颈**根本显现不出来**。RC1 文本实测：`db_fetch` 1.4–2.4s vs `model_wall` 27–37s——binding 瓶颈在 vLLM serving，不在数据搬运。

图像 CLIP 每行 CPU→GPU 搬运 ~**600KB**（文本的 ~600×）+ JPEG decode+resize 重（每图毫秒级，常重于 GPU CLIP forward）。这让 **DB-read / CPU→GPU 数据搬运**变成 binding 瓶颈——这正是要找/优化的对象，也是 2026-08-01 锁定 image-CLIP-first 的原因（找数据搬运瓶颈）。

## 2. CLIP 引擎 vs vLLM 引擎（关键差异，务必先理解）

CLIP 是 **embedding 模型，不是 vLLM 服务的生成式 LLM**。两套引擎从模型类型到观测层都不同：

| 维度 | vLLM（文本 track，§8） | CLIP（图像 track，本篇） |
|---|---|---|
| 模型类型 | 生成式 LLM（Qwen2.5） | embedding 模型（CLIP image encoder） |
| 输出 | token 序列（可变长） | 定长向量（512d） |
| 服务机制 | continuous batching + APC（prefix cache）+ KV cache + paged attention | 批量 embedding（**无 KV cache、无 prefix、无生成**） |
| 关键观测 | `prefix_cache_hit_rate` / `kv_cache_usage` / `running·waiting` / TTFT / TBT-ITL | **CPU decode+resize 计时 / CPU→GPU transfer 计时 / GPU embed 计时 / endpoint 队列深度** |
| 部署 | `start_endpoints.sh`（§8） | 本篇 §5：FastAPI CLIP endpoint + 上游 Ray CPU decode |

**推论**：
- vLLM 的 **prefix-aware routing / prefix 分组对图像不适用**（CLIP 无 prefix 概念）——这正好是 prefix 轨暂停的理由之一（vLLM APC + Daft v0.6.9 已覆盖大半）。
- 但 **active-work / K_max / flush / queue-adaptive 这些调度策略模态无关**，全适用图像——这是项目"调度策略模态无关"claim 的验证点。

## 3. 环境配置（在 AutoDL 服务器上怎么配）

### 3.1 前提（共享平台，不重复）
先按 `deploy/autodl/README.md` §1–§7 完成共享部分：AutoDL 2×4090 实例、`venvs/vllm-4090`（已含 `torch 2.11+cu130` / `transformers 5.14` / `PIL`，**可直接跑 CLIP，不必建新 venv**）、`network_turbo`、代码 git 同步、PostgreSQL+pgvector。

### 3.2 CLIP 模型下载（⭐ 用 Python API，别用 huggingface-cli）
`huggingface_hub 1.x`（实测 1.25.1）的 `huggingface-cli download` **wrapper 解析参数失败、打印 help、下不到文件**。改用 Python `snapshot_download`：

```bash
source /etc/network_turbo 2>/dev/null                       # HF 必开加速
export HF_HUB_DISABLE_XET=1 HF_HUB_ENABLE_HF_TRANSFER=0     # 禁 Xet，否则 stall
/root/autodl-tmp/venvs/vllm-4090/bin/python - <<'PY'
from huggingface_hub import snapshot_download
p = snapshot_download("openai/clip-vit-base-patch32",
                      local_dir="/root/autodl-tmp/models/clip-vit-base-patch32")
print("DOWNLOADED_TO", p)
PY
```
结果 ~1.7G（vision+text 权重）。对照模型 `openai/clip-vit-base-patch16` 同法。

### 3.3 GPU 推理验证（⭐ transformers 5.x 返回类型坑）
transformers 5.x 的 `CLIPModel.get_image_features` 返回 **`BaseModelOutputWithPooling`**（不是旧版的裸 tensor），取 embedding 要 `.image_embeds`（或 `.pooler_output`），**不能直接 `.shape`**：

```bash
/root/autodl-tmp/venvs/vllm-4090/bin/python - <<'PY'
import torch, numpy as np
from PIL import Image
from transformers import CLIPModel, CLIPProcessor
MD="/root/autodl-tmp/models/clip-vit-base-patch32"
m = CLIPModel.from_pretrained(MD).to("cuda").eval()
proc = CLIPProcessor.from_pretrained(MD)
img = Image.fromarray((np.random.rand(224,224,3)*255).astype("uint8"))
inp = proc(images=img, return_tensors="pt").to("cuda")
with torch.no_grad():
    out = m.get_image_features(**inp)          # 5.x → BaseModelOutputWithPooling
emb = out if torch.is_tensor(out) else out.image_embeds
print("CLIP_GPU_OK", tuple(emb.shape), float(emb.norm()))   # 期望 (1, 512) ~10
PY
```
看到 `CLIP_GPU_OK (1, 512) <norm>` = CLIP 在 GPU 能 encode，环境就绪。

## 4. 数据集（哪来 / 怎么配）

### 4.1 COCO val2017（smoke 集，5000 图，首选）
cocodataset.org 直连，**不开 turbo**（turbo 只代理 github/HF）：

```bash
mkdir -p /root/autodl-tmp/data/raw/coco_val2017
wget -cq --tries=10 --timeout=30 \
  http://images.cocodataset.org/zips/val2017.zip \
  -O /root/autodl-tmp/data/raw/coco_val2017/val2017.zip      # ~780M
cd /root/autodl-tmp/data/raw/coco_val2017 && unzip -q val2017.zip   # → val2017/*.jpg
```

### 4.2 正式集 + 质量协议（smoke 通过后再定）
- **正式集**：COCO 2017 train 精选子集（10k–50k 图）或 ImageNet-1K 子集；前者直连，后者需 HF 协议同意。受磁盘约束（train2017 ~19G），需清盘或挂数据盘。
- **质量协议**：ANN-benchmarks `recall@10` 作为 embedding 质量门禁（不是吞吐 headline 指标，是正确性下限），见 `research/daft_db_gpu_bridge_direction_scope_20260731.md` §10.1。

### 4.3 数据怎么进 pipeline
图像 workload 的数据流（与文本对称，只换"行"的内容 + 计数函数）：
```
PostgreSQL 表（图像 bytea 或路径列）
  → Daft DataFrame 读出（数据引擎，df["image"] 列）
  → Ray worker CPU decode（JPEG decode + resize 224 + normalize）  ← 重 CPU 段
  → CPU→GPU transfer（每图 ~600KB）                                ← 搬运段
  → CLIP image encoder（GPU）                                       ← GPU 段
  → pgvector 写回（512d 向量）
```
**文本 pipeline 是 `df["prompt"]`，图像是 `df["image"]`——这是"模态无关、只换数据列"的设计点**。

## 5. 怎么操作（serving 引擎 + 跑实验）

### 5.1 serving 架构（plan "ours" 路径 B）
CLIP serving **不能复用 vLLM**。按 `image_clip_workload_lock_20260731.md` §2 的决定：

- **主路径（ours）B**：**CLIP embedding HTTP endpoint**（FastAPI 包 transformers CLIP）+ 上游 Ray worker 做 CPU decode（重），项目 scheduler（K_max / flush / credit，观测 CLIP endpoint 队列）→ Ray adapter → CLIP endpoint。
- **baseline A**：Daft `@daft.cls(gpus=1)` Native（decode + CLIP 在同一 Daft GPU worker，Daft morsel/backpressure 自己做 overlap）——PolarDB 式强 baseline。
- **claim 门槛**：只有 ours 显著优于 Daft Native，才能声称"模型服务感知调度 > 通用 overlap"（相对 PolarDB Lakebase 的核心 claim）。

### 5.2 第一刀：分阶段瓶颈画像（先测，不是先优化）
**第一步不是写优化策略，是先画像——确认瓶颈到底在哪段、有多重。** 把 §4.3 的数据流分阶段计时：
- DB-read（PostgreSQL 取图）
- CPU JPEG decode + resize + normalize
- CPU→GPU transfer
- GPU CLIP embed

这是"找数据搬运瓶颈"的直接验证，也是后续策略优化的基线。跑法类似文本侧的 GPU-backed E2E motivation profile（`motivation/results/gpu/`，fine vs coalesced 13.4× 那套），只是换模态 + 分阶段更细。

### 5.3 go/no-go 门禁
跑 §6 smoke（5000 图）→ 按 `image_clip_workload_lock` §6 的 **ratio>0.3** 门禁决定是否晋级正式规模。

## 6. 注意事项（坑汇总）

| 坑 | 表现 | 解法 |
|---|---|---|
| `huggingface-cli download` 在 hf_hub 1.x 坏 | 打印 help、0 文件 | 改 Python `snapshot_download`（§3.2） |
| transformers 5.x `get_image_features` 返回类型变 | `.shape` 报 AttributeError | 取 `.image_embeds`（§3.3） |
| COCO 走 turbo 反而慢/不通 | turbo 只代理 github/HF | COCO 直连 cocodataset.org，不 source turbo |
| CLIP 无 KV/prefix | vLLM 观测指标不适用 | 改采 CPU decode/transfer/embed 计时 + endpoint 队列深度（§2） |
| 磁盘 | smoke ~2.5G 够；正式集（COCO train/ImageNet）要清盘/挂数据盘 | smoke 先行，正式前清盘 |
| 代码同步 | 远端 git 常落后本地 main | 跑前同步（见 `deploy/autodl/README.md` §1 同步 gotcha） |
| scoop 边界 | prefix-aware 切片已被 SOLO(ICML'26)/Liu、llm-d/Preble、Daft v0.6.9 占 | 本项目走**数据搬运瓶颈切片**（un-scooped），避开 prefix-aware |

## 7. 关联文档
- 实验 design + go/no-go 门禁：`experiments/plans/image_clip_workload_lock_20260731.md`
- 方向 scope（DB↔GPU Daft bridge，提案）：`research/daft_db_gpu_bridge_direction_scope_20260731.md`
- 评估方法（recall@10、baseline 矩阵）：`research/evaluation_metrics_survey_20260731.md`
- 文本 track（vLLM）部署：`deploy/autodl/README.md` §8
- 共享平台 setup：`deploy/autodl/README.md` §1–§7
- 数据 fetch 总表：`data/README.md`
