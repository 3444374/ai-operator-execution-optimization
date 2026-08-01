# 图像模态（image/CLIP）推理服务引擎——部署与使用

> 本项目按**数据模态**分部署文档：文本（`text_serving.md`）、**图像（本篇）**，后续 video/audio 各起一篇。各模态共享同一套"调度策略模态无关"框架（见 `deploy/autodl/README.md` 总览），本篇只写图像独有部分。
> **共享平台 setup**（实例/venv/network_turbo/代码同步/模型下载方法/PG）在 `deploy/autodl/README.md` §1–§7，本篇不重复。
> 实验设计（测什么、go/no-go 门禁）在 `experiments/plans/image_clip_workload_lock_20260731.md`；本篇只讲"引擎是什么 + 在服务器上怎么部署/跑"。

## 1. 这个"模态"是什么

- **数据模态 = 图像**（不是文本）。workload 的每一行是**一张图**（JPEG 文件），不是一段 prompt 文本。
- **AI 算子 = `AI_EMBED`**：把每张图编码成一个**固定长度向量（embedding）**，写回 pgvector 做相似度检索。这**不是** `AI_COMPLETE`（文本生成）。
- **模型 = CLIP ViT-B/32**：OpenAI 的对比式视觉-语言模型。本项目中只用它的 **image encoder**——把一张图（224×224）压成一个 **512 维向量**。CLIP 也有 text encoder（图文匹配），本 workload 主要用 image 侧。

### 为什么从文本切到图像（核心动机）

文本 prompt 每行 ~1KB，**CPU→GPU 搬运太轻**，"DB 读 + CPU 搬到 GPU"这段数据搬运瓶颈**根本显现不出来**。RC1 文本实测：`db_fetch` 1.4–2.4s vs `model_wall` 27–37s——binding 瓶颈在 vLLM serving，不在数据搬运。

图像 CLIP 每行 CPU→GPU 搬运 ~**600KB**（文本的 ~600×）+ JPEG decode+resize 重（每图毫秒级，常重于 GPU CLIP forward）。这让 **DB-read / CPU→GPU 数据搬运**变成 binding 瓶颈——这正是要找/优化的对象，也是 2026-08-01 锁定 image-CLIP-first 的原因（找数据搬运瓶颈）。

## 2. 引擎角色（关键差异，务必先理解）

CLIP 是 **embedding 模型，不是 vLLM 服务的生成式 LLM**。两套引擎从模型类型到观测层都不同：

| 维度 | vLLM generation（文本） | Ray CLIP actor（ours） | vLLM pooling（图像 baseline） |
|---|---|---|
| 模型类型 | Qwen2.5 生成 | CLIP image encoder | CLIP pooling |
| 输入边界 | prompt | **预处理后的 pixel tensor** | encoded image/data URL |
| batching owner | vLLM | **项目 organizer/scheduler** | vLLM pooling server |
| 预处理位置 | 服务内 tokenizer | **Daft/Ray CPU worker** | vLLM 服务内部 |
| 关键观测 | KV/running/waiting/TTFT/TBT | preprocess、actor queue、GPU embed、overlap | 服务吞吐/延迟/queue |
| 角色 | 文本主平台 | **图像主方法** | 强服务 baseline |

**推论**：
- vLLM 的 **prefix-aware routing / prefix 分组对图像不适用**（CLIP 无 prefix 概念）——这正好是 prefix 轨暂停的理由之一（vLLM APC + Daft v0.6.9 已覆盖大半）。
- 但 **active-work / K_max / flush / queue-adaptive 这些调度策略模态无关**，全适用图像——这是项目"调度策略模态无关"claim 的验证点。

## 3. 环境配置（在 AutoDL 服务器上怎么配）

### 3.1 前提（共享平台，不重复）
先按 `deploy/autodl/README.md` §1–§7 完成共享部分：AutoDL 2×4090、driver
Python `/root/miniconda3/bin/python`、独立 vLLM venv、network turbo、代码同步和
PostgreSQL+pgvector。**主方法 Ray actor 使用 driver Python，不在 vLLM venv 中运行**；
这样 vLLM 的 torch 依赖不会污染 Daft/Ray driver。

driver 环境至少应满足：

```bash
/root/miniconda3/bin/python -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple \
  -r code/requirements.txt
/root/miniconda3/bin/python -c \
  "import ray,daft,torch,transformers,PIL,psycopg; print(ray.__version__, daft.__version__, torch.__version__, torch.cuda.device_count())"
```

### 3.2 CLIP 模型下载（⭐ 用 Python API，别用 huggingface-cli）
`huggingface_hub 1.x`（实测 1.25.1）的 `huggingface-cli download` **wrapper 解析参数失败、打印 help、下不到文件**。改用 Python `snapshot_download`：

```bash
source /etc/network_turbo 2>/dev/null                       # HF 必开加速
export HF_HUB_DISABLE_XET=1 HF_HUB_ENABLE_HF_TRANSFER=0     # 禁 Xet，否则 stall
/root/miniconda3/bin/python - <<'PY'
from huggingface_hub import snapshot_download
p = snapshot_download("openai/clip-vit-base-patch32",
                      local_dir="/root/autodl-tmp/models/clip-vit-base-patch32")
print("DOWNLOADED_TO", p)
PY
```
结果 ~1.7G（vision+text 权重）。对照模型 `openai/clip-vit-base-patch16` 同法。

### 3.3 GPU 推理验证（⭐ transformers 5.x 返回类型坑）
transformers 5.x 的 `CLIPModel.get_image_features` 返回 **`BaseModelOutputWithPooling`**（不是旧版的裸 tensor），取 512d embedding 要 **`.pooler_output`**（实测：`last_hidden_state` 是 (B,50,768) 的 patch tokens、`.pooler_output` 才是投影后的 (B,512)；`.image_embeds` 不存在），**不能直接 `.shape`**：

```bash
/root/miniconda3/bin/python - <<'PY'
import torch, numpy as np
from PIL import Image
from transformers import CLIPModel, CLIPProcessor
MD="/root/autodl-tmp/models/clip-vit-base-patch32"
m = CLIPModel.from_pretrained(MD).to("cuda").eval()
proc = CLIPProcessor.from_pretrained(MD)
img = Image.fromarray((np.random.rand(224,224,3)*255).astype("uint8"))
inp = proc(images=img, return_tensors="pt").to("cuda")
with torch.inference_mode():
    out = m.get_image_features(**inp)          # 5.x → BaseModelOutputWithPooling
emb = out if torch.is_tensor(out) else out.pooler_output
emb = emb / emb.norm(dim=-1, keepdim=True)
print("CLIP_GPU_OK", tuple(emb.shape), float(emb.norm()))   # 期望 (1, 512), norm~1
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
PostgreSQL image_documents.image（canonical BYTEA 轨道）
  → Daft lazy DataFrame（禁止 driver to_arrow/collect）
  → Ray worker CPU decode（JPEG decode + resize 224 + normalize）  ← 重 CPU 段
  → ImageEmbeddingBatch(preprocessed_tensor, work_units)             ← typed boundary
  → 常驻 ClipTensorActor（每 GPU 一个，forward-only）                ← GPU 段
  → pgvector 写回（512d 向量）
```
source URI/共享文件路径只能作独立 baseline，不能与 BYTEA 轨道混为同一实验。

## 5. 怎么操作（serving 引擎 + 跑实验）

### 5.1 当前实现状态

已实现：

- `BatchRequest.work_units/work_unit` 及 scheduler/least-work/Ray adapter 中性计量；
- `src.image.DaftImageSource` lazy source；
- `ClipImagePreprocessor`、typed `ImageEmbeddingBatch/Result`；
- `ClipTensorActor`：常驻 GPU、只接收预处理 tensor、输出 projected + L2-normalized embedding。

已新增 operator-E2E runner：`code/scripts/run_image_clip_e2e.py`，统一比较
Daft Native、Daft Ray 与项目分阶段 Ray pipeline。它从每个 query 的模型 worker
建立/执行开始计时到最后一批 embedding 返回（Ray 框架启动排除），包含模型 worker
建立、DB read/preprocess/transfer/forward/fan-in，
但**暂不含 pgvector 写回**。因此可作强 baseline gate，不能冒充完整 system E2E。
统一 pgvector sink、阶段 trace 和正式 system-E2E runner 仍待接入。

### 5.2 Ray GPU actor 合同 smoke（单卡）

确保此时没有 vLLM 或其他 GPU 实验占卡，然后运行：

```bash
cd /root/autodl-tmp/ai-operator
export IMAGE_MODEL_PATH=/root/autodl-tmp/models/clip-vit-base-patch32
PYTHONPATH=code /root/miniconda3/bin/python - <<'PY'
import os
from pathlib import Path
import ray
from src.image import ClipImagePreprocessor, ClipTensorActor, ImageEmbeddingBatch

model = os.environ["IMAGE_MODEL_PATH"]
image_path = next(Path("/root/autodl-tmp/data/raw/coco_val2017/val2017").glob("*.jpg"))
pixels = ClipImagePreprocessor(model).preprocess([image_path.read_bytes()])

ray.init(num_gpus=1)
RemoteActor = ray.remote(num_gpus=1, num_cpus=1)(ClipTensorActor)
actor = RemoteActor.remote(model, processor_revision=model, dtype="float16")
print(ray.get(actor.ready.remote()))
batch = ImageEmbeddingBatch(
    doc_ids=(image_path.stem,),
    payload=pixels,
    input_kind="preprocessed_tensor",
    work_units=224 * 224,
    work_unit="pixels",
)
result = ray.get(actor.embed.remote(batch))
print("IMAGE_ACTOR_OK", result.embeddings.shape, result.embeddings.dtype,
      float((result.embeddings[0] ** 2).sum()))
ray.shutdown()
PY
```

通过条件：`IMAGE_ACTOR_OK (1, 512) float32`，最后的平方和约为 1，且
`nvidia-smi` 中只出现一个 actor 占用一张 GPU。

### 5.3 vLLM pooling 强服务 baseline

当前 vLLM 已支持 CLIP 图像 embedding。它接收 encoded image、在服务内部预处理，
因此是不同阶段边界的 baseline，不能与 tensor actor 只按一个总吞吐数混读。使用
独立 vLLM venv，且与 Ray actor **串行运行**：

```bash
CUDA_VISIBLE_DEVICES=0 /root/autodl-tmp/venvs/vllm-4090/bin/python \
  -m vllm.entrypoints.openai.api_server \
  --model /root/autodl-tmp/models/clip-vit-base-patch32 \
  --served-model-name clip-vit-b32 --runner pooling \
  --host 127.0.0.1 --port 8100
```

请求格式以 vLLM 官方 `examples/pooling/embed/vision_embedding_online.py` 为准。
正式实验必须记录 vLLM 版本、runner、processor placement 和服务端 batching 参数。

### 5.4 Daft Native / Daft Ray / project-Ray operator E2E gate

三臂必须串行运行，且运行前确认没有 vLLM/其他 GPU 任务。示例 gate：

```bash
cd /root/autodl-tmp/ai-operator
set -a; source /root/autodl-tmp/ai-operator-runtime.env; set +a
MODEL=/root/autodl-tmp/models/clip-vit-base-patch32
OUT=/root/autodl-tmp/experiment-artifacts/image_clip_e2e_gate_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT"

for ARM in daft_native daft_ray project_ray; do
  PYTHONPATH=code /root/autodl-tmp/venvs/vllm-4090/bin/python \
    code/scripts/run_image_clip_e2e.py \
    --arm "$ARM" --model "$MODEL" --pg-dsn "$DATABASE_URL" \
    --limit 256 --warmup-rows 64 --batch-size 64 \
    --cpu-workers 4 --gpu-workers 2 --max-active-batches 8 \
    --phase gate --repeat-index 0 \
    --out-csv "$OUT/runs.csv" \
    --out-manifest "$OUT/${ARM}.manifest.json"
done
```

通过条件：三臂各 256 行、`exactly_once=true`、embedding dimension=512、
`max_norm_error` 在 float32 归一化容差内、checksum 一致；没有残留 Ray/GPU 进程。
正式 5000 行用 3 repeats，并按文档预注册的 Latin-square 顺序交错，不能连续跑完
同一臂后直接比较，以免时间漂移成为混淆变量。
Daft 的 UDF actor 按 query 重建；脚本因此也会在 project-Ray warmup 后销毁并重建
模型 worker pool，同时记录 `worker_setup_s`，避免用持久 project actor 对比冷 Daft actor。

### 5.5 已完成：分阶段瓶颈画像
**第一步不是写优化策略，是先画像——确认瓶颈到底在哪段、有多重。** 把 §4.3 的数据流分阶段计时：
- DB-read（PostgreSQL 取图）
- CPU JPEG decode + resize + normalize
- CPU→GPU transfer
- GPU CLIP embed

这是"找数据搬运瓶颈"的直接验证，也是后续策略优化的基线。跑法类似文本侧的 GPU-backed E2E motivation profile（`motivation/results/gpu/`，fine vs coalesced 13.4× 那套），只是换模态 + 分阶段更细。

5K×100 画像已经通过 `ratio>0.3` 门禁；不得重复跑画像代替端到端 runner gate。

### 5.6 当前实现边界的受控复测

旧画像使用 `CLIPProcessor(..., return_tensors="pt")`，当前实现使用
`ClipImagePreprocessor(..., return_tensors="np") → ClipTensorActor`。两者不能直接
视作同一条路径。同步本次代码后，在没有其他 GPU runner 时执行：

```bash
cd /root/autodl-tmp/ai-operator
set -a
source /root/autodl-tmp/ai-operator-runtime.env
set +a

# 兼容尚未补 IMAGE_MODEL_PATH 的旧 runtime env；正式运行前必须解析为存在的目录。
IMAGE_MODEL_PATH=${IMAGE_MODEL_PATH:-/root/autodl-tmp/models/clip-vit-base-patch32}
test -d "$IMAGE_MODEL_PATH"

RUN_ID=image_clip_preprocess_variants_$(date +%Y%m%d_%H%M%S)
OUT=/root/autodl-tmp/experiment-artifacts/$RUN_ID
mkdir -p "$OUT"

PYTHONPATH=code /root/miniconda3/bin/python \
  code/scripts/profile_image_clip_preprocess_variants.py \
  --model "$IMAGE_MODEL_PATH" \
  --pg-dsn "$DATABASE_URL" \
  --limit 5000 --batch-sizes 1,32,128 \
  --warmup 5 --repeats 30 --seed 20260801 \
  --out-csv "$OUT/raw_repeats.csv" \
  --out-manifest "$OUT/manifest.json" \
  >"$OUT/run.log" 2>&1
```

这是一项实现边界画像，不是正式方法实验。有效结果必须满足：

- `production_np`、`legacy_pt` 均完成；`torchvision_pil_pt` 明确隔离“只换
  processor backend”，`torchvision_tensor_pt` 同时使用 tensor decode 检验真正的
  fast path；torchvision backend 不可用时在 manifest
  记录 skip 原因，不能静默丢臂；
- 非 reference embedding 对 `production_np` 的逐行最小 cosine ≥0.999；
- raw CSV 保留每个 repeat，不只保留均值；同一批图像内 variant 顺序随机交错；
- CSV 含 processor/backend/output kind、torch/transformers、PG/pgvector 和 GPU；
- 若 torchvision 或 production-np 将 CPU/GPU 失衡降到原门禁以下，应撤回 slow
  processor 外推，不能故意保留慢实现制造优化空间。

2026-08-01 已按 `f3d17af` 完成 5000 图、6 batch sizes、四变体、5+30 repeats；
结果已同步到 `motivation/results/gpu/image_clip_preprocess_variants_20260801/`。
命令保留作复现入口，不应无条件重复消耗 GPU。

## 6. 注意事项（坑汇总）

| 坑 | 表现 | 解法 |
|---|---|---|
| `huggingface-cli download` 在 hf_hub 1.x 坏 | 打印 help、0 文件 | 改 Python `snapshot_download`（§3.2） |
| transformers 5.x `get_image_features` 返回类型变 | `.shape` 报 AttributeError | 取 `.pooler_output`（§3.3） |
| COCO 走 turbo 反而慢/不通 | turbo 只代理 github/HF | COCO 直连 cocodataset.org，不 source turbo |
| CLIP 无 KV/prefix | 文本 KV/TTFT 指标不适用 | 改采 preprocess/transfer/embed/actor queue/overlap（§2） |
| 在 AutoDL 强行上 Triton | 无 Docker，编译依赖大 | 首版用 Ray GPU actor；Triton只在容器环境补 upper bound |
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
