# 图像模态（image/CLIP）推理服务引擎——部署与使用

> 本项目按**数据模态**分部署文档：文本（`text_serving.md`）、**图像（本篇）**，后续 video/audio 各起一篇。各模态共享同一套"调度策略模态无关"框架（见 `deploy/autodl/README.md` 总览），本篇只写图像独有部分。
> **共享平台 setup**（实例/venv/network_turbo/代码同步/模型下载方法/PG）在 `deploy/autodl/README.md` §1–§7，本篇不重复。
> 实验设计（测什么、go/no-go 门禁）在 `experiments/plans/image_clip_workload_lock_20260731.md`；本篇只讲"引擎是什么 + 在服务器上怎么部署/跑"。

## 1. 这个"模态"是什么

- **数据模态 = 图像**（不是文本）。workload 的每一行是**一张图**（JPEG 文件），不是一段 prompt 文本。
- **AI 算子 = `AI_EMBED`**：把每张图编码成一个**固定长度向量（embedding）**，写回 pgvector 做相似度检索。这**不是** `AI_COMPLETE`（文本生成）。
- **模型 = CLIP ViT-B/32**：OpenAI 的对比式视觉-语言模型。本项目中只用它的 **image encoder**——把一张图（224×224）压成一个 **512 维向量**。CLIP 也有 text encoder（图文匹配），本 workload 主要用 image 侧。

### 为什么从文本切到图像（核心动机）

文本 prompt 每行约 1KB，RC1 实测 `db_fetch` 1.4–2.4s、`model_wall` 27–37s，
说明该文本 regime 的主要墙钟在 vLLM serving，上游数据路径不是首要优化对象。

图像 CLIP 每行包含 JPEG bytes、CPU decode/resize 和约 **600KB** 的 FP32 pixel
tensor（文本 payload 的数量级更小）。这使 DB read、CPU preprocess、Ray/host copy、
PCIe H2D 和 GPU forward 的木桶效应变得可测，但**不预设其中哪一段是 binding
bottleneck**。当前只确认 CPU prepare 是候选限制；正式归因按 §5.5 的 R0→R4 实验。

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
- **正式集**：COCO 2017 train 的独立图像子集；host-path formal 至少 20K unique，
  并以实测查询阶段不少于 60 秒为第二道独立门槛。60K 单 pass 探针的最快配置
  `operator_e2e=40.53s`、扣除 worker setup 后约 `32.09s`，不满足门槛。因此正式
  矩阵预注册为 **60K unique × 2 logical passes = 120K processed rows**。CSV 必须
  分别记录 `unique_images=60000`、`dataset_passes=2` 和 `rows=120000`；不能把
  processed rows 误称为 120K unique images。
- **节省空间的导入**：train ZIP 约 19 GB。使用
  `import_coco_images.py --zip ... --limit 60000 --workload coco_train2017_60k`
  直接流式写入 PostgreSQL，不同时保留完整解压目录；运行前后检查数据盘与 WAL 空间。
- **行身份合同**：COCO train/val 的原始 image ID 会重叠，表主键必须是
  `(workload_name, doc_id)`。旧表若仍为全局 `PRIMARY KEY(doc_id)`，先执行：

  ```bash
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
    -f deploy/autodl/image_documents_workload_key.sql
  ```

  迁移脚本只接受预期的 legacy/current 主键并在事务内加锁；其他 schema fail closed。
  importer 会在读取/写入前复核主键，禁止用人为 ID offset 绕开冲突。
- **质量协议**：执行路径先以 exactly-once、完整 embedding digest、norm/finite 为
  等价门禁。ImageNet/ResNet18 分类才报告 top-1/top-5；COCO/CLIP 分类需先导入
  annotations，再报告 mAP、micro/macro-F1、precision/recall。只有定义检索任务和
  ground truth 后才报告 Recall@K、MRR/nDCG，不能把它们写成当前吞吐实验已有结果。

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
- `src.modalities.image.DaftImageSource` lazy source；
- `ClipImagePreprocessor`、typed `ImageEmbeddingBatch/Result`；
- `ClipTensorActor`：常驻 GPU、只接收预处理 tensor、输出 projected + L2-normalized embedding。

已新增 operator-E2E runner：`code/scripts/experiments/run_image_clip_e2e.py`，统一比较
fused Daft Native/Ray、Daft-on-Ray staged、Ray Data staged 与项目分阶段 Ray
pipeline。它从每个 query 的模型 worker
建立/执行开始计时到最后一批 embedding 返回（Ray 框架启动排除），包含模型 worker
建立、DB read/preprocess/transfer/forward/fan-in，
但**暂不含 pgvector 写回**。因此可作 operator baseline gate，不能冒充完整 system E2E。
统一 pgvector sink和正式 system-E2E runner仍待接入。schema v8 已补 system
per-core CPU、active-device GPU、逻辑字节和 project-Ray 分段 telemetry；其中
`--detailed-stage-timing` 会同步 CUDA，只能用于机制诊断，不能替代低扰动 headline。
GPU 采样还记录 active-card 功耗/时钟/估算能耗和 PCIe current/max link；
host 侧还记录内存、disk/network byte delta、context switch、CPU-core-seconds，
Ray Data 保存 operator stats；所有 Ray arm 记录 cluster 与 source/preprocess/model
声明 CPU。硬件 byte counter 仍不能用这些逻辑指标替代，PCIe 归因要走 Nsight/CUDA events。
`estimated_e2e_mfu` 仅在命令同时显式提供经校准的 `--model-flops-per-image` 与
`--gpu-peak-flops-per-s` 时生成，默认留空。

### 5.2 Ray GPU actor 合同 smoke（单卡）

确保此时没有 vLLM 或其他 GPU 实验占卡，然后运行：

```bash
cd /root/autodl-tmp/ai-operator
export IMAGE_MODEL_PATH=/root/autodl-tmp/models/clip-vit-base-patch32
PYTHONPATH=code /root/miniconda3/bin/python - <<'PY'
import os
from pathlib import Path
import ray
from src.modalities.image import ClipImagePreprocessor, ClipTensorActor, ImageEmbeddingBatch

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

### 5.4 Fused Daft Native / Daft Ray / project-Ray operator E2E gate

> **arm 分类（2026-08-03 校正：防 `--phase formal` 被拒）**——runner 现按 provenance 区分：
> - **vendor-native formal baseline**（`formal_baseline_eligible=True`，可直接 `--phase formal` 进排名）：`daft_builtin_embed`（Daft 内置 `decode_image→embed_image`）、`ray_data_staged`（Ray Data native `map_batches`）。
> - **项目自写 diagnostic reference**（`daft_native`/`daft_ray`/`daft_staged`，UDF 由项目编写）：`--phase formal` **默认被拒**，必须加 `--allow-non-native-diagnostic` 才能跑，且 `formal_baseline_eligible` 仍为 False、**不进 baseline 排名**（仅粗资源边界隔离 / 复现 2026-08-01 历史 fused 数字）。
>
> formal baseline 排名只用 vendor-native 两臂 + `project_ray`；diagnostic 三臂仅供 `--phase gate` 或显式加 flag 的 diagnostic formal。下方 daft_native/ray/staged 命令已补 flag，但它们的输出**不进入** baseline 排名。

各臂必须串行运行，且运行前确认没有 vLLM/其他 GPU 任务。不要用一个统一的
`--gpu-workers/--cpu-workers` 循环跑完三臂：Native 单卡和 Ray 双卡是两个独立
track，而且 baseline actor shape 必须先校准。当前 5000 图 formal 冻结配置为：

| track | arm | `cpu-workers` | `gpu-workers` | `daft-model-workers` | `source-shards` |
|---|---|---:|---:|---:|---:|
| 单卡 | `daft_native` | 4 | 1 | 4 | 4 |
| 单卡 | `project_ray` | 3 | 1 | — | 4 |
| 双卡 | `daft_ray` | 4 | 2 | 4 | 4 |
| 双卡 | `project_ray` | 4 | 2 | — | 6 |

先把下面命令的 `--limit` 改为 256、`--phase` 改为 gate、`--repeat-index` 改为
0 做正确性 gate；通过后按 baseline→project 交错运行 repeat 1/2/3：

```bash
cd /root/autodl-tmp/ai-operator
set -a; source /root/autodl-tmp/ai-operator-runtime.env; set +a
MODEL=/root/autodl-tmp/models/clip-vit-base-patch32
OUT=/root/autodl-tmp/experiment-artifacts/image_clip_native_baseline_formal_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT"

# 单卡 Daft Native；repeat 1/2/3 时分别修改 repeat-index 与 manifest 名。
PYTHONPATH=code /root/autodl-tmp/venvs/vllm-4090/bin/python \
  code/scripts/experiments/run_image_clip_e2e.py \
  --arm daft_native --model "$MODEL" --pg-dsn "$DATABASE_URL" \
  --limit 5000 --warmup-rows 64 --batch-size 64 \
  --cpu-workers 4 --gpu-workers 1 --daft-model-workers 4 --source-shards 4 \
  --max-active-batches 8 --allow-non-native-diagnostic --phase formal --repeat-index 1 \
  --out-csv "$OUT/runs.csv" --out-manifest "$OUT/native_1gpu_r1.json"

# 单卡 project-Ray。
PYTHONPATH=code /root/autodl-tmp/venvs/vllm-4090/bin/python \
  code/scripts/experiments/run_image_clip_e2e.py \
  --arm project_ray --model "$MODEL" --pg-dsn "$DATABASE_URL" \
  --limit 5000 --warmup-rows 64 --batch-size 64 \
  --cpu-workers 3 --gpu-workers 1 --source-shards 4 \
  --max-active-batches 8 --phase formal --repeat-index 1 \
  --out-csv "$OUT/runs.csv" --out-manifest "$OUT/project_1gpu_r1.json"

# 双卡 Daft Ray。
PYTHONPATH=code /root/autodl-tmp/venvs/vllm-4090/bin/python \
  code/scripts/experiments/run_image_clip_e2e.py \
  --arm daft_ray --model "$MODEL" --pg-dsn "$DATABASE_URL" \
  --limit 5000 --warmup-rows 64 --batch-size 64 \
  --cpu-workers 4 --gpu-workers 2 --daft-model-workers 4 --source-shards 4 \
  --max-active-batches 8 --allow-non-native-diagnostic --phase formal --repeat-index 1 \
  --out-csv "$OUT/runs.csv" --out-manifest "$OUT/daft_ray_2gpu_r1.json"

# 双卡 Daft-on-Ray staged（项目自写 diagnostic reference，非强 baseline；formal 需 --allow-non-native-diagnostic，不进排名）。
PYTHONPATH=code /root/autodl-tmp/venvs/vllm-4090/bin/python \
  code/scripts/experiments/run_image_clip_e2e.py \
  --arm daft_staged --model "$MODEL" --pg-dsn "$DATABASE_URL" \
  --limit 5000 --warmup-rows 64 --batch-size 64 \
  --cpu-workers 4 --gpu-workers 2 --daft-model-workers 2 --source-shards 4 \
  --max-active-batches 8 --allow-non-native-diagnostic --phase formal --repeat-index 1 \
  --out-csv "$OUT/runs.csv" --out-manifest "$OUT/daft_staged_2gpu_r1.json"

# 双卡 Ray Data staged 强 baseline。source-shards 至少覆盖 CPU actor pool，
# 且 runner 会另给 SQL readers 预留 CPU slots，不能手工减掉造成资源死锁。
PYTHONPATH=code /root/autodl-tmp/venvs/vllm-4090/bin/python \
  code/scripts/experiments/run_image_clip_e2e.py \
  --arm ray_data_staged --model "$MODEL" --pg-dsn "$DATABASE_URL" \
  --limit 5000 --warmup-rows 64 --batch-size 64 \
  --cpu-workers 4 --gpu-workers 2 --source-shards 4 \
  --max-active-batches 8 --phase formal --repeat-index 1 \
  --out-csv "$OUT/runs.csv" --out-manifest "$OUT/ray_data_staged_2gpu_r1.json"

# 双卡 project-Ray。
PYTHONPATH=code /root/autodl-tmp/venvs/vllm-4090/bin/python \
  code/scripts/experiments/run_image_clip_e2e.py \
  --arm project_ray --model "$MODEL" --pg-dsn "$DATABASE_URL" \
  --limit 5000 --warmup-rows 64 --batch-size 64 \
  --cpu-workers 4 --gpu-workers 2 --source-shards 6 \
  --max-active-batches 8 --phase formal --repeat-index 1 \
  --out-csv "$OUT/runs.csv" --out-manifest "$OUT/project_2gpu_r1.json"
```

通过条件：五臂各 256 行、`exactly_once=true`、embedding dimension=512、
`max_norm_error` 在 float32 归一化容差内；schema v8 还要求
`embedding_digest_xor_rounded5` 一致。没有残留 Ray/GPU 进程。旧 checksum 只覆盖
第一维求和，不能单独作为完整输出等价证据。
正式 5000 行用 3 repeats，并按文档预注册的随机块顺序交错，不能连续跑完
同一臂后直接比较，以免时间漂移成为混淆变量。
Daft 的 UDF actor 按 query 重建；脚本因此也会在 project-Ray warmup 后销毁并重建
模型 worker pool，同时记录 `worker_setup_s`，避免用持久 project actor 对比冷 Daft actor。

2026-08-01 已完成的 fused 正式结果和原始 manifest 在
`motivation/results/gpu/image_clip_native_baseline_20260801/`。headline 为单卡
project 1.296× Daft Native、双卡 project 1.138× Daft Ray。该结果不包含 pgvector，
也不是相同 CPU reservation 的资源效率证明；复述时必须同时带上报告中的限制。
Daft-on-Ray staged 与 Ray Data staged 已在 2026-08-02 完成 256-row 双卡 resource/
correctness gate，输出 digest 与 exactly-once 一致，两卡均激活；Ray Data stats 记录
4 preprocess + 4 predictor tasks。该规模仍只证明可运行，不能比较两臂吞吐；下一轮
分别校准 batch、source shards、actor pool 与 in-flight。紧凑报告见
`feasibility/results/image_staged_resource_gate_20260802/`。

**Ray 资源门禁**：固定 4 个 preprocess actor + 2 个 GPU actor 只给 6 CPU
会把 SQL read task 饿死，表现为 0 rows 永久等待。runner 现在对 Ray Data 使用
`source_shards + cpu_workers + gpu_workers`，对 Daft staged 使用
`source_shards + cpu_workers + model_workers`，对 fused Daft Ray 使用
`source_shards + model_workers`。程序在 `ray.init` 前用 CPU affinity 校验物理可用
slot，超出时 fail closed，不用虚拟 `num_cpus` 超卖。schema v8 同时记录 host 可用、
cluster 总量和 source/preprocess/model 分项；绕过 runner 时也必须遵守同一账本。

`num_cpus` 只是 Ray 准入 token，不是 OS CPU quota。schema v5 因此额外冻结并记录
每个 worker 的 Torch intra-op/inter-op 线程（runner 默认 `1/1`，也可显式传
`--torch-intraop-threads 1 --torch-interop-threads 1`）。project Ray worker 在正式
查询前校验实测线程值，不匹配则 fail closed。不要把 host 默认的
`OMP_NUM_THREADS=32` 或 Torch 32/64 线程解释成“4 CPU actor”的 matched-resource
结果；actor 数与每 actor 线程数必须分开扫描，并同时报告 host busy cores。
runner 会在所有 `ray.init` 调用中传入共享 `ray_runtime_env()`，自动传播项目
`PYTHONPATH` 与 OMP/MKL/OpenBLAS/NumExpr 单线程合同；不再要求操作者在交互式 shell
手工 export `PYTHONPATH`。绕过 runner 的自定义入口仍必须显式传同一 runtime env。
project Ray 的 Daft native source 在 Ray cluster 外运行；其 `num_threads` 现在作为
`declared_external_cpus` 加入 host 总预算。默认 4 preprocess + 2 GPU actor + 4 source
threads 因而是 Ray cluster=6、host declared total=10，不再误写成总共 6 CPU。
`--source-cpu-threads` 与 `--cpu-workers` 分离；扫描 preprocess actor 数时固定 source
threads，避免一次改变数据源和预处理两块木板。未显式设置时前者仅为兼容而跟随后者。
schema v8 另记录 driver 的 `source_next`、Arrow/Python materialize 与 Ray submit，
用于把剩余 framework gap 缩小到可验证候选；这些墙钟段仍不是数据库内部或 Ray
serialization 的硬件级 attribution。

### 5.5 待完成：host data path 瓶颈判定

旧 5K×100 串行画像只证明 CPU preprocess 时间明显大于一次 actor forward，不能
证明 CPU 已饱和，更不能证明或排除 PCIe、Ray object store 与 host copy。旧
`batch_service` 也不是纯 GPU service。因此“分阶段瓶颈画像已完成”的说法撤回。

正式判定按
`motivation/plans/image_host_data_path_bottleneck.md` 执行 R0→R4 表示阶梯：
GPU-resident compute ceiling → pinned H2D → pageable/Ray tensor → in-memory JPEG
preprocess → PostgreSQL/Daft operator E2E。先跑无 CUDA 同步的正式曲线，再只对
代表点启用 `--detailed-stage-timing` 并用 CUDA events/Nsight 短窗口复核。

R0/R1/R2 的第一层可复用诊断入口：

```bash
OUT=/root/autodl-tmp/experiment-artifacts/image_clip_transfer_ceiling_20260802
PY=/root/autodl-tmp/venvs/vllm-4090/bin/python
mkdir -p "$OUT"

CUDA_VISIBLE_DEVICES=0 "$PY" code/scripts/profiling/profile_clip_transfer_ceiling.py \
  --model /root/autodl-tmp/models/clip-vit-base-patch32 \
  --batch-sizes 16,64,256 --warmup 5 --repeats 30 --seed 20260802 \
  --out-csv "$OUT/raw.csv" --out-manifest "$OUT/manifest.json"
```

这里 R2 复现 pageable FP32 tensor 的 ownership copy + dtype/H2D 边界，但不包含
PostgreSQL、Daft iterator 或完整 Ray actor queue；这些增量由 R3/R4 和 operator-E2E
补齐。R0–R2 是 synthetic ceiling diagnostic，不参与系统 baseline headline 排名。

在该实验过门禁前，只能写“CPU prepare 是候选限制、阶段拆分有 E2E 收益”，不能写
“PCIe 是瓶颈”“CPU 已饱和”或“传输可忽略”。

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
  code/scripts/profiling/profile_image_clip_preprocess_variants.py \
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

### 5.7 60K unique × 2 passes project 静态点 formal

数据导入完成并验证 `count(*)=count(distinct doc_id)=60000` 后，用矩阵 runner 执行
8/16 preprocess actors × active16/32。每个 run 对 60K 唯一图执行两次，逻辑执行 ID
使用 `doc_id#pass=N` 保持 exactly-once 可审计；这只延长稳态，不增加唯一图数量。
runner 按固定 seed 对每个 formal repeat block
洗牌，保存 raw CSV、逐 run manifest/stdout/stderr 和外层 schedule；任一 formal 的
exactly-once、最少 unique 行数或查询阶段 60 秒门禁失败即停止：

```bash
OUT=/root/autodl-tmp/experiment-artifacts/image_project_static_60k_x2_20260802
PY=/root/autodl-tmp/venvs/vllm-4090/bin/python

"$PY" code/scripts/experiments/run_image_clip_matrix.py \
  --config deploy/autodl/image_project_static_formal.example.json \
  --image-runner code/scripts/experiments/run_image_clip_e2e.py \
  --python-executable "$PY" \
  --output-dir "$OUT"
```

若最快 formal 的 steady-state proxy 仍不足 60 秒，本轮 fail-closed，先扩大处理次数或
重新预注册时长口径；禁止删掉门禁继续把短作业写成正式稳态结果。正式报告须同时给出
unique images、logical passes 与 processed rows。项目静态点冻结后，
Daft fused、Daft staged、Ray Data staged 分别独立校准，再在相同物理 CPU/GPU 上限下
做同一 workload、同一随机块顺序的正式比较。

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
| scoop 边界 | prefix/state-aware 有强先验，Daft/PolarDB/Ray Data 已覆盖 staged overlap | 不预设“数据搬运空白”；先以 staged baseline 和 R0→R4 证据决定剩余增量 |

## 7. 关联文档
- 实验 design + go/no-go 门禁：`experiments/plans/image_clip_workload_lock_20260731.md`
- 方向 scope（DB↔GPU Daft bridge，提案）：`research/daft_db_gpu_bridge_direction_scope_20260731.md`
- 评估方法（recall@10、baseline 矩阵）：`research/evaluation_metrics_survey_20260731.md`
- 文本 track（vLLM）部署：`deploy/autodl/README.md` §8
- 共享平台 setup：`deploy/autodl/README.md` §1–§7
- 数据 fetch 总表：`data/README.md`
