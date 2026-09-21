# Daft built-in 与项目 CLIP embedding 语义门禁（2026-08-03）

> 性质：256 张 COCO 图的逐行正确性与比较边界诊断。它决定两个执行臂能否按统一
> AI_EMBED 语义比较，不是吞吐 baseline，也不产生正式性能排名。

## 1. 实验设置

- 服务器：AutoDL，2×RTX 4090；本门禁每个 arm 只使用 1 张 GPU。
- 数据：PostgreSQL `coco_val2017` 的同一组 256 个 `doc_id`，batch=64。
- 模型：同一本地 CLIP ViT-B/32 revision。
- 软件：Daft 0.7.21、Transformers 5.14.1、Ray 2.56.1、PyTorch
  2.12.1+cu130；代码 commit `6092b84`。
- Arm A：Daft 官方 `embed_image` AI Function；provenance 为
  `vendor_builtin_ai_function`，scheduler owner 为 Daft，无项目调度代码。
- Arm B：项目 `project_ray`；项目 Ray actor、float16、输出 L2 normalize。

逐行 capture 只在 `phase=gate`、单 pass、最多 4096 行时开放。它会额外复制并保留
embedding，因此 sidecar 固定记录 `timing_valid_for_performance=false`；默认执行路径不
创建 capture。

## 2. 实验设计

两臂各执行一次相同输入并保存 `doc_ids + embeddings`，随后离线完成：

1. `doc_id` 集合、重复/遗漏、维度和 finite 检查；
2. 比较原始 L2 norm；
3. 分别 L2 normalize 后，按同一 `doc_id` 比较逐行 cosine 与 max-abs；
4. 在各自 embedding 空间计算非自身近邻，比较 Recall-style overlap@1/5/10。

预注册成功条件为：post-normalization cosine P1≥0.999、最小值≥0.99，且平均
overlap@10≥0.90。满足时只判为“尺度/归一化差异”；不凭 checksum 判语义。

复现入口：

```bash
python code/scripts/experiments/run_image_clip_e2e.py \
  --arm daft_builtin_embed --model "$IMAGE_MODEL_PATH" \
  --pg-dsn "$DATABASE_URL" --workload-name coco_val2017 \
  --limit 256 --warmup-rows 64 --batch-size 64 \
  --cpu-workers 3 --gpu-workers 1 --source-shards 4 \
  --max-active-batches 8 --phase gate --repeat-index 0 \
  --save-embeddings /path/daft_builtin.npz \
  --out-csv /path/runs.csv --out-manifest /path/daft_builtin.json

python code/scripts/experiments/run_image_clip_e2e.py \
  --arm project_ray --model "$IMAGE_MODEL_PATH" \
  --pg-dsn "$DATABASE_URL" --workload-name coco_val2017 \
  --limit 256 --warmup-rows 64 --batch-size 64 \
  --cpu-workers 3 --gpu-workers 1 --source-shards 4 \
  --max-active-batches 8 --phase gate --repeat-index 0 \
  --save-embeddings /path/project_ray.npz \
  --out-csv /path/runs.csv --out-manifest /path/project_ray.json

python code/scripts/profiling/probe_embedding_parity.py \
  --arm-a /path/daft_builtin.npz --arm-b /path/project_ray.npz \
  --label-a daft_builtin_embed --label-b project_ray \
  --out-dir /path/parity
```

## 3. 严谨性自检

- 两臂均为 256/256、512 维、finite、`exactly_once=true`，共同 `doc_id=256`，重复为 0。
- `.npz` 的 `doc_ids` 使用 Unicode dtype，可由 `allow_pickle=False` 加载。
- 近邻 overlap 排除查询样本自身，避免 self-match 把结果虚高。
- 报告 cosine 的 min/P1/P50/P99/mean，不用单个均值掩盖尾部。
- 两个不带 `--save-embeddings` 的默认 gate 也分别通过；结束后无 Ray/vLLM/GPU
  残留进程。
- capture arm 的计时受诊断复制影响；默认 gate 也只有 256 行且冷启动主导，两者都不
  用于性能排序。

## 4. 实验数据

完整派生字段见 [`summary.csv`](summary.csv)。

| 指标 | Daft built-in | project_ray / 两臂比较 |
|---|---:|---:|
| raw norm P50 | 10.4718 | 1.0000 |
| post-norm cosine min | — | 0.999716 |
| post-norm cosine P1 / P50 / P99 | — | 0.999788 / 0.999985 / 0.999997 |
| post-norm cosine mean | — | 0.999975 |
| non-self overlap@1 mean | — | 0.9883 |
| non-self overlap@5 mean | — | 0.9945 |
| non-self overlap@10 mean | — | 0.9949 |

远端原始 artifact 保留于：
`/root/autodl-tmp/experiment-artifacts/image_embedding_parity_gate_20260803_6092b84/`，
包括两份 `.npz`、sidecar、逐行 CSV、runner CSV 和默认路径 gate manifest。Git 只保存
小型派生摘要，避免把诊断矩阵误作正式数据集。

## 5. 结果解释

### 事实

1. Daft built-in 返回未单位化的 raw embedding，项目返回单位 norm embedding。
2. 离线单位化后，逐行 cosine 和非自身近邻集合均超过预注册门槛；判定为
   `SCALE_NORMALIZATION_ONLY`。
3. Daft built-in arm 的 provenance 符合 framework-native baseline；`project_ray` 是项目
   method，不属于 baseline。

### 推断

- 对这组数据和版本，两臂的主要语义差异是输出归一化，而不是不同的图像语义表示。
- 因此正式 AI_EMBED 对比可以采用统一 L2-normalized contract；但必须让每个系统在其
  计时边界内完成归一化，不能只给项目计成本、给 Daft 免费离线处理。

### 待确认

- 256 图只覆盖小门禁；正式规模仍需抽样复核 cosine/近邻指标。
- provider-default 与项目 float16 在更换模型、processor 或 dtype 后是否仍满足门槛，需要
  版本变化时重新跑 parity。

### 不能声称

- 不能由本门禁声称项目比 Daft 更快，或 Daft 比项目更快。
- 不能把 capture/default gate 的约 10–12 秒冷启动耗时写成稳态吞吐。
- 不能把 embedding 近邻一致性写成 AI_CLASSIFY accuracy、COCO mAP 或真实检索
  Recall@K；当前门禁没有任务标签/查询相关性 ground truth。

## 6. 对课题的含义

本结果关闭了图像 framework-native baseline 的一个关键可比性缺口：Daft built-in 可作为
原生执行对照，但正式排名必须明确区分“vendor raw output”和“统一 L2-normalized
AI_EMBED contract”。它验证的是输出合同，不是项目调度策略的收益。

## 7. 下一步

1. 在正式 Daft built-in、Ray Data native 与项目矩阵中把归一化放入各自 E2E 边界；
2. 保留 vendor-raw 辅助表，同时只在统一 contract 表中横向排名；
3. 用≥20K unique images、≥60 秒稳态、交错 1 warmup+3 repeats 做正式性能实验；
4. AI_CLASSIFY 另用带标签数据报告 top-1/top-5 或 mAP/F1，不能复用本 parity 指标代替质量。

## 8. 计时内 normalized output contract 落地门禁（2026-08-03）

commit `6f0954b` 上重新执行同一 256 图 capture gate。两臂均显式传入
`--embedding-output-contract l2_normalized`；Daft 保持官方
`decode_image→embed_image` 执行图，仅由 baseline adapter 在消费官方输出后、共同 audit
前执行 CPU L2 normalization，并计入该 arm operator E2E。项目仍由 model actor 归一化。

| 指标 | Daft built-in normalized | project_ray normalized |
|---|---:|---:|
| output rows / exactly-once | 256 / true | 256 / true |
| max norm error | 1.1921e-7 | 1.1921e-7 |
| effective contract | l2_normalized | l2_normalized |
| normalization owner | baseline_adapter | model_actor |
| normalization in timed boundary | true | true |

逐行比较结果：cosine min=0.999727、P1=0.999800、P50=0.999985、P99=0.999997；
non-self overlap@10 mean=0.9949。它再次超过预注册语义门槛，且现在不再依赖“正式运行后
离线免费归一化”。原始 runner CSV、两臂 manifest 和 probe summary 保存在
[`raw/normalized_contract_gate/`](raw/normalized_contract_gate/)。

仓库同时保存两份约 0.5MB 的 `.npz`、其 capture sidecar 和逐行比较 CSV；用 §2 的 probe
重新计算后，生成的 `summary.csv` 与 `per_row.csv` 与服务器 artifact 完全一致。须注意：
commit `6f0954b` 生成的两个 capture sidecar 中 `note` 仍是旧的通用文案，未反映本次已启用
计时内归一化；本次合同事实以 schema v11 runner CSV 和 arm manifest 为准。生成代码已在
本次结果归档时修正，后续 sidecar 会直接记录 requested/effective contract、normalization
owner 和 timed-boundary 标志；历史 raw sidecar 保持原样，不回写伪造。

本 gate 使用 capture，计时仍不进入性能排名；其结论仅为 schema v11 输出合同实现和
语义一致性通过。正式性能必须使用无 capture 的长稳态路径。
