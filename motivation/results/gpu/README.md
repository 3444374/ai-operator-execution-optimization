# GPU-Backed E2E Results

本目录保存生产式 GPU-backed 端到端主动机结果，优先用于回答：

> 为什么数据库 AI 算子的外部执行链路值得优化？

## 当前结果

| 文件 | 含义 |
|---|---|
| `ai_embed_profile.md` / `.csv` | 第一组真实 GPU-backed embedding 端到端画像 |
| `ai_embed_chain_breakdown_20260712.md` / `.csv` | 真实 embedding 链路拆分结果：PostgreSQL fetch、Arrow/batch、operator wall、HTTP model request wall、fan-in、writeback |
| `multi_endpoint_ray_motivation_20260712.md` / `ai_embed_multi_endpoint_20260712.csv` | 两个本地 GPU endpoint 下的 Ray task/actor 初步动机测试 |
| `ai_embed_chain_breakdown_draft_20260712.csv` | 早期草稿结果，含计时字段修正前的行；不要用于正式分析 |
| `image_clip_bottleneck_profile_20260801.md` / `.csv` | **图像 CLIP AI_EMBED** 历史 slow-pt 分阶段画像：5000 图采样池 × 100 iters，CPU processor ~5.2ms/img vs GPU embed 0.29ms/img；只支持继续建设 E2E |
| `clip_preproc_stages_20260801.csv` | slow-pt processor method-wrapper 子阶段历史数据；resize ~1.3ms，旧 residual 为近似未归因时间，不能解释成具体转换步骤 |
| `image_clip_preprocess_variants_20260801/` | 当前 production-np、legacy-pt、torchvision+PIL/tensor-decode 四臂交错复测；720 raw repeats、质量门禁、七步报告。fast path 仍未消除 CPU/GPU 阶段失衡 |
| `image_clip_native_baseline_20260801/` | **图像 CLIP operator-E2E Daft UDF diagnostic**：先校准项目自写 fractional-GPU UDF shape，再做 5000 图×3；项目阶段拆分相对 reference 单卡 +29.6%、双卡 +13.8%。不属于官方/native baseline，正式排名待 Daft built-in、官方 ResNet18 parity 与 Ray Data native graph |
| `image_host_path_screening_20260802/` | 2×4090 图像 host-path 单因素 screening + schema v8 代表点诊断；报告、summary、各扫描 `runs.csv` 与逐臂 manifest 均已归档；当前判为 CPU preprocess 与 driver/Ray submit 混合木桶，PCIe 仅初步 NO-GO，尚非 formal |
| `image_clip_transfer_ceiling_20260802/` | 单卡 CLIP R0 GPU-resident、R1 pinned FP16、R2 pageable FP32，batch16/64/256×30 repeats；raw/summary/七步报告齐全。分离 compute/H2D/ownership ceiling，属于 synthetic diagnostic，不作系统排名 |
| `image_embedding_parity_20260803/` | Daft built-in `embed_image` 与项目 `project_ray` 的 256 图逐行语义门禁；L2-normalize 后 cosine P1=0.999788、overlap@10 mean=0.9949，支持统一 normalized contract；不是性能排名 |
| `daft_builtin_calibration_20260803/` | **Daft built-in `embed_image` batch 独立校准**（vendor-native，step 4 前半）：batch {16,32,64,128,256}×2 rep @5000 图双卡；平台点 batch=64≈177 img/s、GPU 平均利用率仅 1.2–4.1%（双卡均 claim）→ 原生 pipeline 喂入侧瓶颈、GPU 严重未打满；不排名，只独立校准 |

## Endpoint

当前 GPU-backed embedding endpoint：

```text
http://localhost:8000/v1/embeddings
```

多 endpoint 动机测试额外使用：

```text
http://localhost:8001/v1/embeddings
```

模型：

```text
sentence-transformers/all-MiniLM-L6-v2
```

复现实验前先检查 8000 端口；若未启动，按 `ai_embed_profile.md` 或 `ai_embed_chain_breakdown_20260712.md` 中的命令启动。

## 结果边界

- 本目录只放真实 GPU-backed 模型端点结果。
- CPU-only、fake-model、连接验证结果不能放在这里。
- 当前真实模型返回 384 维 embedding；`ai_embed_chain_breakdown_20260712` 使用 JSON text 写回，不是 384 维 pgvector 写回。
- 图像 CLIP operator-E2E 的当前正式入口是
  `image_clip_native_baseline_20260801/README.md`；它包含 PostgreSQL BYTEA 读取、
  preprocess、transfer、forward 和 fan-in，但尚不包含 pgvector sink。
- 木桶机制的最新诊断入口是 `image_host_path_screening_20260802/README.md`；其中
  1-run 曲线只能选择 formal 候选，不能替代三重复 baseline 排名。
- H2D/compute ceiling 的最新入口是 `image_clip_transfer_ceiling_20260802/README.md`；
  它不包含数据库、Daft 或完整 Ray queue，只用于解释传输机制与选择 E2E 消融。
- `model_service_s` 是请求耗时加和；阶段占比优先看 `model_request_wall_s`、`operator_wall_s` 和 `writeback_s`。
- `multi_endpoint_ray_motivation_20260712` 是 Ray 价值的初步动机测试，不是最终 Ray Serve / vLLM / 多 GPU 结论。

## 2026-07-14 pgai-integrated rerun

```text
pgai_integrated_key_rerun_20260714.md
ai_embed_pgai_integrated_key_20260714.csv
```

This rerun uses PostgreSQL 18.4 local rehearsal plus GPU-backed local embedding
endpoints on ports 8000 and 8001. It covers batch granularity, full-chain
writeback timing, single versus dual local endpoints, and 4096-to-8192 row
scaling after the pgai SQL trigger surface was validated.

Boundary: the pgai SQL trigger surface itself remains a feasibility result; the
GPU rerun uses the job-table profile path so the timing boundaries are visible.
The writeback mode is JSON text for 384-dim embeddings, not pgvector vector(384).

## 2026-07-14 pgvector(384) writeback comparison

```text
pgvector_writeback_20260714.md
ai_embed_pgvector_writeback_20260714.csv
```

This test compares no writeback, JSON text writeback, and pgvector
`vector(384)` writeback in the same PostgreSQL 18.4 local GPU-backed Ray actor
chain. It verifies that `document_embeddings.embedding_vector` is `vector(384)`
and that 4096 written vectors have `vector_dims = 384`.
