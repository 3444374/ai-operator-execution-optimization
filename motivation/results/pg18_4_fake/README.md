# PG18.4 Fake-Model Results

本目录保存 PostgreSQL 18.4 本地同构预演环境上的 fake-model 历史结果。

| 文件 | 作用 |
|---|---|
| `system_profile.md` / `.csv` | PG18.4 fake `AI_EMBED` 初始系统画像 |
| `baseline_matrix.md` / `.csv` | Python、Ray task、Ray actor、batch/worker baseline |
| `actor_batch_workers.csv` | Ray actor batch size × worker 数原始结果 |
| `vector_writeback.md` / `.csv` | JSON TEXT 与 pgvector vector 写回对照 |
| `pgvector_scaling.md` / `.csv` | pgvector 批量写回下 total_rows scaling |
| `pgvector_scaling_fine_contrast.csv` | 少量 fine 对照结果 |

这些结果不能写成 PostgreSQL 18.3 内部平台结论，也不能写成真实 GPU 模型服务结论。

