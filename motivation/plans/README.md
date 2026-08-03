# Motivation Plans

本目录保存场景、路线和实验设计，不放原始 CSV 或正式结果。

| 文件 | 作用 |
|---|---|
| `integration.md` | PostgreSQL / 外部 worker / Ray / GPU model service / writeback 的集成路线 |
| `workloads.md` | `AI_EMBED`、`AI_FILTER/AI_CLASSIFY`、`AI_COMPLETE` 三类 workload 和动机测试 |
| `ai_sql_surface.md` | 数据库 AI 算子场景、业务动机和测试标准 |
| `image_host_data_path_bottleneck.md` | 图像链路 R0→R4 表示阶梯，判定 CPU/Ray/PCIe/GPU 哪一段限制 feeding |

修改这里时要同步检查 `motivation/README.md`、`PROJECT_INDEX.md` 和根 `README.md`。
