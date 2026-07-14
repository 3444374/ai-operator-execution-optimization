# Feasibility Results

本目录保存可行性 benchmark、环境验证和连接验证结果。

规则：

- 系统组件 microbenchmark 放在这里。
- PG18.4 连接/环境验证放在这里。
- GPU/CUDA/模型服务 smoke test 可以放在这里，但只能证明环境或服务可用。
- 数据库 AI 算子端到端动机测试、系统画像、瓶颈定位和可优化点分析放在 `motivation/results/`。
- GPU-backed E2E profile 不放这里，放 `motivation/results/`。

## 阅读顺序

本目录按“是否能让后续实验继续跑”来读：

1. `pg18_4_connection_validation.md`：确认本地 PostgreSQL 18.4 + pgvector 链路可用。
2. `pgai_sql_smoke_20260714.md`：确认 pgai SQL embedding 触发面和 pgvector 写回可用。
3. `pg18_4_connection_smoke_*.csv`、`pg18_4_script_dryrun.csv`：确认脚本和小规模 smoke run 可用。
4. `ray_*`、`arrow_serialization.csv`、`shuffle_simulation.csv`：组件级 benchmark，只用于判断是否存在可观测系统信号。
5. `feasibility_report.md`、`current_direction_analysis.md`：历史阶段分析，读作背景，不作为当前 GPU-backed 主链路结论。

如果后续新增 GPU 环境验证，建议命名为：

```text
gpu_model_service_smoke.md
gpu_model_service_smoke.csv
```

这些文件只能说明 GPU 模型服务能跑通；端到端瓶颈和优化结论仍应进入 `motivation/results/`。

## 文件

| 文件 | 内容 |
|---|---|
| `ray_small_task.csv` | Ray small task 实验结果 |
| `ray_object_transfer.csv` | Ray object transfer 实验结果 |
| `arrow_serialization.csv` | Arrow RecordBatch serialization 结果 |
| `shuffle_simulation.csv` | 本地 shuffle simulation 结果 |
| `ray_many_objects.csv` | Ray many-object fan-in 结果 |
| `ray_arrow_fanout_fanin.csv` | Arrow RecordBatch fan-out/fan-in 结果 |
| `pg18_4_connection_validation.md` | PG18.4 本地连接与环境验证报告 |
| `pgai_sql_smoke_20260714.md` | pgai SQL embedding 触发面与 pgvector 写回冒烟验证 |
| `pg18_4_connection_smoke_256_rows.csv` | 首次 256 行 PG18.4 链路冒烟 CSV |
| `pg18_4_connection_smoke_runs.csv` | PG18.4 连接冒烟补充运行 CSV |
| `pg18_4_script_dryrun.csv` | 画像脚本 dry-run 展开验证 CSV |
| `feasibility_report.md` | 自动生成的前期可行性实验分析 |
| `current_direction_analysis.md` | 人工整理的当前方向分析 |

PG18.4 系统画像与瓶颈定位实验已经移动到：

```text
motivation/results/pg18_4_fake/system_profile.md
motivation/results/pg18_4_fake/system_profile.csv
```

## 报告生成

```bash
python feasibility/benchmarks/analyze_results.py \
  --results-dir feasibility/results \
  --output feasibility/results/feasibility_report.md
```

## Trigger Surface Validation Files

```text
trigger_surface_validation_20260714.md
pg18_4_post_migration_health_20260714.csv
pgai_sql_profile_20260714.csv
trigger_surface_comparison_20260714.csv
trigger_surface_pgai_sql_20260714.csv
```

These files validate that the existing PG18.4 job-table chain and the isolated
pgai SQL trigger surface can both run small embedding workloads. They are
feasibility results, not GPU-backed or PostgreSQL 18.3 performance conclusions.

## 2026-07-14 pgai SQL scale file

```text
pgai_sql_scale_20260714.csv
```

This is a feasibility-side SQL trigger-surface timing file. It is not a
GPU-backed result and is not used as PostgreSQL 18.4 or PostgreSQL 18.3
performance evidence.
