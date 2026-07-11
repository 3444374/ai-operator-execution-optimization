# Feasibility Results

本目录保存可行性 benchmark、环境验证和连接验证结果。

规则：

- 系统组件 microbenchmark 放在这里。
- PG18.4 连接/环境验证放在这里。
- 数据库 AI 算子端到端动机测试、系统画像、瓶颈定位和可优化点分析放在 `motivation/results/`。

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
| `pg18_4_connection_smoke_256_rows.csv` | 首次 256 行 PG18.4 链路冒烟 CSV |
| `pg18_4_connection_smoke_runs.csv` | PG18.4 连接冒烟补充运行 CSV |
| `pg18_4_script_dryrun.csv` | 画像脚本 dry-run 展开验证 CSV |
| `feasibility_report.md` | 自动生成的前期可行性实验分析 |
| `current_direction_analysis.md` | 人工整理的当前方向分析 |

PG18.4 系统画像与瓶颈定位实验已经移动到：

```text
motivation/results/pg18_4_system_profile_fake_ai_embed.md
motivation/results/pg18_4_system_profile_fake_ai_embed.csv
```

## 报告生成

```bash
python validation/benchmarks/analyze_results.py \
  --results-dir validation/results \
  --output validation/results/feasibility_report.md
```
