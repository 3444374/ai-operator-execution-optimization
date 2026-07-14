# Figure Scripts

本目录存放项目级图表的可复现生成脚本。脚本可用于 learning、开题报告、PPT、中期汇报和毕业论文图表复现。

## Python 图表脚本

```powershell
python figures\scripts\generate_gpu_experiment_charts.py
```

输入数据：

```text
motivation/results/gpu/ai_embed_chain_breakdown_20260712.csv
motivation/results/gpu/ai_embed_multi_endpoint_20260712.csv
```

默认输出目录：

```text
figures/data/generated_gpu/
```

生成内容：

- 调用粒度对端到端耗时的影响；
- single / dual endpoint 下 Python、Ray task、Ray actor 对比；
- Ray actor 单 / 双 endpoint 扩展对比；
- 数据库到 GPU 再到写回的链路阶段绝对时延；
- 链路阶段占比。

使用边界：

- 只使用真实 GPU-backed CSV 的 formal repeats 平均，排除 warm-up。
- 当前结果是 PostgreSQL 18.4 本地预演、真实 CUDA embedding endpoint、PostgreSQL JSON text writeback。
- 不能写成 PostgreSQL 18.3 内部平台结果，也不能写成 384 维 pgvector 或 Lance 写回结果。
- `figures/data/generated_gpu/` 是重新生成时的中间输出目录；正式材料只引用 `figures/data/report_main/` 或 `figures/data/backup/` 下筛选后的副本。

## 全量有意义对比图脚本

```powershell
python figures\scripts\generate_all_meaningful_experiment_charts.py
```

默认输出目录：

```text
figures/data/generated_all_meaningful/
```

该脚本会把当前项目中对研究有解释价值的实验数据都画成候选图，包括：

- 真实 CPU / GPU endpoint 对比；
- PG18.4 fake-model baseline、pgvector scaling 和 writeback batching；
- fake/CPU workload matrix、granularity attribution、backpressure 和 fake AI_EMBED pipeline；
- feasibility 中 Ray / Arrow / object transfer / small task / shuffle simulation 的组件级对比。

使用边界：

- 真实 GPU-backed 图优先用于开题主证据；
- PG18.4 fake-model 图只能作为本地同构预演和历史信号；
- fake/CPU 图只能作为机制预研，不能替代真实 GPU-backed 链路归因；
- feasibility 图只能说明组件级可观测信号，不承担论文主结论。
- `figures/data/generated_all_meaningful/` 是重新生成时的中间输出目录；未进入 `report_main/` 或 `backup/` 的 PNG / SVG 不长期保留。

## Learning 链路拆分图脚本

```powershell
python figures\scripts\make_chain_breakdown_figures.py
```

输入数据：

```text
motivation/results/gpu/ai_embed_chain_breakdown_20260712.csv
```

默认输出目录：

```text
figures/learning/
```

该脚本只负责把已经完成的 GPU-backed 链路拆分 CSV 转成学习讲解图，不承担实验运行、数据采集或系统 profiling；实验主体代码仍放在 `code/scripts/`。

## 2026-07-14 pgai-integrated GPU rerun charts

```powershell
python figures\scripts\generate_pgai_integrated_gpu_rerun_charts.py
```

Input:

```text
motivation/results/gpu/ai_embed_pgai_integrated_key_20260714.csv
```

Outputs:

```text
figures/data/report_main/06_gpu_pgai_rerun_granularity_20260714.png
figures/data/report_main/06_gpu_pgai_rerun_granularity_20260714.svg
figures/data/report_main/07_gpu_pgai_rerun_stage_writeback_20260714.png
figures/data/report_main/07_gpu_pgai_rerun_stage_writeback_20260714.svg
figures/data/report_main/08_gpu_pgai_rerun_endpoint_comparison_20260714.png
figures/data/report_main/08_gpu_pgai_rerun_endpoint_comparison_20260714.svg
```

Boundary: these charts are PostgreSQL 18.4 local rehearsal and GPU-backed
job-table profile results. They are not PostgreSQL 18.3, pgai SQL performance,
multi-GPU, or pgvector vector(384) writeback results.

## 2026-07-14 pgvector(384) writeback chart

```powershell
python figures\scripts\generate_pgvector_writeback_chart.py
```

Input:

```text
motivation/results/gpu/ai_embed_pgvector_writeback_20260714.csv
```

Outputs:

```text
figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.png
figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.svg
```

Boundary: this chart compares no writeback, JSON text writeback, and
pgvector `vector(384)` writeback in the same PostgreSQL 18.4 local
GPU-backed Ray actor profile chain.
