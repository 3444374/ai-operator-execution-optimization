# 实验图表质检记录

本记录对应项目级图资产库 `figures/`。旧的 `opening/assets/charts/` 仅作为历史生成路径，不再作为正式引用目录。

## 当前正式图目录

- 报告和 PPT 正文主图：`figures/data/report_main/`
- PPT 备份页、飞书补充和后续讨论图：`figures/data/backup/`
- 生成脚本：`figures/scripts/`
- 图表筛选说明：`figures/data/selected_motivation_figures.md`

## 生成脚本

真实 GPU-backed 实验图：

```powershell
python figures\scripts\generate_gpu_experiment_charts.py
```

候选实验图全集：

```powershell
python figures\scripts\generate_all_meaningful_experiment_charts.py
```

系统架构图：

```powershell
python figures\scripts\generate_system_architecture_figure.py
```

## 正文主图清单

| 图表 | 文件 | 数据来源 | 图表类型 | 主要用途 |
|---|---|---|---|---|
| 系统架构图 | `figures/architecture/system_architecture_ai_data_execution.png` / `.svg` | 项目研究边界与系统设计 | 架构图 | 定义数据库驱动 AI workload 的外部数据执行与存储链路 |
| 数据库到 GPU 再到写回的链路阶段时延 | `figures/data/report_main/02_gpu_stage_latency_stack.png` / `.svg` | `motivation/results/gpu/ai_embed_chain_breakdown_20260712.csv`、`motivation/results/gpu/ai_embed_multi_endpoint_20260712.csv` | 横向堆叠柱状图 | 展示 DB fetch、Arrow batch build、Ray residual、GPU request wall、fan-in 和 sink writeback 的绝对时延 |
| 调用粒度对端到端耗时的影响 | `figures/data/report_main/03_invocation_granularity.png` / `.svg` | `motivation/results/gpu/ai_embed_chain_breakdown_20260712.csv` | 双柱状图 | 对比 1024 行下 coalesced 与 fine 的端到端耗时和 endpoint 调用数 |
| single / dual endpoint 执行方式对比 | `figures/data/report_main/04_executor_endpoint_comparison.png` / `.svg` | `motivation/results/gpu/ai_embed_chain_breakdown_20260712.csv`、`motivation/results/gpu/ai_embed_multi_endpoint_20260712.csv` | 分组柱状图 | 说明单 endpoint 下 Ray 与 Python 接近，双 endpoint 下 Ray 降低 operator wall time |
| Ray actor endpoint 扩展与写回约束 | `figures/data/report_main/05_actor_endpoint_scaling_writeback.png` / `.svg` | 同上 | 分组柱状图 | 展示多 endpoint 后写回和 fan-in 仍会限制端到端收益 |

## 质检结果

- 数据来源：通过。正文主图均来自真实 GPU-backed CSV，排除 warm-up，仅使用 formal repeats 平均。
- 证据分层：通过。GPU-backed、PG18.4 fake、fake/CPU 和 feasibility 图分开存放，不合并成同一张性能结论图。
- 图文一致性：通过。报告正文、飞书本地稿和图表筛选说明已统一引用 `figures/` 下的正式图。
- 对比意义：通过。正文主图只合并同一实验边界下可比较的数据，不把连接验证、dry-run、smoke 或 CPU/fake 预研结果当成最终性能结论。
- 可读性：通过。PNG 用于预览和 PPT，SVG 用于报告和后续 Word 转写；标题、坐标轴、图例和关键数值可读。
- 边界标注：通过。图题和图注说明 PostgreSQL 18.4 本地预演、真实 CUDA embedding endpoint、JSON text writeback 等边界，避免写成 PostgreSQL 18.3 平台或 pgvector 384 维最终结果。

## 保留与删除规则

- 正式材料优先引用 `figures/architecture/` 和 `figures/data/report_main/`。
- 解释实验设计来源、答辩追问或 PPT 备份页可引用 `figures/data/backup/`。
- 中间生成目录可以删除；再次需要时用 `figures/scripts/` 从 CSV 重新生成。
- 旧路径 `opening/assets/charts/`、`opening/assets/figures/` 和 `learning/figures/` 不再维护正式图副本。
