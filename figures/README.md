# 2026-07-14 Current Figure Set

For the current opening-report version, prefer these PG18.4
pgai-integrated GPU-backed rerun figures over older 2026-07-12 GPU charts:

```text
figures/data/report_main/06_gpu_pgai_rerun_granularity_20260714.png
figures/data/report_main/07_gpu_pgai_rerun_stage_writeback_20260714.png
figures/data/report_main/08_gpu_pgai_rerun_endpoint_comparison_20260714.png
figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.png
```

The older `03_invocation_granularity`, `04_executor_endpoint_comparison`,
and `05_actor_endpoint_scaling_writeback` figures are retained as historical
motivation assets, but should not be the first citation for the latest local
pgai-integrated GPU-backed rerun.

# Project Figures

做图、改图、迁移图或审查图表前，先读 `figures/AGENTS.md`。本文件只维护当前图资产入口、正式图清单和保留规则。

本目录是项目级图资产库，供 learning 材料、开题报告、开题 PPT、中期汇报和毕业论文共同复用。图不再分散在 `opening/assets/charts/` 和 `opening/assets/figures/` 中；后续新增图也优先放在本目录下，并按用途分子目录。

## 目录结构

```text
figures/
  architecture/       系统架构图、流程图、方法框架图
  data/report_main/   报告、PPT、论文正文优先使用的数据图
  data/backup/        答辩备份、飞书补充和 learning 可选支撑图
  audit/              图表质检、图表选择说明、设计审计记录
  scripts/            可复现绘图脚本
```

## 正文主线图

```text
figures/architecture/
figures/data/report_main/
```

建议进入开题报告正文、PPT 正文，后续中期汇报和毕业论文也优先从这里选图：

| 文件 | 用途 |
|---|---|
| `architecture/system_architecture_ai_data_execution.png` / `.svg` | 课题总体研究框架，定义数据库 -> Daft/Arrow -> Ray -> GPU model service -> sink 的研究对象 |
| `data/report_main/02_gpu_stage_latency_stack.png` / `.svg` | 真实 GPU-backed 链路阶段耗时，说明端到端成本可拆解、可观测 |
| `data/report_main/03_invocation_granularity.png` / `.svg` | 调用粒度对比，说明 batch / invocation 粒度值得调 |
| `data/report_main/04_executor_endpoint_comparison.png` / `.svg` | single / dual endpoint 下执行方式对比，说明 Ray 的价值依赖模型服务并行条件 |
| `data/report_main/05_actor_endpoint_scaling_writeback.png` / `.svg` | actor endpoint scaling 和写回约束，说明只优化模型调用会被 writeback 限制 |

## 备份与补充图

目录：

```text
figures/data/backup/
```

建议用于 PPT 备份页、飞书补充说明、learning 讲解或答辩问答：

| 文件 | 用途 |
|---|---|
| `b01_workload_matrix_speedup.png` | 解释为什么保留 `AI_EMBED`、`AI_FILTER/AI_CLASSIFY`、`AI_COMPLETE` 三类 workload |
| `b02_granularity_attribution.png` | 解释为什么调 batch / partition / task / invocation / object / fan-in |
| `b03_backpressure_queue_pressure.png` | 解释为什么调 bounded in-flight、queue wait 和 backpressure |
| `b04_writeback_batching.png` | 解释为什么 writeback 和持久化协同值得研究 |
| `b05_ray_arrow_fanout_fanin.png` | 解释 Ray / Arrow object 粒度和 fan-in 组件信号 |
| `b06_stage_share.png` / `.svg` | 补充展示 GPU request wall 和 writeback 的阶段占比变化 |

## 使用边界

- `report_main/` 中的 GPU 图来自真实 GPU-backed CSV，可作为开题主动机和可行性分析主证据。
- `data/backup/` 中部分图来自 fake/CPU、PG18.4 fake 或 feasibility benchmark，只能用于解释实验设计来源和变量选择，不能替代真实 GPU-backed 结论。
- 报告或 PPT 中引用这些图时，图注必须说明对应证据层级和不能声称的边界。

## 保留规则

图表资产长期只保留以下内容：

```text
figures/
motivation/results/
feasibility/results/
```

其中 `figures/` 保留最终图、绘图脚本和图表审计；`motivation/results/` 与 `feasibility/results/` 保留原始 CSV / 结果报告。生成过程中的中间 PNG / SVG 不长期保留。

可以删除或不再使用的旧目录包括：

```text
opening/assets/charts/python/
opening/assets/charts/all_meaningful/
opening/assets/charts/gpu_embed_*.png
opening/assets/charts/gpu_embed_*.svg
opening/assets/generate_echarts_experiment_charts.js
opening/assets/charts/selected/
opening/assets/figures/system_architecture_ai_data_execution.*
learning/figures/ 中与本目录重复的正式图副本
```

删除前需要确认：

1. `opening/report/opening_report.md` 不再引用旧图路径。
2. `opening/feishu/opening_report_wiki.md` 不再引用旧图路径。
3. learning、中期汇报、毕业论文草稿不再引用旧图路径。
4. 开题 PPT 源稿和 PPTX 不再引用旧图路径。
5. 线上飞书文档中的图片已经重新上传为 `figures/data/report_main/` 对应版本。
6. 若仍需要复现旧图，保留 Python 脚本和原始 CSV 即可，不保留旧 PNG / SVG 副本。

## 2026-07-14 pgai-integrated GPU rerun figures

Latest report-main figures generated from
`motivation/results/gpu/ai_embed_pgai_integrated_key_20260714.csv`:

```text
figures/data/report_main/06_gpu_pgai_rerun_granularity_20260714.png
figures/data/report_main/06_gpu_pgai_rerun_granularity_20260714.svg
figures/data/report_main/07_gpu_pgai_rerun_stage_writeback_20260714.png
figures/data/report_main/07_gpu_pgai_rerun_stage_writeback_20260714.svg
figures/data/report_main/08_gpu_pgai_rerun_endpoint_comparison_20260714.png
figures/data/report_main/08_gpu_pgai_rerun_endpoint_comparison_20260714.svg
```

Audit:

```text
figures/audit/pgai_integrated_gpu_rerun_charts_audit_20260714.md
```

These figures should be cited before older 2026-07-12 GPU figures when the
opening report needs the latest local pgai-integrated GPU-backed rerun.

## 2026-07-14 pgvector(384) writeback figure

Latest sink-mode comparison generated from
`motivation/results/gpu/ai_embed_pgvector_writeback_20260714.csv`:

```text
figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.png
figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.svg
```

Audit:

```text
figures/audit/pgvector_writeback_chart_audit_20260714.md
```

This figure should be used when discussing whether JSON text writeback and
pgvector `vector(384)` writeback have the same cost in the local GPU-backed
chain.
