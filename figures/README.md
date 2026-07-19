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
| `architecture/system_architecture_ai_data_execution.png` / `.svg` | 课题总体研究框架，定义数据库 -> Daft/Arrow -> Ray -> GPU model service -> sink 的研究对象，并标出计划层、运行层、服务端动态批处理和写回判定位置 |
| `architecture/research_gap_three_islands.png` / `.svg` | 研究缺口图，说明三个成熟方向（DB4AI、推理服务、数据存储）之间的空白和本课题定位 |
| `architecture/cross_layer_method_framework.png` / `.svg` | 研究方案图，说明三类 AI workload、分阶段性能剖析、三层上游执行策略、结果写回瓶颈判定和端到端效果评估 |
| `architecture/runtime_strategy_control_loop.png` / `.svg` | 运行时策略闭环图，当前首选策略机制图；用一个 AI_EMBED SQL 例子说明计划层 batch/partition、运行层 K_max/routing/backpressure、服务端 micro-batch 和写回 guardrail 如何协同 |
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

## 2026-07-15 strategy figure design notes

Before redrawing the strategy-design figure, read:

```text
figures/audit/top_venue_strategy_figure_design_notes.md
figures/audit/strategy_figure_micro_design_points.md
figures/audit/local_reference_figure_reading_notes.md
```

This note extracts design patterns from top systems papers and recommends that
the next strategy figure use a control-loop plus compact rule-table structure,
rather than a three-column list of signals and actions.

`strategy_figure_micro_design_points.md` further decomposes the strategy figure
into smaller drawable mechanisms: workload-aware batch/partition selection,
bounded in-flight control, endpoint routing, writeback guardrails, and the
Trigger -> Action -> Guardrail rule table.

`local_reference_figure_reading_notes.md` records figure-design lessons from
the locally downloaded PDF subset under `opening/literature/reference/` and
connects them to the current runtime control-loop figure.

The generated control-loop figure is:

```text
figures/architecture/runtime_strategy_control_loop.png
figures/architecture/runtime_strategy_control_loop.svg
figures/architecture/runtime_strategy_rule_table.png
figures/architecture/runtime_strategy_rule_table.svg
figures/scripts/generate_runtime_strategy_control_loop.py
figures/audit/runtime_strategy_control_loop_audit.md
```

`runtime_strategy_control_loop.*` and `runtime_strategy_rule_table.*` are used
as a pair: the first figure explains the runtime feedback loop, and the second
figure explains the compact observed-signal -> candidate-action -> guardrail
table. The rule table records candidate logic to be validated, not final proven
rules. Visible labels are mostly Chinese, with only necessary technical tokens
such as `AI_EMBED`, `SQL`, `GPU`, `K_max`, `P99`, and `token` retained.

## 2026-07-15 archived strategy iterations

The following strategy-figure iterations are archived and should not be used as
current opening-report or PPT figures:

```text
figures/archive/architecture/20260715_strategy_iterations/upstream_strategy_design.png
figures/archive/architecture/20260715_strategy_iterations/upstream_strategy_design.svg
figures/archive/architecture/20260715_strategy_iterations/optimization_strategy_logic.png
figures/archive/architecture/20260715_strategy_iterations/optimization_strategy_logic.svg
figures/archive/architecture/20260715_strategy_iterations/_font_test.png
```

Use `runtime_strategy_control_loop.*` and `runtime_strategy_rule_table.*`
instead for the current strategy-design explanation.

## 2026-07-18 local vLLM Ray baseline support figures

Latest learning-support figures generated from the local ShareGPT/BurstGPT
`AI_COMPLETE` baseline:

```text
figures/data/backup/b07_local_vllm_ray_throughput.png
figures/data/backup/b07_local_vllm_ray_throughput.svg
figures/data/backup/b08_local_vllm_ray_e2e_time.png
figures/data/backup/b08_local_vllm_ray_e2e_time.svg
figures/data/backup/b09_local_vllm_ray_task_stage_timing.png
figures/data/backup/b09_local_vllm_ray_task_stage_timing.svg
figures/data/backup/b10_local_vllm_request_count_inflight.png
figures/data/backup/b10_local_vllm_request_count_inflight.svg
figures/data/backup/b11_local_vllm_token_tail_performance.png
figures/data/backup/b11_local_vllm_token_tail_performance.svg
figures/data/backup/b12_local_vllm_latency_probe_breakdown.png
figures/data/backup/b12_local_vllm_latency_probe_breakdown.svg
figures/data/backup/b13_local_vllm_token_tail_penalty.png
figures/data/backup/b13_local_vllm_token_tail_penalty.svg
figures/data/backup/b14_local_vllm_service_tail_gap.png
figures/data/backup/b14_local_vllm_service_tail_gap.svg
figures/data/backup/b15_local_vllm_token_budget_throughput.png
figures/data/backup/b15_local_vllm_token_budget_throughput.svg
figures/data/backup/b16_local_vllm_token_budget_tail_queue.png
figures/data/backup/b16_local_vllm_token_budget_tail_queue.svg
figures/data/backup/b17_local_vllm_arrival_kmax_sweep.png
figures/data/backup/b17_local_vllm_arrival_kmax_sweep.svg
figures/data/backup/b18_local_vllm_batch_kmax_e2e.png
figures/data/backup/b18_local_vllm_batch_kmax_e2e.svg
figures/data/backup/b19_local_vllm_batch_kmax_service_pressure.png
figures/data/backup/b19_local_vllm_batch_kmax_service_pressure.svg
figures/data/backup/b20_local_vllm_batch_kmax_request_granularity.png
figures/data/backup/b20_local_vllm_batch_kmax_request_granularity.svg
figures/data/backup/b21_local_vllm_kmax_interference_small_job.png
figures/data/backup/b21_local_vllm_kmax_interference_small_job.svg
figures/data/backup/b22_local_vllm_length_prefix_tail.png
figures/data/backup/b22_local_vllm_length_prefix_tail.svg
figures/data/backup/b23_local_vllm_length_prefix_signal.png
figures/data/backup/b23_local_vllm_length_prefix_signal.svg
figures/data/backup/b24_local_vllm_interference_sweep_small_job.png
figures/data/backup/b24_local_vllm_interference_sweep_small_job.svg
figures/data/backup/b25_local_vllm_interference_sweep_bulk_tradeoff.png
figures/data/backup/b25_local_vllm_interference_sweep_bulk_tradeoff.svg
figures/scripts/generate_local_vllm_ray_baseline_charts.py
```

Audit:

```text
figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md
```

These are backup and learning figures for the local
`PostgreSQL -> Daft -> Ray -> vLLM` fixed row-batch baseline. Each figure has a
single purpose: throughput, end-to-end time, Ray task stage timing, request
in-flight utilization, token-tail performance, latency metric breakdown,
token-tail penalty, service-tail gap, token-budget throughput comparison,
token-budget tail/queue comparison, arrival-aware `K_max` sweep, or coupled
batch-policy x `K_max` matrix analysis.
They show batch-size overheads and why fixed row count is an imprecise proxy
for model request cost. `b15` and `b16` split the first direct token-budget
policy comparison into a throughput view and a token-tail/queue view. They are
still local single-endpoint, no-writeback results; they motivate the next
`K_max` and queue-adaptive experiments rather than proving the full optimized
method. `b17` is a preliminary single-request-shape scheduling support figure.
`b18`-`b20` are the corrected coupled scheduling figures: they vary fixed-row
and token-budget batch shapes together with `K_max`, showing end-to-end
plateaus, vLLM queue/service-tail pressure, and the request-granularity limit
that makes very large fixed batches leave little room for admission control.
`b21` is the first shared-service interference figure: a foreground small job
shares the same vLLM endpoint with a background bulk job, showing that
unbounded background inflight hurts foreground E2E, service P95, and queue
stability compared with bounded `K_max=8`.
`b22` and `b23` record the first length-align and prefix-aware data
organization ablation. `b22` separates token tail from service tail; `b23`
shows the organization signals. The current prefix-aware result only shows a
small prefix-group-ratio change, not a proven KV-cache or APC benefit.
`b24` and `b25` extend the shared-vLLM interference experiment into a formal
sweep over background `K_max={8,16,unbounded}` plus a tuned queue-adaptive
implementation test. In this run, `K_max=8` protects the foreground job better
than larger background inflight. Tuned adaptive does downshift, but it is not
yet better than static `K_max=8`.
