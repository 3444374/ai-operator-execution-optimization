# Recent Figure Assets

| File | Purpose | When to read/use |
|---|---|---|
| `figures/data/backup/b07_local_vllm_ray_throughput.png` / `.svg` | Local `AI_COMPLETE` Daft + Ray + vLLM throughput support figure | Learning and backup explanation of throughput across six fixed row-batch settings; not an optimized scheduling result |
| `figures/data/backup/b08_local_vllm_ray_e2e_time.png` / `.svg` | Local `AI_COMPLETE` Daft + Ray + vLLM end-to-end time support figure | Learning and backup explanation of end-to-end time across six fixed row-batch settings |
| `figures/data/backup/b09_local_vllm_ray_task_stage_timing.png` / `.svg` | Local `AI_COMPLETE` Ray task stage timing support figure | Learning and backup explanation of source, Daft organize, Ray submit, fan-in, and operator wall timings |
| `figures/data/backup/b10_local_vllm_request_count_inflight.png` / `.svg` | Local `AI_COMPLETE` request-count and in-flight utilization support figure | Learning and backup explanation of how large fixed row batches reduce request count and can underfill the in-flight window |
| `figures/data/backup/b11_local_vllm_token_tail_performance.png` / `.svg` | Local `AI_COMPLETE` token-tail performance support figure | Learning and backup explanation of why fixed row batches do not control token-tail cost or service-tail latency |
| `figures/data/backup/b12_local_vllm_latency_probe_breakdown.png` / `.svg` | Local `AI_COMPLETE` vLLM latency metric probe support figure | Learning and backup explanation of client batch latency versus vLLM server-side latency metrics |
| `figures/data/backup/b13_local_vllm_token_tail_penalty.png` / `.svg` | Local `AI_COMPLETE` token-tail penalty support figure | Explain the link between token P95 and model-service latency tail |
| `figures/data/backup/b14_local_vllm_service_tail_gap.png` / `.svg` | Local `AI_COMPLETE` service-tail gap support figure | Explain P50-to-P95 latency widening for large fixed row batches |
| `figures/data/backup/b15_local_vllm_token_budget_throughput.png` / `.svg` | Local `AI_COMPLETE` token-budget throughput support figure | Compare fixed-row and token-budget throughput under the same local vLLM setup |
| `figures/data/backup/b16_local_vllm_token_budget_tail_queue.png` / `.svg` | Local `AI_COMPLETE` token-budget token-tail and queue support figure | Show token-budget controls token P95 and queue pressure; motivates K_max follow-up |
| `figures/data/backup/b17_local_vllm_arrival_kmax_sweep.png` / `.svg` | Local `AI_COMPLETE` preliminary arrival-aware K_max support figure | Single-shape scheduling sweep with `token_budget=6144`; use as preliminary evidence only |
| `figures/data/backup/b18_local_vllm_batch_kmax_e2e.png` / `.svg` | Local `AI_COMPLETE` batch policy x K_max end-to-end support figure | Show how fixed-row and token-budget shapes interact with K_max in end-to-end time |
| `figures/data/backup/b19_local_vllm_batch_kmax_service_pressure.png` / `.svg` | Local `AI_COMPLETE` batch policy x K_max service-pressure support figure | Show vLLM queue time and batch service P95 rising when inflight submissions are too large |
| `figures/data/backup/b20_local_vllm_batch_kmax_request_granularity.png` / `.svg` | Local `AI_COMPLETE` batch policy request-granularity support figure | Explain why K_max cannot help once upstream batch shape creates too few Ray submissions |
| `figures/data/backup/b21_local_vllm_kmax_interference_small_job.png` / `.svg` | Local `AI_COMPLETE` shared-vLLM K_max interference support figure | Show unbounded background inflight harms foreground small-job latency on a shared vLLM endpoint |
| `figures/data/backup/b22_local_vllm_length_prefix_tail.png` / `.svg` | Local `AI_COMPLETE` length/prefix data-organization tail figure | Compare token tail and service tail for fixed, token-budget, length-align, and prefix-aware policies |
| `figures/data/backup/b23_local_vllm_length_prefix_signal.png` / `.svg` | Local `AI_COMPLETE` length/prefix organization-signal figure | Show prompt-token spread and prefix-group ratio; does not prove cache benefit |
| `figures/data/backup/b24_local_vllm_interference_sweep_small_job.png` / `.svg` | Local `AI_COMPLETE` shared-vLLM foreground interference sweep figure | Show foreground E2E/service/queue impact under background K_max 8/16/unbounded/adaptive |
| `figures/data/backup/b25_local_vllm_interference_sweep_bulk_tradeoff.png` / `.svg` | Local `AI_COMPLETE` shared-vLLM bulk tradeoff sweep figure | Show bulk throughput plateau and service/queue pressure under larger background inflight |
| `opening/slides/opening_defense_20260720_v5.pptx` | 开题答辩 PPT v5 | Current incremental deck based on v4; adds three data-organization mechanism slides after original slide 14 without rerunning `build_ppt.py` |
| `figures/architecture/data_organization_token_budget_mechanism.png` / `.svg` | 数据组织策略机制图：token-budget batching | Formal mechanism figure for converting fixed-row batches into token-budget submissions; not an experimental-result claim |
| `figures/architecture/data_organization_length_align_mechanism.png` / `.svg` | 数据组织策略机制图：length-aligned grouping | Formal mechanism figure for sorting/grouping rows by token length to reduce within-batch compute variance |
| `figures/architecture/data_organization_prefix_aware_mechanism.png` / `.svg` | 数据组织策略机制图：prefix-aware grouping | Formal mechanism figure for grouping shared system prompts; prefix-cache benefit still requires vLLM metric validation |
| `figures/scripts/generate_data_organization_strategy_mechanism.py` | 数据组织策略机制图生成脚本 | Regenerate the three formal mechanism figures and scan for forbidden visible tokens |
| `figures/audit/data_organization_strategy_mechanism_audit.md` | 数据组织策略机制图审计记录 | Read before citing the mechanism figures in report/PPT/thesis material |
| `figures/scripts/generate_local_vllm_ray_baseline_charts.py` | Local vLLM Ray baseline chart generator | Regenerate `b07`-`b25` from the ShareGPT/BurstGPT baseline CSVs |
| `figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md` | Local vLLM Ray baseline chart audit | Check data source, figure role, chart choice, visual QA, and conclusion boundary |
| `learning/local_vllm_ray_baseline_walkthrough.md` | Local vLLM + Ray baseline learning walkthrough | Read when explaining what the fixed row-batch baseline does and does not prove |
| `learning/archive/early_experiments_walkthrough.md` | 早期实验学习讲解（已归档） | pre-convergence 时期实验（组件可行性、fake/CPU、PG18.4 接入等）的历史参考 |
| `learning/metric_selection_methodology.md` | AI_EMBED vs AI_COMPLETE 观察变量选择方法论 | 理解为什么从"阶段时延拆分"转向"多维分布表征" |
| `figures/architecture/runtime_strategy_rule_table.png` / `.svg` | 信号触发候选策略规则表 | 与闭环图配套使用，说明观测信号、候选动作和保护约束；不作为已验证结论 |
| `figures/architecture/runtime_strategy_control_loop.png` / `.svg` | 运行时信号驱动的上游执行闭环图 | 当前首选策略机制图；用一个 AI_COMPLETE SQL 例子说明数据组织（token-budget/length-align/prefix-aware）、提交控制（queue-adaptive flush/K_max/routing）、vLLM 部署平台（观测不修改）的分工；不重切数据库侧已物化批次 |
| `figures/scripts/generate_runtime_strategy_control_loop.py` | 运行时策略闭环图生成脚本 | 重新生成策略机制图 PNG/SVG，并执行边框、箭头和禁用术语自检 |
| `figures/audit/runtime_strategy_control_loop_audit.md` | 运行时策略闭环图审计记录 | 检查新策略机制图的角色、旧图关系、遮挡、箭头和禁用术语 |
| `figures/audit/top_venue_strategy_figure_design_notes.md` | 顶会系统论文方法图设计备忘 | 重绘策略设计图前阅读，采用 control-loop + running example + compact rule table |
| `figures/audit/strategy_figure_micro_design_points.md` | 策略图小机制设计点与论文下载清单 | 重绘策略图前拆分 batch/partition、反压、路由、写回约束和规则表等小图 |
| `figures/audit/local_reference_figure_reading_notes.md` | 本地 PDF 图形阅读笔记 | 记录已下载论文中的机制图经验，并合并到运行时控制闭环图方案 |
# PROJECT_INDEX.md

本文件是项目索引，供 Codex 快速定位材料。先读 `AGENTS.md`，再按任务类型读本文件中的对应材料。

## 1. 快速阅读顺序

### 只想了解当前课题

1. `AGENTS.md`：长期规则、用户目标、当前确定方向。
2. `README.md`：项目概览和目录结构。
3. `PROJECT_OUTLINE.md`：当前题目、研究内容、关键证据、近期优先级。
4. `overview/current_direction_and_plan.md`：阶段性技术路线和计划。
5. `motivation/results/gpu/README.md`：真实 GPU-backed E2E 结果入口。

### 要继续做实验

1. `AGENTS.md`：实验规则。
2. `motivation/plans/workloads.md`：三类 AI 算子场景、动机测试和后续实验优先级。
3. `motivation/plans/integration.md`：PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线。
4. `feasibility/benchmarks/README.md`：组件 benchmark 脚本和运行命令。
5. `motivation/results/README.md`：动机测试正式结果阅读顺序和结论边界。
6. `motivation/results/gpu/README.md`：真实 GPU-backed E2E 结果入口。

### 要写调研/和导师沟通

1. `AGENTS.md`：沟通边界和不能声称什么。
2. `motivation/plans/ai_sql_surface.md`：数据库 AI 算子现状和 AI 算子 SQL 触发面分析。
3. `research/literature_and_evidence_review.md`：文献与官方资料依据。
4. `notes/communication_notes.md`：已有沟通问题和话术。

## 2. 核心文件地图

| 文件 | 内容 | 什么时候读 |
|---|---|---|
| `AGENTS.md` | 项目长期规则、用户真实目标、当前选题边界 | 每次开始任务先读 |
| `PROJECT_INDEX.md` | 文件索引和阅读顺序 | 不知道材料在哪里时读 |
| `PROJECT_OUTLINE.md` | 项目总纲：当前题目、研究内容、关键证据、近期优先级 | 快速了解最新进展 |
| `README.md` | 工作区总览、当前方向、目录结构 | 了解项目背景 |
| `overview/AGENTS.md` | 总览目录规则 | 修改 `current_direction_and_plan.md` 时读 |
| `overview/current_direction_and_plan.md` | 当前方向的快速参考卡片（TL;DR） | 2 分钟了解课题全貌 |
| `research/AGENTS.md` | 背景调研规则 | 写文献、资料依据时读 |
| `research/README.md` | 调研目录入口 | 了解 research/ 下有什么 |
| `research/literature_and_evidence_review.md` | 文献与官方资料依据 | 写调研、论文动机时读 |
| `research/existing_ai_operator_execution_chains.md` | 现有数据库 AI 算子与 AI 数据处理执行链路对比 | 比较外部系统路线时读 |
| `research/knowledge_hub.md` | **知识库总汇**——按问题快速定位参考材料、已知结论和待研究缺口 | 开始设计、做决策前先读 |
| `research/vllm_continuous_batching_reference.md` | vLLM Continuous Batching 机制详解（调度器、APC、metrics） | 设计上游动态 batching 策略时读 |
| `research/ray_actor_dynamic_batching_reference.md` | Ray Serve 动态 batching + Ray Core actor 模式 | 设计 Ray actor 自适应提交架构时读 |
| `research/inference_pipeline_interaction_literature.md` | 推理管线交互文献汇总 | 写相关工作、确认研究空白时读 |
| `motivation/AGENTS.md` | 动机实验规则 | 搭建 AI 算子场景或端到端动机测试前读 |
| `motivation/README.md` | 动机测试目录详细说明 | 了解 motivation/ 下有什么、怎么组织 |
| `motivation/plans/workloads.md` | 三类 AI 算子场景、动机测试和 idea-evaluator 评估 | 比较候选场景、决定下一步测试时读 |
| `motivation/plans/integration.md` | PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线 | 规划集成和测试时读 |
| `motivation/plans/ai_sql_surface.md` | 数据库 AI 算子现状、推荐业务场景、动机测试标准 | 搭建业务场景前读 |
| `motivation/benchmarks/fake_embed_pipeline.py` | fake `AI_EMBED(text)` 端到端动机测试脚本 | 验证 embedding / RAG 链路中的 fan-in 成本 |
| `motivation/benchmarks/workload_matrix.py` | 三类候选 AI 算子场景动机测试脚本 | 比较不同 AI 算子的瓶颈形态 |
| `motivation/benchmarks/granularity.py` | AI 算子粒度归因动机测试脚本 | 拆分 task/object/fan-in/invocation 的收益 |
| `motivation/benchmarks/backpressure.py` | AI 算子模型服务反压模拟脚本 | 验证 queue wait、token backlog、in-flight 和 backpressure |
| `motivation/results/README.md` | 动机测试结果阅读顺序和结论边界 | 讲解动机测试、整理实验结论时读 |
| `motivation/results/gpu/README.md` | 真实 GPU-backed E2E 结果入口 | 当前最优先引用的正式证据 |
| `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md` | GPU-backed embedding 链路拆分 | 引用 stage breakdown 和 fine/coalesced 对比 |
| `motivation/results/gpu/pgai_integrated_key_rerun_20260714.md` | pgai-integrated GPU-backed rerun | 引用最新 rerun 结果 |
| `motivation/results/fake_cpu/analysis.md` | fake/CPU 历史预研分析 | 了解早期为什么关注 task/object/invocation/fan-in/backpressure |
| `motivation/results/pg18_4_fake/` | PG18.4 本地同构预演 | 只作为预演和历史信号，不代表真实 GPU-backed 结论 |
| `feasibility/AGENTS.md` | 可行性验证规则 | 做组件 benchmark 或环境验证前读 |
| `feasibility/README.md` | 可行性验证目录入口 | 了解 feasibility/ 下有什么、怎么组织 |
| `feasibility/benchmarks/README.md` | benchmark 说明和运行命令 | 运行组件 benchmark |
| `feasibility/results/README.md` | 可行性结果索引 | 查看组件验证、连接验证、smoke 结果 |
| `experiments/AGENTS.md` | 正式研究实验规则 | 设计优化实验、消融实验前读 |
| `experiments/README.md` | 正式研究实验入口 | 了解三项研究内容的实验规划 |
| `experiments/results/local_vllm_qwen15b_baseline/README.md` | 本地 vLLM Qwen2.5-1.5B `AI_COMPLETE` 静态行 batch baseline | 做 token-aware batching 和调度消融前，作为固定本地对照 |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_static_batch_sweep_rerun_20260718.csv` | Local `AI_COMPLETE` ShareGPT/BurstGPT fixed row-batch rerun through Daft + Ray + vLLM | Baseline CSV for later data-organization and scheduling comparisons |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_task_batch8_latency_metrics_20260718.csv` | Local `AI_COMPLETE` Daft + Ray + vLLM latency metric probe | Verifies batch token/latency and vLLM server-side metric collection |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv` | Local `AI_COMPLETE` token-budget versus fixed-row matrix | First policy comparison for data-organization experiments; 512 rows, local vLLM, no writeback |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_arrival_kmax_token6144_20260719.csv` | Local `AI_COMPLETE` preliminary arrival-aware K_max sweep | Single request-shape scheduling sweep; superseded by the batch policy x K_max matrix for static baseline selection |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_batch_policy_kmax_matrix_20260719.csv` | Local `AI_COMPLETE` batch policy x K_max matrix | Coupled fixed-row/token-budget and admission-control experiment; main static scheduling baseline before queue-adaptive flush |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_small_20260719.csv` | Local `AI_COMPLETE` foreground small-job K_max interference result | Small-job solo/bounded-bulk/unbounded-bulk CSV for shared-vLLM admission-control motivation |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_bulk_20260719.csv` | Local `AI_COMPLETE` background bulk-job K_max interference result | Bulk-job bounded/unbounded CSV paired with the foreground interference result |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_length_prefix_ablation_20260719.csv` | Local `AI_COMPLETE` length-align and prefix-aware ablation | First data-organization ablation with token-tail, service-tail, prompt-token spread, and prefix-ratio metrics |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_sweep_small_20260719.csv` | Local `AI_COMPLETE` foreground small-job shared-vLLM sweep | Formal K_max/adaptive interference sweep for foreground latency and vLLM queue pressure |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_sweep_bulk_20260719.csv` | Local `AI_COMPLETE` background bulk-job shared-vLLM sweep | Paired bulk-job CSV for background throughput and service-pressure tradeoff |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_adaptive_tuned_small_20260719.csv` | Local `AI_COMPLETE` tuned adaptive foreground interference supplement | Shows adaptive downshift can trigger, but foreground latency remains worse than static K_max=8 |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_adaptive_tuned_bulk_20260719.csv` | Local `AI_COMPLETE` tuned adaptive bulk interference supplement | Paired bulk CSV recording adaptive downshifts, upshifts, and effective limit mean |
| `code/scripts/run_kmax_interference_experiment.py` | Shared-vLLM K_max interference runner | Starts background bulk and foreground small jobs against the same vLLM endpoint |
| `figures/AGENTS.md` | 图表长期规则 | 做图、改图、审查图前必读 |
| `figures/README.md` | 图资产入口 | 查找正式图、备份图和绘图脚本 |
| `learning/AGENTS.md` | 学习讲解规则 | 写学习材料前读 |
| `learning/README.md` | 学习材料入口 | 了解实验 walkthrough 和术语讲解 |
| `learning/experiment_walkthrough.md` | 按推进顺序讲解已完成实验 | 学习实验链路、参数和结果读法 |
| `learning/metric_selection_methodology.md` | AI_EMBED vs AI_COMPLETE 观察变量选择方法论 | 理解为什么从"阶段时延拆分"转向"多维分布表征" |
| `opening/AGENTS.md` | 开题工作规则 | 写开题报告、PPT、飞书材料前读 |
| `opening/README.md` | 开题工作区入口 | 了解开题材料分布和同步规则 |
| `opening/navigation.md` | 开题材料导航 | 不知道开题材料在哪时读 |
| `opening/report/opening_report.md` | 开题报告正文 | 写报告、和导师沟通、定方向 |
| `opening/literature/reading_list.md` | 开题文献精读清单 | 查看文献精读优先级、本地 PDF 子集入口和引用边界 |
| `opening/literature/gpu_scheduler_data_placement_supplement_20260715.md` | GPU 调度与数据放置补充调研 | 查看策略控制器设计的前沿系统依据、可借鉴思想和后续精读清单 |
| `opening/literature/reference/README.md` | 本地已下载 PDF 子集索引 | 查看当前部分参考文献 PDF、页数、初步识别和用途 |
| `data/README.md` | 本地 workload 数据说明；raw payloads 被 git ignore | 查看 ShareGPT/BurstGPT 下载位置、用途和边界 |
| `code/AGENTS.md` | 正式工程代码规则 | 后续迁移可复用代码前读 |
| `code/src/sources.py` | PostgreSQL data source 后端：psycopg/Arrow baseline、Daft SQL entry、`doc_id`/`arrival_time` source order | 切换或修改数据入口与读取顺序时读 |
| `code/src/organizers.py` | ArrowOrganizer / DaftOrganizer 数据组织后端 | 接入或比较 Arrow 与 Daft 文本数据组织路径时读 |
| `code/src/model_backends.py` | fake debug backend、vLLM-compatible HTTP embedding/completion backend、Ollama native completion backend | 修改模型服务接入、vLLM/Ollama endpoint 或 AI_COMPLETE backend 前读 |
| `code/src/sinks.py` | `none/json_text/pgvector` embedding 写回与 completion JSON-text 写回 | 修改写回路径或后续接 Lance sink 前读 |
| `code/src/metrics.py` | Stage timer、GPU snapshot 和 CSV metric helper | 修改 profiling 指标、CSV 输出或计时边界前读 |
| `code/src/workloads.py` | 内置 synthetic / controlled workload seed | 仅用于 smoke/dev；最终 baseline 优先用 ShareGPT/BurstGPT importer |
| `code/scripts/import_ai_complete_workload.py` | ShareGPT prompt + BurstGPT trace 归一化导入脚本 | 构造最终可比 `AI_COMPLETE` baseline workload 前运行 |
| `code/tests/test_sources.py` | data source 查询构造和 source factory 单元测试 | 修改数据入口行为后运行 |
| `code/tests/test_organizers.py` | 数据组织后端最小单元测试 | 修改 organizer 接口或 batch 行为后运行 |
| `code/tests/test_model_backends.py` | 模型后端最小单元测试 | 修改 fake 或 compatible HTTP embedding backend 后运行 |
| `code/tests/test_sinks.py` | 写回后端最小单元测试 | 修改 sink/writeback 行为后运行 |
| `code/tests/test_workloads.py` | 内置 workload seed 单元测试 | 修改 smoke/dev workload 后运行 |
| `code/tests/test_import_ai_complete_workload.py` | ShareGPT/BurstGPT importer 单元测试 | 修改 importer 或 trace 过滤逻辑后运行 |
| `code/scripts/daft_text_organizer_smoke.py` | Daft/Arrow organizer smoke 入口 | 验证文本阶段 Daft 最小接入 |
| `code/scripts/README.md` | 脚本详细说明 | 运行 PostgreSQL 画像、pgai SQL profile、本地 embedding server、Daft text organizer smoke |
| `code_doc/superpowers/plans/` | Superpowers implementation plans for code work | 按 superpowers 工作流执行多步代码任务前读 |
| `deploy/pgai/` | pgai Docker Compose 部署 | 启动 pgai 测试环境 |
| `deploy/postgres18.4/` | PostgreSQL 18.4 Docker Compose 部署 | 启动 PG18.4 同构预演环境 |
| `notes/AGENTS.md` | 沟通材料规则 | 整理导师/企业侧反馈时读 |
| `notes/communication_notes.md` | 和同事/导师需要确认的问题和沟通话术 | 准备沟通 |

## 3. 实验规划在哪里

全局项目路线和近期实验任务：

`PROJECT_OUTLINE.md`

主要内容：

- 当前开题题目、研究内容（两项策略 + 多模态泛化验证 + 算子代价估计补充）；
- 实验主线和当前最重要证据；
- 近期优先级；
- 双向同步规则。

动机测试的计划（场景设计、集成路线）：

`motivation/plans/workloads.md`：三类 AI 算子场景、动机测试和后续实验优先级。

`motivation/plans/integration.md`：PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线和分阶段实验。

可行性验证的参考（组件、环境、脚本可用性）：

`feasibility/benchmarks/README.md`。

正式研究实验（方法有效性验证）：

`experiments/plans/`

| 文件 | 作用 |
|---|---|
| `archive/research_design_catalog.md` | 课题研究方案候选目录（已归档）：28 个候选方案的六维评估，作为设计历史参考 |
| `baseline_reference.md` | 实验 Baseline 参考矩阵：从 CCF-A 文献中提取的各方向最优 baseline 策略（GPU 调度/数据组织/提交控制）|
| `strategy_design_literature_basis.md` | 策略设计思路的文献依据与边界：区分可借鉴思想、baseline/边界和本文自己的策略定义 |
| `strategy_design_implementation_reference.md` | 策略设计与系统实现参考：把 Ray、vLLM、Daft、GPU 数据放置和 DB AI 算子机制沉淀为两项策略 + 端到端验证、实验变量和实现优先级（2026-07-17 已统一口径）|
| `data_organization_batching.md` | 研究内容一实验计划：token-budget、length-aligned、prefix-aware grouping + Daft 引擎级参数 |
| `service_scheduling_backpressure.md` | 研究内容二实验计划：queue-adaptive flush、actor pool 分池路由、K_max 动态控制 + Daft 引擎级参数 |
| `sink_writeback_coordination.md` | 写回工程参考（不作为独立实验阶段）：COPY + deferred index baseline |
| `cross_layer_killer_experiment.md` | 耦合验证实验计划：独立最优拼接 vs 联合 grid search（含策略级 + 引擎级参数的完整交互面）|
| `experiment_status_and_gaps.md` | **实验状态与缺口分析（2026-07-20）**：已完成/未完成实验表、证据链完整性、指标盲区、P0/P1/P2 路线图、审稿人视角风险。当前实验设计的第一参考。|

所有实验计划遵循从 vLLM/Orca/TurboVecDB/GaussML/FlexPushdownDB 五篇 CCF-A 论文提取的共同方法论：曲线 > 单点、先暴露瓶颈再优化、同硬件公平 baseline、消融拆开、诚实报告边界、统计严谨。

## 4. 实验代码在哪里

活跃可行性 benchmark：

`feasibility/benchmarks/`

| 文件 | 作用 |
|---|---|
| `README.md` | benchmark 说明和运行命令 |
| `requirements.txt` | Python 依赖：Ray、NumPy、PyArrow |
| `common.py` | CSV 输出、表格打印、依赖检查等公共函数 |
| `ray_many_objects_benchmark.py` | 固定总数据量下 Ray many-object fan-in |
| `ray_arrow_fanout_fanin_benchmark.py` | Arrow RecordBatch 版 Ray `N upstream -> P downstream` fan-out/fan-in |
| `analyze_results.py` | 汇总 CSV 并生成可行性报告 |

早期排除性实验（Ray small task、object transfer、Arrow serialization、shuffle simulation）保留在 `feasibility/benchmarks/` 中作为历史组件参考。这些实验证明了对应方向不是当前瓶颈，不代表真实 GPU-backed 数据库 AI 算子链路瓶颈。

动机测试脚本：

`motivation/benchmarks/`

| 文件 | 作用 |
|---|---|
| `fake_embed_pipeline.py` | fake `AI_EMBED(text)` 端到端链路 |
| `workload_matrix.py` | 三类候选 AI 算子场景对比 |
| `granularity.py` | task/object/fan-in 收益来源拆分 |
| `backpressure.py` | 模型服务反压离散事件模拟 |

动机测试正式结果位于：

`motivation/results/`

- `fake_cpu/`：CPU/fake 历史预研（仅作背景参考）
- `cpu/`：CPU baseline 对照
- `gpu/`：GPU-backed E2E 主动机结果（当前最优先引用）
- `pg18_4_fake/`：PG18.4 本地同构预演（不代表真实平台结论）

推荐命令：

```bash
python motivation/benchmarks/fake_embed_pipeline.py \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --embedding-dim 128 \
  --repeats 3 \
  --output motivation/results/fake_cpu/fake_embed_pipeline.csv
```

```bash
python feasibility/benchmarks/analyze_results.py \
  --results-dir feasibility/results
```

运行环境：

- 使用 `.venv`；
- 当前没有必要使用 conda；
- Ray benchmark 在 macOS 沙箱中可能需要提权运行。

## 5. 实验结果在哪里

可行性验证结果（组件、环境、连接）：

`feasibility/results/`

动机测试正式结果（唯一来源）：

`motivation/results/`

讲解动机实验时引用 `motivation/results/gpu/`（GPU-backed，最优先）或 `motivation/results/fake_cpu/`（历史预研）。`feasibility/results/` 仅保留组件 benchmark 结果和环境验证。

正式论证优先引用：

1. `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md`：GPU-backed embedding 链路拆分。
2. `motivation/results/gpu/multi_endpoint_ray_motivation_20260712.md`：双 endpoint Ray task/actor 动机。
3. `motivation/results/gpu/pgai_integrated_key_rerun_20260714.md`：pgai-integrated GPU-backed rerun。
4. `motivation/results/gpu/pgvector_writeback_20260714.md`：pgvector(384) 写回对比。

## 6. 文献和资料依据在哪里

业务场景和动机测试文件：

`motivation/plans/ai_sql_surface.md`

主要内容：

- 现有数据库 AI 算子例子；
- AI 算子、数据库 AI 算子、模型 kernel、传统查询算子的区别；
- 推荐初步场景：批量 Embedding / RAG 数据准备；
- 最小原型设计；
- 瓶颈矩阵和动机测试判定标准。

集成与测试方法文件：

`motivation/plans/integration.md`

主要内容：

- PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线；
- 无设备/低端设备/PostgreSQL 18.3 平台的分阶段实验；
- AI_EMBED 算子集成形态；
- 瓶颈与优化点映射。

文献审查文件：

`research/literature_and_evidence_review.md`

主要内容：

- Ray 论文和官方文档；
- Daft Ray runner、partitioning、shuffle、join strategy；
- Spark partition / shuffle / AQE 类比；
- Arrow / Lance 论文背景；
- Snowflake / pgai / PostgresML / pgvector 外部系统依据；
- 本地实验和外部证据如何对应；
- 当前不能声称什么。

外部系统执行链路对比：

`research/existing_ai_operator_execution_chains.md`

使用原则：

- 写调研、汇报、论文动机时优先引用该文件；
- 不要只引用本地 microbenchmark；
- 结论必须区分"文献/官方文档""本地实验""合理推断""待确认"。

## 7. 当前方向边界

开题报告正式题目：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究。

两项策略设计 + 多模态泛化验证 + 算子代价估计补充（2026-07-17 更新，写回已降为实验设置）：

1. **AI workload 感知的动态数据组织与批处理构造策略**（研究内容一）：对比 token-budget 与固定 batch_size 在吞吐和 P99 上的差异，利用异构 actor pool + Daft 引擎级参数实现。
2. **调度与提交控制策略**（研究内容二）：利用 Ray actor 研究去中心化的调度与提交控制，候选策略包括 queue-adaptive flush、K_max 动态控制、actor pool 分池路由等。
3. **多模态泛化验证**（正文实验）：在图像 workload 上使用同一套策略代码验证模态无关性。
4. **算子代价估计**（补充讨论，不作为独立研究内容）。

主场景：`AI_COMPLETE`（文本 LLM）+ `AI_EMBED/AI_CLASSIFY`（图像，多模态泛化验证）。vLLM 为部署平台 + baseline，Daft 为数据引擎，不修改其内部调度器。PG18.4 本地预演，后续进入 PG18.3 内部平台复测。

当前不要优先做：

- 改造整个 Ray；
- 泛泛 Daft + Ray + Lance 集成；
- 单纯 Arrow serialization 优化；
- 单纯数据库 GPU 查询算子优化；
- 没有真实 workload 的 toy benchmark。
- 把 PG18.4 本地预演写成 PostgreSQL 18.3 内部平台结论。

## 8. 下一步优先工作

**已完成**：
- ✅ vLLM + Qwen2.5-1.5B baseline 建立（07-18）
- ✅ Daft 文本阶段直接接入，链路跑通
- ✅ Token-tail revision + Token-budget vs Fixed Row 对照
- ✅ Shared-vLLM K_max 干扰实验
- ✅ Queue-adaptive flush 首次实现与测试

**当前缺口**（详见 `experiments/plans/experiment_status_and_gaps.md`）：
1. **P0**：改进 queue-adaptive 控制器 + 两项策略联合消融
2. **P1**：Prefix 受控 workload + scale 到 2048 行
3. **P2**（触发：P0+P1 完成）：多模态泛化验证
4. 算子代价估计（§6.1 讨论，基于已有数据）

**Scope 缩减触发条件**：Month 1 无 vLLM baseline → 多模态降 Discussion（✅ 已建立，未触发）；文本 RC1+RC2 未完成不启动多模态 pipeline；VLM 生成始终 optional；Adaptive 3 轮不能超 static K_max=8 → RC2 降级。

详细实验计划见 `experiments/plans/`，以 `PROJECT_OUTLINE.md` §近期优先级和 `experiments/plans/experiment_status_and_gaps.md` 为准。

优先沟通问题：

- 达梦实际是否会使用 Ray/Daft/Lance 做数据库内置 AI 算子；
- 数据从数据库到外部执行链路的格式是什么；
- 真实 AI 算子是否批处理，是否涉及 join/groupby/repartition/embedding preprocessing；
- 为什么需要 Ray，而不是数据库内部线程池或普通服务。
