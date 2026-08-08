# 开题统一文本 database-E2E 三臂正式实验（2026-08-07）

> 结论先行：两类 workload 的 24 个单元全部完成，18 个 formal 均通过 source、exactly-once 与 sink digest 门禁，基础设施失败为 0。项目冻结静态臂在 SQuAD 和 ShareGPT 的 service tokens/s 仅为同 workload direct 的 89.93% 和 91.38%，均未过预注册的 95% feeding-saturation 门，因此本实验不能支持项目路径的性能优势。DuckDB AI 的模型服务吞吐与 direct 接近，但在 ShareGPT 固定 256-token cap 下三次 formal 共 4,936/6,144 行返回产品层 cap 语义失败；这是语义兼容边界，不是基础设施失败，也不能从 correct throughput 分母中删除。

## 1. 实验目的

在相同 PostgreSQL source、immutable manifest、双 vLLM endpoint 和统一 PostgreSQL sink 下，对比三条静态执行路径：bounded HTTP 直接控制、DuckDB AI static-sharded 和项目 Daft organizer + Ray actor frozen-static。SQuAD 是短输出均匀控制组；ShareGPT controlled-skew 检验 prompt/output work 方差增大后，上游路径差异是否稳定放大。

本轮只闭合开题 Claim Matrix 中最后一个统一比较缺口，不预设项目胜出。两类 workload 完成后停止增加开题 baseline。

## 2. 实验设置

| 项 | 冻结值 |
|---|---|
| 平台 | AutoDL，2×RTX 4090；本结果是 rehearsal，不冒充目标内部平台 |
| 数据库 | PostgreSQL 18.4，pgvector 0.8.5 |
| 模型服务 | 2×Qwen2.5-7B vLLM endpoint；prefix cache ON；`max_num_seqs=256`；`max_num_batched_tokens=8192`；`max_model_len=8192`；`gpu_memory_utilization=0.90` |
| source / sink | 同一 PostgreSQL source scan；同一 `document_completions` sink；cell 后 count + `(doc_id, completion_text)` digest 回读 |
| 三臂 | `direct_static_sharded`；`duckdb_ai_static_sharded`（harness 预切两 shard，各由独立 DuckDB AI 进程执行）；`project_frozen_static` |
| 并发 | direct / DuckDB 每 endpoint 32；project 每 endpoint 8 actor × concurrency 4，K=32 |
| project 静态合同 | token budget 6,144；active work 65,536/endpoint；request-level release；manifest-pinned routing；httpx async；固定 50 ms flush 标签 |
| 重复 | 每 workload、每 arm：1 warmup + 3 formal；formal 顺序确定性随机交错 |
| 统一 E2E | PostgreSQL scan → manifest 校验 → arm-owned execution → PostgreSQL sink write；旧 sink 清理、metrics scrape 与证据写盘不计入 |
| 代码版本 | `ddcb7c338b176ef406c438942e5dbddb69b8ee9f`；row-limit smoke 修复不影响本轮全量正式合同 |

Workload 合同：

| workload | 行数 | cap | prompt tokens mean / CV | estimated work P50 / P95 / P99 | endpoint estimated-work skew |
|---|---:|---:|---:|---:|---:|
| SQuAD uniform | 10,570 | 64 | 241.88 / 0.365 | 287 / 451 / 629 | 0.96% |
| ShareGPT controlled-skew | 2,048 | 256 | 571.76 / 0.696 | 796.5 / 1,509 / 1,664.12 | 1.57% |

ShareGPT 的 short `<256`、medium `256..1024`、long `>1024` 分别为 538、1,175、335 行。完整分布见 `raw/workload_summary.json`。

## 3. 合规性自检

| 门禁 | 结果 |
|---|---|
| 单元与重复合同 | 24/24：2 workload × 3 arm ×（1 warmup + 3 formal） |
| source / manifest | 每 workload 只有一个 manifest SHA；所有臂一致 |
| exactly-once / sink readback | 24/24 通过；sink count 与内容 digest 全匹配 |
| 基础设施失败 | 0 |
| GPU feeding 辅助门 | 六组 formal 的 GPU utilization mean 均 ≥90.03% |
| service feeding 主门 | direct 与 DuckDB 均通过；project：SQuAD 89.93%、ShareGPT 91.38%，均未过 95% |
| 稳定性 | correct rows/s CV：SQuAD direct 4.79%，其余五组 0.04%–1.02% |
| 产品语义失败 | DuckDB AI：SQuAD 3/31,710；ShareGPT 4,936/6,144；均保留在 correct throughput 分母 |

审计详情见 `raw/audit.json`。`all_feeding_service_token_gates_passed=false` 是科学结论资格标志，不是 runner 失败；因此 matrix status 仍为 passed。

## 4. 实验设计

SQuAD 固定短答案和 64-token cap，作为服务层容易吸收上游差异的控制组。ShareGPT 不重新采样，直接使用数据库中冻结的 2,048 行自然混合 workload，以更高 prompt 与 target-output 方差形成受控异质组。两组只改变 workload 与 cap；三臂 source/sink、endpoint、模型服务 flags、并发和随机种子保持冻结。

Headline 是 `correct_rows / database_e2e_s`，同时报告 raw rows/s 和 vLLM counter 口径的 service tokens/s。DuckDB AI 的 fixed-cap 产品语义失败从 correct rows 中扣除，但其已经消耗的模型服务 token 不被隐藏。

## 5. 实验数据

### 5.1 吞吐、E2E 与质量（formal mean，n=3）

| workload | arm | database-E2E (s) | raw rows/s | correct rows/s | correct CV | service tok/s | feeding vs direct | 质量 / 有效输出 | cap 语义失败 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| SQuAD | direct | 65.41 | 161.85 | 129.85 | 4.79% | 38,927.70 | 100.00% | EM 80.23%；F1 89.35% | 0 |
| SQuAD | DuckDB AI | 62.52 | 169.07 | 135.71 | 0.56% | 40,663.54 | 104.46% | EM 80.27%；F1 89.38% | 3 |
| SQuAD | project | 72.63 | 145.54 | 116.88 | 0.80% | 35,006.05 | **89.93%（未过门）** | EM 80.31%；F1 89.37% | 0 |
| ShareGPT | direct | 180.60 | 11.34 | 11.34 | 0.04% | 9,412.74 | 100.00% | 2,048/2,048 非空 | 0 |
| ShareGPT | DuckDB AI | 180.61 | 11.34 | 2.23 | 1.02% | 9,411.76 | 99.99% | 402.67/2,048 非空（均值） | 4,936 |
| ShareGPT | project | 197.60 | 10.36 | 10.36 | 0.11% | 8,601.29 | **91.38%（未过门）** | 2,048/2,048 非空 | 0 |

SQuAD 的 raw rows/s 高于 correct rows/s，是因为 `correct_rows` 同时要求任务质量，不等于 runner 成功行。ShareGPT 没有 reference answer，因此只审计成功、非空、cap 语义与 exactly-once，不伪造 EM/F1。

### 5.2 模型服务与资源（formal mean，n=3）

| workload | arm | GPU util | running mean / max | waiting mean / max | KV mean / max | prefix hit | TTFT P95 (s) | ITL P95 (s) | MFU | energy / correct row (J) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SQuAD | direct | 97.37% | 43.00 / 57.00 | 0 / 0 | 0.028 / 0.068 | 0.613 | 0.244 | 0.098 | 0.627 | 6.57 |
| SQuAD | DuckDB AI | 94.50% | 44.49 / 56.67 | 0 / 0 | 0.028 / 0.062 | 0.613 | 0.245 | 0.088 | 0.655 | 6.19 |
| SQuAD | project | 90.03% | 35.72 / 60.33 | 0 / 0 | 0.026 / 0.071 | 0.613 | 0.246 | 0.092 | 0.625 | 6.77 |
| ShareGPT | direct | 98.05% | 61.28 / 64.00 | 0.011 / 3.67 | 0.163 / 0.237 | 0.320 | 0.684 | 0.058 | 0.301 | 69.23 |
| ShareGPT | DuckDB AI | 97.99% | 61.16 / 64.00 | 0.007 / 2.33 | 0.162 / 0.235 | 0.320 | 0.705 | 0.059 | 0.301 | 351.71 |
| ShareGPT | project | 97.60% | 54.51 / 64.00 | 0.010 / 2.00 | 0.161 / 0.252 | 0.320 | 0.803 | 0.037 | 0.281 | 74.26 |

KV usage 按 0–1 分数读取。project 的 runner 原始 MFU 状态为 `missing_gpu_peak_tflops`，汇总器按预注册的每张 4090 BF16 peak 165 TFLOPS，从 profiler counter 恢复 MFU；baseline 保留 runner 原始两 GPU 聚合口径。

### 5.3 延迟口径边界与成本

DuckDB AI 的 `request_latency` 是 query-barrier 级别；direct 是 client completion span；project 是 profiler 的请求级延迟。三者不能横向解释为相同 per-row latency，因此主表只使用统一 database-E2E、vLLM histogram TTFT/ITL 与资源 time series。完整原始字段保留在 `raw/formal_runs.csv` 和 `raw/formal_summary.csv`。

本轮没有记录可审计的云租赁单价或 API token 单价，因此 `$ / M tokens` 标为 N/A，不根据事后价格假设杜撰。成本代理使用 GPU energy 与 energy/correct row；DuckDB ShareGPT 的 351.71 J/correct-row 主要来自大量 cap 语义失败。

## 6. 结果解释

**事实**：两组 project frozen-static 的 service feeding 均低于 direct，且 correct rows/s 也更低；异质 ShareGPT 没有让项目冻结静态路径反转为优势。SQuAD 三臂 EM/F1 接近，说明性能差异不是通过降低答案质量换得。DuckDB AI 在 ShareGPT 的 raw throughput 和 service tokens/s 与 direct 几乎相同，但约 80.34% formal 行因 fixed-cap 产品语义返回空/失败，correct throughput 因而降到 2.23 rows/s。

**推断**：项目路径在当前冻结静态实现中仍存在上游供给或路径开销，GPU 高利用率并不能替代 service-token feeding 门。ShareGPT 上 direct/project 的 service gap 从 SQuAD 的 10.07% 收窄到 8.62%，但仍未过门，不能据此声称 heterogeneity 带来有效策略增量。DuckDB AI 的主要可用性边界是输出 cap 语义，而非模型服务容量。

**不能声称**：项目路径普遍优于 direct 或 DuckDB AI；异质 workload 已证明 work-aware 方法有效；DuckDB AI “更慢”；query-barrier 与请求级 latency 可以直接比较；PG18.4 rehearsal 等同内部 PG18.3 正式平台。

## 7. 对课题含义

统一三臂结果是负面但有决定性的开题证据：strong static direct 必须继续作为默认性能参照；项目后续 state-aware 方法只有在同 source/sink、同上限并通过 feeding 门后，才能用吞吐、tail、SLO 或 fairness 申请晋级。异质 workload 本身不足以自动制造项目优势，方法必须提供可观测的状态变化与因果机制。产品 baseline 还必须把固定输出上限的语义兼容性纳入正确吞吐，而不能只看 GPU 已完成的 token work。

## 8. 下一步与停止规则

开题前文本 baseline 到此停止：不增加第二数据库，不扩 Daft/Ray Data 全矩阵，不换模型/workload 寻找正结果，也不做 scale × concurrency 大扫描。当前数据只进入四级 Claim Matrix、开题报告的一页统一三臂表和 PPT 的一页边界证据；主叙事仍由 serving capacity、数据组织 regime、图像 matched-resource 与代价模型 decision quality 四张核心图承担。

开题材料冻结后，按项目计划进入 state-aware 请求成形/提交、多 job 和图像 system database-E2E；任何动态候选仍需超过同上限 frozen static，并同时通过 correctness、feeding-saturation 与 stability。

## 证据与复现入口

- `raw/formal_config.json`：冻结运行配置。
- `raw/preflight.json`：数据库、DuckDB AI、Ray 与 vLLM 服务身份。
- `raw/run_summary.json`、`raw/matrix_status.json`：24 单元状态与交错顺序。
- `raw/all_runs.csv`、`raw/formal_runs.csv`：warmup/formal 单次记录。
- `raw/formal_summary.csv`：六组 formal 聚合。
- `raw/audit.json`、`raw/headline_summary.json`：门禁与开题 headline。
- `raw/*manifest.meta.json`、`raw/workload_summary.json`：manifest 和 workload 分布。
- 服务器全量 raw：`/root/autodl-tmp/experiment-artifacts/opening_database_e2e_text_20260807/`；按项目政策不进入 Git。
- 汇总命令：`python code/scripts/analysis/summarize_opening_database_e2e.py --matrix-root <artifact-root> --output <artifact-root>/aggregate`。
