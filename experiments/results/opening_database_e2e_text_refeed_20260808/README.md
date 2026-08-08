# 统一文本 database-E2E correctness 护栏与静态配置诊断

日期：2026-08-08
状态：**correctness / database-E2E 护栏通过；ShareGPT 性能排名被后续 C32–C256 饱和校准降级**

## 1. 实验目的

在同一 PostgreSQL source/sink、同一模型服务和同一 immutable manifest 下，比较三条静态执行路径：

- `direct_static_sharded`：有界 HTTP 静态直连控制；
- `duckdb_ai_static_sharded`：DuckDB AI 产品路径；
- `project_frozen_static`：项目冻结静态 Daft organizer + Ray actor 路径。

本次先按 workload 校准并冻结项目臂 `K=128`，纠正首轮项目臂未喂饱 vLLM 的问题。问题不是“项目必须胜出”，而是：在服务容量被充分利用后，均匀与异质 workload 是否呈现相同的静态路径差异，以及产品语义是否改变正确吞吐。

## 2. 实验设置

| 项目 | 冻结合同 |
|---|---|
| GPU / 服务 | 2× RTX 4090；2 个 Qwen2.5-7B vLLM endpoint；prefix cache ON |
| vLLM | `max_num_seqs=256`、`max_num_batched_tokens=8192`、`max_model_len=8192`、`gpu_memory_utilization=0.90` |
| 数据库 | PostgreSQL 18.4、pgvector 0.8.5；该结果是 AutoDL rehearsal，不冒充目标 PG 18.3 平台结论 |
| DuckDB | v1.5.4；community `ai` extension 0.4.14 |
| workload | SQuAD uniform 10,570 行、cap 64；ShareGPT controlled-skew 2,048 行、cap 256 |
| direct / DuckDB | 每 endpoint 并发 32，静态等行分片 |
| project | 每 endpoint `K=128`、`W=65,536`、8 workers×32 actor concurrency；正式运行不在线调参 |
| 重复 | 每 workload×arm 1 warm-up + 3 交错 formal，共 24 cells、18 formal |
| 主边界 | PostgreSQL scan → 完整模型结果 → PostgreSQL sink readback 的外部 database-E2E |

SQuAD manifest SHA256 为 `0543b29e…f6b19b`；ShareGPT 为 `54c97a2f…83169b`。完整签名和校准合同 SHA 见 [`run_summary.json`](run_summary.json)。

## 3. 合规性自检

| 门禁 | 结果 |
|---|---|
| cell / formal 数 | 24/24、18/18，全部 `passed` |
| exactly-once / sink readback | 全部通过 |
| manifest、PG/pgvector identity | 全部一致 |
| 基础设施失败 | 0 |
| project/direct service ratio | SQuAD 100.87%；ShareGPT 154.57%；这只是相对 C32 direct 的比值，不再作为 feeding 通过判据 |
| project GPU utilization mean ≥80% | SQuAD 88.39%；ShareGPT 97.19%，均通过 |
| 稳定性 | 六组 correct rows/s CV 均 ≤1.75% |

24/24 cell 的 correctness、sink、identity 与稳定性结论有效。后续独立 ShareGPT bounded calibration 发现 C32 仅为已测峰值的 52.07%，故 ShareGPT 三臂不处于 matched-saturation regime，不能用于项目/direct 性能排名。详见 `opening_bounded_saturation_calibration_20260808/`。

## 4. 实验数据

### 4.1 Headline：三次 formal 均值

| workload | 路径 | DB-E2E (s) | raw rows/s | correct rows/s | service tok/s | service ratio vs C32 direct | cap 语义失败 |
|---|---|---:|---:|---:|---:|---:|---:|
| SQuAD | direct | 62.126 | 170.139 | 136.632 | 40,920.72 | 100.00% | 0/31,710 |
| SQuAD | DuckDB AI | 62.074 | 170.280 | 136.675 | 40,955.99 | 100.09% | 3/31,710 |
| SQuAD | project frozen-static | 61.601 | 171.623 | 137.770 | 41,277.95 | 100.87% | 0/31,710 |
| ShareGPT | direct | 180.332 | 11.357 | 11.357 | 9,425.25 | 100.00% | 0/6,144 |
| ShareGPT | DuckDB AI | 180.416 | 11.352 | 2.260 | 9,421.31 | 99.96% | 4,921/6,144 |
| ShareGPT | project frozen-static | 116.703 | 17.550 | 17.550 | 14,568.91 | 154.57% | 0/6,144 |

SQuAD 的三臂 EM/F1 接近：direct 80.306/89.381，DuckDB AI 80.265/89.359，project 80.274/89.368。ShareGPT direct/project 均为 2,048/2,048 成功非空；DuckDB AI 平均仅 407.67/2,048 行满足当前 fixed-cap 产品语义。

### 4.2 GPU、MFU、能耗与服务状态

`MFU` 为 0–1 分数。project 原始 profiler 未注入 GPU peak，汇总器按每张 4090 BF16 165 TFLOPS 和 estimated-FLOPs counter 恢复；direct/DuckDB 保留 runner 记录的双 GPU 聚合 MFU。该指标用于资源利用解释，不替代 feeding 门。

| workload | 路径 | GPU util mean | MFU | mean power (W, 两卡和) | J / correct row | running mean / max | waiting mean / max | KV mean / max |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| SQuAD | direct | 97.64% | 0.659 | 861.2 | 6.303 | 45.62 / 59.67 | 0.00 / 0.00 | 0.029 / 0.062 |
| SQuAD | DuckDB AI | 94.85% | 0.660 | 844.4 | 6.178 | 44.80 / 58.00 | 0.00 / 0.00 | 0.029 / 0.066 |
| SQuAD | project | 88.39% | 0.741 | 792.9 | 5.731 | 134.51 / 242.67 | 0.13 / 9.67 | 0.088 / 0.164 |
| ShareGPT | direct | 98.09% | 0.302 | 787.5 | 69.342 | 61.44 / 64.00 | 0.01 / 1.67 | 0.163 / 0.236 |
| ShareGPT | DuckDB AI | 98.02% | 0.301 | 784.5 | 347.210 | 61.22 / 64.00 | 0.01 / 3.67 | 0.162 / 0.236 |
| ShareGPT | project | 97.19% | 0.478 | 843.9 | 47.942 | 126.19 / 171.33 | 2.31 / 67.67 | 0.348 / 0.487 |

能耗按 GPU sampler 积分，未包含整机 CPU/DRAM。缺少冻结云单价，因此不伪造 `$ / M tokens`；如后续需要成本比较，应在同一计费合同下补算。

### 4.3 单次 formal 与完整字段

18 个 formal 的单次记录、P50/P95/P99、TTFT/ITL、prompt/generation token、endpoint skew、质量与资源字段位于 [`formal_runs.csv`](summary/formal_runs.csv)；三次均值、中位数、标准差和 CV 位于 [`formal_summary.csv`](summary/formal_summary.csv)。[`audit.json`](summary/audit.json) 保存全部门禁复算。

project 的 request P99 来自项目 profiler，而 direct/DuckDB 的 request latency 以各自静态 shard barrier 记录，边界不同，**不得直接横向排名**。本报告只以统一 DB-E2E、service throughput、correct throughput 与服务资源指标作主比较。

## 5. 结果解释

### 事实

1. K128 replacement 提高了项目臂供给，但“达到 C32 direct 的 95%”不是可靠的饱和门。后续扫描证明 ShareGPT C32 仍明显欠供给。
2. SQuAD 下三臂 correct throughput 和 DB-E2E 接近，项目相对 direct 的 service throughput 仅 +0.87%。
3. ShareGPT 下 project frozen-static 相对 **C32、欠供给的** direct service throughput 为 1.546×，DB-E2E 从 180.33 s 降到 116.70 s；该差异由并发/执行结构混淆，不能作为方法收益。
4. DuckDB AI 在 ShareGPT 的 raw rows/s、service tok/s、GPU util 与 direct 几乎相同，但 4,921/6,144 行触发 fixed-cap 产品语义失败，correct throughput 因此降到 2.26 rows/s。

### 推断

- 静态配置的表现是 workload/regime-dependent：相同 C32 对短输出可能足够，对 ShareGPT 只达到已测峰值的约一半；必须先做 workload-specific saturation calibration。
- DuckDB ShareGPT 结果首先暴露的是产品输出语义边界，而不是“DuckDB 喂不饱 vLLM”或“DuckDB 很慢”。

### 不能声称

- 不能把 `project_frozen_static` 的 ShareGPT 优势写成 state-aware、动态调度或 WorkDescriptor 的独立因果收益；该臂与 direct 的执行结构整体不同。
- 不能声称项目路径普遍优于 DuckDB、Daft 或 Ray Data；SQuAD 结果近似中性，Daft/Ray Data 正式同环境矩阵尚未完成。
- 不能把 PG 18.4 AutoDL rehearsal 外推为目标 PG 18.3 平台的绝对结论。

## 6. 对课题的含义

- **数据组织 / Work Unit**：同一冻结静态点在 SQuAD 与 ShareGPT 的结果差异很大，说明组织策略必须绑定 work 分布和 serving regime；但 WorkDescriptor 的字段必要性仍主要由固定行 token 14.3× 与图像分阶段画像直接支撑。
- **状态感知**：本实验验证 running/waiting/KV/GPU/MFU 可以在统一链路中持续观测，但没有排除 ShareGPT direct 欠供给；后续 C32–C256 扫描才给出欠供给/平台/过量排队三段证据。
- **动态调度**：项目 K128/W65,536 仍是项目内冻结静态点；跨执行路径正式比较改用 bounded C128。动态策略只有在同上限、明确负载变化的 A/B 中达到预注册门槛，才能晋级。
- **算子代价估计**：raw、correct 与 service throughput 的分离说明选择目标必须纳入 work、语义失败和决策 regret；代价估计本身的选择质量由独立 429-formal context-LOO 实验支撑。

## 7. 下一步与停止规则

1. 不再重跑本三臂 database-E2E，也不更换 workload、模型、数据库或 K 追求更大差异。
2. bounded C128、Daft Native、Daft Ray、Ray Data 原生单 job 1+3 已完成，见 `opening_text_native_single_job_formal_20260808/`。
3. 当前只完成原生 short/long 两 job 错峰观察，以及项目 `static_partition` vs `shared_work_credit` 同上限因果 A/B；报告 per-job JCT、服务状态、GPU/MFU、fairness 和 exactly-once。
4. 若差异不足 5%或为负，按阴性结果停止，不扩扫 offset/weight 追正。

## 8. 证据与复现

- 聚合结论：[`headline_summary.json`](summary/headline_summary.json)
- 正式门禁：[`audit.json`](summary/audit.json)
- 运行合同：[`run_summary.json`](run_summary.json)
- 机器与组件 preflight：[`preflight.json`](preflight.json)
- 原始 request/submission/resource 证据：保存在开发者本地磁盘与 AutoDL 的 `/root/autodl-tmp/experiment-artifacts/opening_database_e2e_text_refeed_20260808_retry1/`，不纳入 Git。
