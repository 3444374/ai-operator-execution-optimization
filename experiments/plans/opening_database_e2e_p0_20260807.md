# 开题前统一 database-E2E 文本三臂计划

冻结日期：2026-08-07

状态：**已完成并冻结**。2026-08-07 在 AutoDL 双 RTX 4090 上完成 24/24 单元（2 workload × 3 arm × 1 warmup + 3 formal），18 个 formal 全部 exactly-once、sink digest 匹配，基础设施失败为 0。项目臂在 SQuAD/ShareGPT 的 service feeding 为 89.93%/91.38%，均未过 95% 门；DuckDB AI ShareGPT 有 4,936/6,144 行 cap 语义失败。权威报告见 `experiments/results/opening_database_e2e_text_20260807/README.md`。按预注册停止规则，开题前不再新增 baseline。

## 1. 准入问题

| 问题 | 答案 |
|---|---|
| 支持开题中的哪句话？ | 给出数据库触发后，从 PostgreSQL source 到统一 sink 的三种静态执行路径在均匀与异质 workload 下的可比数据；判断异质 work 是否会放大上游组织差异。 |
| 不运行会缺少什么？ | 现有结果无法支持数据库产品、直接控制和项目静态路径的统一 database-E2E 排名，也没有相同三臂的异质 workload 对照。 |
| 为什么现有正式结果不能回答？ | scale-ramp 的 gate 臂是 request/query-barrier 口径，project 臂的旧 `e2e_s` 还包含模型循环后的证据文件和指标抓取；source、sink、质量和 per-row 合同也未统一。 |

## 2. 冻结系统合同

| 项 | 冻结值 |
|---|---|
| 平台 | AutoDL 双 RTX 4090；实际 PostgreSQL 与 pgvector 版本逐 run 记录，当前预演为 PostgreSQL 18.4 + pgvector 0.8.5，不冒充目标 PG18.3 |
| 模型服务 | 两个独立 vLLM endpoint，各绑定一张 GPU；Qwen2.5-7B；raw Chat Completions；temperature=0；prefix cache ON；`max_num_seqs=256`；`max_num_batched_tokens=8192`；`max_model_len=8192`；`gpu_memory_utilization=0.90` |
| source | 同一 PostgreSQL `documents` 表、同一 `workload_name`、`ORDER BY doc_id`；每个 measured cell 都在 E2E 计时内重新扫描并校验 frozen manifest |
| sink | 同一 PostgreSQL `document_completions`；每个 cell 写入前按本次 doc-id 集合清除旧行，写后执行 count + `(doc_id, completion_text)` digest readback；清理不计入 E2E |
| request set | 两 endpoint 的 immutable JSONL manifest；`equal_rows`，seed=20260807；manifest SHA、sidecar、row count、每 endpoint work 逐 run 记录；三臂必须相同 |
| 并发 | direct 与 DuckDB AI 每 endpoint 32；project 每 endpoint K=32，8 actors × concurrency 4 |
| project static | token budget=6144，active work=65,536/endpoint，request-level，manifest-pinned routing，httpx async，固定 50 ms flush 标签沿用冻结静态合同 |
| 重复 | 每 workload 每 arm 1 warmup + 3 formal；按确定性随机顺序交错执行；warmup 不进 headline |
| headline | `correct_rows / database_e2e_s`；同时报告 raw rows/s、database-E2E、service tokens/s、request latency、TTFT、failure、truncation、GPU、MFU、能耗和 sink 门禁 |

三臂固定命名：

- `direct_static_sharded`：项目实现的 bounded HTTP 静态直接控制；不是数据库产品。
- `duckdb_ai_static_sharded`：experiment harness 先按 manifest 切成两个 shard，再由两个独立 DuckDB AI extension 进程分别拥有本 shard 的 set-oriented 执行。它是 DuckDB AI 组件对照，但不是 DuckDB 原生多 endpoint 能力，不写成“产品原生双卡调度”。
- `project_frozen_static`：项目冻结的 Daft organizer + Ray actor + per-endpoint static K/work credit 路径；本轮不运行任何 adaptive/state-aware 策略。

## 3. P0-1 均匀控制组

- workload：`squad_v11_dev_short_answer` 全量 10,570 行。
- output cap：64。
- output work：manifest 使用 `fixed_cap`，防止真实输出泄漏到执行前分片。
- 质量：SQuAD v1.1 normalized EM、token F1、correct rows、空结果、失败、`finish_reason=length`；DuckDB 若不暴露 finish reason，明确标为 unavailable，不能按 0 处理。
- 目的：建立统一 database-E2E 三臂控制组，不预设性能排序。

## 4. P0-2 异质实验组

- workload：`sharegpt_multiturn` 全量 2,048 行，不再导入新数据。
- 冻结构成：按数据库 `prompt_tokens` 分桶，short `<256` 为 538 行，medium `256..1024` 为 1,175 行，long `>1024` 为 335 行；阈值与自然比例不因结果改变。
- output cap：256，降低 DuckDB AI 因 `finish_reason=length` 返回结构化失败的比例；manifest 的 estimated output 使用 `fixed_cap`，避免未来信息泄漏。该 cap 与控制组不同，因此跨 workload 只比较机制变化，不把绝对吞吐变化归因于 heterogeneity 单因素。
- 必报 workload 描述：prompt、target-output、estimated-work 的 histogram，P50/P95/P99、均值、标准差、CV；三桶行数和比例。
- 质量/有效性：exactly-once、非空完成、failure、finish reason、truncation 可观测性和实际 output-token 分布；该 workload 没有 SQuAD reference，不伪造 EM/F1。
- 追加机制指标：endpoint estimated/observed work imbalance，request P50/P95/P99，TTFT/ITL，cache hit，active work，running/waiting/KV，GPU/MFU 与 energy/correct row。

## 5. 统一 E2E 边界

```text
timer start
  PostgreSQL source scan
  manifest identity/content validation
  arm-owned model execution on two pinned endpoints
  unified PostgreSQL sink write
timer stop
```

timer 之外只允许：runner preflight、endpoint idle、旧 sink 行清理、metrics before snapshot、结果证据 CSV/JSON 写盘、metrics after settle/scrape、sink readback、聚合与画图。project profiler 使用 opt-in clean boundary，在 operator result 完整性检查后立即写 sink并停止计时，再写 trace/evidence 和抓取 after metrics；默认 profiler 时序保持不变。

## 6. 每个 formal cell 的 fail-closed 门禁

1. 两 endpoint 健康、身份和 service flags 与冻结合同一致；cell 前后 idle；同一时刻只有一个 experiment runner。
2. PostgreSQL 扫描行数、doc-id 集合、prompt、token metadata 和 manifest SHA 完全一致。
3. exactly-once，两个 endpoint 都有请求，0 worker/transport failure，sink count/content digest 一致。DuckDB AI 的 cap 语义失败不是基础设施失败，但必须进入总行数和 correct rows/s 分母并单独报告，不能删除。
4. `gpu_utilization_pct_mean`、running/waiting/KV 使用 during-cell time series；不用单点 `gpu_utilization_pct` 下结论。
5. feeding-saturation：E2E service tokens/s 至少达到同协议 bounded control 的 95%。若 DuckDB 的 query-barrier 不能提供 per-request finish reason/latency，字段标 unavailable，不虚填 0。
6. 三个 formal repeat 的 CV 和离群点透明报告；不因某臂表现差而删 run。服务崩溃、计时边界不一致、manifest/sink 门禁失败的 cell 丢弃并原配置重跑，保留 incident。

## 7. 输出与停止规则

- 结果目录：`experiments/results/opening_database_e2e_text_20260807/{README.md,raw/}`。
- `raw/` 保存在服务器 artifact root；Git 仅提交去敏、必要、体积受控的正式汇总和重建图表所需证据。
- 报告按项目八段结构：目的、设置、合规自检、设计、全组件数据、解释、课题含义、下一步。
- 两组完成后立即停止开题 baseline。小于 5% 的差异是有效结果，不触发第二数据库、更多文本引擎、更多 workload、模型替换或 scale × concurrency 扫描。
