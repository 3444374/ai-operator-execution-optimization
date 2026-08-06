# 多卡并发扫描（2×4090，SQuAD 2048，cap=64，c=1..64 完整曲线，1 rep/cell）

> **定位**：bounded_http / duckdb_ai / project_static 三臂在固定规模 2048 下，从 **C_total=2（c/K=1）到 C_total=128（c=64）的完整并发曲线**，回答"上游并发如何喂饱 GPU、各臂形态如何"。**这是 1 rep/cell 的 diagnostic screening，不是 formal ranking**（无 TOST/equivalence margin/CV，"未检出差异"≠"证明等价"）。
>
> **身份（订正）**：`duckdb_ai` 是测试 harness 预切 manifest + 2 个独立 DuckDB 进程（DuckDB `ai` 单 BASE_URL），按 [协议 §2.6](../../../plans/bounded_output_duckdb_comparison_protocol_20260805.md) 标 **`harness_pre_split_diagnostic`**，scheduler owner = 实验 harness，**不进 DuckDB 产品原生主排名**。lb_rr 臂（单进程经 nginx）未纳入本跑，见 §7。
>
> **duckdb 修复验证**：首跑（base conda，DuckDB 1.5.5）duckdb 全 7 格失败（ai extension 在 v1.5.4 路径不匹配）；本跑切 **text-baselines venv（DuckDB 1.5.4 + ai extension 0.4.14）** 后 duckdb 全 passed。deploy README:1175 规定 duckdb 必须用固定 1.5.4 的独立解释器。

## 1. 实验目的

**问题**：固定规模 2048，从低并发（C_total=2）扫到高并发（C_total=128），三臂的 service tokens/s 如何随并发变化？峰值/饱和/塌陷点在哪？形态是否一致？

**关系**：规模爬坡（Phase 1a）已确定 2048 是吞吐峰值区（中段 cache-hot，大段坍塌）；本扫描固定在峰值规模，扫并发维度，补全"从 0 开始的完整并发曲线"。

## 2. 实验设置

| 项 | 值 |
|---|---|
| 平台 | AutoDL 2×RTX 4090；vLLM 0.25.1 V1 engine，双 endpoint 8000(GPU0)/8001(GPU1)，qwen2.5-7b，max_model_len 8192，gpu_mem_util 0.90 |
| **vLLM effective config（诚实）** | 启动 cmdline **只有** `--model/--max-model-len 8192/--gpu-memory-utilization 0.90/--port`；**无** `--max-num-seqs/--max-num-batched-tokens/--enable-prefix-caching` flag → vLLM 用**默认值**。EngineCore 日志确认 `enable_prefix_caching=True, enable_chunked_prefill=True`（默认 ON）。**所以下表的 `service_max_num_seqs=256/8192` 是 adapter 声明值，非 vLLM effective**（effective = vllm 0.25.1 默认）；prefix_caching 默认 ON 恰与声明一致。ramp driver 已加 `_verify_vllm_config` preflight，未来 run 会对声明≠effective 发 WARN。 |
| workload | SQuAD v1.1 dev short-answer，**2048 行**（cap=64, temp=0, chat_completions）；cap=64 门禁已证模型最大输出 57<64 |
| 分片 | equal_rows（2 endpoint，各 1024） |
| 并发轴 | bounded_http/duckdb_ai: c∈{1,2,4,8,16,32,64}（per-endpoint，C_total=2c）；project_static: K∈{1,2,4,8,16,32}（per-endpoint max_inflight，C_total=2K，受 8 actors×4=32 slots 限制无 K64） |
| 重复 | **1 rep/cell**（diagnostic；formal 需 1w+3f） |
| 驱动 | `code/scripts/baselines/multicard_scale_ramp.py`（concurrency-sweep，每格 cache-hot warmup，text-baselines venv） |

## 3. 合规性自检

- **prefix cache**：bounded/duckdb **0.96**；**project 随 K 逐步升温** K1 0.91→K32 0.96（复审 #3：project 按自有 round-robin 路由，不共享 gate manifest 的 endpoint 分配，低 K 时 cache 未完全热——warmup 按 manifest 分片预热但 project 路由独立，prompt 可能落另一 GPU 独立 prefix cache）。
- **feeding-saturation**：duckdb/project 在 C_total=64 达 bounded 的 ~87-89%（<95% 门禁，同 saturated/rich 报告，未达 formal feeding-parity）。
- **GPU**：bounded 80-97%、project 60-94%、duckdb **~69%**（未满但 tok/s 已 plateau → duckdb 瓶颈不在 GPU，见 §6）。
- **exactly-once + 0 error**：passed cell 全通过（duckdb c1-c64 全 passed；bounded c1-c32 passed；project K1-K32 passed）。

## 4. 实验数据（完整并发曲线，service tokens/s）

| C_total | bounded_http (c) | duckdb_ai (c)〔harness_pre_split_diagnostic〕| project_static (K) |
|---|---|---|---|
| 2 | 4,277 (c1) | 77,862 (c1) | 4,226 (K1) |
| 4 | 7,642 (c2) | **79,088** ←峰 (c2) | 7,371 (K2) |
| 8 | 14,838 (c4) | 77,032 (c4) | 14,072 (K4) |
| 16 | 28,410 (c8) | 79,161 (c8) | 27,023 (K8) |
| 32 | 51,287 (c16) | 78,933 (c16) | 48,164 (K16) |
| **64** | **87,393** ←峰 (c32) | 76,449 (c32) | **77,381** ←峰 (K32) |
| 128 | **failed** (c64) | 78,817 (c64) | — (actor slots=32) |

> **group 口径**（复审 #2）：bounded/duckdb = gate.json `group_service_total_tokens / group_service_wall_s`；project = profiler `model_request_tokens_per_s`（本身 group 语义）。旧 `total_tokens/max_jct` 口径高估 gate 臂（bounded 88493 / duckdb 79070 → group 87393 / 76449），且把 duckdb 排在 project 之前（误）；group 口径下 **project 77,381 > duckdb 76,449**。

TTFT P50：c/K=1 约 30ms → c/K=32 约 52-57ms（随并发轻微恶化）。GPU util / rows/s / E2E 见 `ramp_aggregate.md`。

## 5. 结果解释（事实 / 推断 / 不能声称）

**事实**：
- **bounded/project：client inflight 线性喂饱**——tok/s 随 c/K 近线性增长（4k→87k / 4k→77k，group 口径），C_total=64 见顶。两者同形态，project 略低（K32 77k ≈ bounded c32 87k 的 89%，调度开销，rich 报告定位 35-37%）。
- **duckdb：c 轴无关——c=1（C_total=2）就 78k plateau**，c1-c64 全在 78-79k。这是 DuckDB-ai 的 **set-oriented SQL 语义**：client concurrency=1 但 SQL 一次集合提交多行，内部 batch 即使 c=1 也喂饱 GPU。
- **bounded c64（C_total=128）failed**（shard exit 2）——两 venv 都失败，是 vLLM 过载塌陷（与 saturated ADDENDUM 的 c=64 塌陷一致），非 venv 问题。duckdb c64 反而 passed（78k，因 c 无关）。
- **C_total=64 三臂排序（group 口径）**：**bounded 87,393 > project 77,381 > duckdb 76,449**（旧 total/max_jct 口径误排 bounded>duckdb>project；group 口径下 project 略高于 duckdb harness 诊断）。

**推断**：
- 上游调度（bounded/project）的并发控制有意义（线性喂饱到 c32/K32）；DuckDB-ai 的集合语义绕过了 client 并发（c 无关）。**所以 duckdb 的 c 横轴与 bounded/project 不同语义，不能直接横比 c**——这是它该标 `harness_pre_split_diagnostic` 的又一证据。
- duckdb GPU ~69% 未满但 tok/s plateau → 瓶颈在 DuckDB 进程内部（SQL 执行/网络/set 聚合），不在 GPU。

**不能声称**：
- 三臂"统计等价"（1 rep/cell，无 TOST；"未检出差异"≠"证明等价"）。
- duckdb c 无关归因为"确定机制"（set-oriented 是推断，未 deep-profile DuckDB 内部 batching）。
- bounded c64 失败的精确根因（仅观察到 shard exit 2 + 与 saturated c=64 塌陷一致；未抓 stderr 定位）。
- vLLM effective max_num_seqs 的具体值（cmdline 无 flag，用默认；未从 EngineCore 日志提取确切默认值）。

## 6. 对课题含义

- **完整并发曲线拿到**（用户目标）：bounded/project 线性到 C_total=64，duckdb c 无关 plateau，c64 塌陷。曲线形态支持"上游并发控制对 bounded/project 有效、对 set-oriented DuckDB 无意义"。
- **duckdb 修复闭环**：text-baselines venv（1.5.4）解决 ai extension 路径问题，duckdb 恢复可跑。
- **后续 formal**：1w+3f + clean-Ray + 原子落盘 + vLLM config 验证（driver 已加 preflight）后，在 C_total=64（峰值）做 formal 排名。

## 7. 下一步

1. **lb_rr 并发臂 + 规模爬坡（Phase 1b）**：lb_rr 单进程经 nginx，C_total={2..128} + 规模 64→10570。前置 256 行正确性门禁。
2. **bounded c64 根因**：抓 shard stderr 定位（连接/句柄/vLLM 过载）。
3. **formal 1w+3f**：在 C_total=64（峰值）三臂 + lb_rr，clean-Ray + 原子落盘 + vLLM 带 flag。
4. **duckdb 内部 batching profile**：验证 set-oriented 归因（DuckDB SQL 如何 batch 多行）。

## 8. 证据 + 诚实边界

- **raw**：已提交裁剪版（commit 6f6ef75：`ramp_run.json` + 每 cell `gate_output/<cell>/{shard_0,shard_1}/summary.json` + `gate.json` + `ttft_metrics.json` + `gpu_resource.csv` + `service_counters.json`；`requests.csv` 排除）。可由 committed aggregator 完整复算。
- `ramp_aggregate.{json,md}`：committed aggregator 重算（**group 口径 + rows fallback + identity sidecar**）。
- 代码：`multicard_scale_ramp.py`（concurrency-sweep + warmup fail-closed + 单端点 warmup + 原子 ramp_run + clean-Ray + vLLM config preflight + lb_rr backend-balance gate）。

> **诚实边界**：**1 rep/cell diagnostic**（非 formal，无 TOST/CV）；**duckdb/lb_rr = harness_pre_split_diagnostic**（非产品原生排名）；**group 口径**（gate 臂 gate.json group_service_wall_s；旧 total/max_jct 已弃）；**project prefix-hit 随 K 升温**（K1 0.91→K32 0.96，非全 0.96，project 路由独立于 manifest）；**vLLM effective config = 默认**（cmdline 无 max_num_seqs/8192 flag）；**bounded c64 failed**（C_total=128 过载）；**未达 feeding-parity ≥95% 门禁**（duckdb/project ~87-89%）。
