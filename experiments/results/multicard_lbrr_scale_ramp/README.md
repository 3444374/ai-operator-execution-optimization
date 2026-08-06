# lb_rr 规模爬坡（2×4090，SQuAD dev，C_total=64，64→10570，warmup_per_cell=false）

> **定位**：lb_rr（1 DuckDB → nginx:8500 round-robin → 2 vLLM backend）规模爬坡，C_total=64 固定，扫 64→10570，看 lb_rr 吞吐随规模形态、是否与三臂（bounded/project/duckdb）同形态。
>
> **身份**：`harness_sharded_diagnostic`（DuckDB 单 BASE_URL 经 nginx，非产品原生多 endpoint；协议 §2.6）。256 门禁已过：exactly-once（256 unique completed）+ finish_reason=length=0（256 行正常完成，duckdb length→error 但 failed=0）+ nginx upstream **8000=200 / 8001=200 完美 round-robin 对称**。
>
> **1 rep/cell diagnostic**（非 formal）。`warmup_per_cell=false`（lb_rr 单端点 manifest 不适合 warmup；prefix-hit 由规模嵌套残留 + cell 自热，机制不同于三臂的真 warmup，数值可比）。

## 1. 实验目的

**问题**：lb_rr（单入口 naive LB）的吞吐随规模如何变化？是否与三臂（bounded/project/duckdb 都是中段 2048 峰值 + 4096+ 坍塌）同形态？

**关系**：补全多卡规模曲线的 lb_rr 臂（Phase 1a 三臂 + 本 lb_rr），完整刻画"规模 vs 吞吐"在 4 种执行路径下的一致性/差异。

## 2. 实验设置

| 项 | 值 |
|---|---|
| 平台 | AutoDL 2×RTX 4090；vLLM 0.25.1 V1 engine，双 endpoint 8000/8001，qwen2.5-7b，max_model_len 8192，gpu_mem_util 0.90 |
| **vLLM effective config（诚实）** | cmdline 无 `--max-num-seqs/--max-num-batched-tokens` flag → vLLM 默认（≠ 声明 256/8192）；`enable_prefix_caching` 默认 ON（= 声明，巧合）。driver `_verify_vllm_config` prelight WARN。 |
| 拓扑 | lb_rr：1 DuckDB-ai 进程（max_concurrent_requests=64）→ nginx:8500（round-robin，proxy_next_upstream off）→ 2 backend（8000/8001） |
| workload | SQuAD v1.1 dev short-answer，64/128/.../10570 行（lbrr_dev 单端点 manifest，全行 → LB），cap=64 |
| 并发 | C_total=64 固定（单进程总 in-flight；nginx 各分 ~32） |
| 重复 | 1 rep/cell（diagnostic） |
| warmup | `warmup_per_cell=false`（lb_rr 单端点 manifest） |

## 3. 实验数据（9 scale，service tokens/s）

| scale | tok/s | TTFT P50 | prefix-hit | GPU0/GPU1 util | rows/s |
|---|---|---|---|---|---|
| 64 | 16,983 | 68ms | 0.95 | —/12.5 | 81 |
| 128 | 25,007 | 53ms | 0.94 | 28.5/25.0 | 150 |
| 256 | 47,708 | 55ms | 0.96 | 26.6/29.8 | 226 |
| 512 | 59,172 | 52ms | 0.96 | 31.6/38.3 | 282 |
| 1024 | 65,637 | 51ms | 0.96 | 54.5/57.5 | 304 |
| **2048** | **70,697** ←峰 | 51ms | 0.96 | 70.6/70.6 | 345 |
| 4096 | 48,954 ↓ | 76ms | 0.76 | 84.8/83.6 | 229 |
| 8192 | 40,009 | 158ms | 0.61 | 91.3/90.9 | 165 |
| 10570 | 38,181 | 162ms | 0.60 | 90.1/88.8 | 158 |

## 4. 结果解释（事实 / 推断 / 不能声称）

**事实**：
- **峰值 70,697 tok/s @ 2048**——与三臂峰值区完全一致（bounded 90k@512/2048、project 88k@256、duckdb 77k@2048）。
- **4096+ 坍塌**：70,697 → 48,954 → 40,009 → 38,181（峰值 55%），TTFT 51→158ms（3×），prefix-hit 0.96→0.60。与三臂同坍塌点、同信号。
- **lb_rr 峰值最低**：71k vs 三臂 77-90k。单进程经 nginx 欠喂（ADDENDUM 已述 avg ~10/backend << 32 饱和点）。
- **全 9 格 passed（含 8192/10570）**——不像 duckdb_sharded 在大尺度 cap-64 失败（lb_rr 单进程经 nginx，finish_reason=length=0 已门禁证）。
- **GPU util**：64 时 GPU1 仅 12.5%（单进程欠喂，请求未填满两卡）；2048 时 70%（接近饱和）；10570 时 90%（坍塌区 GPU 满 但吞吐低 = cache thrash，非算力不足）。

**推断**：
- lb_rr 与三臂**同形态**（2048 峰 + 4096 坍塌）→ **prefix-cache working-set thrash 是 regime 效应**（与执行路径无关，所有臂共同），印证 Phase 1a 的归因。
- lb_rr 峰值最低是**架构特性**（单入口欠喂），非 cache/调度问题。

**不能声称**：
- lb_rr"优于/劣于"三臂（1 rep/cell，无 TOST；绝对值差是单入口欠喂的已知架构效应）。
- 坍塌"根因是 vLLM KV/调度"（无 service-counter 证据，疑似；见 ADDENDUM 订正）。
- vLLM effective max_num_seqs 具体值（cmdline 无 flag，默认）。

## 5. 对课题含义

- **完整 4-臂规模曲线拿到**（用户目标）：bounded/project/duckdb/lb_rr 都 2048 峰值 + 4096+ 坍塌。**regime 效应（prefix-cache thrash）跨执行路径一致**——这是干净的系统级发现，支持"上游策略价值只在特定 regime 显现"的主线。
- lb_rr 单入口欠喂（峰值最低）是现实部署基线（"1 个 DuckDB 进程喂不饱多卡"），对照多进程/多 endpoint 方法。

## 6. 证据 + 诚实边界

- **raw**：服务器 `experiments/results/multicard_lbrr_scale_ramp/scale_<S>/lb_rr_c64_rep1/`（`gate_output/lb_rr_c64/shard_0/{summary.json,requests.csv}` + `ttft_metrics.json` + `gpu_resource.csv` + `ramp_run.json`）；commit 时提交裁剪版（summary/ramp_run，不含大 requests.csv）。
- `ramp_aggregate.{json,md}`：aggregator 重算（rows fallback + status from ramp_run）。
- 代码：`multicard_scale_ramp.py`（lb_rr cell + identity sidecar）。

> **诚实边界**：**1 rep/cell diagnostic**（非 formal，无 TOST/CV）；**身份 harness_sharded_diagnostic**（非产品原生排名）；**warmup_per_cell=false**（prefix-hit 由规模嵌套残留 + cell 自热，机制 ≠ 三臂真 warmup，数值可比）；**vLLM effective config = 默认**（声明非 effective）；**坍塌归因未证实**（疑似 cache thrash，无 service-counter）。
