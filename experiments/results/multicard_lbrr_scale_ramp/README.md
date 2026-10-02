# lb_rr 规模爬坡（2×4090，SQuAD dev，C_total=64，64→10570，warmup_per_cell=false）

2026-10-02存储整理：原始文件的路径、字节数和校验值见[恢复清单](raw/storage-manifest.jsonl)，
原始数据与脚本的恢复方式见[文件恢复说明](../../../code/scripts/README.md#实验结果恢复)。直接引用的数据和脚本仍可就地读取。

> **定位**：lb_rr（1 DuckDB → nginx:8500 round-robin → 2 vLLM backend）规模爬坡，C_total=64 固定，扫 64→10570，看 lb_rr 吞吐随规模形态、是否与三臂（bounded/project/duckdb）同形态。
>
> **⚠️ 状态**：`diagnostic_observation_pending_evidence_fix`——本次 lb_rr run 有效（9/9、0 error、均衡分流、2048 观察峰值），但**不引用"跨四臂 clean cache-thrash finding"**（4 臂 cache 控制不统一；见 §5）。
>
> **身份**：`comparison_role=gateway_system_diagnostic`（**主字段 = 系统角色**，协议 §2.6 gateway 完整系统轨：DuckDB 单 BASE_URL 经 nginx 第三方 gateway → 2 vLLM endpoint；system-level only，非 DuckDB 原生，不进 formal；scheduler_owner = duckdb_ai_extension + nginx_round_robin + vllm）+ `component_comparison_role=database_product_native_baseline`（单 shard 组件）。256 门禁：exactly-once（256 unique completed）+ **0 error / 未观察到 max_tokens-truncation**（256 行正常完成，duckdb length→error 但 failed=0；注：requests.csv `finish_reason` 字段空，**空 ≠ 已审计为非 length**，仅"无 length 报错"）+ nginx upstream **8000=200 / 8001=200 完美 round-robin 对称**。
>
> **1 rep/cell diagnostic**（非 formal）。`warmup_per_cell=false`（lb_rr 单端点 manifest 不适合 warmup；prefix-hit 由规模嵌套残留 + cell 自热，机制不同于三臂的真 warmup，**不能 vs 三臂直接比、非 cache-controlled**）。

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

| scale | tok/s（**ttft 口径**） | TTFT P50 | prefix-hit | GPU0/GPU1 util | rows/s |
|---|---|---|---|---|---|
| 64 | 17,452 | 68ms | 0.95 | —/12.5 | 81 |
| 128 | 26,774 | 53ms | 0.94 | 28.5/25.0 | 150 |
| 256 | 50,186 | 55ms | 0.96 | 26.6/29.8 | 226 |
| 512 | 61,658 | 52ms | 0.96 | 31.6/38.3 | 282 |
| 1024 | 68,939 | 51ms | 0.96 | 54.5/57.5 | 304 |
| **2048** | **72,934** ←峰 | 51ms | 0.96 | 70.6/70.6 | 345 |
| 4096 | 50,013 ↓ | 76ms | 0.76 | 84.8/83.6 | 229 |
| 8192 | 39,898 | 158ms | 0.61 | 91.3/90.9 | 165 |
| 10570 | 37,966 | 162ms | 0.60 | 90.1/88.8 | 158 |

> **ttft 口径**（复审 #5）：lb_rr shard summary 无 `service_total_tokens_delta`（duckdb 输入 token 估算漏 generation/chat-template），改用 ttft_metrics 两后端 `Σ(vllm_prompt_tokens_delta + vllm_generation_tokens_delta) / shard wall`。旧 summary 口径低估（例 256: 47708 → ttft 50186，+5.2%）。

## 4. 结果解释（事实 / 推断 / 不能声称）

**事实**：
- **峰值 72,934 tok/s @ 2048**（ttft 口径）——与三臂峰值区一致（bounded 87k / project 77k / duckdb 76k@2048，group 口径）。
- **4096+ 坍塌**：72,934 → 50,013 → 39,898 → 37,966（峰值 52%），TTFT 51→158ms（3×），prefix-hit 0.96→0.60。与三臂同坍塌点、同信号。
- **lb_rr 峰值最低**：73k vs 三臂 76-87k（group 口径）。单进程经 nginx 欠喂（ADDENDUM 推断 avg ~10/backend << 32 饱和点，**per-run lbrr64 未审计**）。
- **全 9 格 passed（含 8192/10570）**——不像 duckdb_sharded 在大尺度 cap-64 失败（lb_rr 单进程经 nginx，256 门禁 0 error / 未观察到 max_tokens-truncation；注 `finish_reason` 字段空，**空 ≠ 已审计为非 length**，仅"无 length 报错"）。
- **GPU util**：64 时 GPU1 仅 12.5%；2048 时 70%；10570 时 90%（坍塌区 GPU 利用率高但吞吐低——**只能观察，不能归因 cache thrash**：无 service-counter 因果证据，且本跑 cache 控制不统一，见 §5）。

**推断**：
- lb_rr 本次 run 在 2048 附近峰值、4096+ 下降，**形态上**与三臂相似；但**不能声称"跨执行路径 cache-thrash regime"**——lb_rr `warmup_per_cell=false`（uncontrolled-cache，规模嵌套继承）+ 三臂 warmup 方式不同，cache 控制不统一。只能下：**本次 run 内吞吐下降与 prefix-hit 下降（0.96→0.60）相关**（相关，非因果）。
- lb_rr 峰值最低（73k vs 三臂 76-87k）：**推断**单进程经 nginx 的持续并发有限（per-run lbrr64 未审计，机制待证）；**不能声称"单入口欠喂是已证架构特性"**。

**不能声称**：
- lb_rr"优于/劣于"三臂（1 rep/cell，无 TOST；绝对值差机制未证，per-run lbrr64 未审计）。
- 坍塌"根因是 vLLM KV/调度"（无 service-counter 证据，疑似；见 ADDENDUM 订正）。
- vLLM effective max_num_seqs 具体值（cmdline 无 flag，默认）。

## 5. 对课题含义

- **4-臂规模曲线（各自 run）**：bounded/project/duckdb/lb_rr 都在 2048 附近峰值、4096+ 下降。**但当前标 `diagnostic_observation_pending_evidence_fix`，不引用"跨四臂 clean cache-thrash finding"**——4 臂 cache 控制不统一（lb_rr uncontrolled + 三臂不同 warmup + 规模嵌套继承）。要把"跨四臂 cache regime"写进论文，**必须另做**：统一 cache reset / 双后端 warmup / 随机化 scale 顺序 / ≥1w+3f 的受控重跑。
- lb_rr 峰值最低**形态上**对照多进程/多 endpoint 方法（"1 个 DuckDB 进程喂不饱多卡"是**推断/待证**，非已证结论；per-run lbrr64 未审计）。

## 6. 证据 + 诚实边界

- **raw**：服务器 `experiments/results/multicard_lbrr_scale_ramp/scale_<S>/lb_rr_c64_rep1/`（`gate_output/lb_rr_c64/shard_0/{summary.json,requests.csv}` + `ttft_metrics.json` + `gpu_resource.csv` + `ramp_run.json`）；commit 时提交裁剪版（summary/ramp_run，不含大 requests.csv）。
- `ramp_aggregate.{json,md}`：aggregator 重算（rows fallback + status from ramp_run）。
- 代码：`multicard_scale_ramp.py`（lb_rr cell + identity sidecar）。

> **诚实边界**：**1 rep/cell diagnostic**（非 formal）；**身份** `comparison_role=gateway_system_diagnostic`（**主字段=系统角色**，协议 §2.6 gateway 完整系统轨：DuckDB 单 BASE_URL 经 nginx → 2 vLLM；`component_comparison_role=database_product_native_baseline`；scheduler_owner=duckdb_ai_extension+nginx_round_robin+vllm）；**uncontrolled-cache**（`warmup_per_cell=false` + 规模嵌套 + 前跑 Phase 2 → 缓存继承，**不能 vs bounded ramp 直接比、非 cache-controlled scale curve、不引用跨四臂 cache-thrash**）；**ttft 口径**（shard summary 无 service counter，用 ttft 两后端 Σ delta）；**raw 已提交裁剪版**（6f6ef75，summary/ramp_run/ttft，requests.csv 排除）；**vLLM effective config = 默认**（cmdline 无 max_num_seqs/8192 flag）；**坍塌归因未证实**（疑似，无 service-counter）；**身份机器闭环**：identity sidecar 由新 driver（本轮 commit）写 `comparison_role`（系统主）+ `component_comparison_role`，历史 raw（6f6ef75）无 sidecar → aggregate `comparison_role=null`，报告层身份标注 standalone，重跑才机器闭环。

本节合并原独立补充报告，数值、全部重复、失败说明与结论按原记录保留。

<a id="scale-aggregate"></a>
<a id="scale-aggregate-多卡-ramp-聚合规模-6410570mean-across-passed-reps"></a>
## 规模聚合表
| scale | arm | conc | status | tok/s mean | tok/s CV | rows/s | TTFT P50 | E2E P50 | prefix-hit | GPU0 util | GPU1 util | n_passed/n |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 64 | lb_rr | c64 | passed | 17452.2 | 0.0% | 81.19 | 67.9ms | — | 0.95 | — | 12.5 | 1/1 |
| 128 | lb_rr | c64 | passed | 26773.9 | 0.0% | 150.28 | 52.8ms | — | 0.94 | 28.5 | 25.0 | 1/1 |
| 256 | lb_rr | c64 | passed | 50186.4 | 0.0% | 225.93 | 54.6ms | — | 0.96 | 26.6 | 29.8 | 1/1 |
| 512 | lb_rr | c64 | passed | 61657.8 | 0.0% | 282.47 | 52.3ms | — | 0.96 | 31.6 | 38.3 | 1/1 |
| 1024 | lb_rr | c64 | passed | 68938.5 | 0.0% | 303.96 | 51.1ms | — | 0.96 | 54.5 | 57.5 | 1/1 |
| 2048 | lb_rr | c64 | passed | 72934.4 | 0.0% | 344.7 | 51.0ms | — | 0.96 | 70.6 | 70.6 | 1/1 |
| 4096 | lb_rr | c64 | passed | 50013.4 | 0.0% | 228.93 | 75.7ms | — | 0.76 | 84.8 | 83.6 | 1/1 |
| 8192 | lb_rr | c64 | passed | 39898.0 | 0.0% | 165.41 | 158.4ms | — | 0.61 | 91.3 | 90.9 | 1/1 |
| 10570 | lb_rr | c64 | passed | 37966.4 | 0.0% | 157.85 | 162.0ms | — | 0.60 | 90.1 | 88.8 | 1/1 |
