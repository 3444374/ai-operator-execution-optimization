# lb_rr（nginx gateway 1-proc）scale-ramp（formal, reps=3, 2026-08-07）

2026-10-02存储整理：原始文件的路径、字节数和校验值见[恢复清单](raw/storage-manifest.jsonl)，
原始数据与脚本的恢复方式见[文件恢复说明](../../../code/scripts/README.md#实验结果恢复)。直接引用的数据和脚本仍可就地读取。

lb_rr 单臂规模爬坡：1 个 DuckDB 进程（单 BASE_URL）→ nginx 8500 round-robin → 2 vLLM backend（8000/8001）。endpoint_count=1 `lbrr_dev` manifest（全行→LB→nginx 分），`concurrency=64`（单进程 TOTAL ≈32/backend，C_total=64）。reps=3，warmup_per_cell（单 endpoint manifest 用 endpoint_index=0 把全集暖两 backend，两 vLLM prefix cache 独立不共享）。driver 5878d51。

**完整三路径对比（bounded / duckdb / lb_rr）、合规自检、数据表、边界、下一步** 见 `../multicard_scale_ramp_formal_20260806/README.md`（本 lb_rr run 是其中 gateway 轨）。

身份：`comparison_role=gateway_system_diagnostic`（协议 §2.6 gateway 完整系统轨，主字段=系统角色）；`component_comparison_role=database_product_native_baseline`；`scheduler_owner=duckdb_ai_extension + nginx_round_robin + vllm`；`formal_baseline_eligible=false`。**系统级结论 only，不与 bounded/duckdb 并入同柱排名。**

峰值 74088 tok/s @ 2048（cv2.0%），4096 拐点（→39401），平台到 10570（38540，cv0.9%）。9/9 scale 全 3/3 passed。tokens/s 口径 = ttft 两后端 Σ(prompt+gen delta)/shard wall（无 gate.json，aggregator priority-2 ttft 口径）。计时粒度 `query_barrier` → `query_jct_s`（无 per-row E2E）。

provenance：`run_provenance.json`（同 scale 目录，两 vLLM 共用）；`ramp_aggregate.{json,md}`；per-cell `identity.json` + `ttft_metrics.json`（含 backend request/token-work skew，`(max-min)/max` @ multicard_scale_ramp.py:366）。

本节合并原独立补充报告，数值、全部重复、失败说明与结论按原记录保留。

<a id="scale-aggregate"></a>
<a id="scale-aggregate-多卡-ramp-聚合规模-6410570mean-across-passed-reps"></a>
## 规模聚合表
| scale | arm | conc | status | tok/s mean | tok/s CV | rows/s | TTFT P50 | E2E P50 | prefix-hit | GPU0 util | GPU1 util | n_passed/n |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 64 | lb_rr | c64 | passed | 20081.2 | 2.62% | 93.5 | 56.3ms | — | 0.95 | 25.0 | 25.0 | 3/3 |
| 128 | lb_rr | c64 | passed | 24879.3333 | 5.07% | 139.7267 | 52.7ms | — | 0.94 | 20.0 | 24.6 | 3/3 |
| 256 | lb_rr | c64 | passed | 49562.1667 | 3.16% | 223.18 | 52.2ms | — | 0.96 | 35.6 | 39.0 | 3/3 |
| 512 | lb_rr | c64 | passed | 62910.4 | 4.62% | 288.2433 | 52.0ms | — | 0.96 | 42.0 | 42.9 | 3/3 |
| 1024 | lb_rr | c64 | passed | 70090.3333 | 2.57% | 309.05 | 52.3ms | — | 0.96 | 58.7 | 69.5 | 3/3 |
| 2048 | lb_rr | c64 | passed | 74088.2333 | 2.04% | 350.1733 | 51.6ms | — | 0.96 | 75.0 | 75.0 | 3/3 |
| 4096 | lb_rr | c64 | passed | 39400.5667 | 0.65% | 180.3533 | 149.1ms | — | 0.62 | 86.6 | 86.0 | 3/3 |
| 8192 | lb_rr | c64 | passed | 39331.3667 | 0.39% | 163.0567 | 158.6ms | — | 0.61 | 90.5 | 90.9 | 3/3 |
| 10570 | lb_rr | c64 | passed | 38539.8 | 0.86% | 160.2333 | 161.1ms | — | 0.60 | 91.7 | 91.6 | 3/3 |
