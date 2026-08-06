# lb_rr（nginx gateway 1-proc）scale-ramp（formal, reps=3, 2026-08-07）

lb_rr 单臂规模爬坡：1 个 DuckDB 进程（单 BASE_URL）→ nginx 8500 round-robin → 2 vLLM backend（8000/8001）。endpoint_count=1 `lbrr_dev` manifest（全行→LB→nginx 分），`concurrency=64`（单进程 TOTAL ≈32/backend，C_total=64）。reps=3，warmup_per_cell（单 endpoint manifest 用 endpoint_index=0 把全集暖两 backend，两 vLLM prefix cache 独立不共享）。driver 5878d51。

**完整三路径对比（bounded / duckdb / lb_rr）、合规自检、数据表、边界、下一步** 见 `../multicard_scale_ramp_formal_20260806/README.md`（本 lb_rr run 是其中 gateway 轨）。

身份：`comparison_role=gateway_system_diagnostic`（协议 §2.6 gateway 完整系统轨，主字段=系统角色）；`component_comparison_role=database_product_native_baseline`；`scheduler_owner=duckdb_ai_extension + nginx_round_robin + vllm`；`formal_baseline_eligible=false`。**系统级结论 only，不与 bounded/duckdb 并入同柱排名。**

峰值 74088 tok/s @ 2048（cv2.0%），4096 拐点（→39401），平台到 10570（38540，cv0.9%）。9/9 scale 全 3/3 passed。tokens/s 口径 = ttft 两后端 Σ(prompt+gen delta)/shard wall（无 gate.json，aggregator priority-2 ttft 口径）。计时粒度 `query_barrier` → `query_jct_s`（无 per-row E2E）。

provenance：`run_provenance.json`（同 scale 目录，两 vLLM 共用）；`ramp_aggregate.{json,md}`；per-cell `identity.json` + `ttft_metrics.json`（含 backend request/token-work skew，`(max-min)/max` @ multicard_scale_ramp.py:366）。
