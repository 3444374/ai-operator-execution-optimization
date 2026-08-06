# 多卡 scale-ramp 聚合（c=32/K=32 固定，规模 4096→8192→10570）

| scale | arm | service tok/s | rows/s | TTFT P50 | E2E P50 | E2E P95 | prefix-hit | GPU0 util | GPU1 util | gpu_samples |
|---|---|---|---|---|---|---|---|---|---|---|
| 10570 | bounded_http | — | — | — | — | — | — | — | — | — |
| 10570 | duckdb_ai | — | — | — | — | — | — | — | — | — |
| 4096 | bounded_http | — | — | — | — | — | — | — | — | — |
| 4096 | duckdb_ai | — | — | — | — | — | — | — | — | — |
| 8192 | bounded_http | — | — | — | — | — | — | — | — | — |
| 8192 | duckdb_ai | 0.0 | 0.0 | — | 46.673 | 46.673 | — | 93.0 | 92.8 | 252 |
