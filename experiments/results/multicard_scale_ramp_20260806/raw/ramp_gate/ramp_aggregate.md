# 多卡 ramp 聚合（规模 4096→10570，mean across passed reps）

| scale | arm | conc | status | tok/s mean | tok/s CV | rows/s | TTFT P50 | E2E P50 | prefix-hit | GPU0 util | GPU1 util | n_passed/n |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 4096 | bounded_http | c32 | passed | 43786.3 | 0.0% | 200.43 | 153.8ms | 9.106 | 0.63 | 94.6 | 92.9 | 1/1 |
| 4096 | duckdb_ai | c32 | passed | 42056.9 | 0.0% | 192.5 | 149.9ms | — | 0.63 | 91.5 | 91.4 | 1/1 |
| 8192 | bounded_http | c32 | passed | 42871.3 | 0.0% | 177.74 | 162.5ms | 21.08 | 0.62 | 96.3 | 96.5 | 1/1 |
| 8192 | duckdb_ai | c32 | failed | — | —% | — | — | — | — | — | — | 0/1 |
| 10570 | bounded_http | c32 | passed | 41778.9 | 0.0% | 173.71 | 163.8ms | 28.929 | 0.61 | 95.1 | 96.9 | 1/1 |
| 10570 | duckdb_ai | c32 | failed | — | —% | — | — | — | — | — | — | 0/1 |
