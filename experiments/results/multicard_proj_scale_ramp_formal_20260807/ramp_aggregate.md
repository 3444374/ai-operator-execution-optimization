# 多卡 ramp 聚合（规模 64→10570，mean across passed reps）

| scale | arm | conc | status | tok/s mean | tok/s CV | rows/s | TTFT P50 | E2E P50 | prefix-hit | GPU0 util | GPU1 util | n_passed/n |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 64 | project_static | c32 | passed | 49934.6667 | 20.95% | 37.9267 | 71.0ms | 1.7807 | 0.95 | 32.8 | — | 3/3 |
| 128 | project_static | c32 | passed | 47952.1 | 1.15% | 56.7967 | 63.7ms | 2.004 | 0.94 | 19.2 | — | 3/3 |
| 256 | project_static | c32 | passed | 91407.4 | 2.08% | 103.92 | 52.6ms | 2.3487 | 0.96 | 24.3 | — | 3/3 |
| 512 | project_static | c32 | passed | 86897.2333 | 0.87% | 136.2633 | 54.0ms | 3.0277 | 0.95 | 29.4 | — | 3/3 |
| 1024 | project_static | c32 | passed | 83957.7333 | 1.94% | 242.7167 | 53.3ms | 2.4453 | 0.95 | 52.2 | — | 3/3 |
| 2048 | project_static | c32 | passed | 76463.6667 | 2.15% | 238.0333 | 53.0ms | 5.2513 | 0.94 | 60.9 | — | 3/3 |
| 4096 | project_static | c32 | passed | 42402.9 | 0.78% | 170.2367 | 141.1ms | 11.7727 | 0.66 | 88.2 | — | 3/3 |
| 8192 | project_static | c32 | passed | 42287.5 | 0.31% | 161.2833 | 153.9ms | 23.5927 | 0.65 | 86.8 | — | 3/3 |
| 10570 | project_static | c32 | passed | 41146.0 | 0.26% | 160.4033 | 155.1ms | 31.435 | 0.64 | 88.6 | — | 3/3 |

## 效率与尾延迟（§7.5D 补齐；MFU=[0,1] 分数，非 %；vLLM estimated_flops 保守估计）

| scale | arm | conc | MFU(frac) | ITL p95 | ITL p99 | TTFT p99 | decode | prefill | J/1k-tok |
|---|---|---|---|---|---|---|---|---|---|
| 64 | project_static | c32 | 0.134 | — | — | 98.9ms | 50.5ms | 41ms | 2.5433 |
| 128 | project_static | c32 | 0.155 | — | — | 98.3ms | 54ms | 41.1ms | 2.7467 |
| 256 | project_static | c32 | 0.222 | — | — | 81.7ms | 59.3ms | 38.7ms | 1.64 |
| 512 | project_static | c32 | 0.245 | — | — | 79.4ms | 60ms | 39.7ms | 2.6833 |
| 1024 | project_static | c32 | 0.242 | — | — | 82.8ms | 71.8ms | 39.7ms | 5.53 |
| 2048 | project_static | c32 | 0.244 | — | — | 82.9ms | 78ms | 39.7ms | 6.5067 |
| 4096 | project_static | c32 | 0.611 | — | — | 340.6ms | 158.5ms | 87.1ms | 17.7167 |
| 8192 | project_static | c32 | 0.624 | — | — | 413.9ms | 182.2ms | 94.7ms | 18.6367 |
| 10570 | project_static | c32 | 0.618 | — | — | 414.5ms | 187.7ms | 94.8ms | 19.45 |
