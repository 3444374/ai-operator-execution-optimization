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
