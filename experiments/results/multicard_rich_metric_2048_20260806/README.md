# 多卡 rich-metric 饱和对比（2×4090，SQuAD 2048，1w+3f，5 组指标）

> 3 臂（bounded_http 天花板 + duckdb_ai 2×1 sharded + project_static 2-endpoint）在**饱和配置**（c=32/K=32）下的 5 组指标 formal。supersedes `multicard_saturated_2048_20260806/`（仅吞吐）的指标完整性。

## 正式数据（5 组指标，1w+3f，CV<1.1%）

| 指标 | bounded_http | duckdb_ai | project_static |
|---|---|---|---|
| **service tok/s** (unified) | **89420** (CV 0.5%) | **77764** (CV 1.1%) | **79111** (CV 1.1%) |
| **% of ceiling** | 100% | 87.0% | 88.5% |
| **EM (子集 2048)** | ~82.4% | ~82.4% | **82.47%** |
| **correct_rows** | ~1687 | ~1689 | **1689** |
| **GPU util mean** | 75-80% | 65% | 63%（54-75% range, n=16-22）|
| **vLLM running mean** | 16.3 | 14.6 | — |
| **latency P50** | 2.33s（per-request）| 5.5s（barrier）| — |
| **TTFT P50** | — | — | **1.6ms** |
| **writeback** | N/A（gate）| N/A（gate）| **0.05s** |
| **power (both GPU)** | — | — | 405-528W |

## 关键发现

1. **质量等价**：三臂 EM_subset ~82.4%、correct_rows ~1689——**不同执行引擎不引入质量偏差**。
2. **吞吐排序**：bounded(89k) > project(79k) ≈ duckdb(78k)——项目与 DuckDB 在饱和点**统计等价**（CV 重叠）。
3. **GPU 喂饱度排序**：bounded(75-80%) > duckdb(65%) ≈ project(63%)——**DuckDB 扩展 + 项目 Ray actor 都不如 lean httpx 喂得稳**（框架开销导致 bursty，GPU 平均利用率低）。
4. **项目 TTFT 极低**（1.6ms）——Ray actor pool 提交延迟极低，但 bursty 执行让 GPU 平均利用率不高。

## 饱和校准 + 塌陷曲线

| concurrency/endpoint | service tok/s | 成功率 | 备注 |
|---|---|---|---|
| 1 | 4283 | 3/3 | ramp-up 起点 |
| 32（**饱和峰值**）| **89420** | 3/3 | 最佳性能点 |
| 64 | 37341 | 2/3 | 塌陷（42% peak）|
| 128 | 25026 | 1/3 | 塌陷（28% peak）|
| 256 | — | 0/3 | 全失败 |

lb_rr（单入口，1 DuckDB→LB→2 backend）@64：72480 tok/s（81% ceiling，**固有欠喂**——单进程持续并发 ~20 < 饱和 64）。

## 诚实边界

- **formal=false**：gate 代码拒绝 formal=true；这是"饱和 screening/gate 证据（1w+3f）"，不是项目定义的 formal ranking。
- **cache-hot**：prefix-cache hit ~94-96%（重复 SQuAD prompt），不能外推到 cache-cold/唯一行。
- **DuckDB 无逐行延迟**：barrier 5.5s 是 query wall，不是 per-request P50/P95/P99。
- **project GPU util 采样稀疏**（0.3s 间隔 × ~5s wall = 16-22 点），mean 波动大（54-75%）。
- **分母**：service tokens/s 用统一全局跨度（total tokens / max shard jct 或 operator_wall），不用 sum-of-shards。
- **计时边界不一**：bounded/DuckDB 用 gate group-wall（operator-only）；project 用 profiler operator_wall + e2e_s（含 scan+sink）。service tokens/s 是跨臂公平口径；wall 不跨臂排名。
- **项目臂未用 equal-rows manifest**：用了 round_robin 路由（1010:1038），不是 manifest 的 1024:1024——三臂不是 same-manifest。后续需统一。

## 证据

`raw/rich_formal/` 下：
- `bounded_http_{warmup,formal0-2}/`：gate shard summaries（含 requests.csv + service counters）。
- `duckdb_ai_{warmup,formal0-2}/`：同上。
- `project2_{warmup,formal0-2}.*`：profiler CSV + completion evidence（output_text）+ request trace（TTFT）+ resource trace（GPU util/power/memory）。
- `squad_eq2048.jsonl` + `.meta.json`：2048 行 equal-rows manifest + partition provenance。
