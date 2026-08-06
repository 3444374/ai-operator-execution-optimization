# 多卡饱和配置正式对比（2×4090，SQuAD 2048，1w+3f）

> **范围**：bounded_http 天花板 + duckdb_ai 2×1 static-sharded + project_static 2-endpoint，**饱和配置**下 1 warmup + 3 formal repeats 的 vLLM **service tokens/s** 正式对比。这是多卡 baseline 轨的**第一块正式饱和对比**，不是 smoke，也不是完整矩阵（缺 lb_rr / direct_static_50_50 / project_smart / framework-native，见 §7）。

## 1. 实验目的（问题 + 方法 + 关系）

**问题**：在 2×RTX 4090 上，项目的多 endpoint 上游调度（project_static，per-endpoint K admission + Ray actor pool）相对 DuckDB-ai 的 2×1 static-shard baseline，在**两者都喂饱 GPU（达系统最佳性能点）**的条件下，vLLM 服务吞吐有没有差距？

**为什么这样问**：nature-reviewer 指出之前 256 行、K=8 的 preliminary 让项目"看着慢 40%"——但 K=8 远低于饱和点，是**人为欠喂**。要公平对比，必须先找到系统饱和点，让所有臂在最佳性能点比。

## 2. 实验设置

| 项 | 值 |
|---|---|
| 平台 | AutoDL 2×RTX 4090；vLLM 0.25.1，双 endpoint 8000(GPU0)/8001(GPU1)，qwen2.5-7b，prefix-cache enabled，max_model_len 8192，gpu_mem_util 0.90 |
| workload | SQuAD v1.1 dev short-answer，**2048 行**（cap=64, temp=0, chat_completions, httpx_async） |
| 分片 | equal_rows（sha256(doc_id) 排序 round-robin，1024:1024） |
| 饱和配置 | **c=32/K=32 per endpoint**（见 §3 sweep 依据） |
| bounded_http | concurrency_per_endpoint=32（服务天花板参照） |
| duckdb_ai sharded | 2 独立进程各绑一 endpoint，max_concurrent_requests=32 |
| project_static | K=32/endpoint，8 actors × 4 concurrency = 32 slots，active-work=65536，token_budget=6144，admission-scope per_endpoint |
| 度量 | **vLLM service tokens/s**（prompt+generation counter delta ÷ operator span）——所有臂同一种测法（模型真实产出），跨臂可比；adapter span / group-wall 只作次要定位 |
| 重复 | 1 warmup + 3 formal（formal 报 mean ± stdev + CV） |

## 3. 合规性自检（feeding-saturation 门禁）

**bounded_http 2-ep 并发饱和 sweep（2048，纯 service tokens/s）**找系统最佳性能点：

| concurrency/endpoint | service tok/s | group_wall |
|---|---|---|
| **32** | **86274** | 5.08s（峰值）|
| 64 | 36655 | 12.18s（塌陷）|
| 128 | 25115 | 17.37s（塌陷）|
| 256 | — | shard 崩溃 |

→ **系统饱和点 ≈ 32 concurrent/endpoint**（更高并发 vLLM 过载塌陷）。所有臂固定在 c=32/K=32。

**饱和达标**：bounded=天花板（100%）；duckdb=88%；project=88%（均 ≥88% 天花板，**已喂饱**）。CV<1%（极稳）。exactly-once + 0 error（gate 全 passed）。

**配置理由（为什么 c=32/K=32）**：sweep 实测 c=32 是吞吐峰值，c=64 起塌陷（vLLM 在 ~64 总并发过载）。项目冻结的 K=8 远低于饱和点 → 欠喂；调到 K=32 才达最佳性能点。**这是"项目最优配置"的关键校准**。

## 4. 实验设计

三臂、同 workload（2048 SQuAD）、同 endpoint/model/cap/分片、**饱和配置**、统一度量（service tokens/s）、1w+3f。bounded_http 作服务天花板参照（feeding-saturation 的同协议 bounded 基线）；duckdb_ai sharded 是 2×1 static-shard baseline；project_static 是项目冻结静态方法（2 endpoint，per-endpoint K=32）。

## 5. 实验数据（formal 1w+3f，service tokens/s）

| 臂 | mean tok/s | stdev | CV | formal reps |
|---|---|---|---|---|
| bounded_http c=32（天花板） | **89287** | 341 | **0.4%** | [89299, 88941, 89622] |
| duckdb_ai sharded c=32 | **78112** | 608 | **0.8%** | [77515, 78091, 78729] |
| project_static K=32（8×4） | **78739** | 731 | **0.9%** | [78463, 78186, 79567] |

% of ceiling：duckdb **87.5%**，project **88.2%**。

## 6. 结果解释

- **事实**：饱和配置下 project_static（78739）与 duckdb_ai sharded（78112）**service tokens/s 几乎相同（项目高 0.8%）**，CV 都 <1%（reps 不重叠区间极窄）。两者都是 bounded 天花板的 ~88%。
- **推翻 preliminary 假象**：256 行 K=8 时 project 看着慢 40%——那是 **K=8 欠喂**（8 << 饱和点 32）的人为结果，不是项目真实能力。调到饱和 K=32 后，项目与 DuckDB-ai 持平。
- **推断**：在 SQuAD 均匀短答案 + 饱和配置下，项目的 Ray-actor 框架开销在 2048 行被摊薄，静态调度（per-endpoint K + actor pool）与 DuckDB-ai 扩展的 set-oriented batching **服务吞吐等价**。
- **不能声称**：
  - 项目"优于"DuckDB-ai（0.8% 在 CV 范围内，统计等价，非显著）。
  - 项目的**自适应**调度价值（project_smart 未测；static 不 exercise 自适应贡献）。
  - SQuAD 均匀长度下的结论外推到**倾斜长度** workload（均匀长度下 equal_rows≈work_balanced，调度价值不显现——见 §7）。
  - 跨臂 **database-E2E wall** 排名（各臂计时边界不同：gate group-wall vs profiler operator_wall；本报告用 service tokens/s 这一同口径跨臂公平度量，不用 wall 排名）。

## 7. 对课题含义 + 下一步

- **含义**：项目方法在"喂饱 GPU"的公平条件下，**不输** DuckDB-ai 的数据库产品 baseline（服务吞吐等价）。这把"项目是否值得做"的问题，从"会不会被 DuckDB-ai 的 lean 扩展碾压"收敛到"自适应调度在什么条件下产生净收益"。
- **下一步（补全矩阵 + 测真价值）**：
  1. **project_smart**（自适应臂）——项目的真正贡献在自适应，static 只是基线；要 smart vs static 的净收益。
  2. **倾斜长度 workload**（ShareGPT/BurstGPT 变长输出）——SQuAD 均匀测不出调度价值；倾斜下 work_balanced / 自适应才会分化。
  3. **duckdb_ai_lb_rr**（nginx 单入口现实部署）+ **direct_static_50_50**（无代理因果对照，隔离 nginx 开销）。
  4. **framework-native**（Daft / Ray Data，它们自带多 endpoint 调度）——另一类对照。
  5. **统一 database-E2E 计时边界**（service tokens/s 已公平，但 wall 排名需统一边界）。

## 8. 证据

- `formal.log`：1w+3f driver 输出（mean/stdev/CV/reps）。
- `ps8_formal/{cell}_{tag}/`：每臂每 repeat 的 gate shard summaries（bounded + duckdb）。
- `proj_formal_*.csv`：project_static 每 repeat 的 profiler 输出。
- `sweep.log`：bounded 饱和 sweep（c=32/64/128/256）。
- 代码：`42cfc9a`（partition-policy A/B/C 修复 + 饱和校准 driver）。

> **诚实边界**：这是饱和配置下 3 臂（bounded 天花板 + duckdb_sharded + project_static）的正式 1w+3f 对比，CV<1%，数据干净。**不是完整多卡矩阵**（缺 lb_rr/direct_static/project_smart/framework-native），**不是倾斜 workload**（SQuAD 均匀），**不用 wall 跨臂排名**（用 service tokens/s）。完整矩阵 + 自适应价值 + 倾斜 workload 是后续。
