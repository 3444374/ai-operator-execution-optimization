# 多卡饱和配置对比（2×4090，SQuAD 2048，1w+3f）——已订正

> **⚠️ 订正注（2026-08-06，codex 审计 + raw 独立复算）**：本报告原版有四处分歧，已订正：
>
> 1. **身份**：`duckdb_ai 2×1 static-sharded` 是测试 harness 预切 manifest + 启动 2 个独立 DuckDB 进程，而 DuckDB `ai` 扩展只拥有单一 `BASE_URL`。按 [bounded_output_duckdb_comparison_protocol_20260805.md](../../plans/bounded_output_duckdb_comparison_protocol_20260805.md) §2.6（line 114-117），这只能标 **`harness_pre_split_diagnostic`**（scheduler owner = 实验 harness，不是 DuckDB），**不进 DuckDB 产品原生主排名**，也不能称"DuckDB-ai 数据库产品 baseline"。**层级注**：raw `summary.json::comparison_role=database_product_native_baseline` 与 `resolved_config.json::formal_baseline_eligible=true` 是 runner **单 shard 层**声明（duckdb 单 endpoint 的 product 语义，单 shard 正确）；ramp 层（本报告）2-shard harness 预切才使其变 `harness_pre_split_diagnostic`、不进 formal native——两者不同层级，非矛盾。任务 #8 将加 ramp 层 identity sidecar 做权威 override（recompute 以 sidecar 为准，而非单 shard 角色）。
> 2. **数据口径**：原 §5/§6 对 gate 臂用"分片速率求和"（`tok₀/wall₀ + tok₁/wall₁`，duckdb=78112），但 project 臂只有"group 总 tokens / group wall"一种测法。跨臂同口径要求 gate 臂也用 group 口径（`(tok₀+tok₁)/group_wall`）。按 group 口径独立复算 raw：**duckdb 76780 tok/s（86.4% 天花板）、project 78739 tok/s（88.6%）、project 高 +2.55%**（原 +0.8% 是跨口径比较）。**定性结论仍成立**：饱和下 project 不输 duckdb、两者 ~88% 天花板、reps 完全无重叠（project [78186,78463,79567] vs duckdb-group [76042,76868,77429]）。
> 3. **措辞 + 统计**：原"统计等价"无 TOST/equivalence margin。按 group reps 重算 Welch t=3.36 df≈4 **p≈0.0284**（**非**早前误引的 0.127——0.127 属 rich 实验）→ project **统计显著高于** duckdb harness diagnostic（+2.55%, reps 不重叠）；但 duckdb 是 harness 非产品，"显著高 vs harness"≠"优于产品"。"质量等价"见 rich EM/F1 复核。
> 4. **证据**：§8 原引用的 `formal.log`/`sweep.log` 未提交（.gitignore），但结构化 raw（`gate.json` + 每片 `summary.json` + `proj_formal_*.csv`）齐全，三臂可完全独立复算（本注订正数字即来自该 raw，非日志）。LB RR 附录（`ADDENDUM_lbrr_collapse.md` 的 72480 tok/s）因 `collapse.log`/`lbrr64_*`/`ps8_collapse` 未提交，**当前不可独立审计**，见该附录订正。
> 5. **vLLM effective config（诚实）**：本报告/§5 的 `max_num_seqs=256 / max_num_batched_tokens=8192` 是 **adapter 声明值**；vLLM 启动 cmdline 无这些 flag（仅 `--model/--max-model-len 8192/--gpu-memory-utilization 0.90`），实际用 vllm 0.25.1 **默认值**（≠ 声明）；`enable_prefix_caching` 默认 ON（= 声明，巧合）。数据有效（c32<C_total=64<默认 max_num_seqs），但 service config 字段是声明非 effective。
>
> 更可复现的同一结论（含 committed 聚合器 `multicard_rich_aggregate.py`）见 [`multicard_rich_metric_2048_20260806/`](../multicard_rich_metric_2048_20260806/README.md)。本报告订正后保留作历史 formal 记录。
>
> **范围**：bounded_http 天花板 + **duckdb_ai harness_pre_split_diagnostic** + project_static 2-endpoint，饱和配置 1w+3f 的 vLLM service tokens/s 对比。**duckdb 臂是 harness 诊断，非 DuckDB 产品原生排名**。不是完整矩阵（缺 lb_rr / direct_static_50_50 / project_smart / framework-native，见 §7）。

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

**饱和达标（group 口径）**：bounded=天花板 100%；duckdb=86.4%；project=88.6%。**注意 feeding-parity 门禁未达**：duckdb/project 均 <95%（项目规则 feeding-saturation 要求 ≥95% 同协议 bounded），这是"饱和 screening"，**不是"已通过 feeding-parity 的正式 ranking"**。CV<1%（极稳）。exactly-once + 0 error（gate 全 passed）。

**配置理由（为什么 c=32/K=32）**：sweep 实测 c=32 是吞吐峰值，c=64 起塌陷（vLLM 在 ~64 总并发过载）。项目冻结的 K=8 远低于饱和点 → 欠喂；调到 K=32 才达最佳性能点。**这是"项目最优配置"的关键校准**。

## 4. 实验设计

三臂、同 workload（2048 SQuAD）、同 endpoint/model/cap/分片、**饱和配置**、统一度量（service tokens/s）、1w+3f。bounded_http 作服务天花板参照（feeding-saturation 的同协议 bounded 基线）；duckdb_ai sharded 是 2×1 static-shard baseline；project_static 是项目冻结静态方法（2 endpoint，per-endpoint K=32）。

## 5. 实验数据（formal 1w+3f，service tokens/s，**group 口径——订正**）

> **口径订正**：gate 臂（bounded/duckdb）原用"分片速率求和"（`tok₀/wall₀+tok₁/wall₁`），与 project 臂的"group 总 tokens / group wall"不同语义（apples-to-oranges）。下表统一用 **group 口径**（`(tok₀+tok₁)/group_service_wall`）跨臂同语义可比。group_service_wall 比 max(shard jct) 长 80–90ms（两分片起止未对齐），证明它才是真聚合窗。原分片求和值（bounded 89287 / duckdb 78112）见顶部订正注。

| 臂 | mean tok/s | stdev | CV | formal reps（group 口径） |
|---|---|---|---|---|
| bounded_http c=32（天花板） | **88847** | 548 | **0.62%** | [88977, 88246, 89318] |
| duckdb_ai harness_pre_split_diagnostic c=32 | **76780** | 698 | **0.91%** | [77429, 76042, 76868] |
| project_static K=32（8×4） | **78739** | 731 | **0.93%** | [78463, 78186, 79567] |

> stdev 为 **sample（n-1 分母）**，适合 n=3 推断（复审：原 population stdev 低估）。

% of ceiling（bounded 88847）：duckdb **86.4%**，project **88.6%**。project vs duckdb：**+2.55%**（reps 完全无重叠：project min 78186 > duckdb max 77429）。

## 6. 结果解释

- **事实**：饱和配置（group 口径）下 project_static（78739）与 duckdb_ai harness_pre_split_diagnostic（76780）service tokens/s，**项目高 +2.55%**，reps **完全无重叠**（project min 78186 > duckdb max 77429）；Welch t-test **t=3.36, df≈4, p≈0.0284**（two-tailed，scipy 一致）→ project **统计显著高于** duckdb harness diagnostic。两者都是 bounded 天花板（88847）的 ~87-89%。
- **推翻 preliminary 假象**：256 行 K=8 时 project 看着慢 40%——那是 **K=8 欠喂**（8 << 饱和点 32）的人为结果，不是项目真实能力。调到饱和 K=32 后，项目不低于 DuckDB-ai harness 诊断。
- **推断**：SQuAD 均匀短答案 + 饱和配置下，项目静态调度 service tokens/s **显著高于** duckdb_ai harness_pre_split_diagnostic（+2.55%, p≈0.0284, n=3）。注意：harness_pre_split_diagnostic **不是** DuckDB 产品原生 baseline（duckdb 单进程单 endpoint 才是产品轨），所以"显著高 vs harness 诊断"**不等于**"项目优于 DuckDB 产品"；仅表明项目多 endpoint 方法在本 setup 不低于（数值上高于）harness 预切诊断。
- **不能声称**：
  - 项目"优于"DuckDB-ai **产品**：p≈0.0284 是 vs **harness diagnostic**（非产品原生）；要下"优于产品"结论需 DuckDB 单 endpoint 产品轨 + 更大 n。即便如此，n=3 小样本、无预注册 superiority/equivalence margin，p 值不应过度解读。
  - 项目的**自适应**调度价值（project_smart 未测；static 不 exercise 自适应贡献）。
  - SQuAD 均匀长度下的结论外推到**倾斜长度** workload（均匀长度下 equal_rows≈work_balanced，调度价值不显现——见 §7）。
  - 跨臂 **database-E2E wall** 排名（各臂计时边界不同：gate group-wall vs profiler operator_wall；本报告用 service tokens/s 这一同口径跨臂公平度量，不用 wall 排名）。

## 7. 对课题含义 + 下一步

- **含义**：项目方法在饱和配置下**未检出显著低于** DuckDB-ai 的 harness_pre_split_diagnostic（**非 DuckDB 产品原生 baseline**；身份见顶部订正）。这把"项目是否值得做"的问题，从"会不会被 DuckDB-ai 的 lean 扩展碾压"收敛到"自适应调度在什么条件下产生净收益"。
- **下一步（补全矩阵 + 测真价值）**：
  1. **project_smart**（自适应臂）——项目的真正贡献在自适应，static 只是基线；要 smart vs static 的净收益。
  2. **倾斜长度 workload**（ShareGPT/BurstGPT 变长输出）——SQuAD 均匀测不出调度价值；倾斜下 work_balanced / 自适应才会分化。
  3. **duckdb_ai_lb_rr**（nginx 单入口现实部署）+ **direct_static_50_50**（无代理因果对照，隔离 nginx 开销）。
  4. **framework-native**（Daft / Ray Data，它们自带多 endpoint 调度）——另一类对照。
  5. **统一 database-E2E 计时边界**（service tokens/s 已公平，但 wall 排名需统一边界）。

## 8. 证据（独立可复算）

> `formal.log` / `sweep.log` 未提交（.gitignore），但下列结构化 raw 齐全，三臂 service tokens/s 可完全独立复算（本报告订正数字即来自此复算，不依赖日志）。

- `raw/ps8_formal/{bounded_http,duckdb_ai}_formal{0,1,2}/{arm}/gate.json` + `shard_{0,1}/summary.json`：gate 臂每 repeat 的 group + 分片 service counters（**group 口径复算源**）。
- `raw/proj_formal_formal{0,1,2}.csv`：project_static profiler 输出（`model_request_tokens_per_s` = `(prompt+generation delta)/model_request_wall_s`）。
- `raw/ps8_collapse/bounded_c{64,128,256}_*`：bounded 过订阅 sweep 的 shard summary（c=32 在 `ps8_formal/bounded_http_*`）。
- 代码：饱和校准 driver `42cfc9a`。

> **诚实边界**：饱和配置下 3 臂（bounded 天花板 + **duckdb_ai harness_pre_split_diagnostic** + project_static）1w+3f，CV<1%，group 口径数据可独立复算。**身份**：duckdb 臂是 harness 诊断，非 DuckDB 产品原生排名。**未达 feeding-parity ≥95% 门禁**（duckdb/project 86–89%）。**不是完整多卡矩阵**（缺 lb_rr/direct_static/project_smart/framework-native），**不是倾斜 workload**（SQuAD 均匀），**不用 wall 跨臂排名**（用 service tokens/s）。
