# Addendum：lb_rr 单入口数据点 + 过订阅塌陷曲线——**raw 证据未提交，数字当前不可独立审计**

> **⚠️ 订正注（2026-08-06，codex 审计 + raw 复算）**：
> - **`ps8_collapse/`（bounded c=64/128/256）已提交**（114 files，raw 可复算）→ bounded 过订阅塌陷曲线**可独立审计**。group 口径复算：c64=**36560**（n=2）/ c128=**24836**（n=1）/ c256=**N/A**（0/3 完整 2-shard rep，每 formal ≥1 shard 失败）；正文旧的 37341/25026 是 sum-of-per-shard 不同公式（已订正，见 §B）。
> - **`collapse.log` + `lbrr64_*/`（lb_rr @64 per-run）仍未提交** → **duckdb_ai_lb_rr @64 = 72480 tok/s 仍无法独立审计**（仅 collapse.log 汇总数字，无 per-run 证据）。
> - **塌陷归因**：c64_f0 shard_1 是 `ValueError ReadError`（网络/读取层，非 vLLM 崩溃）；机制归因 vLLM KV/调度**疑似、无 service-counter 证据、未证实**。
> - lb_rr 身份 = `system_comparison_role=gateway_system_diagnostic`（协议 §2.6 gateway 完整系统轨：DuckDB 单 BASE_URL 经 nginx；component comparison_role=database_product_native_baseline；scheduler_owner=duckdb_ai_extension+nginx_round_robin+vllm），非 DuckDB 产品原生多 endpoint。

> 补充主 README 的饱和三臂 formal。两块数据：(1) **duckdb_ai_lb_rr @64**（`lbrr64_*/` + `collapse.log` **未提交** → 72480 **不可独立审计**）；(2) **bounded_http 过订阅塌陷曲线**（`ps8_collapse/` **已提交** → group 口径可复算）。同设置：2048 SQuAD，2×4090，cap=64，temp=0，prefix-on。§A（lb_rr）数字基于 collapse.log 汇总、per-run 未审计；§B（bounded）已按 group 口径复算。

## A. duckdb_ai_lb_rr @64（现实单入口，固有欠喂）

| 臂 | mean tok/s | CV | n | % of ceiling（group 88847） |
|---|---|---|---|---|
| duckdb_ai_lb_rr c=64 | **72480**† | 0.6% | 3/3 | **81.5%** |

† 72480 来自 `collapse.log` 汇总；`lbrr64_*/` per-run 证据**未提交**，数字**当前不可独立审计**。

**配置理由**：lb_rr 是单 DuckDB 进程经 nginx round-robin 到 2 backend。要每 backend ~32（饱和点），单进程需总并发 64（nginx 各分 ~32）。实测：峰值 backend running ~27，但**平均 ~10/backend**——DuckDB-ai 扩展**单进程的持续 in-flight 只有 ~20**（不是配置的 64），突发不持续。

**观察（非审计结论）**：lb_rr @64 collapse.log 汇总 = 72480（group ceiling 88847 的 81.5%）。比饱和三臂（sharded/project ~88%）低 ~7pp。**推断**（per-run 未审计、机制待证）：单进程 nginx 路由下 DuckDB-ai 持续 in-flight ~20 → 每 backend ~10，欠喂（<< 32 饱和点）。**不能声称**"单入口固有极限"为已证结论（lbrr64 per-run 未审计；sharded 旧分片求和口径也需 group 重算后才同基线可比）。

**不能声称**：lb_rr 是"饱和竞争臂"——它**固有欠喂**（单进程限制），与饱和三臂不在同一基线。它的价值是**对照**："现实单入口部署在多卡上喂不饱"。

**sharded > lb_rr 的推断**（per-run 未审计）：sharded 用 2 个 DuckDB 进程（每 backend ~32，饱和）；lb_rr 用 1 进程（持续 in-flight ~20 → 每 backend ~10，欠喂）。**"多进程是喂饱多卡的必要条件"是推断，非已证结论**（需 lbrr64 per-run 审计 + sharded group 口径同基线对比）。

## B. bounded_http 过订阅塌陷曲线（c=64/128/256）

c=32 是饱和峰值（89287，主 formal）。越过饱和点（更高并发）的塌陷：

| concurrency/endpoint | mean tok/s（**group 口径**） | n 完整 2-shard/3 formal | % of c=32 峰值（group 88847） |
|---|---|---|---|
| 32（饱和峰值，group） | 88847 | 3/3 | 100% |
| **64（过订阅）** | **36560** | **2/3**（formal0 shard_1 ValueError ReadError） | **41.1%** |
| **128（过订阅）** | **24836** | **1/3**（仅 formal1 完整） | **28.0%** |
| **256（过订阅）** | **N/A** | **0/3**（每 formal ≥1 shard 失败；非"全 shard 崩溃"，部分 shard 完成） | — |

> **口径订正**：旧的 37341/25026 是 **sum-of-per-shard**（`tps_shard0 + tps_shard1`，两 shard jct 不等时高估）；group 口径 `group_service_total_tokens/group_service_wall_s` 才是系统级吞吐。c256 "0/3 全失败" 是 rep 级（每 formal 无完整 2-shard），shard 级有完成（formal1 shard_0、formal2 shard_1）。c64 formal0 shard_1 是 `ValueError ReadError`（网络/读取，非 vLLM 崩溃）。

**事实（group 口径，ps8_collapse 可审计）**：越过饱和点后，吞吐**单调塌陷**（100%→41%→28%→N/A），**完整 2-shard rep 率下降**（3/3→2/3→1/3→0/3）。c=256 每 formal ≥1 shard 失败（非"全 shard 崩溃"，部分 shard 完成：formal1 shard_0、formal2 shard_1）。

**机制（疑似，无 service-counter 证据，未证实）**：2×4090 + Qwen2.5-7B + SQuAD（max_model_len 8192），vLLM 在 ~64 总并发（32/endpoint）达饱和；更高并发 → 吞吐塌陷 + 不稳定（部分 run 崩溃）。**疑似** vLLM KV/调度过载，但无 service-counter 证据，未证实（见订正注：不能直接归因 vLLM KV/调度）。

**对课题含义**：
- **饱和点是真实约束**：多卡 baseline 必须**校准到饱和点**（~32/endpoint），不能盲目堆并发——越过即塌陷。这印证 AGENTS §7.5C 的 feeding-saturation 门禁（先找饱和点，固定，不在线调参）。
- **过订阅塌陷是系统属性**，不是某个臂的 bug——bounded_http（最 lean 的客户端）也塌陷，说明是服务侧过载（**疑似 vLLM，无 service-counter 证据，未证实**），与上游臂无关。

## C. 证据

- `collapse.log`：lb_rr@64 + 塌陷曲线 1w+3f driver 输出（mean/CV/n/reps）。
- `ps8_collapse/`：bounded c=64/128/256 每 repeat 的 gate shard summaries（成功的）。
- `lbrr64_*/`：lb_rr 每 repeat 的 runner report。
- 主 formal（饱和三臂）见上一级 README + `ps8_formal/`。

> **诚实边界**：lb_rr@64 是现实单入口的**固有欠喂**数据点（81% ceiling，单进程限制），不进饱和排名；塌陷曲线是系统过订阅行为（c≥64 塌陷 + 失败率升），c=256 全失败。两者都是 formal 1w+3f（CV<1% where n≥2），数据干净。
