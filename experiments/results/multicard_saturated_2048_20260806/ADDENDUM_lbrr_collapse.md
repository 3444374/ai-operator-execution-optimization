# Addendum：lb_rr 单入口数据点 + 过订阅塌陷曲线（formal 1w+3f）

> 补充主 README 的饱和三臂 formal。两块新 formal 数据：(1) **duckdb_ai_lb_rr @64**——现实单入口部署（1 DuckDB → nginx → 2 backend），作为"单入口喂不饱多卡"的对照数据点（**不在饱和排名里**，因单进程固有欠喂）；(2) **bounded_http 过订阅塌陷曲线**（c=64/128/256）——系统越过饱和点后的吞吐塌陷 + 失败率上升。同设置：2048 SQuAD，2×4090，cap=64，temp=0，prefix-on。

## A. duckdb_ai_lb_rr @64（现实单入口，固有欠喂）

| 臂 | mean tok/s | CV | n | % of ceiling (89287) |
|---|---|---|---|---|
| duckdb_ai_lb_rr c=64（1 DuckDB→nginx:8500→2 backend） | **72480** | 0.6% | 3/3 | **81.2%** |

**配置理由**：lb_rr 是单 DuckDB 进程经 nginx round-robin 到 2 backend。要每 backend ~32（饱和点），单进程需总并发 64（nginx 各分 ~32）。实测：峰值 backend running ~27，但**平均 ~10/backend**——DuckDB-ai 扩展**单进程的持续 in-flight 只有 ~20**（不是配置的 64），突发不持续。

**结论（事实）**：lb_rr @64 = 72480（ceiling 的 81.2%，CV 0.6%）。比饱和三臂（sharded 87.5%、project 88.2%）低 ~7pp。**不是配置错误，是单入口架构的固有极限**——1 个 DuckDB 进程的扩展持续并发喂不饱两张卡（avg ~10/backend << 32 饱和点）。

**不能声称**：lb_rr 是"饱和竞争臂"——它**固有欠喂**（单进程限制），与饱和三臂不在同一基线。它的价值是**对照**："现实单入口部署在多卡上喂不饱"。

**为什么 sharded（87.5%）> lb_rr（81.2%）**：sharded 用 **2 个 DuckDB 进程**（2× 扩展持续并发 → 每 backend ~32，饱和）；lb_rr 用 **1 个进程**（扩展持续并发 ~20 → 每 backend ~10，欠喂）。多进程是喂饱多卡的必要条件。

## B. bounded_http 过订阅塌陷曲线（c=64/128/256）

c=32 是饱和峰值（89287，主 formal）。越过饱和点（更高并发）的塌陷：

| concurrency/endpoint | mean tok/s | CV | n 成功/3 formal | % of c=32 峰值 |
|---|---|---|---|---|
| 32（饱和峰值） | 89287 | 0.4% | 3/3 | 100% |
| **64（过订阅）** | **37341** | 0.6% | **2/3** | **42%** |
| **128（过订阅）** | **25026** | — | **1/3** | **28%** |
| **256（过订阅）** | — | — | **0/3 全失败** | — |

**事实**：越过饱和点后，吞吐**单调塌陷**（100%→42%→28%→全失败），**失败率随并发上升**（0%→33%→67%→100%）。c=256 三次 formal 全失败（shard 崩溃）。

**机制**：2×4090 + Qwen2.5-7B + SQuAD（max_model_len 8192），vLLM 在 ~64 总并发（32/endpoint）达饱和；更高并发 → KV/调度过载 → 吞吐塌陷 + 不稳定（部分 run 崩溃）。

**对课题含义**：
- **饱和点是真实约束**：多卡 baseline 必须**校准到饱和点**（~32/endpoint），不能盲目堆并发——越过即塌陷。这印证 AGENTS §7.5C 的 feeding-saturation 门禁（先找饱和点，固定，不在线调参）。
- **过订阅塌陷是系统属性**，不是某个臂的 bug——bounded_http（最 lean 的客户端）也塌陷，说明是 vLLM 服务侧的过载，与上游臂无关。

## C. 证据

- `collapse.log`：lb_rr@64 + 塌陷曲线 1w+3f driver 输出（mean/CV/n/reps）。
- `ps8_collapse/`：bounded c=64/128/256 每 repeat 的 gate shard summaries（成功的）。
- `lbrr64_*/`：lb_rr 每 repeat 的 runner report。
- 主 formal（饱和三臂）见上一级 README + `ps8_formal/`。

> **诚实边界**：lb_rr@64 是现实单入口的**固有欠喂**数据点（81% ceiling，单进程限制），不进饱和排名；塌陷曲线是系统过订阅行为（c≥64 塌陷 + 失败率升），c=256 全失败。两者都是 formal 1w+3f（CV<1% where n≥2），数据干净。
