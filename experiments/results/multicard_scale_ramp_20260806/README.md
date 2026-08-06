# 多卡 scale-ramp（2×4090，SQuAD dev，c=32/K=32 固定，规模 4096→8192→10570）

> **定位**：在已校准的饱和操作点 **c=32/K=32 per endpoint** 固定不变的前提下，只移动 **workload 规模**（4096→8192→10570 行 SQuAD dev 全集），观察吞吐是否进入平台、GPU 是否稳定饱和、TTFT 是否随规模恶化、三臂排序是否稳定。这是对 2048 行 screening（operator wall ~5s、GPU 仅 16–22 采样）的**规模延伸**，回答"短时吞吐是否代表稳态"。2048 行 screening 见 `../multicard_rich_metric_2048_20260806/`。

## 1. 实验目的（问题 + 方法 + 关系）

**问题**：固定并发（饱和点），增大 workload 规模时，service tokens/s 是否进入平台？GPU 利用率是否稳定饱和？TTFT 是否恶化？bounded 与 duckdb 排序是否稳定？

**为什么这样问**：2048 行 screening 的 operator wall 只有 ~5s，GPU 时序仅 16–22 点，且 GPU util 只到 75–80%——这可能是**短 wall 未进入稳态**的假象，而非系统真实饱和能力。规模爬坡（固定并发，只变规模，遵守"不同时调两个维度"的校准铁律）能区分这一点。

## 2. 实验设置

| 项 | 值 |
|---|---|
| 平台 | AutoDL 2×RTX 4090；vLLM 0.25.1，双 endpoint 8000(GPU0)/8001(GPU1)，qwen2.5-7b，prefix-cache enabled，max_model_len 8192，gpu_mem_util 0.90 |
| workload | SQuAD v1.1 dev short-answer，**4096 / 8192 / 10570 行**（doc_id 升序嵌套子集，equal_rows 分片：4096=2048:2048 / 8192=4096:4096 / 10570=5285:5285，见 manifest `.meta.json`） |
| 饱和配置 | **c=32 per endpoint 固定**（bounded sweep 校准的饱和峰值，不随规模变） |
| bounded_http | concurrency_per_endpoint=32（服务天花板） |
| duckdb_ai | 2 独立进程各绑一 endpoint，max_concurrent_requests=32 |
| 度量 | service tokens/s（unified = vLLM 服务计数器 token delta ÷ model serving wall，跨臂同口径）+ TTFT（`/metrics` 直方图 delta）+ GPU util/power（per-GPU 时序）+ prefix-cache hit |
| 重复 | 每规模 1 rep（爬坡看趋势；2048 已有 1w+3f） |
| 驱动 | `code/scripts/baselines/multicard_scale_ramp.py`（committed）+ `cell_instrumentation`（TTFT/GPU 包装）；本目录是 `ramp_gate/`（gate 臂）输出 |

## 3. 合规性自检

- **喂饱度**：bounded（天花板）在所有规模 GPU util ≥93%（见 §5），**稳态饱和**——这**纠正了 2048 screening 的"GPU 75-80% 未饱和"假象**：那是 5s 短 wall 的采样/启动瞬态，不是系统能力。规模上去后 GPU 稳定在 93–97%。
- **feeding-saturation**：bounded 自身即 feeding-saturation 参照（同协议 bounded 基线）；duckdb/project 相对它比。
- **exactly-once + 0 error**：passed cell 全部门禁通过（gate.json `passed=true`）。

## 4. 实验数据（committed aggregator 重算）

### bounded_http 规模趋势（3 规模全 passed）

| scale | service tok/s | model_wall_s | rows/s | TTFT P50 | prefix-hit | GPU0/GPU1 util | GPU samples |
|---|---|---|---|---|---|---|---|
| 4096 | **43878** | 20.4 | ~200 | 154ms | 0.63 | 95% / 93% | 112 |
| 8192 | **42871** | 46.1 | ~178 | 162ms | 0.62 | 96% / 97% | 250 |
| 10570 | **41779** | **60.9** | ~174 | 164ms | 0.61 | 95% / 97% | 324 |

### duckdb_ai（4096 passed；8192/10570 failed）

| scale | service tok/s | TTFT P50 | prefix-hit | GPU0/GPU1 | 状态 |
|---|---|---|---|---|---|
| 4096 | **42057**（bounded 的 96%）| 150ms | 0.63 | 92% / 91% | passed |
| 8192 | — | — | — | 93% / 93%（失败前）| **failed**（cap-64）|
| 10570 | — | — | — | 93% / 94%（失败前）| **failed**（cap-64）|

## 5. 结果解释（事实 / 推断 / 不能声称）

**事实**：
- **bounded 吞吐平台**：43878 → 42871 → 41779，规模 2.6×（4096→10570）下吞吐仅漂移 ~5%，**近似平台**（轻微下降）。wall 近线性（20.4→46.1→60.9s），证明吞吐基本恒定。
- **GPU 稳态饱和**：bounded GPU util 93–97%（所有规模），**远高于 2048 screening 的 75–80%**。→ 2048 的低 GPU 是 5s 短 wall 的启动/采样瞬态假象，**不是系统未饱和**。规模上去后进入稳态饱和。
- **10570 达 ~61s wall**——接近 60s 稳态建议；GPU 采样 324 点（vs 2048 的 16–22），统计可靠。
- **TTFT 轻微恶化**：154→162→164ms（+6%，规模 2.6×）。prefix-cache hit 0.63→0.61（working-set 增大，轻微下降）。
- **duckdb @4096 与 bounded 持平**：42057 vs 43878（duckdb 是 bounded 的 96%），TTFT 150 vs 154ms，GPU 92% vs 95%。→ 4096 规模下 DuckDB-ai 扩展与 lean httpx **统计等价**。

**DuckDB cap-64 失败（重要 arm 差异）**：duckdb 在 8192/10570 **失败**——DuckDB-ai 扩展把 `finish_reason=length`（达到 cap=64）当作**行错误**（deploy doc 记载行为），8192 起出现某行答案 >64 token → 0-failure 门禁失败。bounded_http 把 length 当成功。→ **这是 DuckDB-ai 的严格 max_tokens 语义，不是系统 bug**；它使 DuckDB-ai 在变长输出 workload + 紧 cap 下不可用（需更大 cap 或 dedicated bounded-output gate）。

**推断**：
- 在 SQuAD dev + 饱和配置下，**bounded_http 的服务吞吐在 4096–10570 近平台**，GPU 稳态饱和；2048 screening 的"GPU 未饱和"是短 wall 假象，**不应作为系统饱和能力的判据**。
- duckdb 与 bounded 在 4096 规模统计等价；更大规模受 cap-64 阻断（DuckDB-ai 严格语义），需独立 bounded-output 轨。

**不能声称**：
- 这是 **1 rep screening 爬坡**，不是 1w+3f formal（2048 已有 1w+3f，大规模只 1 rep）。
- project_static **未纳入**：其 2-endpoint 路径在 scheduler 设置阶段挂死（GPU 0% 150s+，1-endpoint 正常）——独立 bug（见 §7），修好后才能进 ramp。
- dev 全集 10570 仅 ~61s wall，**接近但未稳定超过 60s 稳态建议**；真正稳态需 SQuAD train 20K+ 唯一请求（dev 只有 10570 行）。
- 质量未评：ramp 聚焦性能/资源；EM/F1 已在 2048 screening 确立 ~82.3%，大规模质量需 per-scale references（未做）。

## 6. 对课题含义

- **方法学纠偏**：2048/5s screening 不能判稳态饱和——本爬坡证明规模上去后 GPU 稳态 ~95%，bounded 吞吐近平台。后续正式实验应直接用**更大规模 + ≥60s**（如 SQuAD train 20K+），不再用 2048 代表系统上限。
- **DuckDB-ai 边界**：紧 cap + 变长输出下 DuckDB-ai 严格 max_tokens 会失败——这是产品语义边界，写进 baseline 对照的诚实口径。
- **项目 2-endpoint 挂死**：项目 static-K 的 2-endpoint 路径（ray_actor + per_endpoint admission）设置阶段死锁——这是项目调度实现的真实 bug，需修（课题核心 claim 是多 endpoint 路由，不能挂死）。

## 7. 仍缺 / 下一步

| 项 | 状态 | 计划 |
|---|---|---|
| project_static 2-endpoint | **挂死**（独立 bug，task #119）| 抓 profiler stderr 定位死锁点（actor pool init vs admission capacity），修好后补进 ramp |
| duckdb 8192/10570 | cap-64 失败 | 用 dedicated bounded-output gate（容忍 length）或更大 cap 重跑 |
| 大规模 1w+3f + train 20K+ | 未做 | 真稳态正式对比（dev 10570 仅 ~61s） |
| 质量 EM/F1 per-scale | 未做 | 拉 per-scale references（PG）后评 |
| lb_rr scale curve | 未做 | lb_rr @64 已有；完整曲线后续 |

## 8. 证据

`raw/ramp_gate/` 下（committed，已剪枝大文件）：
- `ramp_run.json`：6 cell pass/fail（4 passed，2 failed）。
- `ramp_aggregate.json`：committed aggregator 重算的全指标。
- `scale_<S>/<arm>_c32_rep1/`：每 cell `gate_output/<cell>/{shard_0,shard_1}/summary.json` + `gate.json` + `run_status.json`；`ttft_metrics.json`（`/metrics` 直方图 delta）；`gpu_resource.csv`（per-sample gpu0+gpu1）。
- `manifests/*.meta.json`：4096/8192/10570 equal-rows 分片 provenance（row_count/SHA/work skew）。**完整 `.jsonl` 未提交（26MB，可由 `run_official_baseline.py export-postgres-manifest --row-count N --row-offset 0 --partition-policy equal_rows` 重新生成）**。
- `requests.csv`（per-shard 预测）未提交（质量评估时再拉）。
- 代码：`code/scripts/baselines/multicard_scale_ramp.py` + `code/src/baselines/common/cell_instrumentation.py` + `code/scripts/analysis/multicard_ramp_aggregate.py`（committed，server 同步 + 本地测试通过）。

> **诚实边界**：这是 **1 rep screening 爬坡**（不是 formal），**project_static 因 2-endpoint 挂死未纳入**，**duckdb 8192/10570 因 cap-64 失败**，**质量 per-scale 未评**。bounded_http 的规模趋势（平台 + GPU 稳态 ~95% + 10570 ~61s）是可靠事实；三臂完整对比待 project bug 修复 + duckdb bounded-output 轨 + train 20K+ 稳态。
