# 多卡 rich-metric 饱和 screening（2×4090，SQuAD 2048，1w+3f）

> **定位**：3 臂（bounded_http 天花板 + **duckdb_ai 2×1 sharded〔身份订正：`harness_sharded_diagnostic`——harness 预切 manifest + 2 独立 DuckDB 进程，DuckDB `ai` 单 BASE_URL，按协议 §2.6 不算产品原生多 endpoint、不进产品主排名〕** + project_static 2-endpoint）在饱和配置（c=32/K=32）下的 cache-hot 饱和 screening/gate 证据。

> **vLLM effective config（诚实）**：`max_num_seqs=256/max_num_batched_tokens=8192` 是 adapter 声明；vLLM cmdline 无这些 flag，用 vllm 0.25.1 默认；`enable_prefix_caching` 默认 ON（= 声明，巧合）。数据有效，service config 字段是声明非 effective。**不是项目定义的 formal ranking**（gate 代码拒绝 `formal=true`）。本报告由**已提交的可复现聚合器** `code/scripts/analysis/multicard_rich_aggregate.py` 从原始证据重算，**取代** ad-hoc `rich_results.json`（后者未提交、且含三处报告错误，见 §6 修订记录）。

## 1. 实验目的（问题 + 方法 + 关系）

**问题**：在 2×RTX 4090 上，项目的多 endpoint 上游调度（project_static，per-endpoint K admission + Ray actor pool）相对 DuckDB-ai 的 2×1 static-shard baseline，在**两者都喂饱 GPU（达系统最佳性能点）**的条件下，vLLM 服务吞吐与质量有没有差距？

**为什么这样问**：256 行、K=8 的 preliminary 让项目"看着慢 40%"——但 K=8 远低于饱和点，是**人为欠喂**。要公平对比，必须先找到系统饱和点（bounded sweep 得 c=32/endpoint 为峰值，c=64 起塌陷），让所有臂在最佳性能点比。

## 2. 实验设置

| 项 | 值 |
|---|---|
| 平台 | AutoDL 2×RTX 4090；vLLM 0.25.1，双 endpoint 8000(GPU0)/8001(GPU1)，qwen2.5-7b，prefix-cache enabled，max_model_len 8192，gpu_mem_util 0.90 |
| workload | SQuAD v1.1 dev short-answer，**2048 行**（cap=64, temp=0, chat_completions, httpx_async） |
| 分片 | equal_rows（sha256(doc_id) 排序 round-robin，**1024:1024**，manifest sidecar 可证） |
| 饱和配置 | **c=32/K=32 per endpoint**（见 §3 sweep 依据） |
| bounded_http | concurrency_per_endpoint=32（服务天花板参照） |
| duckdb_ai sharded | 2 独立进程各绑一 endpoint，max_concurrent_requests=32 |
| project_static | K=32/endpoint，8 actors × 4 concurrency = 32 slots，active-work=65536，token_budget=6144，admission-scope per_endpoint |
| 度量 | **service tokens/s（unified = vLLM 服务计数器 token delta ÷ model serving wall）**——跨臂同口径；adapter wall / e2e 只作定位 |
| 重复 | 1 warmup + 3 formal（formal 报 mean ± CV） |
| 参考 | `squad_eq2048_references.json`（2048 子集 doc_id→answers，committed evidence） |

## 3. 合规性自检（feeding-saturation 门禁）

**bounded_http 2-ep 并发饱和 sweep（2048，纯 service tokens/s）**：

| concurrency/endpoint | service tok/s | group_wall | 成功率 |
|---|---|---|---|
| 1 | 4283 | — | 3/3（ramp 起点）|
| **32** | **86274–89420** | 4.8–5.1s（**峰值**）| 3/3 |
| 64 | 36655–37341 | ~12s | 2/3（塌陷，42% peak）|
| 128 | 25115–25026 | ~17s | 1/3（塌陷，28% peak）|
| 256 | — | shard 崩溃 | 0/3 |

→ **系统饱和点 ≈ 32 concurrent/endpoint**（更高并发 vLLM 过载塌陷）。三臂固定在 c=32/K=32。

**喂饱度**（service tokens / model serving wall）：bounded=天花板 100%；duckdb=87.0%；project=88.5%。CV<1%（极稳）。exactly-once + 0 error（gate 全 passed）。

**注意（诚实边界）**：项目规则的 feeding parity ≥95% 门禁**未达**——duckdb/project 均在 87–89%，未到 95%。这是"饱和 screening"，不是"已通过 feeding-parity 的 formal ranking"。GPU 利用率也未稳定 ≥80%（见 §5）。

## 4. 实验设计

三臂、同 workload（2048 SQuAD）、同 endpoint/model/cap、**饱和配置**、统一度量（service tokens/s）、1w+3f。bounded_http 作服务天花板参照（feeding-saturation 的同协议 bounded 基线）；duckdb_ai sharded 是 2×1 static-shard baseline；project_static 是项目冻结静态方法（2 endpoint，per-endpoint K=32）。

## 5. 实验数据（committed aggregator 重算，1w+3f）

### 5.1 吞吐（unified service tokens/s）

| 臂 | mean tok/s | CV | reps | % of ceiling |
|---|---|---|---|---|
| bounded_http c=32（天花板） | **89420** | 0.4% | [89788, 89512, 88961] | 100% |
| duckdb_ai sharded c=32 | **77764** | 0.9% | [78513, 77940, 76838] | **87.0%** |
| project_static K=32（8×4） | **79111** | 0.9% | [78644, 80111, 78578] | **88.5%** |

### 5.2 质量（SQuAD EM/F1，**2048 子集分母**——修正点）

| 臂 | EM%（2048 子集） | EM%（10570 全量，旧错误值，仅审计） | F1%（2048 子集） | correct_rows |
|---|---|---|---|---|
| bounded_http | **82.26** | 15.93 | ~89 | 1685 |
| duckdb_ai | **82.34** | 15.96 | ~89 | 1686 |
| project_static | **82.31** | 15.95 | ~89 | 1686 |

→ 三臂 EM ~82.3%、correct_rows ~1685（**未观察到执行引擎引入的 EM/F1 下降**；注：无预注册 equivalence margin/TOST，"未观察到下降"≠"证明无质量偏差"）。

### 5.3 延迟

| 臂 | per-req E2E P50 | TTFT P50（真实 vLLM 首 token） | submit→service P50 |
|---|---|---|---|
| bounded_http | 2.35s | **未采集**（summary 无 TTFT 直方图，pending re-run）| — |
| duckdb_ai | 5.5s（barrier）| **未采集**（同上）| — |
| project_static | 3.6–6.0s | **52.2ms**（formal0，vLLM `/metrics` 直方图）| ~1.6ms |

> **修正（codex audit #2）**：旧 README 把 1.6ms 写成 "TTFT"，实际它是 `submit_to_service_s`（actor 提交→服务开始）。真实 vLLM TTFT P50 = **52.2 / 51.9 / 52.6 ms**（formal0/1/2），P95 ~74.7ms。来源：`project2_*.csv::vllm_time_to_first_token_p50_s`。

### 5.4 项目调度开销（codex audit #7，关键发现）

| repeat | scheduling_control_overhead_pct | submit_s | writeback_s | e2e_s |
|---|---|---|---|---|
| formal0 | **36.97%** | 1.958s | 0.050s | 6.76s |
| formal1 | **34.79%** | 1.803s | 0.048s | 8.24s |
| formal2 | **35.83%** | 1.899s | 0.048s | 9.23s |

→ **项目调度控制开销 ~35–37%**（submit 阶段 1.8–2.0s）。这是当前项目只达 bounded 88.5%（而非 100%）的**主要线索**：上游调度/提交控制本身吃掉了相当比例的 wall，值得后续重点分析。写回（writeback ~0.05s）可忽略，**不是瓶颈**。

### 5.5 GPU 利用率 / 功耗

| 臂 | GPU0 util mean | GPU1 util mean | 备注 |
|---|---|---|---|
| bounded_http | 75–80% | 75–80% | summary 计数；**原始时序 CSV 未保留**（不可独立复算）|
| duckdb_ai | ~65% | ~64% | 同上 |
| project_static | 54–75%（gpu0）| **未采集**（profiler 资源 trace 每样本只记 gpu0）| n=16 稀疏样本 |

## 6. 修订记录（codex audit 修正）

本报告修正了 ad-hoc `rich_results.json` 的三处错误：

1. **EM 分母错误**：旧值 15.96% 用了 10570 全量分母 + 2048 预测；正确 2048 子集分母 → **82.3%**。（§5.2）
2. **TTFT 误标**：旧 "TTFT 1.6ms" 实为 submit→service；真实 TTFT P50 = **52ms**。（§5.3）
3. **project 读取失败**：旧 `n_outputs=0`/EM 0.03% 是 ad-hoc 脚本找错 evidence 文件名；实际 2048 行输出齐全，EM = **82.31%**。（§5.2）

另外修正可复现性：生成报告的脚本现已提交（`multicard_rich_aggregate.py`），参考答案作为 evidence 提交（`squad_eq2048_references.json`）。

## 7. 仍缺（pending 统一 re-run）

| 缺口 | 影响 | 计划 |
|---|---|---|
| bounded/duckdb **TTFT** | 无法跨臂比首 token 延迟 | re-run 时在 gate summary 加 `/metrics` 直方图 stamp |
| bounded/duckdb **原始 GPU 时序 CSV** | util/power 不可独立复算 | re-run 时给 gate cell 接 dual-GPU sampler |
| **c=2/4/8/16** 全臂 sweep | 只有 c=32 + 塌陷点，缺完整 ramp 曲线 | 统一 sweep driver |
| project **same-manifest** | project 用运行时 round-robin（1010:1038），非 manifest 1024:1024 | re-run 时 project 也用 manifest 校验 |
| 全臂统一 database-E2E 计时边界 | service tokens/s 已公平，wall 不可跨臂排名 | 后续 |

## 8. 结果解释（事实 / 推断 / 不能声称）

- **事实**：饱和配置下 project_static（79111）与 duckdb_ai harness_sharded_diagnostic（77764）service tokens/s 接近（项目高 1.7%，3 reps），CV<1%；bounded 天花板 89420。三臂 EM ~82.3%、correct_rows ~1685。project 真实 TTFT P50 52ms；调度开销 35–37%。
- **推断**：SQuAD 均匀短答案 + 饱和配置下，项目静态调度的服务吞吐与 DuckDB-ai harness 诊断**未检出显著差异**（无预注册 equivalence margin/TOST，3 reps；"未检出"≠"证明等价"）；项目未达天花板的主要可归因线索是**上游调度控制开销（35–37%）**，而非写回（0.05s）。
- **不能声称**：
  - 项目"优于"DuckDB-ai（1.7% 在 CV 内，非显著）。
  - 项目的**自适应**调度价值（static 不 exercise 自适应）。
  - SQuAD 均匀长度下的结论外推到**倾斜长度** workload。
  - 跨臂 **wall 排名**（计时边界不同；service tokens/s 是公平口径）。
  - "三臂都已充分饱和 / 通过 feeding-parity"（duckdb/project 87–89% < 95% 门槛）。

## 9. 下一步

1. **统一 re-run**（B1+B2+C）：bounded/duckdb TTFT + dual-GPU 采样 + c{2,4,8,16,32} 全臂 sweep + project same-manifest。
2. **project_smart**（自适应臂）——项目真正贡献在自适应；要 smart vs static 净收益。
3. **倾斜长度 workload**（ShareGPT/BurstGPT 变长输出）——均匀长度测不出调度价值。
4. **统一 database-E2E 计时边界**。

## 10. 证据

`raw/rich_formal/` 下：
- `bounded_http_{warmup,formal0-2}/`、`duckdb_ai_{warmup,formal0-2}/`：gate shard summaries（requests.csv + service counters）。
- `project2_{warmup,formal0-2}.{csv,_evidence.csv,_resource.csv,_trace.csv}`：profiler 输出 + 完成证据（output_text）+ 请求 trace（TTFT）+ 资源 trace（GPU util/power）。
- `squad_eq2048.jsonl` + `.meta.json`：2048 行 equal-rows manifest + partition provenance（1024:1024）。
- `squad_eq2048_references.json`：2048 子集参考答案（committed evidence，聚合器用它算 EM/F1）。
- `rich_results_corrected.json`：committed aggregator 重算结果（取代 ad-hoc `rich_results.json`）。
- 代码：`code/scripts/analysis/multicard_rich_aggregate.py`（可复现聚合）。

> **诚实边界**：这是饱和配置下 3 臂 cache-hot screening（prefix hit ~95.8%，不可外推到 cache-cold）。**formal=false**，**未通过 feeding-parity ≥95% 门槛**（duckdb/project 87–89%），**bounded/duckdb 缺 TTFT 与原始 GPU 时序**（pending re-run），**project 未用 same-manifest**。完整矩阵 + 自适应价值 + 倾斜 workload 是后续。
