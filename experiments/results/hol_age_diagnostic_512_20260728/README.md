# HOL-age 诊断实验（2026-07-28 实际运行）

配套设计文档：`../hol_age_diagnostic_512_20260727/README.md`（设置 A：单作业 512，预注册判据）。

## 1. 目的

验证「诊断优先」假设：把 `adaptive_admission_controller_20260726/` 诊断点名的两个信号盲区修上后，**动态控制能否第一次击败同上限静态 `static_k16`**。

- 杠杆①：控制器读 **Ray 侧排头请求年龄（HOL-age）**（`HolAgeAimdAdmissionController`，不依赖恒为 0 的 `vllm:num_requests_waiting`）。
- 杠杆②：**request-level replenishment**（`--ray-batch-rows 1` 让 1 submission = 1 请求，`wait_one` 逐请求回收）。

## 2. 设置

- 6 臂 ×（1 warmup + 3 formal），formal 随机交错，24/24 ok。
- 环境（按 07-27 设计；runs.csv 确认 server=PG18.4、pgvector 0.8.5、workload=sharegpt_burstgpt、token_budget=6144、executor=ray_task）：单 RTX 5070，vLLM 0.25.1 + Qwen2.5-1.5B，prefix cache off，CUDA Graph on，本地 PG18.4，arrival replay ×0.0005，flush fixed_timeout 50ms。
- 主指标：SLO-goodput（req/s）+ request P99；次：tokens/s。预注册「正向」判据：`aimd_hol`/`replenish_static_k8`/`aimd_hol_replenish` 之一在 SLO-goodput 上相对 `static_k16` >+5% 且 SLO 违约 <1%。

## 3. 结果（formal 中位数）

| arm | SLO-goodput/s | request P99(s) | tokens/s | vs static_k16 |
|---|---:|---:|---:|---:|
| `static_k16`（同上限基线） | 15.27 | 11.78 | 6,829 | — |
| `aimd_vllm_wait`（现状 AIMD） | 15.38 | 11.82 | 6,894 | +0.7%（不可分辨，符合预期） |
| `static_k8`（guardrail） | 11.47 | 22.83 | 5,153 | −24.9% |
| `aimd_hol`（HOL-age 信号） | 6.78 | 52.81 | 3,037 | **−55.6%** |
| `replenish_static_k8`（request-level） | 4.62 | 86.77 | 2,059 | −69.7% |
| `aimd_hol_replenish`（双杠杆） | 2.91 | 149.88 | 1,292 | **−81.0%** |

## 4. 结论

**负向——「诊断优先」假设被强否定。**

- `aimd_vllm_wait` 与 `static_k16` 不可分辨（+0.7%），再次确认既有 AIMD 的表观增益来自并发升至 K≈16，而非控制律。
- 三个新臂（`aimd_hol` / `replenish_static_k8` / `aimd_hol_replenish`）SLO-goodput 反而**大幅低于** `static_k16`（−56% / −70% / −81%），P99 恶化 4–13×。
- 即：换上「正确」的 HOL-age 信号 + request-level replenishment 后，动态控制在稳态下**仍不优于最佳静态 `static_k16`**，且会触发更差的尾延迟。锁定刻画型 framing：上游调度层在当前 vLLM continuous-batching 饱和点下，动态 K 相对同上限静态无额外空间。

## 5. 不能声称 / 边界

- 不能声称「HOL-age 信号本身有害」——只测了单 GPU / Qwen2.5-1.5B / prefix-cache-off / 本地 PG18.4 / arrival replay ×0.0005；结论不可外推多 GPU、多模态或 PG18.3 平台。
- `replenish_*` 臂的 `--ray-batch-rows 1` 同时改变 vLLM 内部 batching 形态，非纯隔离「逐请求释放」（详见 07-27 设计 §风险），仅作首步探针。
- runs.csv 为旧格式，无 `model_request_tokens_per_s` 与 GPU 列（tokens/s 用运行级 `tokens_per_s`）；`request_slo_violation_ratio` 列对高 P99 臂仍记 0，与 SLO-goodput 下降不一致，故主判据以 `request_slo_goodput_per_s` 为准，违约比例不单独报告。
- 07-27 目录是该实验的**预注册设计 + 配置**（当日本机无 GPU 未运行），实际执行在本目录。

## 6. 证据分级（p6）

对 §3–§5 每条结论打证据层级标签（本地实验事实 / 合理推断 / 待确认 / 不能声称）：

- **§3 结果表的 6 臂中位数**（SLO-goodput 15.27 / 15.38 / 11.47 / 6.78 / 4.62 / 2.91 req/s；P99 11.78–149.88 s；tokens/s 1,292–6,894；vs static_k16 −81.0%~+0.7%）——**本地实验事实**（`runs.csv` formal×3 中位数，已逐项复核）。
- **「aimd_vllm_wait 与 static_k16 不可分辨（+0.7%）」**——**本地实验事实**（`runs.csv`：两者 SLO-goodput 中位数 15.38 vs 15.27，P99 11.82 vs 11.78 s）。
- **「既有 AIMD 表观增益来自并发升至 K≈16，而非控制律」**——**合理推断**（`runs.csv`：`adaptive_limit_mean`=15.953 贴在 max=16；与 static_k16 不可分辨 ⇒ 增益等价于"静态 K=16"，但"控制律贡献为零"是解释，未单独消融控制律）。
- **「三个新臂 SLO-goodput 大幅低于 static_k16（−56% / −70% / −81%），P99 恶化 4–13×」**——**本地实验事实**（`runs.csv` formal 中位数；P99 恶化倍数实测 52.81/11.78≈4.5×、86.77/11.78≈7.4×、149.88/11.78≈12.7×）。
- **「换上 HOL-age 信号 + request-level replenishment 后，动态控制稳态下仍不优于最佳静态、且尾延迟更差」**——前半句（该配置下不优于 static_k16）为**本地实验事实**；"稳态下"概括（3 次 formal repeat）为**合理推断**，未做更长运行或更多 seed。
- **「上游调度层在当前 vLLM continuous-batching 饱和点下，动态 K 相对同上限静态无额外空间」（§4 锁定的刻画型 framing）**——**合理推断**，超出原始 SLO-goodput / P99 测量。原始数据只直接支持"三个动态臂在该配置下不优于 static_k16"；`runs.csv` 进一步显示 `aimd_hol` 的 `vllm_running_mean`=10.45（约为 static_k16 28.92 的 36%）、`adaptive_limit_mean`=4.726（贴 min-window=4，远低于 max=16）、`mfu_estimate`=0.0573（约为 static_k16 0.1322 的 43%）——这更提示控制器**收缩偏置、欠驱动 vLLM**，与"vLLM 已饱和、动态无空间"是两条相竞争的解释，当前数据不足以区分，故 framing 仅作**合理推断**，不作定论。
- **§5 全部边界**（不能外推多 GPU / 多模态 / PG18.3；replenish 非纯隔离）——**不能声称**（外推边界）；**`request_slo_violation_ratio` 记 0 与 SLO-goodput 下降不一致**——**待确认**（根因未定位，故主判据改用 SLO-goodput）。

## 7. 下一步（p7）

§4 的 framing 是**合理推断**且与"控制器收缩偏置"解释相竞争，需以下消融区分。三项均复用本目录 6 臂骨架，只换一处变量，本地单机即可跑：

1. **不同饱和度 regime / 多 GPU 复现（检验 null 是否 regime-specific）。** 本结果仅在 arrival ×0.0005、单 GPU、prefix-cache-off 取得；`runs.csv` 显示 static_k16 稳态 `vllm_waiting_mean`≈0（无排队），HOL-age 信号在"无排队"regime 下携带信息有限。下一步提高 `--arrival-time-scale`（×0.001 / ×0.002）制造排队 regime，或迁到双 4090 多 endpoint（见 `../checkpoint_b_service_quantum_gate_20260729/`），重跑 aimd_hol vs static_k16。判据同主实验：SLO-goodput >+5% 且违约 <1% 才算"动态有空间"。
2. **HOL-age 信号隔离探针（解耦 `--ray-batch-rows`，并诊断收缩偏置）。** §5 已注明 `aimd_hol_replenish` 的 `--ray-batch-rows 1` 改变 vLLM batching 形态；但 `aimd_hol` 臂（ray-batch-rows 64，与 static_k16 同形态）单独已 −55.6%，且 `adaptive_limit_mean`=4.726 贴下限——疑似控制器收缩偏置。下一步加一个 HOL-age 死区 / 地板探针（如 `--controller-min-window 12`，禁止 K 落到 4–11）：若限幅后 aimd_hol 吞吐恢复到与 static_k16 不可分辨（<5%），则问题在控制律收缩偏置而非 HOL 信号，§4 framing 应改写为"控制律未调好"而非"动态 K 无空间"。
3. **更窄 K 动作集。** aimd_hol 现为 min=4 / max=16 / init=8，稳态落在 4.726。下一步把动作集收紧到 {12, 14, 16}（或 min=12 / init=14），强制控制器只在 static_k16 工作点附近探索。若窄动作集下 aimd_hol 与 static_k16 不可分辨（<5%），则"动态无空间"是当前宽动作集 + 收缩偏置的产物；若仍显著更差，则 HOL 信号本身在该 regime 下确无增益，§4 framing 可升级为**本地实验事实**。

优先级 (2) > (1) > (3)：(2) 直接决定 §4 framing 是否需要改写。
