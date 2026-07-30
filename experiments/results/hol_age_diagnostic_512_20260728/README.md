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
