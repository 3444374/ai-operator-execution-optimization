# HOL-age 诊断实验（设置 A:单作业 512,2026-07-27）

## 目的

验证「诊断优先」假设:把信号盲区诊断点名的两个杠杆修上后,**动态控制能否第一次击败同上限静态 K=16**。

- 背景:`aimd`/`ewma_aimd`/`pid` 相对 `static K=8` 的 +46% tokens/s **全部来自并发升到 K≈16**,加同上限 `static K=16` 对照后不可分辨(±0.7%)。trace 诊断为控制器读 `vllm:num_requests_waiting`(恒为 0),看不见请求在 Ray 侧排队;credit 又按 submission 整体回收(产生 HOL)。
- 两个杠杆:① 控制器读 **Ray 侧排头请求年龄(HOL-age)**(`HolAgeAimdAdmissionController`,不依赖 vLLM 队列);② **request-level replenishment**(`--ray-batch-rows 1` 让 1 submission = 1 请求,`wait_one` 回收即逐请求)。

## Arm 集(6 个 × 3 formal repeat,warmup 1)

| scenario_id | 策略 | 关键变量 |
|---|---|---|
| `static_k16` | static K=16 | 同上限基线(判据对照) |
| `static_k8` | static K=8 | guardrail 点 |
| `aimd_vllm_wait` | AIMD,vLLM-waiting 信号 | 现状基线(预期与 K=16 不可分辨) |
| `aimd_hol` | AIMD,**HOL-age 信号** | 杠杆①单独 |
| `replenish_static_k8` | static K=8 + `--ray-batch-rows 1` | 杠杆②单独(request-level) |
| `aimd_hol_replenish` | HOL-age + request-level | 杠杆①②叠加 |

环境:单 RTX 5070,vLLM 0.25.1 + Qwen2.5-1.5B,prefix cache off,CUDA Graph on,本地 PG18.4,ShareGPT/BurstGPT,token_budget 6144,arrival replay ×0.0005,flush fixed_timeout 50ms。

## 预注册判据(先定死)

- **主指标**:SLO-goodput(req/s)与 request P99;**次指标**:tokens/s(防"买延迟卖吞吐")。
- **"正向"判据**:至少一个新 arm(`aimd_hol` / `aimd_hol_replenish` / `replenish_static_k8`)在 SLO-goodput 上**相对 `static_k16` 提升 >5% 且超出合并 pooled std 的 95% 区间**,同时 SLO violation < 1%。
- **关键诚实性要求**:任何"动态优于静态"的声称,**必须对比同上限 `static_k16`**;不得只对比 `static_k8`(否则重复"并发红利被当成控制律贡献"的错误)。
- **满足正向** → 转正面方法贡献,再投资 Route B(Orca 式 streaming actor + 完整 SLO-aware flush)。
- **不满足** → 锁定刻画型 framing;正文写成"即使补上正确信号 + 请求级回收,稳态下动态仍不优于最佳静态"。

## 如何运行

> 需要先起 vLLM docker(Qwen2.5-1.5B,`--gpu-memory-utilization 0.75`,默认 `--enforce-eager` 关闭即 CUDA Graph on,`--no-enable-prefix-caching`)与 PG18.4 + pgvector 容器,并确保 ShareGPT/BurstGPT 数据已入库。

```bash
cd code
python scripts/run_ai_operator_scenarios.py \
  ../experiments/results/hol_age_diagnostic_512_20260727/scenario_config.json \
  --result-dir ../experiments/results/hol_age_diagnostic_512_20260727
```

设置 B(shared-vLLM 双作业,盲区复现点,前台 128 + 后台 512)另起:

```bash
cd code
python scripts/run_kmax_interference_experiment.py \
  --background-rows 512 \
  --background-static-kmax 8,16 \
  --include-aimd --include-aimd-hol \
  --repeats 3 --trace-dir ../experiments/results/hol_age_diagnostic_512_20260727/traces
```

> ⚠️ `run_kmax_interference_experiment.py` 的 `--background-rows` 默认 1024,与设计中的 512 不一致,必须显式 `--background-rows 512`。

## 状态

- **未运行**(2026-07-27):本机此 shell 无 vLLM/GPU/PG 环境。代码与配置已就绪、单测全绿。
- 预期产物:`runs.csv` / `summary_long.csv` / control trace(含 `hol_age_s`、`controller_action`);`aimd_hol` 的 control trace 应出现 `decrease`(对照 `aimd_vllm_wait` 的 0 decrease)——这是信号通路正确的首要冒烟证据。

## 边界

单卡 / 单模型 / prefix-cache-off / 本地 PG18.4 预演;`replenish_*` arm 的 `--ray-batch-rows 1` 同时改变 vLLM 内部 batching,非纯隔离"逐请求释放",仅作首步探针(详见计划文件 §风险)。结论不可外推多 GPU / 多模态 / PG18.3 平台。
