# SAOR fixed-envelope active-set 2-Job formal（2026-08-12，commit 69affc7e）

> **性质**：project-derived SAOR `saor_release` 状态感知准入 vs 5 个 baseline（direct / static / FIFO / DRR / external VTC），2×4090 + Qwen2.5-7B，2-Job（bulk 后台 + foreground@5s）guaranteed-overlap。固定包络（K128/W65536/actors8/concurrency32/token_budget6144/credit_quantum2048），同 request window 比。**非** 4-Job（未跑）。
>
> **最终结论**：formal 40/40 cell 完成、0 incident、exactly-once；但 **summarize 的 formal gate `status=failed`（fail-closed）**——`shared_drr` 和 `external_vtc` 在 formal repeat 2 未观察到 credit mechanism（pre-borrow / overlap reclaim / post-drain work-conservation），按 §12/§17 不进入策略胜出声明。SAOR 自身 mechanism 3/3 全过、FIFO 3/3 全过。

## provenance

- **代码**：git `69affc7e`（main，"Persist SAOR formal experiment contract"）。runner `code/scripts/experiments/run_shared_vllm_experiment.py`（统一十场景矩阵，1w+3f 确定性交错）；summarizer `code/scripts/analysis/summarize_saor_active_set.py`；readiness `code/scripts/analysis/audit_saor_formal_readiness.py`。
- **formal 合同 env**：`deploy/autodl/saor_active_set_formal.env.example`（69affc7e 新增），复制到仓库外 `/root/autodl-tmp/runtime/saor-active-set-formal.env`，source 顺序：先 `ai-operator-runtime.env`（DB/服务/机器），再 formal env（冻结实验合同），`set -a` 期间 source。
- **平台/服务**：AutoDL 2×RTX 4090（24G/卡，sm89）；PostgreSQL 18.4 + pgvector（extname `vector` 0.8.5）；2× vLLM endpoint 8000/8001（各绑 1 GPU），`/version` 自报 `0.25.1`，`max_num_batched_tokens=8192`、`max_num_seqs=256`、`gpu_memory_utilization=0.9`、prefix-cache ON、mfu-metrics ON、**scheduling-policy=fcfs**；模型 `qwen2.5-7b`（`/v1/chat/completions`，chat_completions 协议，raw prompt，return_token_ids）。Ray 2.56.1 head `127.0.0.1:6380`（1 节点 32 CPU / 2 GPU）。driver `/root/miniconda3/bin/python`（有意不含 vLLM，driver/服务环境隔离）。`COMPLETION_HTTP_KEEPALIVE_EXPIRY_S=4`（< vLLM/Uvicorn 5s server keepalive；zero-retry，任何 ReadError = incident）。
- **冻结合同值**（readiness `status=passed`，与 `saor_active_set_formal.env.example` 一致）：
  - K=128/endpoint，active work=65536/endpoint，actors=8/endpoint，actor concurrency=32，CPU=0.25/actor，token_budget=6144，credit_quantum=2048，SLO=30000ms。
  - workload=`sharegpt_multiturn`；manifests=`opening_multijob_manifests_20260808_work_balanced/{long_512,short_512}.jsonl`（immutable，bulk/foreground 各 512 行，doc_id 互不重叠，覆盖两 endpoint）；`max_output_tokens=256`（manifest 行校验）。
  - `SAOR_ARRIVAL_TIME_SCALE=0.0001`（**非** 0.001——0.001 是供给可达性门失败的旧 rehearsal：5s 前 bulk work<65536 envelope，触发不了 pre-borrow；0.0001 下 5s 前两 endpoint ~140K/138K predicted work，span≈6.69s）；`SAOR_MAX_EFFECTIVE_MANIFEST_SPAN_S=120`；`SAOR_MIN_PRE_FOREGROUND_WORK_ENVELOPES=1.0`；foreground offset=5s。
  - calibration selection：`opening_multijob_calibration_selection_20260808.json`（SHA256 `bc2042d7…`，`status=ready`）。
- **preflight**：`manage_environment.py check --groups core,text,analysis` → `status=ok`。
- **readiness**：`audit_saor_formal_readiness.py` → `status=passed`，errors=[]；resolved evidence 显示 protocol=chat_completions、URL=/v1/chat/completions、scale=0.0001、calibration SHA 一致、pre-foreground work 达标。
- **rehearsal**：commit `7c11cc7c` rehearsal 10/10 cell completed、0 incident、mechanism gate（二维工作守恒）通过（formal 不重跑 rehearsal）。
- **formal run**：`saor_active_set_release_formal_20260812_69affc7e/`，10 scenario × (1 warmup + 3 formal) = 40 cell，确定性交错，seed 20260812，zero-retry。
- **raw**：服务器 `experiment-artifacts/saor_active_set_release_formal_20260812_69affc7e/`（manifest.json + records/40 + group_runs.csv + traces/ + jobs/ + summary/validation.json）。raw-not-in-git；下载到本地镜像 `C:\Users\ays\Desktop\results\_mirror_20260812\`（见 raw MANIFEST）。
- **聚合（进 git，本目录）**：README.md + summary/{validation.json} + records/*.json（40 cell per-cell summary）+ group_runs.csv。

## 1. 实验设置 / 2. 设计

见 provenance。十场景：6 个 active-set 臂（两 Job 并发，相同 K/W/manifest/资源，唯一变量=调度策略）+ 4 个 solo 臂（单 Job 独占，作 JCT slowdown 分母；project 臂用 project solo，direct 臂用 direct solo）。SAOR 要在同最大资源上限下相对 baseline 进入 Pareto 前沿（tok/s / 两 Job JCT slowdown / P95-P99 / SLO goodput / Jain / service disparity），单项 Jain 或吞吐改善不称胜出（§15-16）；未进 FIFO/DRR Pareto 前沿则淘汰、不追正（§17）。

## 3. 严谨性自检

- **正确性**：40/40 cell `request_success_delta`=1024（active-set）/ 512（solo），`job_failed_rows=0`，exactly-once；`actor_worker_failures=0`；0 incident；两 endpoint 都接收；metrics/resources/credit trace 全 `ok`。
- **lifecycle gate**：所有 6 个 active-set 臂 × 全 repeat `active_set_lifecycle_passed=True`（`observed_staggered_two_job_overlap`，两 Job 真实 overlap + exactly-once 完成）。
- **mechanism gate**（二维工作守恒：`d_j(t)=max{R_j/K^req, W_j/K^work}`，post-drain 逐 endpoint 检查队首 `R_e+1≤K_e^req ∧ W_e+h_{j,e}≤K_e^work`）：
  - `shared_fifo` 3/3 passed；**`saor_release` 3/3 passed**；
  - `shared_drr` **rep2 `mechanism_not_observed`**（rep1/rep3 passed）；`external_vtc` **rep2 `mechanism_not_observed`**（rep1/rep3 passed）。
- **fail-closed**：summarize 检测到 DRR/VTC rep2 mechanism 失败 → `validation.json status=failed`，**拒绝产 formal_summary.csv**（不进入策略胜出声明）。这是预注册硬规则（§12），非 bug。
- **观测口径**：tok/s、JCT、P99、KV、waiting、SLO 均取自 per-run time-series 聚合（`*_mean/p95/max`），非单点。`vllm_kv_cache_usage` 按分数读。

## 4. 实验数据（formal 3-repeat；**gate 未全过，指标仅供定位，不构成策略胜出声明**）

active-set 臂（mean of 3 formal repeats；JCT/P99 单位 s；SLO viol 是分数 0–1）：

| arm | tok/s | JCT bulk | JCT fg | P99 bulk | P99 fg | kv_max | wait_max | SLOviol bulk | SLOviol fg | mechanism |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| direct_no_job | 13676 | 51.0 | 58.6 | 44.5 | 53.3 | 1.000 | 142 | 0.535 | 0.986 | N/A |
| static_partition | 9508 | 89.9 | **36.2** | 83.1 | **28.8** | 0.424 | 0 | 0.673 | **0.000** | N/A |
| shared_fifo | 12103 | 62.2 | 65.3 | 55.7 | 58.7 | 0.554 | 20 | 0.460 | 0.968 | ✅ 3/3 |
| shared_drr | 12411 | 68.5 | 62.6 | 61.8 | 57.0 | 0.550 | 17 | 0.471 | 0.845 | ❌ 2/3 |
| external_vtc | 12441 | 68.4 | 60.2 | 61.9 | 53.7 | 0.552 | 21 | 0.463 | 0.894 | ❌ 2/3 |
| saor_release | 12393 | 68.5 | **57.0** | 62.0 | **47.8** | 0.551 | 18 | 0.471 | **0.832** | ✅ 3/3 |

solo（slowdown 分母，3-repeat mean）：solo_project_bulk tok/s=11670 JCT=59.17；solo_project_foreground tok/s=8998 JCT=16.51；solo_direct_bulk tok/s=14743 JCT=47.97；solo_direct_foreground tok/s=10174 JCT=15.97。

**JCT slowdown = concurrent JCT / matched solo JCT**（project 臂用 project solo）：

| arm | bulk slowdown | fg slowdown |
|---|---:|---:|
| static_partition | 1.52 | **2.19** |
| shared_fifo | 1.05 | 3.96 |
| shared_drr | 1.16 | 3.79 |
| external_vtc | 1.16 | 3.65 |
| saor_release | 1.16 | **3.45** |

CV：各臂 tok/s 跨 3 repeat CV 均 <0.5%（极稳，如 SAOR [12406,12353,12420]、FIFO [12133,12074,12100]）。

## 5. 事实 / 推断 / 不能声称

- **事实**：
  1. formal 40/40 完成、0 incident、exactly-once；lifecycle gate 全过。
  2. `validation.json status=failed`：DRR、VTC 在 formal rep2 未观察到 credit mechanism（rep1/rep3 过）。FIFO、SAOR mechanism 3/3 全过。
  3. **static_partition 在前台延迟上是独立 Pareto 点**：fg_slowdown 2.19（最低）、fg P99 28.8s（最低）、fg SLO 违反 0%（唯一为 0）；代价是吞吐最低（9508）。
  4. **在 4 个 credit 臂内部**：吞吐几乎同档（12103–12441，差 <3%）；SAOR 的 fg JCT（57.0）/ fg P99（47.8）/ fg slowdown（3.45）/ fg SLO 违反（0.832）**均不差于** FIFO/DRR/VTC，多数更优；SAOR 是 credit 臂里 fg 尾延迟最优的，且是唯一 mechanism 3/3 全过的 credit 臂（与 FIFO 并列）。
  5. direct_no_job 顶满（kv=1.0、wait=142）吞吐最高，但 fg SLO 违反 98.6%（最差尾延迟）——符合"direct 是直连天花板参照，吞吐差含执行链路差异，非纯算法差"。
- **推断**：
  1. SAOR 进入了 FIFO/DRR 的 Pareto 前沿（同吞吐档、fg 尾延迟不差且更优、mechanism 稳定）→ 按 §17 **不被淘汰**。
  2. 但 **static_partition 是前台尾延迟/SLO 的更强 Pareto 点**——SAOR 没有在所有维度超过 static；两者是吞吐–尾延迟权衡的两个点（static 低吞吐换极低 fg 尾延迟，credit 臂高吞吐但 fg 尾延迟高）。
  3. DRR/VTC rep2 mechanism 未观察到，是该 baseline 在本合同下的机制不稳定性（不是 SAOR 的问题），但导致 validation fail-closed，本轮无法做"策略胜出"的正式声明。
- **不能声称**：
  - "SAOR 胜出 / SAOR 优于 baseline"——validation failed，且 static 在 fg 尾延迟上更强。
  - "DRR/VTC 无效"——只是 rep2 机制未观察，rep1/rep3 过。
  - 任何 4-Job 结论（未跑）、任何 dynamic-K 结论（K 来自冻结合同，未动态选）、定理证明（仅 empirical）。
  - "external VTC = in-engine VTC reproduction"——它是外部 VTC-style baseline。

## 6. 对课题含义

SAOR `saor_release` 在同最大资源上限下：（a）机制稳定（mechanism 3/3 全过，与 FIFO 并列、优于 DRR/VTC）；（b）在 credit 臂内进入 Pareto 前沿且 fg 尾延迟最优。**但** static_partition 在前台尾延迟/SLO 上是更强的 Pareto 点，且 summarize 因 DRR/VTC rep2 mechanism 失败而 fail-closed，本轮**不能正式声明策略胜出**。state-aware 在 2-Job 下的价值方向性成立（fg 尾延迟最优 + 机制最稳），但需（i）固定 DRR/VTC 的 mechanism 不稳定根因后重跑通过 validation，或（ii）补 4-Job 看扩展性，才能升级 claim。

## 7. 下一步（不阻塞开题）

1. 诊断 DRR/VTC rep2 mechanism 未观察的根因（是否某次 arrival 抖动使 pre-borrow/overlap-reclaim 信号未达阈值）；若可修复，同合同重跑 formal 使 validation 通过。
2. validation 通过后再做正式 Pareto 声明 + 决定是否补 4-Job（§18：本轮若进 Pareto 仅"登记允许补 4-Job"，不自动启动）。
3. static_partition 的强 fg 表现值得单独分析（是否对开题主线的"低尾延迟"叙事更有利）。

## 不能声称的边界

formal gate fail-closed（DRR/VTC rep2 mechanism 未观察）；不称 SAOR 胜出、不称 baseline 无效、不称 4-Job/dynamic-K/定理结论。SAOR 在 credit 臂内进 Pareto 前沿 + fg 尾延迟最优 + 机制最稳，是方向性正面信号，但**不是正式策略胜出声明**。
