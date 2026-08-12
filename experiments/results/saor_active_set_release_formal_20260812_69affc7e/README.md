# SAOR fixed-envelope active-set 2-Job formal（2026-08-12，commit 69affc7e）

> **性质**：project-derived SAOR `saor_release` 状态感知准入 vs 5 个 baseline（direct / static / FIFO / DRR / external VTC），2×4090 + Qwen2.5-7B，2-Job（bulk 后台 + foreground@5s）guaranteed-overlap。固定包络（K128/W65536/actors8/concurrency32/token_budget6144/credit_quantum2048），同 request window 比。**非** 4-Job（未跑）。
>
> **最终结论**：formal 40/40 cell 完成、0 incident、exactly-once；但 **summarize 的 formal gate `status=failed`（fail-closed）**——`shared_drr` 和 `external_vtc` 在 formal repeat 2 未观察到 credit mechanism（pre-borrow / overlap reclaim / post-drain work-conservation），按 §12/§17 不进入策略胜出声明。SAOR 自身 mechanism 3/3 全过、FIFO 3/3 全过。
>
> **2026-08-12 审计回放补充**：把 post-drain 是否适用绑定到 runner 的 250 ms trace 分辨率后，DRR rep2（5.83 ms）和 VTC rep2（4.83 ms）被判为 `post_drain_not_applicable`，compact `group_runs.csv` 的 12 个 credit formal cell 因而 effective 12/12 通过。该回放只修正机制审计假阴性；Git 中没有服务器完整 manifest/raw trace，故不改写原始 `summary/validation.json`，也不把完整 formal validation 升格为 passed。性能与 Pareto 结论不变。

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
- **聚合（进 git，本目录）**：README.md + `group_runs.csv` + `summary/{validation.json,mechanism_gate_replay.json}`。完整 manifest、per-cell records 与 raw traces 仍只在服务器/本地镜像，不从 compact 表反向补造。

## 1. 实验设置 / 2. 设计

见 provenance。十场景：6 个 active-set 臂（两 Job 并发，相同 K/W/manifest/资源，唯一变量=调度策略）+ 4 个 solo 臂（单 Job 独占，作 JCT slowdown 分母；project 臂用 project solo，direct 臂用 direct solo）。SAOR 要在同最大资源上限下相对 baseline 进入 Pareto 前沿（tok/s / 两 Job JCT slowdown / P95-P99 / SLO goodput / Jain / service disparity），单项 Jain 或吞吐改善不称胜出（§15-16）；未进 FIFO/DRR Pareto 前沿则淘汰、不追正（§17）。

## 3. 严谨性自检

- **正确性**：40/40 cell `request_success_delta`=1024（active-set）/ 512（solo），`job_failed_rows=0`，exactly-once；`actor_worker_failures=0`；0 incident；两 endpoint 都接收；metrics/resources/credit trace 全 `ok`。
- **lifecycle gate**：所有 6 个 active-set 臂 × 全 repeat `active_set_lifecycle_passed=True`（`observed_staggered_two_job_overlap`，两 Job 真实 overlap + exactly-once 完成）。
- **mechanism gate**（二维工作守恒：`d_j(t)=max{R_j/K^req, W_j/K^work}`，post-drain 逐 endpoint 检查队首 `R_e+1≤K_e^req ∧ W_e+h_{j,e}≤K_e^work`）：
  - `shared_fifo` 3/3 passed；**`saor_release` 3/3 passed**；
  - `shared_drr` **rep2 `mechanism_not_observed`**（rep1/rep3 passed）；`external_vtc` **rep2 `mechanism_not_observed`**（rep1/rep3 passed）。
- **rep2 机制门定位**：DRR rep2 的两 Job 绝对完成时刻只差约 **5.8 ms**，VTC rep2 只差约
  **4.8 ms**；两臂该 repeat 的 `active_set_bulk_only_post_samples=0`。因此当前证据支持“没有形成可采样的
  post-drain 窗口”，不支持“算法违反工作守恒”。validation 仍按预注册规则保持 fail-closed。
- **冻结后的 simultaneous-drain 规则**：若两个 Job 的完成间隔小于观测周期且区间内没有 trace
  样本，则 post-drain 性质没有可检验时间窗，记为 `not_applicable`；若间隔达到 250 ms 或区间内
  实际存在样本，则仍必须观察到工作守恒，否则 fail-closed。新 runner 显式写出 duration、interval、
  observed samples、applicable 和 status；legacy compact evidence 只允许在 lifecycle、pre-borrow、
  overlap reclaim 已通过且无 fit violation 时兼容重分类。
- **离线回放**：`summary/mechanism_gate_replay.json` 为 `status=passed`、12/12 effective pass，只有
  DRR/VTC rep2 两项被重分类。文件显式记录 `scope=compact_mechanism_gate_only` 与
  `full_formal_validation_updated=false`。
- **fail-closed**：summarize 检测到 DRR/VTC rep2 mechanism 失败 → `validation.json status=failed`，**拒绝产 formal_summary.csv**（不进入策略胜出声明）。这是预注册硬规则（§12），非 bug。
- **观测口径**：tok/s、JCT、P99、KV、waiting、SLO 均取自 per-run time-series 聚合（`*_mean/p95/max`），非单点。`vllm_kv_cache_usage` 按分数读。

## 4. 实验数据（formal 3-repeat；**gate 未全过，指标仅供定位，不构成策略胜出声明**）

active-set 臂（mean of 3 formal repeats；JCT/P99 单位 s；SLO viol 是分数 0–1）：

| arm | tok/s | JCT bulk | JCT fg | P99 bulk | P99 fg | kv_max | wait_max | SLOviol bulk | SLOviol fg | mechanism |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| direct_no_job | 13676 | 51.0 | 58.6 | 44.5 | 53.3 | 1.000 | 142 | 0.535 | 0.986 | N/A |
| static_partition | 9508 | 89.9 | **36.2** | 83.1 | **29.2** | 0.424 | 0 | 0.673 | **0.000** | N/A |
| shared_fifo | 12103 | 62.2 | 65.3 | 55.7 | 58.7 | 0.554 | 20 | 0.460 | 0.968 | ✅ 3/3 |
| shared_drr | 12411 | 68.5 | 62.6 | 61.9 | 55.8 | 0.550 | 17 | 0.471 | 0.845 | ❌ 2/3 |
| external_vtc | 12441 | 68.4 | 60.2 | 62.0 | 53.6 | 0.552 | 21 | 0.463 | 0.894 | ❌ 2/3 |
| saor_release | 12393 | 68.5 | **57.0** | 62.0 | **50.3** | 0.551 | 18 | 0.471 | **0.831** | ✅ 3/3 |

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

辅助机制量（3-repeat mean；只用于解释，不作胜负指标）：

| arm | Jain | running mean | credit idle fraction | borrowed work mean | fg overlap dominant-share max |
|---|---:|---:|---:|---:|---:|
| static_partition | 0.914 | 73.4 | N/A | N/A | N/A |
| shared_fifo | 0.695 | 99.0 | 0.373 | 14794 | 0.737 |
| shared_drr | 0.722 | 113.1 | 0.345 | 12170 | 0.565 |
| external_vtc | 0.730 | 112.6 | 0.346 | 12171 | 0.669 |
| saor_release | 0.741 | 112.9 | 0.353 | 11723 | 0.746 |

## 5. 事实 / 推断 / 不能声称

- **事实**：
  1. formal 40/40 完成、0 incident、exactly-once；lifecycle gate 全过。
  2. 原始 `validation.json status=failed`：DRR、VTC formal rep2 未观察到 post-drain 样本；冻结采样分辨率规则后的 compact replay 将两项标为不适用并得到 effective 12/12，但尚未用服务器完整 raw 重放，所以原始完整 validation 不变。
  3. **static_partition 在前台延迟上是独立 Pareto 点**：fg_slowdown 2.19（最低）、fg P99 29.2s（最低）、fg SLO 违反 0%（唯一为 0）；代价是吞吐最低（9508）。
  4. **在 4 个 credit 臂内部**：吞吐几乎同档（12103–12441，差 <3%）；SAOR 的 fg JCT（57.0）/ fg P99（50.3）/ fg slowdown（3.45）/ fg SLO 违反（0.831）均优于 FIFO/DRR/VTC；SAOR 是 credit 臂里 fg 尾延迟最优的，且与 FIFO 都是 mechanism 3/3 全过的 credit 臂。
  5. direct_no_job 顶满（kv=1.0、wait=142）吞吐最高，但 fg SLO 违反 98.6%（最差尾延迟）——符合"direct 是直连天花板参照，吞吐差含执行链路差异，非纯算法差"。
- **推断**：
  1. SAOR 进入了 FIFO/DRR 的 Pareto 前沿（同吞吐档、fg 尾延迟不差且更优、mechanism 稳定）→ 按 §17 **不被淘汰**。
  2. 但 **static_partition 是前台尾延迟/SLO 的更强 Pareto 点**——SAOR 没有在所有维度超过 static；两者是吞吐–尾延迟权衡的两个点（static 低吞吐换极低 fg 尾延迟，credit 臂高吞吐但 fg 尾延迟高）。
  3. DRR/VTC rep2 已由冻结规则定位为低于 trace 分辨率的不可适用窗口，不再解释为 baseline
     机制不稳定；但 compact 回放不替代缺失的完整 raw validation，本轮仍不做“策略胜出”声明。
- **不能声称**：
  - "SAOR 胜出 / SAOR 优于 baseline"——validation failed，且 static 在 fg 尾延迟上更强。
  - "DRR/VTC 无效"——只是 rep2 机制未观察，rep1/rep3 过。
  - 任何 4-Job 结论（未跑）、任何 dynamic-K 结论（K 来自冻结合同，未动态选）、定理证明（仅 empirical）。
  - "external VTC = in-engine VTC reproduction"——它是外部 VTC-style baseline。

### 5.1 第一性原理定位：为什么当前 SAOR 效果有限

| 原因 | 实验/实现证据 | 数学含义 |
|---|---|---|
| 非抢占 release 的因果下界 | bulk 在前台到达前可借满包络；已进入 vLLM 的请求不能撤销 | 若前台到达时保护余量为 0，则其最早可用容量受下一批 completion 限制；只改 release order 不能复制 static 的即时隔离 |
| formal 实际不是 SLO-aware controller | `slo_weight=0`，30s SLO 只被测量；score 由 entitlement deficit 与 fairness debt 构成 | 优化目标没有把前台 deadline 作为 hard constraint，故 fg SLO 0.831 并不反常 |
| static 消除了 arrival uncertainty | static 为每个 Job 保留 K64/W32768，前台 5s 到达时已有专属空间 | static 吞吐损失是 reservation 的机会成本；其 fg 优势不是当前 SAOR 仅靠事后 reclaim 能免费取得的 |
| work 估计对前台偏乐观 | project credit 臂的 `actual/predicted work`：bulk≈1.064，foreground≈1.289 | 前台低估约 28.9%，相对 bulk 的 6.4% 低估约为 4.5 倍；同一 nominal credit 会给前台更少真实服务预算 |
| 当前 score 与目标错位 | equal entitlement + fairness debt；observe-only 服务状态未进入动作 | 它近似“公平 release”，不是“在吞吐约束下最小化前台 tail”的解 |
| 两 Job 场景的可支配自由度很小 | 四个 credit 臂吞吐仅差 <3%，KV≈0.55、running≈99–113 | 固定 K/W 且 FCFS 内核不变时，上游只能决定未来 admission；大部分执行顺序已在 vLLM 内固化 |

把 endpoint 包络记作 $K$，前台到达时 bulk 已占用 $A_B(t_a)$，为前台保留 $r$。当前
non-preemptive release 的回收债务为

$$
D_B(t_a;r)=\left[A_B(t_a)-(K-r)\right]^+.
$$

当 $r=0$ 且 $A_B(t_a)\approx K$ 时，前台即时可用容量近似为 0；在未来 completion 释放至少
$D_B$ 之前，任何只控制新请求 release 的算法都无法给出 static 式的即时容量。因此要同时逼近
static 的前台 tail 和 shared 的吞吐，至少需要以下一个额外信息或动作：有限保护余量、可预测的
到达/阶段信号、引擎内抢占。项目不修改 vLLM，故当前可行解应是前两者的组合，而不是继续调
fairness score 权重。

### 5.2 基于该模型的修订方向

| 修订 | 形式化定义 | 目的 |
|---|---|---|
| 有限保护余量 | 为前台保留 $r_e(t)\in[0,K_e]$，空闲时允许 bulk 借用，但记录 reclaim debt | 把 static 的隔离能力变成可借用 reservation，而不是完全事后补偿 |
| 风险上界 credit | admission 使用 $\overline W_i=\widehat W_i+\kappa\sigma_i$，完成后用 actual work 记账 | 防止前台输出 work 系统性低估导致实际资源份额偏小 |
| 词典序可行集 | 先满足 envelope/correctness、fg SLO、无饥饿，再在可行策略中最大化 goodput | 避免 soft score 用吞吐/公平项抵消 SLO 安全条件 |
| stage lead-time | foreground 已知 offset/DB stage 信号触发提前回收；未知到达只保留最小保险余量 | 在不可抢占边界内缩短 $D_B$ 的清零时间 |
| fail-closed fallback | 状态过期、风险区间过宽或 SLO debt 超阈值时回退冻结 static | 给正式 claim 提供安全边界 |

建议优化问题改写为约束形式，而不是单一加权分数：

$$
\max_\pi\; G(\pi)
\quad\text{s.t.}\quad
\Pr\{L_F>30\text{s}\}\le\epsilon,
\quad J_{norm}(\pi)\ge J_{min},
\quad A_e^{req}\le K_e^{req},
\quad A_e^{work}\le K_e^{work}.
$$

当前数据不足以声称已经找到该约束问题的可行动态解；它只说明 `saor_release` 相对无保护的
credit baseline 改善了前台，但距离 static 的可行域仍很远。

## 6. 对课题含义

SAOR `saor_release` 在同最大资源上限下：（a）自身 mechanism 3/3 全过，且 compact 机制回放后
四个 credit 臂 effective 12/12；（b）在 credit 臂内
以几乎同档吞吐获得最佳 fg 延迟。**但** static_partition 在前台尾延迟/SLO 上是更强的 Pareto
点；原始完整 validation 因 DRR/VTC rep2 没有 post-drain 可观测窗口而保留 fail-closed，compact
回放不会改变性能排序，本轮**不能正式声明策略胜出**。更重要的是，当前实现验证的是 fixed-envelope fairness/release，而不是完整
SLO-aware 控制：在 `slo_weight=0`、无保护余量、不可抢占的合同下，它理论上无法免费复制
static 的前台隔离。下一版本应从“调 score 权重”转为“有限 reservation + borrow/reclaim +
风险上界 work credit + hard SLO feasible set”。

## 7. 下一步（不阻塞开题）

1. **审计器修复已完成**：已增加 `post_drain_not_applicable` 分支、legacy compact 兼容回放和
   新 schema fail-closed 边界；compact 12/12 effective pass，但完整 raw replay 尚待安全远端会话。
2. **release-only 可达性诊断已实现、待 GPU 运行**：`foreground_strict_priority` 在前台 Job
   存活期间停止发新 bulk credit，但不撤销已有 lease；Job 完成后显式 `finish_job` 恢复 bulk。
   runner 把 `[0,1]` priority 动作写入 group evidence，独立汇总器以 fg P99≤30.7s、fg SLO
   violation≤1% fail-closed 判定 release-only 是否可达。它只是能力上界，不是新 proposed。
3. **只扫一维 reservation 曲线**：固定其他配置，比较 $r/K=0,0.25,0.5$ 的
   reserve-borrow-reclaim；建议晋级门为 fg P99≤30.7s、fg SLO 违反≤1%、吞吐≥9984 tok/s。
   若只有 0.5K 通过，则等价于 static，淘汰动态方案；若更小 reserve 通过且吞吐较 static≥5%，
   才有方法价值。
4. **再做 work-risk 消融**：同一 reserve 下比较 mean estimate、q95 upper-bound 与 actual-work
   oracle；只在 oracle 显示可达空间时继续估计器。两 Job 问题闭环前不启动 4-Job 扩展。

## 不能声称的边界

原始完整 formal gate 保留 fail-closed；compact mechanism replay 通过不等于完整 validation
通过。不得称 SAOR 胜出、baseline 无效、strict-priority 已有 GPU 效果，也不称
4-Job/dynamic-K/定理结论。SAOR 在 credit 臂内进入经验 Pareto 前沿且 fg
延迟最好，是方向性正面信号；它没有越过 static，也尚未验证 SLO-aware 动作，因而**不是正式
策略胜出声明**。
