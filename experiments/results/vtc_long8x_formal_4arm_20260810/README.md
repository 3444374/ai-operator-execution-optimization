# VTC-compatible 8-client long-overload 正式 4 臂交错实验（2026-08-10）

> **性质**：external completion-corrected VTC-style baseline 的多 job 共享调度对照。回答三问：① frozen shared DRR vs state-aware adaptive 主 A/B；② external VTC-style vs shared DRR 的效率—公平权衡；③ FIFO vs DRR/VTC（work-conserving vs 公平记账）。**非** vLLM/Daft/Ray 原生 VTC，**无** 理论 VTC guarantee。

## provenance

- **代码**：git `5f3a605`（含 `ca02511` 非对称容量迟滞；preflight `git merge-base --is-ancestor ca02511 HEAD` = OK）。
- **config**：`vtc_long8x_formal_4arm_20260810/config.json`，SHA-256 `903ba1f95730098478ebdd1dbd31151b4ded071121ff1453d7796bd0b620987c`。
- **平台/服务**：AutoDL 2×RTX 4090（24G/卡，sm89）；PostgreSQL 18.4 + pgvector；2× vLLM 0.25.1 endpoint（各绑 1 GPU），Qwen2.5-7B-Instruct，`max_num_seqs=256`、`max_num_batched_tokens=8192`、`gpu_memory_utilization=0.9`、prefix-cache ON、mfu-metrics ON；Ray 2.56.1 head `127.0.0.1:6380`（32 CPU / 2 GPU）。driver `/root/miniconda3/bin/python`。
- **workload**：`vtc_compatible_overload_multi_720s_20260810`（PG 2963 行），8 client 各 274/289/282/431/416/435/417/419 行；chat completions、`max_tokens=256`+`ignore_eos`（固定 512 tok/req）、raw prompt、T=0、prefix-cache ON；arrival replay scale 0.125（每 cell wall≈101s）。
- **4 臂**：`shared_fifo` / `shared_drr_frozen` / `external_vtc` 冻结 K128/W131072；`state_aware_adaptive` 初始 K96/W98304、候选 [K96/W98304, K128/W131072, K160/W131072]、increase-hyst=2、decrease-hyst=8、cooldown=8、target 7600 tok/s/ep、congestion KV 0.85。公共：8 actor/ep × concurrency 32、token_budget 6144、flush fixed 50ms、no writeback、SLO 30s。
- **重复**：1 warmup + 3 formal × 4 臂 = 16 group run，确定性交错，seed 20260810。
- **raw**：服务器 `experiment-artifacts/vtc_long8x_formal_4arm_20260810/run/`（per-run requests/submissions/resources/credits/states CSV + manifest），按 raw-not-in-git 政策不进 git，可下载到本地镜像。
- **聚合（进 git，本目录）**：`manifest.json` + `records/*.json`（16 cell per-cell summary）+ `group_runs.csv`（runner 直接产的 per-cell 运行表）+ `formal_summary.csv`（per-arm 3-rep 聚合，codex shared_vllm schema；vs-independent 3 列因无 independent 臂而省略）。

## 1. 实验设置

见 provenance。三臂静态冻结 K128、adaptive 从 K96 起；同 workload/服务/endpoint/manifest/arrival；4 臂交错。

## 2. 实验设计

4 臂同源同 sink 同 endpoint，3 formal reps，确定性交错。主 A/B = adaptive vs frozen DRR；次 = external VTC vs DRR；FIFO 区分 work-conserving vs 公平记账。static-partition 不加（已有证据 + 省 ~10–12 min）。

## 3. 严谨性自检（门禁）

**正确性门 1–10：全过 ✅** — manifest `completed`/incidents=[]；16/16 group（4w+12f）；8-client arrival replay 最大 lateness 0.0002s；每 client 完成行数精确 [274,289,282,431,416,435,417,419]；`request_success_delta=2963`（exactly-once，0 missing/dup/unexpected）；actor/HTTP/timeout/ReadError 全 0；两 endpoint 都接收；每轮 final active/waiting 全归零；traces 非空；用 time-series mean/p95/max。

**喂饱门（资格）**：adaptive PASS；DRR/VTC/FIFO FAIL（见 §4）。
**动态动作门 ✅**：按 endpoint 分，干净单向上行 K96→128（两端）→160（≥1 端），0 decrease/fallback/振荡；increase 触发条件 `ready_backlog_below_target` 且 waiting=0、KV≤0.52<0.85（合法）。
**VTC 门 ✅**：`attained_service_by_job`+`granted_work_by_job` 每端 8 条（completion 修正的实际 work）、weights=[1]×8、final 清空。

## 4. 实验数据（3-rep mean；direct long8x 参照 15401.10 tok/s，95% 门=14631.05）

| 臂 | K | tok/s | %direct | 喂饱 | CV% | JCT | p99 | goodput/s | run mean/max | wait max | KV max | MFU | Jain |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| shared_fifo | 128 | 14624.9 | 94.9% | FAIL | 0.51 | 97.7 | 13.5 | 3.79 | 213/254 | 0 | 0.40 | 0.458 | 0.968 |
| shared_drr_frozen | 128 | 14564.0 | 94.6% | FAIL | 0.37 | 97.9 | 13.8 | 3.78 | 212/254 | 0 | 0.40 | 0.456 | 0.969 |
| external_vtc | 128 | 14613.3 | 94.9% | FAIL | 0.29 | 97.4 | 13.8 | 3.79 | 213/254 | 0 | 0.40 | 0.458 | 0.970 |
| state_aware_adaptive | 96→160 | 14781.6 | 96.0% | PASS | 0.73 | 96.2 | 12.3 | 3.85 | 235/314 | 0 | 0.52 | 0.462 | 0.967 |

单次 tok/s：fifo [14620.2,14717.8,14536.7]；drr [14638.3,14513.4,14540.3]；vtc [14580.2,14587.5,14672.3]；adaptive [14851.0,14629.6,14864.4]（rep2 14629.6 在门线 14631.05 的 99.99%）。GPU util mean 全 97–98%；TTFT p95 ~0.24s、ITL p95 ~0.066s；service disparity_ratio 全 0.431。adaptive 动作轨迹（states.csv 按 endpoint）：K96(hold·missing_service_rate→deadband)→K128(increase·ready_backlog_below_target)→hold(cooldown/deadband)；endpoint-1 再→K160。rep3 两端都到 K160。

## 5. 事实 / 推断 / 不能声称

- **事实**：adaptive 是唯一过喂饱门的臂（96.0%），把 K 升到 160（running max 284–314 vs 静态 254）；3 静态 K128 臂 94.6–94.9%，差 ~1–1.5pp 没过 95%。动态动作干净（合法 increase、0 振荡/fallback），VTC 记账存在且正确。
- **推断**：adaptive 过门来自用了 K160（静态 K128 用不到）；3 静态臂 ~5% 低于 direct 天花板 = shared-credit 协调开销 vs direct 无界灌满。
- **不能声称**：
  - **"adaptive > DRR（动态优于静态）"**——DRR 没过喂饱门（94.6%），**主 A/B 在严格门下不结论性**；且后续 K160 对照（见 `../vtc_long8x_drr_k160_control_20260810/`）证伪了本 run 的"adaptive 过门 = 动态价值"读法——frozen-K160 DRR 同样过门且略高，过门是 K160 上限的功劳。
  - 原生 VTC / 理论 guarantee。
  - 公平胜出——Jain 全 0.967–0.970 几乎相同。
  - KV-congestion 下行分支有效——KV max 0.52<0.85，下行未被真实触发。

## 6. 对课题含义

饱和 regime 下首份 formal 多臂 A/B；但 **bound-vs-adaptation 已被 K160 对照证伪**（frozen-K160 ≈ adaptive）→ 本 run 的"adaptive 过门"是 K160 上限主导，非动态适应。state-aware 仍 `待验证`（claim_matrix 第 59 行不升级）。控制器行为正确（干净 ramp、不振荡），且首次在饱和 regime 真正 actuate（补上早前"结构性零效应"的缺口）。

## 7. 下一步

见 `../vtc_long8x_drr_k160_control_20260810/`（K160 对照已证伪本 run 的正向读法）。要证动态价值需 phase-change/变载 workload（on-off gate 此前从未跑）。claim_matrix 第 59 行维持待验证。

## 不能声称的边界

本实验是 external completion-corrected VTC-style baseline；不称原生 VTC、不称理论 guarantee、不称"动态优于静态"（K160 对照已证伪）。
