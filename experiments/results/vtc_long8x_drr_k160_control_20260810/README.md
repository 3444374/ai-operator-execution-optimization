# frozen-K160 DRR 对照：上限 vs 动态适应（2026-08-10）

> **性质**：`vtc_long8x_formal_4arm_20260810` 的 follow-up 控制实验。隔离"adaptive 过喂饱门"是 **K160 上限**的功劳还是**动态适应**的增量——把 frozen DRR 从 K128 提到 K160（与 adaptive 最高候选同 bound），其余全同。

## provenance

- **代码**：git `5f3a605`（同 4-arm）。
- **config**：`vtc_long8x_drr_k160_control_20260810/config.json`，SHA-256 `ee2c4297119ed43c78f00c9784d26ceb23d270e906bfa1bc7c2deccc4220468b`。
- **派生方式**：从冻结的 4-arm config 程序化派生——唯一变量 `request_limit_per_endpoint` 128→160（shared_drr scenario），其余 common_args(81 条)/service_metadata/arrival_offsets/rows_per_jobs/manifests 字节级一致（copy）。非改代码、非在线调参、非挑数据重跑；config 跑前冻结 SHA。
- **平台/服务/workload**：同 4-arm（2×4090 / vLLM 0.25.1 Qwen2.5-7B / 8-client 2963 行 720s overload / arrival scale 0.125）。
- **重复**：1 warmup + 3 formal = 4 group run，seed 20260810。该 follow-up 在 4-arm 完成后顺序运行，未与原 4 臂交错。
- **raw**：服务器 `experiment-artifacts/vtc_long8x_drr_k160_control_20260810/run/`（raw-not-in-git）。
- **聚合（进 git，本目录）**：`manifest.json` + `records/*.json`（4 cell）+ `group_runs.csv`（runner per-cell 运行表）+ `formal_summary.csv`（per-arm 3-rep 聚合，codex shared_vllm schema 派生）。

## 1. 实验设置 / 2. 设计

单臂 shared_drr @ K160/W131072，1w+3f。与 4-arm 的 shared_drr_frozen(K128) 仅 K 不同；与 adaptive 的 K160 候选同 bound。回答：frozen-K160 是否也能过喂饱门、是否 ≈ adaptive。

## 3. 严谨性自检

**正确性门 1–10 全过 ✅**：completed/4 group(1w+3f)/每 client 行数精确 [274,…,419]/exactly-once success=2963/0 failure(actor·HTTP·timeout·ReadError)/两 endpoint 接收/每轮 final 归零/lateness 0.0002s/offset-normalized barrier skew 0.000133s/traces 非空/time-series。

## 4. 实验数据（3-rep mean）

| 臂 | K | tok/s | %direct | 喂饱门(14631.05) | CV% | running mean/max | wait max | KV max | MFU | Jain |
|---|---|---|---|---|---|---|---|---|---|---|
| shared_drr_frozen（4-arm） | 128 | 14564.0 | 94.57% | FAIL | 0.37 | 212/254 | 0 | 0.40 | 0.456 | 0.969 |
| **shared_drr_k160（本对照）** | 160 | **14830.5** | **96.30%** | **PASS** | **0.04** | 248/317 | 0 | 0.49 | 0.463 | 0.966 |
| state_aware_adaptive（4-arm） | 96→160 | 14781.6 | 96.00% | PASS | 0.73 | 235/314 | 0 | 0.52 | 0.462 | 0.967 |
| direct long8x（参照） | — | 15401.1 | 100% | — | — | — | — | — | — | — |

K160 单次 tok/s [14836.9, 14822.8, 14831.9]（CV 0.04%，极稳）。

表中 GPU util 是百分数，MFU 是 0–1 分数；`formal_summary.csv` 的对应列 `mfu_pct_mean` 已换算为百分数。`start_skew_s_max=max(observed_start-offset)-min(observed_start-offset)`，不包含预设 arrival offset 跨度。

## 5. 事实 / 推断 / 不能声称

- **事实**：frozen-K160 DRR = 14830.5 tok/s（96.30%），**过喂饱门**，数值上比 adaptive（14781.6）高 +48.9 tok/s（+0.33%）。K128=14564 不过门，K160=14830 过门；三臂 Jain 都在 0.966–0.969。K160 与 adaptive 的均值差小于 adaptive 自身 CV 0.73%（SD 131.9 tok/s），且两组未交错，因此不能把 +0.33% 解释为静态显著胜出。
- **推断（判定 ①）**：冻结 K160 已足以复现“过喂饱门”，支持 **K 上限主导** 的解释；在该 sustained-overload regime，尚无证据显示在线动态控制在同一 K160 上限之上带来额外吞吐收益。
- **不能声称**：
  - “state-aware 动态适应带来吞吐收益”——本 regime 的数据不支持该结论。
  - “动态增量严格为零”或“frozen-K160 显著优于 adaptive”——follow-up 未交错，且 +0.33% 小于 adaptive 重复波动。
  - "state-aware 无价值"——**也不能这么说**。动态的价值在变载/多 regime（高 K 全程开在低压力相会伤尾延迟/公平/KV，adaptive 该降才降）；本 sustained-overload 单 regime 测不出。KV 全程 ≤0.52<0.85，**控制器下行分支仍没被真实触发**。
  - 仍**不升级 claim_matrix 第 59 行**——单 workload、sustained overload、且 frozen-K160 已等价。

## 6. 对课题含义

该对照使“adaptive 过门即可证明动态价值”的读法不再成立：**K160 静态上限本身已足以过门，动态控制的额外吞吐收益未获支持**。但顺序 follow-up 不能把真实增量精确钉死为零。要验证动态价值，应换 phase-change/变载场景（on-off 间歇、低-高-低 offered load），让“高 K 在低压力相过压”可观测，并以同上限 frozen-K160 与 adaptive 交错重复。这把 state-aware 的验证从“找稳态峰值吞吐差”转到“验证 regime-conditional 的安全、尾延迟或恢复收益”。

## 7. 下一步

1. **on-off / phase-change workload**（on-off gate 此前从未跑，8-10 核验里是空 stub）——比 sustained overload 更能区分 bound vs adaptation。
2. frozen-K160 vs adaptive 在多个 K160-safe / K160-unsafe 场景的对比矩阵。
3. KV-congestion（>0.85）触发场景，验控制器下行分支。

## 不能声称的边界

本对照只覆盖 sustained-overload 单 regime、equal weight、纯文本，且是顺序 follow-up；不称“动态无价值”、不称“动态增量严格为零”、不称“动态有价值”（需变载），不升级 claim_matrix 第 59 行。
