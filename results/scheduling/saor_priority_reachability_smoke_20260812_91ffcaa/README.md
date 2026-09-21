---
experiment_id: saor-priority-reachability-smoke-260812-01
date: 2026-08-12
status: completed-development-smoke
evidence_level: directional-not-formal
git_commit: 91ffcaafec31cd9f62a256034d8a7b29fdb89d31
repeats_per_arm: 2
formal_repeats: 0
incidents: 0
---

# SAOR foreground strict-priority release-only 短测（2026-08-12）

## 1. 实验目的

在不修改 vLLM、不能撤销已提交请求的约束下，回答一个比“SAOR 是否胜出”更窄的问题：仅改变新释放 credit 的分配顺序，是否足以让 5 s 后到达的 foreground 接近或越过 frozen static 的保护效果。`foreground_strict_priority` 是非抢占 release-only 能力上界，不是 proposed policy。

## 2. 实验设置

| 项目 | 合同 |
|---|---|
| 平台 | 同一双 RTX 4090 / 双 vLLM endpoint / Qwen2.5-7B 签名 |
| workload | 与 fixed-envelope 2-Job formal 相同的 512 bulk + 512 foreground，foreground offset 5 s |
| 冻结包络 | 每 endpoint K128 / W65536，token budget 6144，8 actors，actor concurrency 32 |
| 三臂 | `static_partition`、`saor_release`、`foreground_strict_priority` |
| 重复 | 两个独立 rehearsal-only repeat/arm；没有 warm-up + 3 formal |
| 代码 | `91ffcaaf`；strict-priority 的审计动作是 priorities `[0,1]` |
| raw | 服务器仓库外 artifact；本目录归档 compact summary/repeats，不提交服务器地址或凭据 |

## 3. 合规性自检

| 门禁 | 结果 |
|---|---|
| machine preflight | 显式绑定实际路径后 28 项通过；首次缺少 5 个通用路径变量的失败报告留在远端 |
| runner | 6/6 cell completed，0 incident，actor failures 0 |
| exactly-once / lifecycle | 三臂各 2/2 lifecycle pass；每个 run 两 Job 完整完成 |
| mechanism | SAOR 2/2、strict-priority 2/2；static 不适用 |
| GPU feeding | GPU mean 96.63%–98.06%，不是空跑 |
| 正式性 | **未通过 formal 身份门**：仅 2 repeats/arm，formal repeats=0 |

## 4. 实验数据

| arm | tok/s | bulk JCT(s) | fg JCT(s) | bulk P99(s) | fg P99(s) | bulk SLOviol | fg SLOviol | fg slowdown | GPU mean | mechanism |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| static | 9550.8 | 89.46 | 36.20 | 82.65 | 29.39 | 0.671 | 0.000 | 2.194 | 98.06% | N/A |
| SAOR | 12382.4 | 68.65 | 59.36 | 62.13 | 52.90 | 0.467 | 0.869 | 3.598 | 97.07% | 2/2 |
| strict-priority | 11791.4 | 72.31 | **20.04** | 65.88 | **14.27** | 0.801 | **0.000** | **1.214** | 96.63% | 2/2 |

| 对比 | tok/s | bulk JCT | fg JCT | fg P99 |
|---|---:|---:|---:|---:|
| strict-priority vs SAOR | −4.77% | +5.33% | **−66.25%** | **−73.02%** |
| strict-priority vs static | **+23.46%** | **−19.17%** | **−44.64%** | **−51.43%** |

完整均值与单次值分别见 `summary.csv`、`repeats.csv`。

## 5. 结果解释

| 类型 | 判断 |
|---|---|
| 事实 | 两轮 short smoke 中，strict-priority 均使 foreground JCT 约 20.0 s、P99 约 14.27 s、SLO violation 0，同时吞吐只比 SAOR 低 4.77%。 |
| 推断 | current SAOR 的主要问题不是“非抢占必然来不及”，而是 soft entitlement/fairness release score 在 foreground 存活期仍给 bulk 新 credit，目标函数与 foreground tail/SLO 错位。 |
| 推断 | strict-priority 在 foreground 到达前允许 bulk 借满、到达后停止 bulk refill，因此把 static 的永久分区成本变成了阶段性机会成本；这解释了其短测中同时越过 static 的吞吐和 fg 延迟。 |
| 指标警告 | strict-priority 的 normalized-progress Jain≈1，但 bulk SLO violation=0.801，说明该 Jain 在异质 solo 分母下不能替代 per-class SLO/尾延迟审计。 |
| 不能声称 | 两轮 rehearsal 不能构成 formal 排名；hard priority 可能饿死持续 bulk，也不能据此声称 SAOR、reservation 或通用多 Job 策略胜出。 |

## 6. 对课题的含义

该结果把设计空间从“继续扫 SAOR soft score 权重”收紧为“显式 deadline/优先级 guard + 有界反饥饿”。reservation 不再是两 Job 可达性的唯一解释；它仍可能是未知到达、多个高优先级 Job、预测误差和长期公平下的安全组件。下一版应把性能目标写成约束问题，而不是无量纲加权分数：

$$
\min_\pi\; Q_{0.99}(T_F^\pi)
\quad\text{s.t.}\quad
\lambda^\pi \ge (1-\epsilon)\lambda_{\text{shared}},\;
S_B^\pi\le \bar S_B,\;
\sup_t L_B^\pi(t)\le L_{\max}.
$$

其中 foreground tail 是主目标，吞吐、bulk slowdown 和最大 service lag 是硬约束。实现上优先尝试“foreground SLO slack 为负时 lexicographic priority；其余时间 DRR/SAOR；再加连续优先窗口或 lag cap”，而不是直接把 strict-priority 当最终算法。

## 7. 下一步

1. 先把 hard priority 改成有界 priority window / lag guard，冻结 2–3 个点做短测；不做大范围权重搜索。
2. 在同一 2-Job 合同下要求 fg P99 不劣于 static、吞吐至少 static +5%、bulk lag/SLO 不越界。
3. 仅当有界版本两轮方向稳定，再注册 1 warm-up + 3 formal；在此之前不扩 4-Job。

## 原始材料

服务器 raw artifact 保留在仓库外；本地仅提交无连接信息的 compact 表。用户已明确要求不进行 Wiki 同步。
