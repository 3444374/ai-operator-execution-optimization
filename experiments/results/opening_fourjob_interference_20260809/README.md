# 文本 1 Short + 3 Long 四 Job 干扰与原生框架观察（2026-08-09）

## 1. 实验目的

本实验回答四个相互关联、但不能混为系统排名的问题：

1. 一个 short Job 独占服务时的基线是多少，三个 long Job 在 5 s 后加入会怎样影响它；
2. 三个工作量相近、但彼此独立的 long Job 在同一服务上会怎样互相影响；
3. Project 的固定四分区中，多少退化来自单 Job 只得到 1/4 配额，多少来自真实服务竞争；
4. 在相同全局 K/work 上限下，共享 work credit/idle borrowing 相对固定分区带来怎样的效率、隔离和公平权衡。

Daft Native、Daft Ray 与 Ray Data 只运行各自官方/原生执行图。项目不替它们重排
workload、不注入 work credit、跨 Job 路由、fair queue 或负载均衡，也不扫描参数后选择
有利点。原生框架只做各系统内部的 `single → four-job` 相对观察；绝对 JCT 不用于跨框架
性能排名。

## 2. 实验设置

- 硬件与服务：2×RTX 4090、两个独立 vLLM endpoint、Qwen2.5-7B-Instruct，prefix
  cache on、`max_num_batched_tokens=8192`、`max_num_seqs=256`、MFU metrics on。
- workload：`short@0s → {long1,long2,long3}@5s`；每个 Job 512 行、两个 endpoint 各
  256 行、四个 manifest 的 `doc_id` 互斥。Project 与三个原生框架使用完全相同的四份
  immutable manifest。
- 重复：15 个原生 arm 与 10 个 Project scenario 均为 1 warm-up + 3 formal，runner
  按冻结 seed 交错。
- Project 控制：四个 full-pool single、四个 reserved-quarter-pool single、
  `static_partition` 四 Job、`shared_work` 四 Job。full/quarter 都是真实 512 行 Job；
  quarter 只表示静态本地容量上限为总池 1/4，不是缩小任务。
- Project 上限：每 endpoint K128/W65,536；静态四分区为每 Job K32/W16,384；共享臂
  保持相同 endpoint 全局上限并允许 work-conserving idle borrowing。
- 原生框架：Daft `functions.prompt` Native、Daft `functions.prompt` Ray runner、Ray
  Data official HTTP graph。每个 Job 独立启动两个官方 endpoint shard；Ray Data 使用
  事先冻结的 `batch_size=16, concurrency=8/endpoint`，未为本实验重新扫描。
- 边界：no-writeback。此实验隔离 serving-side 多 Job 干扰；sink 已由统一
  database-E2E 护栏验证，不在这里重复。

运行入口：

```bash
PYTHONPATH=code python code/scripts/baselines/run_text_native_multijob.py \
  --config deploy/autodl/opening_text_native_fourjob.example.json \
  --runner-script code/scripts/baselines/run_official_baseline.py

PYTHONPATH=code python code/scripts/analysis/summarize_opening_fourjob_interference.py \
  --project-root <PROJECT_FORMAL_ROOT> \
  --native-root <NATIVE_FORMAL_ROOT> \
  --output <NEW_SUMMARY_ROOT>
```

## 3. 严谨性自检

- 原生矩阵 60/60 runs、45/45 formal passed，`comparison_admission=admissible`；三个
  adapter 各 15 formal，所有 Job 均 512/512 exactly-once。
- Project v2 为 40/40 runs、30 formal passed；四 Job 均有实测 overlap、credit 最终
  归零、manifest/endpoint/资源合同一致。
- 统一汇总通过：120 条逐 Job formal、75 条组级 formal、18 条 Project phase；四份
  manifest 在 Project 与原生轨的 SHA256 完全一致。
- 原生 provenance 逐 shard 验证：`comparison_role=framework_native_baseline`、
  `custom_scheduling_code=false`、`formal_baseline_eligible=true`，scheduler owner 分别是
  Daft Native、Daft Ray 和 Ray Data。
- 原生 adapter 只有可靠的 Job barrier 时间，故不伪造 request P95/P99；Project 的
  P95/P99 从逐请求 timestamp 重新计算。
- MFU/GPU/running/waiting/KV 均来自正式 run 的时间序列聚合，不使用单点 GPU snapshot。
- 汇总与 runner 回归测试 14/14 passed；仓库全量 secret scan 无违规。
- Project formal v1 的 replay-start 门禁误把调度后的 first-submit 延迟当成输入到达偏差，
  以及原生 formal v1 的 2% endpoint-work 门禁不接受已冻结 short 的 3.58% skew，均按
  fail-closed 保留且不进入均值；修复后另建 v2，未覆盖原始失败证据。

## 4. 实验数据

### 4.1 Project：先拆 quota，再拆真实竞争

| Job | full single JCT | quarter single JCT | static 4-job JCT | shared 4-job JCT | quarter→static 真实竞争 | static→shared |
|---|---:|---:|---:|---:|---:|---:|
| short | 13.07 s | 36.65 s | 58.79 s | 16.33 s | +60.40% | **−72.23%** |
| long1 | 38.94 s | 97.02 s | 136.39 s | 125.09 s | +40.58% | −8.28% |
| long2 | 39.73 s | 94.43 s | 135.53 s | 108.10 s | +43.52% | −20.24% |
| long3 | 40.08 s | 93.82 s | 135.28 s | 64.04 s | +44.19% | −52.66% |

full→quarter 的纯配额损失已经很大：short JCT +180.38%，三个 long +134.09% 到
+149.16%。在相同 quarter 上限下再加入其它 Job，short 仍额外 +60.40%，三个 long
额外 +40.58% 到 +44.19%；因此四 Job 退化不能只归因于静态配额，也存在真实共享服务
竞争。

shared 相对 static 明显改善全部四个 Job，但收益分配不均：long1/2/3 分别 −8.28%、
−20.24%、−52.66%。long2/long3 的 shared JCT CV 分别为 12.93%/10.19%，而 static
均低于 0.2%；现有共享策略提高效率，但仍缺少稳定的 per-job floor/SLO/fairness guard。

### 4.2 原生框架：只比较各自 single→four-job

| 系统 | Job | single JCT | four-job JCT | 变化 | four-job CV |
|---|---|---:|---:|---:|---:|
| Daft Native | short | 12.82 s | 21.35 s | +66.62% | 5.29% |
|  | long1/2/3 | 37.61/36.23/37.33 s | 110.70/108.67/103.86 s | +194.31%/+199.91%/+178.18% | 0.67%/1.03%/10.27% |
| Daft Ray | short | 24.68 s | 30.97 s | +25.48% | 2.98% |
|  | long1/2/3 | 46.79/46.68/46.53 s | 112.15/108.14/120.09 s | +139.66%/+131.68%/+158.11% | 8.01%/8.98%/0.57% |
| Ray Data | short | 138.81 s | 232.60 s | +67.57% | 25.49% |
|  | long1/2/3 | 165.30/164.68/163.41 s | 361.59/366.91/356.32 s | +118.74%/+122.80%/+118.05% | 9.12%/4.75%/6.50% |

三个原生系统的 short 和全部 long 均在四 Job 共享服务时退化；三个 long 的完成顺序也会
随 repeat 变化。该表说明多 Job 影响不只属于 Project，但不把变化归因于 Daft/Ray Data
内部算法，更不把不同系统的绝对 JCT 当作统一计时边界下的容量排名。

### 4.3 组级效率、状态和公平性

| 系统/策略 | group JCT | service tok/s | GPU util | MFU | running | waiting | KV fraction | Jain |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Daft Native 原生 4-job | 115.90 s | 10,103.78 | 93.33% | 60.57% | 264.21 | 613.73 | 0.820 | N/A |
| Daft Ray 原生 4-job | 125.22 s | 9,351.13 | 85.31% | 55.93% | 241.09 | 562.73 | 0.749 | N/A |
| Ray Data 原生 4-job | 377.73 s | 4,513.23 | 89.63% | 16.16% | 25.76 | 0.00 | 0.089 | N/A |
| Project static | 143.33 s | 11,863.05 | 98.59% | 38.20% | 95.99 | 0.41 | 0.309 | 0.960 |
| Project shared | 131.91 s | 12,892.38 | 98.59% | 46.76% | 120.76 | 0.39 | 0.429 | 0.923 |

Daft 两臂表现为 high-running/high-waiting/high-KV；Ray Data 表现为 low-running、零
waiting、低 KV 与低 MFU，即使 GPU utilization 仍接近 90%。这直接说明感知不能只看
GPU util 或 Job 数，至少要联合 completed work rate、running/waiting、KV、MFU 和 tail。

Project shared 相对 static：group tok/s +8.68%、group JCT −7.97%、MFU +8.56 个百分点、
running +24.77、KV +0.120，但 Jain 从 0.960 降到 0.923。共享更多 work 是有效机制，
但“提高总效率”与“公平地服务每个 Job”不是同一个目标。

为与 VTC/FairServe 的 work-based fairness 思路对齐，又从同一
`job_slowdown_comparisons.csv` 计算 isolated-normalized progress：对每个完整 Job 定义
`p_i = JCT_i(single matched control) / JCT_i(four-job)`，再计算
`Jain(p)=(Σp_i)^2/(4Σp_i^2)`。它不是 VTC 在服务内部的理论 service bound，而是可同时用于
Daft/Ray Data/Project barrier 证据的系统内 slowdown fairness；值越高只表示四个 Job 的
相对保留进度越接近，不表示吞吐更高。

| 系统/对照 | normalized-progress Jain | min–max progress | max/min | 事实含义 |
|---|---:|---:|---:|---|
| Daft Native four/single | 0.931 | 0.333–0.600 | 1.80× | short 保留进度明显高于 long |
| Daft Ray four/single | 0.902 | 0.387–0.797 | 2.06× | 四 Job 相对退化最不均匀 |
| Ray Data four/single | 0.984 | 0.449–0.597 | 1.33× | 相对退化较均匀，但绝对容量仍低 |
| Project static/full-single | 0.988 | 0.222–0.296 | 1.33× | 较均匀但所有 Job 保留进度都低 |
| Project static/quarter-single | 0.998 | 0.623–0.711 | 1.14× | 扣除 quota 后，竞争损失很均匀 |
| Project shared/full-single | 0.876 | 0.311–0.801 | 2.57× | 总效率提高，但 long1 得到的相对进度最低 |

这补强了现有结论：只看原始吞吐 Jain 会混入 Job 工作量和完成窗口；只看
normalized-progress Jain 又会遗漏总效率。正式讲述必须把 group throughput/MFU、原始
work-service Jain、isolated-normalized progress Jain 和最坏 Job JCT/P99 并列。

### 4.4 Project 三阶段状态

| 阶段 | static 时长 / running / completed work/s | shared 时长 / running / completed work/s | 解释边界 |
|---|---:|---:|---|
| short-only 0–5 s | 5.00 s / 59.74 / 3,194 | 5.00 s / 233.08 / 2,183 | shared 借用空闲 credit，早期 running 更高；5 s 短窗完成率有预填充噪声 |
| four-job overlap | 53.79 s / 134.29 / 12,603 | 11.33 s / 201.39 / 12,771 | shared 使 short 更早完成，所以 overlap 窗显著缩短 |
| long-only drain | 82.60 s / 75.41 / 11,449 | 113.78 s / 109.63 / 13,047 | shared 更早进入 drain，时长不可直接解释为 long 服务更慢 |

shared 的 long1/long2 到 first-submit 平均等待约 281/795 ms，而 short/long3 约 51/49 ms；
static 四个 Job 都约 39 ms。该差异与 long JCT 分配不均相吻合，是后续调度器需要显式
约束 admission order、deficit/floor 和 SLO slack 的代码方向。

## 5. 结果解释

### 事实

1. 四个 Job 都是完整 512 行；quarter 是容量反事实，不是缩小 workload。
2. Project 的静态四分区同时包含 quota loss 和真实竞争；配对 single-quarter 已将二者分开。
3. 三条原生路径中，short 和三个 long 在四 Job 重叠时均相对自身 single 退化。
4. 原生系统落入不同服务压力形态；GPU utilization 单指标不能区分 overqueue 和 underfeed。
5. Project shared 提高 aggregate 效率并改善所有 Job JCT，但 Jain 和 long 间稳定性回退。

### 推断与对课题的含义

- **Work Unit / 数据组织**：调度单位必须携带 per-job primary work、stage、remaining work、
  locality、deadline/SLO 和 uncertainty；固定 Job 数、行数或均分 1/4 都不能表达真实需求。
- **状态感知**：至少观测 job arrival/active/drain、remaining/completed work，以及 endpoint
  running/waiting、KV、MFU、completion rate 和 tail；观测用于区分欠供给、平台和过量排队。
- **动态调度**：idle borrowing 有价值，但必须加入 per-job floor/cap、work-fair deficit、
  SLO/fairness guard 和 admission-order 控制；当前 shared 不是最终算法。
- **算子代价估计**：需要为 work/remaining work、service time 和 SLO slack 提供统一输入，
  以初始化 credit、计算 deficit 和判断借用/回收。估计精度不由本实验验证，仍以 429-formal
  context-LOO 的 ranking/regret 结果为证据。

### 不能声称

- 不能说 Project 普遍优于 Daft 或 Ray Data；原生与 Project 的完整 T0/执行边界不同。
- 不能说 shared/dynamic 已全面优于 static；本实验中 Jain 与 long 稳定性回退，旧在线
  replay 中也出现 shared 伤害 short 的相反方向。
- 不能把 Daft/Ray Data 的外部状态归因于其内部调度算法。
- 不能伪造原生 request P95/P99，也不能把短 single cell 当成 ≥60 s 框架容量排名。
- 不能外推到 weighted priority、Long→Short 前台到达、图像 phase-change、音频或视频。

## 6. 待画图清单（本轮不画）

1. **Project 因果分解图**：每个 Job 的 full→quarter→static→shared JCT/P99/work-rate；
   用连线或 waterfall 明确 quota-only、competition-only 和 scheduler effect。
2. **原生系统内干扰图**：Daft Native、Daft Ray、Ray Data 分面显示四个 Job 的
   `single→four-job` normalized JCT 与 CV；不画跨系统绝对 JCT 排名。
3. **效率—公平权衡图**：Project static/shared 的 group tok/s、JCT、MFU、Jain 和三个
   long 的 JCT spread，用 aligned small multiples，不用双 y 轴。
4. **Project 三阶段状态图**：short-only、four-overlap、long-drain 的 running、completed
   work rate、KV、active work；阶段宽度按真实时间，解释 shared 提前结束 short。
5. **原生状态指纹图**：三个原生系统的 running/waiting/KV/MFU 四个小图，突出
   overqueue 与 underfeed，不用无解释散点。
6. **Admission 分解图**：Project 每个 Job 的 arrival→first-submit 与 submit→completion，
   对比 static/shared，解释 long 间收益分配和公平性缺口。

开题正文可把 1+3 合成“为什么需要多 Job 管理/动态调度”，把 2+5 合成“为什么需要
状态感知”；4+6 放备份页或方法实验设计页。

## 7. 数据与服务器归档

Git 保存紧凑、可绘图且已通过哈希复核的数据：

- `data/combined/job_formal_runs.csv` / `job_summary.csv`：逐 Job formal 与三重复汇总；
- `data/combined/job_slowdown_comparisons.csv`：全部因果/相对变化；
- `data/combined/isolated_normalized_fairness.csv`：按各 Job 自身 single control 归一化的
  slowdown fairness、服务差和最坏 Job；
- `data/combined/group_formal_runs.csv` / `group_summary.csv`：组级效率和服务状态；
- `data/combined/long_job_spread.csv`：三个 long 的离散度、完成顺序和最慢 Job；
- `data/combined/project_phase_runs.csv` / `project_phase_summary.csv`：Project 三阶段状态；
- `data/combined/audit.json`：统一门禁、行数、manifest SHA 和 timing 边界。

服务器保留完整目录：

```text
/root/autodl-tmp/experiment-artifacts/opening_project_fourjob_formal_20260809_v2
/root/autodl-tmp/experiment-artifacts/opening_text_native_fourjob_formal_20260809_v2
/root/autodl-tmp/experiment-artifacts/opening_fourjob_summary_20260809_v1
/root/autodl-tmp/experiment-artifacts/opening_fourjob_full_archive_20260809_v1.tar.gz
```

完整 archive 为 34 MiB，SHA256：

```text
db705caac77c438f272a9ac1e4687b69dfef3be96500f768b9e7ca5d1ca416fb
```

归档同时包含 gate、停止的 Ray Data concurrency 诊断、Project/native v1 失败证据、v2
正式证据、不可变 manifest、日志和汇总；未删除或覆盖服务器上的任何扫描数据。

## 8. 下一步

开题实验层已足以说明：多 Job 共享服务会产生 quota、竞争、状态与公平问题；Work Unit、
状态感知、动态调度和代价估计各自有对应证据/设计职责。停止继续扩大 offset、Job 数或原生
框架参数矩阵。代码优化的最小方向是给 shared scheduler 增加 per-job floor/SLO/fairness
guard，并以同一 full/quarter/static/shared 合同做消融；图像 phase-change 与 weighted
priority 留论文阶段，不作为当前开题 blocker。
