# 文本多 Job 前台干扰与共享调度权衡（2026-08-09）

## 1. 实验目的

本实验只回答一个开题动机问题：当一个 long Job 在 short Job 已经运行后到达，现有
执行路径会怎样影响前台 short Job；项目的固定分区和共享 work-credit 又呈现什么
效率、隔离与公平权衡。

到达方向固定为 `Short@0s → Long@5s`。因此本实验的反事实是“已运行的 short 在
long 到达前后如何变化”，不回答“long 已占用服务后，新到 short 的排队/SLO”。后者
需要独立的 `Long→Short` 对照，属于论文阶段的补充问题，不是本轮干扰结论的替代条件。

这不是完整系统排名，也不验证图像、多模型、weighted fairness 或最终 state-aware
控制器。原 15 s offset 下 Daft Native 的 short 在 long 到达前结束，因而只保留为
arrival observation；本报告以所有系统共同使用的 5 s guaranteed-overlap 补充矩阵为准。

## 2. 实验设置

- 硬件与服务：2×RTX 4090，两个独立 vLLM endpoint，Qwen2.5-7B-Instruct，
  `max_num_batched_tokens=8192`、`max_num_seqs=256`、prefix cache on、MFU metrics on。
- workload：两个互斥 ShareGPT manifest，各 512 行；short SHA256 为
  `85b3f90c...c971`，long SHA256 为 `8e532819...e9c1`。short 在 0 s 启动，long 在
  5 s 启动。
- 重复：每个场景 1 warm-up + 3 formal，正式顺序由 seeded runner 交错。
- 项目路径：PostgreSQL/Daft source、token-budget 6144、request-level replay、两个
  endpoint；全局上限 K128/W65,536 per endpoint。比较 `static_partition` 与
  endpoint-shared DRR/work credit。
- 项目匹配控制：single-short full pool，以及只启用一个 short、但预留两个静态分区的
  half pool。后者让 short 获得 K64/W32,768，却不启动 synthetic competing Job。
- 原生路径：Daft `functions.prompt` Native、Daft `functions.prompt` Ray runner、Ray Data
  HTTP official graph。每个 Job 独立启动两个官方 endpoint shard；不注入项目 credit、
  router、actor pool 或调度器。5 s 只对齐 **Job 启动时刻**；原生 graph 在 Job 启动后
  获得完整 manifest，不重放每行 `arrival_time`，而项目按 `arrival_time_scale=0.001`
  逐请求 replay。因此两轨的绝对 JCT/吞吐不具备系统排名合同。
- 边界：`writeback-mode=none`。该实验测 serving-side multi-job interference，不承担
  database-E2E sink 排名；项目 request trace 可报告 short P99/work rate，原生 adapter
  未采集 request P95/P99，不能由 Job barrier JCT 伪造。

## 3. 严谨性自检

- 项目 5 s 矩阵：8/8 group、6/6 formal，manifest status completed；资源、MFU、
  exactly-once、零 worker failure/incident 和 manifest 隔离门全部通过。
- 原生 5 s 矩阵：12/12 cells、9/9 formal、0 error；exactly-once、provenance、服务
  counter 和两个 endpoint 使用门通过。
- 统一汇总：30 formal rows、10 summary rows、6 comparisons、18 project phase rows、
  12 条 project request-timing rows；short manifest 身份一致，且逐次满足
  `JCT = arrival span + post-last-arrival drain`。所有 two-job arms 的实际 overlap 都大于 0。
- formal 稳定性：项目 service tok/s CV 为 0.065%（static）和 0.228%（shared）；原生
  two-job service tok/s CV 为 0.59%（Daft Native）、1.18%（Daft Ray）、0.60%
  （Ray Data）。
- 两次无效启动均单独保留：v1 在创建 cell 前因 offset 环境变量未展开而失败；v2 在
  static warm-up 后因一次 `httpx.ReadError` fail closed，不进入正式均值。endpoint 随后
  健康、服务日志无 5xx/CUDA/engine crash；残留的具名 shared-credit actor 已精确清理，
  异常路径 cleanup 已加入回归测试。

## 4. 实验数据

### 4.1 项目：先隔离 quota，再测 long 竞争

| 对比 | short JCT | short P99 | short work rate | 实际 overlap |
|---|---:|---:|---:|---:|
| full-pool single → half-pool single | −0.003% | −0.013% | −0.004% | 0 s |
| matched half single → static + long | **+3.79%** | **+90.80%** | **−3.57%** | 68.94 s |
| matched full single → shared + long | **+8.95%** | **+173.33%** | **−8.28%** | 72.62 s |

full/half single-short 的 JCT 均约 71.24 s，说明 K/W 减半在这个 short workload 上不是
退化来源；long 真正加入后，short 的尾延迟和完成 work rate 才发生变化。

### 4.2 项目：共享额度提高总效率，但没有免费隔离

| 指标 | static partition | shared work credit | shared 相对 static |
|---|---:|---:|---:|
| group service tokens/s | 7,028.59 | 8,506.81 | **+21.03%** |
| long JCT | 117.19 s | 95.73 s | **−18.31%** |
| short JCT | 73.94 s | 77.62 s | **+4.98%** |
| max request P99 | 50.89 s | 31.73 s | **−37.66%** |
| MFU | 26.50% | 32.01% | +5.51 pp |
| Jain fairness（median） | 0.759 | 0.707 | −0.052 |

5 s 前的 pre-long 窗口中，两臂都只有约 6.3 running requests，随后 overlap 窗口提高到
约 67.6（static）和 86.7（shared）。shared 把更多全局 work 投入服务，因此提高总吞吐
并缩短 long drain；与此同时 short JCT/work rate 和 Jain fairness 变差。动态/共享
调度的目标必须显式包含前台隔离和公平，而不能只优化 aggregate throughput。

### 4.3 原生框架：同一系统内的 single → overlap 观察

| 系统 | single short JCT | short+long short JCT | short JCT 变化 | overlap | two-job MFU | two-job waiting mean |
|---|---:|---:|---:|---:|---:|---:|
| Daft Native | 11.06 s | 20.17 s | **+82.42%** | 15.17 s | 58.72% | 167.18 |
| Daft Ray | 14.74 s | 30.19 s | **+104.84%** | 25.19 s | 50.85% | 147.16 |
| Ray Data HTTP | 128.91 s | 171.14 s | **+32.76%** | 166.14 s | 17.67% | 0.00 |

三条原生路径都发生真实重叠，且后到 long 与 short 的 JCT 退化同时出现。Daft 两臂表现为
高 running/high waiting/KV 接近满，Ray Data 表现为 low running/no waiting/低 MFU；
因此仅用“Job 数”或 GPU utilization 不能描述共享服务压力，需要联合 work、完成速率、
running/waiting、KV、MFU 和 tail 状态。

原生对比标记为 `observational:overlap_present`：它描述两个独立官方应用竞争同一 vLLM
服务后的外部现象，不把框架内部算法或项目未控制的提交语义作因果归因。

### 4.4 为什么项目 single short 是 71.24 s，而 Daft Native 是 11.06 s

这两个绝对值不能解释为“项目比 Daft 慢 6.4 倍”。项目保留逐请求 arrival replay；
Daft Native/Daft Ray/Ray Data 只执行 Job 级错峰，Job 启动后完整 manifest 已对原生 graph
可见。项目 single-short 的 service tok/s=2,218、running mean=26.1、waiting=0、KV≈3.0%、
MFU=6.63%；Daft Native 分别为 14,727、250.1、0、24.5%、44.04%。项目在 pre-long
前 5 s 也只有约 6.3 running、完成 6 条请求，说明该 71.24 s 主要处于 arrival-limited
在线回放，而不是最大吞吐测试。

项目 full/half pool 的 JCT 为 71.2416/71.2397 s，进一步排除 K/W 减半是该绝对值的
主要来源。Ray actor、token-budget 和 flush 的独立成本仍未由同 replay bounded control
分解，不能从现有矩阵继续归因。该合同不影响各轨内部 `single → two-job` 的干扰变化，
但禁止把项目与原生轨的绝对 JCT、tok/s、running 或 MFU 横向排名。

### 4.5 项目 71.24 s 的逐请求时间分解

服务器 raw 回读后，项目 single-short full-pool 的三次 formal 精确分解为：

| 时间边界 | mean | P99/总量 | 含义 |
|---|---:|---:|---|
| arrival span | 66.875 s | 占 JCT 93.87% | 512 条请求按冻结 replay 时钟逐步变为可提交 |
| post-last-arrival drain | 4.367 s | — | 最后一条到达后到全 Job 完成 |
| arrival→flush buffer | 75.14 ms/request | 87.25 ms | 50 ms flush 与 replay/pending 形成的上游等待 |
| flush→submit | 3.29 ms/request | 9.23 ms | scheduler/actor 提交前等待 |
| submit→service | 3.00 ms/request | 6.40 ms | HTTP/Ray 到服务起点 |
| service | 3.847 s/request | 4.838 s | backend service span |
| request E2E | 3.928 s/request | 4.922 s | 上述逐请求边界总和 |

因此 `71.2416 = 66.8750 + 4.3666 s`；除 backend service 外的逐请求平均开销约
81.4 ms，只占 request E2E 约 2.1%。项目 profiler 的更宽 E2E 为 79.394 s，其中
source fetch 2.216 s、actor-ready 4.389 s、operator wall 71.245 s；这些 stage 有嵌套，
不得相加。Daft Native 的 11.059 s 从完整 manifest、provider、DataFrame 和 expression
均已准备后才在 `collect()` 前开始，故仍不是与 79.394 s 相同的 pipeline 边界。

在模型服务内部，项目 single-short 的 vLLM request E2E mean 为 3.837 s，低于
Daft Native 的 6.654 s；项目 decode/prefill 为 3.785/0.0365 s，Daft Native 为
6.381/0.1056 s。Daft 的总 Job 更快来自一次性暴露全部 work，令 running mean 从项目
26.1 提到 250.1、MFU 从 6.63% 提到 44.04%，而不是 Daft 单请求 service 更短。

后续 Project all-at-t0 1+3 已按统一 T0–T4 合同补测。其 T0 profiler E2E 为14.957s；
对齐到 T3“最早模型提交→最晚响应完成”为11.354s，Daft Native 同边界为11.059s，
Project 仅+2.67%。两者 service tokens/s 为14,361/14,727，MFU为42.93%/44.04%，
差约−2.5%。因此“Project 模型请求路径慢6.4×”已被排除。Daft 未记录 source/framework
准备前的 T0，故Project14.957s与Daft11.059s仍不能作为完整系统排名。权威对齐数据见
`../opening_project_short_all_at_t0_diagnostic_20260809/`。

### 4.6 long Job 具体影响了 short 的哪些阶段

arrival span 在所有项目场景都固定为 66.875 s，所以 long 没有改变 offered-arrival
合同；它改变的是最后 drain、上游 pending/backpressure 和 backend service：

| 指标 | matched single | static + long | 变化 | matched single | shared + long | 变化 |
|---|---:|---:|---:|---:|---:|---:|
| post-last-arrival drain | 4.365 s | 7.061 s | +61.78% | 4.367 s | 10.741 s | +145.98% |
| buffer mean | 73.4 ms | 129.8 ms | +76.78% | 75.1 ms | 990.8 ms | +1,218.61% |
| buffer P99 | 85.6 ms | 917.4 ms | +972.06% | 87.3 ms | 3.835 s | +4,295.48% |
| flush→submit P99 | 7.77 ms | 698.8 ms | +8,893.00% | 9.23 ms | 1.285 s | +13,814.24% |
| service mean | 3.845 s | 6.142 s | +59.74% | 3.847 s | 7.239 s | +88.17% |
| service P99 | 4.837 s | 8.433 s | +74.36% | 4.838 s | 10.170 s | +110.22% |
| request E2E P99 | 4.922 s | 9.380 s | +90.59% | 4.922 s | 13.454 s | +173.33% |

submit→service mean 没有变差（约 2–3 ms），vLLM queue mean 也只有 23–62 µs；因此
long 的主要影响不是“请求卡在 vLLM waiting 队列”，而是：共享 GPU 后 prefill/decode
service 变长，同时项目 Ray/pending/credit 层出现 buffer 与 flush→submit 软拥塞。
shared work 把更多 long work 注入服务，故 aggregate throughput 更高，但 short 的这两层
退化都比 static 更强。

### 4.7 统一 eager 到达后的 Project 配对重测

为消除 Project 在线 replay 与原生 eager-manifest 的到达差异，追加 Project-only
near-all-at-t0 诊断：同一 short/long manifest、同一多 Job runner 和 T1 request-JCT
边界，DB 的 66.875 s arrival span 统一压缩为 66.76 µs。三个主场景和独立 half-pool
控制均为 1 warm-up + 3 formal，12/12 formal exactly-once、零 incident；所有 short
均为 512 条且 endpoint 各 256 条。

干扰增量统一使用本矩阵内的 full/half single，不把前一节独立 generic runner 的
T3=11.354s 混作基线；本矩阵 full single 的同 runner T1 JCT=12.379s。这样 single 与
two-job 的启动延迟、资源采样、Job barrier 和统计代码完全一致。

| Project 场景 | short JCT | short P99 | short work/s | group tok/s | MFU | running mean | long JCT |
|---|---:|---:|---:|---:|---:|---:|---:|
| full-pool single | 12.379 s | 12.338 s | 11,980.6 | 11,726.1 | 41.82% | 186.2 | — |
| half-pool single | 19.682 s | 19.671 s | 7,505.1 | 7,647.7 | 22.87% | 98.4 | — |
| static half + long | 31.249 s | 30.724 s | 4,729.2 | 9,287.2 | 34.79% | 68.4 | 86.694 s |
| shared full + long | 15.957 s | 15.921 s | 9,267.7 | 12,245.0 | 45.57% | 102.4 | 64.375 s |

这组配对把两个影响分开了：full→half 的 quota-only 已使 short JCT +59.00%；在相同
half quota 下，long 真正加入又使 static short JCT +58.77%、P99 +56.19%、work rate
−36.99%。shared+long 相对 full single 的 short JCT/P99 为 +28.90%/+29.04%，但相对
static+long 将 short JCT 降低 48.94%、group tok/s 提高 31.85%、long JCT 降低 25.75%，
Jain fairness 从均值 0.894 提到 0.972。这里的 shared 价值是 work-conserving idle
borrowing，不等于最终 state-aware 控制器已经验证。

long 对 short 的细阶段影响也已对齐。static 的 matched half→long 使
arrival→flush mean/P99 +45.64%/+54.83%，flush→submit +67.45%/+100.63%，service
+50.34%/+78.62%，request E2E +48.16%/+57.49%；shared 的 matched full→long 主要使
service mean/P99 +14.63%/+28.70%，而 arrival→flush 仅 +6.08%/+6.54%。
submit→service 仍约 2 ms，不是主要瓶颈。

状态时间线解释了差异：long 到达前，static 预留一半容量时 running 总和均值为 120.6，
shared 可借空闲额度达到 230.1；overlap 时分别为 133.3/184.8。两臂 GPU utilization
都接近 100%，但 MFU、running、KV 和完成速率明显不同，再次说明不能只看 GPU util。
在线 replay 下 half quota 近似中性且 shared 伤害 short；eager 下 half quota 明显欠供给且
shared 同时改善效率、隔离和公平。因此策略价值具有 arrival regime dependence，不能用
任一单一负载宣称 shared/dynamic 普遍胜出。

与原生轨只报告系统内归一化 short-JCT 影响：Daft Native +82.42%、Daft Ray +104.84%、
Ray Data +32.76%，Project static 的 matched competition-only +58.77%、Project shared
+28.90%。这些是各轨内部的干扰指纹；原生 adapter 没有 request P99，且 T0 准备边界仍
不同，所以不把归一化变化进一步包装成跨框架方法排名。

## 5. 结果解释与开题对应

### 事实

1. 没有 overlap 的 15 s Daft Native 结果不能证明前台干扰；统一 5 s 后三条原生路径与
   两条项目策略都发生真实 overlap。
2. 在线 replay 下 half quota 近似中性；eager 下 half quota 单独使 short JCT +59.00%。
   静态预留是否浪费容量取决于到达 regime，不能跨负载复用结论。
3. 在线 replay 下 shared 提升总吞吐但伤害 short/fairness；eager 下 shared 相对 static
   同时改善 short、long、总吞吐和公平，但相对 full single 仍使 short JCT +28.90%。
4. 现有原生路径在相同任务下落入 overqueue 或 underfeed 等不同状态形态，且 short
   都受到后到 long 的影响。
5. 项目与原生轨只对齐 Job 级 5 s offset，没有对齐逐请求 arrival replay；71.24 s 与
   11.06 s 不构成系统绝对性能比较。
6. 项目 71.24 s 中 66.875 s 是冻结 arrival span；single-short 的逐请求 service 并不慢于
   Daft Native。long 加入后，同时放大 short 的 backend service 和项目上游 buffer。
7. Project all-at-t0 的 T3/service throughput/MFU 与Daft Native只差约2.5%–2.7%；
   当前大差距来自 arrival 与计时合同，不应先扫K256/K512或把short人为拉到60s。
8. eager Project 重测的 12 formal 和 half-pool 控制均通过；long 对 short 的 backend
   service 与上游 pending 影响被分别量化，shared 的收益来自 idle borrowing 而非 GPU
   utilization 单指标。

### 对设计的支撑

- **Work Unit / WorkDescriptor**：quota 和竞争应按 prompt/output work，而不是只按 Job
  数或行数计量；还需携带 job、stage、deadline/SLO、uncertainty 与 locality 元数据。
- **感知**：至少观测 per-job remaining/completed work、arrival/active/drain 状态，以及
  endpoint completion rate、running/waiting、KV、MFU 和 tail。
- **动态调度**：需要 work-conserving idle borrowing，但必须同时设置 per-job floor/cap、
  fairness 或 SLO guard；本实验说明“共享更多”不是完整策略。
- **算子代价估计**：为 remaining work、service time、SLO slack 和 credit 大小提供共同
  输入；本实验本身不验证估计精度，精度与 selection regret 仍由 cost-profile 实验承担。

### 不能声称

- 不能说 shared/dynamic 全面优于 static；在线 replay 中 short isolation/fairness 回退，
  eager 中则改善，结论明确依赖到达 regime。
- 不能从原生 JCT 变化归因 Daft/Ray Data 内部调度算法，也不能称项目已优于三个框架。
- 不能把项目 arrival-replay 的 71.24 s 与原生 eager-manifest 的 11.06/14.74/128.91 s
  做绝对 JCT 或吞吐排名。
- 不能把原生 short cell 当作 ≥60 s 稳态容量排名，不能伪造原生 request P99。
- 不能外推到 4+ Job、weighted/SLO、图像、音频、视频或故障恢复。
- 不能外推为 `Long→Short` 的新到前台 SLO 结论；本轮只运行了 `Short→Long`。
- 不能把本实验当 database-E2E sink 结果；它有意使用 no-writeback 来隔离 serving 竞争。

## 6. 待画图清单（本轮不画）

1. **前台干扰主图**：每个系统一组 `single short` 与 `short+long` 的 normalized short-JCT
   delta/误差线，同时标注实际 overlap；Project 使用 eager matched full/half 控制，不画
   跨轨绝对 JCT 柱，也不混排原生 request P99。
2. **项目因果分解图**：full single、half single、static+long、shared+long 四点，分别显示
   short JCT、P99 和 work rate；在线/eager 分面，突出 quota-only 与 competition 都会
   随 arrival regime 改变。
3. **效率—隔离权衡图**：static/shared 的 aggregate tok/s、long JCT、short JCT、Jain
   fairness 四个 aligned small multiples，并按在线/eager 分面；避免双 y 轴和无解释散点。
4. **状态时间线图**：0–5 s pre-long、overlap、long-drain 三段，画 running/work rate/GPU
   util；MFU 只报 group aggregate，因为没有 interval FLOPs counter。
5. **原生状态指纹图**：Daft Native、Daft Ray、Ray Data 的 running、waiting、KV、MFU
   小倍图，用来解释相同“两个 Job”为什么处于不同服务压力形态。

这五项可压缩成开题正文两张组合图：一张回答“后到 Job 是否伤害前台”，一张回答
“为什么需要 work-aware、state-aware 的多 Job 调度”。

## 7. 数据与服务器归档

Git 只保存紧凑审计数据：

- `data/combined/`：统一 30-row formal、10-row summary、6 个对比和三阶段数据；
- `data/eager_project/`：12-row Project eager formal、四场景汇总、quota/competition
  对比、short 逐阶段分解、三阶段状态和跨轨归一化干扰数据；
- `data/combined/project_request_timing_summary.csv`：项目四场景逐请求时间分解；
- `data/combined/single_short_project_daft_timing.csv`：项目/Daft timer 与 vLLM 边界对齐表；
- `data/combined/project_long_impact_breakdown.csv`：long 对 short 各阶段的 matched-control 增量；
- `data/combined/project_issue_audit.csv`：已确认、已排除、待 same-replay 验证的问题台账；
- `data/project/`：项目 5 s static/shared 的逐次、汇总、pairwise 与 audit；
- `data/native/`：三条原生路径的逐次、汇总与 audit。

服务器保留全部 manifest、commands、per-job requests/submissions/resources/credits、原生
shard log、GPU/service time series 和失败 incident：

| 归档 | SHA256 |
|---|---|
| `opening_multijob_forced_overlap_20260809_v3.tar.gz` | `f766faf7f91fb3a30a6dde8ab1b79c6cc02bae4678a5454bc4e533abae814cfa` |
| `opening_short_job_controls_20260809_v1.tar.gz` | `b8bcb0be35bf46e07806b24b7e838781dc4de50410289f932a9674080ff02480` |
| `opening_short_job_native_controls_20260809_v1.tar.gz` | `5cd8daa607e986a2b2f8503368a4bde0db9d3968c85ce13be5d4f38b6c348a93` |
| `opening_text_native_multijob_forced_overlap_20260809_v1.tar.gz` | `515b33a5a07e77c39131e02ba1ee8fcb1ff3c000b4f2a582cd117c6b5ca095a7` |
| `opening_short_job_interference_forced_overlap_20260809_v1.tar.gz` | `b7aa4c8b6cd728285fa3929acdec0a03ac5052492ef7f1dc99bf8419ea617e6d` |
| `opening_project_multijob_eager_retest_20260809_v2.tar.gz` | `713292a1e1f0998a2721b0f747a02c2d0ea60cd1a42b89044f05165c5500c4df` |
| config-load failed v1 | `fbe52e3a53a76d0660b23253e6295d78f3d4dda64814ff5a06260122cf096c8e` |
| transient-ReadError failed v2 | `6c2bc324accfa92efbf7f1d2a7a25fee480a50e1ce773bca06da1380890ef77a` |

服务器原始目录不删除、不覆盖：三份全量目录位于
`/root/autodl-tmp/experiment-artifacts/<run_name>/`（当前约 11 MiB、16 MiB、36 KiB），
压缩包位于 `/root/autodl-tmp/experiment-artifacts/archives/`。2026-08-09 回读复核时三份
压缩包 SHA256 与上表完全一致；截至归档后仍约有 23 GiB 可用空间。
