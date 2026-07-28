# 双 GPU 请求级连续补充实验

日期：2026-07-28

## 1. 实验问题

本实验比较 Ray 上游的 whole-submission completion barrier 与
request-level continuous replenishment，并验证双 endpoint 下 global K 与
per-endpoint K 的语义。核心问题不是“更大的 K 是否更快”，而是：

1. 在相同每卡 offered work 下，请求级补位是否优于整批完成后补位；
2. 以请求/批次数量计 credit 是否会因 admission unit 大小不同而失真；
3. 当前扫描是否已经找到 request-level 的容量甜点。

## 2. 实验设置

- 链路：PostgreSQL 18.4 → Daft → Ray actor → 两个独立 vLLM endpoint →
  Qwen2.5-7B，2× RTX 4090。
- workload：ShareGPT/BurstGPT，1024 行，arrival replay scale `0.001`。
- 数据组织：sequential token-budget，`token_budget=32768`，
  `ray_batch_rows=64`，fixed 50 ms flush。
- 每个场景 1 次 warm-up、3 次 formal repeat，场景交错运行。
- admission：batch 使用约 2.977 行/submission；request 使用 1 行/slot。
- SLO：300 s。所有场景 violation 均为 0，因此本实验的 SLO goodput
  没有区分能力。
- 服务 manifest 中 `max_num_batched_tokens` 和 `max_num_seqs` 为
  `unknown`。结果可用于本次固定服务实例内比较，不能用于声明 vLLM 容量上限。

精确展开参数和运行顺序见 `manifest.json`，逐次汇总见 `runs.csv`。

## 3. 严谨性自检

- 15/15 formal runs 成功；每个场景 `n=3`。
- global K32 与 per-endpoint K16 的吞吐均值仅差 0.31%，支持
  “双 endpoint 下两者等价”的实现语义。
- admission 的预测工作量按
  `prompt_tokens + estimated_output_tokens` 计算。batch 每个 slot 平均
  1106.892 token，request 每个 slot 平均 371.847 token。
- 因此 batch K16 的名义每卡在途工作约 17,710 token；request K48 约
  17,849 token，才是本矩阵中最接近的 work-matched 对照。request K64
  约 23,798 token，比该对照高约 33.3%。
- batch 场景的 request latency 为 submission 粒度：同一 batch 内的行共享
  完成时间；request 场景为真实 request 粒度。两类场景的 P95/P99 仅作诊断，
  不能视为完全同口径的逐请求尾延迟因果比较。
- 三次重复足以判断大幅、稳定信号，但不足以支持跨 workload、模型或硬件泛化。

## 4. 正式结果

| 场景 | tps | MFU | E2E (s) | P95 (s) | P99 (s) | bounded wait (s) |
|---|---:|---:|---:|---:|---:|---:|
| batch / global K32 | 4234.77 | 18.40% | 81.30 | 25.60 | 26.36 | 40.48 |
| batch / per-endpoint K16 | 4247.97 | 18.37% | 80.97 | 25.52 | 26.74 | 40.49 |
| request / per-endpoint K32 | 3534.55 | 15.21% | 97.29 | 39.98 | 42.29 | 55.34 |
| request / per-endpoint K48 | 4248.01 | 18.43% | 80.95 | 25.71 | 31.67 | 37.89 |
| request / per-endpoint K64 | 4768.04 | 20.54% | 72.15 | 23.93 | 33.15 | 29.06 |

相对 batch per-endpoint K16：

- request K48：tps `+0.001%`、E2E `-0.019%`，吞吐与总时间不可分辨；
  bounded wait `-6.43%`，但 P99 `+18.45%`。
- request K64：tps `+12.24%`、MFU `+11.80%`、E2E `-10.89%`、
  bounded wait `-28.22%`；同时 P99 `+24.00%`。

逐次均值及 admission-work 核算见 `formal_summary.csv`。

## 5. 结果解释

### 实验事实

1. global K32 与双 endpoint 的 per-endpoint K16 等价，确认 K 语义修复正确。
2. request K32 的 offered work 只有 work-matched 水平约 66.7%，吞吐较低主要是
   供给不足，不能解释为 request 粒度本身更差。
3. 在最接近等工作量的 batch K16 与 request K48 之间，吞吐、MFU 和 E2E
   基本相同。本实验尚未隔离出 continuous replenishment 的吞吐增量。
4. request K64 是本矩阵的最高吞吐场景，但它同时提高了约 33.3% 的名义
   offered work；其 `+12.24%` 吞吐不能全部归因于请求级连续补充。
5. request K64 仍位于已测曲线的上边界，且 P99 变差，不能称为容量最优点。

### 合理推断

- “一个 batch/request 消耗一个等价 K slot”不是跨粒度稳定的 admission
  语义。使用 token-based active-work credit 是后续公平比较和在线控制的必要条件。
- 请求级补位提供了消除 completion barrier 的机制基础，但本轮数据只证明
  机制可运行以及更高 offered work 能继续填充服务，尚未证明机制本身带来吞吐收益。

### 不能声称

- 不能声称 request-level continuous replenishment 已被证明普遍优于 batch barrier。
- 不能把 K64 写成最优配置；只能称为 `BEST_TESTED_REQUEST_K`。
- 不能用 300 s SLO 的全零 violation 声称 SLO 得到改善。
- 不能从本实验推出 `token_budget=32768` 或 vLLM 内部 capacity 的最优值。

## 6. 对课题的含义

这组数据把“力大砖飞”的现象拆成了两个变量：数据如何分组，以及向服务提供
多少 token work。当前吞吐提升主要跟 offered work 增加同步，说明研究重点应从
继续放大 batch/K，转为在固定 active work 下比较组织与补位机制，并同时约束
P99/SLO。active-work admission 因而不是额外复杂度，而是让策略实验具备可比性
和因果解释的基础。

## 7. 下一步

1. 先运行 request-level per-endpoint active-work curve，覆盖
   16,384–65,536 predicted tokens，找到饱和区和尾延迟拐点。
2. 固定同一 `max_active_work_per_endpoint`，把 `max_inflight` 设为非约束上限，
   直接比较 batch barrier 与 request replenishment；保持组织分组、arrival、
   flush、路由和 endpoint 容量完全一致。
3. 使用有区分能力的 30 s SLO，并显式固定、记录 vLLM
   `max_num_batched_tokens` 与 `max_num_seqs`。
4. 主结果同时报告 throughput、MFU、P95/P99、bounded wait 和 SLO goodput；
   只有在 matched work 下仍有稳定增量，才能把收益归因于 continuous
   replenishment。
