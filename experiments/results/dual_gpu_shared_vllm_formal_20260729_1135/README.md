# Shared-vLLM 1/2/4-job 正式实验

## 1. 实验设置

- 日期：2026-07-29
- 远端结果目录：
  `/root/autodl-tmp/ai-operator/experiments/results/dual_gpu_shared_vllm_formal_20260729_1135`
- 代码版本：`1f1d165f2a1e84ba8f42dfa78599ed6341641ae5`
- 硬件：2 × NVIDIA RTX 4090；每卡一个独立 vLLM endpoint
- 模型：Qwen2.5-7B
- Ray：所有 job、monitor 和 shared-credit coordinator 显式连接同一个
  `127.0.0.1:6380` cluster
- 每 job 512 行；request-level completion/replenishment；token budget 32,768
- endpoint 总容量：256 active requests、65,536 predicted active work
- 组合：`job_count ∈ {1,2,4}` ×
  `{independent_full, static_partition, shared_drr}`
- 每个组合 1 次 warm-up + 3 次 formal，共 36 个 group run；正式统计使用
  27 个 formal run

紧凑证据：

- [`group_runs.csv`](group_runs.csv)：36 个 group run 的原始组级记录
- [`manifest.json`](manifest.json)：完成状态、场景顺序与 incident
- [`formal_summary.csv`](formal_summary.csv)：27 个 formal run 的聚合统计
- [`credit_summary.csv`](credit_summary.csv)：shared-credit 精确峰值与
  work-conservation 审计

完整 per-job request/submission trace、资源 trace 和 credit trace 保留在远端
结果目录；仓库只保存可复核的紧凑结果，避免提交数十 MB 原始 trace。

## 2. 实验设计

研究问题不是“增加 job 能否让 GPU 更忙”，而是：多个数据库 AI job 共享同一组
vLLM endpoint 时，endpoint-shared request/work credit 与 work-conserving DRR
能否在不突破下游容量的前提下，改善高竞争下的吞吐、尾延迟与公平性。

三臂含义：

- `independent_full`：每个 job 独立持有完整 credit，代表当前过量认购行为；
- `static_partition`：将 endpoint 容量静态均分，容量安全但空闲份额不可借用；
- `shared_drr`：一个 Ray named coordinator 持有 endpoint 全局 credit，
  以 equal-weight DRR 分配，允许借用空闲份额。

预注册门槛：

1. 硬正确性：0 incident、0 worker failure、每 job exactly-once、全局
   request/work 不越界、结束状态归零；
2. 1-job：`shared_drr` 相对等价静态配置吞吐损失不超过 3%；
3. 2/4-job：Jain fairness 中位数至少 0.95，任一 job 的 normalized service
   不低于均值 90%；
4. 相对 `independent_full`，最大 JCT 或 request P99 至少改善 5%，同时
   group tokens/s 不退化超过 5%。

## 3. 严谨性自检

- 36/36 group run 完成，manifest 为 0 incident；runner、租约均正常退出。
- 63 个 formal job trace 共 32,256 条 request；request id 全局唯一，
  每个 job 均为 512/512 completed，doc id 在 job 内 exactly-once。
- 同一 group 的不同 job 故意重放同一组 512 个 doc，用于 equal-workload
  隔离；跨 job 的唯一性由 request id 保证，不能把重复 doc id 误判为重复执行。
- 每个 formal job 都使用两个 endpoint；汇总分布为
  `task-0=16,135`、`task-1=16,121`。
- formal 最大启动 skew 为 24.5ms，最大 start lateness 为 210.8ms，
  小于 0.5s 门禁，不足以解释策略差异。
- 9 个 formal `shared_drr` credit trace 均满足：
  active requests 峰值 197 ≤ 256，active work 峰值 65,536 ≤ 65,536，
  结束 active/waiting request/work 全部归零。
- 2-job/4-job 有等待时，active-work ratio 均值分别为 0.9966/0.9960；
  没有出现“存在 waiting 且剩余 credit 足以容纳最大请求”的采样点。这支持
  equal-workload 下 work-conserving 行为，但不是 staggered idle borrowing
  的直接证据。

## 4. 实验数据

核心均值如下；每格为 3 次 formal repeat 的均值：

| jobs | policy | tokens/s | MFU | max JCT (s) | max P99 (s) | mean SLO violation | Jain median |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | independent | 3040.5 | 12.77% | 55.72 | 15.81 | 0.000 | 1.0000 |
| 1 | static partition | 3029.7 | 12.73% | 55.62 | 15.59 | 0.000 | 1.0000 |
| 1 | shared DRR | 3029.1 | 12.73% | 55.55 | 15.63 | 0.000 | 1.0000 |
| 2 | independent | 4972.1 | 20.89% | 68.52 | 28.27 | 0.000 | 0.9999 |
| 2 | static partition | 4918.2 | 20.66% | 68.80 | 28.49 | 0.000 | 0.9997 |
| 2 | shared DRR | 4970.1 | 20.88% | 68.32 | 28.26 | 0.000 | 0.9999 |
| 4 | independent | 5986.0 | 25.15% | 127.00 | 86.51 | 0.497 | 0.9996 |
| 4 | static partition | 6078.0 | 25.53% | 119.39 | 78.66 | 0.503 | 0.9993 |
| 4 | shared DRR | 6558.8 | 27.55% | 106.81 | 67.04 | 0.468 | 0.9961 |

相对变化：

- 1-job `shared_drr` vs `static_partition`：tokens/s -0.02%，满足 3% 开销门槛。
- 2-job `shared_drr` vs `independent_full`：tokens/s -0.04%，max P99 -0.04%，
  max JCT -0.28%，没有达到 5% 收益门槛。
- 4-job `shared_drr` vs `independent_full`：tokens/s +9.57%，max P99
  -22.52%，max JCT -15.89%。
- 4-job `shared_drr` vs `static_partition`：tokens/s +7.91%，max P99
  -14.78%，max JCT -10.53%。
- 4-job mean SLO violation 从 independent 的 49.71% 降至 46.76%，仅改善
  2.95 个百分点；SLO goodput/job 提升约 17.9%，但仍有接近一半请求超过 30s。
- 2-job shared 的 Jain median/min 为 0.999869/0.999806，最低
  normalized service / mean 为 0.9861；4-job 为
  0.996109/0.995419 与 0.9193，公平性门槛通过。

重复稳定性必须单独看：

- 2-job shared 相对 independent 的 tokens/s 分别为
  +0.24%、+1.32%、-1.65%，属于噪声范围。
- 4-job shared 相对 independent 的 tokens/s 分别为
  +8.43%、-0.28%、+22.60%；max P99 分别为
  -12.03%、+3.75%、-28.95%。

因此 4-job 聚合均值达到预注册门槛，但并非三次都胜出；independent 4-job
本身波动较高，不能把聚合结果写成无条件稳定加速。

## 5. 结果解释

### 事实

- shared credit 的全局 request/work 安全边界、结束归零与 per-job
  exactly-once 均通过。
- 单 job 协调开销可忽略；2-job 三臂几乎等价；4-job 高竞争下
  `shared_drr` 的聚合吞吐、JCT、P99 和 SLO goodput 更好。
- 4-job 下所有策略都已进入明显排队区，30s SLO 违约接近 50%。

### 推断

- 上游调度的收益不是让单条 vLLM 推理 kernel 更快，而是在 job 数足够高、
  独立 credit 发生过量认购时，约束总 active work 并持续回收/再分配 credit，
  减少无效排队和竞争放大。
- 2-job 无差异说明当前容量在中等竞争下已足够，shared DRR 没有必要制造收益；
  4-job 才出现可利用的 contention-control 空间。这比“请求越多越快”的表述
  更准确：先达到下游饱和，再避免超过饱和点后只增加排队。

### 待确认

- 4-job 结果需要新的 held-out 顺序或增加 repeats，确认收益不是
  independent arm 高波动造成。
- equal-workload 同步启动不能证明空闲份额借用；需要预注册的
  `staggered_2job`。
- equal-weight 不能证明权重隔离；需要共同 overlap window 指标后再运行
  `weighted_2job_3to1`。
- 当前没有专门回答“小 job 本身喂不饱 vLLM，能否通过上游重排把 15s
  降到 5s”的 transient-ramp 问题；该问题需要分解 ramp-up/steady/drain，
  并与无调度直接提交、最佳静态饱和控制做固定工作量对照。

### 不能声称

- 不能声称 shared DRR 改进了 vLLM 内部 continuous batching 或单请求推理速度。
- 不能声称 shared DRR 是 1/2/4-job 的通用默认最优策略。
- 不能用 4-job 聚合均值掩盖逐 repeat 的一次回退。
- 不能声称 staggered work conservation、3:1 weighted fairness 或异构 workload
  已经验证。

## 6. 对课题的含义

该结果首次给出 Ray 上游调度在多 job 高竞争下的正向条件性证据：同一套
vLLM 服务能力不变时，endpoint-shared active-work credit 可以阻止 job-local
上限膨胀，DRR 可以在安全容量内持续分配可用 credit。价值体现为高竞争下更高
有效吞吐和更低尾延迟，而不是更快的模型 kernel。

同时，2-job 负结果界定了机制边界：当下游尚未发生明显过量认购时，复杂调度
不会凭空加速。这一边界应保留在论文叙述中。

## 7. 下一步

1. 不重复本矩阵，不继续调 SLO-EWMA 的 25–50ms 参数。
2. 用全新目录做 4-job held-out repeats；只有复验后仍稳定跨过 5% 门槛，
   才考虑将 `shared_drr` 晋升为高竞争默认。
3. 在补齐 overlap-window service-rate 指标后，分别做
   `staggered_2job` 和 `weighted_2job_3to1`，隔离借用与公平机制。
4. 单独建立 transient saturation 实验：固定总工作量和下游容量，比较
   direct flood、最佳静态 active-work、快速 ramp controller，并报告
   time-to-saturation、饱和占比、drain tail、JCT 与 P99。它才直接回答
   “小任务 15s 能否通过上游更快喂饱 GPU 降到 5s”。
