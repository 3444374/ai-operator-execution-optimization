# 双 GPU request-level active-work 容量曲线

日期：2026-07-28

## 1. 实验问题

本实验在 request-level continuous replenishment 已可运行的基础上，将
admission 从请求计数改为每 endpoint 的预测 token work 上限，回答：

1. 双 Qwen2.5-7B endpoint 在当前 workload 下随 active work 增加是否仍能被填充；
2. 吞吐、MFU、尾延迟、SLO goodput 与能耗的拐点分别位于哪里；
3. 后续固定 work 的 batch barrier / request replenishment 因果对照应选什么范围。

本实验不是 submission policy 的胜负对照，也不用于证明 vLLM 内部容量上限。

## 2. 实验设置

- 链路：PostgreSQL 18.4 → Daft → Ray actor → 2 个独立 vLLM endpoint。
- 模型与硬件：Qwen2.5-7B，2× RTX 4090。
- workload：ShareGPT/BurstGPT，2048 行，arrival replay scale `0.001`。
- 数据组织：sequential token-budget，`token_budget=32768`，row cap 256。
- 提交：request 粒度、per-endpoint active-work credit、least-queued routing、
  `max_inflight=256`、fixed 50 ms flush。
- active-work 档位：16,384、24,576、32,768、49,152、65,536 predicted tokens /
  endpoint。
- 服务容量：vLLM 0.25.1，`max_num_batched_tokens=8192`，
  `max_num_seqs=256`，chunked prefill 开启，prefix cache 关闭。
- SLO：30 s；每档 1 次 warm-up + 3 次 formal repeat，交错运行。
- provenance：远端 detached worktree `44087ae`，仅带等价的启动脚本 PATH
  顺序调整；完整配置和随机顺序见 `manifest.json`。

## 3. 严谨性自检

- 20/20 runs 成功，15 个 formal run 均为 `ok`，0 incident、0 skipped。
- 每档三次重复的吞吐 CV 为 0.22%–0.94%，曲线信号稳定。
- 每档 `max_active_work_per_endpoint_seen` 均达到配置上限，证明 credit
  实际成为有效约束，而非未触发参数。
- 五档共享模型、组织、arrival、flush、routing 与 vLLM 容量，主要自变量是
  active-work cap。
- `vllm_waiting_mean=0` 不代表链路无排队；低 cap 的等待主要发生在上游
  admission，需结合 bounded wait 与逐请求 E2E 判断。
- 三次重复只支持当前模型、硬件和 workload 内的大幅稳定信号，不支持跨环境泛化。

## 4. 正式结果

| active work / endpoint | tps | MFU | E2E (s) | P95 (s) | P99 (s) | SLO violation | SLO goodput (req/s) | bounded wait (s) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 16,384 | 4888.16 | 21.03% | 139.11 | 66.79 | 68.66 | 52.12% | 7.05 | 93.22 |
| 24,576 | 6076.19 | 26.12% | 111.98 | 41.87 | 42.80 | 33.30% | 12.20 | 66.37 |
| 32,768 | 6836.85 | 29.55% | 99.51 | 30.23 | **33.24** | 9.21% | 18.69 | 52.09 |
| 49,152 | 7702.79 | 33.19% | 88.34 | 20.57 | 34.99 | **1.89%** | 22.75 | 38.99 |
| 65,536 | **8128.66** | **35.16%** | **83.78** | **16.96** | 36.63 | 2.15% | **23.92** | **27.76** |

相邻档位吞吐增益依次约为 `+24.3%`、`+12.5%`、`+12.7%` 和 `+5.5%`。
49,152→65,536 增加 33.3% credit，只换来 5.5% 吞吐，同时 P99 增加约 4.7%；
边际收益已经明显递减。

完整逐次结果见 `runs.csv`，绘图与复核用均值见 `formal_summary.csv`。33 MB
逐请求、submission、flush 和 resource traces 保留在远端原始结果目录，不纳入
Git；manifest 中的 trace 路径可用于回溯。

## 5. 结果解释

### 实验事实

1. 在 16K–65K 范围内，吞吐与 MFU仍随 active work 单调上升；65K 是
   `BEST_TESTED_THROUGHPUT_CAP`，不是已证明的容量最优点。
2. 增益明显递减，49K 是当前 `KNEE_CANDIDATE`：相比 65K 少约 5.2% 吞吐，
   但有更低 P99 和最低的 30 s SLO violation。
3. 低 active-work cap 并未保护端到端尾延迟，反而形成较长的上游 bounded
   wait；这解释了“更大 batch/并发看起来总是更好”的一部分现象。
4. 超过 32K 后 P95 继续改善而 P99 转差，说明容量提升开始把少数请求推入更重的
   服务竞争，平均吞吐与极端尾延迟的目标已经分叉。
5. 能耗从 137.92 降到 85.94 J / 1K observed tokens，当前范围内提高填充也改善
   单位 token 能效。

### 合理推断

- 本轮已经找到边际收益拐点区，而没有找到吞吐下降点。下一步不需要继续无上限
  放大 work；应在 49K 主点和 65K 敏感性点固定 offered work，隔离比较提交机制。
- active-work credit 的价值不是保证“小 cap 更快”，而是提供跨 batch/request
  粒度一致的工作量坐标，使机制对照不再暗中改变 offered load。

### 不能声称

- 不能把 65,536 写成最佳配置或 vLLM 饱和上限；它位于扫描边界。
- 不能从这条单策略曲线声称 continuous replenishment 优于 batch barrier。
- 不能把 `vllm_waiting=0` 写成无排队或无 HOL；等待可能位于 Ray/admission 层。
- 不能把 49,152 的拐点直接泛化到其他模型、输出长度、arrival rate 或 GPU。

## 6. 对课题的含义

这组数据把“力大砖飞”转化成了可控研究变量：此前 K 或 batch 变大时，同时改变了
提交单位和 offered work；现在 active-work cap 可以固定后者。优化问题因而从
“继续加并发”转为“在相同 work 下，哪种组织、补位、路由和 flush 能取得更好的
吞吐—P99—SLO goodput 前沿”。这是后续策略贡献能够进行因果归因的前提。

## 7. 下一步

1. 以 49,152 为主控制点、65,536 为高负载敏感性点，固定 active work、
   token budget、row cap、arrival 与服务容量，对比 whole-submission barrier 和
   request-level replenishment。
2. 同一固定 work 下复验 token-budget 与 membership，避免再把 offered-load
   增量误归因于数据组织。
3. 提交机制独立收益成立后，再做 least-work routing、SLO-aware EWMA flush 和
   submission-policy 消融；若收益不成立，则将 contribution 收敛为
   work-normalized admission 与边界诊断。
