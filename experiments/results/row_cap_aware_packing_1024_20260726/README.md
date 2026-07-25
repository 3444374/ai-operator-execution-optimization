# 1024 行 row-cap-aware packing held-out 验证

日期：2026-07-26

## 1. 实验设置

沿用 512 行胜出配置，不重新调参：

- PostgreSQL 18.4 + pgvector 0.8.2 → Daft → Ray task → vLLM 0.25.1；
- 单张 RTX 5070 12 GB；
- `sharegpt_burstgpt` 前 1024 行，`doc_id` 顺序；
- row cap 64、token budget 6144、`K_max=8`、输出上限 16；
- vLLM prefix cache 关闭，MFU metrics 开启；
- sequential、classic BFD、row-cap-aware 各 1 次预热 + 3 次正式重复。

## 2. 实验设计

本实验只回答 512 行的小幅正向信号能否在未参与调参的 1024 行规模保持。
配置、模型、请求、资源采样和 SLO 均不改变。

## 3. 严谨性自检

- 12/12 运行成功，0 incident；9/9 正式运行进入汇总。
- 每个正式 run 均有 1024 个 completed request 和唯一 request/doc ID。
- request → submission 外键完整，资源 trace 非空。
- vLLM FLOP delta 为正，全部 `mfu_status=ok`。
- 最终 vLLM running/waiting 均回到 0。

## 4. 实验数据

均值 ± 样本标准差：

| 指标 | Sequential | Classic BFD | Row-cap-aware |
|---|---:|---:|---:|
| E2E (s) | 13.4903 ± 0.0611 | 13.3497 ± 0.0607 | 13.3800 ± 0.0196 |
| Tokens/s | 10381.5 ± 47.1 | 10490.5 ± 48.0 | 10466.3 ± 14.9 |
| Request P50 (s) | 10.0312 ± 0.0558 | 11.5786 ± 0.0470 | 11.6185 ± 0.0201 |
| Request P95 (s) | 12.7395 ± 0.0105 | 12.6649 ± 0.0466 | 12.7165 ± 0.0202 |
| Request P99 (s) | 12.7817 ± 0.0110 | 12.6649 ± 0.0466 | 12.7165 ± 0.0202 |
| SLO violation ratio | 0.5039 ± 0.0118 | 0.8887 ± 0 | 0.8867 ± 0 |
| SLO goodput (req/s) | 37.656 ± 0.804 | 8.540 ± 0.039 | 8.670 ± 0.013 |
| Submissions | 25 | 30 | 30 |
| Packing utilization | 0.9119 | 0.7599 | 0.7599 |
| Energy / 1k tokens (J) | 11.2997 ± 0.1934 | 11.3337 ± 0.1205 | 11.3103 ± 0.0051 |
| Mean GPU util. (%) | 71.72 ± 2.01 | 69.20 ± 0.86 | 69.98 ± 1.44 |
| MFU | 0.5159 ± 0.0008 | 0.5240 ± 0.0025 | 0.5217 ± 0.0014 |

相对 sequential，row-cap-aware 的 tokens/s `+0.82%`、P95 `-0.18%`、
MFU `+1.12%`、energy/1k tokens `+0.09%`；但 SLO violation 从
`50.39%` 上升到 `88.67%`，SLO goodput 从 `37.66` 降到 `8.67 req/s`。

原始入口：

- `runs.csv`
- `summary_long.csv`
- `manifest.json`
- 同目录下 request/submission/resource traces

## 5. 结果解释

**事实**：decreasing-order 两种方案以约 1% 吞吐增益换来了显著更差的
10 秒 SLO goodput；row-cap-first 没有修复这一问题。

**推断**：把长请求集中到相同 submission 改变了完成时间分布。P95 看似略好，
但 P50 从约 10.03 秒增至 11.62 秒，使大量请求越过固定 SLO 门槛。

**不能声称**：不能用平均吞吐或 P95 的小幅改善宣称策略整体更优；SLO 分布
已经否定其默认晋级资格。

## 6. 对课题的含义

- Sequential token-budget 保持默认。
- Classic BFD 不进入默认候选。
- Row-cap-first 实现保留为研究消融和可配置扩展点，但当前不启用。
- 后续数据组织策略必须把 SLO goodput/P50 纳入目标，不能只优化 packing
  utilization、tokens/s 或 MFU。

## 7. 下一步

停止 row-cap-aware 的更大规模扩展。研究内容一转向：

1. sequential token-budget + 受控 prefix/length workload；
2. 把 SLO goodput 作为联合搜索约束；
3. 与提交控制独立最优拼接和联合搜索比较。
