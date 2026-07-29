---
type: paper-note
tags:
  - deep-reading
  - llm-serving
  - fairness
  - prefix-locality
  - arxiv
status: 精读完成
read_date: 2026-07-29
---

# 精读笔记：DLPM / D2LPM（arXiv 2025）

## 第一层：基本信息

| 字段 | 内容 |
|---|---|
| 论文 | Shiyi Cao et al. *Locality-aware Fair Scheduling in LLM Serving* |
| 出处 | arXiv:2501.14312 |
| 来源级别 | 预印本；核心补充文献 |
| 本地 PDF | `research/reference/dlpm_arxiv2025.pdf` |

**核心结论**：DLPM 用 deficit quantum 将 token-cost fairness 与 longest-prefix matching 结合，D2LPM 再增加 worker quantum，在分布式 serving 中平衡 client fairness、prefix locality 和负载。

## 第二层：方法与实验

服务量只计不共享的 prefix extension 和 output token，论文使用输入/输出权重 1/2。DLPM 对每个 client 累积 deficit，在额度内选择 prefix locality 最好的请求；D2LPM 通过 worker quantum 控制请求在 worker 间的偏好。论文给出两个 backlogged clients 的服务差上界 `2 × (U + Q_u)`。

实验在单 A10 和最多 8×A100 上使用 3B/8B 模型，workload 包括 Tree-of-Thoughts、LLM-as-Judge、Long-Context QA 和多轮对话；对比 VTC、LPM、RR+LPM、Preble。论文报告相对 VTC 最高约 2.87× throughput，相对 Preble victim latency 最高降低约 7.18×。

## 第三层：批判性评估

- 收益依赖 prefix cache 开启并存在 client 内共享前缀；本项目当前 cache-off 结果不能验证该机制。
- quantum 越大，locality/throughput 越好但公平界越弱；不能只选最高吞吐点。
- 方法忽略跨 client prefix 和程序依赖，D2LPM 也可能比 VTC 更不公平。
- 当前为预印本，不能替代 VTC 的正式 Top 15 地位。

## 第四层：与本项目的连接

DLPM 是未来 prefix-cache-on + multi-job 的算法来源：在 shared-credit fair queue 中加入可控 service quantum，并把 locality gain 与 fairness loss 一起记录。若 prefix cache 关闭，它只剩 deficit scheduling 部分，不能声称 locality-aware 增益。
