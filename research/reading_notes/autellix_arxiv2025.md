---
type: paper-note
tags:
  - deep-reading
  - agent-serving
  - program-scheduling
  - arxiv
status: 精读完成
read_date: 2026-07-29
---

# 精读笔记：Autellix（arXiv 2025）

## 第一层：基本信息

| 字段 | 内容 |
|---|---|
| 论文 | Michael Luo et al. *Autellix: An Efficient Serving Engine for LLM Agents as General Programs* |
| 出处 | arXiv:2502.13965 |
| 来源级别 | 预印本；核心补充文献 |
| 本地 PDF | `research/reference/autellix_arxiv2025.pdf` |

**核心结论**：Autellix 将一个 agent program 而非单次 LLM call 作为调度单位，用 attained service、依赖关系、preemption、session state 和 locality-aware routing 优化 program-level JCT。

## 第二层：方法与实验

PLAS 面向单线程程序，ATLAS 支持分布式/多线程 DAG；调度依据累计 attained service，而不是每个 call 的独立到达时间。workload 包括 ShareGPT（平均 6.66 calls）、BFCL（10.75）、LATS/MCTS（159.7）及混合负载。实验使用 A100 80GB 与 Llama 3.1 8B/70B、Falcon 180B，对比 vLLM、vLLM-opt 和 MLFQ。

论文报告在相同 program latency 下吞吐最高 4–15×；PLAS 相对 MLFQ 最高约 1.5×，ATLAS 在 LATS 中最高约 2.5×；多 engine routing 最高约 1.4×。这些收益包含 engine preemption、swap 优化、状态化 API 和 program-aware scheduling。

## 第三层：批判性评估

- agent DAG 与数据库一行一次 AI_COMPLETE 的执行模型不同。
- 关键收益依赖修改 serving API、preemption/swapping 和 stateful session，超出当前上游边界。
- 单 call workload 上未必有 program-level 优势，不能把最高倍数迁移到当前实验。
- 当前为预印本。

## 第四层：与本项目的连接

Autellix 支持“多 job 不是多请求的简单复制”：job 有依赖、剩余 work 和 program-level SLO。项目可借鉴 cumulative service、job-level JCT 和 HoL 诊断，但首轮 multi-job 仍应从独立 calls 的 VTC/shared-credit 开始，不把 agent runtime 作为直接 baseline。
