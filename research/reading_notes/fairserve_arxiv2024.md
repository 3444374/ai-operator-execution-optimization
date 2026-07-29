---
type: paper-note
tags:
  - deep-reading
  - llm-serving
  - fairness
  - throttling
  - arxiv
status: 精读完成
read_date: 2026-07-29
---

# 精读笔记：FairServe（arXiv 2024）

## 第一层：基本信息

| 字段 | 内容 |
|---|---|
| 论文 | Redwan Ibne Seraj Khan et al. *Ensuring Fair LLM Serving Amid Diverse Applications* |
| 出处 | arXiv, 2024 |
| 来源级别 | 预印本；核心补充文献 |
| 本地 PDF | `research/reference/fairserve_arxiv2024.pdf` |

**核心结论**：FairServe 针对多应用、多阶段交互提出 overload-and-interaction throttling 与 weighted service counter，用历史 token 分布归一化不同应用阶段的服务量，避免 RPM 对长交互和 token 成本的不公平。

## 第二层：方法与实验

系统区分 application、interaction 和 call，OIT 在过载时优先保留有价值的交互，WSC 按应用阶段预期 input/system/output length 归一化服务量。论文固定使用 α=1、β=2、γ=1 的经验权重。

相对 RPM，FairServe 报告被 throttle 的 interaction 减少 21.15×、特定场景 token waste 为 0、仅 0.93% 用户被延迟；相对 VTC/RPM，延迟用户分别低 10.67×/93×，吞吐高 1.03–1.75×。Table 3 中 RPM/VTC/FairServe(W+I) 的 prompt TPS 约 11155/12190/12248，decode TPS 约 183/263/268。

## 第三层：批判性评估

- 依赖应用/阶段历史分布，workload shift 会使归一化失真。
- α/β/γ 是启发式，不是可迁移到数据库 job 的普适权重。
- 重点是限流、滥用防护和交互完整性，不是固定规模 batch job 的最短 JCT。
- 当前仅预印本，不按 CCF-A 正式论文引用。

## 第四层：与本项目的连接

FairServe 支持将“请求”提升为“job/interaction”调度单位，并为 heterogeneous job 设计 normalized service。可用于 weighted shared-credit 和 staggered multi-job 实验，但正式算法基线优先 VTC。代价估计应按 operator/model/workload 校准权重，而不是照搬 α/β/γ。
