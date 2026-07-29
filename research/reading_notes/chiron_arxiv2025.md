---
type: paper-note
tags:
  - deep-reading
  - llm-serving
  - autoscaling
  - backpressure
  - arxiv
status: 精读完成
read_date: 2026-07-29
---

# 精读笔记：Chiron（arXiv 2025）

## 第一层：基本信息

| 字段 | 内容 |
|---|---|
| 论文 | Archit Patke et al. *Hierarchical Autoscaling for Large Language Model Serving with Chiron* |
| 出处 | arXiv:2501.08090 |
| 来源级别 | 预印本；核心补充文献 |
| 本地 PDF | `research/reference/chiron_arxiv2025.pdf` |

**核心结论**：Chiron 用本地 ITL/backpressure 控制 batch size，再用全局 queue/SLO 控制实例数，将毫秒级执行反馈与分钟级资源扩缩容分层。

## 第二层：方法与实验

本地控制器根据 inter-token latency 的 EWMA 调整 batch；全局控制器按队列、SLO 和估计等待时间扩缩实例，并区分 interactive/batch pool。实验使用 Llama 8B/70B、A100 80GB 和 ShareGPT workload，对比 Llumnix tuned/untuned。

论文报告 SLO attainment 最高约 90%、吞吐最高约 300%、资源最高减少约 70%；local/global ablation 各可带来约 30–60% throughput 改善。等待时间模型在约 2,000 请求时 `R²≈0.99`，小队列下更不稳定。

## 第三层：批判性评估

- 最大收益包含增加/减少 GPU 实例，不属于固定双 GPU实验的同资源优化。
- 队列足够大时估计更准，小 job 的 warm-up/ramp 可能恰是薄弱点。
- 论文的 batch size 可到 2048–4096，与当前上游 HTTP request/row 语义不同。
- 当前为预印本，不能按 CCF-A 题录使用。

## 第四层：与本项目的连接

Chiron 的可迁移部分是“分层控制”和低复杂度 backpressure：先确定固定资源下的最小饱和 active work，再在多 job 下调 shared credit；慢速 cost-model 校准与快速 queue feedback 分开。不可迁移部分是 autoscaling 本身，因此它是设计参考而非直接 baseline。
