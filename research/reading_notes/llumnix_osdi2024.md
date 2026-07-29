---
type: paper-note
tags:
  - deep-reading
  - llm-serving
  - migration
  - scheduling
  - osdi2024
status: 精读完成
read_date: 2026-07-29
---

# 精读笔记：Llumnix（OSDI 2024）

## 第一层：基本信息

| 字段 | 内容 |
|---|---|
| 论文 | Biao Sun et al. *Llumnix: Dynamic Scheduling for Large Language Model Serving*. OSDI 2024 |
| 来源级别 | CCF-A 正式 research paper |
| 本地 PDF | `research/reference/llumnix_osdi2024.pdf` |
| 主题 | 多实例动态调度、KV cache 迁移、优先级、autoscaling |

**核心结论**：Llumnix 通过虚拟 usage 统一表达实例负载，并用低停顿 live migration 在运行时纠正初始放置错误，从而改善尾延迟、优先级隔离、碎片和资源成本。

## 第二层：方法与实验

LLM 请求的输出长度和 KV 增长不可预知，初始 routing 很容易在运行中失衡。Llumnix 将请求迁移视为常规调度动作：检测实例间虚拟 usage 差异，在不中断整体服务的情况下复制 KV 状态并切换执行位置。虚拟 usage 同时支持负载均衡、反碎片、优先级和扩缩容。

论文以 vLLM 为底层，在最高 16 GPU 的真实 workload 中比较 Round-Robin、INFaaS++ 等方法。结果包括：P99 TTFT 最高改善约 15×，P99 per-token latency 最高约 2×，高优先级请求约 1.5×，资源成本最高下降约 36%；平均碎片从 INFaaS++ 的 7.9% 降到 0.7%，preemption loss 平均下降 70.4%。64 实例扩展实验的一部分使用模拟/休眠而非全真实硬件。

## 第三层：批判性评估

- 主要创新依赖跨实例 KV cache live migration，超出本项目“不修改 vLLM 内部”的边界。
- 其优势在多实例负载漂移、碎片和优先级场景；单 endpoint、单 job 饱和实验不能体现主要机制。
- 论文观察到 batch size/composition 可造成约 2.6× decode speed 变化，但这不意味着上游任意重排都能获得同等收益。
- 大规模部分实验不是全部在真实 GPU 上运行，不能把扩展结果直接迁移到双 4090。

## 第四层：与本项目的连接

Llumnix 是多 endpoint 路由和在线容量估计的算法来源，而不是可直接部署的 baseline。可迁移的部分包括：

1. 用虚拟 usage/estimated remaining work 表示 endpoint 负载；
2. 将路由错误视为可在线纠正的问题；
3. 区分可用容量、已承诺 work 和未来增长；
4. 对高优先级 job 单独报告 tail latency。

本项目首版不迁移 KV，只能在请求尚未提交 vLLM 前重路由。因此需要更准确的 prompt/output work 与 completion-time 估计，并在 trace 中记录 predicted work、actual work、route decision 和 residual。Llumnix 支持“多 endpoint 下动态状态很重要”，但不能证明当前 Ray actor pool 形状本身会提高吞吐。
