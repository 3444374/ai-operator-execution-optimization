---
type: paper-note
tags:
  - deep-reading
  - llm-serving
  - fairness
  - scheduling
  - osdi2024
status: 精读完成
read_date: 2026-07-29
---

# 精读笔记：Fairness in Serving Large Language Models（VTC，OSDI 2024）

## 第一层：基本信息

| 字段 | 内容 |
|---|---|
| 论文 | Ying Sheng et al. *Fairness in Serving Large Language Models*. OSDI 2024 |
| 来源级别 | CCF-A 正式 research paper |
| 本地 PDF | `research/reference/vtc_osdi2024.pdf` |
| 主题 | 多租户 LLM 服务、公平调度、work-conserving |

**核心结论**：VTC 用 virtual token counter 按客户端累计服务量排序，在不知道输出长度的情况下在线计费，并给出 backlogged clients 之间服务差的紧界；它优化的是跨客户端公平性，不是让 GPU 单请求计算更快。

## 第二层：方法与实验

### 问题和架构

FCFS、RPM/TPM 限流和请求计数公平会忽略 prompt/output token 差异。VTC 将服务量定义为加权输入、输出 token 数；新到或重新活跃的客户端会把 counter 抬升到当前活跃集合的最低水平，避免利用空闲历史取得不公平优势。调度器每次选择 counter 最小的客户端，并在请求完成过程中更新实际输出成本。

该算法是 work-conserving 的：只要存在可执行请求就不故意空置资源。论文证明任意两个持续 backlogged 客户端的服务差不超过与最大请求代价相关的紧界；主算法不依赖输出长度 oracle。

### 实验

| 维度 | 内容 |
|---|---|
| Baseline | FCFS、LCF、RPM、VTC、带预测和 oracle 的 VTC |
| Workload | 合成 workload；LMSYS Chatbot Arena trace |
| 真实设置 | 27 clients、10 分钟、约 210 req/min、Llama-2-7B、A10G 24GB |
| 指标 | throughput、最大/平均服务差、完成时间、公平性 |

真实 trace 中 FCFS/VTC/VTC-predict/oracle 的吞吐约为 777/779/773/781 token/s，说明公平控制可在基本不损失吞吐的情况下实施；RPM(5) 仅约 340 token/s，RPM(30) 约 747 token/s。论文的主要增益是服务差与完成时间公平，而非平台峰值吞吐。

## 第三层：批判性评估

- 公平性单位取决于输入/输出权重，权重不等于数据库 job 的业务优先级或 SLO。
- 理论界随最大请求代价和内存池容量增大；超长请求会放宽公平界。
- 主方案不做 preemption，公平控制位于模型 serving scheduler 内部；本项目不修改 vLLM，因此只能迁移 counter/credit 思想，不能直接复现其细粒度保证。
- 单请求 token 计费没有表示 DAG、交互式 agent 或跨算子依赖。

## 第四层：与本项目的连接

| 角色 | 判断 |
|---|---|
| 算法来源 | 是：endpoint-shared work counter、最小已服务量优先、idle borrowing |
| 实验 baseline | 是：多 job 公平调度的算法 baseline；不是数据库系统 baseline |
| 边界工作 | 是：证明公平不等于 GPU 加速 |

对当前设计的直接启发是：shared credit 不能只限制全局在途量，还要把每个 job 的累计 token work 记入调度状态，并允许空闲 job 的配额被借用。评价必须同时报告聚合吞吐、Jain fairness、每 job JCT/P99 和最坏 slowdown；不能只报告“公平策略吞吐未下降”。

代价估计可为 counter 提供预测 prompt/output work，但完成后必须按真实 usage 校正。VTC 不支持“一个喂不饱 vLLM 的小 job 从 15 秒变 5 秒”的主张；若 workload 本身并行度不足，公平调度无法创造更多可执行 token。
