---
type: paper-note
tags:
  - deep-reading
  - semantic-operators
  - cost-model
  - cidr2025
status: 精读完成
read_date: 2026-07-29
---

# 精读笔记：Palimpzest（CIDR 2025）

## 第一层：基本信息

| 字段 | 内容 |
|---|---|
| 论文 | Chunwei Liu et al. *Palimpzest: Optimizing AI-Powered Analytics with Declarative Query Processing* |
| 出处 | CIDR 2025 |
| 来源级别 | 非 CCF-A；核心补充系统论文 |
| 本地 PDF | `research/reference/palimpzest_cidr2025.pdf` |

**核心结论**：Palimpzest 将 AI 数据处理写成声明式程序，枚举模型选择、代码合成、prompt marshaling、context reduction 和 filter pushdown 等物理计划，并用采样得到的时间、成本、质量估计选择 Pareto 计划。

## 第二层：方法与实验

系统使用 sentinel plans 在约 5% workload 上采样，估计候选计划的 time/cost/quality，再按用户 policy 选择 Pareto 点。Real Estate Search 实验含 100 个 listings、每项 3 张图、23 个 positive；优化约 13.1s。生成计划相对 GPT-4 naïve baseline runtime 降低约 3.3×、成本降低约 2.9×、F1 最高提高约 1.1×。跨 policy 平均 runtime 降 67.5%、成本降 65.7%、F1 提升 6%。

Baseline 包括 naïve GPT-4、GPT-3.5 和 Mixtral。原型约 9,200 行 Python，以单线程 iterator 为主，目的是暴露“减少 work”的收益。

## 第三层：批判性评估

- 核心证据主要来自一个 workload，外部有效性有限。
- 依赖商业模型 API，成本和延迟随服务版本变化。
- 单线程设置不能代表 GPU 饱和 serving，也不适合直接评价 Ray/vLLM 调度。
- 采样估计可能漏掉尾部输入和动态到达行为。

## 第四层：与本项目的连接

Palimpzest 属于“代价驱动计划选择”算法来源和官方系统 baseline，不占 CCF-A Top 15。可迁移的思想是用小样本 profile 初始化 operator service time，再用正式运行 residual 校正；但本项目首版只做简单解析模型 + profile calibration + residual correction，不扩展为全局 learned optimizer。

若部署 Palimpzest baseline，必须把其单线程/商业 API 默认配置改造成与本项目同模型、同 endpoint、同工作量的受控版本，否则只能比较功能，不比较峰值吞吐。
