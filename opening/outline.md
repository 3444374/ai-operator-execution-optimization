# 开题材料统一骨架

## 统一题目口径

暂定题目：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究

备选表述：

> 面向数据库驱动 AI workload 的 Daft-Ray-Lance 数据执行系统优化研究

当前不要把题目写成“外部链路优化”或“Ray 调度优化研究”。“外部链路”适合解释系统边界，但题目应落到更学术的问题上：数据库驱动 AI workload、分布式数据执行、GPU 服务感知调度、反压和存储协同。Daft/Ray/Lance 要作为系统机制和验证平台出现，不要写成简单产品集成。

## 汇报主线

```text
数据库成为 AI workload 的入口
  -> AI workload 引入数据组织、模型服务、队列、调度、写回等新环节
  -> Daft batch、Ray 调度、GPU 服务调用和 Lance/数据库写回可能成为端到端瓶颈
  -> 本项目构建 Database source + Daft/Arrow + Ray + GPU endpoint + Lance/数据库 sink 的可控执行路径
  -> 通过阶段画像定位瓶颈
  -> 研究数据组织、task/actor、endpoint routing、backpressure、writeback 的协同优化
```

## PPT 建议页序

| 页 | 标题 | 主结论 |
|---|---|---|
| 1 | 题目页 | 说明题目、学生、导师、方向 |
| 2 | 研究背景 | 数据库正在出现 AI 算子，执行过程发生变化 |
| 3 | 问题定义 | 研究对象是数据库驱动 AI workload 的分布式数据执行链路 |
| 4 | 现有系统与相关工作 | Snowflake、pgai、PostgresML、Ray/Daft/Lance 分别提供背景和机制参考 |
| 5 | 研究缺口 | 现有系统难以在可控环境中拆分模型服务、调度、fan-in、writeback 成本 |
| 6 | 技术路线 | Database source -> Daft/Arrow batch -> Ray -> model service -> fan-in -> Lance/数据库 sink |
| 7 | 初步实验设置 | 本地 PG18.4 + GPU embedding endpoint + Ray task/actor |
| 8 | 初步结果 1 | batch 显著影响端到端时间 |
| 9 | 初步结果 2 | 单 endpoint 下 Ray 未必明显优于 Python，多 endpoint 下才开始体现调度价值 |
| 10 | 初步结果 3 | writeback 已成为大块成本，后续必须纳入优化 |
| 11 | 研究内容 | 数据组织与批处理构造、Ray 服务感知调度与反压、结果汇聚与持久化协同 |
| 12 | 预期创新点 | 与研究内容和评价指标对应，克制表述 |
| 13 | 实验计划 | embedding、AI_FILTER/CLASSIFY、AI_COMPLETE 三类场景逐步推进 |
| 14 | 风险与控制 | GPU、数据规模、系统复杂度、文献与 baseline 风险 |
| 15 | 进度安排 | 开题后阶段计划 |
| 16 | 总结 | 题目成立、路线可执行、下一步明确 |

## 报告建议结构

1. 研究背景与意义
2. 国内外研究现状
3. 研究目标与研究内容
4. 拟解决的关键问题
5. 技术路线与研究方法
6. 初步实验与可行性分析
7. 预期创新点
8. 进度安排
9. 预期成果
10. 参考文献

## 飞书进度汇报建议结构

```text
本周完成
  -> 当前判断
  -> 遇到的问题
  -> 下周计划
  -> 需要确认的问题
```
