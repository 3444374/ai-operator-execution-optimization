# Motivation Results

本目录保存数据库 AI 算子动机测试的正式结果和结果分析。

## 文件

| 文件 | 内容 |
|---|---|
| `fake_ai_embed_pipeline.csv` | fake `AI_EMBED(text)` 端到端动机测试结果 |
| `ai_operator_scenario_motivation.csv` | embedding / classify-filter / offline LLM 三类 AI 算子场景对比结果 |
| `ai_operator_granularity_attribution.csv` | task/object/fan-in/operator invocation 收益来源拆分结果 |
| `ai_operator_backpressure.csv` | 模型服务 queue wait / token backlog / backpressure 模拟结果 |
| `motivation_test_results_analysis.md` | 动机测试结果分析 |

## 说明

这些结果用于研究方向筛选和动机补强，不等价于最终论文实验。正式结论仍需接真实数据库形态、真实 AI 算子或真实模型服务验证。
