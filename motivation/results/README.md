# 动机测试结果

本目录保存数据库 AI 算子动机测试的**唯一正式结果**和分析。`validation/results/` 不再存放动机测试 CSV 副本。

## 文件

| 文件 | 内容 |
|---|---|
| `fake_ai_embed_pipeline.csv` | fake `AI_EMBED(text)` 端到端动机测试结果 |
| `ai_operator_scenario_motivation.csv` | embedding / classify-filter / offline LLM 三类 AI 算子场景对比结果 |
| `ai_operator_granularity_attribution.csv` | task/object/fan-in/operator invocation 收益来源拆分结果 |
| `ai_operator_backpressure.csv` | 模型服务 queue wait / token backlog / backpressure 模拟结果 |
| `motivation_test_results_analysis.md` | 动机测试结果分析 |

## 运行命令

```bash
python motivation/fake_ai_embed_pipeline_benchmark.py \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --embedding-dim 128 \
  --repeats 3 \
  --output motivation/results/fake_ai_embed_pipeline.csv
```

```bash
python motivation/ai_operator_scenario_motivation_benchmark.py \
  --scenarios embed_rag classify_filter offline_llm \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --embedding-dim 128 \
  --text-tokens 32 \
  --repeats 3 \
  --output motivation/results/ai_operator_scenario_motivation.csv
```

```bash
python motivation/ai_operator_granularity_attribution_benchmark.py \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --payload-bytes-per-row 512 \
  --compute-us-per-row 0.25 \
  --repeats 3 \
  --output motivation/results/ai_operator_granularity_attribution.csv
```

```bash
python motivation/ai_operator_backpressure_benchmark.py \
  --total-requests 512 \
  --producer-rate 2000 8000 \
  --replicas 2 4 \
  --queue-limit 0 8 32 \
  --repeats 3 \
  --output motivation/results/ai_operator_backpressure.csv
```

## 说明

这些结果用于研究方向筛选和动机补强，不等价于最终论文实验。正式结论仍需接真实数据库形态、真实 AI 算子或真实模型服务验证。
