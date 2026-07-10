# Feasibility Results

本目录用于记录前期可行性实验结果。

## 1. 推荐文件命名

| 实验 | CSV 文件 |
|---|---|
| Ray small task | `ray_small_task.csv` |
| Ray object transfer | `ray_object_transfer.csv` |
| Arrow serialization | `arrow_serialization.csv` |
| Shuffle simulation | `shuffle_simulation.csv` |
| Ray many objects | `ray_many_objects.csv` |
| Ray Arrow fan-out/fan-in | `ray_arrow_fanout_fanin.csv` |
| fake AI_EMBED pipeline | `fake_ai_embed_pipeline.csv` |
| AI operator scenario motivation | `ai_operator_scenario_motivation.csv` |
| AI operator granularity attribution | `ai_operator_granularity_attribution.csv` |
| AI operator backpressure | `ai_operator_backpressure.csv` |
| PostgreSQL 18 local profile | `postgres_ai_operator_profile.csv` |

PostgreSQL 同构链路的完整实验设置、流程、数据库表核对、严谨性边界和结论：

```text
validation/results/postgres18_local_environment_validation.md
```

## 2. 推荐运行命令

```bash
python validation/benchmarks/ray_small_task_benchmark.py \
  --tasks 100 1000 10000 \
  --repeats 3 \
  --output validation/results/ray_small_task.csv
```

```bash
python validation/benchmarks/ray_object_transfer_benchmark.py \
  --sizes-kb 1 10 100 1024 10240 \
  --types bytes numpy arrow \
  --repeats 3 \
  --output validation/results/ray_object_transfer.csv
```

```bash
python validation/benchmarks/arrow_recordbatch_serialization_benchmark.py \
  --rows 1000 10000 100000 \
  --cols 4 16 \
  --repeats 3 \
  --output validation/results/arrow_serialization.csv
```

```bash
python validation/benchmarks/shuffle_simulation_benchmark.py \
  --upstream 8 32 128 \
  --downstream 8 32 \
  --rows-per-partition 10000 \
  --strategies fine coalesced \
  --repeats 3 \
  --output validation/results/shuffle_simulation.csv
```

```bash
python validation/benchmarks/ray_many_objects_benchmark.py \
  --total-mb 16 \
  --objects 1 16 64 256 \
  --repeats 3 \
  --output validation/results/ray_many_objects.csv
```

```bash
python validation/benchmarks/ray_arrow_fanout_fanin_benchmark.py \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --embedding-dim 128 \
  --repeats 3 \
  --output validation/results/ray_arrow_fanout_fanin.csv
```

```bash
python motivation/fake_ai_embed_pipeline_benchmark.py \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --embedding-dim 128 \
  --repeats 3 \
  --output validation/results/fake_ai_embed_pipeline.csv
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
  --output validation/results/ai_operator_scenario_motivation.csv
```

```bash
python motivation/ai_operator_granularity_attribution_benchmark.py \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --payload-bytes-per-row 512 \
  --compute-us-per-row 0.25 \
  --repeats 3 \
  --output validation/results/ai_operator_granularity_attribution.csv
```

```bash
python motivation/ai_operator_backpressure_benchmark.py \
  --total-requests 512 \
  --producer-rate 2000 8000 \
  --replicas 2 4 \
  --queue-limit 0 8 32 \
  --repeats 3 \
  --output validation/results/ai_operator_backpressure.csv
```

## 3. 生成分析报告

```bash
python validation/benchmarks/analyze_results.py \
  --results-dir validation/results \
  --output validation/results/feasibility_report.md
```

报告会根据已有 CSV 自动判断：

- 是否有 Ray small task 开销证据；
- 是否有 Ray object transfer 开销证据；
- 是否有 Arrow serialization 成本证据；
- 是否有 shuffle object 粒度证据；
- 是否有 Ray many objects fan-in 放大证据；
- 是否有 Arrow RecordBatch fan-out/fan-in 放大证据；
- 是否有 fake `AI_EMBED(text)` 端到端链路证据；
- 当前路线是否有足够实验依据继续推进。
