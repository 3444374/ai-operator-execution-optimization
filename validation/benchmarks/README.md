# Feasibility Benchmarks

本目录存放前期可行性测试代码。目标不是完整性能评测，而是快速判断：

> Ray task、object transfer、Arrow/RecordBatch serialization、shuffle object 粒度是否存在可观察开销。

## 1. 建议环境

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r validation/benchmarks/requirements.txt
```

当前脚本会在运行时导入依赖；如果没有安装 `ray` 或 `pyarrow`，只有对应脚本会报错。

## 2. 脚本列表

| 脚本 | 目的 |
|---|---|
| `ray_small_task_benchmark.py` | 测 Ray 大量小 task 的调度/执行固定开销 |
| `ray_object_transfer_benchmark.py` | 测 bytes、numpy、Arrow 对象在 Ray object store / task 之间传递的成本 |
| `arrow_recordbatch_serialization_benchmark.py` | 测 Arrow RecordBatch 的 IPC 序列化/反序列化成本 |
| `shuffle_simulation_benchmark.py` | 模拟 hash shuffle，比较小 object、多 partition、合并策略的影响 |
| `ray_many_objects_benchmark.py` | 固定总数据量，测试 Ray fan-in 时 object 数量放大的影响 |
| `ray_arrow_fanout_fanin_benchmark.py` | 固定总行数，测试 Arrow RecordBatch 在 Ray `N upstream -> P downstream` fan-in 下的 object 数量影响 |

端到端动机脚本放在 `motivation/`：

| 脚本 | 目的 |
|---|---|
| `motivation/fake_ai_embed_pipeline_benchmark.py` | 模拟数据库 `AI_EMBED(text)` 链路，对比 fine/coalesced object 在 build、Ray put、fake embedding、fan-in、write 和端到端耗时中的影响 |
| `motivation/ai_operator_scenario_motivation_benchmark.py` | 比较 embedding、classify/filter、offline LLM 三类 AI 算子场景的粒度敏感性 |
| `motivation/ai_operator_granularity_attribution_benchmark.py` | 拆分 task 数、Ray refs、fan-in refs、operator invocation 对端到端的影响 |
| `motivation/ai_operator_backpressure_benchmark.py` | 模拟模型服务消费慢时的 queue wait、token backlog、in-flight 请求和 backpressure |

## 3. 运行示例

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

生成可行性报告：

```bash
python validation/benchmarks/analyze_results.py \
  --results-dir validation/results \
  --output validation/results/feasibility_report.md
```

## 4. 读结果时看什么

| 观察 | 说明 |
|---|---|
| empty task 平均延迟随 task 数量线性放大 | task batching / lightweight runtime 有价值 |
| 小 object round-trip 明显慢 | object batching 有价值 |
| Arrow object 比 numpy/bytes 明显慢 | RecordBatch 传输路径需要进一步分析 |
| fine shuffle 比 coalesced shuffle 慢很多 | shuffle object 数量和 partition 粒度是关键问题 |
| 固定总量下 object 数量越多 fan-in 越慢 | object coalescing / fan-in 优化有价值 |
| Arrow RecordBatch fine 策略比 coalesced 策略慢 | 数据库 AI 算子批处理链路中的 RecordBatch object 合并有价值 |
| fake AI_EMBED 端到端 fine 策略比 coalesced 策略慢 | RecordBatch fan-in 现象可以迁移到批量 embedding / RAG 数据准备链路 |
| two-stage coalescing 只降 fan-in refs 但端到端不快 | 只做下游合并不够，需要在任务划分阶段减少过细 operator invocation |
| 无界提交不提升 tokens/s 但放大 token backlog | 模型服务状态感知 backpressure 有继续验证价值 |
