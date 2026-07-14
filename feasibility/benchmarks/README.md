# 可行性验证 Benchmarks

本目录存放当前活跃的可行性验证代码。

早期排除性实验（Ray small task、object transfer、Arrow serialization、shuffle simulation）已归档至 [`archive/`](../archive/README.md)，这些实验证明了对应方向不是当前瓶颈。

## 1. 环境

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r validation/benchmarks/requirements.txt
```

## 2. 活跃脚本

| 脚本 | 目的 |
|---|---|
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

动机脚本运行命令见 [`motivation/results/README.md`](../../motivation/results/README.md)。

生成分析报告：

```bash
python validation/benchmarks/analyze_results.py \
  --results-dir validation/results \
  --motivation-results-dir motivation/results \
  --output validation/results/feasibility_report.md
```

## 4. 读结果时看什么

| 观察 | 说明 |
|---|---|
| 固定总量下 object 数量越多 fan-in 越慢 | object coalescing / fan-in 优化有价值 |
| Arrow RecordBatch fine 策略比 coalesced 策略慢 | 数据库 AI 算子批处理链路中的 RecordBatch object 合并有价值 |
| fake AI_EMBED 端到端 fine 策略比 coalesced 策略慢 | RecordBatch fan-in 现象迁移到批量 embedding / RAG 链路 |
| two-stage coalescing 只降 fan-in refs 但端到端不快 | 只做下游合并不够，需要在任务划分阶段减少过细 operator invocation |
| 无界提交不提升 tokens/s 但放大 token backlog | 模型服务状态感知 backpressure 有继续验证价值 |
