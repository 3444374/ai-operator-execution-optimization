# PG18.4 baseline 矩阵实验：executor、batch size 与 actor 数

生成日期：2026-07-11

## 1. 实验定位

本实验延续 `pg18_4_system_profile_fake_ai_embed.md`，用于补齐下一轮 baseline。

它回答的问题是：

> 在 PostgreSQL 18.4 -> Arrow RecordBatch -> fake AI_EMBED -> PostgreSQL 写回链路中，收益主要来自 Python/Ray 执行器差异、batch 粒度、Ray task/actor 形态，还是 actor 并行度配置？

它不回答：

- 公司 PostgreSQL 18.3 内部平台是否表现相同。
- 真实 CPU/GPU embedding 模型是否有同样收益。
- pgvector `vector(128)` 写回成本是否等于当前 JSON 文本写回成本。
- Ray Serve / vLLM 模型服务队列是否表现相同。

原始 CSV：

```text
motivation/results/pg18_4_baseline_matrix.csv
motivation/results/pg18_4_actor_batch_workers.csv
```

## 2. 代码改动

入口脚本：

```text
code/scripts/postgres_ai_operator_profile.py
```

本轮新增：

- `--executor ray_task`：增加 Ray task baseline，用于回答 actor 是否必要。
- `submit_ray_tasks(...)`：复用同一 fetch、batch、writeback 路径，只替换 Ray 提交形态。

已有 executor：

| executor | 含义 |
|---|---|
| `python` | 本地 Python 顺序执行 fake embedding |
| `ray_task` | 每个 batch 一个 Ray task |
| `ray_actor` | 每个 batch 调用 Ray actor，actor 数由 `--model-workers` 控制 |

## 3. 实验 A：executor × strategy baseline

### 3.1 设置

固定参数：

| 参数 | 值 |
|---|---:|
| total_rows | 4096 |
| db_fetch_rows | 512 |
| ray_batch_rows | 256 |
| embedding_dim | 128 |
| model_workers | 2 |
| max_inflight | 8 |
| warm-up | 1 |
| formal repeats | 2 |

对照矩阵：

| 维度 | 取值 |
|---|---|
| executor | `python`, `ray_task`, `ray_actor` |
| strategy | `fine`, `coalesced` |

策略含义：

- `fine`：每行一个 operator invocation / object。
- `coalesced`：每 256 行一个 operator invocation / object。

### 3.2 运行命令

```powershell
$py = '.conda\pg-ai-profile\python.exe'
$out = 'motivation\results\pg18_4_baseline_matrix.csv'
foreach ($executor in @('python','ray_task','ray_actor')) {
  foreach ($strategy in @('fine','coalesced')) {
    & $py code\scripts\postgres_ai_operator_profile.py `
      --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
      --setup --seed-rows 4096 --total-rows 4096 `
      --db-fetch-rows 512 --ray-batch-rows 256 `
      --embedding-dim 128 --model-workers 2 --max-inflight 8 `
      --executor $executor --strategy $strategy `
      --warmup-runs 1 --repeats 2 `
      --experiment-id pg18_4_baseline_4096 `
      --output $out
  }
}
```

### 3.3 Formal 结果

以下只统计 `phase = formal` 的 2 次重复均值。

| executor | strategy | objects | invocations | e2e_s | rows/s | bounded_wait_s | fanin_s | writeback_s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| python | fine | 4096 | 4096 | 64.228 | 63.773 | 0.000 | 0.000 | 0.468 |
| python | coalesced | 16 | 16 | 3.729 | 1098.429 | 0.000 | 0.000 | 0.464 |
| ray_task | fine | 4096 | 4096 | 8.797 | 466.259 | 4.507 | 0.513 | 0.451 |
| ray_task | coalesced | 16 | 16 | 2.200 | 1861.552 | 0.000 | 0.006 | 0.458 |
| ray_actor | fine | 4096 | 4096 | 33.567 | 122.058 | 28.224 | 0.661 | 0.490 |
| ray_actor | coalesced | 16 | 16 | 2.353 | 1740.677 | 0.000 | 0.006 | 0.481 |

关键比值：

| executor | fine / coalesced e2e |
|---|---:|
| python | 17.224x |
| ray_task | 3.999x |
| ray_actor | 14.265x |

## 4. 实验 B：Ray actor batch size × worker 数

### 4.1 设置

固定参数：

| 参数 | 值 |
|---|---:|
| total_rows | 4096 |
| db_fetch_rows | 4096 |
| executor | `ray_actor` |
| strategy | `coalesced` |
| embedding_dim | 128 |
| max_inflight | 8 |
| warm-up | 1 |
| formal repeats | 2 |

对照矩阵：

| 维度 | 取值 |
|---|---|
| ray_batch_rows | 64, 256, 1024 |
| model_workers | 1, 2, 4 |

说明：本实验将 `db_fetch_rows` 设为 4096，是为了让同一轮中有足够 batch 可同时提交，避免大 batch 下并行度被 fetch 分段限制。

### 4.2 运行命令

```powershell
$py = '.conda\pg-ai-profile\python.exe'
$out = 'motivation\results\pg18_4_actor_batch_workers.csv'
foreach ($batchRows in @(64,256,1024)) {
  foreach ($workers in @(1,2,4)) {
    & $py code\scripts\postgres_ai_operator_profile.py `
      --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
      --setup --seed-rows 4096 --total-rows 4096 `
      --db-fetch-rows 4096 --ray-batch-rows $batchRows `
      --embedding-dim 128 --model-workers $workers --max-inflight 8 `
      --executor ray_actor --strategy coalesced `
      --warmup-runs 1 --repeats 2 `
      --experiment-id pg18_4_actor_batch_workers_4096 `
      --output $out
  }
}
```

注：首轮 `batch=64, workers=1` 曾出现一次 Ray 本地节点启动超时，未写入 CSV；随后单独补跑成功。

### 4.3 Formal 结果

以下只统计 `phase = formal` 的 2 次重复均值。

| batch rows | workers | objects | e2e_s | rows/s | bounded_wait_s | avg_bounded_wait_s | fanin_s | writeback_s |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 1 | 64 | 4.554 | 899.457 | 3.501 | 0.063 | 0.017 | 0.461 |
| 64 | 2 | 64 | 2.595 | 1578.517 | 1.782 | 0.032 | 0.014 | 0.474 |
| 64 | 4 | 64 | 1.648 | 2486.075 | 0.949 | 0.017 | 0.012 | 0.492 |
| 256 | 1 | 16 | 3.871 | 1058.026 | 1.711 | 0.214 | 0.007 | 0.503 |
| 256 | 2 | 16 | 2.268 | 1806.127 | 0.933 | 0.117 | 0.006 | 0.478 |
| 256 | 4 | 16 | 1.500 | 2729.842 | 0.554 | 0.069 | 0.006 | 0.499 |
| 1024 | 1 | 4 | 3.763 | 1088.431 | 0.000 | 0.000 | 0.002 | 0.458 |
| 1024 | 2 | 4 | 2.193 | 1867.858 | 0.000 | 0.000 | 0.002 | 0.456 |
| 1024 | 4 | 4 | 1.433 | 2860.244 | 0.000 | 0.000 | 0.001 | 0.459 |

## 5. 结果解释

本地实验事实：

- 在固定 4096 行 fake embedding 链路中，三个 executor 都表现出明显 fine/coalesced 差异。
- `ray_task` 是必要 baseline：在本轮 fake workload 中，`ray_task/coalesced` e2e 均值为 `2.200s`，略快于 `ray_actor/coalesced` 的 `2.353s`。
- `ray_actor/fine` 明显慢于 `ray_task/fine`，主要暴露在 `bounded_wait_s`：`28.224s` 对 `4.507s`。
- `writeback_s` 在多数组合中约 `0.45s-0.50s`，在最快的 actor 组合中已经是显著成本。
- actor batch/worker 矩阵显示，增加 workers 会降低 e2e 和 bounded wait。
- 本轮最快组合是 `batch=1024, workers=4`，e2e 均值 `1.433s`；但它只有 4 个 object/invocation，真实模型场景下是否仍合适需要继续验证。

合理推断：

- 当前 PG18.4 fake 链路中，首要收益来自减少过细 operator invocation/object；执行器选择是第二层因素。
- actor 数和 batch size 需要联合调参，不是单纯 batch 越小或越大越好。
- `ray_task` 不应被省略；它是判断 Ray actor 是否必要的关键消融。
- 如果真实模型服务存在内部 batching、GPU 利用率或 queueing，最优 batch/worker 组合可能改变。

不能声称：

- 不能说 `ray_task` 一定优于 `ray_actor`；本轮 fake embedding 是无状态函数，真实模型加载、缓存或连接复用可能更适合 actor。
- 不能说 `batch=1024, workers=4` 是最终最优配置；当前只测了 4096 行和 fake model。
- 不能说 PG18.3 内部平台有同样结果。
- 不能用当前 JSON 文本写回代表 pgvector `vector(128)` 写回。

## 6. 严谨性自检

已控制：

- 每个正式组合均有 1 次 warm-up 和 2 次 formal repeat。
- CSV 记录真实 `server_version` 和 `pgvector_version`。
- baseline 共享同一数据库读取、Arrow 构造、fake embedding 逻辑和 PostgreSQL 写回路径。
- `ray_task` 与 `ray_actor` 只替换 Ray 提交形态。

仍不足：

- formal repeat 只有 2 次，后续写论文图表前应增加到 3-5 次。
- fake embedding 使用 `time.sleep` 和本地随机向量生成，不能代表真实 CPU/GPU embedding。
- 当前写回仍是 `embedding_json TEXT`。
- Ray 每个脚本进程会启动本地 Ray 实例，启动行为偶发抖动；本轮 `batch=64, workers=1` 首次启动失败后已补跑。

## 7. 下一步

优先继续补：

1. pgvector `vector(128)` 写回对照，判断 `writeback_s` 是否被 JSON 文本误导。
2. `total_rows = 1024, 4096, 16384` scaling，重点只跑 coalesced 和少量 fine 对照。
3. 真实 CPU embedding 小模型，验证 fake invocation 信号是否迁移。
4. Ray actor / Ray Serve 真实 backpressure 实验，记录 queue wait、in-flight、actor idle time 和 throughput。
