# PG18.4 pgvector 批量写回 scaling 实验

生成日期：2026-07-11

## 1. 实验定位

本实验延续 `pg18_4_vector_writeback.md`，在确认 pgvector `vector(128)` 批量写回可用后，测试行数扩大时瓶颈是否迁移。

它回答的问题是：

> 使用 pgvector `vector(128)` 批量写回后，`total_rows = 1024, 4096, 16384` 扩大时，端到端耗时、写回占比、Ray task/actor 并行收益和 fine/coalesced 差异是否稳定？

它不回答：

- PostgreSQL 18.3 内部平台是否表现相同。
- 真实 CPU/GPU embedding 模型是否表现相同。
- pgvector index、COPY、二进制协议或生产级 bulk loader 的写回成本。
- Ray Serve / vLLM 模型服务队列表现。

原始 CSV：

```text
motivation/results/pg18_4_pgvector_scaling.csv
motivation/results/pg18_4_pgvector_scaling_fine_contrast.csv
```

## 2. 实验设置

### 2.1 Coalesced scaling

固定参数：

| 参数 | 值 |
|---|---:|
| PostgreSQL | 18.4 (Debian 18.4-1.pgdg12+1) |
| pgvector | 0.8.2 |
| strategy | `coalesced` |
| ray_batch_rows | 1024 |
| model_workers | 4 |
| max_inflight | 8 |
| embedding_dim | 128 |
| writeback_mode | `pgvector` |
| write_batch_rows | 256 |
| warm-up | 1 |
| formal repeats | 3 |

对照矩阵：

| 维度 | 取值 |
|---|---|
| total_rows | 1024, 4096, 16384 |
| executor | `python`, `ray_task`, `ray_actor` |

### 2.2 Fine contrast

固定参数与 coalesced scaling 相同，只改变：

| 维度 | 取值 |
|---|---|
| strategy | `fine` |
| total_rows | 1024, 4096 |
| executor | `ray_task`, `ray_actor` |
| formal repeats | 2 |

没有跑 `16384 fine`，因为前面实验已经确认 fine 会制造大量 invocation/object；继续扩大只会消耗时间，并不能增加新的判断力。

## 3. 运行命令

### 3.1 Coalesced scaling

```powershell
$py = '.conda\pg-ai-profile\python.exe'
$out = 'motivation\results\pg18_4_pgvector_scaling.csv'
foreach ($rows in @(1024,4096,16384)) {
  foreach ($executor in @('python','ray_task','ray_actor')) {
    & $py code\scripts\postgres_ai_operator_profile.py `
      --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
      --setup --seed-rows $rows --total-rows $rows `
      --db-fetch-rows $rows --ray-batch-rows 1024 `
      --embedding-dim 128 --model-workers 4 --max-inflight 8 `
      --executor $executor --strategy coalesced `
      --writeback-mode pgvector --write-batch-rows 256 `
      --warmup-runs 1 --repeats 3 `
      --experiment-id pg18_4_pgvector_scaling `
      --output $out
  }
}
```

### 3.2 Fine contrast

```powershell
$py = '.conda\pg-ai-profile\python.exe'
$out = 'motivation\results\pg18_4_pgvector_scaling_fine_contrast.csv'
foreach ($rows in @(1024,4096)) {
  foreach ($executor in @('ray_task','ray_actor')) {
    & $py code\scripts\postgres_ai_operator_profile.py `
      --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
      --setup --seed-rows $rows --total-rows $rows `
      --db-fetch-rows $rows --ray-batch-rows 1024 `
      --embedding-dim 128 --model-workers 4 --max-inflight 8 `
      --executor $executor --strategy fine `
      --writeback-mode pgvector --write-batch-rows 256 `
      --warmup-runs 1 --repeats 2 `
      --experiment-id pg18_4_pgvector_scaling_fine `
      --output $out
  }
}
```

## 4. Formal 结果

### 4.1 Coalesced scaling

以下只统计 `phase = formal` 的 3 次重复均值。

| rows | executor | objects | e2e_s | rows/s | writeback_s | writeback/e2e | bounded_wait_s | fanin_s | model_service_s |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1024 | python | 1 | 0.914 | 1120.291 | 0.105 | 11.4% | 0.000 | 0.000 | 0.803 |
| 1024 | ray_task | 1 | 0.924 | 1108.693 | 0.104 | 11.3% | 0.000 | 0.001 | 0.801 |
| 1024 | ray_actor | 1 | 1.070 | 957.090 | 0.105 | 9.8% | 0.000 | 0.001 | 0.808 |
| 4096 | python | 4 | 3.588 | 1141.693 | 0.384 | 10.7% | 0.000 | 0.000 | 3.184 |
| 4096 | ray_task | 4 | 1.240 | 3303.636 | 0.419 | 33.8% | 0.000 | 0.002 | 3.174 |
| 4096 | ray_actor | 4 | 1.363 | 3008.302 | 0.381 | 28.0% | 0.000 | 0.002 | 3.268 |
| 16384 | python | 16 | 14.247 | 1150.040 | 1.483 | 10.4% | 0.000 | 0.000 | 12.706 |
| 16384 | ray_task | 16 | 3.172 | 5165.845 | 1.493 | 47.1% | 0.804 | 0.007 | 12.794 |
| 16384 | ray_actor | 16 | 4.967 | 3299.551 | 1.546 | 31.1% | 1.735 | 0.007 | 12.894 |

### 4.2 Fine contrast

以下只统计 `phase = formal` 的 2 次重复均值。

| rows | executor | objects | invocations | e2e_s | rows/s | writeback_s | writeback/e2e | bounded_wait_s | fanin_s |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1024 | ray_task | 1024 | 1024 | 2.110 | 485.414 | 0.100 | 4.7% | 1.110 | 0.122 |
| 1024 | ray_actor | 1024 | 1024 | 4.246 | 241.158 | 0.102 | 2.4% | 3.172 | 0.146 |
| 4096 | ray_task | 4096 | 4096 | 8.722 | 470.366 | 0.422 | 4.8% | 4.172 | 0.489 |
| 4096 | ray_actor | 4096 | 4096 | 16.981 | 241.308 | 0.385 | 2.3% | 12.557 | 0.594 |

Fine / coalesced e2e 比值：

| rows | executor | fine/coalesced e2e |
|---:|---|---:|
| 1024 | ray_task | 2.28x |
| 1024 | ray_actor | 3.97x |
| 4096 | ray_task | 7.03x |
| 4096 | ray_actor | 12.46x |

## 5. 结果解释

本地实验事实：

- Python coalesced 在 1024、4096、16384 行下 rows/s 基本稳定在 `1120-1150 rows/s`，说明顺序 fake embedding + pgvector 批量写回近似线性扩展。
- Ray task / Ray actor 在 1024 行只有 1 个 batch，无法发挥并行优势，e2e 与 Python 接近甚至略慢。
- 4096 行开始，Ray task 和 Ray actor 都显著快于 Python；4096 行 Ray task e2e 为 `1.240s`，Python 为 `3.588s`。
- 16384 行下 Ray task e2e 为 `3.172s`，吞吐 `5165.845 rows/s`，是本轮 coalesced scaling 中最高吞吐。
- 16384 行下 Ray actor e2e 为 `4.967s`，慢于 Ray task，主要多出 `bounded_wait_s = 1.735s`。
- 随着 Ray 并行化降低模型阶段墙钟时间，writeback 占比上升：Ray task 在 16384 行下 writeback 占 e2e `47.1%`。
- Fine 对照中，writeback 占比很低，不是因为写回便宜，而是因为大量 invocation/object 引发的 bounded wait 和 fan-in 淹没了写回成本。

合理推断：

- pgvector 批量写回后，coalesced 链路的主要瓶颈会随并行化发生迁移：Python 下主要是 fake model 顺序执行，Ray task 高并行下 writeback 成为大块成本。
- 当前 fake workload 下，Ray task 是强 baseline，不能默认 Ray actor 更优。
- Fine 粒度仍然不适合作为后续主路径；它会把问题变成 invocation/queue/fan-in 爆炸，遮蔽模型和写回的瓶颈迁移。
- 后续接真实 CPU embedding 时，必须继续保留 `model_service_s`、`bounded_wait_s`、`fanin_s`、`writeback_s` 四个边界，否则无法解释瓶颈是否从 fake model 迁移到真实模型或写回。

不能声称：

- 不能说 Ray task 总是优于 Ray actor；真实模型加载、模型状态、连接复用、缓存和 GPU 服务可能改变结论。
- 不能说 16384 行 Ray task 的 `5165 rows/s` 代表真实 embedding 吞吐；当前仍是 fake embedding。
- 不能说 PG18.3 内部平台有同样 scaling 曲线。
- 不能说 writeback 是最终最大瓶颈；真实模型接入后模型计算可能重新主导。

## 6. 严谨性自检

已控制：

- 所有 coalesced scaling 组均使用 pgvector `vector(128)` 写回。
- 所有 coalesced scaling 组均使用 `write_batch_rows = 256`。
- 每条 CSV 均记录真实 `server_version` 和 `pgvector_version`。
- 每个 coalesced 组合有 1 次 warm-up 和 3 次 formal repeat。
- Fine 对照只用于确认过细粒度是否仍然破坏链路，不用于主线吞吐结论。

仍不足：

- 当前 fake embedding 仍由 `time.sleep` 和随机向量生成模拟。
- Ray 每个脚本进程启动本地实例，可能带来环境抖动。
- 没有测 pgvector index、COPY、二进制写入和更高维向量。
- 没有接真实 CPU/GPU embedding 服务。

## 7. 对下一步的影响

本实验支持继续进入真实 CPU embedding 小模型，但要带着三个约束：

1. 默认使用 coalesced + pgvector 批量写回，避免 fine 和单行 upsert 遮蔽真实模型信号。
2. 保留 Python、Ray task、Ray actor 三个 executor baseline，尤其不能省略 Ray task。
3. 继续记录模型、bounded wait、fan-in、writeback 的分段时间。

下一步建议：

> 接真实 CPU embedding 小模型，先跑 `total_rows = 1024, 4096`，比较 `python batched / ray_task / ray_actor`，并复用本实验的 pgvector 批量写回路径。
