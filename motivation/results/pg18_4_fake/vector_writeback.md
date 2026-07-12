# PG18.4 pgvector vector(128) 写回对照实验

生成日期：2026-07-11

## 1. 实验定位

本实验延续 `baseline_matrix.md`，专门验证上一轮发现的写回成本：

> 当前最快 fake AI_EMBED 链路已经把 writeback 推成显著成本；如果不先测真实 pgvector `vector(128)` 写回，后续接真实模型时无法判断瓶颈到底来自模型、Ray，还是写回。

它回答的问题是：

- JSON TEXT 写回是否误导了 `writeback_s`？
- 真实 pgvector `vector(128)` 写回是否更慢？
- 单行 upsert 与批量 upsert 对端到端链路影响有多大？

它不回答：

- PostgreSQL 18.3 内部平台是否表现相同。
- 有 pgvector index 时写入成本是否相同。
- 真实 CPU/GPU embedding 模型是否有同样瓶颈。
- COPY / 二进制协议 / 专用 bulk loader 是否能进一步降低写回成本。

原始 CSV：

```text
motivation/results/pg18_4_fake/vector_writeback.csv
```

## 2. 代码改动

入口脚本：

```text
code/scripts/postgres_ai_operator_profile.py
```

本轮新增：

- `--writeback-mode json_text|pgvector`
- `--write-batch-rows N`
- `document_embeddings.embedding_vector vector(128)` 列

写回模式：

| 模式 | 含义 |
|---|---|
| `json_text` | 写入原有 `embedding_json TEXT` 列 |
| `pgvector` | 写入 `embedding_vector vector(128)` 列，`embedding_json` 只写占位值以满足旧表约束 |

说明：当前两种模式都会在 Python 端把 `float32[128]` 转成 PostgreSQL 可接收的文本字面量，因此本实验比较的是数据库写入与类型解析路径，不是最优二进制向量导入路径。

## 3. 实验设置

固定参数：

| 参数 | 值 |
|---|---:|
| PostgreSQL | 18.4 (Debian 18.4-1.pgdg12+1) |
| pgvector | 0.8.2 |
| total_rows | 4096 |
| db_fetch_rows | 4096 |
| executor | `ray_actor` |
| strategy | `coalesced` |
| ray_batch_rows | 1024 |
| model_workers | 4 |
| max_inflight | 8 |
| embedding_dim | 128 |
| warm-up | 1 |
| formal repeats | 3 |

这个配置来自上一轮最快链路附近：

```text
ray_actor + coalesced + batch=1024 + workers=4
```

对照矩阵：

| 维度 | 取值 |
|---|---|
| writeback_mode | `json_text`, `pgvector` |
| write_batch_rows | `1`, `64`, `256`, `1024`, `0` |

其中 `write_batch_rows = 0` 表示一次 `executemany` 写完整个结果集。

## 4. 运行命令

```powershell
$py = '.conda\pg-ai-profile\python.exe'
$out = 'motivation\results\pg18_4_fake\vector_writeback.csv'
foreach ($mode in @('json_text','pgvector')) {
  foreach ($writeBatch in @(1,64,256,1024,0)) {
    & $py code\scripts\postgres_ai_operator_profile.py `
      --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
      --setup --seed-rows 4096 --total-rows 4096 `
      --db-fetch-rows 4096 --ray-batch-rows 1024 `
      --embedding-dim 128 --model-workers 4 --max-inflight 8 `
      --executor ray_actor --strategy coalesced `
      --writeback-mode $mode --write-batch-rows $writeBatch `
      --warmup-runs 1 --repeats 3 `
      --experiment-id pg18_4_vector_writeback_4096 `
      --output $out
  }
}
```

## 5. 数据库核对

正式实验后核对 `document_embeddings` 中目标 4096 行：

| 指标 | 结果 |
|---|---:|
| rows | 4096 |
| non-null embedding_vector | 4096 |
| vector_dims(embedding_vector) | 128 |

## 6. Formal 结果

以下只统计 `phase = formal` 的 3 次重复均值。

| writeback mode | write batch rows | e2e_s | rows/s | writeback_s | writeback / e2e | fanin_s |
|---|---:|---:|---:|---:|---:|---:|
| json_text | 1 | 4.116 | 995.175 | 3.127 | 75.98% | 0.002 |
| json_text | 64 | 1.519 | 2695.915 | 0.540 | 35.55% | 0.002 |
| json_text | 256 | 1.500 | 2732.269 | 0.518 | 34.53% | 0.002 |
| json_text | 1024 | 1.440 | 2845.000 | 0.462 | 32.08% | 0.002 |
| json_text | all | 1.455 | 2820.206 | 0.458 | 31.48% | 0.002 |
| pgvector | 1 | 3.935 | 1041.055 | 2.960 | 75.22% | 0.002 |
| pgvector | 64 | 1.407 | 2913.295 | 0.433 | 30.77% | 0.002 |
| pgvector | 256 | 1.357 | 3018.118 | 0.383 | 28.22% | 0.002 |
| pgvector | 1024 | 1.385 | 2962.602 | 0.402 | 29.03% | 0.002 |
| pgvector | all | 1.360 | 3013.097 | 0.377 | 27.72% | 0.002 |

## 7. 结果解释

本地实验事实：

- 单行 upsert 会让写回成为绝对主瓶颈：`json_text` 单行写回 `3.127s`，`pgvector` 单行写回 `2.960s`，均占 e2e 约 75%。
- 批量写回后，`json_text` 写回降到约 `0.46s-0.54s`，`pgvector` 写回降到约 `0.38s-0.43s`。
- 在本轮 4096 行、128 维 fake embedding 下，pgvector `vector(128)` 批量写回没有比 JSON TEXT 更慢，反而略低。
- 即使使用最快的 pgvector 批量写回，`writeback_s = 0.377s`，仍占 e2e `1.360s` 的约 `27.72%`。
- `fanin_s` 在所有批量组中约 `0.002s`，不是本轮最快链路的主要成本。

合理推断：

- 上一轮“writeback 已经是显著成本”的判断成立；在最快链路附近，写回约占端到端 28%-35%。
- 但“JSON TEXT 写回误导出写回瓶颈”这个担心没有被本轮支持；真实 pgvector `vector(128)` 批量写回同样是可见成本。
- 当前更关键的写回因素是是否批量 upsert，而不是 JSON TEXT 与 pgvector 的类型差异。
- 后续接真实 CPU embedding 前，应该保留 writeback 计时边界，否则模型时间和写回时间会混在一起。

不能声称：

- 不能说 pgvector 写回总是比 JSON TEXT 快；本轮没有测 index、COPY、二进制协议、不同维度和不同机器。
- 不能说写回已经是最终最大瓶颈；真实模型接入后，模型计算可能重新主导端到端。
- 不能说 PostgreSQL 18.3 内部平台有同样结果。
- 不能说当前 `executemany` 是 pgvector 的最优写入方式。

## 8. 对后续实验的影响

这一步解释了为什么要先测写回：

- 如果后续真实模型很快，写回会继续是端到端中的大块成本。
- 如果后续真实模型很慢，写回占比会下降，但仍需要单独计时，否则无法解释瓶颈迁移。
- 如果使用单行 upsert，写回会压倒 Ray / batch / model 的其他信号，因此后续实验必须默认使用批量写回。

下一步可以进入 scaling，但需要保留这些设置：

- 默认使用 `pgvector` 写回。
- 默认使用批量写回，建议 `write_batch_rows = 256` 或 `0`。
- 每条结果继续记录 `writeback_s`，不要只看 e2e。

## 9. 下一步

建议继续做：

1. `total_rows = 1024, 4096, 16384` scaling，使用 `pgvector` 批量写回。
2. 只保留少量 fine 对照，避免 fine 组把时间浪费在已确认的 invocation 爆炸上。
3. scaling 完成后接真实 embedding 小模型或轻量模型服务；如果 GPU 环境可用，同时补一组 small-scale GPU-backed E2E profile，验证 fake invocation / batch / writeback 信号在真实计算端点下是否迁移。
