# PG18.4 连接验证记录

验证日期：2026-07-11

## 1. 定位

本文件只记录本地 PostgreSQL 18.4 + pgvector 环境是否能被项目系统连接和写回。

它回答的问题是：

> 本地同构预演环境是否可用，项目脚本是否能真实连接数据库、读表、写表？

它不回答：

- 系统瓶颈在哪里；
- fine/coalesced 是否有优化收益；
- Ray/actor 是否优于普通 Python；
- 真实 GPU / 真实模型链路是否成立；
- 公司 PostgreSQL 18.3 内部平台上的性能表现。

系统瓶颈与可优化点实验见：

```text
motivation/results/pg18_4_fake/system_profile.md
```

## 2. 环境信息

| 项目 | 结果 |
|---|---|
| Docker 容器 | `ai-operator-postgres18`，healthy |
| PostgreSQL | 18.4 (Debian 18.4-1.pgdg12+1) |
| pgvector | 0.8.2 |
| 主机端口 | `localhost:5432` |
| 数据库 | `ai_operator` |
| 连接地址 | `postgresql://postgres:postgres@localhost:5432/ai_operator` |
| 向量冒烟测试 | `'[1,2,3]' <-> '[1,2,4]' = 1` |

部署说明：

```text
deploy/postgres18.4/README.md
```

## 3. 项目链路冒烟

冒烟 CSV：

```text
feasibility/results/pg18_4_connection_smoke_256_rows.csv
feasibility/results/pg18_4_connection_smoke_runs.csv
```

链路：

```text
PostgreSQL documents/job table
  -> psycopg fetch
  -> Arrow RecordBatch
  -> Ray fake-embedding actors
  -> bounded in-flight + fan-in
  -> PostgreSQL document_embeddings writeback
```

首次 256 行冒烟配置：

| 参数 | 值 |
|---|---:|
| database trigger | job table |
| strategy | coalesced |
| total rows | 256 |
| DB fetch rows | 128 |
| Ray batch rows | 64 |
| embedding dim | 128 |
| model workers | 2 |
| max in-flight | 4 |

首次 256 行冒烟结果：

| 指标 | 结果 |
|---|---:|
| status | ok |
| total/written rows | 256 / 256 |
| object count | 4 |
| operator invocations | 4 |
| max in-flight seen | 2 |
| database fetch | 0.004885 s |
| Arrow build / fetch-to-Arrow outer timing | 0.021212 s |
| fake model service cumulative time | 0.282211 s |
| fan-in | 0.000953 s |
| writeback | 0.038092 s |
| end-to-end | 0.587693 s |
| throughput | 435.602 rows/s |

## 4. 数据库核对

首次冒烟后核对：

| 表 | 结果 |
|---|---:|
| `documents` | 256 rows |
| `document_embeddings` | 256 rows |
| `ai_operator_jobs` | 2 rows |
| latest job 2 | `AI_EMBED / finished` |

后续正式系统画像实验已将表扩展到 4096 行；这属于系统画像实验的数据状态，不改变本文件的连接验证定位。

## 5. 结论

本地实验事实：

- 本地 PostgreSQL 18.4 + pgvector 0.8.2 容器可用。
- 项目脚本能通过真实数据库 URL 连接 PG18.4。
- 项目脚本能完成建表、读取 documents、构造 Arrow RecordBatch、执行 fake AI_EMBED、更新 job 状态并写回 `document_embeddings`。

不能声称：

- 不能据此判断瓶颈或优化收益。
- 不能把本地 PG18.4 写成公司 PostgreSQL 18.3 内部平台结果。
- 不能把 256 行冒烟结果写成正式性能实验。
