# PostgreSQL 18 本地同构环境验证记录

验证日期：2026-07-11

## 1. 环境定位

本地实例用于预演数据库 AI 算子外部执行链路，不是公司内部 PostgreSQL
18.3 验证平台。部署规则和命令见：

```text
deploy/postgres18.4/AGENTS.md
deploy/postgres18.4/README.md
```

## 2. 数据库环境

| 项目 | 结果 |
|---|---|
| Docker 容器 | `ai-operator-postgres18`，healthy |
| PostgreSQL | 18.4 (Debian 18.4-1.pgdg12+1) |
| pgvector | 0.8.2 |
| 宿主机端口 | `localhost:5432` |
| 数据库 | `ai_operator` |
| 向量冒烟测试 | `'[1,2,3]' <-> '[1,2,4]' = 1` |
| 持久化目录 | PostgreSQL 18 约定的 `/var/lib/postgresql` named volume |

连接地址：

```text
postgresql://postgres:postgres@localhost:5432/ai_operator
```

## 3. 项目链路首次连通

项目画像脚本已完成一次 256 行同构链路冒烟运行，原始 CSV：

```text
validation/results/postgres_ai_operator_profile.csv
```

运行环境依赖：NumPy 2.2.6、PyArrow 25.0.0、psycopg 3.3.4、Ray 2.56.0。

连接、读取、Ray 执行与写回代码的函数级索引见：

```text
code/scripts/README.md
```

### 3.1 实验设置

| 参数 | 值 | 含义 |
|---|---:|---|
| database trigger | job table | 任务表模拟数据库触发入口 |
| strategy | coalesced | 每个 Ray batch 处理多行 |
| total rows | 256 | 本次处理总行数 |
| DB fetch rows | 128 | 每次数据库读取行数 |
| Ray batch rows | 64 | 每次 actor 调用行数 |
| embedding dim | 128 | fake embedding 维度 |
| model workers | 2 | Ray actor 数量 |
| max in-flight | 4 | 最大在途调用数 |

运行命令：

```bash
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/ai_operator" \
.venv/bin/python code/scripts/postgres_ai_operator_profile.py \
  --setup --seed-rows 256 --total-rows 256 \
  --db-fetch-rows 128 --ray-batch-rows 64 \
  --model-workers 2 --max-inflight 4 \
  --strategy coalesced \
  --output validation/results/postgres_ai_operator_profile.csv
```

### 3.2 实验过程

```text
PostgreSQL documents/job table
  -> psycopg fetch
  -> Arrow RecordBatch
  -> 2 Ray fake-embedding actors
  -> bounded in-flight + fan-in
  -> PostgreSQL document_embeddings writeback
```

### 3.3 实验数据

| 指标 | 结果 |
|---|---:|
| status | ok |
| total/written rows | 256 / 256 |
| object count | 4 |
| operator invocations | 4 |
| max in-flight seen | 2 |
| server version | 18.4 (Debian 18.4-1.pgdg12+1) |
| pgvector version | 0.8.2 |
| database fetch | 0.004885 s |
| Arrow build（包含 fetch-to-Arrow 外层计时） | 0.021212 s |
| fake model service 累计时间 | 0.282211 s |
| fan-in | 0.000953 s |
| writeback | 0.038092 s |
| end-to-end | 0.587693 s |
| throughput | 435.602 rows/s |

数据库核对：

| 表/状态 | 结果 |
|---|---:|
| `documents` | 256 rows |
| `document_embeddings` | 256 rows |
| `ai_operator_jobs` | 2 rows |
| latest job 2 | `AI_EMBED / finished` |

### 3.4 严谨性自检

本次是首次连通性冒烟实验，不是性能对比实验：

- 已运行两次连通性冒烟，但没有预先定义 warm-up，也没有正式重复统计；
- 使用 fake embedding，不是真实 CPU/GPU 模型；
- 使用 PostgreSQL 18.4 本地替身，不是公司 PostgreSQL 18.3 平台；
- 只运行 coalesced 策略，没有 fine、Python worker 或 task baseline；
- `model_service_s` 是多个 actor 调用耗时的累计值，不能直接当作端到端模型墙钟时间；
- `arrow_build_s` 外层覆盖数据库 fetch 和 Arrow 构造，和 `db_fetch_s` 有嵌套，不能相加解释阶段占比；
- 当前 embedding 写成 JSON 文本，尚未写入 pgvector `vector` 列。

因此本次只能证明：项目脚本能够真实连接 PostgreSQL，完成读取、Ray/Arrow
处理、任务状态更新和等行数写回。不能据此判断主要瓶颈或优化收益。

### 3.5 对课题方向的含义

本地实验事实：数据库 AI 算子外部执行链路的最小形态已经跑通，Phase 1
不再停留在设计层。

待确认：在真实模型、更多数据、重复实验和公司 PostgreSQL 18.3 平台下，
task/object、模型队列、fan-in 或写回谁是主要成本。

## 4. 下一步实验

1. 修正阶段计时的嵌套边界，把 DB fetch 与 Arrow build 拆开；
2. 增加至少一次 warm-up 和 3 次正式重复；
3. 比较 fine/coalesced，并保持总行数和计算量一致；
4. 增加普通 Python worker baseline；
5. 把输出从 JSON 文本扩展为 pgvector `vector` 列；
6. 再接真实本地 embedding 模型，最终到公司 PostgreSQL 18.3 复验。
