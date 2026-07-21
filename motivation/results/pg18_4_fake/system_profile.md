# PG18.4 系统画像实验：fake AI_EMBED 链路瓶颈与可优化点

生成日期：2026-07-11

## 1. 实验定位

当前项目口径更新：本文件是 PG18.4 本地同构 fake-model 历史画像，只用于预演阶段计时、写回边界和早期变量选择。正式主线已经收敛为”数据库 AI 负载的执行优化与调度”，真实瓶颈归因应优先引用 `motivation/results/gpu/` 下的 GPU-backed 结果。

本文件记录 PG18.4 本地同构链路上的系统画像实验，用于测试系统瓶颈并寻找可优化点。

它回答的问题是：

> 在 PostgreSQL 18.4 -> Arrow RecordBatch -> Python/Ray fake AI_EMBED -> PostgreSQL 写回的真实数据库触发链路中，fine 与 coalesced 粒度是否会产生可观测差异？差异来自哪些阶段？

它不回答：

- 本地数据库能否连接。连接验证见 `feasibility/results/pg18_4_connection_validation.md`。
- 公司 PostgreSQL 18.3 内部平台是否表现相同。
- 真实 CPU/GPU embedding 模型是否有同样收益。
- 最终论文优化点是否已经锁定。

原始 CSV：

```text
motivation/results/pg18_4_fake/system_profile.csv
```

## 2. 代码改动

入口脚本：

```text
code/scripts/postgres_ai_operator_profile.py
```

本轮新增能力：

- `--executor python|ray_actor`：增加普通 Python baseline，用于对照 Ray actor 链路。
- `--warmup-runs` / `--repeats`：同一参数组合内区分 warm-up 与 formal 重复实验。
- `--experiment-id`：标记同一批实验。
- Ray 改为按需导入：Python baseline 和 dry-run 不再强制依赖 Ray。
- 拆分 `db_fetch_s` 与 `arrow_build_s`。
- `--seed-rows` 现在会补齐到目标行数，而不是表非空后直接跳过。

本轮使用的本地 conda 环境：

```text
.conda/pg-ai-profile
```

该目录是本地执行环境，已加入 `.gitignore`，不作为实验结果版本化。

## 3. 实验设置

数据库：

| 项目 | 值 |
|---|---|
| PostgreSQL | 18.4 (Debian 18.4-1.pgdg12+1) |
| pgvector | 0.8.2 |
| 连接地址 | `postgresql://postgres:postgres@localhost:5432/ai_operator` |
| 触发方式 | `ai_operator_jobs` job table |
| 输出表 | `document_embeddings` |

固定参数：

| 参数 | 值 |
|---|---:|
| `total_rows` | 4096 |
| `db_fetch_rows` | 512 |
| `ray_batch_rows` | 256 |
| `embedding_dim` | 128 |
| `model_workers` | 2 |
| `max_inflight` | 8 |
| warm-up | 1 |
| formal repeats | 3 |

对照矩阵：

| 维度 | 取值 |
|---|---|
| executor | `python`, `ray_actor` |
| strategy | `fine`, `coalesced` |

策略含义：

- `fine`：每行一个 operator invocation / object。
- `coalesced`：每 256 行一个 operator invocation / object。

## 4. 运行命令

环境准备：

```powershell
conda create --prefix .conda\pg-ai-profile python=3.10 -y
.conda\pg-ai-profile\python.exe -m pip install -r code\requirements.txt
```

正式矩阵：

```powershell
$py = '.conda\pg-ai-profile\python.exe'
$out = 'motivation\results\pg18_4_fake\system_profile.csv'
foreach ($executor in @('python','ray_actor')) {
  foreach ($strategy in @('fine','coalesced')) {
    & $py code\scripts\postgres_ai_operator_profile.py `
      --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
      --setup --seed-rows 4096 --total-rows 4096 `
      --db-fetch-rows 512 --ray-batch-rows 256 `
      --embedding-dim 128 --model-workers 2 --max-inflight 8 `
      --executor $executor --strategy $strategy `
      --warmup-runs 1 --repeats 3 `
      --experiment-id pg18_4_fake_4096 `
      --output $out
  }
}
```

## 5. 数据库核对

正式实验后数据库表状态：

| 表 | 行数 |
|---|---:|
| `documents` | 4096 |
| `document_embeddings` | 4096 |
| `ai_operator_jobs` | 20 |

最近 job 状态均为 `finished`。

## 6. Formal 结果汇总

以下只统计 `phase = formal` 的 3 次重复均值。

| executor | strategy | object_count | invocations | e2e_s 均值 | rows/s 均值 | model_service_s 均值 | bounded_wait_s 均值 | fanin_s 均值 | writeback_s 均值 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| python | fine | 4096 | 4096 | 64.288 | 63.713 | 63.561 | 0.000 | 0.000 | 0.479 |
| python | coalesced | 16 | 16 | 3.798 | 1078.509 | 3.259 | 0.000 | 0.000 | 0.507 |
| ray_actor | fine | 4096 | 4096 | 32.922 | 124.438 | 58.882 | 28.378 | 0.593 | 0.477 |
| ray_actor | coalesced | 16 | 16 | 2.435 | 1689.546 | 3.349 | 0.000 | 0.006 | 0.470 |

关键比值：

| executor | fine / coalesced e2e |
|---|---:|
| python | 16.927x |
| ray_actor | 13.523x |

## 7. 结果解释

本地实验事实：

- 固定 4096 行、固定文本 token 数、固定 128 维 fake embedding 下，`fine` 粒度显著慢于 `coalesced`。
- Python baseline 中，`fine` 的主要成本体现在 `model_service_s`，因为 4096 次单行 fake operator invocation 触发大量微小 `sleep` 与 Python 调用开销。
- Ray actor 中，`fine` 的端到端耗时约为 `32.922s`，低于 Python fine 的 `64.288s`，说明 2 个 actor 与 bounded in-flight 提供了一定并行度。
- Ray actor fine 仍显著慢于 Ray actor coalesced，并出现 `bounded_wait_s` 均值 `28.378s`、`fanin_s` 均值 `0.593s`，说明大量小 invocation/object 会把队列等待与 fan-in 成本暴露出来。
- `db_fetch_s` 与 `arrow_build_s` 在四组中都很小，不是本轮 4096 行 fake 链路的主要成本。
- `writeback_s` 约 `0.47s-0.51s`，在 coalesced Ray actor 下已经是可见成本之一。

合理推断：

- 本轮结果支持继续研究数据库 AI 算子外部执行链路中的 batch / invocation / object 粒度控制。
- 对 Ray actor 链路来说，coalescing 的收益不只是 fan-in refs 变少，还包括 operator invocation 数量、队列等待和 actor 调用粒度变化。
- 如果后续换成真实模型或 GPU 服务，收益来源可能转移到 batch size、GPU 利用率、CPU preprocess、模型服务队列和 writeback。

不能声称：

- 不能说这是 PostgreSQL 18.3 内部平台结果。
- 不能说真实 GPU embedding 一定有同样的 13x 级收益。
- 不能说 Ray 本身慢；本轮 fine 配置刻意制造了大量小 invocation/object。
- 不能把该结果直接包装成最终论文贡献。

## 8. 对技术路线的判断

基于 idea-evaluator 的 fatal-flaws 视角，当前最需要避免的风险是：

| 风险 | 对应 fatal flaw | 防御方式 |
|---|---|---|
| 先默认 Daft/Ray/Lance 一定是最终系统，再反向找优化点 | F9 Solution hunting for a problem | 把 Daft/Ray/Lance 写成候选外部执行系统，先用 PG18.4/PG18.3 画像证明瓶颈 |
| 把“走 Ray vs 不走 Ray”变成论文主线 | F4 No compelling motivation / F8 Overly ambitious scope | Ray/non-Ray 只做边界 baseline，主线放在数据库 AI 算子外部执行系统的瓶颈定位与调优 |
| 只用 fake workload 声称真实 GPU/模型收益 | F6 Unverifiable claim | 下一步先接真实 CPU embedding 或轻量模型服务，并在 PG18.3 内部平台复验；GPU 只做少量边界对照 |

当前更合理的定位：

> 以数据库内置 AI 算子的外部执行系统为对象，优先假设公司侧或候选实现会采用类似 Daft/Ray/Lance 的分布式数据处理链路；论文主线做该系统中的 batch、partition、task/actor、object、backpressure、writeback 调优。Ray/non-Ray baseline 作为消融和合理性边界，不作为主问题。

也就是说，用户偏好的“对系统做调优”是更匹配当前目标的主线。但这个“系统”不能现在就写死为 Daft+Ray+Lance 产品化适配，应该表述为：

> 面向数据库 AI 负载的执行优化与调度系统。

## 9. 严谨性自检

已控制：

- 每组使用相同 `total_rows = 4096`、相同文本、相同 embedding 维度。
- 每组有 1 次 warm-up 和 3 次 formal 重复。
- 每行 CSV 记录真实 `server_version` 与 `pgvector_version`。
- `db_fetch_s` 与 `arrow_build_s` 已拆分。
- `python` 与 `ray_actor` 共享同一 fake embedding 逻辑。

仍不足：

- fake embedding 使用 `time.sleep` 模拟模型服务，Windows 微小 sleep 粒度会放大单行 invocation 成本。
- 当前写回仍是 JSON 文本，不是 pgvector `vector` 列。
- 当前只跑了 4096 行一个规模，没有形成 rows scaling 曲线。
- Ray actor 的 `model_service_s` 是多个 actor 调用时间累计值，不能直接当作端到端模型墙钟时间。
- 本地 PG18.4 只是同构预演，不是公司 PG18.3 内部平台。

## 10. 下一步

1. 将写回从 `embedding_json TEXT` 扩展到 pgvector `vector(128)` 对照。
2. 增加 `total_rows = 1024, 4096, 16384` 的缩放曲线，但 fine 组需要谨慎控制，避免 Windows sleep 粒度主导全部结论。
3. 增加真实 CPU embedding 小模型或本地模型服务，验证 fake operator invocation 信号是否保留。
4. 如果 GPU 环境可用，尽早补一组 small-scale GPU-backed E2E profile，复用相同阶段计时，重点看 batch size、actor 并行度、in-flight、queue wait、GPU 利用率和 writeback；这组实验用于校准数据执行与存储链路画像，不把 GPU 迁移或 GPU kernel 优化作为主要实验线。
5. 最终在公司 PostgreSQL 18.3 内部验证平台复验。
