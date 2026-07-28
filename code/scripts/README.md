# PostgreSQL AI 算子画像脚本

## 文件定位

当前连接与测试流程集中在：

```text
code/scripts/postgres_ai_operator_profile.py
```

pgai SQL trigger-surface profile entry:

```text
code/scripts/pgai_sql_operator_profile.py
```

Daft text DataOrganizer smoke entry:

```text
code/scripts/daft_text_organizer_smoke.py
```

Shared-vLLM K_max interference runner:

```text
code/scripts/run_kmax_interference_experiment.py
```

Seeded scenario runner:

```text
code/scripts/run_ai_operator_scenarios.py
```

该 runner 在输出目录持有 `.runner-lease.json` 原子租约，禁止两个进程同时写
同一 manifest/CSV。中断恢复必须复用原配置和输出目录；只有确认旧 owner
已经消失后，才可同时传 `--resume --recover-stale-lease`。不要手工删除租约。

它既是当前 Phase 1 的实验驱动脚本，也是后续拆分正式 worker 之前的最小端到端实现。当前没有另一份隐藏的连接代码。

本目录只放实验主体、服务启动、数据采集和 profiling 入口。绘图、图表复现和素材筛选脚本统一放在 `figures/scripts/`。

## 流程与函数映射

```text
PostgreSQL documents/job table
  -> DataSource (arrow_postgres or daft_postgres)
  -> ArrowOrganizer / DaftOrganizer
  -> typed BatchRequest + endpoint topology
  -> SynchronousScheduler + RaySubmissionAdapter
  -> Ray task/actor -> model backend (fake, compatible_http, or ollama)
  -> sink.write / finish_job
  -> metrics append
```

| 环节 | 函数/对象 | 作用 |
|---|---|---|
| 数据库连接 | `connect` | 使用 psycopg 和 `--database-url` 建立连接 |
| 平台识别 | `database_metadata` | 读取真实 PG 和 pgvector 版本并写入 CSV |
| 建表 | `setup_schema` / `SCHEMA_SQL` | 创建 documents、jobs、embeddings、completions 表 |
| 任务触发替身 | `create_job` / `finish_job` / `fail_job` | 用 job table 模拟数据库 AI 算子触发，并记录成功或失败终态 |
| 数据读取 | `PostgresArrowSource` / `DaftPostgresSource` | 从 PG 基线路径或 Daft SQL 入口读取并返回 Arrow Table |
| 批划分 | `ArrowOrganizer` / `DaftOrganizer` | 按策略决定 actor 输入粒度；Daft 后端通过 `code/src/organizers.py` 接入 |
| AI 算子 | `FakeEmbeddingActor` / `CompatibleHTTPEmbeddingActor` / `FakeCompletionActor` / `CompatibleHTTPCompletionActor` / `OllamaCompletionActor` | `fake` 只用于离线 smoke 和控制变量；`compatible_http` 用于 vLLM-compatible embedding 或 completion endpoint；`ollama` 用于本地 Ollama `/api/generate` completion smoke |
| 并发与反压 | `submit_ray_tasks` / `submit_with_backpressure` → `SynchronousScheduler` | 静态 task/actor 路径统一执行 K_max、路由、等待和 fan-in；旧 queue-adaptive 分支暂时隔离保留 |
| 数据写回 | `code/src/sinks.py::write_embeddings` / `write_completions` | embedding 支持 `none`、JSON 文本和 pgvector；completion 支持 `none` 和 JSON 文本 |
| 指标输出 | `code/src/metrics.py::preflight_metrics_schema` / `append_metrics` | 正式工作前用 dry-run keys 拒绝旧 schema；追加时要求已有 header 与当前 row keys 精确一致 |
| 场景单写者 | `code/src/runner_lease.py::acquire_runner_lease` | 原子占用输出目录，校验 owner、进程启动身份与 config fingerprint，显式记录 stale recovery |
| completion 粒度 | `profiling.replay::_service_quantum_envelopes` | 在 planning batch 内按预测 work 切完整行，分别生成 HTTP/Ray completion 与 credit 释放单元；不拆单行 prompt |
| actor worker pool | `ActorWorkerPoolSubmitter` / `RaySubmissionAdapter` | 每个 endpoint 显式限制 worker slots，按 round-robin 或 least-active-work 分配，completion/failure 后由 canonical handle 精确释放 |

## 当前本地运行

```bash
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/ai_operator" \
.venv/bin/python code/scripts/postgres_ai_operator_profile.py \
  --setup --seed-rows 256 --total-rows 256 \
  --db-fetch-rows 128 --ray-batch-rows 64 \
  --model-workers 2 --max-inflight 4 \
  --strategy coalesced \
  --output feasibility/results/pg18_4_connection_smoke_256_rows.csv
```

`--submission-granularity service_quantum --service-quantum-tokens N` 同时
适用于 offline 与 arrival replay。planning batch 仍由 token-budget、
length-align 等组织策略决定；service quantum 只改变下游完成与补位粒度。
汇总 CSV 分开记录 organization batch 和 service quantum 的 count/rows/work，
submission trace schema 4 记录两级 ID、oversized 标记、credit-held 与
Ray-to-service 时间，避免把“更小 completion 单元”误写成“更好的数据组织”。

`--actor-workers-per-endpoint W --ray-actor-max-concurrency C` 的物理上限是
每 endpoint `W × C` 个 driver-owned slots；即使 `--max-inflight` 更大，
effective endpoint admission 也不会越过该上限。使用
`--actor-worker-routing least_active_work` 时，只在仍有空 slot 的 worker 中按
active work、running 数和稳定 worker index 选择。汇总里的
`actor_worker_slot_held_utilization` 包含 Ray/HTTP 等待时间，不是 GPU compute
utilization；submission trace 另记 worker ID/index/PID 供归因。

双 GPU 饱和后门禁使用两份隔离模板：

- `deploy/autodl/dual_gpu_actor_pool_shape.example.json` 固定每 endpoint
  256 slots，比 1×256/2×128/4×64；
- `deploy/autodl/dual_gpu_service_quantum.example.json` 固定所选 pool、active
  work 与 planning budget，比 batch、512/1024/2048/4096 quantum 和 request
  diagnostic。

不能把 actor 总 slots 随 arm 改变。当前 8192 quantum 大于已观测组织批次
最大 work（约 5892），不会切分任何批次，故不进入正式矩阵。

## 结果位置

- 原始数据：`feasibility/results/pg18_4_connection_smoke_256_rows.csv`
- 设置、过程、表核对、严谨性与结论：
  `feasibility/results/pg18_4_connection_validation.md`
- PostgreSQL 18.4 + pgvector 数据库部署：`deploy/postgres18.4/README.md`
- pgai SQL 算子触发面预演：`deploy/pgai/README.md`

## Daft text organizer smoke

`daft_text_organizer_smoke.py` is the smallest script-level entry for the
organizer abstraction in `code/src/organizers.py`. It does not connect to
PostgreSQL or vLLM; it verifies that text rows can pass through either
`ArrowOrganizer` or `DaftOrganizer` and return downstream Arrow batches. Use
`--runner ray` when checking Daft `into_partitions` or `repartition`;
NativeRunner reports these partition operations as no-op. The default output is
under `tmp/` because this is a local smoke result, not a formal experiment
result.

```powershell
.conda\pg-ai-profile\python.exe code\scripts\daft_text_organizer_smoke.py `
  --organizer arrow --rows 256 --batch-size 64 `
  --output tmp\daft_text_organizer_smoke.csv

.conda\pg-ai-profile\python.exe code\scripts\daft_text_organizer_smoke.py `
  --organizer daft --runner ray --rows 32 --batch-size 8 `
  --partition-mode into_partitions --partitions 4 `
  --output tmp\daft_text_organizer_smoke.csv
```

当前结果只证明 PostgreSQL 18.4 同构链路连通，不是公司 PostgreSQL 18.3
平台结果，也不是性能优化结论。

## 正式对照实验

2026-07-11 已为 `postgres_ai_operator_profile.py` 增加可重复对照实验参数：

- `--executor python|ray_task|ray_actor`
- `--data-source arrow_postgres|daft_postgres`
- `--source-order doc_id|arrival_time`
- `--source-max-prompt-tokens`
- `--arrival-replay`
- `--arrival-time-scale`
- `--flush-policy immediate|fixed_timeout|queue_adaptive`
- `--flush-timeout-ms`
- `--flush-max-wait-ms`
- `--flush-trace-output`
- `--submission-trace-output`
- `--resource-trace-output`
- `--resource-sample-interval-s`
- `--model-flops-per-token`
- `--gpu-peak-tflops`
- `--mfu-precision`
- `--request-trace-output`
- `--request-slo-ms`
- `--scenario-id`
- `--random-seed`
- `--batching-policy fixed_rows|token_budget|best_fit_token_budget|length_align_fixed_rows|length_align_token_budget|prefix_aware_fixed_rows|prefix_aware_token_budget`
- `--token-budget`
- `--output-cost-mode prompt_only|fixed_output_cap|trace_target_output`
- `--cost-model-id`
- `--cost-tokenizer-id`
- `--scheduling-policy static|queue_adaptive|aimd|ewma_aimd|pid`
- `--adaptive-min-inflight`
- `--adaptive-max-inflight`
- `--adaptive-queue-threshold`
- `--adaptive-running-threshold`
- `--adaptive-kv-threshold`
- `--adaptive-poll-interval-s`
- `--controller-min-window` / `--controller-max-window`
- `--controller-initial-window`
- `--adaptive-sample-interval-s`
- `--ewma-alpha`
- `--pid-proportional-gain` / `--pid-integral-gain` / `--pid-derivative-gain`
- `--control-trace-output`
- `--endpoint-routing round_robin|least_queued|prefix_affinity`
- `--pool-routing none|request_cost`
- `--endpoint-pool-ids` / `--endpoint-gpu-ids`
- `--long-request-token-threshold`
- `--operator ai_embed|ai_complete`
- `--organizer arrow|daft`
- `--organizer-partition-mode none|into_partitions|repartition`
- `--organizer-partitions`
- `--daft-runner native|ray`
- `--model-backend fake|compatible_http|http_openai|ollama`
- `--embedding-endpoint-url`
- `--embedding-model`
- `--embedding-api-key`
- `--completion-endpoint-url`
- `--completion-model`
- `--completion-api-key`
- `--completion-max-tokens`
- `--completion-return-token-ids`
- `--completion-prompt-format raw|chatml`
- `--completion-temperature`
- `--model-metrics-url`
- `--writeback-mode none|json_text|pgvector`
- `--write-batch-rows`
- `--warmup-runs`
- `--repeats`
- `--run-phase warmup|formal`
- `--run-repeat-index`
- `--experiment-id`

运行级 CSV 现在直接记录 `tokens_per_s`，计算口径为 vLLM Prometheus 的
`(prompt_tokens_delta + generation_tokens_delta) / e2e_s`。该字段是服务端
实际 token 增量，不是 organizer 的 token cost 估计。

`queue_adaptive` flush 使用双窗口：`--flush-timeout-ms` 是低负载和指标
缺失时的 fixed-timeout fallback，`--flush-max-wait-ms` 是 waiting、KV 或
running 压力下的扩展窗口。running 压力阈值使用本次运行的
`--max-inflight`，不使用独立硬编码常量。窗口在 pending batch 打开时选择
一次，并写入 flush trace 的 `selected_wait_s` 和 `window_reason`。

`--source-order doc_id` is the offline throughput mode: PostgreSQL already
contains the workload rows, and the profile scans them in stable document-id
order before Daft organization. `--source-order arrival_time` reads rows by
`arrival_time_s NULLS LAST, doc_id`, but sorting alone is not arrival replay.
K_max experiments may use the sorted stream, while online flush experiments
must also pass `--arrival-replay`. Replay requires `daft_postgres` and a Ray
task/actor executor, preserves the observed inter-arrival gaps on a monotonic
clock, and rejects missing or decreasing arrival values.
`--arrival-time-scale` multiplies normalized replay offsets while leaving raw
database timestamps and flush timeouts unchanged. It defaults to `1.0`; values
below one are controlled trace acceleration and are recorded in every run.

The three runtime decisions are separate:

1. `--batching-policy` and `--token-budget` determine batch membership.
2. `--flush-policy` determines when a pending partial batch closes.
3. `--scheduling-policy` and its K_max/controller options govern closed-batch
   admission.

`--admission-scope global` preserves the historical meaning: one K_max is
shared by every endpoint. For a static multi-endpoint run,
`--admission-scope per_endpoint --max-inflight K` gives each endpoint an
independent K-credit cap and sets the scheduler-wide safety ceiling to
`K * endpoint_count`. The CSV records the configured K, per-endpoint cap, and
effective global ceiling separately. Per-endpoint scope is intentionally not
accepted for adaptive controllers yet: those controllers still maintain one
global window, so labelling them per-endpoint would be false.

`best_fit_token_budget` applies deterministic best-fit-decreasing packing to
complete rows visible to one organizer call. It is an offline organization
policy and is rejected with `--arrival-replay`, because replay must preserve
arrival order. `--output-cost-mode` controls only organization and scheduling
cost estimates; it never changes the backend `--completion-max-tokens` cap:

All token-budget policies enforce both `--token-budget` and
`--ray-batch-rows`. The latter is a hard per-submission row cap for sequential
and best-fit packing, so algorithm comparisons do not silently change maximum
request fan-out.

- `prompt_only` uses zero estimated output tokens
  (`output_cost_source=configured_zero`);
- `fixed_output_cap` uses the configured completion cap for every row
  (`output_cost_source=backend_completion_cap`);
- `trace_target_output` reads each row's `target_output_tokens`
  (`output_cost_source=burstgpt_unpaired_trace_metadata`).

The current trace targets are unpaired BurstGPT metadata, not oracle output
lengths for the configured prompt/model. Formal outputs therefore also record
`cost_model_id`, `cost_tokenizer_id`, `packing_scope`, the explicit packing
algorithm, budget utilization, oversized rows, and batch cost-unit
percentiles. A global-BFD claim is valid only when the full compared workload
is visible in one organizer call and `packing_scope=organizer_input`.

If `--flush-trace-output` is omitted, replay writes
`<output-stem>_flush_trace.csv` beside the main CSV. Queue-adaptive replay reads
vLLM metrics through a background sampler so metric I/O cannot block the hard
maximum wait.

Formal runs should also set `--submission-trace-output` and
`--resource-trace-output`. The first records one row per closed batch with
an explicit `submission_id`, document identity, token counts, and service
timestamps (schema 2). The second samples
GPU utilization/memory and vLLM running/waiting/KV signals every 250 ms without
blocking the submission loop.

The main run row aggregates the resource trace into GPU utilization
mean/P50/P95/max, low-utilization time ratio, memory mean/max, vLLM
running/waiting/KV distributions, and (when `nvidia-smi` exposes
`power.draw`) power, integrated energy, and energy per 1,000 observed tokens.
`--resource-sample-interval-s` must remain identical across compared
scenarios.
For Ray endpoint experiments, `--endpoint-gpu-ids` also scopes `nvidia-smi`
sampling to the GPUs serving those endpoints. This prevents a single-endpoint
control on a multi-GPU host from averaging in an idle, out-of-scope device.

MFU is an explicitly labelled estimate, not a renamed GPU-utilization value.
It is left empty unless `--gpu-peak-tflops` and the matching
`--mfu-precision` are configured. The preferred numerator is the delta of
vLLM's `estimated_flops_per_gpu_total` counter. On vLLM 0.25.1 the service
must be started with `--enable-mfu-metrics`: the counter name can exist while
remaining permanently zero when the flag is absent, so a preflight must
verify a positive single-request delta rather than only metric presence.
Older vLLM versions may fall
back to `--model-flops-per-token` multiplied by observed prompt+generation
tokens. The time basis is `operator_wall_s`; output rows retain the selected
method, status, and all inputs for audit. The fallback scalar FLOP/token
estimate approximates prefill and decode jointly, so formal reports must
describe both paths as estimated MFU.

After repeated runs, generate plot-ready long-form statistics with:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\summarize_output_aware_bfd.py `
  --runs experiments\results\<experiment>\runs.csv `
  --output experiments\results\<experiment>\summary.csv
```

The summary includes row/token throughput, E2E/tail/SLO metrics, stage times,
batch and packing shape, GPU/memory, vLLM pressure/latency, energy, and MFU.
It excludes warm-ups and failed runs and reports `n`, mean, sample standard
deviation, P50, min, and max per scenario. Older CSVs remain readable: metrics
that were not recorded are emitted with `n=0` instead of rejecting the file.

`--request-trace-output` additionally writes one row per complete input prompt
on typed static/AIMD/EWMA/PID Ray paths. Arrival replay rows use
`request_time_origin=replayed_arrival`; offline organization rows use one
`offline_job_start` origin so their E2E includes source fetch, organization,
submission, and model completion. Each row records buffer/organization,
submit, service, completion, client E2E, endpoint/GPU identity, and optional
SLO status. A multi-prompt endpoint response exposes only submission-level
completion timing, so these rows use `latency_granularity=submission`; they
are client-observed per-prompt E2E values, not vLLM internal per-sequence
completion timestamps. Request trace schema version 3 records the time origin
and finish reason explicitly so offline and replay latency distributions
cannot be conflated.
Client lifecycle timestamps share one stable clock. Backend service epochs use
`service_clock_domain=backend`; when backend/client clocks cannot be ordered
reliably, `submit_to_service_s` is left empty instead of inventing queue time.

Aggregate compatible-endpoint token usage is never split into fabricated
per-request values. For vLLM, `--completion-return-token-ids` opts into genuine
per-choice token IDs and finish reasons; generic compatible endpoints keep the
extension disabled. `client_estimated_output_tokens` remains an explicitly
labelled whitespace-token estimate, while `actual_output_tokens` is populated
only when the backend supplies per-choice token IDs.
Replay timestamps use one epoch anchor plus monotonic elapsed time, so wall
clock adjustments cannot invert arrival and flush ordering.

## Seeded scenario runner

`run_ai_operator_scenarios.py` executes each profiler run in a separate
process. Warm-ups preserve configuration order; formal scenarios are shuffled
once per repeat with the recorded seed. Before every run, the runner requires
the model health endpoint to return HTTP 200 and the vLLM running/waiting
gauges to both equal zero. It stops at the first failed process or missing run
CSV row, and atomically updates `manifest.json` after every completed run.

`--resume` verifies that the config, seed, schedule, manifest, and successful
CSV rows still agree before skipping completed runs. A recovered failure remains
in the incident history. `--skip-failed-scenarios` is available only with
`--resume`; it records every omitted schedule item instead of fabricating a
successful CSV row.

The JSON configuration contains shared profiler arguments and scenario-specific
arguments. Output paths and run identity are owned by the runner and cannot be
overridden by the configuration. Persisted commands redact API credentials,
authentication tokens, secrets, passwords, and database URL passwords while
retaining performance controls such as token budgets.

Optional top-level `service_metadata` contains JSON scalar values such as the
vLLM version, prefix-cache state, and MFU-metrics state. It is persisted in the
redacted manifest and therefore participates in resume compatibility checks.
Secret-like metadata keys are redacted.

```powershell
.conda\pg-ai-profile\python.exe code\scripts\run_ai_operator_scenarios.py `
  --config experiments\results\request_lifecycle_gate_20260725\scenario_config.json `
  --profiler code\scripts\postgres_ai_operator_profile.py `
  --python-executable .conda\pg-ai-profile\python.exe `
  --output-dir experiments\results\request_lifecycle_gate_20260725 `
  --health-url http://localhost:8000/health `
  --metrics-url http://localhost:8000/metrics `
  --idle-timeout-s 60
```

Single-GPU smoke configuration:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --data-source daft_postgres --source-order arrival_time --arrival-replay `
  --arrival-time-scale 0.0005 `
  --executor ray_actor --operator ai_complete `
  --model-backend compatible_http `
  --completion-endpoint-url http://localhost:8000/v1/completions `
  --model-metrics-url http://localhost:8000/metrics `
  --batching-policy token_budget --token-budget 6144 `
  --scheduling-policy static --max-inflight 8 `
  --flush-policy fixed_timeout --flush-timeout-ms 25 `
  --warmup-runs 1 --repeats 1 `
  --output experiments\results\arrival_replay_smoke\runs.csv
```

Run the same smoke once for `immediate`, `fixed_timeout`, and
`queue_adaptive`. Do not start formal repeats unless the run CSV,
request/submission trace, flush trace, control/resource time series, and
manifest are all non-empty. Contract tests and dry-runs do not satisfy this
gate.

`--batching-policy fixed_rows` preserves the original row-count batching path.
`--batching-policy token_budget --token-budget N` greedily forms upstream
submission batches using `prompt_tokens + completion_max_tokens` as the
estimated model cost. This only changes the Ray/vLLM submission units; it does
not modify vLLM continuous batching or Ray's internal scheduler. CSV rows record
`batching_policy`, `token_budget`, and `model_request_timeout_s`.

Length- and prefix-aware variants reorder rows before creating the upstream
submission batches:

```text
length_align_fixed_rows
length_align_token_budget
prefix_aware_fixed_rows
prefix_aware_token_budget
```

The length-aware variants sort by `prompt_tokens`. The prefix-aware variants
sort by `prefix_key`, then `prompt_tokens`. CSV rows record
`organization_policy_family`, `batch_prompt_token_spread_mean`, and
`prefix_group_ratio`. These are organization signals only; prefix-cache benefit
still requires APC/cache metrics or a controlled prefix-share workload.

`--scheduling-policy static` uses the configured `--max-inflight` as a fixed
admission window through the typed scheduler and Ray adapter for both task and
actor execution. `--scheduling-policy queue_adaptive` currently follows the
isolated legacy branch and polls the vLLM metrics
endpoint and switches between `--adaptive-min-inflight` and
`--adaptive-max-inflight` according to queue/running/KV thresholds. CSV rows
record `adaptive_downshifts`, `adaptive_upshifts`, and
`adaptive_limit_mean`.

`aimd`, `ewma_aimd`, and `pid` use the typed dynamic admission gate. They
require a Ray executor and `--model-metrics-url`. Sampling is cached and does
not sleep in the submission loop. If `--control-trace-output` is omitted, the
trace is written beside the main CSV with `_control_trace.csv` appended to the
stem. UCB is not a CLI choice yet: its policy/reward core is tested, but formal
online use still requires reward-epoch aggregation and a static-K8 reward
baseline. The request E2E/SLO trace needed for that aggregation is now
available.

Actor pools and task endpoints share the same routing configuration. Pool and
GPU lists contain one value per actor/endpoint. `request_cost` routing requires
an explicitly resolved long-request threshold (the tuning-workload P75), which
is stored in the run CSV. Multiple logical endpoints on GPU `0` validate
routing behavior only, not multi-GPU scaling.

The service endpoint and Ray actor worker counts are separate. Each HTTP
endpoint may have multiple client actors, so the configured actor concurrency
ceiling is:

```text
endpoint_count * actor_workers_per_endpoint * ray_actor_max_concurrency
```

HTTP client actors/tasks use `ray_worker_num_cpus` and always record
`ray_worker_num_gpus=0`; GPU ownership stays with the external vLLM service.
Formal completion retries remain disabled (`max_retries=0`,
`max_restarts=0`, and `max_task_retries=0`) so a completed request is not
silently duplicated. Main CSV rows record `ray_version`,
`actor_workers_per_endpoint`, `ray_actor_max_concurrency`,
`ray_worker_num_cpus`, `ray_worker_num_gpus`, `endpoint_count`,
`actor_worker_count`, and semicolon-separated
`actor_worker_submission_counts`. Python executor rows use an empty
`ray_version`, `ray_actor_max_concurrency=0`, and
`ray_worker_num_cpus=0.0` as explicit non-applicable sentinels. Ray task rows
have no actor workers, record effective task-worker CPU, and also use
`ray_actor_max_concurrency=0` because that field describes actors only.
Internally, task definitions still resolve safe `RayWorkerOptions`. Fake Ray
task/actor definitions now receive the same CPU, zero-GPU, and disabled-retry
options, but remain debug backends rather than HTTP workers. Multi-GPU
performance testing remains pending and must use independent GPU-backed
service endpoints rather than multiple logical URLs or actors aimed at one
endpoint.

`append_metrics` writes a header for a new or empty CSV. Before appending to a
non-empty CSV it reads the existing header and requires an exact ordered match
with the current row keys. A stale/legacy schema raises `ValueError` before any
bytes are appended; use a new output file or explicitly migrate the old CSV.

`run_kmax_interference_experiment.py` is a small orchestration wrapper around
`postgres_ai_operator_profile.py`. It starts a background bulk `AI_COMPLETE`
job and then starts a foreground small job against the same vLLM endpoint. Use
it when testing the admission-control motivation for `K_max`: bounded
background inflight versus unbounded background inflight under shared service.
It supports static background `K_max` sweeps, the legacy `queue_adaptive`
admission baseline, typed AIMD, deterministic per-repeat scenario shuffling,
independent request/submission/resource/flush/control traces, and selectable
fixed or queue-adaptive flush. Its default outputs are:

```text
experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_small_20260726.csv
experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_bulk_20260726.csv
```

脚本现在会拆分 `db_fetch_s` 与 `arrow_build_s`，支持普通 Python baseline，并且只在
`--executor ray_actor` 或 `--executor ray_task` 时按需导入 Ray。2026-07-12 增加了
OpenAI-compatible endpoint 后端，用于后续连接本地 vLLM、Ray Serve 或其他
GPU-backed model service。推荐参数名是 `compatible_http`，旧的 `http_openai`
只作为兼容别名保留。`fake` 仍是默认值，只能作为脚本调试、PG18.4 同构预演或
历史对照，不能写成 GPU-backed 结论。`AI_COMPLETE` 当前使用
`--completion-endpoint-url` 指向 vLLM-compatible `/v1/completions`，并将 JSON 文本写回
`document_completions`；也支持 `--model-backend ollama` 连接本地 Ollama
`/api/generate` 做 completion smoke。它还不是 token-aware/prefix-aware 策略实现。

示例命令：

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 4096 --total-rows 4096 `
  --db-fetch-rows 512 --ray-batch-rows 256 `
  --embedding-dim 128 --model-workers 2 --max-inflight 8 `
  --executor ray_actor --strategy coalesced `
  --warmup-runs 1 --repeats 3 `
  --experiment-id pg18_4_fake_4096 `
  --output motivation\results\pg18_4_fake\system_profile.csv
```

完整矩阵、CSV 位置与结果解释：

```text
motivation/results/pg18_4_fake/system_profile.md
```

GPU-backed embedding endpoint 配置检查示例：

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --dry-run `
  --executor ray_actor `
  --model-backend compatible_http `
  --embedding-endpoint-url http://localhost:8000/v1/embeddings `
  --embedding-model local-embedding `
  --experiment-id gpu_ai_embed_config_check `
  --output feasibility\results\gpu_ai_embed_config_dry_run.csv
```

AI_COMPLETE vLLM-compatible completion endpoint 配置检查示例：

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --dry-run `
  --operator ai_complete `
  --executor ray_actor `
  --model-backend compatible_http `
  --completion-endpoint-url http://localhost:8000/v1/completions `
  --completion-model local-llm `
  --completion-max-tokens 128 `
  --experiment-id ai_complete_config_check `
  --output feasibility\results\ai_complete_config_dry_run.csv
```

Local vLLM + Qwen2.5-1.5B startup on the current Windows/WSL Docker machine:

```powershell
docker run -d --name ai-operator-vllm-qwen --gpus all `
  -p 8000:8000 `
  --ipc=host `
  -e VLLM_WSL2_ENABLE_PIN_MEMORY=1 `
  -e VLLM_USE_V2_MODEL_RUNNER=0 `
  -v D:\Code\ai-operator-execution-optimization\models\Qwen2.5-1.5B-Instruct:/models/qwen:ro `
  vllm/vllm-openai:v0.25.1-cu129-ubuntu2404 `
  --model /models/qwen `
  --served-model-name qwen2.5-1.5b `
  --dtype auto `
  --max-model-len 2048 `
  --gpu-memory-utilization 0.75 `
  --enforce-eager
```

Minimal `AI_COMPLETE + Daft + vLLM` smoke:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 4 --total-rows 2 `
  --db-fetch-rows 2 --ray-batch-rows 1 `
  --operator ai_complete `
  --executor python `
  --model-backend compatible_http `
  --completion-endpoint-url http://localhost:8000/v1/completions `
  --completion-model qwen2.5-1.5b `
  --completion-max-tokens 8 `
  --data-source daft_postgres --organizer daft `
  --writeback-mode json_text `
  --experiment-id vllm_local_qwen15b_daft_ai_complete_smoke `
  --output tmp\vllm_local_qwen15b_ai_complete_smoke.csv
```

Controlled `AI_COMPLETE + Daft + Ray + vLLM` baseline workload:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\import_ai_complete_workload.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --workload-name sharegpt_burstgpt `
  --start-doc-id 1000000 `
  --max-rows 1024 `
  --batch-rows 500 `
  --tokenizer-path models\Qwen2.5-1.5B-Instruct `
  --max-model-len 2048 `
  --completion-max-tokens 16
```

Then profile the imported workload:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup `
  --total-rows 128 `
  --db-fetch-rows 128 --ray-batch-rows 8 `
  --operator ai_complete `
  --executor ray_task `
  --model-backend compatible_http `
  --completion-endpoint-url http://localhost:8000/v1/completions `
  --completion-model qwen2.5-1.5b `
  --completion-max-tokens 32 `
  --model-metrics-url http://localhost:8000/metrics `
  --source-workload-name sharegpt_burstgpt `
  --source-order doc_id `
  --data-source daft_postgres --organizer daft `
  --writeback-mode none `
  --experiment-id vllm_qwen15b_sharegpt_burstgpt_ray_task_batch_8 `
  --output experiments\results\local_vllm_qwen15b_baseline\sharegpt_burstgpt_ray_baseline.csv
```

AI_COMPLETE Ollama native completion smoke 示例：

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 4 --total-rows 2 `
  --db-fetch-rows 2 --ray-batch-rows 1 `
  --operator ai_complete `
  --executor python `
  --model-backend ollama `
  --completion-endpoint-url http://localhost:11434 `
  --completion-model qwen2.5:1.5b `
  --completion-max-tokens 16 `
  --data-source daft_postgres --organizer daft `
  --writeback-mode json_text `
  --experiment-id ollama_daft_ai_complete_smoke `
  --output tmp\ollama_ai_complete_smoke.csv
```

正式 GPU-backed 结果应输出到：

```text
motivation/results/gpu/ai_embed_profile.csv
```

只有在 `--model-backend compatible_http` 连接到真实 GPU-backed endpoint 时，结果才可放入
`motivation/results/gpu/`。

本地真实模型 endpoint 可用 `local_embedding_server.py` 启动：

```powershell
$env:HF_HOME="D:\Code\ai-operator-execution-optimization\.cache\huggingface"
$env:HF_HUB_CACHE="D:\Code\ai-operator-execution-optimization\.cache\huggingface\hub"
$env:TRANSFORMERS_CACHE=$env:HF_HUB_CACHE
$env:TORCH_HOME="D:\Code\ai-operator-execution-optimization\.cache\torch"

.conda\pg-ai-profile\python.exe code\scripts\local_embedding_server.py `
  --model .cache\models\all-MiniLM-L6-v2 `
  --device cuda `
  --batch-size 64 `
  --port 8000
```

该服务提供 OpenAI-compatible `/v1/embeddings` 接口，供
`postgres_ai_operator_profile.py --model-backend compatible_http` 调用。
2026-07-12 的首轮 GPU-backed profile 中，该 endpoint 是用户手动启动的。

## 2026-07-14 GPU key rerun

Latest GPU-backed key rerun after pgai SQL trigger-surface validation:

```text
motivation/results/gpu/pgai_integrated_key_rerun_20260714.md
motivation/results/gpu/ai_embed_pgai_integrated_key_20260714.csv
```

This rerun uses `local_embedding_server.py` on ports 8000 and 8001 with
`--device cuda`. It keeps pgai SQL surface validation separate from the
job-table GPU timing profile.

## 2026-07-14 pgvector(384) writeback support

`postgres_ai_operator_profile.py --setup --embedding-dim 384` now creates
`document_embeddings.embedding_vector` as `vector(384)`. If an old
`embedding_vector` column has a different dimension, the script drops and
recreates that column only; it does not delete Docker volumes or the
documents/job tables.

Latest GPU-backed sink comparison:

```text
motivation/results/gpu/pgvector_writeback_20260714.md
motivation/results/gpu/ai_embed_pgvector_writeback_20260714.csv
```

## 2026-07-26 Workload materialization and cost estimation

`import_ai_complete_workload.py` can use a live vLLM-compatible `/tokenize`
endpoint when a local tokenizer checkout is unavailable. Controlled-prefix
materialization clones complete rows, chooses an exact nested subset
deterministically, preserves the original prompt suffix, and fails rather than
truncating a row that exceeds the model context.

`estimate_operator_cost.py` fits a grouped held-out cost model from one or more
profile CSVs. It uses only pre-execution features and writes the feature schema,
split groups, coefficients, normalization values, and regression metrics to
JSON.
