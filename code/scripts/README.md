# PostgreSQL AI 算子画像脚本

## 文件定位

脚本按职责分为六组：

| 子目录 | 只负责 |
|---|---|
| `data/` | workload 导入 |
| `services/` | 本地调试服务 |
| `baselines/` | 原生 baseline/gate 薄入口 |
| `profiling/` | 数据链路画像与机制诊断 |
| `experiments/` | 场景、矩阵和多 job 正式编排 |
| `analysis/` | 离线汇总、代价估计和 calibration 选择 |

入口脚本只解析参数并调用 `src/`；不得因为移动目录而复制生产逻辑。历史结果目录里的
raw manifest 保留执行时旧路径作为不可变证据，README 中的复现命令使用当前新路径。

当前连接与测试流程集中在：

```text
code/scripts/profiling/postgres_ai_operator_profile.py
```

pgai SQL trigger-surface profile entry:

```text
code/scripts/profiling/pgai_sql_operator_profile.py
```

Daft text DataOrganizer smoke entry:

```text
code/scripts/profiling/daft_text_organizer_smoke.py
```

Shared-vLLM K_max interference runner:

```text
code/scripts/experiments/run_kmax_interference_experiment.py
```

Seeded scenario runner:

```text
code/scripts/experiments/run_ai_operator_scenarios.py
```

该 runner 在输出目录持有 `.runner-lease.json` 原子租约，禁止两个进程同时写
同一 manifest/CSV。中断恢复必须复用原配置和输出目录；只有确认旧 owner
已经消失后，才可同时传 `--resume --recover-stale-lease`。不要手工删除租约。

它既是当前 Phase 1 的实验驱动脚本，也是后续拆分正式 worker 之前的最小端到端实现。当前没有另一份隐藏的连接代码。

本目录只放实验主体、服务启动、数据采集和 profiling 入口。绘图、图表复现和素材筛选
脚本统一放在 `figures/scripts/`。

图像 CLIP 当前有三类不同入口，不能混读：

- `profiling/profile_image_clip_bottleneck.py`：历史 slow-pt 单进程阶段画像；
- `profiling/profile_clip_preproc_stages.py`：slow processor method-wrapper 诊断，未归因时间
  不能解释成具体转换步骤；
- `profiling/profile_image_clip_preprocess_variants.py`：当前 production-np、历史 legacy-pt、
  torchvision+PIL 和 torchvision+tensor-decode 的交错受控复测，经过同一
  `ClipTensorActor` 合同并做 embedding parity gate。它仍不是
  PG→Daft→Ray→pgvector E2E runner。

图像正式链路另有两个入口：

- `experiments/run_image_clip_e2e.py`：单个 vendor-native/diagnostic/project arm 的 operator-E2E、
  资源、正确性与 schema v10 原始记录；v10 在 unique/pass/processed rows 之外记录
  implementation provenance、scheduler owner 和 formal eligibility。Daft 内置
  `embed_image` 与 Ray Data native graph 可作 baseline；项目自写 Daft UDF formal
  默认拒绝，不能冒充官方实现；
  Daft built-in 只使用公开原生 `batch_size`，GPU 并发由 provider/Daft 推断，dtype
  记录为 `provider_default`，不会伪装成命令行 `--dtype` 已生效；
- `../configs/image_vendor_baselines.json`：固定 Daft 官方 image-classification
  benchmark 的 commit、入口 SHA256 与允许适配白名单；vendor-code parity 不通过
  项目 runner 重写其 batching、actor 或 backpressure；
- `experiments/run_image_clip_matrix.py`：读取 JSON 场景矩阵，用固定 seed 做 warmup + formal
  block 内交错，持有输出目录租约，并对 unique rows、exactly-once 与最小稳态时长
  fail closed。原始 CSV、逐 run manifest/stdout/stderr 和外层 schedule 必须保存在
  同一结果目录，不能只摘录汇总数字。

`data/import_coco_images.py` 同时支持 `--dir` 与 `--zip`；ZIP 模式直接顺序读取成员并在
单事务内写 PostgreSQL，不落地完整解压目录，适用于 COCO train 正式规模。
导入前强制表主键为 `(workload_name, doc_id)`；legacy 全局 `doc_id` 主键需先执行
`deploy/autodl/image_documents_workload_key.sql`，不能给某个 split 人工加 ID offset。

`profiling/profile_clip_transfer_ceiling.py` 是 H2D 机制诊断：R0 GPU-resident、R1 pinned
FP16、R2 pageable FP32 分别保存每个 batch/repeat 的 CUDA-event H2D、forward、
ownership copy 和同步 wall。它不含数据库/Daft/Ray queue，不能作为系统 E2E baseline。

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
| 批划分 | `ArrowOrganizer` / `DaftOrganizer` | 按策略决定 actor 输入粒度；Daft 后端通过 `code/src/data/materializers/text.py` 接入 |
| AI 算子 | `FakeEmbeddingActor` / `CompatibleHTTPEmbeddingActor` / `FakeCompletionActor` / `CompatibleHTTPCompletionActor` / `OllamaCompletionActor` | `fake` 只用于离线 smoke 和控制变量；`compatible_http` 用于 vLLM-compatible embedding 或 completion endpoint；`ollama` 用于本地 Ollama `/api/generate` completion smoke |
| 并发与反压 | `submit_ray_tasks` / `submit_with_backpressure` → `SynchronousScheduler` | 静态 task/actor 路径统一执行 K_max、路由、等待和 fan-in；旧 queue-adaptive 分支暂时隔离保留 |
| 数据写回 | `code/src/data/sinks/postgres.py::write_embeddings` / `write_completions` | embedding 支持 `none`、JSON 文本和 pgvector；completion 支持 `none` 和 JSON 文本 |
| 指标输出 | `code/src/observability/metrics/::preflight_metrics_schema` / `append_metrics` | 正式工作前用 dry-run keys 拒绝旧 schema；追加时要求已有 header 与当前 row keys 精确一致 |
| 场景单写者 | `code/src/infrastructure/runner_lease.py::acquire_runner_lease` | 原子占用输出目录，校验 owner、进程启动身份与 config fingerprint，显式记录 stale recovery |
| completion 粒度 | `profiling.replay::_service_quantum_envelopes` | 在 planning batch 内按预测 work 切完整行，分别生成 HTTP/Ray completion 与 credit 释放单元；不拆单行 prompt |
| actor worker pool | `ActorWorkerPoolSubmitter` / `RaySubmissionAdapter` | 每个 endpoint 显式限制 worker slots，按 round-robin 或 least-active-work 分配，completion/failure 后由 canonical handle 精确释放 |

## 当前本地运行

```bash
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/ai_operator" \
.venv/bin/python code/scripts/profiling/postgres_ai_operator_profile.py \
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
  256 slots 和 0.5 Ray CPU reservation，比
  1×256/2×128/4×64/8×32/16×16；按 97%-ceiling 选择最小 actor 数，
  16×16 只用于确认平台和方差；
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

`profiling/daft_text_organizer_smoke.py` is the smallest script-level entry for the
organizer abstraction in `code/src/data/materializers/text.py`. It does not connect to
PostgreSQL or vLLM; it verifies that text rows can pass through either
`ArrowOrganizer` or `DaftOrganizer` and return downstream Arrow batches. Use
`--runner ray` when checking Daft `into_partitions` or `repartition`;
NativeRunner reports these partition operations as no-op. The default output is
under `tmp/` because this is a local smoke result, not a formal experiment
result.

```powershell
.conda\pg-ai-profile\python.exe code\scripts\profiling\daft_text_organizer_smoke.py `
  --organizer arrow --rows 256 --batch-size 64 `
  --output tmp\daft_text_organizer_smoke.csv

.conda\pg-ai-profile\python.exe code\scripts\profiling\daft_text_organizer_smoke.py `
  --organizer daft --runner ray --rows 32 --batch-size 8 `
  --partition-mode into_partitions --partitions 4 `
  --output tmp\daft_text_organizer_smoke.csv
```

当前结果只证明 PostgreSQL 18.4 同构链路连通，不是公司 PostgreSQL 18.3
平台结果，也不是性能优化结论。

## 正式对照实验

2026-07-11 已为 `profiling/postgres_ai_operator_profile.py` 增加可重复对照实验参数：

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
- `trace_target_output` reads each row's `target_output_tokens` and caps the
  estimate at `completion_max_tokens`
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
.conda\pg-ai-profile\python.exe code\scripts\analysis\summarize_output_aware_bfd.py `
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

`experiments/run_ai_operator_scenarios.py` executes each profiler run in a separate
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
.conda\pg-ai-profile\python.exe code\scripts\experiments\run_ai_operator_scenarios.py `
  --config experiments\results\request_lifecycle_gate_20260725\scenario_config.json `
  --profiler code\scripts\profiling\postgres_ai_operator_profile.py `
  --python-executable .conda\pg-ai-profile\python.exe `
  --output-dir experiments\results\request_lifecycle_gate_20260725 `
  --health-url http://localhost:8000/health `
  --metrics-url http://localhost:8000/metrics `
  --idle-timeout-s 60
```

Single-GPU smoke configuration:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
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

`experiments/run_kmax_interference_experiment.py` is a small orchestration wrapper around
`profiling/postgres_ai_operator_profile.py`. It starts a background bulk `AI_COMPLETE`
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
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
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
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
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
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
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
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
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

Legacy 07-25..07-28 `AI_COMPLETE + Daft + Ray + vLLM` baseline workload (sharegpt_burstgpt, 1024 rows, doc_id 1000000). The CURRENT main workload is sharegpt_multiturn (2048 rows, doc_id 300000-302047, target_output 1-256) — substitute `--workload-name sharegpt_multiturn` / `--source-workload-name sharegpt_multiturn` (and the corresponding doc-id/row-count flags) in the commands below for new runs:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\data\import_ai_complete_workload.py `
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
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
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
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
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

本地真实模型 endpoint 可用 `services/local_embedding_server.py` 启动：

```powershell
$env:HF_HOME="D:\Code\ai-operator-execution-optimization\.cache\huggingface"
$env:HF_HUB_CACHE="D:\Code\ai-operator-execution-optimization\.cache\huggingface\hub"
$env:TRANSFORMERS_CACHE=$env:HF_HUB_CACHE
$env:TORCH_HOME="D:\Code\ai-operator-execution-optimization\.cache\torch"

.conda\pg-ai-profile\python.exe code\scripts\services\local_embedding_server.py `
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

This rerun uses `services/local_embedding_server.py` on ports 8000 and 8001 with
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

`data/import_ai_complete_workload.py` can use a live vLLM-compatible `/tokenize`
endpoint when a local tokenizer checkout is unavailable. Controlled-prefix
materialization clones complete rows, chooses an exact nested subset
deterministically, preserves the original prompt suffix, and fails rather than
truncating a row that exceeds the model context.

For a disjoint held-out suffix, `--max-prompt-tokens` expresses a workload
eligibility boundary independently from `max_model_len`, and
`--source-row-offset N` skips `N` rows only after all ShareGPT/BurstGPT,
prompt-token and tokenizer/context filters. Never use a new `start_doc_id`
alone: that would relabel the first prompts instead of selecting new prompts.
The safe append contract is:

1. reuse the verified raw-file hashes, tokenizer and explicit workload filter;
2. set `--source-row-offset N --start-doc-id N`;
3. set `--verify-existing-prefix-rows N --append-only --dry-run`;
4. remove only `--dry-run` after every field of doc IDs `0..N-1` matches;
5. keep `--append-only`, so any conflicting new doc ID aborts instead of
   updating existing rows.

Prefix verification is read-only and returns `status=verified_dry_run`.
Without an exact match, the importer fails before the suffix is written.

`analysis/estimate_operator_cost.py` fits a grouped held-out cost model from one or more
profile CSVs. It uses only pre-execution features and writes the feature schema,
split groups, coefficients, normalization values, and regression metrics to
JSON.

## 2026-07-29 Shared-vLLM multi-job runner

`experiments/run_shared_vllm_experiment.py` is the shared-endpoint multi-job group runner. Unlike
`experiments/run_ai_operator_scenarios.py`, one scheduled run contains multiple concurrent
profiler processes. It requires one explicit Ray address, gives every job an
  independent summary/request/submission trace, records group-level vLLM/resource
  metrics and MFU once, and uses one uniquely named Ray credit actor for
  `shared_drr`. A common replay epoch plus lateness/skew checks prevents startup
  jitter from becoming a hidden fairness variable. Durable per-group records
  rebuild the compact CSV on resume instead of appending duplicate rows.

Committed templates:

- `deploy/autodl/dual_gpu_shared_vllm_gate.example.json`
- `deploy/autodl/dual_gpu_shared_vllm_formal.example.json`
- `deploy/autodl/dual_gpu_shared_vllm_j4_gate.example.json`
- `deploy/autodl/dual_gpu_shared_vllm_j4_formal.example.json`

The AutoDL formal template runs 1/2/4 jobs after a separate four-job gate.
The former `ray_task` path expanded to more than 200
Ray workers and exhausted the container's `vm.max_map_count=65530`. Shared
multi-job templates now use one persistent async actor per endpoint per job;
the loader rejects an explicit four-or-more-job `ray_task` configuration before
any output directory or external request is created. The j4 gate must pass
before the 1/2/4 formal template or the j4-only isolation template is eligible.

The config must not contain `--setup`, reset, output/trace, Ray-address, or
credit flags. The runner owns them so concurrent jobs cannot race schema setup,
append to one CSV, or silently connect to different Ray clusters. Use the full
startup, gate, resume, evidence-preservation, and cleanup procedure in
`deploy/autodl/README.md`.

## 2026-07-29 同条件官方 baseline 入口

`baselines/run_official_baseline.py` 为同条件 baseline 提供薄执行入口。它不决定实验
矩阵，只执行已经固定的 manifest shard，并把不同实现统一到
`requests.csv + summary.json`：

- `bounded_http`：无 Daft/Ray 的强 AsyncIO 因果对照；其 httpx
  `max_connections/max_keepalive_connections` 显式等于全部 endpoint 的配置
  并发总量，禁止由客户端默认连接池暗中截断 C128/C256；
- `vllm_bench`：官方 serving ceiling，先保存详细原始结果，再显式归一化；
- `daft_native` / `daft_ray`：官方 `daft.functions.prompt()`；
- `ray_data_http`：官方 Ray Data HTTP Processor；
- `oceanbase`：仅在 OceanBase CE 能力门禁通过后启用。

所有 arm 使用一行一个完整 Chat Completions 请求、`temperature=0`、
相同模型和相同输出上限。两张卡先通过不可变 manifest 做
largest-work-first 固定分片；执行器不得自行重新洗牌。示例：

```bash
python code/scripts/baselines/run_official_baseline.py export-postgres-manifest \
  --database-url "$DATABASE_URL" \
  --workload-name "$SOURCE_WORKLOAD_NAME" \
  --row-count 64 \
  --row-offset 0 \
  --max-output-tokens 256 \
  --estimated-output-mode trace_target \
  --endpoint-count 2 \
  --output /root/autodl-tmp/gates/official_baseline_gate_manifest.jsonl

python code/scripts/baselines/run_official_baseline_gate.py \
  --config deploy/autodl/dual_gpu_official_baseline_gate.example.json \
  --driver-python /root/miniconda3/bin/python \
  --vllm-python /root/autodl-tmp/venvs/vllm-4090/bin/python \
  --manifest /path/to/manifest.jsonl \
  --output-root /path/to/fresh-gate-output
```

校准时不得复制或临时改写远端 JSON。只选择已提交配置中的实验臂，并显式覆盖
各 arm 的 per-endpoint concurrency；例如在同一份 256 行 manifest 上运行
vLLM Bench 与 bounded HTTP 的 C64：

```bash
python code/scripts/baselines/run_official_baseline_gate.py \
  --config deploy/autodl/dual_gpu_official_baseline_gate.example.json \
  --driver-python /root/miniconda3/bin/python \
  --vllm-python /root/autodl-tmp/venvs/vllm-4090/bin/python \
  --manifest /path/to/immutable-256-row-manifest.jsonl \
  --rows-total 256 \
  --output-root /path/to/fresh-c64-output \
  --include-cell vllm_bench \
  --include-cell bounded_http \
  --concurrency-override vllm_bench=64 \
  --concurrency-override bounded_http=64
```

每个更高并发档必须使用新的输出根目录。未知 cell、重复或非正并发、以及对未选
cell 的覆盖都会在启动请求前失败；`resolved_config.json` 保存最终选择和有效
并发。C64/C128 是校准压力点，不是默认值，更不能据单次 gate 直接得出正式
性能结论。

`baselines/run_official_baseline_gate.py` 是可复现的双 endpoint core gate runner：
每个 cell 都先同时启动两个 shard，再等待二者完成；逐 endpoint 保存命令与
日志，轮询 vLLM queue 归零后才归一化和执行 gate。任一 shard、归一化或 gate
失败都立即停止后续 cell，写 `run_status.json` 并保留现场；输出根目录已存在
时拒绝运行。`project_profiler` cell 显式记录为 blocked，仍由现有 profiler
执行，不能被 core runner 中的近似实现替代。
vLLM Bench 必须从独立 vLLM venv 启动，并在该 venv 安装与服务完全同版本的
`vllm[bench]` extra；仅安装 serving 包会在 CustomDataset 读取阶段失败。

`run_official_baseline.py --dry-run` 仍用于单 shard 接口检查且不创建输出目录。
单 cell 校验由 `validate-gate` 合并两份 summary 和 request CSV；任一
exactly-once、预测 work 偏斜、endpoint 未使用、服务元数据不一致、worker
failure 或 vLLM 最终队列非空都会 fail closed。

配置边界见：

- `deploy/autodl/dual_gpu_official_baseline_gate.example.json`
- `deploy/autodl/dual_gpu_official_baseline_calibration.example.json`
- `deploy/autodl/dual_gpu_same_condition_project_equivalence_gate.example.json`
- `experiments/plans/baseline_reference.md`
- `experiments/plans/text_native_baseline_rerun_20260802.md`

模板是预注册规格，不是允许远端临时拼接 formal 命令的替代品。64 行 gate
通过前不得启动 calibration；calibration 通过前不得启动 2,048 held-out。

Project profiler 的 512 行 broad calibration 之前还有一道更窄门禁：
`static_k256` 与 `work98304_nonbinding` 各运行一次同压力 warm-up 和三次
formal repeat。actor-ready barrier 不计入 measured E2E，耗时写入
`actor_ready_s`；submission trace schema 5 记录 HTTP request、headers 和
body-read 边界。两臂 throughput/JCT 没有收敛到 5% 内时必须停止，不能以
单次最佳结果选择参数。

`analysis/select_strategy_calibration.py` 把通过门禁的 Completions feeding、
direct bounded gate、token-budget 和同协议 actor-shape formal CSV 合并为
`selection.json + calibration.env`。它按 95% feeding parity、
至少三次 formal repeat、97%-ceiling 和下一档增益小于 3% 的预注册规则冻结
token budget、per-endpoint K、active work；actor shape 在总 slots 固定时
选择达到峰值 97% 的最小 actor 数。后续
data-organization、submission-policy 和 shared-vLLM formal runner 会核对
该选择文件；旧 8K/K64、缺失 actor-pool 证据或环境漂移会在外部请求前失败。

`analysis/summarize_static_k_workload_surface.py` 读取
`dual_gpu_static_k_workload_surface.example.json` 的 formal CSV，先用
95% capacity floor 排除欠喂点，再按 SLO goodput（缺失时用 JCT）选择各
workload 的静态 K。只有最佳 K 至少迁移 2×或 97% 可接受集合不重叠、错配
损失至少 5%，且至少 2/3 paired repeats 同向时才输出 `passed`。
`--require-pass` 在不存在动态优化空间时返回 2，供远端 runner fail closed。

`analysis/summarize_static_credit_workload_surface.py` 用于 prompt 长度等 workload
变化下的 request/work credit 审计。输入为重复的
`--surface workload=/path/to/runs.csv`，统一输出 formal 中位数、均值、CV、
SLO goodput/JCT、observed/configured limit、无准入压力标志和交叉 regret。
如果候选臂 CV 超过 5%、未绑定等价臂相差超过 5%，或缺少 per-request output
token IDs，结果固定为 `inconclusive`，不能用算术平均表触发 adaptive
GO/NO-GO。07-30 short/long screening 正是因这些审计失败而被降级。
