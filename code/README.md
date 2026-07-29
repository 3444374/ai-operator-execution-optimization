# Project Code

Current implementation flow, completed mechanisms, evidence boundaries, and
remaining work are summarized in `code/INFRA_STATUS.md`.

本目录存放可以迁移到正式课题工程的代码。一次性 benchmark 仍放在 `feasibility/benchmarks/` 或 `motivation/benchmarks/`。

绘图、图表复现和素材筛选脚本统一放在 `figures/scripts/`；本目录优先保留实验主体代码、服务入口和 profiling 驱动。

## 目录结构

```
code/
├── scripts/
│   ├── postgres_ai_operator_profile.py   ← PostgreSQL AI 算子链路画像（Ray actor + GPU endpoint + writeback）
│   ├── run_shared_vllm_experiment.py     ← 1/2/4-job 共享 vLLM 正式 group runner
│   ├── pgai_sql_operator_profile.py      ← pgai SQL 触发面画像（ai.ollama_embed via pgai 扩展）
│   ├── local_embedding_server.py         ← 本地 OpenAI 兼容 embedding 服务（Ollama）
│   ├── daft_text_organizer_smoke.py      ← Daft 文本 DataFrame / into_batches / Ray runner smoke
│   └── summarize_output_aware_bfd.py     ← BFD 正式重复实验的长表统计汇总
├── configs/                              ← 后续工程配置文件（当前为空）
├── src/
│   ├── sources.py                        ← PostgreSQL/Daft 数据入口后端
│   ├── organizers.py                     ← ArrowOrganizer / DaftOrganizer 数据组织后端
│   ├── request_costs.py                   ← 严格的输出成本模式与来源标签
│   ├── shared_vllm_experiment.py          ← 多 job 配置、并发编排、组级指标与公平性验证
│   ├── packing.py                         ← 与模态无关的确定性 BFD 与 row-cap-first 候选
│   ├── runner_lease.py                    ← 场景输出目录的原子单写者租约与显式 stale recovery
│   ├── model_backends.py                 ← fake / compatible HTTP embedding and completion backend
│   ├── sinks.py                          ← none/json_text/pgvector embedding 写回 + completion JSON 写回
│   ├── metrics.py                        ← timing / GPU snapshot / CSV metrics helper
│   ├── profiling/
│   │   ├── cli.py/config.py              ← profiler 参数面与环境解析
│   │   ├── schema.py/traces.py           ← 正式汇总/trace CSV 契约
│   │   ├── replay.py                     ← Arrow envelope、arrival replay 与生命周期种子
│   │   └── ray.py                        ← Ray 提交、typed scheduler 与 credit/fan-in
│   ├── profile_*.py                      ← 旧导入路径的薄兼容模块
│   ├── workloads.py                      ← 内置 synthetic / controlled workload seed
│   └── scheduling/
│       ├── organization/                 ← token-budget policy + complete-row service-quantum slicer
│       ├── submission_control/           ← request/work admission and shared fair credit
│       ├── endpoint_routing/             ← round-robin/queue/work/prefix routing
│       ├── runtime/                      ← Ray adapters and cached service observations
│       └── models.py/scheduler.py/...    ← shared typed core and compatibility imports
├── tests/
│   ├── test_sources.py                   ← 数据入口后端最小单元测试
│   ├── test_organizers.py                ← 数据组织后端最小单元测试
│   ├── test_request_costs.py              ← 输出成本语义测试
│   ├── test_packing.py                    ← BFD membership/指标测试
│   ├── test_output_aware_summary.py       ← 正式结果长表汇总测试
│   ├── test_kmax_interference_script.py  ← K_max runner 默认输出 schema 版本测试
│   ├── test_token_budget_controller.py   ← 动态预算安全动作与 EWMA 测试
│   ├── test_shared_credit.py              ← 多 job shared-credit 纯策略测试
│   ├── test_shared_credit_ray.py          ← named Ray actor ownership 测试
│   ├── test_runner_lease.py               ← runner 活跃 owner、stale recovery 与 fingerprint 门禁
│   ├── test_model_backends.py            ← 模型后端最小单元测试
│   ├── test_sinks.py                     ← 写回后端最小单元测试
│   ├── test_workloads.py                 ← 内置 workload seed 单元测试
│   └── test_import_ai_complete_workload.py ← ShareGPT/BurstGPT importer 单元测试
└── requirements.txt                      ← Python 依赖（numpy, pyarrow<25, ray, psycopg, daft, torch, transformers）
```

安装依赖：

```bash
pip install -r code/requirements.txt
pip install -r code/requirements-dev.txt
ruff check code
```

`pyproject.toml` 先启用仓库现状能够全量通过的 correctness lint。完整 import
排序和 `ruff format` 将与 profiler 的后续模块拆分一起推进，避免在
机制修复提交中混入大面积无语义格式 diff。新代码仍按 100 列、4 空格缩进和
双引号格式编写。

## PostgreSQL data source backends

`code/src/sources.py` defines the data entry boundary used by
`code/scripts/postgres_ai_operator_profile.py`:

- `arrow_postgres`: baseline psycopg read plus Arrow table construction.
- `daft_postgres`: Daft `read_sql` PostgreSQL entry; it requires `sqlglot` and
  `connectorx`.

Data entry can use either the baseline psycopg path or Daft:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --dry-run --data-source daft_postgres --organizer daft `
  --output tmp\postgres_profile_dry_run.csv
```

Current writeback remains `none`, `json_text`, or `pgvector`. Lance is a future
optional sink backend and is not part of this implementation.

## Runtime code boundaries

The main profiling script should stay as orchestration code. Reusable behavior
now lives under `code/src/`:

- `sources.py`: PostgreSQL/Daft data entry.
- `organizers.py`: Arrow/Daft batch organization.
- `request_costs.py`: strict, shared output-cost modes and provenance.
- `packing.py`: deterministic, modality-neutral classic BFD and a
  row-cap-first placement candidate sharing the same validation and ordering.
- `model_backends.py`: `fake` debug backend, `compatible_http` embedding/completion backend, and Ollama native completion backend.
- `sinks.py`: existing PostgreSQL embedding writeback modes plus `document_completions` JSON-text writeback.
- `metrics.py`: timers, GPU/memory/power sampling, energy/MFU estimates, and
  schema-safe CSV preflight/append helpers. Formal runs preflight the main
  output against dry-run keys before database or GPU work. Empty files receive
  a header; non-empty files reject appended rows whose ordered keys do not
  exactly match the existing header.
- `profiling/traces.py`: versioned control/flush/submission/request/resource CSV
  serializers. Control schema 2 records the actual `hol_age_s` input;
  submission schema 3 uses scheduler lifecycle IDs and records pool,
  endpoint, GPU, status, and error instead of synthesizing batch IDs for
  request-granularity runs.
- `profiling/cli.py`: the profiler command-line surface only; it does not start
  Ray, connect to PostgreSQL, or inspect the environment beyond argument
  defaults.
- `profiling/config.py`: post-parse endpoint/metrics precedence and Ray worker
  resource resolution. Explicit CLI values win over plural/single environment
  defaults.
- `profiling/schema.py`: the ordered formal summary-row contract and schema-drift
  guard, separate from runtime orchestration.
- `profiling/replay.py`: offline/replayed Arrow envelope construction,
  token-budget row grouping, batch/request submission expansion, and request
  lifecycle seed assembly. It does not submit Ray work or call model services.
- `profiling/ray.py`: Ray task/actor submitters, endpoint topology, typed
  scheduler wiring, credit release/fan-in, and the explicitly retained legacy
  adaptive baselines. It does not parse CLI arguments or write trace CSVs.
- `workloads.py`: small built-in seed workloads for smoke/dev only.
- `scheduling/`: engine-independent typed core split by decision boundary:
  `organization/`, `submission_control/`, `endpoint_routing/`, and `runtime/`.
  Thin modules at the old root import paths preserve existing callers while new
  code imports the owning subpackage. The formal payload/execution path remains
  Daft -> Arrow -> Ray.

`fake` is retained only as a local control backend for offline smoke tests and
pipeline debugging. It is not a model-service result source. For vLLM-compatible
experiments, use `--model-backend compatible_http`; the older `http_openai`
name is accepted only as a compatibility alias. `AI_COMPLETE` is selected with
`--operator ai_complete` and expects a vLLM-compatible `/v1/completions` URL
through `--completion-endpoint-url`. For local Ollama smoke runs, use
`--model-backend ollama --completion-endpoint-url http://localhost:11434`.

## Scheduling foundation

`code/src/scheduling/` contains immutable request metadata, endpoint topology,
data-organization policies, request/work admission, shared multi-job credit,
endpoint routing, a deterministic policy-composition scheduler, and Ray runtime
adapters. Policy modules do not import Daft, Arrow, Ray, or HTTP; only runtime
adapters receive the active Ray module explicitly.
Arrival replay is produced through a one-element bounded queue so waiting for
the next source arrival cannot block Ray completion collection. The scheduler
keeps submit/routing/credit/lifecycle state on its main thread: it prioritizes
an already-ready arrival, polls `ray.wait(timeout=0)` during arrival gaps, and
uses blocking collection only when admission or active-work capacity is full.
This preserves offered-load saturation while releasing request-level credit
and recording completion timestamps promptly under sparse or bursty replay.
The lifecycle module joins complete-row replay seeds, immutable submission
events, backend service timestamps, and explicitly sourced token counts into
exactly-once request trace rows.

The formal framework remains:

```text
PostgreSQL -> Daft -> Arrow payload boundary -> Ray task/actor -> endpoint
```

The synchronous fake adapter used in unit tests is not a formal execution path.
The profiler's static `ray_task` and `ray_actor` paths now delegate to the
typed scheduler through `RaySubmissionAdapter`. The Daft-to-Ray contract tests
use a real `DaftOrganizer`, Arrow batches, and local single-node Ray task and
actor execution:

A model-service endpoint is not a Ray actor worker. One endpoint is an
independently addressed HTTP service; `--actor-workers-per-endpoint` creates
that many Ray HTTP client actors for each endpoint. The configured upper bound
on simultaneous actor method execution is:

```text
effective actor concurrency =
  endpoint_count * actor_workers_per_endpoint * ray_actor_max_concurrency
```

These HTTP client workers reserve CPU but explicitly reserve `0` Ray GPUs,
because the external model-service endpoint owns the GPU. Ray task retries,
actor restarts, and actor task retries remain disabled for formal completion
accounting. The profiler CSV records the Ray version, resolved worker/resource
settings, service endpoint count, actor worker count, and per-worker submission
counts. Python executor rows leave `ray_version` empty and use the explicit
non-applicable sentinels `ray_actor_max_concurrency=0` and
`ray_worker_num_cpus=0.0`. Ray task rows record their effective
CPU reservation but use `ray_actor_max_concurrency=0`, because an actor-only
constraint is not applicable to a task. Internally the task worker options
still use their safe task definition. Fake Ray task/actor definitions receive
the same CPU, zero-GPU, and retry/restart options as other Ray workers; this
does not make the fake backend an HTTP or performance path.
Endpoint-local worker rotation and the legacy endpoint round-robin submitter
are created once per run, so fetch chunks do not reset either position.
Per-worker metrics remain chunk-local deltas for correct run-level merging.
Jobs created before Ray initialization or submission failures are marked
`failed` without masking the original exception.

The bounded actor submitter owns explicit per-worker slots and supports stable
round-robin or least-active-work assignment. A canonical Ray handle releases
the selected worker exactly once after either success or failure. Summary rows
record configured/effective slots, per-worker peaks/failures and slot-held
utilization; submission trace schema 4 records planning-batch, service-quantum,
worker identity, credit-held and Ray-to-service timing. Slot-held utilization
includes queue, HTTP and service time and must not be interpreted as GPU
compute utilization.

`submission_granularity=service_quantum` uses the same complete-row slicer in
offline and arrival-replay paths. It preserves planning-batch identity while
allowing smaller HTTP/Ray completions to release active-work credit
independently; an oversized row remains intact and is explicitly marked.

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_scheduling_models.py
.conda\pg-ai-profile\python.exe code\tests\test_scheduling_policies.py
.conda\pg-ai-profile\python.exe code\tests\test_scheduler.py
.conda\pg-ai-profile\python.exe code\tests\test_ray_adapter.py
.conda\pg-ai-profile\python.exe code\tests\test_adaptive_admission.py
.conda\pg-ai-profile\python.exe code\tests\test_dynamic_admission.py
.conda\pg-ai-profile\python.exe code\tests\test_flush_policies.py
.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
.conda\pg-ai-profile\python.exe code\tests\test_scheduling_daft_ray_contract.py
.conda\pg-ai-profile\python.exe code\tests\test_token_budget_controller.py
.conda\pg-ai-profile\python.exe code\tests\test_shared_credit.py
.conda\pg-ai-profile\python.exe code\tests\test_shared_credit_ray.py
```

Typed AIMD, optional EWMA-AIMD, and PID controllers can now drive the same Ray
task/actor scheduler. Service metrics are sampled by a background provider, so
network scrapes never run on the admission-decision path. Stale samples do not
update controller state; every decision can be written to a control-trace CSV
with the exact sample age. The legacy `queue_adaptive` branch remains an
explicit baseline. UCB has a tested finite-action policy and SLO-aware reward,
but is not exposed in the profiler until epoch-level request metrics are
available. None of these code tests is evidence of a throughput or latency
improvement.

Static token budget remains the default. The optional `service_quantum`
controller chooses only from an offline-calibrated action set and changes by at
most one step per closed batch using arrival/service-rate feedback. Endpoint
admission can additionally bound estimated prompt+output active work, so
different batch sizes and request lengths do not consume identical credits.
Least-work routing uses predicted outstanding work rather than request count.
For concurrent database jobs, an optional named Ray actor owns shared
per-endpoint request/work credits and applies deficit-round-robin fairness with
work-conserving borrowing. These mechanisms are implemented and unit-tested but
remain GPU-unvalidated until the staged dual-GPU matrices complete.

Data organization exposes `token_budget` (sequential),
`best_fit_token_budget` (classic BFD), and
`row_cap_aware_token_budget` (decreasing order with row-cap-first placement).
All three enforce the same token and row constraints, and Arrow/Daft call the
same pure packing functions. Sequential remains the default until repeated
GPU experiments show that another mechanism generalizes.

Pool and endpoint routing support request-cost pools, least-queued selection,
deterministic prefix affinity, health fallback, and explicit per-endpoint pool
and GPU identifiers. The independent flush policy core supports immediate,
fixed-timeout, queue-adaptive, and SLO-aware EWMA decisions with a hard maximum
wait. `slo_ewma` smooths arrival and per-endpoint service rates, floors the
service denominator with a capacity value calibrated by the saturated-work
curve, interpolates the wait window around aggregate load ratio 1.0, and
enforces oldest-request SLO slack plus hysteresis. Missing/stale feedback falls
back to the configured maximum fixed window.
Least-queued routing includes scheduler-local in-flight submissions instead of
reusing the profiler's static startup snapshot. Multi-endpoint vLLM runs should
pass every Prometheus endpoint through `--model-metrics-urls`; counters and
request gauges are aggregated across endpoints, while KV-cache pressure uses
the maximum endpoint value. GPU snapshots aggregate all visible devices.
`--source-order arrival_time` only defines source order; it does not reproduce
arrival gaps. Online flush experiments must additionally enable
`--arrival-replay`. The replay path normalizes the first arrival to zero,
preserves subsequent gaps with a monotonic clock, builds complete-row pending
batches, and writes a separate flush trace. Batching decides which rows belong
together, flush decides when a partial batch closes, and admission limits how
many submissions may be in flight. The default
`--submission-granularity batch` preserves the historical multi-row HTTP call.
`--submission-granularity request` keeps the same organization and flush
boundaries but expands each closed group into complete one-row HTTP requests,
so every completed request immediately releases one admission slot and the
scheduler can continuously replenish vLLM. In request mode, `max_inflight`
counts requests rather than multi-row batches; results from the two modes must
not be compared as if the same numeric K represented the same offered load.

Optional request tracing records one client-observed row per prompt with
explicit request- or submission-granularity completion timing and SLO metrics.
Epoch timestamps are
derived from a single wall-clock anchor plus monotonic elapsed time. Aggregate
endpoint token usage is never divided across prompts. For vLLM, the explicit
`--completion-return-token-ids` opt-in records genuine per-choice output-token
counts and finish reasons; generic compatible endpoints keep the extension
disabled and those fields remain unavailable.
Backend service epochs are marked as a separate clock domain; cross-domain
submit-to-service time remains empty when the clocks cannot be ordered.
Request trace schema 3 retains the explicit `submission_id` join key and adds
`finish_reason`; consumers do not reconstruct identity from row position.

Typed adaptive controllers consume each asynchronous vLLM metrics sample at
most once. A still-recent cached sample remains visible for diagnostics but is
marked non-fresh after its first decision, preventing AIMD/EWMA/PID from
reapplying one scrape at scheduler-loop speed. The HOL-age controller uses the
configured sample interval as its control clock; its current signal is still
the age of the oldest in-flight submission and should not be described as pure
pre-submit queueing delay.

`code/src/experiment_scenarios.py` and
`code/scripts/run_ai_operator_scenarios.py` provide deterministic formal-run
interleaving, per-run service-idle gates, failure incidents, redacted commands,
and an atomically updated manifest. Scenario argument strings may reference
explicit `${ENV_NAME}` values; an unset variable fails before health checks or
profiler subprocesses, so model, endpoint, database, and dataset-facing config
can change without editing Python. Each profiler subprocess uses explicit
phase/repeat identity and writes isolated request/submission/time-series files.

The real contract test covers Daft `RecordBatch`/Arrow conversion and local Ray
task/actor exactly-once execution. It is integration evidence only. GPU
performance claims require the real PostgreSQL source and vLLM endpoint.

## AI_COMPLETE workload import

Final comparable AI_COMPLETE baselines should use the normalized
ShareGPT/BurstGPT workload instead of the legacy synthetic seed. Raw payloads
live under `data/raw/` and are ignored by git; see `data/README.md`.

Import 1024 local rows without clearing existing `documents` rows:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\import_ai_complete_workload.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --workload-name sharegpt_burstgpt `
  --start-doc-id 1000000 `
  --max-rows 1024 `
  --batch-rows 500
```

Run the profiling script against this workload with:

```powershell
--source-workload-name sharegpt_burstgpt
```

## Daft 文本组织 smoke

当前 Daft 接入的项目代码在 `code/src/organizers.py`：`ArrowOrganizer` 是 baseline 后端，`DaftOrganizer` 是文本阶段 Daft DataFrame 后端。独立 smoke 入口只负责验证 `rows -> Arrow Table -> organizer -> batches`，并可显式切换 Ray runner 验证 `into_partitions`。这不是正式性能实验，不写入 `motivation/results/gpu/`。

```powershell
.conda\pg-ai-profile\python.exe code\scripts\daft_text_organizer_smoke.py `
  --organizer daft --runner ray --rows 32 --batch-size 8 `
  --partition-mode into_partitions --partitions 4 `
  --output tmp\daft_text_organizer_smoke.csv
```

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_organizers.py
```

## PostgreSQL AI 算子链路画像

当前新增入口：

```text
code/scripts/postgres_ai_operator_profile.py
```

目标是采集 PostgreSQL 18 触发 AI 算子后的外部执行链路画像；当前本地运行
18.4，最终目标平台为公司内部 18.3：

```text
PostgreSQL documents/job table
  -> external worker
  -> ArrowOrganizer / DaftOrganizer
  -> Ray actor fake or compatible HTTP AI_EMBED / AI_COMPLETE
  -> bounded backpressure
  -> fan-in
  -> writeback document_embeddings / document_completions
```

当前本机已通过 Docker 运行 PostgreSQL 18.4 + pgvector 0.8.2 同构预演实例，
数据库、扩展和向量查询已经验证；WSL `.venv` 已安装 Ray、PyArrow、NumPy
和 psycopg，并完成 256 行 PostgreSQL -> Arrow -> Ray actor -> fake embedding
-> PostgreSQL 写回冒烟运行。CSV 位于
`feasibility/results/pg18_4_connection_smoke_256_rows.csv`（及 `pg18_4_connection_smoke_runs.csv`），完整记录见
`feasibility/results/pg18_4_connection_validation.md`。
脚本内部连接、读取、Ray 执行和写回函数的对应关系见 `scripts/README.md`。

最小 dry-run：

```bash
.venv/bin/python code/scripts/postgres_ai_operator_profile.py \
  --dry-run \
  --output feasibility/results/postgres_ai_operator_profile_dry_run.csv
```

连接当前本地同构 PostgreSQL 实例：

```bash
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/ai_operator" \
.venv/bin/python code/scripts/postgres_ai_operator_profile.py \
  --setup \
  --seed-rows 10000 \
  --total-rows 10000 \
  --db-fetch-rows 1024 \
  --ray-batch-rows 512 \
  --model-workers 2 \
  --max-inflight 8 \
  --strategy coalesced \
  --organizer arrow \
  --output feasibility/results/pg18_4_connection_smoke_256_rows.csv
```

Daft organizer dry-run:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --dry-run --executor python `
  --organizer daft --daft-runner native `
  --output tmp\postgres_profile_dry_run.csv
```

真实报告必须说明数据库平台类型：

- PostgreSQL 18.3 内部验证平台；
- 普通 PostgreSQL + pgvector 同构预演替身；
- 其他开源数据库 AI 算子平台。
