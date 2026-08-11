# Project Code

Current implementation flow, completed mechanisms, evidence boundaries, and
remaining work are summarized in `code/INFRA_STATUS.md`.

全项目代码分层、文本/图像模态边界与分阶段迁移计划见
[`ARCHITECTURE_REFACTOR_PLAN.md`](ARCHITECTURE_REFACTOR_PLAN.md)。`src/` 的职责分层、
文本/图像模态隔离、baseline 分层、旧兼容入口清理，以及 metrics、model backend、
shared-vLLM 三个大文件的语义拆分，以及 scripts/tests 镜像整理已合入 `main`。

本目录存放可以迁移到正式课题工程的代码。一次性 benchmark 仍放在 `feasibility/benchmarks/` 或 `motivation/benchmarks/`。

绘图、图表复现和素材筛选脚本统一放在 `figures/scripts/`；本目录优先保留实验主体代码、服务入口和 profiling 驱动。

## 目录结构

```
code/
├── src/
│   ├── data/                     ← PostgreSQL/Daft source、Arrow/Daft materializer、sink、workload
│   ├── planning/                 ← 纯 cost estimator 与 work-unit packing；不依赖执行引擎
│   ├── scheduling/
│   │   ├── core/                 ← typed state、capacity contract、execution ledger、scheduler loop
│   │   ├── organization/         ← pending batching、token/work budget、service quantum
│   │   ├── submission_control/   ← request/work credit、legacy baselines、SAOR policy/ordered release
│   │   ├── endpoint_routing/     ← pinned/queue/work/prefix routing
│   │   └── runtime/              ← Ray adapter 与服务观测缓存
│   ├── serving/                  ← completion/embedding backend 与 vLLM probe
│   ├── modalities/
│   │   ├── text/                 ← prompt/output-token work 语义
│   │   └── image/                ← encoded bytes/tensor/CLIP/source/audit 合同
│   ├── observability/            ← metrics 与 profiler 配置/replay/trace/Ray 接线
│   ├── baselines/
│   │   ├── common/               ← manifest/result/provenance/validity gate
│   │   ├── text/                 ← ceilings/controls/frameworks/products/orchestration
│   │   └── image/                ← provenance 与 Daft/Ray Data native graph
│   ├── experiments/              ← calibration、scenario、shared-vLLM 编排
│   └── infrastructure/           ← config env、机器/资产合同、runtime env 与 runner lease
├── scripts/
│   ├── data|services|baselines/  ← 数据导入、服务入口、原生 baseline runner
│   └── profiling|experiments|analysis|environment/ ← 画像、编排、分析、跨机器检查
├── tests/                        ← 按生产域镜像；远端完整依赖环境当前 622 个 unittest
├── configs/                      ← vendor baseline pin 与可复现实验配置
└── requirements.txt
```

跨机器运行不要直接对着 `requirements.txt` 反复试装。先按
`deploy/runtime/README.md` 选择 machine profile 与能力组，保存只读 preflight 报告；
可选能力分别维护在 `requirements/`，例如 learned estimator 与通用下载工具不会被
强制装入每个 driver 或 vLLM 环境。

图像 baseline 的来源合同由 `src/baselines/image/provenance.py` 统一维护。正式 native
baseline 当前为 Daft 内置 `embed_image` 和由 Ray Data 自己调度的官方
`read_sql → map_batches(CPU) → map_batches(GPU)` graph；项目自写的 Daft fused/staged
UDF 只保留为机制诊断 reference。所有 runner CSV/manifest 必须记录 scheduler owner、
upstream source、是否包含项目调度代码和 formal eligibility。

Daft 官方 803,580-row ResNet18 vendor-code baseline 固定在
`configs/image_vendor_baselines.json`。该轨道直接运行固定 commit 的官方 Daft/Ray Data
入口；允许的适配只有凭证/路径、物理 GPU 数、外部指标采集和结果审计，禁止加入项目
actor pool、credit、active window、routing 或 backpressure。

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

目录化后的完整测试发现必须指定 top-level，避免 `tests/experiments` 与
`src/experiments` 被 Python 当成同名顶级包：

```bash
python -m unittest discover -s code/tests -t code -p 'test_*.py'
```

## PostgreSQL data source backends

`code/src/data/sources/postgres_text.py` defines the data entry boundary used by
`code/scripts/profiling/postgres_ai_operator_profile.py`:

- `arrow_postgres`: baseline psycopg read plus Arrow table construction.
- `daft_postgres`: Daft `read_sql` PostgreSQL entry; it requires `sqlglot` and
  `connectorx`.

Data entry can use either the baseline psycopg path or Daft:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
  --dry-run --data-source daft_postgres --organizer daft `
  --output tmp\postgres_profile_dry_run.csv
```

Current writeback remains `none`, `json_text`, or `pgvector`. Lance is a future
optional sink backend and is not part of this implementation.

## Runtime code boundaries

The main profiling script should stay as orchestration code. Reusable behavior
now lives under `code/src/`:

- `data/sources/postgres_text.py`: PostgreSQL/Daft data entry.
- `data/materializers/text.py`: Arrow/Daft batch organization.
- `modalities/text/costs.py`: strict, shared output-cost modes/provenance and the
  completion actual-token-work extractor injected into the generic scheduler.
- `planning/work.py`: modality-neutral staged `WorkDescriptor` and atomic
  `RuntimeStateSnapshot`; preserves scalar credit compatibility while exposing
  prepare/model/result demand, locality, deadline/SLO, uncertainty and
  calibration identity.
- `planning/packing/scalar.py`: deterministic, modality-neutral classic BFD and a
  row-cap-first placement candidate sharing the same validation and ordering.
- `serving/backends/`: 公共 backend 合同、embedding backend 与
  completion/async/Ollama backend；包入口保持原公开 import 不变。
- `modalities/image/`: engine-independent image batch/result semantics, a lazy Daft
  PostgreSQL image source, CPU CLIP preprocessing, and a tensor-only GPU actor.
  The actor owns no hidden batching; organizer/scheduler remains the batching
  owner. Native framework baselines are deliberately separate under
  `baselines/image/frameworks/`.
  `modalities/image/metrics.py` only derives scale-aware unit-resource and
  streaming-onset fields from observed totals; it does not infer hidden engine
  queues or relabel text TTFT/ITL as image metrics.
- `infrastructure/runtime_env.py`: one shared contract for `PYTHONPATH` plus single-threaded
  OpenBLAS/MKL/OMP/NumExpr settings inherited by Ray workers and multi-job
  subprocesses. This prevents a 4-job run from multiplying 32 BLAS threads per
  worker before any model request is sent.
- `infrastructure/config_env.py` and `infrastructure/environment.py`: strict
  `${ENV_VAR}` config expansion plus read-only machine/Python/model/dataset
  preflight. Missing values fail closed; installs/downloads remain explicit CLI
  actions.
- Image workers apply the same rule inside long-lived Ray/Daft processes:
  `experiments/run_image_clip_e2e.py` defaults Torch intra-op/inter-op pools to `1/1`,
  records them in schema v8, and the project Ray pool verifies the observed
  values before admitting work. Ray `num_cpus` remains an admission token, not
  an OS thread quota; actor count and per-actor thread count are separate
  experiment variables.
  The image runner passes the shared `ray_runtime_env()` contract at every
  `ray.init`, so workers receive project imports and numeric thread limits
  without relying on an interactive shell's `PYTHONPATH`.
  Schema v8 also separates Ray cluster slots from external Daft source threads
  and includes both in the host physical-resource budget.
  `--source-cpu-threads` is independent from preprocess `--cpu-workers`, so
  capacity sweeps change one stage at a time.
  Project execution additionally times source iterator waits, driver batch
  materialization, and Ray submission so the residual framework gap is not
  mislabeled as GPU or PCIe time.
- `baselines/`: vLLM Bench service ceiling、项目自写 bounded controls、Daft/Ray Data
  framework-native adapters、OceanBase product-native adapter、immutable manifest 和
  fail-closed 双 endpoint gate。`text/frameworks/` 只封装 vendor API graph，不注入项目
  credit/router；`provenance.py` 防止 control/ceiling 被误报为原生 baseline。
- `data/sinks/postgres.py`: existing PostgreSQL embedding writeback modes plus `document_completions` JSON-text writeback.
- `observability/metrics/`: `timing.py`、`csv.py`、`statistics.py`、`resources.py`、
  `vllm.py`、`retrieval.py`、`squad.py` 分别负责计时、schema-safe CSV、重复统计、
  GPU/能耗/MFU、vLLM TTFT/ITL/cache 指标、显式真值检索质量和 SQuAD v1.1
  Exact Match/token-F1；
  包入口保持原公开 import 不变。Formal runs preflight the main
  output against dry-run keys before database or GPU work. Empty files receive
  a header; non-empty files reject appended rows whose ordered keys do not
  exactly match the existing header.
  `model_request_tokens_per_s` isolates the model-submission window,
  `operator_tokens_per_s` uses the operator wall, and legacy `tokens_per_s`
  remains the complete E2E rate; comparisons must not mix these time bases.
- `observability/profiling/traces.py`: versioned control/flush/submission/request/resource CSV
  serializers. Control schema 2 records the actual `hol_age_s` input;
  submission schema 3 uses scheduler lifecycle IDs and records pool,
  endpoint, GPU, status, and error instead of synthesizing batch IDs for
  request-granularity runs.
- `observability/profiling/cli.py`: the profiler command-line surface only; it does not start
  Ray, connect to PostgreSQL, or inspect the environment beyond argument
  defaults.
- `observability/profiling/config.py`: post-parse endpoint/metrics precedence and Ray worker
  resource resolution. Explicit CLI values win over plural/single environment
  defaults.
- `observability/profiling/schema.py`: the ordered formal summary-row contract and schema-drift
  guard, separate from runtime orchestration.
- `observability/profiling/replay.py`: offline/replayed Arrow envelope construction,
  token-budget row grouping, batch/request submission expansion, and request
  lifecycle seed assembly. It does not submit Ray work or call model services.
- `observability/profiling/ray.py`: Ray task/actor submitters, endpoint topology, typed
  scheduler wiring, credit release/fan-in, and the explicitly retained legacy
  adaptive baselines. It does not parse CLI arguments or write trace CSVs.
- `data/workloads/text.py`: small built-in seed workloads for smoke/dev only.
- `scheduling/`: engine-independent typed core split by decision boundary:
  `core/`, `organization/`, `submission_control/`, `endpoint_routing/`, and `runtime/`.
  `core/control.py` owns the neutral capacity-arm contract;
  `core/execution.py` owns exactly-once pending/completion/lifecycle state and accepts a
  modality-specific actual-work extractor. `submission_control/saor.py` contains only the
  finite-action DPP policy and typed inputs; `ordered_release.py` is the single owner of
  per-Job ready queues, active work and monotonic `release_seq`. Neither module contains
  K128/K160, KV thresholds, endpoint counts or text/image branches: calibrated arms,
  predicted service and admissible actions are caller inputs.
  `submission_control/stage_work.py` is a bounded candidate that moves only one
  offline-calibrated work-credit step and falls back to the workload-specific
  static point on stale or mismatched state; it is not yet a performance claim.
  Duplicate root compatibility modules have been removed. The formal
  payload/execution path remains Daft -> Arrow -> Ray.

`fake` is retained only as a local control backend for offline smoke tests and
pipeline debugging. It is not a model-service result source. For vLLM-compatible
experiments, use `--model-backend compatible_http`; the older `http_openai`
name is accepted only as a compatibility alias. `AI_COMPLETE` is selected with
`--operator ai_complete`. Use `/v1/completions` for the original multi-prompt
mechanism path, or `/v1/chat/completions` with
`--completion-protocol chat_completions` for product/official-runtime
compatibility. `--completion-http-transport httpx_async` creates one persistent,
bounded async connection pool per Ray actor. In Completions mode one actor call
keeps the original multi-prompt HTTP body; in Chat mode a multi-row actor call
dispatches one independent Chat request per row with `asyncio.gather`. For local
Ollama smoke runs, use
`--model-backend ollama --completion-endpoint-url http://localhost:11434`.

`src/observability/profiling/` owns profiler implementation. The obsolete root
`src/profile_*.py` compatibility modules have been removed; production and test callers now
use the owning subpackage directly, and an AST architecture test prevents the old paths from returning.

## Scheduling foundation

`code/src/scheduling/` contains immutable request metadata, endpoint topology,
data-organization policies, request/work admission, shared multi-job credit,
endpoint routing, a deterministic policy-composition scheduler, and Ray runtime
adapters. Policy modules do not import Daft, Arrow, Ray, or HTTP; only runtime
adapters receive the active Ray module explicitly.

The first SAOR core revision is intentionally not wired into a formal runner yet. Its slow
path selects among caller-enumerated safe actions using queue and weighted-fairness debt;
its fast path publishes validated Job-head requests with a monotonic sequence and releases
capacity on completion. Release work and predicted epoch service are separate fields, so a
long request is not silently treated as service completed within the current control slot.
The convenience action builder requires every service/goodput/tail/energy/switch prediction;
release-action values are explicit marginal deltas against the zero-delta hold reference.
GPU utilization, `waiting`, KV thresholds and phase labels remain observation/experiment
inputs outside the policy; no single metric is hard-coded into the action selector.
The package facade now resolves compatibility exports lazily, so importing SAOR does not
load AIMD/PID/UCB or Ray adapters. This removes an import-time dependency, but it does not
yet make the repository SAOR-only: the profiler, shared-vLLM runner, image multi-Job runner,
and observability wiring still import legacy baselines directly. Those callers must migrate
to a tested SAOR runtime profile before legacy files can be archived or removed.

Endpoint state separates service health (`healthy`) from request-specific
admission capacity (`available`). Routers use `schedulable_endpoints()` for
selection, while `healthy_endpoints()` remains a pure health query. Temporary
request/work-credit exhaustion raises typed backpressure so the scheduler
collects a completion and retries; a manifest-pinned request keeps both its
endpoint and that endpoint's pool.
Arrival replay is produced through a one-element bounded queue so waiting for
the next source arrival cannot block Ray completion collection. The scheduler
keeps submit/routing/credit/lifecycle state on its main thread: it prioritizes
an already-ready arrival, polls `ray.wait(timeout=0)` during arrival gaps, and
uses blocking collection only when admission or active-work capacity is full.
This preserves offered-load saturation while releasing request-level credit
and recording completion timestamps promptly under sparse or bursty replay.

The shared-vLLM experiment runner can pin a distinct immutable request manifest
and source offset for every job. This is required for staggered short/long or
otherwise heterogeneous-job evidence; reusing the same rows for all jobs only
validates concurrency semantics and cannot support a work-aware fairness claim.
For a causal single-short control, a static scenario may declare
`static_partition_count` larger than its active `job_count`. The unused fixed
partition stays reserved, so `job_count=1, static_partition_count=2` reproduces
the K/work quota available to one job in a two-job static partition without
launching a synthetic competing job. Other policies reject this field.
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
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_models.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_policies.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduler.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_ray_adapter.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_adaptive_admission.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_dynamic_admission.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_flush_policies.py
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_daft_ray_contract.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_token_budget_controller.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_shared_credit.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_shared_credit_ray.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_execution_ledger.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_saor.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_ordered_release.py
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

`code/src/experiments/scenarios/core.py` and
`code/scripts/experiments/run_ai_operator_scenarios.py` provide deterministic formal-run
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

Import 2048 local rows of the current main workload (sharegpt_multiturn, doc_id 300000-302047, target_output_tokens 1-256) without clearing existing `documents` rows:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\data\import_ai_complete_workload.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --workload-name sharegpt_multiturn `
  --start-doc-id 300000 `
  --max-rows 2048 `
  --batch-rows 500
```

Run the profiling script against this workload with:

```powershell
--source-workload-name sharegpt_multiturn
```

Note: `sharegpt_burstgpt` (formerly 1024 rows, now 2048) is a legacy workload retained for reproducing 0725-0728 experiments; it is no longer the default for current baselines.

## Daft 文本组织 smoke

当前 Daft 接入的项目代码在 `code/src/data/materializers/text.py`：`ArrowOrganizer`
是 baseline 后端，`DaftOrganizer` 是文本阶段 Daft DataFrame 后端。独立 smoke 入口只负责验证
`rows -> Arrow Table -> organizer -> batches`，并可显式切换 Ray runner 验证
`into_partitions`。这不是正式性能实验，不写入 `motivation/results/gpu/`。

```powershell
.conda\pg-ai-profile\python.exe code\scripts\profiling\daft_text_organizer_smoke.py `
  --organizer daft --runner ray --rows 32 --batch-size 8 `
  --partition-mode into_partitions --partitions 4 `
  --output tmp\daft_text_organizer_smoke.csv
```

```powershell
.conda\pg-ai-profile\python.exe code\tests\planning\test_organizers.py
```

## PostgreSQL AI 算子链路画像

当前新增入口：

```text
code/scripts/profiling/postgres_ai_operator_profile.py
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
.venv/bin/python code/scripts/profiling/postgres_ai_operator_profile.py \
  --dry-run \
  --output feasibility/results/postgres_ai_operator_profile_dry_run.csv
```

连接当前本地同构 PostgreSQL 实例：

```bash
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/ai_operator" \
.venv/bin/python code/scripts/profiling/postgres_ai_operator_profile.py \
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
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
  --dry-run --executor python `
  --organizer daft --daft-runner native `
  --output tmp\postgres_profile_dry_run.csv
```

真实报告必须说明数据库平台类型：

- PostgreSQL 18.3 内部验证平台；
- 普通 PostgreSQL + pgvector 同构预演替身；
- 其他开源数据库 AI 算子平台。

## 同条件 baseline 模块

`code/src/baselines/` 把现有系统对照与本项目实现分离成可审计适配层：

- `contracts.py` / `manifests.py` 固定请求语义、顺序、hash 与 endpoint 分片；
- `controls/` 提供项目自写 direct controls，`ceilings/vllm_bench.py` 提供 serving
  ceiling；
- `runtime/daft_prompt.py` / `runtime/ray_data_http.py` 分别对接 Daft 与 Ray Data
  vendor-native API graph；
- `provenance.py` 固定 role、scheduler owner、custom scheduling、formal eligibility
  与 upstream source，避免 control/ceiling 冒充 native baseline；
- `postgres_manifest.py` 从正式 PostgreSQL workload 只读导出完整行与 source
  hash，随后交给共同 endpoint 分片器；
- `products/oceanbase.py` 对接原生 `AI_COMPLETE`，不以 Python HTTP 模拟产品算子；
- `results.py` / `gate.py` 统一 exactly-once、延迟、吞吐和 fail-closed 门禁；
- `cli.py` 只做 shard dispatch、原始证据保存和格式归一化；
- `gate_runner.py` 串行 core cell、并行双 endpoint shard，并在空队列校验后
  fail closed，不复制项目 profiler。

该模块的目标不是让 baseline 共享本项目调度逻辑，而是共享同一个不可变
Chat Completions workload 与结果契约。vLLM Bench 是下游上限，不属于数据库
算子；bounded HTTP 是强因果对照，不冒充已有产品；OceanBase 缺少 AI Function
能力时记为 capability failure，不阻塞 bounded HTTP 与官方 runtime 主矩阵。

开题前的两个文本证据缺口使用 `scripts/baselines/opening_database_e2e_matrix.py`。
它只运行 direct static sharded、DuckDB AI static sharded 和 project frozen static
三臂，统一 PostgreSQL source、immutable manifest、双 endpoint、数据库 sink、质量与
资源口径，并按确定性随机顺序执行 1 warmup + 3 formal。冻结合同以
`../experiments/plans/opening_database_e2e_p0_20260807.md` 为准；该 runner 不作为新增
通用 baseline 框架，也不允许加入 adaptive arm 或参数扫描。

`src/baselines/text/orchestration/native_matrix.py` 在明确冻结每臂校准指纹后，
复用 core gate 执行原生文本框架的 1 warmup + N 交错 formal；它不复制
adapter 或请求计数逻辑，并为每个 run 保存逐 GPU 资源时序、vLLM gauge 与
latency/estimated-FLOPs delta，供 GPU 利用率、能耗和 MFU 审计。
`native_multijob.py` 只负责绝对时间启动两个错峰
原生 job、保存四个 endpoint shard 证据、组级计数、vLLM gauge/latency delta
与逐 GPU 利用率/功耗时序；禁止项目 credit/router/inflight 参数进入
Daft/Ray Data 观察臂。两个 endpoint shard 共享显式进程 wall deadline，超时会终止
survivor、保存 job evidence 并 fail closed；HTTP 单 request timeout 不替代该生命周期门。
项目 static/shared 因果 A/B 仍由
`src/experiments/shared_vllm/` 执行。

`src/calibration.py` 与 `scripts/analysis/select_strategy_calibration.py` 负责把 feeding、
token-budget 和同协议 actor-shape 校准结果冻结为后续策略实验的机器可校验
合同，避免示例环境中的历史默认值静默进入正式 data-organization、
submission 或多 job 矩阵。
合同分别记录 throughput-oriented token budget 与在 95% throughput floor
内最大化 SLO goodput 的候选预算，避免把一个预算误写成所有目标的通用最优。
actor shape 保持每 endpoint 总 slots 不变，并选择达到峰值 97% 的最小
actor 数；Chat 结果不能冻结 Completions 主线。
Shared-vLLM 的 4-job 数据面必须使用有界 persistent async actor pool；显式
4-job `ray_task` 配置会在外部请求前失败，防止数百 worker 再次耗尽容器 VMA。
