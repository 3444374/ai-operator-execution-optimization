# 项目日志

## 2026-07-20 实验状态全面审计与缺口分析

- **触发**：用户要求对当前实验进展做系统性评估，回答"现在的实验能说明什么、还需要做什么"。
- **评估方法**：结合 idea-evaluator（五维评分 + fatal-flaws audit + paradigm-shift probe）、ars-reviewer（模拟三审稿人）、deep-research 和 vibe-research-workflow 四种视角。
- **核心发现**：
  1. **动机链完整**：token-tail revision 证明了"固定行 batch 是计算量的弱代理"，shared-vLLM interference 证明了"K_max 在共享 vLLM 下必要"——这两个动机实验质量好，可直接写进论文。
  2. **RC1 策略机制已验证**：token-budget batching 能约束 token tail（P95 从 26678→6144），但存在 tradeoff（4096 吞吐较低）。
  3. **RC2 策略未验证**：queue-adaptive flush 已实现但不如静态 K_max=8（foreground E2E 10.2s vs 7.3s）。这是当前最高风险的 gap。
  4. **两项策略联合消融缺失**：完全没跑过独立拼接 vs 联合 grid search。
  5. **指标盲区**：缺 `tokens/s`（比 rows/s 更公平的 AI_COMPLETE 效率指标）、缺 inflight/queue 时间序列、缺 per-request latency 分布、缺系统性 `service_p99`。
- **新建文件**：
  - `experiments/plans/experiment_status_and_gaps.md`：完整的状态-缺口-路线图文档，包含已完成/未完成实验表、证据链评估、指标盲区、P0/P1/P2 实验路线图、审稿人视角的拒绝风险。
  - `learning/metric_selection_methodology.md`：AI_EMBED vs AI_COMPLETE 观察变量选择方法论，解释为什么从"阶段时延拆分"转向"请求形状 + 服务端压力 + 端到端分布"的四层变量体系。
- **更新文件**：
  - `PROJECT_OUTLINE.md`：§当前最重要证据 重写为以本地 vLLM baseline 为首要证据；§近期优先级 重写为已完成项 + P0/P1/P2 缺口 + 指标盲区 + 新增 adaptive 放弃条件。
  - `experiments/plans/README.md`：新增状态审计文档入口。
  - `learning/README.md`：新增指标方法论文档入口。
  - `experiments/results/local_vllm_qwen15b_baseline/README.md`：§Remaining Formal Experiments 重写为结构化的下一步清单。
- **idea-evaluator 裁决**：Accept with Revisions。Higher 6, Faster 7, Stronger 8, Cheaper 5, Broader 6。Paradigm-shift potential possible（3.5/4）。两个 MAJOR flaw（adaptive < static、单 GPU 限制），均有明确修复路径。
- **ars-reviewer 共识**：动机实验扎实，但 adaptive < static 和缺乏联合消融是两个 MAJOR concern，如不修复在 VLDB/SIGMOD 级会议上大概率被拒。

## 2026-07-19 Shared-vLLM K_max interference experiment

- Added `code/scripts/run_kmax_interference_experiment.py`, a wrapper around
  `postgres_ai_operator_profile.py` that runs a foreground small job while a
  background bulk job shares the same local vLLM endpoint.
- Ran the first two-job `AI_COMPLETE` interference experiment:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_small_20260719.csv`
  and
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_bulk_20260719.csv`.
  Foreground: 128 rows, fixed16, `K_max=8`, `completion_max_tokens=64`.
  Background: 1024 rows observed in CSV, fixed16,
  `completion_max_tokens=64`, comparing `K_max=8` versus unbounded
  (`max_inflight=100000`). Both jobs share
  `http://localhost:8000/v1/completions`.
- Result: foreground small-job E2E averaged 4.923s solo, 6.535s during bounded
  bulk, and 11.384s during unbounded bulk. Foreground service P95 rose from
  2.580s under bounded bulk to 4.386s under unbounded bulk; vLLM queue mean
  rose from 0.001s to 0.445s.
- Interpretation boundary: this supports the K_max admission-control motivation
  under a shared vLLM service. It does not imply K_max is necessary when every
  job has an exclusive vLLM endpoint, and it is not yet a full fairness/SLO
  sweep.
- Added `figures/data/backup/b21_local_vllm_kmax_interference_small_job.*` and
  updated the local baseline README, code script README, figure README, audit,
  and `PROJECT_INDEX.md`.

## 2026-07-19 Batch policy x K_max AI_COMPLETE scheduling matrix

- Adjusted the scheduling baseline design after reviewing the role of `K_max`:
  `K_max` is admission control over already-formed Ray submissions, so it must
  be tested jointly with upstream batch shape rather than as a substitute for
  batch construction.
- Ran the local ShareGPT/BurstGPT `AI_COMPLETE` matrix:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_batch_policy_kmax_matrix_20260719.csv`.
  Matrix: fixed rows `32/64/128`, token budgets `4096/6144/8192`, and
  `K_max=2/4/8/16/unbounded`; 512 rows, `source_order=arrival_time`,
  `ray_task`, Daft source/organizer, local vLLM Qwen2.5-1.5B,
  `completion_max_tokens=16`, no writeback, warmup 1, formal repeats 3.
  All 120 rows completed with `status=ok`.
- Result boundary: too-small `K_max` increases Ray-side bounded wait and
  end-to-end time; larger `K_max` mostly improves or plateaus E2E in this local
  offline job, while raising vLLM queue/service-tail pressure at high inflight.
  This matrix does not prove that `K_max` is required for end-to-end
  improvement.
- Batch shape remains necessary in the analysis: `fixed128` creates only 4
  upstream requests, so `K_max>4` has little scheduling space, while token P95
  remains about 26680. Token-budget settings bound token P95 but create more
  requests, making admission control observable.
- Added `figures/data/backup/b18_local_vllm_batch_kmax_e2e.*`,
  `figures/data/backup/b19_local_vllm_batch_kmax_service_pressure.*`, and
  `figures/data/backup/b20_local_vllm_batch_kmax_request_granularity.*`.
  Updated the local baseline README, figure README, audit note, and
  `PROJECT_INDEX.md`. The earlier
  `sharegpt_burstgpt_arrival_kmax_token6144_20260719.csv` / `b17` run is now
  documented as a preliminary single-shape K_max sweep.
- Started and then stopped a heavier offline stress sweep after determining it
  would still not establish the right motivation for `K_max`. The next
  scheduling experiment should instead use a real admission-control objective:
  multi-job or burst arrival workload, shared vLLM endpoint, SLO tail latency,
  timeout rate, queue-length peak, and fairness.

## 2026-07-19 Token-budget vs fixed-row AI_COMPLETE baseline

- Added upstream `--batching-policy fixed_rows|token_budget` and
  `--token-budget` support to `code/scripts/postgres_ai_operator_profile.py`
  through `code/src/organizers.py`. Token-budget batching greedily groups rows
  by estimated `prompt_tokens + completion_max_tokens` before Ray submission;
  it does not modify Ray or vLLM internals.
- Added CSV fields `batching_policy`, `token_budget`, and
  `model_request_timeout_s`, plus organizer unit tests for token-budget batch
  construction.
- Ran the local ShareGPT/BurstGPT `AI_COMPLETE` policy matrix:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv`.
  Matrix: fixed rows `16/32/64/128` versus token budgets `4096/6144/8192`,
  512 rows, `ray_task`, Daft source/organizer, local vLLM Qwen2.5-1.5B,
  `max_inflight=8`, no writeback, warmup 1, formal repeats 3, request timeout
  300s.
- Result boundary: token-budget controls request token tail and queue pressure
  (`4096/6144/8192` token P95 near budget, versus fixed 64/128 at 16377/26677),
  but throughput is a tradeoff. `4096` is most queue-stable but slower;
  `6144/8192` approach fixed 32/64 throughput while keeping token P95 much
  lower than fixed 64/128. This supports dynamic batching motivation, not the
  full method claim.
- Added `figures/data/backup/b15_local_vllm_token_budget_throughput.*` and
  `figures/data/backup/b16_local_vllm_token_budget_tail_queue.*`, then updated
  the local baseline README, figure README, audit note, and script README.
- Recorded the remaining local baseline follow-up list in
  `experiments/results/local_vllm_qwen15b_baseline/README.md`: arrival-aware
  `K_max` sweep next, queue-adaptive flush after that, length-align and
  prefix-aware ablations later, and COPY + deferred-index writeback deferred.

## 2026-07-19 PostgreSQL source-order mode for AI_COMPLETE profiles

- Added `--source-order doc_id|arrival_time` to
  `code/scripts/postgres_ai_operator_profile.py` and propagated the value into
  CSV rows.
- Updated `code/src/sources.py` so both `PostgresArrowSource` and
  `DaftPostgresSource` share the same source-order semantics:
  `doc_id` for offline throughput/data-organization scans, and
  `arrival_time_s NULLS LAST, doc_id` for arrival-aware service scheduling
  experiments.
- Updated `code/tests/test_sources.py`, `code/scripts/README.md`,
  `experiments/results/local_vllm_qwen15b_baseline/README.md`,
  `figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md`,
  `learning/local_vllm_ray_baseline_walkthrough.md`, and `PROJECT_INDEX.md`.
- Boundary: existing 2026-07-18/2026-07-19 local baseline CSVs should be read
  as `doc_id` offline-throughput runs. Future K_max, queue-adaptive flush, and
  backpressure experiments should use `--source-order arrival_time` when the
  claim depends on request arrival rhythm.

## 2026-07-19 Local vLLM fixed-row baseline token-tail revision

- Added the 2026-07-19 Ray task token-tail sweep CSV:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_task_batch128_token_sweep_20260719.csv`.
- Revised the baseline interpretation from a plain row-batch sweep to a
  fixed-row proxy limitation test: larger row batches reduce request count, but
  token P95 and service P95 grow sharply and local throughput plateaus around
  16-32 rows and remains flat through the 64/128 stress points.
- Updated `figures/scripts/generate_local_vllm_ray_baseline_charts.py` to add
  `b11_local_vllm_token_tail_performance.*` and
  `b13_local_vllm_token_tail_penalty.*` as the main token-tail motivation
  figures, then added `b14_local_vllm_service_tail_gap.*` to isolate the
  service P50-to-P95 tail gap.
- Revised `b10` into `b10_local_vllm_request_count_inflight.*`, using two
  aligned panels for model-service call count and in-flight utilization instead
  of mixing both quantities on one axis.
- Updated `experiments/results/local_vllm_qwen15b_baseline/README.md`,
  `figures/README.md`, and
  `figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md` with the
  revised baseline question, command, result table, boundaries, and figure
  roles.

## 2026-07-18 Local vLLM Ray baseline figures and learning note

- Added `figures/scripts/generate_local_vllm_ray_baseline_charts.py` to
  regenerate backup figures from the ShareGPT/BurstGPT local
  `AI_COMPLETE` baseline CSVs.
- Generated separate single-purpose figures instead of a dashboard:
  `b07_local_vllm_ray_throughput.*`, `b08_local_vllm_ray_e2e_time.*`,
  `b09_local_vllm_ray_task_stage_timing.*`,
  `b10_local_vllm_request_count_inflight.*`,
  `b11_local_vllm_token_tail_performance.*`, and
  `b12_local_vllm_latency_probe_breakdown.*`.
- Added `figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md` and
  `learning/local_vllm_ray_baseline_walkthrough.md`.
- Boundary: these are local PG18.4 fixed row-batch baseline and metric
  observability support figures. They are not token-aware batching,
  queue-adaptive scheduling, writeback-inclusive, or PostgreSQL 18.3 internal
  platform results.

## 2026-07-18 AI_COMPLETE latency and vLLM metric probe

- Added batch-level result statistics to `code/src/metrics.py` and
  `code/scripts/postgres_ai_operator_profile.py`: batch row min/max/mean,
  batch token min/max/mean, and batch service latency P50/P95/P99.
- Added optional `--model-metrics-url` Prometheus scraping for vLLM run-level
  delta metrics: prompt/generation token deltas, request success delta, mean
  vLLM e2e/queue/inference/prefill/decode latency, and final running/waiting
  request gauges.
- Verified with
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_task_batch8_latency_metrics_20260718.csv`:
  4 rows, 3 formal rows, all `status=ok`, all `vllm_metrics_status=ok`.
- Boundary: this validates metric collection on a small local Daft + Ray +
  vLLM probe. It is not a full optimized scheduling result.

## 2026-07-18 ShareGPT/BurstGPT tokenizer-filtered Ray rerun

- Updated `code/scripts/import_ai_complete_workload.py` so the imported
  `sharegpt_burstgpt` workload can use the local Qwen2.5-1.5B-Instruct
  tokenizer for `prompt_tokens` and filter rows by
  `prompt_tokens + completion_max_tokens <= max_model_len`.
- Re-imported 1024 `sharegpt_burstgpt` rows into the local PostgreSQL
  rehearsal database with `max_model_len=2048` and `completion_max_tokens=16`.
  Current prompt-token range is 1..1851.
- Reran the local `AI_COMPLETE` baseline through
  `PostgreSQL -> DaftPostgresSource -> DaftOrganizer -> Ray -> vLLM` for
  `ray_task` and `ray_actor`, batch sizes 1/2/4/8/16/32. Results are recorded
  in
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_static_batch_sweep_rerun_20260718.csv`.

## 2026-07-18 Local vLLM Qwen static batch baseline

- Established the first local `AI_COMPLETE` baseline for `PostgreSQL -> DaftPostgresSource -> DaftOrganizer -> vLLM-compatible completion backend -> no writeback`.
- Used local `models/Qwen2.5-1.5B-Instruct` served by `vllm/vllm-openai:v0.25.1-cu129-ubuntu2404` as `qwen2.5-1.5b`.
- Ran fixed row-batch sweep with `ray_batch_rows` in `1,2,4,8,16`, `total_rows=32`, `completion_max_tokens=8`, `executor=python`, `warmup_runs=1`, `repeats=3`.
- Result CSV and report: `experiments/results/local_vllm_qwen15b_baseline/static_batch_sweep.csv` and `experiments/results/local_vllm_qwen15b_baseline/README.md`.
- All 20 rows returned `status=ok`; formal rows only: mean throughput improved from 10.328 rows/s at batch size 1 to 91.976 rows/s at batch size 16.
- Boundary: this is a local fixed row-batch baseline, not a token-aware scheduling result, not an optimal batch-size claim, and not a PostgreSQL 18.3 internal-platform result.

## 2026-07-18 ShareGPT and BurstGPT raw workload downloads

- Downloaded ShareGPT Vicuna unfiltered prompt data to `data/raw/sharegpt_vicuna/ShareGPT_V3_unfiltered_cleaned_split.json`; local check found 94,145 conversation records.
- Downloaded BurstGPT trace data to `data/raw/burstgpt/BurstGPT_1.csv`; local check confirmed columns `Timestamp`, `Model`, `Request tokens`, `Response tokens`, `Total tokens`, and `Log Type`.
- Added `data/README.md` to document raw data paths, intended use, and boundary.
- Updated `.gitignore` so `data/raw/**` payloads are not tracked by git.
- Boundary: this only establishes raw workload availability. Comparable baseline and optimized experiments should be generated from a normalized ShareGPT/BurstGPT workload table, not from the earlier synthetic seed rows.

## 2026-07-18 ShareGPT/BurstGPT workload import path

- Added `code/scripts/import_ai_complete_workload.py` to normalize ShareGPT prompts with BurstGPT timestamp/token metadata into the PostgreSQL `documents` table.
- Extended `documents` with workload metadata columns: `workload_name`, `prompt_tokens`, `target_output_tokens`, `arrival_time_s`, `session_id`, and `prefix_key`.
- Added `--source-workload-name` to `code/scripts/postgres_ai_operator_profile.py`, so different workloads can coexist in `documents` and profiling can select one explicitly.
- Imported local `sharegpt_burstgpt` workload rows into PostgreSQL with `start_doc_id=1000000`, `rows=1024`, `prompt_tokens=8..1797`, `target_output_tokens=2..2048`, and categories covering short/medium/long x ChatGPT/GPT-4.
- Verified a small `DaftPostgresSource -> DaftOrganizer -> Ray task -> vLLM` smoke under `tmp/sharegpt_burstgpt_daft_ray_vllm_smoke.csv` with `status=ok`, `total_rows=8`, `source_workload_name=sharegpt_burstgpt`.
- Boundary: this validates the final workload import/read path. It is not yet the full baseline sweep or an optimized scheduling result.

## 2026-07-18 vLLM local Qwen AI_COMPLETE smoke

- Started `vllm/vllm-openai:v0.25.1-cu129-ubuntu2404` with local Hugging Face model files from `models/Qwen2.5-1.5B-Instruct`, avoiding runtime Hub downloads.
- Required local Windows/WSL Docker settings for this machine: `VLLM_WSL2_ENABLE_PIN_MEMORY=1`, `VLLM_USE_V2_MODEL_RUNNER=0`, and `--enforce-eager`; the default vLLM V1/V2 runner path previously failed with `RuntimeError: UVA is not available`.
- Verified OpenAI-compatible `/v1/models` returned `qwen2.5-1.5b`; verified `/v1/completions` with a minimal prompt.
- Verified project E2E smoke under `tmp/vllm_local_qwen15b_ai_complete_smoke.csv`: `operator=ai_complete`, `data_source=daft_postgres`, `organizer=daft`, `model_backend=compatible_http`, `model_name=qwen2.5-1.5b`, `writeback_mode=json_text`, `total_rows=2`, `written_rows=2`, `status=ok`.
- Ran the layer-2 structural matrix under `tmp/vllm_local_qwen15b_layer2_matrix.csv`: `data_source` (`arrow_postgres`, `daft_postgres`) x `organizer` (`arrow`, `daft`) x `executor` (`python`, `ray_task`, `ray_actor`) x `writeback_mode` (`none`, `json_text`). All 24 rows returned `status=ok`; all `json_text` rows wrote `written_rows=2`; all `none` rows wrote `written_rows=0`.
- Boundary: this establishes the local vLLM + Qwen + Daft + PostgreSQL completion path. It is not yet a formal performance experiment or token-aware/prefix-aware batching result.

## 2026-07-18 Ollama AI_COMPLETE backend

- Added `ollama` as an `AI_COMPLETE` backend in `code/src/model_backends.py`, using Ollama native `/api/generate`.
- Updated `code/scripts/postgres_ai_operator_profile.py` so `--operator ai_complete --model-backend ollama` defaults to `http://localhost:11434` when no completion endpoint URL is provided.
- Verified local PG18.4 smoke with Docker Ollama `qwen2.5:1.5b`: `ollama_ai_complete_smoke` completed with `written_rows=2`; `ollama_daft_ai_complete_smoke` completed with `data_source=daft_postgres`, `organizer=daft`, and `written_rows=2`.
- Ran the layer-3 structural matrix under `tmp/ollama_ai_complete_layer3_matrix.csv`: `data_source` (`arrow_postgres`, `daft_postgres`) x `organizer` (`arrow`, `daft`) x `executor` (`python`, `ray_task`, `ray_actor`) x `writeback_mode` (`none`, `json_text`). All 24 rows returned `status=ok`; all `json_text` rows wrote `written_rows=4`.
- This is a local Ollama completion smoke. It does not replace the future vLLM-compatible `/v1/completions` path and is not a token-aware/prefix-aware batching result.

## 2026-07-18 AI_COMPLETE runtime skeleton

- Added `--operator ai_embed|ai_complete` to `code/scripts/postgres_ai_operator_profile.py`; default remains `ai_embed`.
- Extended `code/src/model_backends.py` with fake and vLLM-compatible `/v1/completions` completion backends.
- Extended `code/src/sinks.py` with `write_completions` and added `document_completions` to the local schema.
- `AI_COMPLETE` supports `none/json_text` writeback. `pgvector` remains embedding-only and is rejected for `AI_COMPLETE`.
- Added Ray worker `PYTHONPATH` runtime env so Ray task/actor workers can import `code/src` modules after the runtime split.
- Verified PG18.4 local smoke under `tmp/postgres_ai_complete_fake_smoke.csv`: fake `AI_COMPLETE` completed with `total_rows=16`, JSON-text writeback completed with `written_rows=8`, and `ray_task` completed with `status=ok`. This is a local function smoke, not a vLLM performance result or token-aware batching conclusion. The Windows Ray run printed a raylet shutdown access-violation warning after producing the result row.

## 2026-07-18 Runtime code boundary cleanup

- Split reusable runtime helpers out of `code/scripts/postgres_ai_operator_profile.py`:
  - `code/src/model_backends.py`: fake debug embedding backend and compatible HTTP embedding backend.
  - `code/src/sinks.py`: existing `none/json_text/pgvector` PostgreSQL writeback.
  - `code/src/metrics.py`: stage timer, GPU snapshot, and CSV append helper.
- Kept `fake` only as an offline smoke/control backend. vLLM-compatible runs should use `--model-backend compatible_http`; `http_openai` remains accepted as a compatibility alias.
- Added `code/tests/test_model_backends.py` and `code/tests/test_sinks.py`.
- Updated `code/README.md`, `code/scripts/README.md`, and `PROJECT_INDEX.md` with the new code boundaries.

## 2026-07-17 Daft PostgreSQL data entry implementation

- Added `code/src/sources.py` with `PostgresArrowSource` and `DaftPostgresSource`, plus `code/tests/test_sources.py`.
- Updated `code/scripts/postgres_ai_operator_profile.py` with `--data-source arrow_postgres|daft_postgres`; default remains `arrow_postgres`.
- Kept writeback unchanged: `none/json_text/pgvector`. Lance remains a future optional sink and is not implemented in this step.
- Added Daft SQL runtime dependencies `sqlglot` and `connectorx` to `code/requirements.txt`.
- Verified local PG18.4 smoke under `tmp/postgres_daft_source_e2e.csv`: `source_arrow_smoke` and `source_daft_smoke` both completed with `total_rows=64` and `object_count=4`. This is a local smoke result, not a formal performance conclusion.

## 2026-07-17 Superpowers implementation plan for Daft PostgreSQL data entry

- **触发**：用户要求使用 `superpowers:brainstorming` / `superpowers:writing-plans` 构思后续代码，并明确当前写回按既有方案，Lance 仅作为后续可能方向。
- **新增**：
  - `code_doc/README.md`
  - `code_doc/superpowers/README.md`
  - `code_doc/superpowers/plans/README.md`
  - `code_doc/superpowers/plans/2026-07-17-daft-postgres-entry-existing-writeback.md`
- **范围**：计划聚焦 Daft 作为 PostgreSQL data entry；当前 writeback 保持 `none/json_text/pgvector`；Lance 仅作为 future optional sink，不进入本轮实现。

## 2026-07-17 Daft 文本 DataOrganizer smoke 接入

- **触发**：用户要求实际使用 Daft，并要求遵循 `karpathy-guidelines`、保证代码可维护性。
- **实现**：
  - 新增 `code/src/organizers.py`，实现 `ArrowOrganizer` 与 `DaftOrganizer`。两者接收 Arrow table，输出 downstream 可复用的 Arrow batch 列表和指标。
  - 新增 `code/scripts/daft_text_organizer_smoke.py`，通过 `--organizer arrow|daft` 验证 `rows -> Arrow Table -> organizer -> batches`，并支持显式 `--runner ray` 检查 Daft `into_partitions`。
  - 更新 `code/scripts/postgres_ai_operator_profile.py`：主链路的 `fetch_record_batch + split_batch` 已替换为 organizer 后端选择，新增 `--organizer arrow|daft`、`--organizer-partition-mode`、`--organizer-partitions`、`--daft-runner`。默认仍为 `arrow`，保留旧路径作为 baseline。
  - 新增 `code/tests/test_organizers.py`，覆盖 Arrow 后端和 Daft native 后端的 batch 输出一致性。
  - 更新 `code/requirements.txt`：新增 `daft`，并将 `pyarrow` 约束为 `>=16,<25`，匹配 Daft 0.7.20 的依赖边界。
  - 更新 `code/README.md`、`code/scripts/README.md`、`PROJECT_INDEX.md`，登记新增入口和运行命令。
- **本地验证**：
  - NativeRunner：`--rows 256 --batch-size 64` 生成 4 个 64 行 batch。
  - Ray runner：`--runner ray --rows 32 --batch-size 8 --partition-mode into_partitions --partitions 4` 生成 4 个 8 行 batch。
- **边界**：主脚本已具备 Daft organizer 后端选择，但这仍不是正式性能实验；真实 PostgreSQL/vLLM/GPU-backed 结论需要后续 E2E 运行数据。

## 2026-07-17 多模态正文实验 + Daft 文本阶段直接接入 + 优化空间扩展

- **触发**：与导师讨论后明确多模态实验进入正文（§5.3 策略泛化性验证），不是仅 Discussion；用户确认 Daft 从文本阶段直接接入（不再经过 Arrow 中间态）；用户明确"参数优化也可以作为贡献"。
- **六项关键决策**：
  1. **多模态正式进入正文**：在图像 workload（ImageNet/HuggingFace，CLIP + Qwen2.5-VL）上使用同一套策略代码，验证 token-budget → frame-budget、queue-adaptive flush → 完全复用的模态无关性。VLM 生成实验标记为 optional。
  2. **Daft 文本阶段直接接入**：取消 Arrow→Daft 过渡方案。Daft DataFrame API 对文本（`df["prompt"]`）和图像（`df["image"]`）是同一套接口，后续多模态实验只需替换列类型。
  3. **优化空间扩展为"策略级 + 引擎级"双层**：策略级（token-budget、queue-adaptive flush、routing）+ 引擎级（Daft `into_batches`/`batch_size`/`max_concurrency`/`gpus`/`repartition`）。论文贡献不是"发明了某个 knob"，而是"在数据库 AI 算子外部执行场景中系统表征了优化空间 + 提出了策略级决策方法 + 跨模态验证"。
  4. **算子代价估计定位**：作为 §6.1 补充讨论（不作为独立研究内容），基于实验阶段已采集的 profile 数据，不新增实验。
  5. **完整优化实验清单建立**：P0（batch 粒度/分组策略/提交节奏 3 消融）→ P1（Daft 引擎参数 + 耦合验证）→ P2（多模态泛化 + 算子代价估计）。
  6. **Scope 缩减触发条件写死**：Month 1 无 vLLM baseline → 多模态降 Discussion；文本 RC1+RC2 未完成前不启动多模态 pipeline；VLM 生成始终 optional。
  7. **写回降级**：写回不作为独立研究内容或实验阶段，降为实验设置中的工程细节。PostgreSQL + pgvector + COPY + deferred index 作为默认写回路径。
- **idea-evaluator 重评估结果**：Accept with Revisions（两个 MAJOR 但可防御）。评分：Higher 8, Faster 8, Stronger 8, Cheaper 6, Broader 7。范式转移潜力 possible（3.5/4）。最大风险是单 GPU 限制多 endpoint 并行实验深度 + 串行依赖过多导致周期膨胀（有 scope 缩减触发条件）。
- **更新文件**：
  - `AGENTS.md` §1/§2/§3 — 新增 Daft + 多模态 + 算子代价估计 + scope 缩减条件
  - `PROJECT_OUTLINE.md` — 研究内容扩展为 5 项、近期优先级重排、新增 scope 缩减条件
  - `research/knowledge_hub.md` §10.5.1 — 重写为"Daft 文本阶段直接接入 + 优化空间三层框架 + 完整实验清单"
  - `experiments/plans/strategy_design_implementation_reference.md` — 此前已完成口径统一（三层策略 → 两项策略 + 验证），§4.2 已更新 Daft 引擎抽象
- **涉及文件**：`AGENTS.md`, `PROJECT_OUTLINE.md`, `research/knowledge_hub.md`, `experiments/plans/strategy_design_implementation_reference.md`

## 2026-07-17 Daft+Ray 多模态与具身智能调研

- **新建** `research/daft_ray_multimodal_reference.md`：Daft+Ray 多模态执行引擎技术手册，涵盖 Swordfish 流式引擎、Flotilla 分布式架构、@daft.cls GPU UDF 机制、与具身智能的连接、及与本课题的关系分析。
- **更新** `research/knowledge_hub.md`：新增 §10 "Daft+Ray 多模态执行引擎与具身智能负载"，含架构对比、Snowflake Cortex 多模态 AI 算子、具身智能管线、及与本课题的互补关系论证。
- **更新** `opening/literature/ai_operator_literature_inventory.md`：新增 8 篇文献（Daft SciPy Talk、Ray Data Streaming Batch、Flotilla、@daft.cls、Snowflake Cortex Multimodal、阿里云 EMR Daft 具身智能、IBM 具身数据缺口、HeteroHub），总数 57→65 篇。
- **核心结论**：
  1. Daft+Ray 优化引擎层的物理资源调度（CPU/GPU 重叠、内存管理），本课题优化策略层的调度决策（按什么规则组 batch、按什么节奏发请求）——两者互补而非竞争。
  2. Snowflake Cortex 已 GA 多模态 AI SQL 算子，数据库 AI 算子处理多模态数据是工业现实。
  3. 本课题的调度策略框架（token-budget→frame-budget、queue-adaptive flush、actor pool 路由）对多模态负载具有自然泛化能力。
  4. 建议在论文 Discussion (§6) 中以具身智能为 generalization case，不做主实验。
- **涉及文件**：`research/knowledge_hub.md`, `research/daft_ray_multimodal_reference.md`, `opening/literature/ai_operator_literature_inventory.md`

## 2026-07-16 推理管线交互文献系统性收集

- **新建** `research/inference_pipeline_interaction_literature.md`：系统性搜索和收集 28 篇 CCF-A 论文、技术报告和工业系统文档。
- **覆盖五个方向**：
  1. LLM 推理服务与连续批处理（vLLM, Orca, Sarathi-Serve, FastServe, DistServe, Splitwise, Mooncake, S-LoRA）
  2. 自适应批处理与推理服务调度（Clipper, Nexus, Clockwork, Triton）
  3. 数据管线与推理服务交互（Ray Data Streaming Batch, Ray Data LLM integration, NeuStream, HedraRAG）
  4. Token/Prefix-Aware 优化（Parrot, SGLang, KVFlow, ChunkAttention, EPIC）
  5. Ray-Specific 推理服务模式（Ray Serve LLM, Ray Compiled Graphs）
- **核心发现**：确认存在研究空白——无任何已有工作系统性研究"上游数据管线 batch 参数（batch_size, partition_count, concurrency, token-aware/prefix-aware 分组）如何影响下游推理引擎 continuous batching 效率及最优协调策略"。
- **最新 2026 论文**：收录 BatchLLM (MLSys 2026)、PKAS (HPDC 2026)、PLA-Serve (MLSys 2026)、Load-Aware Prefill Deflection、PEACE 等。
- `research/README.md` 和 `PROJECT_INDEX.md` 已在先前 session 预添加了该文件的索引条目。

## 2026-07-16 方向重大调整：AI_COMPLETE 为主线 + 上游动态 Batching + Ray 架构设计空间

- **触发**：用户明确 AI_COMPLETE（生成式 LLM 推理）才是真正目标场景，AI_EMBED 只是"能跑的先跑"的过渡；用户不想要静态 batch，希望借鉴 vLLM continuous batching 思路做上游动态 batching，并充分利用 Ray 的 actor 灵活性做架构设计。
- **方向调整（七项共识）**：
  1. **RC3 降格**：从"研究内容三"降为"端到端验证实验：写回瓶颈判定"，只使用当前最优写回方法（COPY + deferred index）
  2. **"协同"操作化定义**：协同 = 上游数据组织的"形状"（batch token 分布）和提交"节奏"（K_max、queue-adaptive flush）共同影响下游 vLLM continuous batching 的调度效率。不再是模糊的"跨层协同"。
  3. **vLLM 重定位**：vLLM 不是竞争对手，是部署平台 + baseline。课题研究"在 vLLM continuous batching 之上，上游 Ray 数据执行层如何最优地组织请求"。
  4. **AI_COMPLETE 为主线**：AI_EMBED 降为预研验证；AI_COMPLETE（生成式 LLM）成为论文主体 workload，引入 token 长度分布、shared prefix、TTFT/TPOT、generation straggler 等更丰富的交互变量
  5. **上游动态 Batching（借鉴 Continuous Batching 原理）**：计划层不再是静态选择 `batch_size`，而是设计动态 batching policy——token-budget batching（类似 vLLM `max_num_batched_tokens`）、length-aligned grouping、prefix-aware grouping
  6. **Ray 作为架构设计空间**：不是只把 Ray 当 task executor，而是利用 actor 异构化（ShortTokenActor / LongTokenActor / PrefixAffinityActor）、运行时自适应（queue-adaptive flush）、去中心化协调（每个 actor 自主决策）、actor pool 分池路由等架构设计杠杆
  7. **耦合验证前置**：独立最优拼接 vs 联合 grid search 作为第一个关键消融实验；无交互效应时 fallback 为"分层独立优化框架"，仍为合格硕士论文
- **文献确认**：多源检索确认无 CCF-A 论文研究"上游数据管道 batch 参数 × 下游 continuous batching 性能"这一交叉点，研究空白判断成立。
- **用户三层划分**：模型结构层（GQA/MQA）→ 计算执行层（Flash-Attention）→ 服务部署层（PagedAttention + In-Flight-Batching）。课题聚焦层级 3，前两层为模型/实现选型，不进入优化范围。
- **需同步更新**：`AGENTS.md` §1/§2/§3/§5、`experiments/plans/strategy_design_literature_basis.md` §7、`motivation/plans/workloads.md`、`opening/report/opening_report.md`、`PROJECT_OUTLINE.md`
- **注意**：同期三个评估 skill（idea-evaluator / ars-reviewer / nature-reviewer）接收的是旧 framing（AI_EMBED + 静态 batch）；新 framing（AI_COMPLETE + 动态 batch + Ray 架构）更强。评估结果到达后应做 framing 对比再最终确认。

## 2026-07-15 开题报告移除 fake/CPU 主文证据

- 根据当前已经完成 pgai SQL 触发面集成和真实 GPU-backed `AI_EMBED` 完整链路复测的事实，更新 `opening/report/opening_report.md` 和 `opening/feishu/opening_report_wiki.md`。
- 删除 4.2 中历史 fake/CPU 预研图、表和相关表述，避免读者误解课题仍停留在 toy/fake benchmark 阶段。
- 4.2 可行性证据现在只保留 PG18.4 + pgvector 环境、GPU-backed `AI_EMBED` 链路和双 endpoint Ray 动机测试；调优变量依据改为文献机制 + 当前真实 GPU-backed 复测。
- 已覆盖同步新版开题报告飞书 docx，并重新插入 8 张正式 PNG；回读确认 revision 更新到 `72`，未检出 fake/CPU、图 4-7、表 4-4、Mermaid 旧图或本地 `figures/` 路径残留。

## 2026-07-15 开题报告飞书新版 docx 同步

- 使用 user 身份将 `opening/feishu/opening_report_wiki.md` 覆盖同步到新版开题报告飞书 docx：`https://my.feishu.cn/docx/CRgXdyTlToXpgjxo3otcf3kInGb`。
- 覆盖写入后飞书返回 `partial_success`，原因是 Markdown 中的本地图片路径不能直接导入为图片资源；随后逐张上传并插入 8 张 PNG：研究缺口图、总体研究框架图、三层上游执行策略图、运行时策略闭环图、粒度对比图、阶段时延图、endpoint 对比图、pgvector 写回对比图。
- 回读线上文档确认 revision 更新到 `51`，关键图注附近为真实飞书图片 URL；关键词检查未发现本地 `figures/` 路径和旧的“三岛/Killer/联合最优/边界确认/阶段画像/Ours-v0”等表述残留。

## 2026-07-15 策略设计与实现参考沉淀

- 新增 `experiments/plans/strategy_design_implementation_reference.md`，把 Ray OSDI 2018、Ray Data / Ray Serve、vLLM / Orca、Triton、GPU 数据放置和 DB AI 算子文献机制沉淀为三层策略参考。
- 明确三层策略：计划层负责数据库侧 `batch_size` / `partition_count` / `object_merge`；运行层负责 `K_max` / routing / backpressure / actor pool；服务端层负责 dynamic / continuous `micro-batch`。
- 文档同时给出每层可观测信号、可调变量、实现边界、实验指标、baseline 顺序和实现优先级，供后续实验设计和原型实现使用。
- 进一步补充“系统优化蓝图”和“机制到实现任务优先级”，将 Workload Profiler、Plan-time Data Organizer、Ray Admission Controller、Endpoint Router、Service-side Micro-batcher、E2E Guardrail 拆成可实现模块，并给出每个模块的借鉴来源、最小实现、验证问题和放弃条件。
- 同步更新 `experiments/plans/README.md`、`experiments/plans/strategy_design_literature_basis.md` 和 `PROJECT_INDEX.md`。

## 2026-07-15 GPU 调度与数据放置补充调研

- 新增 `opening/literature/gpu_scheduler_data_placement_supplement_20260715.md`，补充 GPU / LLM 推理调度、异构数据管线、GPU 数据库算子、GPU-resident 数据放置和数据库 AI 算子几条文献线索。
- 明确当前策略不应写成“重新发明 GPU scheduler”或“改造 Ray 调度器”，而是位于数据库外部执行链路和模型服务入口之间的轻量级 runtime strategy controller。
- 同步 `opening/README.md`、`opening/literature/reading_list.md` 和 `PROJECT_INDEX.md`，将该补充调研纳入开题文献入口。

## 2026-07-15 策略设计重新评判与三层收窄

- 根据用户反馈和补充调研，将策略从“全运行时控制器”收窄为 three-layer upstream execution strategy：计划层在执行前选择 `batch_size` / `partition_count` / `object_merge`，运行层调整 `K_max`、routing、backpressure，服务端用 dynamic / continuous batching 形成推理 `micro-batch`。
- 明确当前不采用“运行时重切数据库侧已物化 RecordBatch”作为主方案；动态 batch 借鉴 vLLM / Ray Serve / Triton 思路，放在模型服务侧尚未执行的请求队列中。
- 补充 Ray OSDI 2018 调度思想映射：task/actor、resource-aware scheduling、local/global scheduler、object store locality 和 actor pool 可迁移为 task 粒度、actor 池、资源约束、placement/locality、`K_max` 与 routing 等实验变量。
- 同步更新 `figures/scripts/generate_runtime_strategy_control_loop.py`、`figures/audit/runtime_strategy_control_loop_audit.md`、`figures/audit/top_venue_strategy_figure_design_notes.md`、`experiments/plans/strategy_design_literature_basis.md` 和 `PROJECT_INDEX.md`；重新生成 PNG/SVG 并通过边框、箭头和禁用术语自检。

## 2026-07-16 实验计划与开题报告同步更新

- **开题报告对齐实验计划**：6 项修改——
  1. §4.1 新增 Killer Experiment 六组对照（BL1-BL6）定义，明确核心 claim 的验证条件
  2. §4.2 新增"合理默认 vs 诊断工具"区分：逐行调用和无界 in-flight 仅作为诊断工具，不作为论文 §7 方法对照 baseline
  3. §3.2 研究内容三 扩展：提及 B 系列工程实验和三路写回架构对比（driver / worker-direct / queue-worker）
  4. §4.1 末尾新增 FILTER/COMPLETE 模拟 workload 诚实声明（参照 Orca 合成权重做法）
  5. §6 新增统计严谨性指标（中位数、IQR、重复次数、Ray 重启、随机种子）
  6. §2.3 补上 ColStorEval[50] 引用
- **PROJECT_INDEX.md 同步更新**：§3 新增四个实验计划文件列表、§7 更新研究内容三标题、§8 更新下一步优先级（P0/P1/P2 结构）
- **PROJECT_OUTLINE.md 同步更新**：研究内容三标题降级为"边界分析与轻量写回优化"、近期优先级改为 P0/P1/P2 三阶段
- 上述修改使开题报告与 `experiments/plans/` 下四份实验计划在 BL 矩阵、baseline 分级、统计规范和 workload 标注上口径一致

## 2026-07-16 实验计划六项评估方法论修正

- **修正四个实验计划文件**，统一遵循从 vLLM/Orca/TurboVecDB/GaussML/FlexPushdownDB 五篇 CCF-A 论文提取的六项评估标准：
  1. **前置依赖声明**（§0）：每个文件写明 P0 必须先完成 vLLM + B 系列，否则所有 baseline 是 suboptimal
  2. **假设先行**（§2）：每个实验段在参数矩阵之前先写"要推翻什么假设"，不是盲目扫参——每个假设标注对应实验段和推翻后的含义
  3. **模型 batch scaling 前置实验**（研究内容一 §4）：在讨论 batch_size 选择之前，先跑模型自身的 batch_size→吞吐曲线
  4. **FILTER/COMPLETE 诚实标注**（研究内容一/二 §3）：标注为 simulated workload（参照 Orca 合成权重的做法）
  5. **统计规范**（各文件 §10）：重复次数、中位数（不取平均值）、IQR、Ray 状态重置、warm-up 策略、随机种子——全部标准化
  6. **可验证边界**（各文件 §11）："When does it NOT help?" 的每个边界条件对应一个可跑实验点，不是空洞自省
- 修改文件：`data_organization_batching.md`（重写）、`service_scheduling_backpressure.md`（新增 §0/§2/§10，修正 §9/§11）、`sink_writeback_coordination.md`（新增 §0/§2/§10，修正 §9）、`cross_layer_killer_experiment.md`（新增 §0/§2）

## 2026-07-16 实验计划骨架填充 + 评估方法论标准化

- **四个实验计划文件新建**：`experiments/plans/` 下三份研究内容实验计划 + 一份跨层 Killer Experiment 计划。
  - `data_organization_batching.md`（研究内容一）：Grid search、workload 对比、selectivity-aware 策略、模型 batch scaling 前置实验
  - `service_scheduling_backpressure.md`（研究内容二）：K_max sweep、routing 策略、adaptive vs static K_max、vLLM baseline 前置实验
  - `sink_writeback_coordination.md`（研究内容三）：B 系列工程 baseline（UPSERT vs COPY, logged vs unlogged, online vs deferred index）、三路架构对比、sink 对照
  - `cross_layer_killer_experiment.md`（跨层核心）：BL1-BL4 + 联合方案的完整矩阵、代价模型 R²、消融瀑布、跨 workload 泛化、统计严谨性要求
- **评估方法论标准化**：所有四个计划遵循从 vLLM (SOSP 2023)、Orca (OSDI 2022)、TurboVecDB (VLDB 2025)、GaussML (ICDE 2024)、FlexPushdownDB (VLDB 2021) 五篇 CCF-A 论文提取的共同原则——曲线 > 单点、先暴露瓶颈再优化、同硬件公平 baseline、消融拆开、诚实报告边界、统计严谨。
- **实验前置依赖明确**：P0 必须先跑 vLLM 接入 + B 系列写回工程 baseline，否则所有 Grid Search 都基于 suboptimal baseline。
- 同步更新：`experiments/plans/README.md`、`PROJECT_LOG.md`。

## 2026-07-16 Baseline 分级重构：移除 strawman C 级

- **Baseline 分级重构**：`experiments/plans/research_design_catalog.md` §10.1-§10.4。
  - 移除 "C 级（Naive）"——row-by-row 调用、无界 in-flight 等故意劣化配置降级为"诊断工具"，只用于 §4 理解瓶颈机制，不作为 baseline 对照。
  - "合理默认配置"（coalesced batch=64、driver 写回）取代旧 C 级作为 §4 动机展示的参照点——这是正常工程师会写的第一版代码，不是 strawman。
  - S/A/B 三级保留：S（文献最优）→ A（工程最优）→ B（单维调优）。§7 方法对照至少包含 A 级。
  - §10.1 新增原则 5："动机展示不用 strawman"。§10.3 检查清单新增两条防 strawman 项。
  - A2.1 baseline 描述从 "Unbounded in-flight" 改为 "Ray 默认行为（无显式 K_max）"。
- **未同步到其他文件**：`baseline_reference.md`、`AGENTS.md` 和 `experiments/plans/README.md` 的 strawman 相关措辞已经合理，无需修改。

## 2026-07-15 研究方案候选目录 + Baseline 设计考量

- **新增研究方案候选目录**：`experiments/plans/research_design_catalog.md`，覆盖三个研究内容和跨层协同优化的 28 个候选方案，每个方案在六个维度（文献支撑、工程可行性、硬件可行性、开源依赖、创新空间、实验可验证性）上评分。
- **方案来源**：基于 57 篇文献清单 + 2026 年 7 月前沿检索（Ray Serve 2025 Custom Router/Async Inference/Autoscaling、NexusSched 两层调度、Multi-Bin Batching 队列理论、MAB 反馈控制、GFS/DARIS 优先级抢占、Arrow Flight Ballista/Spark SPIP、Iceberg v3 Deletion Vectors、COSTREAM/CONCERTO/GRACEFUL Learned Cost Models）。
- **Baseline 设计考量**：目录第 10 节为每个候选方案指定了对应 baseline（文献最优 S 级 / 工程最优 A 级 / 常见实践 B 级 / Naive C 级），并给出实现优先级（P0: COPY deferred index + vLLM 接入）。
- **分阶段路线图**：Phase 0-4，覆盖 2026-07 至 2026-10，Phase 3 的 Killer Experiment（BL1-BL6）是论文核心 claim 的验证点。
- **风险分析**：6 项风险（vLLM 消除外部调度收益 / 单 GPU 限制 / 写回优化边际 / workload 扩展 / Joint Opt 增量 < 10% / PG18.3 平台），每项标注了反证条件。
- 同步更新：`experiments/plans/README.md`、`PROJECT_LOG.md`。

## 2026-07-16 写回文献调研 + Baseline 矩阵 + 文献优先设计规则

- **文献清单 v3**：`opening/literature/ai_operator_literature_inventory.md` 从 45 篇扩充至 57 篇，新增写回/持久化方向 12 篇 CCF-A 文献（第六组精读 + E 组补充）。
- **新增实验 Baseline 参考矩阵**：`experiments/plans/baseline_reference.md`，覆盖 GPU 调度侧（6 个）、写回侧（7 个）、数据组织侧（4 个）、跨层决策侧（3 个），所有 baseline 标注来源论文/系统。
- **新增文献优先设计规则（§6.5）**：根 `AGENTS.md` 加入"系统/算法/实验方案设计时，优先从 CCF-A 文献提取设计模式"的规则。完整方法论写入 `research/README.md` §文献优先设计方法论。
- **idea-evaluator 评估**：课题方向 Accept with Revisions，无 CRITICAL 缺陷，paradigm-shift probe 4/4 yes。五项调整建议已记录在对话中。
- 同步更新：`AGENTS.md` §6.5、`research/README.md`、`experiments/plans/README.md`、`experiments/plans/baseline_reference.md`（新建）。

## 2026-07-13 制图脚本目录归位

- 将 `code/scripts/make_chain_breakdown_figures.py` 迁移到 `figures/scripts/make_chain_breakdown_figures.py`。
- 明确 `code/scripts/` 只放实验主体、服务启动、数据采集和 profiling 入口；绘图、图表复现和素材筛选脚本统一放入 `figures/scripts/`。
- 同步更新 `code/AGENTS.md`、`code/README.md`、`code/scripts/README.md`、`figures/scripts/README.md` 和 `figures/learning/README.md`，避免后续实验代码目录与图资产目录混用。

## 2026-07-13 图资产规则沉淀

- 根据今天系统架构图和实验数据图的多轮修改经验，扩展 `figures/AGENTS.md` 为项目级图表长期规则文件。
- 规则中明确：论文级核心图先用 `figure-designer` 判断图型和版式；投稿/论文级质检可用 `nature-figure`；报告转 PPT 可用 `nature-paper2ppt` 或 `ppt-master`；实验数据图优先用 Python + Matplotlib / Seaborn 从 CSV 可复现生成。
- 补充系统架构图常见返工点：箭头遮挡、编号越界、模块未对齐、框内内容不规整、观测层和执行层割裂、图文术语不一致。
- 补充实验图规则：哪些数据值得画图，哪些只适合表格或文字；正式图必须标注数据来源、warm-up 处理、证据层级和不能声称的结论。
- 在 `PROJECT_INDEX.md` 顶部补充图资产规则入口，提醒后续新增、修改、迁移或审查图表前先读 `figures/AGENTS.md`。

## 2026-07-13 项目目录一致性复核

- 复核根目录、`overview/`、`research/`、`motivation/`、`learning/`、`opening/` 和 `figures/` 中与当前开题方向相关的入口文件。
- 将 `overview/project_outline.md` 从旧的“数据库内置 AI 算子外部执行链路”口径重写为“数据库驱动 AI 工作负载的分布式数据执行与存储协同优化”口径。
- 同步更新 `AGENTS.md`、`PROJECT_INDEX.md`、`research/literature_and_evidence_review.md`、`research/existing_ai_operator_execution_chains.md`、`motivation/plans/integration.md` 和 `motivation/results/README.md` 中的旧表述。
- 对 `motivation/results/pg18_4_fake/system_profile.md` 与 `motivation/results/fake_cpu/analysis.md` 增加当前口径说明，保留历史实验语境，但明确真实瓶颈归因应优先引用 GPU-backed 结果。
- 本次复核只调整会影响项目规划、阅读入口和方向判断的文件；历史日志和旧实验过程记录不做大面积改写。

本文件记录项目级简要操作，便于日后复盘方向、入口和关键材料调整。详细实验日志仍放在对应结果目录；开题材料的详细修改记录见 `opening/logs/project_log.md`。

## 2026-07-13 开题主线调整为数据库驱动 AI workload

- 根据用户确认的判断，将开题题目从“面向数据库 AI 算子的模型服务感知批处理执行与写回协同优化研究”调整为“面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究”。
- 同步更新项目级方向口径：数据库 AI 算子主要作为 workload 入口和验证场景，研究主体调整为 Daft/Arrow 数据组织、Ray 执行调度、GPU 模型服务和 Lance / pgvector / PostgreSQL sink 之间的数据执行与存储协同。
- 同步修改 `README.md`、`PROJECT_OUTLINE.md`、`PROJECT_INDEX.md`、`AGENTS.md`、`overview/current_direction_and_plan.md`、`motivation/plans/integration.md` 以及 opening 相关源稿，避免项目规划与开题报告割裂。
- 已生成新的本地飞书源稿 `opening/feishu/opening_report_wiki.md`；飞书写入时 `lark-cli` 在用户目录刷新锁文件处返回 `Access is denied`，提升权限重试被自动审批拒绝，需后续获得权限后再同步线上 wiki。

## 2026-07-12 根目录总纲与项目日志

- 新增 `PROJECT_OUTLINE.md`，作为根目录项目总纲入口，汇总当前题目、研究内容、实验主线、关键证据、近期优先级和同步规则。
- 新增 `PROJECT_LOG.md`，作为项目级简要操作日志，用于记录跨目录、影响项目方向或入口结构的调整。
- 后续如果开题报告、实验主线、项目方向或关键入口发生变化，需要同步更新 `PROJECT_OUTLINE.md` 和本日志。

## 2026-07-12 实验主线入口调整

- 将项目实验主线入口从 `feasibility/guide.md` 调整到 `motivation/README.md`、`motivation/plans/workloads.md`、`motivation/plans/integration.md`、`motivation/results/README.md` 和 `motivation/results/gpu/README.md`。
- 明确 `feasibility/` 只负责组件、环境和脚本可用性验证，不承担当前实验大纲、开题主线或 GPU-backed 性能结论职责。

## 2026-07-12 开题与项目规划双向同步

- 明确开题报告和项目规划不是单向关系：开题报告基于项目进展撰写；开题题目、研究内容、技术路线或侧重点调整后，也会反向影响项目规划、实验优先级和对外口径。
- 项目入口文档需要与 `opening/report/opening_report.md` 保持一致，不能长期出现不同方向。

## 2026-07-12 开题报告与飞书内容复核

- 按当前 `PROJECT_OUTLINE.md`、`motivation/results/README.md` 和 `motivation/results/gpu/README.md` 复核开题报告与飞书源稿。
- 确认 `opening/report/opening_report.md` 当前主线基本合适：正式证据优先引用真实 GPU-backed 结果，PG18.4 / fake / CPU 结果有边界说明。
- 清理 `opening/feishu/opening_report_wiki.md` 的本地源稿说明，避免发布到飞书后出现工作流元话语。
- 补充飞书后续计划：后续进入 PostgreSQL 18.3 内部平台复测，避免把 PG18.4 本地同构预演写成正式平台结论。
- 修正 `motivation/results/README.md` 中 GPU-backed 结果入口的过时措辞。

## 2026-07-12 实验结论写作标准

- 根据用户反馈，将 `learning/AGENTS.md` 的实验讲解标准提升为项目级实验结论写作参照。
- 更新 `PROJECT_OUTLINE.md`、`PROJECT_INDEX.md` 和 `opening/work_rules.md`，要求实验结论、数据分析、开题可行性分析和飞书实验摘要都说明实验目的、链路流程、参数含义、数据来源、结果读法、不能证明什么、结论类型和下一步验证。
- 后续正式报告可以比学习材料更凝练，但结论边界和分析精细程度不能低于 `learning/AGENTS.md` 的要求。

## 2026-07-12 开题实验飞书页与 PPT 生成

- 新增 `opening/feishu/motivation_feasibility_wiki.md`，按真实 GPU-backed 证据、fake/CPU 历史预研、可行性验证边界和下一步实验组织动机测试与可行性测试内容。
- 使用 user 身份覆盖写入动机测试与可行性测试飞书 wiki：`https://my.feishu.cn/wiki/R2MywYu12i2PtWk84Vzcbp9Lnme?from=from_copylink`，飞书返回成功并生成 5 个 Mermaid whiteboard。
- 基于学校 PPT 模板生成开题汇报 PPTX：`opening/slides/opening_defense_20260712.pptx`，内容来自开题报告、GPU-backed 动机实验和当前项目总纲。
- 已将 PPTX 以 user 身份导入为飞书在线幻灯片：`https://my.feishu.cn/slides/NXsJsm2FRlZAAgdSfAmcqk9rnCg`。
# 2026-07-14 pgvector(384) writeback comparison

- Updated `code/scripts/postgres_ai_operator_profile.py` so `--setup --embedding-dim 384` creates `document_embeddings.embedding_vector` as `vector(384)`.
- Ran the same GPU-backed Ray actor chain for no writeback, JSON text writeback, and pgvector `vector(384)` writeback.
- Added result report and CSV under `motivation/results/gpu/`.
- Added report-main figure `figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.png`.
- Updated opening report, learning walkthrough, figure indexes, and result indexes. Boundary: PG18.4 local rehearsal, not PostgreSQL 18.3 internal platform.

# 2026-07-14 合并 agent/postgres18-local-profile 分支并全项目校准

- 将 `origin/agent/postgres18-local-profile` 合并到 `main`，恢复 `opening/` 开题材料目录。
- 分支带来的重构：`validation/` → `feasibility/`，`motivation/` 脚本 → `benchmarks/`、设计文档 → `plans/`、结果按 `fake_cpu/cpu/gpu/pg18_4_fake` 分类。
- 新增目录：`deploy/`、`experiments/`、`figures/`、`learning/`、`opening/`、`projects/`。
- 创建 `CLAUDE.md` 作为 Claude Code 环境规则入口，导入全部 `AGENTS.md`。
- 全项目文档路径校准（12 个文件）：
  - 根 `AGENTS.md`：§3 证据更新为 GPU-backed 结果，§4 目录加新结构，§5 实验规则更新。
  - 根 `README.md`：目录树重写、标题对齐 `PROJECT_OUTLINE.md`、证据和运行命令更新。
  - `PROJECT_INDEX.md`：全文重写，所有路径更新，新目录入口，当前证据优先级。
  - `overview/current_direction_and_plan.md`、`overview/project_outline.md`：加弃用声明，指向根 `PROJECT_OUTLINE.md`。
  - `motivation/results/README.md`：从扁平文件列表重写为子目录结构。
  - `feasibility/benchmarks/README.md`：命令路径和脚本引用全部更新。
  - `opening/ppt_rules.md`：图表规则重写，引用 `figures/` 为权威来源，Python+Matplotlib 优先于 ECharts。
  - `opening/work_rules.md`：过期引用更新。
  - `experiments/AGENTS.md`：新增 `karpathy-guidelines` 和图表 skill 引用。
- 镜像同步规则：`CLAUDE.md` 和 `AGENTS.md` §9 包含相同的 6 行变更→更新清单，互相指向对方。

# 2026-07-15 开题报告研究方案图补充

- 将 `figures/architecture/cross_layer_method_framework.png` / `.svg` 调整为研究方案图，明确三类数据库 AI 算子、阶段画像、数据组织策略、模型服务调度策略、联合调优验证和写回瓶颈判定实验。
- 重绘 workload 区块为三张卡片：场景名、SQL 算子名、调度压力三行排版；图中移除 `RC` / `BL` 缩写、`Workload 入口`、`边界确认` 和未解释的 `vs` 表达。
- 在 `opening/report/opening_report.md` 与 `opening/feishu/opening_report_wiki.md` 的 Killer Experiment 段落后插入该图作为图 4-1，并顺延后续第 4 章图号。
- 新增 `figures/audit/cross_layer_method_framework_audit.md`，并更新 `figures/README.md` 的正式图资产说明。

# 2026-07-15 研究方案图作图规则同步

- 将研究方案图的版式和审查经验同步到 `figures/AGENTS.md`：方案图必须回答“我要做什么”，并按 workload、阶段画像、策略设计、联合验证和写回瓶颈判定组织。
- 明确禁止在正式可见图中使用 `RC/BL` 内部缩写、未解释的 `vs`、`边界确认` 等模糊标签；workload 区块优先使用“三行卡片”排版。
- 补充遮挡和越界检查要求：卡片边框必须完整可见，文字不得裁切，生成后同时执行程序化像素/关键词残留检查和人工 PNG 预览。
- 同步更新 `opening/ppt_rules.md`，要求 PPT 中的研究方案图也遵守同一套语义和排版规则。

# 2026-07-15 开题主线调整为上游链路调优与端到端效果评估

- 根据用户确认，将开题主叙事从“独立最优组合 vs 跨层联合最优”调整为“上游执行链路调优 + 端到端效果评估”：优化侧重点在数据组织与模型服务调度，尤其是模型服务状态感知调度；写回纳入端到端效果评价。
- 更新 `opening/report/opening_report.md` 和 `opening/feishu/opening_report_wiki.md`：研究路线改为“分阶段性能剖析 -> 上游执行链路调优 -> 加入写回的全链路验证 -> 多 workload 验证”，并将独立最优拼装对照降级为阶段间耦合明显时的增强对照。
- 更新 `figures/architecture/cross_layer_method_framework.*`：中心卡片改为“上游执行链路调优”，评价标准改为加入写回后的端到端耗时、吞吐、排队和写回占比整体改善。
- 同步调整 `PROJECT_OUTLINE.md`、`PROJECT_INDEX.md`、`experiments/plans/` 和 `figures/audit/` 中的入口说明，避免把跨层联合优化写成当前唯一核心 claim。

# 2026-07-15 上游执行链路策略设计图

- 新增 `figures/architecture/upstream_strategy_design.png` / `.svg`，用于说明阶段画像之后的已定位瓶颈如何转化为数据组织优化、模型服务调度、写回约束处理、执行配置与端到端验证。
- 新增 `figures/scripts/generate_upstream_strategy_design.py`，统一生成 PNG/SVG，并检查所有核心卡片边界和箭头是否越界或穿过无关卡片。
- 新增 `figures/audit/upstream_strategy_design_audit.md`，记录该图不声称最终 learned optimizer，而采用“已定位瓶颈 -> 优化动作 -> 执行配置 -> 端到端验证”的保守方法图定位。

# 2026-07-15 策略设计文献依据与边界

- 新增 `experiments/plans/strategy_design_literature_basis.md`，将策略设计从文献中站住：区分 Cortex/Smart、vLLM/Orca、Ray/Daft、COPY/pgai/Delta/TurboVecDB 等工作中可借鉴的优化思想、只能作为 baseline/边界的部分，以及本文自己的上游执行链路策略定义。
- 明确当前策略推荐写成 “workload-aware 数据组织 + 模型服务状态感知调度 + 写回约束验证”，不提前声称 finalized learned optimizer、通用 Ray Serve 调度器或存储引擎优化。
- 同步更新 `experiments/plans/README.md` 和 `PROJECT_INDEX.md`，要求后续更新策略设计图或方法口径前先查阅该文件。

# 2026-07-15 Ours-v0 优化策略逻辑图

- 新增 `figures/architecture/optimization_strategy_logic.png` / `.svg`，将优化策略细化为“输入信号 -> 规则表选择器 -> 策略动作与配置 -> 端到端验证与回填”。
- 新增 `figures/scripts/generate_optimization_strategy_logic.py`，统一生成 PNG/SVG，并检查核心卡片边框、底部验证框和 7 条箭头是否越界或穿过无关卡片。
- 新增 `figures/audit/optimization_strategy_logic_audit.md`，记录该图的文献边界和可见标签要求：不声称 finalized learned optimizer，不把写回或跨层联合最优作为当前主 claim。
- 同步更新 `figures/README.md` 和 `PROJECT_INDEX.md`，将该图纳入正式架构/方法图资产。

# 2026-07-15 顶会系统论文策略图范式整理

- 新增 `figures/audit/top_venue_strategy_figure_design_notes.md`，从 vLLM、Orca、Cortex AISQL、Ray Data 等系统论文图形中抽取方法图范式：优先画运行时机制、running example、data/control path 区分和紧凑规则表，而不是三列术语堆叠。
- 明确下一版策略图建议采用 “control-loop + running example + compact rule table”：上半部画 database AI query 到 batch queue、strategy selector、actor/endpoint、sink、E2E metrics 的控制循环，下半部画 Trigger -> Action -> Guardrail 规则表。
- 同步更新 `figures/README.md` 和 `PROJECT_INDEX.md`，要求后续重绘策略图前先阅读该设计备忘。

# 2026-07-15 策略图小机制拆分与论文下载清单

- 新增 `figures/audit/strategy_figure_micro_design_points.md`，将后续策略设计图拆分为可独立绘制的小优化点：workload-aware batch/partition、bounded in-flight 反压、endpoint routing、写回约束和 Trigger -> Action -> Guardrail 规则表。
- 为每个小机制记录优化对象、参考论文图形范式、建议画法、所需实验证据和 reviewer 风险，避免继续画成“大而全”的术语堆叠图。
- 补充 vLLM、Orca、Ray Data Streaming Batch、Cortex AISQL、Sarathi-Serve、DistServe、Splitwise、FlexPushdownDB 等优先下载/精读链接，并同步更新 `figures/README.md` 和 `PROJECT_INDEX.md`。

# 2026-07-15 本地参考文献 PDF 子集登记与图形阅读

- 新增 `opening/literature/reference/README.md`，登记用户已下载的 14 篇本地 PDF 子集，包括 Ray Data、vLLM、Ray、Sarathi-Serve、ServerlessLLM、GaussML、Galois、LEADS、NeurDB、Lance 等；明确该目录只是部分文献，不替代完整文献清单。
- 新增 `figures/audit/local_reference_figure_reading_notes.md`，记录从本地 PDF 图中提取的图形经验：用 `AI_EMBED` running example 锚定主图、把策略动作贴到执行位置、区分数据/控制/反馈流、用规则表或 mini timeline 补充机制。
- 更新 `opening/README.md`、`opening/literature/reading_list.md`、`figures/README.md` 和 `PROJECT_INDEX.md`，将本地 PDF 子集和图形阅读笔记纳入项目入口。

# 2026-07-15 Ours-v0 运行时策略闭环图

- 新增 `figures/architecture/runtime_strategy_control_loop.png` / `.svg`，用一个 `AI_EMBED` SQL 运行例子贯穿 RecordBatch queue、Ray submit gate、Endpoint queues、GPU model service、Results/sink 和 E2E metrics，直接展示 batch/partition、K_max、routing 和 writeback guardrail 的作用位置。
- 新增 `figures/scripts/generate_runtime_strategy_control_loop.py`，统一生成 PNG/SVG，并执行核心卡片边框、主数据流箭头和禁用术语自检；本次生成已通过程序化检查和 PNG 人工预览。
- 新增 `figures/audit/runtime_strategy_control_loop_audit.md`，记录该图为策略机制主图；`cross_layer_method_framework.*` 保留为研究方案总览，`upstream_strategy_design.*` 保留为过渡图，`optimization_strategy_logic.*` 降级为规则表草图。
- 同步更新 `figures/README.md` 和 `PROJECT_INDEX.md` 的图资产入口。
# 2026-07-15 策略图迭代版本归档

- 将暂时不用的策略图迭代版本移入 `figures/archive/architecture/20260715_strategy_iterations/`：`upstream_strategy_design.*`、`optimization_strategy_logic.*` 和内部字体测试图 `_font_test.png`。
- 当前策略设计说明优先使用 `figures/architecture/runtime_strategy_control_loop.*` 与 `figures/architecture/runtime_strategy_rule_table.*`。
- 同步更新 `figures/README.md`、`PROJECT_INDEX.md` 和相关审计文件中的路径说明，避免旧图继续出现在当前主图清单中。

# 2026-07-15 策略闭环图中文化与箭头修正

- 将 `runtime_strategy_control_loop.*` 和 `runtime_strategy_rule_table.*` 的可见标签尽量中文化，仅保留 `AI_EMBED`、`SQL`、`GPU`、`K_max`、`P99`、`token` 等必要技术记号。
- 将主流程框从泛泛的状态字段改为“观测量 / 调节项 / 判定项 / 约束项 / 评价项”，说明这些框是策略选择器读取的信号来源及其作用。
- 收窄主流程卡片、拉大间距并缩小箭头头部，使主数据流箭头有完整线段；重新生成 PNG/SVG 后通过边框、箭头和禁用术语自检。

# 2026-07-15 开题报告 architecture 图同步到三层策略

- 重绘 `figures/architecture/system_architecture_ai_data_execution.*`，将总体架构图同步为计划层数据组织、运行层入口调度、服务端 dynamic micro-batch 与写回瓶颈判定的当前口径。
- 重绘 `figures/architecture/cross_layer_method_framework.*`，将研究方案图从“上游链路调优”进一步明确为“三层上游执行策略与端到端评价”。
- 将 `figures/architecture/runtime_strategy_control_loop.*` 补入 `opening/report/opening_report.md` 与 `opening/feishu/opening_report_wiki.md` 作为图 4-2，替代原 Mermaid 链路示意，用于解释策略机制。
- 同步更新 `figures/README.md`、`figures/audit/*` 和 `PROJECT_INDEX.md`，去除当前主图入口中的 `Ours-v0`、`下一轮配置`、`边界确认` 等旧表述。

# 2026-07-15 architecture 图颜色语义修正

- 将 `cross_layer_method_framework.*` 中的三层策略改为三个并列中性卡片：计划层、运行层、服务端层，避免被误读为两个策略框或与上方 workload 颜色一一对应。
- 将 `system_architecture_ai_data_execution.*` 底部研究内容卡片统一改为中性色；上方系统阶段继续保留数据层、Ray 执行层、GPU 模型服务和结果存储的颜色编码。
- 将研究内容 2 标题调整为 `运行层调度与服务端批处理`，更准确表达当前方案横跨 Ray 入口调度、endpoint routing 和模型服务侧 `micro-batch`。

# 2026-07-15 研究缺口图与候选规则表口径修正

- 重绘 `research_gap_three_islands.*`，将底部研究内容同步为数据组织与批处理构造、运行层调度与服务端批处理、写回瓶颈判定，避免旧的“GPU 服务感知调度”与当前三层策略不一致。
- 将 `runtime_strategy_rule_table.*` 从“策略规则表”改为“候选策略规则表”，明确表中规则是待实验验证的触发逻辑，不代表已证明结论。
- 同步更新 `figures/README.md`、`PROJECT_INDEX.md` 和相关审计记录。

# 2026-07-15 开题报告正文同步三层策略口径

- 更新 `opening/report/opening_report.md` 和 `opening/feishu/opening_report_wiki.md`，将文献综述、研究目标、研究内容、研究方案、进度安排和预期成果同步到当前方向。
- 研究内容二统一表述为“运行层调度与服务端批处理协同方法”，覆盖 `K_max`、endpoint routing、actor pool、backpressure 和服务端 `micro-batch`。
- 将方向三改为“写回瓶颈判定与端到端收益检查”，避免把写回写成当前独立主贡献。
- 清理旧的“岛”“GPU 调度优化”“联合最优/Killer Experiment”等主叙事表述，保留其作为后续增强对照的可能性。
