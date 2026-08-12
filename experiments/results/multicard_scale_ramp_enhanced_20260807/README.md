# Enhanced multi-card scale ramp: bounded HTTP and DuckDB AI

## Status and scope

This directory preserves the compact, auditable evidence for the 2026-08-07
enhanced scale ramp. The rerun added during-cell vLLM gauge sampling and the
identity sidecar needed by the project evidence contract; it did not introduce
a new scheduling method or change the existing comparison boundary.

- Workload: SQuAD v1.1 short-answer, scales 64, 128, 256, 512, 1,024, 2,048,
  4,096, 8,192 and 10,570.
- Service: 2 x RTX 4090, two Qwen2.5-7B vLLM endpoints, prefix cache enabled,
  `max_num_seqs=256`, `max_num_batched_tokens=8192`.
- Arms: direct bounded HTTP at concurrency 32 per endpoint, and the
  harness-pre-split DuckDB AI diagnostic at concurrency 32 per endpoint.
- Repetitions: three measured cells per scale and arm.

The direct arm passed 27/27 cells. DuckDB AI passed 22/27: repetitions 2 and 3
at scale 8,192 and all three repetitions at scale 10,570 failed. These failed
cells remain part of the archive through `run_error.json`, `run_status.json`
and the available partial evidence; they were not dropped from aggregation.

## Evidence layout

- `ramp_run.json`: authoritative status for all 54 cells.
- `ramp_aggregate.json` and `ramp_aggregate.md`: all passed-repetition values,
  means and sample-CV summaries.
- `scale_*/<arm>_rep*/identity.json`: comparison role and scheduler owner.
- `gpu_resource.csv`, `ttft_metrics.json` and `vllm_gauges.json`: GPU/energy,
  TTFT/ITL/service counters and during-cell running/waiting/KV observations.
- `gate_config.json`, `resolved_config.json`, `commands.json`, `gate.json`,
  `service_counters.json`, shard `summary.json` and
  `manifest_metadata.json`: executable contract, correctness gate and compact
  per-shard evidence.

The aggregate is reproducible from the archived files with
`code/scripts/analysis/multicard_ramp_aggregate.py`; a 2026-08-12 clean rebuild
matched both committed aggregate files byte for byte.

## Archive policy and conclusion boundary

The server audit intentionally excluded `requests.csv` and shard logs. They
accounted for most of the remaining volume, may contain generated text, and
are unnecessary for rebuilding the aggregate or checking cell status,
identity, service pressure and resource metrics. The full server directories
remain untouched.

This is a two-path scale/capacity diagnostic, not a native multi-endpoint
product ranking and not evidence that a project scheduler wins. Bounded HTTP
is a direct-client control; the DuckDB arm is explicitly
`harness_pre_split_diagnostic`. Request-granularity and query-barrier timing
must remain separate. The cross-path interpretation and the four-path metric
table are maintained in
`../multicard_proj_scale_ramp_formal_20260807/README.md` section 9.
