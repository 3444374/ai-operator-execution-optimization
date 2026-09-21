# Enhanced multi-card LBRR scale ramp

## Status and scope

This directory preserves the compact, auditable evidence for the 2026-08-07
enhanced LBRR scale ramp. The path is one DuckDB AI process through an nginx
round-robin gateway to two vLLM endpoints. Its identity remains
`gateway_system_diagnostic`; it is not a DuckDB-native multi-endpoint baseline.

- Workload: SQuAD v1.1 short-answer, scales 64 through 10,570 over the same
  nine-point grid as the bounded/DuckDB ramp.
- Service: 2 x RTX 4090, two Qwen2.5-7B vLLM endpoints, prefix cache enabled,
  `max_num_seqs=256`, `max_num_batched_tokens=8192`.
- Concurrency: 64 at the single gateway-facing DuckDB process.
- Repetitions: three measured cells per scale; 27/27 passed.

## Evidence layout

- `ramp_run.json`: authoritative status and backend request/work skew for all
  27 cells.
- `ramp_aggregate.json` and `ramp_aggregate.md`: all repetition values, means
  and sample-CV summaries.
- Each `scale_*/lb_rr_c64_rep*/` directory keeps `identity.json`,
  `gpu_resource.csv`, `ttft_metrics.json`, `vllm_gauges.json`, shard
  `summary.json` and `manifest_metadata.json`.

The aggregate is reproducible from these files with
`code/scripts/analysis/multicard_ramp_aggregate.py`; a 2026-08-12 clean rebuild
matched both committed aggregate files byte for byte.

## Archive policy and conclusion boundary

The server audit intentionally excluded per-request `requests.csv` and logs.
The full server directory remains untouched. The committed evidence is enough
to audit status, identity, backend balance, service counters, TTFT/ITL,
running/waiting/KV and GPU/energy metrics without committing generated text.

The LBRR path uses query-barrier timing and a third-party gateway, so it must
not be ranked as a native DuckDB scheduler or mixed with request-level E2E
latency. Its scale curve is a gateway-system diagnostic. The joint four-path
interpretation is maintained in
`../multicard_proj_scale_ramp_formal_20260807/README.md` section 9.
