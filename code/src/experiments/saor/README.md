# SAOR experiment cores

This package contains reusable, typed experiment contracts and offline evidence
logic. Command-line scripts under `code/scripts/` must remain thin wrappers.

For the current multi-Job comparison:

- `native_system_contract.py` and `native_system_parser.py` define the five-arm
  PostgreSQL-source-to-completion identity, typed Job releases, and MFU contract.
- `native_system_validator.py`, `native_system_evidence.py`, and
  `native_system_publisher.py` reject mixed evidence, redact persisted text, and
  publish fail-closed generations.
- `native_system_bindings.py` checks the matched contract against the native and
  Project executor configurations before dispatch.
- `native_system_readiness.py` composes four ordered gates. The live read-only
  system producer is `native_system_preflight.py`; it probes health/PG/Ray and
  recomputes bounded raw evidence instead of trusting passed booleans. A static
  or service-only pass is explicitly not rehearsal-ready.
- `native_system_artifacts.py` deep-validates actual correctness-smoke and
  rehearsal roots, cell artifact hashes, completion/provenance, and exact tar mirrors.
- `native_system_execution.py` owns executor adapters; `native_system_completion.py`
  validates executor completion traces against frozen manifest identities without
  writing to an output sink.
- `native_system_matched.py` owns authorization and cell orchestration;
  `native_system_summary.py` revalidates sealed evidence and produces only the
  five-arm system, per-Job, and resource tables.
- `official_vtc_capability.py` is a separate, non-running official S-LoRA
  FCFS/VTC capability contract. It never enters the five-arm ranking.
- `cross_layer_scheduler_capability.py` defines the separate four-arm SAOR
  versus DRR/VTC-on-vLLM complete-system capability and evidence boundary.
- `in_engine_scheduler_logic.py` is the dependency-free FCFS/DRR/VTC semantic
  oracle; `vllm_0251_source_audit.py` hashes the actual frozen install and can
  pass only against frozen expected distribution/source SHA values, while
  `vllm_scheduler_plugin.py` exposes a custom-FCFS parity class and deliberately
  blocked DRR/VTC reproduction class paths.

Historical Project FIFO/DRR/VTC-style/strict-priority modules and evidence stay
available for internal attribution, but they are not native system baselines and
are not cells in the current five-arm matrix.
