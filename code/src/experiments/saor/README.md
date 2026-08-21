# SAOR experiment cores

This package contains reusable, typed experiment contracts and offline evidence
logic. Command-line scripts under `code/scripts/` must remain thin wrappers.

For the current multi-Job comparison:

- `native_system_contract.py` and `native_system_parser.py` define the five-arm
  database-E2E identity, typed Job releases, and MFU contract.
- `native_system_validator.py`, `native_system_evidence.py`, and
  `native_system_publisher.py` reject mixed evidence, redact persisted text, and
  publish fail-closed generations.
- `native_system_bindings.py` checks the matched contract against the native and
  Project executor configurations before dispatch.
- `native_system_readiness.py` composes the three-config static audit with the
  exact installed-source evidence, an immediate current-install re-audit, and
  the complete live service-identity/Python-runtime gate. A static pass is
  explicitly not rehearsal-ready.
- `native_system_execution.py` owns executor adapters; `native_system_sink.py`
  owns the shared PostgreSQL completion sink/readback correctness boundary.
- `native_system_matched.py` owns authorization and cell orchestration;
  `native_system_summary.py` revalidates sealed evidence and produces only the
  five-arm system, per-Job, and resource tables.
- `official_vtc_capability.py` is a separate, non-running official S-LoRA
  FCFS/VTC capability contract. It never enters the database-E2E ranking.
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
