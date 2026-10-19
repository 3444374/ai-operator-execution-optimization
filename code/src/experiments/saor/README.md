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
- `native_system_observation.py` validates the common pass-through gateway trace and
  derives T0--T4, request SLO/tails, actual-token weighted fairness/service lag, and
  explicitly within-run (not full-solo) interference observations for all five arms.
- `native_system_matched.py` owns authorization and cell orchestration;
  `native_system_summary.py` revalidates sealed evidence and produces only the
  five-arm system, per-Job, and resource tables.
- `vllm_0251_source_audit.py` checks the installed distribution and source hashes
  for native readiness. Its three source-identity tests remain in
  `test_vllm_0251_source_audit.py`.

The unexecuted official S-LoRA FCFS/VTC and four-arm cross-layer prototypes,
including their plugin and pure policy oracle, have been retired. Their complete
sources and templates remain at the
[fixed historical commit](https://github.com/3444374/ai-operator-execution-optimization/tree/928a339334a4075fb2cf96a2dddc4727dec984fe).
Runtime compatibility, Job identity transport, and custom-FCFS qualification
were incomplete; they provide no model comparison or performance qualification.

Historical Project FIFO/DRR/VTC-style/strict-priority modules and evidence stay
available for internal attribution, but they are not native system baselines and
are not cells in the current five-arm matrix.
