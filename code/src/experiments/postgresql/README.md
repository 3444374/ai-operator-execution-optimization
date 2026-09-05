# PostgreSQL resource measurement tools

These tools observe the existing synchronous SemMap fixture path. Production SQL, planner, provider and
wire semantics remain in their existing modules. The [Map engineering contract](../../../../experiments/plans/postgresql_semmap_generation_contract.md)
owns the implementation and verification plan.

| Module | Responsibility |
|---|---|
| `resource_lifecycle.py` | Immutable settings, required phases and pure final assessment |
| `resource_phase.py` | Baseline, operation checkpoint, cleanup sampling and phase evidence |
| `semmap_resource_runner.py` | Exclusive run directory, build, isolated PG cases and CLI |
| `resource_qualification.py` | Sampled peak and cleanup policies with versioned thresholds |
| `provider_session_attribution.py` | Strict session/task replay, scoped socket attribution and residual identity checks |
| `semmap_resource_gateway_observer.py` | Experiment-only peer/socket/session observations |
| `semmap_resource_fault_gateway.py` | Separate test-only handshake barrier for observable fault/recovery connections |
| `resource_client_v3.c` | Parameterized single-row libpq fixture consumer with an explicit exit barrier |
| `runtime_helpers.py` | Shared owned-process and isolated PostgreSQL helpers, also used by choice checks |

Collector and recorder primitives live in `src/observability/process_resources/`. Test categories are
lifecycle, collection, attribution, policy, observer and CLI; the old `audit_round2` source-string checks
have been replaced by observable behavior checks. Diagnostic mode is 1×100 and never grants formal
qualification. See [CLI usage](../../../scripts/README.md) and [current evidence](../../../../experiments/results/postgresql/semmap_resource_lifecycle_20260906/README.md).
