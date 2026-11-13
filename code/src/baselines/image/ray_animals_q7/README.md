# Ray Animals image Filters

The historical package/module path is retained for existing supervised callers.
The bound integer `query` selects original Q1/Q3/Q7/Q10; omission keeps Q7.
Q1/Q3/Q10 use one complete Filter, one native lifecycle and N model calls.
Q7 uses the original zebra/impala Filters and City intersection, two sequential
native lifecycles and 2N calls. All four reuse the same binding, worker, processor,
budget and final-result release; the official query owns its relational result.
The existing official sf64 manifest remains exactly 64 images, with 128 calls for
Q7 and 64 for each single-pass query; the
same entry also accepts the existing explicitly labeled finite software samples.
It does not relabel a sample as the complete official source or broaden the
manifest's supported input roles and size range.
The original prompts, parser (including its no-verdict default) and evaluator
remain responsible for semantics. A shared runtime or same-model warm run is
not claimed.

## Runtime and configuration

[config.example.json](config.example.json) gives the explicit engine and sampling
fields. `runtime_profile` selects one fixed combination:

| Profile | Ray | vLLM |
|---|---|---|
| `official-ray-0.22` | 2.56.1 | 0.22.0 |
| `unified-candidate-0.25.1` | 2.56.1 | 0.25.1 |

The example selects the official profile. The optional profile needs its own
qualification; results from the two environments must remain separate. Use
existing isolated driver/native environments and verified source, image-manifest,
model, tokenizer and processor identities. The package installs nothing.

## Supervised query

The outer host controller owns preflight, the single host/GPU lease, the complete
query deadline and its cleanup allowance. Before launching, it creates a fresh
private output directory, reserves an `AttemptBudget(budget_id, passes*N)`, and uses
`CellBudgetLedger` to reserve/claim a shared unit whose ID is `budget_id` in
`output/logical-unit.sqlite`. No warmup or retry is added.

Freeze `output/RUN-BOUND.json` with the fields below. Resource numbers are an
example allocation; the controller must bind the agreed actual values and the
SHA256 of each supplied helper/configuration file.
The following example is for the unchanged complete sf64 source. For another
accepted Q7 manifest, `physical_attempt_limit` must be exactly twice its verified
row count. The driver checks that count before either native stage is dispatched;
each actor uses that same frozen cap and the existing shared ledger.
For Q1/Q3/Q10 the complete cap is N instead of 2N. The shared task metadata
determines the number of full-input Filter passes, not a per-question executor.
There is no shared-runtime or same-model warm claim.

```json
{
  "source": "/absolute/maintained-checkout",
  "query": 7,
  "budget_id": "unique-q7-cold-id",
  "physical_attempt_limit": 128,
  "config": "/absolute/reviewed-config.json",
  "native_python": "/existing/native-environment/bin/python",
  "checkout": "/existing/pinned/SemBench",
  "manifest": "/existing/manifest.json",
  "manifest_digest": "verified-manifest-content-digest",
  "model": "/existing/model",
  "num_cpus": 8,
  "num_gpus": 1,
  "object_store_memory": 536870912,
  "host_adapter": {
    "host_helper": {"path": "/absolute/host_helpers.py", "sha256": "verified-file-sha256"},
    "socket_helper": {"path": "/absolute/private_temp_sockets.py", "sha256": "verified-file-sha256"},
    "resource_config": {"path": "/absolute/frozen-resource-config.json", "sha256": "verified-file-sha256"},
    "stage_timeout_s": 480
  }
}
```

Under the existing outer supervisor, use the LOTUS driver interpreter:

```sh
export PYTHONPATH="$Q7_SOURCE/code"
export LITELLM_LOCAL_MODEL_COST_MAP=True
"$DRIVER_PYTHON" -B -m src.baselines.image.ray_animals_q7.runner /absolute/output
```

`Q7_SOURCE` must equal the frozen `source`. [runner.py](runner.py) reads the bound
file and constructs [StageSupervisor](host_adapter.py) from `host_adapter`.
Each native stage uses `native_python`. Any nonfinal stage must establish worker exit,
physical idle, resource acceptance, owned-process exit and private-socket cleanup
before the next starts. The final stage publishes its complete predictions
atomically and holds its public Ray runtime until the original query returns its
complete query result. The driver records that memory-sink timestamp and sends an
explicit release; only then does the final worker call `ray.shutdown()` and the
same supervisor finish physical cleanup. The adapter uses the pinned host/socket helpers and the
maintained [inactive semaphore cleanup](../../../observability/process_resources/inactive_semaphores.py).
The host/resource checks reject recognized native workloads, GPU use and incomplete
inventories before unlinking. Unclassified Python is retained as an observation,
not evidence of contention or harmlessness. If new semaphores remain, its presence
at either boundary prevents time-window-only ownership inference and unlinking.

The adapter uses the maintained `process_resources.host_idle` policy through
`host_adapter.load_host`, while retaining the hash-checked helper's lease and owned
process cleanup. The outer owner must load the same policy for its initial/final
checks, not call the old helper directly:

```python
from src.baselines.image.ray_animals_q7.host_adapter import from_bound, load_host
host = load_host('bound_host', bound['host_adapter']['host_helper'])
api = host.production_api(Path(bound['source']) / 'code')
```

Bind `source`, its commit/code tree and the controller import path to the new
verified support checkout; the workload source remains independently pinned.
`from_bound` uses this loader for every stage. The private host helper bytes remain
unchanged; snapshot `schema` and `policy_sha256` identify the maintained guard.
Old `load_verified` remains only the generic hash-checked module loader (including
the socket helper), not the current host-policy entry. Archived owners still use
their frozen source and cannot inherit this new boundary qualification.
Private `TMPDIR`, `VLLM_RPC_BASE_PATH` and `RAY_TMPDIR` are per stage and restored
afterward. The outer lease stays held across all original Filter stages. A stage timeout is
separate from the mandatory outer query deadline.

The final prediction/release exchange is bounded by the existing stage timeout;
waiting for the driver does not restart the supervisor's timeout. The driver runs
that existing supervisor in one thread, retaining its process ownership and
cleanup rules. Parser failure releases the final worker with an abort. Native,
audit, close or host-cleanup failure fails the query and preserves available raw
predictions and `QUERY-failure.json`; a sink already observed is retained, not
published as a successful run. Stale input, prediction or release files are
rejected before dispatch. The public runtime hold does not change Ray Data's
internal actor lifetime or establish that its model engine remains resident.

If the post-stage idle guard rejects foreign work or a resource check fails,
`stage-N-supervision/EXIT.json` records a failed boundary, the observed worker
exit, resource/ownership checks and which cleanup steps were attempted.
`resources-at-failure.json` retains the last successful resource observation.
Unknown checks remain null or `not_reached`; a zero worker exit does not imply
host idleness. The idle guard still runs before socket/semaphore unlink, and a
rejection cannot authorize that cleanup. An original worker failure remains
primary if finalization also fails; evidence-write failures are attached to it.

## Evidence and timing

The outer controller reconciles both predictions files, native events and the
shared ledger: exactly 2N charged/start/confirmed-terminal occurrences, with no
missing, duplicate, error or unconfirmed request. Request IDs are local to an
owner; correlate PID/request ID and ledger attempt. `QUERY.json` completion alone
does not establish final physical resource acceptance.

The maintained `sembench.source_to_sink.v3` interval begins after manifest/pixel
and source-code checks, original function binding and LOTUS client setup.
It includes a fresh manifest scan, native image reading/decoding, all native
startups, required input/prediction transport, native work and any nonfinal
public Ray close and inter-stage cleanup. Its endpoint is the original complete
in-memory query result, before the final public Ray close, final host cleanup, message-summary
audit, evaluation, result serialization and report persistence. This is not a
zero-I/O execution profile: the existing native workers consume input files and
return predictions through files. Supervised Popen,
runner entry, native constructor return, request events and physical exit have
separate clocks. Full controller entry and first engine admission remain unknown
in this runner; request spans are not pure GPU time. The common definitions are
in [native baseline calibration](../../../../../experiments/plans/原生基线校准.md).

`timing.source_started_monotonic_ns` and `sink_ready_monotonic_ns` define the
versioned `source_to_sink_seconds`. `query_entered_monotonic_ns` remains the
earlier entry event, with its measured `query_entry_to_sink_seconds` retained
separately. Historical results retain their original wider interval; do not
reconstruct v3 by subtracting estimated preflight time. Model startup remains
inside v3, including both lifecycles for Q7; no ready-first or warm claim applies.

Qualification scope:

- The v3 boundary has 35 distinct local software checks, including unchanged
  original messages/parser/full results/evaluators for Q1/Q3/Q10 at N=1/7/13,
  the Q7 two-stage path, and a controlled clock over all four original queries.
  The latter retains source work and nonfinal cleanup while excluding preflight,
  client setup and post-sink finalization. A real CPU-child handoff initially
  failed because the macOS sandbox denied process-tree inspection; that single
  unchanged test passed with inspection permitted. Both outputs are retained.
  These checks used a clean candidate export excluding the three unrelated
  draft files, no model or native Ray runtime. Target qualification of this
  changed clock is pending; older GPU clocks do not acquire v3 identity.

- The shared one/two-pass candidate has local CPU checks for Q1/Q3/Q10 at
  N=1/7/13, their full original messages/parser/results/evaluators, single-stage
  failure/abort/close paths, and the existing Q7 two-stage owner release.
  The new test first compared unordered gold rows as ordered metadata; its
  correction compares all original gold rows without imposing an ORDER BY.
  Predictions and evaluator scores are unchanged. One earlier CPU-child attempt
  was blocked by macOS process-tree inspection; its failure is retained and the
  unchanged supervisor subsequently passed with process inspection permitted.
  New single-pass native SDK/GPU/physical-cleanup qualification is pending;
  historical Q7 GPU receipts below do not qualify this candidate or other queries.

- Frozen `c1648b796dbead66fa5a86423ae1137b26ac89b8` completed 128 public generate
  calls and 128 terminals, two constructors, original City F1=1, and message/token
  identity checks. Its two native stages each left a 32-byte semaphore and four
  inactive sockets requiring project-owned cleanup. This is a qualification run,
  not a single-owner warm or formal performance result.
  Its historical query clock included both stage closes and host cleanup; the
  new memory-sink interval cannot be reconstructed from that old clock.
- The maintained cleanup migration at `4e8f0a0354fcfef4857e4c2000107de322372eb1`
  passed target Linux imports, mocked cases and a real self-created POSIX
  semaphore close/unlink. Its first fixture was correctly rejected because the
  sampled lower bound followed the file mtime by 4.08 ms; the successful fixture
  used the actual outer stage-before boundary without timestamp changes or a
  tolerance. This accepts the cleanup interface, not a new Ray/GPU teardown run.
- The corresponding private handoffs remain
  `ray-q7-cleanup-main-target-cpu-20261110-r1/HANDOFF.json` (SHA256
  `949840bc4763bd9124f17671b563bcdf466033a7d5d1b84f315f225befc93d1f`)
  and `ray-q7-cleanup-main-target-cpu-20261110-r2/HANDOFF.json` (SHA256
  `3cb5b387ba24c8529e7898877a03b332a9c6229358b172d5553b308b35f19d93`).
  Original success/failure outputs retain their source and execution identities.
- The final-stage release change has CPU order/failure checks and a real local
  CPU-child handoff through the unchanged supervisor. Ray, GPU and physical-host
  receipt fields in these fixtures are not target-runtime qualifications. The
  target qualification of the changed final-owner/cleanup ordering is recorded
  separately below; the earlier 128 model results do not establish it.
- Integrated `909f842c456cae36b45a0bb5921f98570ea43385` passed target zero-model
  replay of the 64 original images, all 128 saved responses, complete original
  City relation and evaluator. Its subsequent native attempt stopped after 64
  confirmed first-stage requests: a separately launched Python status observer
  was absent from the declared host process identities. Its command identity is
  confirmed; CPU/RSS use and performance impact were not measured. The existing
  guard refused cleanup; this attempt remains failed and did not exercise the
  final-stage sink. The subsequent diagnostic-only fix replaced persistent
  Python polling with one-shot shell observation and left that frozen guard
  unchanged. The current maintained resource policy described above is a
  separate change; it cannot turn this failed attempt into a successful one.
- A separate complete cold run of `909f842c456cae36b45a0bb5921f98570ea43385`
  subsequently completed 128 confirmed requests, the original City sink with
  precision/recall/F1 = 1, and final physical cleanup. The sole server coordinator
  reported completion at 2026-11-11 14:03:47 UTC using shell observation and the
  unchanged guard. This run is independent of the failed 64-request attempt;
  neither requests nor clocks are spliced together. The later failed-stage
  evidence/EXIT-status changes have local CPU qualification only.
- The maintained host-resource policy has 22 local CPU checks covering ordinary
  Python observations, real-conflict rejection, incomplete inventories, safe SHM
  refusal, and outer-before → stage → outer-after policy consistency.
  Exact `933157666e1db81d47872fe21422ac8315ea7be3` subsequently passed those
  22 checks on the target and an actual supervised `pass` stage. Outer-before,
  stage-before, stage-after and outer-after all used
  `semloom.host_resource_boundary.v1` and policy SHA256
  `9139a7e954c713796e960e3b15838c861963d31c9d1d022be7434156957e411c`.
  Both unclassified Python observations were recorded without asserting their
  harmlessness or CPU isolation; resources, sockets, SHM and lease acceptance
  passed. The pinned `7c1d0d3f...` cleanup helper remained unchanged. This accepts
  the new host policy and outer/stage binding, not a new Ray Q7 model run.
  Run identity `semloom-pr99-step-actions-20261112-r1` and archive SHA256
  `f1879a799e8c6afd89b7add0c797dfe7d8f13c9301967242b7eb9247ecbe3b0d`
  locate the retained private originals; the separate workload comparison is
  reported in [PR #99](https://github.com/3444374/ai-operator-execution-optimization/pull/99)
  and failed strict output-token equivalence despite clean resource release.
  Its outer controller also checked exact 1 MiB; that historical extra condition
  is not copied into the maintained policy, which uses the frozen configured
  idle-memory ceiling and complete compute inventory.
  The later unknown Python PID in the a535 source check remains unattributed and
  failed under its original policy. New snapshot semantics do not rewrite it.
- Source-size candidate `d8308aac96707093b6d877d8781d9af8acba557c` passed the
  target's original full-query N=1/7/13 software test, native capture, 128-row
  token/pixel processor gate and actual Ray CPU worker-hook gate. The source
  fixtures retain their software identity; official sf64 remains 64 images.
  Model and constructor calls were zero. Twelve outer/stage resource boundaries
  used the qualified `933157666` policy; resource and lease cleanup passed.
  The outer `FINAL.json` still says failed: its archive `copytree` tried to copy
  two exited plasma-store sockets and followed two `session_latest` links.
  Independent recovery verified 405 ordinary files, retaining both socket and
  both absolute-link metadata without deleting nodes or rerunning any gate.
  Recovery is separate evidence, not a change to that failed status. Run identity
  `baseline-join-ray-zero-1112-r1`, archive SHA256
  `e18451aceace8ab8ac081421575411e8a17494a586724107b45ff19f9a522fd1`,
  and recovery-summary SHA256
  `0a2bb2dd6bec473225a0ab846bc21e7a21587e228927d3f6102b1f9591b56ebe`
  locate the preserved originals. No new candidate model/GPU result is claimed.

The former persistent remote `python3 -u -B -c` status loop is replaced by the
frozen, one-shot `observe_q7_status.sh` operational artifact. Bind its verified
path and the current run directories outside the repository:

```sh
sh "$VERIFIED_Q7_STATUS_SCRIPT" "$CURRENT_RUN_DIR" "$CURRENT_SHORT_DIR"
```

Each invocation reads existing status files and `ps`, then exits. The local
coordinator schedules any later observation; no remote polling process remains.
That operational change did not alter the frozen guard or cleanup ownership rules.
The old watcher source and failed-run records remain evidence, not active launch
instructions. The successful target run used shell commands; it does not by
itself prove deployment of this exact frozen script.

## Zero-model checks

Run only under the existing CPU/host supervision, using fresh output paths and
the same verified assets/configuration. Set `CUDA_VISIBLE_DEVICES=''`, offline HF
mode, `Q7_SOURCE`, `Q7_CHECKOUT`, `Q7_MANIFEST`, `Q7_MODEL`, `Q7_OUTPUT`, `Q7_CONFIG`
and `PYTHONPATH="$Q7_SOURCE/code"` explicitly.

```sh
"$DRIVER_PYTHON" -B -m src.baselines.image.ray_animals_q7.capture_cpu --query 7
"$NATIVE_PYTHON" -B -m src.baselines.image.ray_animals_q7.processor_cpu
"$NATIVE_PYTHON" -B -m src.baselines.image.ray_animals_q7.hook_cpu /fresh/hook-output
```

Capture accepts the same original Q1/Q3/Q7/Q10 identity as the worker (default Q7).
It checks original wire images, parser and full sink with explicit software
answers, including reversed occurrence replies; a direct original-SDK replay
checks the relation without another per-query sink implementation.
Processor consumes that capture's original query identity, Filter-pass count and
complete call count. It checks native message/token/pixel identity and the shared
N or 2N boundary plus the next rejected attempt. No processor-side query override
or fixed Q7 two-pass assumption is used. The current capture receipt records
`complete_original_sink_match`; old City-intersection-only receipts retain their
frozen source. Hook checks export/install in an actual Ray CPU worker without
engine construction or CUDA initialization. Outputs and native SDK provenance
are qualification evidence, not model results. Host cleanup is checked separately.
The variable-size processor receipt reports `software_guard_next_attempt_rejected`
and numeric `actual_shared_ledger_CPU_boundary`; older 128/129-named receipt fields
remain only in their frozen historical evidence.

For the handoff and failure checks, run from the maintained checkout with the
existing driver environment. This starts only a local CPU child; process-tree
inspection permission is required by the unchanged supervisor.

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=code LITELLM_LOCAL_MODEL_COST_MAP=True \
  HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
  "$DRIVER_PYTHON" -B -m unittest tests.test_ray_animals_q7 tests.test_ray_animals_q7_sink \
  tests.test_ray_animals_q7_sizes -v
```

The tests use software Ray/host receipts and cover the original binding callback,
final ACK, required first-stage cleanup, aborted parsing, post-sink audit/close
failures, unconfirmed exits, empty results, stale handoffs and timeout. They do
not load a model or replace the target image/token and physical cleanup gates.
