# Ray Animals Q7

This entry runs the original Animals Q7 zebra and impala Filters and their City
intersection. Each Filter uses a fresh native Ray processor/runtime for 64
occurrences: two sequential native lifecycles and 128 model calls in total.
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
private output directory, reserves an `AttemptBudget(budget_id, 128)`, and uses
`CellBudgetLedger` to reserve/claim a shared unit whose ID is `budget_id` in
`output/logical-unit.sqlite`. No warmup or retry is added.

Freeze `output/RUN-BOUND.json` with the fields below. Resource numbers are an
example allocation; the controller must bind the agreed actual values and the
SHA256 of each supplied helper/configuration file.

```json
{
  "source": "/absolute/maintained-checkout",
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
Each native stage uses `native_python`. The first must establish worker exit,
physical idle, resource acceptance, owned-process exit and private-socket cleanup
before the second starts. The final stage publishes its complete predictions
atomically and holds its public Ray runtime until the original query returns its
complete City result. The driver records that memory-sink timestamp and sends an
explicit release; only then does the final worker call `ray.shutdown()` and the
same supervisor finish physical cleanup. The adapter uses the pinned host/socket helpers and the
maintained [inactive semaphore cleanup](../../../observability/process_resources/inactive_semaphores.py).
The host/resource checks reject foreign or unknown ownership before unlinking.
Private `TMPDIR`, `VLLM_RPC_BASE_PATH` and `RAY_TMPDIR` are per stage and restored
afterward. The outer lease stays held across both stages. A stage timeout is
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

## Evidence and timing

The outer controller reconciles both predictions files, native events and the
shared ledger: exactly 128 charged/start/confirmed-terminal occurrences, with no
missing, duplicate, error or unconfirmed request. Request IDs are local to an
owner; correlate PID/request ID and ledger attempt. `QUERY.json` completion alone
does not establish final physical resource acceptance.

The fresh-source-to-sink interval includes source work, both startups, required
input/prediction transport, native work, the first public Ray close and necessary
inter-stage cleanup. Its endpoint is the original complete in-memory City
result, before the final public Ray close, final host cleanup, message-summary
audit, evaluation, result serialization and report persistence. This is not a
zero-I/O execution profile: the existing native workers consume input files and
return predictions through files. Supervised Popen,
runner entry, native constructor return, request events and physical exit have
separate clocks. Full controller entry and first engine admission remain unknown
in this runner; request spans are not pure GPU time. The common definitions are
in [native baseline calibration](../../../../../experiments/plans/原生基线校准.md).

Qualification scope:

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
  changed final-owner/cleanup ordering still needs a bounded target qualification;
  the earlier 128 model results do not establish it.

## Zero-model checks

Run only under the existing CPU/host supervision, using fresh output paths and
the same verified assets/configuration. Set `CUDA_VISIBLE_DEVICES=''`, offline HF
mode, `Q7_SOURCE`, `Q7_CHECKOUT`, `Q7_MANIFEST`, `Q7_MODEL`, `Q7_OUTPUT`, `Q7_CONFIG`
and `PYTHONPATH="$Q7_SOURCE/code"` explicitly.

```sh
"$DRIVER_PYTHON" -B -m src.baselines.image.ray_animals_q7.capture_cpu
"$NATIVE_PYTHON" -B -m src.baselines.image.ray_animals_q7.processor_cpu
"$NATIVE_PYTHON" -B -m src.baselines.image.ray_animals_q7.hook_cpu /fresh/hook-output
```

Capture checks the original parser and complete City intersection with synthetic
answers. Processor checks native message/token/pixel identity and the shared
128/129 boundary. Hook checks export/install in an actual Ray CPU worker without
engine construction or CUDA initialization. Outputs and native SDK provenance
are qualification evidence, not model results. Host cleanup is checked separately.

For the handoff and failure checks, run from the maintained checkout with the
existing driver environment. This starts only a local CPU child; process-tree
inspection permission is required by the unchanged supervisor.

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=code LITELLM_LOCAL_MODEL_COST_MAP=True \
  HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
  "$DRIVER_PYTHON" -B -m unittest tests.test_ray_animals_q7 tests.test_ray_animals_q7_sink -v
```

The tests use software Ray/host receipts and cover the original binding callback,
final ACK, required first-stage cleanup, aborted parsing, post-sink audit/close
failures, unconfirmed exits, empty results, stale handoffs and timeout. They do
not load a model or replace the target image/token and physical cleanup gates.
