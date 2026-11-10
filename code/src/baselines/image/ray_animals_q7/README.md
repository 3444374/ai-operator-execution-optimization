# Ray Animals Q7 qualification

Candidate, not GPU-ready. Reuses the successful Q1 native SDK image/token checks,
now capturing original Q7's full 128 occurrences. No model constructor permitted.
Runtime must contain Ray 2.56.1 and the explicit runtime_profile: official-ray-0.22 (vLLM 0.22.0) or unified-candidate-0.25.1 (vLLM 0.25.1). Profiles must never be pooled.
No claim of GPU engine compatibility follows from CPU SDK qualification.

From a fresh output path on the existing server, with an existing isolated Python:

    export CUDA_VISIBLE_DEVICES=''
    export HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
    export Q7_SOURCE=/path/to/this/checkout
    export PYTHONPATH="$Q7_SOURCE/code"
    export Q7_CHECKOUT=/root/autodl-tmp/sembench-visual-c814-1105-r1
    export Q7_MANIFEST=/root/autodl-tmp/data/sembench-animals-c814-sf64/manifest.json
    export Q7_MODEL=/root/autodl-tmp/models/Qwen2-VL-2B-Instruct
    export Q7_OUTPUT=/path/to/new/q7-cpu-output
    export Q7_CONFIG=/path/to/reviewed/unified-config.json
    "$EXISTING_ISOLATED_PYTHON" -m src.baselines.image.ray_animals_q7.capture_cpu
    "$EXISTING_ISOLATED_PYTHON" -m src.baselines.image.ray_animals_q7.processor_cpu

The config must provide explicit engine_kwargs and sampling. It is deliberately
not populated from the historical .22 profile. CPU script uses engine_kwargs;
actual native callback also consumes sampling. Public unified values remain
subject to protocol coordinator review. The server paths above are historical
verified inputs; preflight must confirm their current identities.

Outputs: CPU-NATIVE-CAPTURE.json, OCCURRENCES.json, private compressed messages,
TOKEN-PIXEL-DETAILS.json, CPU-RAY-PROCESSOR.json and RAY-CLOSE.json. The latter
reports actual native stage order, SDK hashes, versions, local processor metadata,
raw and expanded tokens, pixel identity and zero-engine/zero-GPU assertions.

native_query.execute is the official build_processor callback, 64 rows per stage.
Do not invoke it in CPU qualification: the capture uses only synthetic answers.
native_probe.native_observer_setup is the historical process-shared public
AsyncLLM.generate hook with a 128-call cap, installed by the existing controller
worker setup mechanism. It requires a controller-created logical-unit.sqlite and
Q7_BUDGET_ID, and is not a standalone launch mechanism. CPU ledger boundary
128/129 is executable in processor_cpu; hook-level fake AsyncLLM 128/129 test passes locally (no model construction).

Remaining before acceptance: execute both CPU commands on target .25.1 environment;
execute both-mask/independent ordinary sink assertions on target assets;
record weight/tokenizer/template full hashes and final common-contract config;
coordinator review and future separately authorized micro engine qualification.
No GPU launch command or performance result is supplied.

Each native callback currently builds its own official processor. Two Filter calls
may construct separate actors/engines; no same-owner claim is made. Actual native
constructor events must establish owner identity and both startup costs belong
in the complete cold clock. Never mix old Q1 zebra outputs with new impala
outputs into a fresh full Q7 performance result.

Local verification: five unittest cases pass against maintained source, including
actual observation hook wrapped around a fake AsyncLLM and SQLite 128/129 guard.
Both CPU entrypoints --help pass. Full original-query software fixture passed on
2026-11-10 using 64 synthetic 16x16 images and the pinned clean SemBench checkout:
128 original messages, both original parser masks, RGB identity, ordinary City
intersection and original evaluator. This is not official image/token qualification.
The original parser's no-verdict default=True behavior is deliberately retained.
The CPU HTTP guard blocked a LiteLLM optional cost-map fetch; its packaged fallback
was used. No model call or target Ray/vLLM SDK execution occurred.

## Supervised real-query candidate (not GPU-tested)

The existing Q1 host controller already owns host preflight, runner lease,
ResourceMonitor, supervise(Popen), cancellation and evidence-based cleanup.
Retain those components. Do not run the query module without that outer scope.
The thin runner uses the LOTUS driver interpreter for the original query; each
Filter invokes the configured native Ray interpreter once, sequentially. Both
native workers spend the SAME claimed CellBudgetLedger unit. No warmup, retries,
resident actor implementation or alternate scheduler is introduced.

Supervisor setup: create a fresh private output directory; reserve AttemptBudget
(budget_id, 128), reserve_unit(budget_id, 128), claim_shared_unit(budget_id), using
output/logical-unit.sqlite. Write output/RUN-BOUND.json with these explicit fields:

    {"source":"/absolute/source", "budget_id":"unique-q7-cold-id",
     "physical_attempt_limit":128, "config":"/absolute/config.json",
     "native_python":"/existing/ray/environment/bin/python",
     "checkout":"/existing/pinned/SemBench", "manifest":"/existing/manifest.json",
     "manifest_digest":"verified-manifest-content-digest",
     "model":"/existing/Qwen2-VL-2B-Instruct", "num_cpus":8, "num_gpus":1,
     "object_store_memory":536870912, "ray_temp_dir":"/fresh/private/ray-temp"}

Resource numbers above are the prior Q1 reference, not automatic hardware policy.
The controller must bind the actual agreed allocation and freeze this file.
Set PYTHONPATH=source/code and LITELLM_LOCAL_MODEL_COST_MAP=True. Under existing
supervise, launch the existing LOTUS driver with arguments:

    -m src.baselines.image.ray_animals_q7.runner /absolute/output

Retain supervisor.started_monotonic as supervised launch (Popen boundary), NOT
full controller entry. Host preflight occurs earlier. Runner records its own
entry and fresh source/query entry; no full-controller timestamp is invented.
Native observation records constructor-ready and public generate start/terminal.
First engine admission is unknown; request intervals are not pure GPU time.
Original City sink timestamp precedes evaluation and report persistence. Each
stage's public Ray close currently occurs before original sink; both startups,
IPC serialization, native work and closes are included in query elapsed time.
There are explicitly two sequential native worker/Ray lifecycles. Native model
constructor events must establish actual owner counts; same-engine is not claimed.

The outer controller MUST audit native events/receipts against both predictions
files and the shared ledger: exactly128 charged/start/confirmed-terminal, no
missing/duplicate/error/unconfirmed. Native request IDs may repeat across owners:
use (PID, request_id) plus ledger attempt, never request ID alone. QUERY.json
completed means original query returned, not final resource/physical acceptance.
Keep post-query snapshot, quality, persistence, public close and host cleanup
separate in final acceptance. No private actor-close workaround is introduced.

Local regression: six tests pass, including runner clock ordering and stop after
first native-worker failure. Native worker hook export and two real Ray lifecycles
remain target-runtime untested. Frozen c2da31c CPU packet is unchanged.

### Required inter-stage physical-exit gate (supersedes direct query CLI)

Direct non-native runner CLI is now fail-closed. The existing host controller must
call run_query(root, bound, supervise_stage=callback) in its LOTUS driver context.
Callback signature: callback(command, stage_index, root) -> evidence receipt.
It must run existing api.supervise with ResourceMonitor, collect owned identities,
perform host.live_owned/cleanup_owned, host.snapshot/assert_idle, and the already
verified inactive semaphore/private socket cleanup from the Q1 controller.
Only then return all true: worker_exit_zero, physical_idle, resource_passed,
owned_processes_exited, private_sockets_clear, with linked detailed receipts.
Do not release the overall Q7 lease between stages. The per-stage gate is inside
the source-to-sink interval and is explicitly project lifecycle cost.

Historical Q1 only ran this cleanup in its overall finally block. That placement
is insufficient for Q7: the existing controller needs to expose that same
capability per stage. No new cleaner or private actor-close algorithm is supplied.
Until that callback is wired and validated, the runner cannot launch real Q7.
A result file plus ray.is_initialized()==False does not satisfy this gate.
Missing capability or unconfirmed stage exit stops Q7 while preserving outputs.

Actual target Ray worker hook zero-model probe (under existing CPU supervision):

    CUDA_VISIBLE_DEVICES='' Q7_SOURCE=/source Q7_CONFIG=/reviewed/config.json \
      PYTHONPATH=/source/code /existing/native/python \
      -m src.baselines.image.ray_animals_q7.hook_cpu /fresh/hook-output

This exports and installs the actual hook in a real Ray CPU worker, inspects the
wrapped AsyncLLM methods without construction, verifies worker PID event and zero
ledger spending/CUDA initialization. This command is supplied but not run on Mac;
CPU supervisor must verify physical cleanup separately. It is not engine testing.

### Implemented pinned host adapter

The normal runner CLI now constructs StageSupervisor from RUN-BOUND.host_adapter.
It directly calls existing host/api/ResourceMonitor and uses the maintained process_resources.inactive_semaphores cleanup function. It does
not call that controller's main/run_round or duplicate its cleanup algorithm.
The earlier requirement to develop a server callback is superseded by this adapter.
Add this object to RUN-BOUND.json (resource hash must match the actual agreed file):

    "host_adapter": {
      "host_helper": {
        "path": "/root/autodl-tmp/semloom-host-helper-infrastructure-20261105/host_helpers.py",
        "sha256": "340bb1acf01b8e3e8faded3f21bc11177bfb49a31b670a31e101f2568d733760"
      },
      "socket_helper": {
        "path": "/root/autodl-tmp/medical4-native-plan-1107-r1/private_temp_sockets.py",
        "sha256": "64bb48944f91524eea254a7f7097175165af9d128fd1b459fc6a830be15b7577"
      },
      "resource_config": {"path": "/absolute/frozen-resource-config.json", "sha256": "actual-file-sha256"},
      "stage_timeout_s": 480
    }

The outer existing supervisor still owns the host lease, global deadline and
shared128 budget. Under that scope execute the earlier normal runner command.
Per-stage TMPDIR/VLLM_RPC_BASE_PATH/RAY_TMPDIR are unique and restored afterward;
native Ray temp resides inside that stage's private tree so socket checks cover it.
Timeout is per stage; the outer full-query deadline is separate and mandatory.
No stage starts on foreign GPU/unknown ownership or unconfirmed cleanup. Every
stage writes before/after, supervisor, resources, socket/semaphore cleanup and EXIT
receipts. Failures propagate to the existing outer final cleanup with no retry.

Local final check: seven tests pass, including successful existing-helper calls
and unresolved-owned-process rejection before unlink. Actual target helper imports,
Linux resource monitoring, real cleanup and GPU execution remain unrun for this
new adapter. CPU image/token qualification of frozen c2da31c remains independent.


## Main integration and validated scope

This candidate is based on main 6b0f4f6. Native execution, original Q7 algorithms,
prompt/parser and two64 lifecycle remain byte-identical to the c1648 qualification
except the host adapter imports the maintained inactive semaphore helper instead
of the entire historical Q1 controller. Default config is official Ray2.56.1 /
vLLM0.22.0; optional .25.1 remains a separate profile, not a required upgrade.

Server c1648 qualification completed 128 public generate / 128 terminals, no
unknown/duplicate, two constructors, original City F1=1 and all message/prompt-token
identity checks. Each stage left one32-byte semaphore and four inactive sockets;
project-owned cleanup was required. This was qualification, not formal performance
or a single-owner warm run. Main integration has CPU migration tests only; its
Linux unlink deployment has not been exercised and the frozen qualification source
and receipts remain authoritative for the observed real run.

Exit mapping:
- Historical Q1 controller.py cleanup_inactive_semaphores -> maintained
  src.observability.process_resources.inactive_semaphores. Caller still proves
  exclusive-stage host idle, no live owned processes, ResourceMonitor new names,
  birth-time bounds, UID/size, no holders, readable scans and unchanged inode.
- Remove host_adapter.q1_controller from new RUN-BOUND configuration. Keep the
  historical controller file and Q1/Q7 experiment packets until deployment verifies
  migrated cleanup and all remaining references are checked. Evidence is not trash.
- Pinned host_helper and socket_helper remain active external dependencies; do not
  delete either. Their broad migration is outside this narrow change.
- qualification/ray_animals_q7 scratch implementation was already replaced by this
  package; no parallel scratch implementation is included in the integration.

Navigation suggestions for the separate documentation integrator: link this README
from code/src/baselines/README.md, record c1648 Q7 qualification in the evidence
registry, and state project lifecycle cleanup versus native scheduling explicitly.
This branch does not change shared/root documentation owned by other workers.

### Final cleanup migration qualification, 2026-11-10

The earlier target-pending statements above are superseded for the Linux cleanup
module/interface only. Source 4e8f0a0354fcfef4857e4c2000107de322372eb1 passed target
module/adapter import and seven mocked cases in r1. The first real fixture was
protectively rejected because its narrowly sampled lower bound followed the file
mtime by 4.08ms; that failure and exact-own fallback cleanup remain recorded.

r2 used the genuine outer stage-before boundary and one uniquely self-created
POSIX semaphore. The unchanged helper passed sem_close, empty holder/error scan
and actual sem_unlink with no fallback. No timestamp mutation or tolerance was
introduced. Resources and lease were clean; zero Ray runtimes, models or GPU work.
This accepts the cleanup-interface migration, not a new Ray engine teardown or
performance result. The c1648 full128 qualification retains its original identity.

Evidence identities (private archive filenames, no source rewrite):
- ray-q7-cleanup-main-target-cpu-20261110-r1/HANDOFF.json:
  949840bc4763bd9124f17671b563bcdf466033a7d5d1b84f315f225befc93d1f
- ray-q7-cleanup-main-target-cpu-20261110-r2/HANDOFF.json:
  3cb5b387ba24c8529e7898877a03b332a9c6229358b172d5553b308b35f19d93
- r2 adds20 verified originals. No earlier receipts or failed evidence were deleted.

New Q7 adapter/config no longer references the historical Q1 controller. Retain
that file for old frozen experiments and retain the still-active pinned host/socket
helpers. This change authorizes no blanket deletion of archived evidence.
