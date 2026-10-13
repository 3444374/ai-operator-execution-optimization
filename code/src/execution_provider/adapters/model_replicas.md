# Equivalent physical model replicas

This is an engineering assembly of the existing `SessionEngine`, Job grants,
`SessionPolicies`, `BoundedAsyncBackend`, complete-response transport and Ray Map
transport. A logical `model` role can resolve to several explicitly declared HTTP
services. `build_fixed_model_execution` and its original single-endpoint default
retain their existing implementation.

## Configuration and capacity

`ModelReplicaSet` names one model and an ordered tuple of `ModelReplica` values.
Each replica declares an identity, actual URL, opaque location label, physical
device IDs, equivalent deployment SHA-256, request capacity and work capacity.
URLs stay in a private configuration; observation events expose replica IDs and
digests. Repeated IDs, equivalent URLs, mismatched deployment digests and global
capacities exceeding the sum of replica capacities are rejected before startup.

The deployment digest must refer to a separately audited model revision/weights,
tokenizer/template, generation defaults, serving configuration and data policy.
Matching declarations and matching served model names alone do not prove those
properties. This assembly does not discover services or change model semantics.

Global request/work limits apply to one execution owner and its registered Jobs.
Replica limits apply to that owner's outstanding requests at each endpoint.
Requests retain their original independent HTTP body. Work uses the caller's
existing descriptor and unit; the default value of one measures request count,
without claiming token or GPU cost. These limits do not limit another service
client or change vLLM's internal sequence capacity.

`round_robin` visits the stable configured order and skips occupied replicas.
It balances submission opportunities among eligible endpoints, so a faster
replica can receive more work. `static` pins `session sequence % replica count`
and waits for that endpoint, even when another endpoint is free. A task too
large for every replica, or for its static endpoint, fails without sending HTTP.
Neither policy retries a failed request or moves an already accepted request.

## Optional completion/pressure feedback

`routing="adaptive"` requires an explicit `adaptation` object in the private
configuration. `ReplicaAdaptationConfig` accepts `target_drain_s` (optional soft
work target), `sample_interval_s`, `stale_after_s`, `rate_alpha` and
`congestion_kv_usage`. Existing `static`/`round_robin` behavior, configuration
signatures and the default single-endpoint assembly are preserved.

This mode reuses `LeastWorkEndpointRouter`. A bounded accumulator per endpoint
estimates successful completed descriptor work divided by the union of busy
transport intervals; an exponential average smooths successive samples. It
includes preparation, HTTP/client waits and service queueing. It is not actual
token throughput, GPU utilization or calibrated service capacity. HTTP errors,
local errors and unknown outcomes do not add completed work. A request-count
descriptor stays in request-count units and is never compared to token/s.
Until all replicas have fresh rates, neutral outstanding work is used and an
unmeasured idle endpoint receives a probe opportunity. Probing sends only an
original queued call, without duplicate requests or hidden retries.

On each new complete queue/KV sample, the per-replica supply target moves one
request down under pressure or one up on a clear queue, between one and its hard
declaration. Aggregate admission uses the smaller of the declared global request
limit and the sum of current targets. Routing still checks that endpoint's hard
request/work limits against Core records. A sample does not cancel or retire any
already submitted work. Missing/stale pressure restores declared static supply;
missing/stale rates use the neutral-work fallback. The optional drain target
additionally limits new outstanding work; one hard-legal task can run alone on an
idle endpoint even if its estimate exceeds the soft target.

`replica_observations={endpoint_id: provider}` accepts existing cached/nonblocking
observation providers. Service running counts can include the owner's requests,
so they are not added to Core counts. Providers remain caller-owned. The gateway
CLI's explicit `--replica-metrics` option starts the existing nonblocking sampler
against `/metrics` on each declared listener, outside the Core decision thread,
and closes the samplers with the gateway. Missing metrics never become fabricated
zero queue or zero KV values. Live metrics do not reserve resources against other
clients or prove deployment identity.

Adaptive mode rejects replicas declaring the same `(location_id, device_id)`;
several URLs cannot automatically add capacity on one physical device. Device
declarations still require the separate live identity probe. Empty device tuples
are allowed for software fixtures and establish no GPU capacity. Existing
explicit static shared-device checks are unchanged.

This is an optional execution capability, not a new scheduling framework or a
research contribution. [The local paired report](../../../../experiments/results/diagnostics/replica_adaptation_20261013/README.md)
separates the already present completion-driven round-robin benefit from new
decisions, includes stronger fixed-rate routing and retains supply regressions.
The default stays round-robin. Shared preparation remains unsupported by this
assembly; a transport completion is not a device-read lifetime receipt.

Core derives outstanding counts from its own records. Submitted, cancelled and
unknown remote tasks remain in both global and endpoint request/work accounting
until Core receives an authoritative terminal. No local HTTP timeout, detached
consumer or fixture activity observation can settle an unknown remote request.
`replica_usage(execution)` reports those records on the owner thread. Result
storage remains reserved until the existing delivery lease is released.

## Python assembly

```python
from src.execution_provider.adapters.model_replicas import (
    ModelReplicaSet, build_model_replica_execution,
)
from src.execution_provider.adapters.native_tasks import NativeTaskSession

replicas = ModelReplicaSet.load(private_replica_path)
execution = build_model_replica_execution(
    fixed_model_config, replicas=replicas, response_mode="full", max_tasks=32,
)
flow = NativeTaskSession(execution, query_id, operator_id)
```

The existing `prepare_native_task`, accepted-prefix, `advance`, full-response
decoder, lease release and query cleanup APIs remain the caller's interface.
HTTP status, duplicate header pairs and raw body reach the original supplier
parser, including HTTP errors, malformed JSON, usage and logprobs. Model,
messages, generation settings, row/call/stage identity and query/flow ownership
are retained. Responses are bounded by the task's existing result reservation.

`physical={replica_id: RayMapConfig(...)}` enables the existing real Daft/Arrow
payload and Ray HTTP transports separately for each replica. Every endpoint
requires a configuration with one existing cluster address; named worker pools
must be distinct. Workers and payload batch rows must fit the effective endpoint
request limit. Input preparation is currently unsupported for this assembly and
is rejected explicitly. A remote request guard can be passed to the Ray path.
HTTP workers remain CPU clients and do not prove model/GPU replica counts.
Daft native runner selection happens once on the startup thread before concurrent
replica payload pools can materialize their first inputs. This reuses the existing
runner configuration API and avoids concurrent cold initialization.

The execution service closes only after all Core records and Jobs settle.
Child transports close in reverse creation order so the first child's owned Ray
connection remains available to the others. Startup and finalization attempt all
owned child cleanup; a cleanup error remains visible on subsequent close calls.
Unknown work requires service-owner investigation; there is no force-release API.

A failure of the new `replica_transport_started` observer produces the existing
local error value before any child call. It has no invented HTTP status and can
settle through the normal result lease. Child observation exceptions are retained
as one bounded diagnostic and do not replace a completed response or an actual
I/O exception. Closing the service attempts all child cleanup, then reports the
observation failure. A dispatched timeout still retains its original unknown
request/work responsibility; diagnostic failure cannot refund that request.

## Existing PostgreSQL Map gateway

`code/scripts/services/run_model_replica_gateway.py` passes this factory to the
existing gateway and serves the existing Map v6 protocol. A private JSON example
is shown below; replace the digest with the actual audited identity.

```json
{
  "model_id": "fixture",
  "max_active_requests": 8,
  "max_active_work": 8,
  "routing": "round_robin",
  "replicas": [
    {
      "endpoint_id": "replica-a",
      "endpoint_url": "http://127.0.0.1:8000/v1/chat/completions",
      "location_id": "machine-a",
      "device_ids": ["GPU-DECLARED-A"],
      "deployment_sha256": "<AUDITED_DEPLOYMENT_SHA256>",
      "max_active_requests": 4,
      "max_active_work": 4
    },
    {
      "endpoint_id": "replica-b",
      "endpoint_url": "http://127.0.0.1:8001/v1/chat/completions",
      "location_id": "machine-a",
      "device_ids": ["GPU-DECLARED-B"],
      "deployment_sha256": "<AUDITED_DEPLOYMENT_SHA256>",
      "max_active_requests": 4,
      "max_active_work": 4
    }
  ]
}
```

```bash
python code/scripts/services/run_model_replica_gateway.py \
  --replica-config <private-replicas.json> \
  --fixed-model-config <private-fixed-model.json> \
  --incremental-map --socket /tmp/replica-provider.sock \
  --max-active-requests 8 --max-held-tasks 32
```

Repeat `--replica-ray-config replica-a=<private-ray-a.json>` for all endpoints
to select real Daft/Ray. The fixed-model file retains existing model/auth/timeout
validation. The gateway retains its Map completion decoder and evidence format;
full native HTTP envelopes are selected through the Python API. Physical replica
attribution is external observation evidence. The existing PG semantic/provider
digest does not bind each response to a physical deployment.

Native Ray/Daft baselines keep their own execution and route assignment. They
can use the same audited endpoint/capacity manifest in a matched run, but should
not import this SemLoom router or its admission policy.

## Live process/device verification

After separately authorized model startup, execute
`code/scripts/services/verify_model_replicas.py` on the endpoint's Linux machine.
It reads the existing `launch_vllm_with_identity.py` sidecars, `/proc` and
`nvidia-smi` compute-process data. It sends no HTTP or model requests.

```bash
python code/scripts/services/verify_model_replicas.py \
  --replica-config <private-loopback-replicas.json> \
  --runtime-identity replica-a=<private-a.runtime_identity.json> \
  --runtime-identity replica-b=<private-b.runtime_identity.json> \
  --output <new-private-device-evidence.json>
```

The probe checks PID start time, actual API command/model/port, owned listener,
explicit `--tensor-parallel-size 1`, GPU-bearing descendants, exact device IDs,
separate process trees and equal serving arguments/package versions. Independent
GPU qualification also rejects other GPU compute owners. Shared-device mode is
explicit and reports the actual GPU count. Ray actors, stale sidecars and mere
device declarations cannot pass. This currently covers one device per local replica;
cross-machine verification and model content auditing remain separate work.

Probe evidence schema 2 retains each qualifying listener's address, address
family, port, inode and owning process/start time. A port-only observation cannot
qualify a URL. Wildcards cover only their own address family; IPv6 listeners are
not presumed to accept IPv4. `localhost` requires current resolver evidence and
every returned loopback address must reach the model tree. Prefer literal loopback
URLs when only one family is served. The live collector rechecks the listener
inode/address and its owning process after the GPU snapshot. Old schema 1 reports
do not establish this address binding. HTTPS and application-path behavior require
separate HTTP qualification.

## Verification scope

[Local tests](../../../tests/postgres/test_semloom_model_replicas.py) use two
independent loopback HTTP servers and the actual Core, execution factory and v6
CLI. They cover exact request bytes/counts, full results/identity, each capacity,
slow/failed endpoints, cancellation, unknown outcomes, cleanup and single-endpoint
compatibility. [Identity tests](../../../tests/infrastructure/test_replica_identity.py)
use synthetic process observations and make no live GPU claim.

[Actual library tests](../../../tests/execution_provider/test_model_replicas_ray_integration.py)
require `SEMLOOM_REPLICA_RAY_TEST=1`, real Ray/Daft/Arrow and an isolated CPU
allocation. They start a private Ray cluster with zero GPU resources and a
256MiB object store, then test both Daft and Arrow payloads, two actual HTTP
actors/endpoints, full consumption, cancellation, query reuse and cleanup.
Controlled fixture timing supplies no model performance conclusion. Real model,
GPU-process qualification, PG18.3 database runs and matched native performance
comparisons require their own current signatures and authorized run plans.
