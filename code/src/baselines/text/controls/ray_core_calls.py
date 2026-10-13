"""Ray Core APIs in a fixed actor topology, identified as diagnostic_reference.

Ray owns actor queues and their scheduling. Static input sharding is explicit;
this reference is separate from the vendor-owned Ray Data HTTP Processor graph.
"""
import ray

from src.baselines.text.frameworks.native_calls import _call_from_wire, _call_wire, _outcome_from_wire, _outcome_wire, _request


class _RayHttpActor:
    async def complete(self, wire):
        return _outcome_wire(await _request(_call_from_wire(wire)))


def rows(calls, options, control):
    if ray.__version__ != "2.56.1" or not ray.is_initialized():
        raise RuntimeError("complete-call Ray Core reference requires caller-owned Ray 2.56.1")
    if options.concurrency > min(options.num_threads, ray.cluster_resources().get("CPU", 0)):
        raise ValueError("Ray Core actor count exceeds the declared logical CPU allowance")
    if control.stopped.is_set():
        return
    # No driver request pool, dynamic replenisher or SemLoom policy is installed.
    actor_type = ray.remote(num_cpus=1, num_gpus=0, max_concurrency=1,
                            max_restarts=0, max_task_retries=0)(_RayHttpActor)
    actors = []
    pending = []
    try:
        for _ in range(options.concurrency):
            if control.stopped.is_set():
                return
            actor = actor_type.remote()
            actors.append(actor)
            control.register_actor(actor)
        for index, call in enumerate(calls):
            if control.stopped.is_set():
                return
            pending.append(actors[index % len(actors)].complete.remote(_call_wire(call)))
        while pending and not control.stopped.is_set():
            ready, pending = ray.wait(pending, num_returns=1, timeout=0.01)
            for ref in ready:
                yield _outcome_from_wire(ray.get(ref))
    finally:
        for ref in pending:
            ray.cancel(ref, force=False)
        control.stop()
