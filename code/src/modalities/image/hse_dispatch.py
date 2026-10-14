"""Image/Ray metadata adaptation for the optional staged destination policy."""

from __future__ import annotations

import weakref

from ...planning.blocks import StageBlockDescriptor
from ...scheduling.core.models import BatchRequest
from ...scheduling.runtime.stage_dispatch import StageDestination, StageModelDispatcher


_dispatch_owners: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def bind_hse_dispatcher(dispatcher: StageModelDispatcher, worker_pool) -> None:
    """A dispatcher cannot reuse observations with a different actual pool."""
    if tuple(item.actor_index for item in dispatcher.actors) != tuple(range(len(worker_pool.gpu_actors))):
        raise ValueError("model dispatcher must cover each GPU actor index exactly once")
    owner = _dispatch_owners.get(dispatcher)
    if owner is not None and owner() is not worker_pool:
        raise ValueError("model dispatcher belongs to another pool; create a new dispatcher")
    _dispatch_owners[dispatcher] = weakref.ref(worker_pool)
    dispatcher.reset_service_feedback()


def image_model_routing_request(descriptor: StageBlockDescriptor) -> BatchRequest:
    work = descriptor.work.for_stage("model")
    if work is None:
        raise ValueError("image block must declare model work")
    return BatchRequest(
        request_id=descriptor.block_id, job_id=descriptor.job_id, operator="ai_embed",
        row_count=len(descriptor.row_ids), prompt_tokens=0, estimated_output_tokens=0,
        prefix_key="", first_arrival_s=descriptor.created_at_s,
        oldest_arrival_s=descriptor.created_at_s, payload_id=descriptor.block_id,
        work_units=work.units, work_unit=work.unit,
    )


def observe_ray_stage_destination(*, endpoint_id: str, execution_group_id: str) -> StageDestination:
    """Call inside a live actor using public Ray runtime context APIs.

    Group identity is declared by the owner. Observed actor/node/accelerator
    IDs do not prove physical GPU UUIDs, HBM availability or independent cards.
    No actor RPC, payload fetch, model execution or device synchronization is
    performed here. Opt-in actor adapters can expose this during their setup.
    """
    import ray

    context = ray.get_runtime_context()
    actor_id = context.get_actor_id()
    if actor_id is None:
        raise RuntimeError("stage destination must be observed inside its actor")
    return StageDestination(
        endpoint_id=endpoint_id, execution_group_id=execution_group_id,
        runtime_actor_id=actor_id, node_id=context.get_node_id(),
        accelerator_ids=tuple(context.get_accelerator_ids().get("GPU", ())),
    )
