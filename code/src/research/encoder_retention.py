"""Candidate hint ordering for vLLM's already reclaimable encoder entries.

Call immediately before native can_allocate in the scheduler's owner thread.
Hints use native multimodal identities and predicted saved rebuild time. A
missing hint has zero predicted value; stable ties retain native LRU order.
No reference set, free-slot count, capacity or physical tensor is changed here.
"""


def prioritize_freeable_encoder(manager, future_value_ns):
    if not future_value_ns:
        return
    for key in sorted(manager.freeable, key=lambda key: future_value_ns.get(key, 0)):
        manager.freeable.move_to_end(key)
