"""One fixed workload/model role per embedded interpreter, including aliases."""

import sys
import threading
import types


slot = sys.modules.setdefault("integrations.imlane_records._method_owner", types.ModuleType("_method_owner"))
for name, initial in (("worker", None), ("fingerprint", None), ("finished", set()), ("lock", threading.RLock())):
    slot.__dict__.setdefault(name, initial)


def owned_worker(config, fingerprint, factory):
    with slot.lock:
        if slot.fingerprint is not None and fingerprint != slot.fingerprint:
            raise ValueError("interpreter cannot change workload, method, model role or engine configuration")
        if slot.worker is not None and slot.worker.broken and config["query_id"] != slot.worker.query_id:
            raise ValueError("failed interpreter cannot be reused for a later query")
        if slot.worker is None or (slot.worker.closed and config["query_id"] != slot.worker.query_id):
            if config["query_id"] in slot.finished or len(slot.finished) >= 64:
                raise ValueError("query repeats or exceeds interpreter query allowance")
            slot.worker = factory(config)
            slot.fingerprint = fingerprint
        return slot.worker
