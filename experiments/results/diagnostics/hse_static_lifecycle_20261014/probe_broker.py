"""Local broker history-cost probe; no Ray, database, model or GPU work."""

import hashlib
import json
import platform
import subprocess
import sys
import time
import types
from dataclasses import replace
from pathlib import Path

from src.baselines.common.redact import redact_json_values
from src.scheduling.runtime import stage_broker as current
from tests.scheduling.test_stage_broker import _descriptor


BASE = "3e7bd54bbc0e3416dfca991fcaaa2ffc41270a71"
SOURCE = "code/src/scheduling/runtime/stage_broker.py"


def load_baseline():
    text = subprocess.run(["git", "show", f"{BASE}:{SOURCE}"], check=True,
                          capture_output=True, text=True).stdout
    module = types.ModuleType("src.scheduling.runtime.hse_probe_baseline")
    module.__package__ = "src.scheduling.runtime"
    sys.modules[module.__name__] = module
    exec(compile(text, "<baseline-broker>", "exec"), module.__dict__)
    return module, hashlib.sha256(text.encode()).hexdigest()


def measure(module, count):
    broker = module.BoundedStageBroker(module.StageBrokerLimits(100, 80, 8, 1, 1))
    started = time.perf_counter()
    for sequence in range(count):
        descriptor = _descriptor(sequence)
        broker.enqueue_encoded(descriptor)
        prepare = broker.lease_prepare(now_s=sequence + 1)
        broker.complete_prepare(prepare.lease_id,
            replace(descriptor, representation="ready"), now_s=sequence + 2)
        model = broker.lease_model(now_s=sequence + 3)
        broker.complete_model(model.lease_id, output_row_ids=descriptor.row_ids)
        broker.snapshot()
    elapsed = time.perf_counter() - started
    snapshot = broker.snapshot()
    assert snapshot.completed == count and broker.is_drained()
    assert len(broker._states) == count
    return elapsed


def main():
    baseline, base_digest = load_baseline()
    values = []
    for count in (1000, 2000, 4000):
        for repeat in range(3):
            for arm, module in (("baseline", baseline), ("changed", current)):
                values.append(dict(arm=arm, blocks=count, repeat=repeat,
                                   elapsed_s=measure(module, count)))
    result = dict(
        kind="local_cpu_broker_mechanism", baseline_sha=BASE,
        baseline_source_sha256=base_digest,
        changed_source_sha256=hashlib.sha256(Path(SOURCE).read_bytes()).hexdigest(),
        python=platform.python_version(), platform=platform.system(),
        active_window=1, retained_duplicate_history=True,
        ray_started=False, model_calls=0, gpu_runs=0, samples=values,
    )
    print(json.dumps(redact_json_values(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
