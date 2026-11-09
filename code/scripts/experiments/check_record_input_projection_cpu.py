"""Measure only the Python frame-input transformation, without pandas or models.

The row-list control is the current input to pandas, not a simulated DataFrame
constructor. The projected vectors introduce real Python work; this measurement
cannot establish whether avoiding unused pandas columns pays for that work.
"""

import argparse
from datetime import datetime, timezone
import json
import platform
from pathlib import Path
import time
import tracemalloc

from src.semantic_methods.lotus.record_method import record_frame_input
from src.semantic_methods.lotus.record_types import RecordOccurrence


def run():
    samples = []
    for count in (1, 64, 512):
        for width in (1, 8, 64, 128):
            records = [RecordOccurrence('r-' + str(i), i, i // 2,
                {('text' if k == 0 else 'unused_' + str(k)): 'fixture-🙂-' + str(i) + '-' + 'x' * 64
                 for k in range(width)}) for i in range(count)]
            before = [r.describe() for r in records]
            for repeat in range(7):
                for projected in ((False, True) if repeat % 2 == 0 else (True, False)):
                    columns = ['text'] if projected else None
                    for _ in range(2):
                        record_frame_input(records, columns=columns)
                    started = time.perf_counter_ns()
                    for _ in range(32):
                        result = record_frame_input(records, columns=columns)
                    per_call_ns = (time.perf_counter_ns() - started) / 32
                    if projected:
                        assert result['text'] == [r.values['text'] for r in records]
                    else:
                        assert all(value is record.values for value, record in zip(result, records))
                    tracemalloc.start()
                    result = record_frame_input(records, columns=columns)
                    _current, peak = tracemalloc.get_traced_memory()
                    tracemalloc.stop()
                    samples.append(dict(rows=count, source_columns=width, repeat=repeat,
                        arm='projected_vectors' if projected else 'current_row_list',
                        input_build_ns=per_call_ns, input_build_peak_python_bytes=peak,
                        declared_frame_cells=count if projected else count * width))
            assert [r.describe() for r in records] == before
    return dict(schema='semloom.record_input_projection_cpu.v1',
        completed_utc=datetime.now(timezone.utc).isoformat(),
        python=platform.python_version(), architecture=platform.machine(),
        identity='software_fixture; no SemBench data or SDK',
        measurement='Python input-container build only; excludes pandas, formatter and inference',
        source_preparation_in_timing=False, warmup_calls=2, timed_calls_per_repeat=32,
        repeats=7, model_calls=0, pandas_used=False, samples=samples)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    options = parser.parse_args()
    report = run()
    options.output.parent.mkdir(parents=True, exist_ok=True)
    options.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(dict(status='completed', samples=len(report['samples']), model_calls=0)))
