"""Finite local Core comparison; table/Ray/model substitutes, no network."""

import argparse
from dataclasses import replace
import gzip
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import time
from unittest.mock import patch

from src.baselines.common.redact import redact_json_values
from src.experiments import map_observation_probe as fixture
from src.scheduling.runtime.stage_broker import StageBrokerLimits


def run_case(output, staged, rows):
    current = []
    base_transport, base_build = fixture.RayMapTransport, fixture.build_fixed_model_execution

    class Transport(base_transport):
        def __init__(self, *args, **kwargs):
            kwargs['physical'] = replace(kwargs['physical'], window_bytes=2**21, object_bytes=2**22,
                preparation=StageBrokerLimits(2**21, 2**22, 64, 1, rows) if staged else None)
            super().__init__(*args, **kwargs)
            current.append(self)

    def build(*args, **kwargs):
        if staged:
            kwargs['preparation_factory'] = lambda transport, limits, notify: transport.prepare_inputs(limits, notify)
        return base_build(*args, **kwargs)

    def batches(data, limits, *, batch_rows, backend):
        assert backend == 'arrow' and len(data) <= limits.rows
        assert sum(len(row[2]) + 24 for row in data) <= limits.bytes
        current[0].observer(dict(event='fixture_window', rows=len(data)))
        for start in range(0, len(data), batch_rows):
            time.sleep(fixture.PREPARE_S)
            yield fixture.SyntheticTable(data[start:start + batch_rows])

    with patch.object(fixture, 'RayMapTransport', Transport), \
         patch.object(fixture, 'build_fixed_model_execution', build), \
         patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', batches):
        result = fixture.run_case(output, 'durable', 'buffered', rows=rows, capacity=4, batch_rows=4)
    with gzip.open(output / 'events.jsonl.gz', 'rt') as stream:
        events = [json.loads(line) for line in stream]
    observations = [e for e in events if e['event'] == 'ray_preparation_completed']
    if staged:
        assert observations and not any(e['failed'] for e in observations)
        assert sum(e['rows'] for e in observations) == rows
        for event in observations:
            usage = event['preparation']
            assert usage['held_tasks'] <= rows
            assert usage['stages']['encoded_held_bytes'] <= 2**21
            assert usage['stages']['ready_held_bytes'] <= 2**22
            assert usage['stages']['ready_held_work'] <= 64
        assert current[0].preparation.snapshot()['held_tasks'] == 0
    assert result['synthetic_calls'] == result['accounted_calls'] == rows
    assert result['core_active_peak'] <= 4 and result['synthetic_worker_peak'] <= 4
    result['preparation_stage_peak'] = {
        key: max((e['preparation']['stages'][key] for e in observations), default=0)
        for key in ('encoded_held_bytes', 'ready_held_bytes', 'ready_held_work', 'prepare_inflight')}
    return result


def main(output):
    output.mkdir(exist_ok=False)
    began = time.monotonic()
    cases = []
    for phase, repeat, rows in [('check', 0, 16), ('warmup', 0, 64),
                                ('measurement', 1, 64), ('measurement', 2, 64), ('measurement', 3, 64)]:
        for staged in ((False, True) if repeat % 2 else (True, False)):
            assert time.monotonic() - began < 120
            arm = 'staged' if staged else 'immediate'
            name = f'{phase}-{repeat}-{arm}'
            result = run_case(output / name, staged, rows)
            cases.append(dict(phase=phase, repeat=repeat, arm=arm, case=name, result=result))
    assert sum(c['result']['synthetic_calls'] for c in cases) == 544
    source_paths = subprocess.check_output(['git', 'diff', '--name-only', '--', 'code/src']).decode().splitlines()
    source_paths.append('code/src/execution_provider/adapters/map_preparation.py')
    summary = dict(status='passed', simulated_calls=544, http_requests=0, model_requests=0, pg_queries=0,
        elapsed_seconds=time.monotonic()-began,
        base_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode().strip(),
        source_sha256={p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in source_paths},
        cases=cases, metrics={})
    for arm in ('immediate', 'staged'):
        summary['metrics'][arm] = {}
        for metric in ('query_seconds', 'first_result_seconds', 'case_seconds', 'preparation_windows'):
            values = [c['result'][metric] for c in cases if c['phase']=='measurement' and c['arm']==arm]
            summary['metrics'][arm][metric] = dict(values=values, median=statistics.median(values))
    (output / 'summary.json').write_text(json.dumps(redact_json_values(summary), indent=2) + '\n')
    print(json.dumps(dict(status=summary['status'], simulated_calls=544, metrics=summary['metrics'])))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    main(parser.parse_args().output)
