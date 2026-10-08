"""Finite local comparison; synthetic tables/workers, no network or model."""

import argparse
from collections import Counter
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


def run_case(output, arm, rows, prepare_s):
    transports = []
    base_transport = fixture.RayMapTransport
    base_build = fixture.build_fixed_model_execution
    base_capture = fixture.EventCapture

    class Transport(base_transport):
        def __init__(self, *args, **kwargs):
            kwargs['physical'] = replace(kwargs['physical'], window_bytes=2**21, object_bytes=2**22,
                preparation=StageBrokerLimits(2**21, 2**22, 256, 1, 128) if arm != 'immediate' else None,
                coalesce_queued_preparation=arm == 'coalesced')
            super().__init__(*args, **kwargs)
            transports.append(self)

    class Capture(base_capture):
        def __init__(self, maximum, writer=None):
            super().__init__(max(maximum, 32 * rows + 1000), writer)

    def build(*args, **kwargs):
        kwargs['max_tasks'] = min(128, kwargs['max_tasks'])
        if arm != 'immediate':
            kwargs['preparation_factory'] = lambda transport, limits, notify: transport.prepare_inputs(limits, notify)
        return base_build(*args, **kwargs)

    def batches(data, limits, *, batch_rows, backend):
        assert backend == 'arrow' and len(data) <= limits.rows
        assert sum(len(row[2]) + 24 for row in data) <= limits.bytes
        transports[0].observer(dict(event='fixture_window', rows=len(data)))
        for start in range(0, len(data), batch_rows):
            time.sleep(prepare_s)
            yield fixture.SyntheticTable(data[start:start + batch_rows])

    with patch.object(fixture, 'RayMapTransport', Transport), \
         patch.object(fixture, 'build_fixed_model_execution', build), \
         patch.object(fixture, 'EventCapture', Capture), \
         patch.object(fixture, 'PREPARE_S', prepare_s), \
         patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', batches):
        result = fixture.run_case(output, 'durable', 'buffered', rows=rows, capacity=64, batch_rows=16)
    with gzip.open(output / 'events.jsonl.gz', 'rt') as stream:
        events = [json.loads(line) for line in stream]
    groups = [e['rows'] for e in events if e['event'] == 'ray_preparation_completed']
    if arm != 'immediate':
        assert groups and sum(groups) == rows
        assert not any(e.get('failed') for e in events if e['event'] == 'ray_preparation_completed')
        assert transports[0].preparation.snapshot()['held_tasks'] == 0
    result['prepared_group_sizes'] = dict(sorted(Counter(groups).items()))
    result['prepared_blocks'] = len(groups)
    assert result['synthetic_calls'] == result['accounted_calls'] == rows
    assert result['core_active_peak'] <= 64 and result['synthetic_worker_peak'] <= 64
    assert result['resources_drained'] and result['exactly_once']
    return result


def main(output):
    output.mkdir(exist_ok=False)
    began = time.monotonic()
    cases = []
    for prepare_s in (.0005, .015):
        for phase, repeat, rows in [('check', 0, 16), ('warmup', 0, 1024),
                                    ('measurement', 1, 1024), ('measurement', 2, 1024),
                                    ('measurement', 3, 1024)]:
            arms = ('immediate', 'original', 'coalesced')
            if repeat % 2 == 0:
                arms = arms[::-1]
            for arm in arms:
                assert time.monotonic() - began < 360
                name = f'{prepare_s:g}-{phase}-{repeat}-{arm}'
                print(json.dumps(dict(case=name, status='starting')), flush=True)
                result = run_case(output / name, arm, rows, prepare_s)
                cases.append(dict(prepare_seconds=prepare_s, phase=phase, repeat=repeat,
                                  arm=arm, case=name, result=result))
                print(json.dumps(dict(case=name, status='passed', seconds=result['query_seconds'],
                                      prepared_blocks=result['prepared_blocks'])), flush=True)
    assert sum(c['result']['synthetic_calls'] for c in cases) == 24672
    source = subprocess.check_output(['git', 'diff', '--name-only', '--', 'code/src']).decode().splitlines()
    source.append(str(Path(__file__).resolve().relative_to(Path.cwd())))
    summary = dict(status='passed', simulated_calls=24672, http_requests=0, model_requests=0,
        pg_queries=0, elapsed_seconds=time.monotonic()-began,
        base_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode().strip(),
        source_sha256={p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in source},
        cases=cases, metrics={})
    for cost in (.0005, .015):
        summary['metrics'][str(cost)] = {}
        for arm in ('immediate', 'original', 'coalesced'):
            measurements = [c['result'] for c in cases
                            if c['phase']=='measurement' and c['arm']==arm and c['prepare_seconds']==cost]
            summary['metrics'][str(cost)][arm] = {
                metric: dict(values=[m[metric] for m in measurements],
                             median=statistics.median(m[metric] for m in measurements))
                for metric in ('query_seconds', 'first_result_seconds', 'case_seconds',
                               'preparation_windows', 'prepared_blocks', 'core_active_mean', 'worker_active_mean')
                if metric in measurements[0]}
    (output / 'summary.json').write_text(json.dumps(redact_json_values(summary), indent=2) + '\n')
    print(json.dumps(dict(status='passed', simulated_calls=24672, metrics=summary['metrics'])),flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    main(parser.parse_args().output)
