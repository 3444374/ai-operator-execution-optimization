"""Repeat the local observation substitute measurement; no SDK, HTTP or model."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import statistics
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch

REPO=Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO/'code'))
from src.baselines.common.redact import redact_json_values
from src.experiments.postgresql.supplier_adapter_query import _LotusBatchObservation
from tests.experiments.test_lotus_observation_modes import fixture_prepare, observations


def sample(rows, mode):
    data=[([dict(role='user', content='x'*2048)], None)]*rows
    values=[dict(row_id=str(index)) for index in range(rows)]
    observed, writer, raw=observations()
    responses=[SimpleNamespace(model_dump=lambda:dict(model='fixture', value='ok'))]*rows
    native_preparations=0
    def original(batch, kwargs, show, description):
        nonlocal native_preparations
        for messages, _ in batch:
            fixture_prepare(None, messages, kwargs)
            native_preparations+=1
        return responses
    lm=SimpleNamespace(_process_uncached_messages=original)
    observer=_LotusBatchObservation(lm, values, observed, observation_mode=mode)
    with patch('src.semantic_methods.lotus.sdk.prepare_call', side_effect=fixture_prepare) as prepare:
        started=time.perf_counter_ns()
        result=observer(lm, data, dict(max_tokens=16, temperature=0), False, 'fixture')
        elapsed=(time.perf_counter_ns()-started)/1e9
    expected=rows if mode=='qualification' else 0
    assert result is responses and native_preparations==rows and prepare.call_count==expected
    assert len(observed.calls)==rows and len(raw.getvalue().splitlines())==rows
    return dict(seconds=elapsed, expected_preparations=prepare.call_count,
                native_substitute_preparations=native_preparations,
                method_occurrences=len(observed.calls), events=len(writer.events))


def run():
    cases=[]
    for rows in (128, 2000):
        warmups={mode:sample(rows, mode) for mode in ('qualification', 'performance')}
        samples={mode:[] for mode in warmups}
        for repeat in range(7):
            order=('qualification', 'performance') if repeat%2==0 else ('performance', 'qualification')
            for mode in order:
                samples[mode].append(sample(rows, mode))
        medians={mode:statistics.median(s['seconds'] for s in values) for mode, values in samples.items()}
        cases.append(dict(rows=rows, prompt_characters=2048, warmups_excluded=warmups,
            samples=samples, median_seconds=medians,
            median_difference_seconds=medians['qualification']-medians['performance']))
    sources=('code/src/experiments/postgresql/supplier_adapter_query.py',
             'code/tests/experiments/test_lotus_observation_modes.py')
    return dict(schema='semloom.lotus_observation_substitute.v1',
        baseline_commit='1e96935610069d17430c6fb75fe37a75659aa84e',
        source_sha256={name:hashlib.sha256((REPO/name).read_bytes()).hexdigest() for name in sources},
        python_version=platform.python_version(), platform=platform.system(), machine=platform.machine(),
        identity='local JSON preparation and native SDK response substitutes; in-memory event/raw writers',
        comparison_scope='complete batch observer plus unchanged serial JSON-only native substitute',
        limitations='not actual LOTUS/LiteLLM preparation, model latency, vendor scheduling or server performance',
        real_model_requests=0, http_requests=0, gpu_work=0, warmups_per_mode=1, repeats_per_mode=7,
        timing_adjustment='none; each sample is its directly measured full interval', cases=cases)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    arguments=parser.parse_args()
    value=json.dumps(redact_json_values(run()), ensure_ascii=False, indent=2)+'\n'
    if arguments.output:
        with arguments.output.open('x') as stream:
            stream.write(value)
    else:
        print(value, end='')
