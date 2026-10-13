"""Save the targeted local regression results with redacted diagnostics."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import sys
import unittest

REPO=Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO/'code'))
from src.baselines.common.redact import redact_json_values

MODULES=(
    'tests.experiments.test_lotus_observation_modes',
    'tests.experiments.test_supplier_adapter_observation',
    'tests.experiments.test_native_adapter_metrics',
    'tests.experiments.test_persistent_adapter_cleanup',
    'tests.experiments.test_sembench_queries',
    'tests.semantic_methods.test_lotus_adapter',
    'tests.semantic_methods.test_lotus_response_replay',
    'tests.experiments.test_supplier_adapter_libraries',
    'tests.experiments.test_sembench_lotus_libraries',
)


class Results(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.outcomes=[]

    def addSuccess(self, test):
        super().addSuccess(test)
        self.outcomes.append(dict(test=test.id(), status='passed'))

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self.outcomes.append(dict(test=test.id(), status='skipped', reason=reason))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args=parser.parse_args()
    # This verifier cannot silently opt into any native graph or author runtime.
    for name in ('SEMLOOM_SUPPLIER_ACTUAL_ARM', 'SEMLOOM_MOVIE_ACTUAL_ARMS',
                 'SEMLOOM_MOVIE_UNSUPPORTED_OUTPUT', 'SEMLOOM_MOVIE_ERROR_FIXTURE'):
        os.environ.pop(name, None)
    suite=unittest.defaultTestLoader.loadTestsFromNames(MODULES)
    stream=io.StringIO()
    result=unittest.TextTestRunner(stream=stream, verbosity=2, resultclass=Results).run(suite)
    sources=('code/src/experiments/postgresql/supplier_adapter_query.py',
        'code/src/experiments/postgresql/native_adapter_query.py',
        'code/src/experiments/postgresql/persistent_native_adapter_query.py',
        'code/src/experiments/postgresql/sembench_query_cli.py',
        'code/src/experiments/postgresql/native_adapter_metrics.py',
        'code/tests/experiments/test_supplier_adapter_observation.py',
        'code/tests/experiments/test_lotus_observation_modes.py',
        'code/tests/experiments/test_supplier_adapter_libraries.py')
    value=dict(schema='semloom.lotus_observation_regression.v1',
        baseline_commit='1e96935610069d17430c6fb75fe37a75659aa84e',
        python_version=platform.python_version(), platform=platform.system(), modules=MODULES,
        source_sha256={name:hashlib.sha256((REPO/name).read_bytes()).hexdigest() for name in sources},
        tests_run=result.testsRun, passed=len([v for v in result.outcomes if v['status']=='passed']),
        skipped=len(result.skipped), errors=len(result.errors), failures=len(result.failures),
        success=result.wasSuccessful(), outcomes=result.outcomes,
        errors_detail=[dict(test=test.id(), diagnostic=details) for test, details in result.errors+result.failures],
        log=stream.getvalue(), real_model_requests=0, gpu_work=0,
        controlled_http_posts=16 if result.wasSuccessful() else None,
        http_scope='two-row JSON-only supplier substitute via the real localhost gateway; other exercised tests use no HTTP',
        prior_attempts=[dict(source='summary retained from tool output', tests_run=9, errors=3,
                reason='sandbox denied localhost bind; no model or HTTP requests'),
            dict(source='summary retained from tool output', tests_run=43, errors=4,
                reason='three sandbox process-sampling errors and one existing stale lotus_response test patch'),
            dict(source='summary retained from tool output', tests_run=9, errors=0, failures=0,
                reason='local gateway regression allowed by automatic review; 16 fixture POST')])
    with args.output.open('x') as output:
        output.write(json.dumps(redact_json_values(value), ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({k:value[k] for k in ('tests_run','passed','skipped','errors','failures','success','controlled_http_posts')}))
    raise SystemExit(0 if result.wasSuccessful() else 1)
