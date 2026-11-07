"""Record actual unittest outcomes for the existing CPU-only Ray qualification."""
import argparse
from pathlib import Path
import unittest

from src.infrastructure.private_artifacts import write_private_json


MODULE = 'tests.execution_provider.test_core_feedback_ray_future'
REQUIRED = frozenset(MODULE+'.RealRayFeedbackFutureTests.'+name for name in (
    'test_pending_then_done_keeps_one_real_future_and_yields_feedback_without_wait',
    'test_real_ray_exception_releases_pair_and_allows_one_new_optional_read',
    'test_expired_real_ray_request_is_cancelled_once_then_drained',
    'test_cancel_failure_retains_pair_across_query_until_old_read_settles',
    'test_wrong_query_snapshot_releases_pair_without_delivering_then_new_query_succeeds',
))


def qualified(value):
    selected, passed = value.get('selected_test_ids'), value.get('passed_test_ids')
    return (value.get('schema') == 'semloom.ray_future_tests.v1' and value.get('successful') is True
        and type(selected) is list and type(passed) is list
        and all(type(name) is str for name in selected+passed)
        and REQUIRED <= set(passed) == set(selected))


class OutcomeResult(unittest.TextTestResult):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.passed = []

    def addSuccess(self,test):
        self.passed.append(test.id())
        super().addSuccess(test)


def _tests(suite):
    for item in suite:
        if isinstance(item,unittest.TestSuite):
            yield from _tests(item)
        else:
            yield item


def run_tests(output, *, suite=None):
    suite = unittest.defaultTestLoader.loadTestsFromName(MODULE) if suite is None else suite
    selected = [test.id() for test in _tests(suite)]
    result = unittest.TextTestRunner(verbosity=2,resultclass=OutcomeResult).run(suite)
    value = dict(schema='semloom.ray_future_tests.v1',selected_test_ids=selected,
        passed_test_ids=result.passed,successful=result.wasSuccessful())
    write_private_json(output,value)
    return 0 if qualified(value) else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True,type=Path)
    raise SystemExit(run_tests(parser.parse_args().output))
