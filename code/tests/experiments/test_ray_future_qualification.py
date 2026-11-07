"""Actual unittest outcomes from explicit CPU cases, not Ray execution evidence."""
from contextlib import redirect_stderr
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


TOOL=Path(__file__).resolve().parents[2]/'scripts/profiling/record_observation/ray_future_qualification.py'
spec=importlib.util.spec_from_file_location('ray_qualification_cpu_target',TOOL)
qualification=importlib.util.module_from_spec(spec);spec.loader.exec_module(qualification)


class QualificationResultTests(unittest.TestCase):
    def test_actual_default_module_selects_required_ray_cases_and_cpu_provider_cases(self):
        suite=unittest.defaultTestLoader.loadTestsFromName(qualification.MODULE)
        selected={test.id() for test in qualification._tests(suite)}
        self.assertTrue(qualification.REQUIRED<=selected)
        self.assertTrue(any('.ProviderShapeTests.' in name for name in selected))

    def test_actual_worker_with_ray_disabled_records_skips_and_cannot_qualify(self):
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'RESULT.json'
            result=subprocess.run([sys.executable,'-B',str(TOOL),'--output',str(output)],
                env=dict(os.environ,SEMLOOM_REAL_RAY_CPU='0',CUDA_VISIBLE_DEVICES=''),
                cwd=TOOL.parents[4],capture_output=True,text=True,timeout=30)
            self.assertEqual(result.returncode,1,result.stdout+result.stderr)
            value=json.loads(output.read_text())
            self.assertFalse(qualification.qualified(value))
            self.assertFalse(qualification.REQUIRED.intersection(value['passed_test_ids']))
            self.assertTrue(any('.ProviderShapeTests.' in name for name in value['passed_test_ids']))

    def run_cases(self, outcomes):
        class CPUCase(unittest.TestCase):
            def __init__(self,name,outcome):
                super().__init__('runTest');self.name,self.outcome=name,outcome
            def id(self):return self.name
            def runTest(self):
                if self.outcome=='skip':self.skipTest('explicit CPU fixture skip')
                if self.outcome=='failure':self.fail('explicit CPU fixture failure')
                if self.outcome=='error':raise ValueError('explicit CPU fixture error')
        suite=unittest.TestSuite(CPUCase(name,outcome)for name,outcome in outcomes)
        with tempfile.TemporaryDirectory() as directory,redirect_stderr(io.StringIO()):
            output=Path(directory)/'RESULT.json'
            status=qualification.run_tests(output,suite=suite)
            return status,json.loads(output.read_text())

    def test_required_cases_and_extra_cpu_cases_pass_without_a_fixed_total(self):
        names=sorted(qualification.REQUIRED)
        for extra in ((),('additional CPU case',),('CPU one','CPU two','CPU three')):
            with self.subTest(extra=extra):
                code,value=self.run_cases((name,'ok')for name in [*names,*extra])
                self.assertEqual(code,0)
                self.assertTrue(qualification.qualified(value))
                self.assertEqual(value['passed_test_ids'],[*names,*extra])

    def test_required_skip_failure_error_or_missing_case_is_rejected(self):
        names=sorted(qualification.REQUIRED)
        for outcome in ('skip','failure','error','missing'):
            with self.subTest(outcome=outcome):
                cases=[(name,'ok')for name in names[1:]]
                if outcome!='missing':cases.append((names[0],outcome))
                code,value=self.run_cases(cases)
                self.assertEqual(code,1)
                self.assertFalse(qualification.qualified(value))

    def test_extra_skip_failure_or_error_is_not_hidden_by_required_successes(self):
        for outcome in ('skip','failure','error'):
            with self.subTest(outcome=outcome):
                cases=[(name,'ok')for name in sorted(qualification.REQUIRED)]+[('extra',outcome)]
                code,value=self.run_cases(cases)
                self.assertEqual(code,1)
                self.assertFalse(qualification.qualified(value))
