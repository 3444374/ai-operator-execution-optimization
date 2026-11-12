"""Compile and execute the production C++ argument parser without author libs.

The function body is copied directly from the current translation unit at test
setup, never reimplemented. This executes its CLI contract, not DuckDB/SQL;
Linux native SQL remains a separate mandatory zero-model gate.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from src.experiments import native_imlane_filters as binding
from src.infrastructure.private_artifacts import file_identity

SOURCE = Path(binding.__file__).resolve().parents[2] / 'integrations/imlane_duckdb/mmqa_filter.cpp'


class FilterArgumentExecutionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        root = Path(cls.temporary.name)
        source = SOURCE.read_text()
        start = source.index('std::map<std::string, std::string> arguments(')
        end = source.index('\nvoid check(', start)
        harness = root/'arguments.cpp'
        harness.write_text('#include <map>\n#include <string>\n#include <stdexcept>\n#include <iostream>\n' + source[start:end] + '''
int main(int argc, char **argv) {
    try { std::cout << arguments(argc, argv).at("--expected-rows") << "\\n"; return 0; }
    catch (const std::exception &error) { std::cerr << error.what() << "\\n"; return 1; }
}
''')
        cls.binary = root/'arguments'
        compiler = os.environ.get('CXX') or shutil.which('c++')
        if not compiler:
            raise RuntimeError('a C++ compiler is required; parser execution was not validated')
        subprocess.run([compiler,'-std=c++17','-Wall','-Wextra','-Werror',str(harness),'-o',str(cls.binary)],
                       check=True,capture_output=True,text=True,timeout=30)

    def run_parser(self, value=None, extra=()):
        command = [str(self.binary),'--input','input.csv','--module','kernel.py','--function','infer_mmqa',
                   '--kind','thread','--output','query.json','--sink-release','release.ack']
        if value is not None: command += ['--expected-rows',value]
        return subprocess.run(command+list(extra),capture_output=True,text=True,timeout=5)

    def test_positive_int64_cardinality_has_no_query_whitelist(self):
        for count in ('1','7','25','120','1200','2000','2001','9223372036854775807'):
            with self.subTest(count=count):
                result = self.run_parser(count)
                self.assertEqual(result.returncode,0,result.stderr)
                self.assertEqual(result.stdout.strip(),count)

    def test_invalid_or_unrepresentable_cardinality_is_rejected(self):
        for count in ('','0','-1','+120',' 120','120 ','120x','1.2','nan','１２','١٢','9223372036854775808','18446744073709551616'):
            with self.subTest(count=count): self.assertNotEqual(self.run_parser(count).returncode,0)

    def test_count_must_be_explicit_and_cannot_be_overridden(self):
        self.assertNotEqual(self.run_parser().returncode,0)
        self.assertNotEqual(self.run_parser('120',('--expected-rows','25')).returncode,0)


class FilterBuildSourceIdentityTests(unittest.TestCase):
    def test_stale_driver_is_rejected_before_model_start_even_with_valid_binary_kernel(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            record = dict(schema='semloom.imlane.filter_driver_build.v1',status='built',variant='original-author')
            for name in ('binary','duckdb_library','kernel'):
                path = root/name; path.write_text(name)
                record[name+'_path'] = str(path); record[name] = file_identity(path)
            record['driver_source'] = file_identity(SOURCE)
            path = root/'build.json'; path.write_text(json.dumps(record))
            def files(p):
                if Path(p) == binding.CODE/'integrations/imlane_duckdb/mmqa_filter_udf.py': return record['kernel']
                return file_identity(p)
            with patch.object(binding,'DSO_SHA256',record['duckdb_library']['sha256']), \
                    patch.object(binding,'file_identity',side_effect=files):
                self.assertEqual(binding.verified_build(path),record)
                for stale in ({'sha256':'0'*64,'bytes':1},None):
                    record['driver_source'] = stale; path.write_text(json.dumps(record))
                    with self.subTest(stale=stale), self.assertRaisesRegex(ValueError,'current native driver source'):
                        binding.verified_build(path)
                    from types import SimpleNamespace
                    from tests.experiments.test_native_imlane_filters import config
                    service = Mock()
                    with self.assertRaisesRegex(ValueError,'current native driver source'):
                        binding.run_worker(SimpleNamespace(output=root/'unused',build_record=path),
                                           config(),service_factory=service)
                    service.assert_not_called()
                    self.assertFalse((root/'unused').exists())


if __name__ == '__main__': unittest.main()
