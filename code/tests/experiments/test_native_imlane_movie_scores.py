"""Q9 CLI refuses changed artifacts or a spent permanent unit before model startup."""
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
from src.experiments import native_imlane_movie_scores as runner
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger

class Q9PreflightTests(unittest.TestCase):
    def test_spent_unit_is_not_retried_or_given_a_new_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            budget=AttemptBudget('q9-fixture',256)
            ledger=CellBudgetLedger.create(root/'budget.sqlite',budget,deadline_utc=time.time()+30)
            ledger.reserve_unit('cold',256);unit=ledger.claim_shared_unit('cold')
            unit.reserve('0'*64)
            identity=root/'identity.json';identity.write_text(json.dumps(dict(Reviews_csv_sha256='input')))
            config=root/'config.json';config.write_text(json.dumps(dict(input_identity=str(identity),data_dir=str(root),
                build_record='build.json',
                request_budget=dict(path=str(ledger.path),budget_id=budget.budget_id,limit=256,unit_id='cold'))))
            factory=Mock()
            with patch.object(runner,'verified_build',return_value={'binary_path':'driver'}), patch.object(runner,'file_sha256',return_value='input'):
                with self.assertRaisesRegex(ValueError,'continuation/retry forbidden'):
                    runner.run(config,root/'out',service_factory=factory)
            factory.assert_not_called();self.assertEqual(unit.attempts,1)
            self.assertFalse((root/'out').exists())

    def test_original_dso_is_required_before_model_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            record=Path(tmp)/'build.json'
            record.write_text(json.dumps(dict(schema='semloom.imlane.movie_scores_build.v1',status='built',duckdb_library={'sha256':'wrong'})))
            with self.assertRaisesRegex(ValueError,'original qualified DOUBLE DSO'):
                runner.verified_build(record)

    def test_build_binds_current_driver_kernel_ack_and_artifact_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            binary=root/'q9';binary.write_bytes(b'fixture executable')
            library=root/'libduckdb.so';library.write_bytes(b'fixture original DSO')
            record=dict(schema='semloom.imlane.movie_scores_build.v1',status='built',
                binary_path=str(binary),binary=runner.file_identity(binary),
                duckdb_library_path=str(library),duckdb_library=runner.file_identity(library))
            for name,filename in [('driver_source','q9.cpp'),('kernel','q9_udf.py'),('relation_handoff','relation_handoff.hpp')]:
                record[name]=runner.file_identity(runner.KERNEL.parent/filename)
            path=root/'build.json';path.write_text(json.dumps(record))
            with patch.object(runner,'ORIGINAL_DSO_SHA256',record['duckdb_library']['sha256']):
                self.assertEqual(runner.verified_build(path),record)
                record['relation_handoff']['sha256']='0'*64;path.write_text(json.dumps(record))
                with self.assertRaisesRegex(ValueError,'relation_handoff.hpp'):
                    runner.verified_build(path)
