import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.common.private_artifacts import write_private_json
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.postgresql.query_config import QueryConfig
from src.experiments.postgresql.query_workloads import prepare
from src.experiments.postgresql.ready_semantic_query import run_ready_pg_query


class ReadyPgLifecycleTests(unittest.TestCase):
    def test_summary_write_error_does_not_replace_original_pg_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            prepare(root/'inputs','movie',[('a','m','review','POSITIVE','original')],{},max_rows=1)
            model=root/'model.json'
            model.write_text(json.dumps(dict(endpoint_url='http://127.0.0.1:1/v1/chat/completions',model_id='fixture',timeout_ms=1000)))
            class Gateway:
                def __init__(self,**_):pass
                def __enter__(self):return self
                def __exit__(self,*_):pass
                def endpoint_url(self,*_):return 'http://127.0.0.1:1/v1/chat/completions'
            def write(path,value):
                if Path(path).name=='summary.json':raise OSError('summary write failed')
                return write_private_json(path,value)
            with patch('src.experiments.postgresql.ready_semantic_query.ObservationGateway',Gateway), \
                 patch('src.experiments.postgresql.ready_semantic_query.run_query',side_effect=RuntimeError('original PG failure')), \
                 patch('src.experiments.postgresql.ready_semantic_query.write_private_json',side_effect=write):
                with self.assertRaisesRegex(RuntimeError,'original PG failure'):
                    run_ready_pg_query(QueryConfig('fixture','pg','map','raw_input'),
                        manifest_path=root/'inputs/manifest.json',model_path=model,
                        budget_path=root/'unused-budget.sqlite',budget=AttemptBudget('ready.fixture',1),
                        root=root/'query',dsn='fixture',pg_log=root/'unused.log')


if __name__=='__main__':unittest.main()
