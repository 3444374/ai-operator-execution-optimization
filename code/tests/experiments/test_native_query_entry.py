"""Native adapters release an iterator before executing source or model work."""
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

from src.baselines.text.frameworks.lotus_pg import open_rows as lotus_rows
from src.baselines.text.frameworks.ray_data_pg_http import open_rows as ray_rows,RaySqlHttpConfig


class NativeEntryTests(unittest.TestCase):
    def test_lotus_query_failure_occurs_in_iteration(self):
        from contextlib import nullcontext
        original=Mock(side_effect=ValueError('native query failed'))
        with patch('src.baselines.text.frameworks.lotus_pg.observe_filter_rows',return_value=nullcontext()),\
             patch('src.baselines.text.frameworks.lotus_pg.run_original_lotus_query',original):
            with lotus_rows(None,None,None,3,Mock(),Mock()) as rows:
                original.assert_not_called()
                with self.assertRaisesRegex(ValueError,'native query failed'):next(rows)
                original.assert_called_once()

    def test_ray_sql_plan_failure_occurs_in_iteration(self):
        read=Mock(side_effect=ValueError('SQL support probe failed'))
        ray=SimpleNamespace(__version__='2.56.1',is_initialized=lambda:True,data=SimpleNamespace(read_sql=read))
        llm=SimpleNamespace(HttpRequestProcessorConfig=Mock(),build_processor=Mock())
        inputs=SimpleNamespace(select_sql=lambda **kwargs:('SELECT fixture',[]))
        with patch.dict(sys.modules,{'ray':ray,'ray.data':ray.data,'ray.data.llm':llm}):
            with ray_rows(inputs,None,RaySqlHttpConfig(1,1,1,1),None,None) as rows:
                read.assert_not_called()
                with self.assertRaisesRegex(ValueError,'SQL support probe'):next(rows)
                read.assert_called_once()
