"""Original source/SDK/tokenizer input freezing, without any model owner."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from src.experiments import native_imlane_filters as entry
from tests.experiments.test_native_imlane_filters import config

CHECKOUT = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')
TOKENIZER = os.environ.get('SEMLOOM_NATIVE_TEST_TOKENIZER')


@unittest.skipUnless(CHECKOUT and TOKENIZER, 'set pinned source and local tokenizer paths')
class FreezeConfigTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        pd.DataFrame({'title': ['duplicate', 'duplicate', 'other'],
                      'text': ['a comedy', 'a comedy', 'line one\nline two 好']}).to_csv(
            self.root / 'lizzy_caplan_text_data.csv', index=False)
        self.seed = config()
        for name in ('input_sha256', 'prompt_columns', 'max_model_calls'):
            self.seed.pop(name)
        self.seed.update(query_id='3a', data_dir=str(self.root), sembench_checkout=CHECKOUT,
                         tokenizer=TOKENIZER)

    def test_first_raw_order_source_count_and_original_full_messages_tokens(self):
        observed = []
        original = entry.prepare
        def capture(query, **kwargs):
            result = original(query, **kwargs)
            observed.append(list(result[0].columns))
            return result
        with patch.object(entry, 'prepare', side_effect=capture):
            frozen = entry.freeze_config(self.seed)
        self.assertEqual(observed, [frozen['prompt_columns']])
        self.assertEqual(set(frozen['prompt_columns']), {'title', 'text'})
        self.assertEqual(frozen['max_model_calls'], 3)
        self.assertEqual(frozen['input_sha256'], {'lizzy_caplan_text_data.csv': hashlib.sha256(
            (self.root / 'lizzy_caplan_text_data.csv').read_bytes()).hexdigest()})
        query, method, prepared, requests, tokens, _rows = entry.prepare_query(frozen)
        from lotus.templates.task_instructions import df2multimodal_info, filter_formatter
        from lotus.nl_expression import nle2str
        from transformers import AutoTokenizer
        messages = [filter_formatter(method.lm, doc, nle2str(method.method.instruction, method.columns))
                    for doc in df2multimodal_info(query.operation.frame, method.columns)]
        self.assertEqual([row['messages'] for row in requests], messages)
        self.assertEqual([json.loads(row['messages_json']) for row in prepared], messages)
        self.assertIn('[Title]:', str(messages))
        self.assertIn('[Text]:', str(messages))
        tokenizer = AutoTokenizer.from_pretrained(TOKENIZER, local_files_only=True, trust_remote_code=False)
        self.assertEqual(tokens, [tokenizer.apply_chat_template(m, tokenize=True,
            add_generation_prompt=True) for m in messages])
        with self.assertRaisesRegex(ValueError, 'original prompt columns'):
            entry.prepare_query(dict(frozen, prompt_columns=['Title', 'Text']))
        with self.assertRaisesRegex(ValueError, 'every original occurrence'):
            entry.prepare_query(dict(frozen, max_model_calls=25))
        (self.root / 'lizzy_caplan_text_data.csv').write_text('title,text\nchanged,data\n')
        with self.assertRaisesRegex(ValueError, 'source files changed'):
            entry.prepare_query(frozen)

    def test_cli_config_consumed_unchanged_across_python_hash_seeds(self):
        seed_path = self.root / 'seed.json'
        seed_path.write_text(json.dumps(self.seed))
        entry.main(['freeze-config', '--config', str(seed_path), '--output', str(self.root / 'frozen')])
        config_path = self.root / 'frozen/CONFIG.json'
        script = '''import json,sys
from src.experiments.native_imlane_filters import prepare_query
q,m,p,r,t,rows=prepare_query(json.load(open(sys.argv[1])))
print(json.dumps(dict(columns=m.columns,requests=r,tokens=t)))'''
        results = []
        for seed in ('1', '42'):
            completed = subprocess.run([sys.executable, '-B', '-c', script, str(config_path)],
                env=dict(os.environ, PYTHONHASHSEED=seed), text=True, capture_output=True, timeout=90,
                check=True)
            results.append(json.loads(completed.stdout.strip().splitlines()[-1]))
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0]['columns'], json.loads(config_path.read_text())['prompt_columns'])
        with self.assertRaises(FileExistsError):
            entry.main(['freeze-config', '--config', str(seed_path), '--output', str(self.root / 'frozen')])
