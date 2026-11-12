"""CLI provenance checks with local tokenizer/SDK fixtures and no model calls."""
from contextlib import nullcontext
from pathlib import Path
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.research.data_organization import native_frame_profile
from src.research.data_organization.message_frame_replay import fixture_frontier
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec


def module(name, **values):
    result = ModuleType(name)
    result.__dict__.update(values)
    return result


class NativeFrameProfileTests(unittest.TestCase):
    def test_explicit_template_cannot_emit_default_template_tokens(self):
        generation = GenerationSpec(max_tokens=8)
        engine = EngineSpec('/tmp/fixture-model', sys.executable, sys.executable,
                            chat_template='CUSTOM_TEMPLATE')
        row = fixture_frontier(1, 16)[0]['row']
        stage = SimpleNamespace(generation=generation)
        query = SimpleNamespace(operation=SimpleNamespace(stage=stage, records=[object()]),
                                query_id='cpu-frame-replay', describe=lambda: {})
        method = SimpleNamespace(stage=stage, prepare_rows=lambda *a, **k: [dict(row)])
        tokenizer = SimpleNamespace(chat_template='DEFAULT_TEMPLATE', init_kwargs={},
            apply_chat_template=lambda messages, **kw: (
                [7, 8, 9] if kw.get('chat_template') == 'CUSTOM_TEMPLATE' else [1, 2]))
        external = {
            'lotus': module('lotus', settings=SimpleNamespace(context=lambda **k: nullcontext())),
            'transformers': module('transformers', AutoTokenizer=SimpleNamespace(
                from_pretrained=lambda *a, **k: tokenizer)),
            'src.experiments.sembench_text_query': module('src.experiments.sembench_text_query',
                configuration=lambda path: ({}, engine, generation, None),
                query_from_config=lambda *a: query),
            'src.semantic_methods.lotus.record_method': module('src.semantic_methods.lotus.record_method',
                LotusRecordMethod=lambda selected: method),
            'src.semantic_methods.lotus.source': module('src.semantic_methods.lotus.source',
                CHAIN_FILES=(), source_report=lambda *a: {}),
        }
        with tempfile.TemporaryDirectory() as directory, patch.dict(sys.modules, external):
            output = Path(directory) / 'profile'
            with self.assertRaisesRegex(ValueError, 'tokenizer-owned chat template'):
                native_frame_profile.main([
                    '--config', '/tmp/fixture-config', '--sembench-checkout', '/tmp/fixture-checkout',
                    '--data-dir', '/tmp/fixture-data', '--tokenizer', '/tmp/fixture-tokenizer',
                    '--output-dir', str(output), '--rows', '1', '--repeats', '1',
                    '--item-input-bytes', '65536', '--input-bytes', '65536',
                    '--item-result-bytes', '8192',
                ])
            self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
