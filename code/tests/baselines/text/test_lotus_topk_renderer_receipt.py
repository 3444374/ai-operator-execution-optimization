"""Source-derived TopK pairs and receipts; no model or vLLM qualification."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from .replay_lotus_topk import pair_fixture, renderer_check


class RendererReceiptTests(unittest.TestCase):
    def test_pair_fixture_uses_declared_fields_and_retains_equal_occurrences_at_any_size(self):
        import pandas as pd
        from src.baselines.text.sembench_text import _operation
        from src.serving.backends.vllm_protocol import GenerationSpec

        tokenizer = Mock()
        tokenizer.apply_chat_template.return_value = [1, 2]
        for count in (0, 1, 3, 9):
            with self.subTest(count=count):
                frame = pd.DataFrame(dict(summary=['summary marker'] * count,
                    title=['title marker'] * count, unused=['never serialized'] * count))
                op = _operation('topk', frame, ('Rank {summary} and {title}',),
                    dict(K=count, method='quick', return_stats=True), model_name='openai/local-fixture',
                    generation=GenerationSpec(), query_id='software-pairs')
                fixture = pair_fixture(SimpleNamespace(operation=op), tokenizer)
                pairs = fixture['pairs']
                self.assertEqual({(p['left'], p['right']) for p in pairs},
                    {(left, right) for left in range(count) for right in range(count) if left != right})
                self.assertEqual(len(pairs), count * (count - 1))
                for pair in pairs:
                    serialized = json.dumps(pair['messages'])
                    self.assertIn('summary marker', serialized)
                    self.assertIn('title marker', serialized)
                    self.assertNotIn('never serialized', serialized)
                    self.assertEqual(pair['expected_token_ids'], [1, 2])
                if pairs:
                    self.assertEqual(len({p['messages_sha256'] for p in pairs}), 1)

    def test_cli_receipt_and_context_rejection_both_close_executor(self):
        conversation = [{"role": "user", "content": "fixture"}]
        pair = dict(messages=conversation, normalized_conversation=conversation,
                    expected_token_ids=[1, 2])
        executor = Mock()
        renderer = SimpleNamespace(
            render_chat=Mock(return_value=([conversation], [{"prompt_token_ids": [1, 2]}])),
            _executor=executor)
        config = ModuleType("vllm.config")
        config.DeviceConfig = config.VllmConfig = lambda **kw: kw
        config.ModelConfig = lambda **kw: SimpleNamespace(**kw)
        renderers = ModuleType("vllm.renderers")
        renderers.ChatParams = lambda **kw: kw
        renderers.renderer_from_config = lambda config: renderer
        vllm = ModuleType("vllm")
        vllm.__version__ = "0.25.1"
        torch = ModuleType("torch")
        torch.cuda = SimpleNamespace(is_initialized=lambda: False)
        with TemporaryDirectory() as directory:
            fixture, output = Path(directory)/"pairs.json", Path(directory)/"receipt.json"
            fixture.write_text(json.dumps(dict(pairs=[pair], requested_output_tokens=8192)))
            with patch.dict("sys.modules", {"vllm": vllm, "vllm.config": config,
                    "vllm.renderers": renderers, "torch": torch}):
                renderer_check(str(fixture), "software-fixture-only", str(output))
                fixture.write_text(json.dumps(dict(pairs=[pair], requested_output_tokens=32767)))
                rejected = Path(directory) / 'rejected.json'
                with self.assertRaises(AssertionError):
                    renderer_check(str(fixture), "software-fixture-only", str(rejected))
                self.assertFalse(rejected.exists())
            receipt = json.loads(output.read_text())
            self.assertEqual(receipt["status"], "passed")
            self.assertEqual(receipt["pairs"], 1)
            self.assertEqual(receipt["prompt_tokens"], [2])
            self.assertEqual(receipt["requested_output_tokens"], 8192)
            self.assertTrue(receipt["all_pairs_fit_context"])
            self.assertFalse(receipt["CUDA_initialized"])
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            self.assertEqual(executor.shutdown.call_count, 2)
            executor.shutdown.assert_called_with(wait=True)


if __name__ == "__main__":
    unittest.main()
