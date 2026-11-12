"""Receipt path regression only; stub renderer provides no vLLM qualification."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from .replay_lotus_topk import renderer_check


class RendererReceiptTests(unittest.TestCase):
    def test_cli_string_output_writes_receipt_and_closes_executor(self):
        conversation = [{"role": "user", "content": "fixture"}]
        pair = dict(messages=conversation, normalized_conversation=conversation,
                    expected_token_ids=[1, 2])
        executor = Mock()
        renderer = SimpleNamespace(
            render_chat=Mock(return_value=([conversation], [{"prompt_token_ids": [1, 2]}])),
            _executor=executor)
        config = ModuleType("vllm.config")
        config.DeviceConfig = config.ModelConfig = config.VllmConfig = lambda **kw: kw
        renderers = ModuleType("vllm.renderers")
        renderers.ChatParams = lambda **kw: kw
        renderers.renderer_from_config = lambda config: renderer
        vllm = ModuleType("vllm")
        vllm.__version__ = "0.25.1"
        torch = ModuleType("torch")
        torch.cuda = SimpleNamespace(is_initialized=lambda: False)
        with TemporaryDirectory() as directory:
            fixture, output = Path(directory)/"pairs.json", Path(directory)/"receipt.json"
            fixture.write_text(json.dumps(dict(pairs=[pair])))
            with patch.dict("sys.modules", {"vllm": vllm, "vllm.config": config,
                    "vllm.renderers": renderers, "torch": torch}):
                renderer_check(str(fixture), "software-fixture-only", str(output))
            receipt = json.loads(output.read_text())
            self.assertEqual(receipt["status"], "passed")
            self.assertEqual(receipt["pairs"], 1)
            self.assertEqual(receipt["prompt_tokens"], [2])
            self.assertFalse(receipt["CUDA_initialized"])
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            executor.shutdown.assert_called_once_with(wait=True)


if __name__ == "__main__":
    unittest.main()
