from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.contracts import ChatRequest  # noqa: E402

_DC_PATH = CODE_ROOT / "src" / "baselines" / "text" / "products" / "direct_client.py"
_spec = importlib.util.spec_from_file_location("direct_client", _DC_PATH)
dc = importlib.util.module_from_spec(_spec)
sys.modules["direct_client"] = dc
_spec.loader.exec_module(dc)


def _req(doc_id: int, cap: int = 64, endpoint: int = 0) -> ChatRequest:
    return ChatRequest(
        doc_id=doc_id, prompt=f"p{doc_id}", arrival_time_s=0.0,
        prompt_tokens=4, max_output_tokens=cap, estimated_output_tokens=10,
        source_row_hash=f"id{doc_id}", endpoint_index=endpoint,
    )


class DirectClientConfigTests(unittest.TestCase):
    def test_valid(self) -> None:
        c = dc.DirectClientConfig(endpoint_url="http://h/v1/chat/completions",
                                  model="m", max_tokens=64)
        self.assertEqual(c.max_concurrent_requests, 32)  # default fairness

    def test_rejects_empty_endpoint(self) -> None:
        with self.assertRaises(ValueError):
            dc.DirectClientConfig(endpoint_url="", model="m", max_tokens=64)

    def test_rejects_empty_model(self) -> None:
        with self.assertRaises(ValueError):
            dc.DirectClientConfig(endpoint_url="http://h", model="", max_tokens=64)

    def test_rejects_neg_max_tokens(self) -> None:
        with self.assertRaises(ValueError):
            dc.DirectClientConfig(endpoint_url="http://h", model="m", max_tokens=-1)

    def test_rejects_concurrency_out_of_range(self) -> None:
        with self.assertRaises(ValueError):
            dc.DirectClientConfig(endpoint_url="http://h", model="m", max_tokens=64,
                                  max_concurrent_requests=0)
        with self.assertRaises(ValueError):
            dc.DirectClientConfig(endpoint_url="http://h", model="m", max_tokens=64,
                                  max_concurrent_requests=5000)


class ValidateRequestsTests(unittest.TestCase):
    def test_rejects_empty(self) -> None:
        with self.assertRaises(ValueError):
            dc._validate_requests(())

    def test_rejects_multi_endpoint(self) -> None:
        with self.assertRaises(ValueError):
            dc._validate_requests((_req(1, endpoint=0), _req(2, endpoint=1)))

    def test_rejects_mismatched_cap(self) -> None:
        with self.assertRaises(ValueError):
            dc._validate_requests((_req(1, cap=64), _req(2, cap=128)))


class RunDirectClientCapMismatchTests(unittest.TestCase):
    def test_rejects_before_touching_http(self) -> None:
        # config.max_tokens (64) != shard cap (128) -> ValueError before asyncio/httpx.
        config = dc.DirectClientConfig(
            endpoint_url="http://h/v1/chat/completions", model="m", max_tokens=64,
        )
        requests = (_req(1, cap=128),)
        with self.assertRaises(ValueError):
            dc.run_direct_client(requests, config)


if __name__ == "__main__":
    unittest.main()
