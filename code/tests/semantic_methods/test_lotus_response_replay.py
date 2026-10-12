"""Shared replay dispatch and pinned SDK differential parsing, with no HTTP I/O."""
from importlib.util import find_spec
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.native_calls import NativeCallOutcome
from src.execution_provider.adapters.full_response import FullModelResponse
from src.semantic_methods.lotus import native, sdk


class ReplayDispatchTests(unittest.TestCase):
    def test_native_compatibility_entry_preserves_shared_inputs(self):
        lm, messages, kwargs, prepared = object(), object(), object(), object()
        full = FullModelResponse(429, (), b'{}')
        outcome = NativeCallOutcome("fixture", 0, full)
        marker = object()
        with patch.object(native, "parse_lotus_response", return_value=marker) as parser:
            self.assertIs(native.parse_native_response(lm, messages, kwargs, prepared, outcome), marker)
        parser.assert_called_once_with(lm, messages, kwargs, prepared, full, "")

    def test_native_compatibility_entry_preserves_timeout_metadata(self):
        outcome = NativeCallOutcome("fixture", 0, None, "TimeoutError: fixture")
        with patch.object(native, "parse_lotus_response", side_effect=ValueError("fixture")) as parser:
            with self.assertRaises(ValueError):
                native.parse_native_response(None, [], {}, None, outcome)
        self.assertEqual(parser.call_args.args[-2:], (None, "TimeoutError: fixture"))


PINNED = all(find_spec(name) is not None for name in ("lotus", "litellm", "openai", "httpx"))


@unittest.skipUnless(PINNED, "fixed LOTUS/OpenAI/LiteLLM packages are unavailable; no install performed")
class PinnedSDKReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sdk.validate_source()

    def inputs(self):
        lm = SimpleNamespace(model="openai/fixture-model")
        messages = [dict(role="user", content="complete fixture")]
        kwargs = dict(api_base="http://localhost/v1", api_key="fixture", timeout=.1,
                      num_retries=0, max_retries=0, max_tokens=8)
        prepared = sdk.prepare_call(lm, messages, kwargs)
        return lm, messages, kwargs, prepared

    def reference(self, lm, messages, kwargs, prepared, full, transport_error=""):
        import httpx
        from openai import OpenAI
        from litellm import completion
        count = []

        def cached(request):
            count.append(request)
            self.assertEqual(json.loads(request.read()), json.loads(prepared.payload))
            if transport_error:
                raise httpx.ReadTimeout(transport_error, request=request)
            return httpx.Response(full.status_code, headers=list(full.headers), content=full.body,
                request=request, extensions={"http_version": full.http_version.encode()})

        with httpx.Client(transport=httpx.MockTransport(cached)) as http:
            client = OpenAI(api_key=prepared.api_key, base_url=prepared.api_base,
                            timeout=prepared.timeout_s, max_retries=0, http_client=http)
            try:
                return completion(model=lm.model, messages=messages, drop_params=True,
                                  client=client, **kwargs)
            finally:
                client.close()
                self.assertEqual(len(count), 1)

    def successful(self):
        return dict(id="fixture", object="chat.completion", created=1, model="fixture-model",
            choices=[dict(index=0, message=dict(role="assistant", content="ok"), finish_reason="stop",
                          logprobs=dict(content=[dict(token="ok", logprob=-.1, bytes=[111,107], top_logprobs=[])]))],
            usage=dict(prompt_tokens=8, completion_tokens=2, total_tokens=10,
                       prompt_tokens_details=dict(cached_tokens=2)))

    def outcome(self, call):
        try:
            value = call()
            return value, None
        except Exception as error:
            return None, error

    def test_normal_usage_logprobs_and_optional_fields_match_sdk(self):
        args = self.inputs()
        for omitted in (None, "usage", "logprobs"):
            with self.subTest(omitted=omitted):
                body = self.successful()
                if omitted == "usage":
                    body.pop("usage")
                elif omitted == "logprobs":
                    body["choices"][0].pop("logprobs")
                full = FullModelResponse(200, (), json.dumps(body).encode())
                expected, expected_error = self.outcome(lambda: self.reference(*args, full))
                actual, actual_error = self.outcome(lambda: sdk.parse_lotus_response(*args, full))
                if expected_error is not None:
                    self.assertIs(type(actual_error), type(expected_error))
                else:
                    self.assertIsNone(actual_error)
                    self.assertEqual(actual.choices, expected.choices)
                    self.assertEqual(actual.usage, expected.usage)

    def test_http_bad_json_missing_fields_and_timeout_match_sdk(self):
        args = self.inputs()
        missing = self.successful()
        missing["choices"][0]["message"].pop("content")
        cases = [(FullModelResponse(status, (("x-request-id", "fixture"),),
                    b'{"error":{"message":"fixture","type":"fixture"}}'), "") for status in (401,429,500)]
        cases += [(FullModelResponse(200, (), b'{broken'), ""),
                  (FullModelResponse(200, (), json.dumps(missing).encode()), ""),
                  (None, "TimeoutError: fixture")]
        for full, transport_error in cases:
            with self.subTest(status=None if full is None else full.status_code, transport_error=transport_error):
                expected, expected_error = self.outcome(lambda: self.reference(*args, full, transport_error))
                actual, actual_error = self.outcome(lambda: sdk.parse_lotus_response(*args, full, transport_error))
                if expected_error is None:
                    self.assertIsNone(actual_error)
                    self.assertEqual(actual.choices, expected.choices)
                    continue
                self.assertIs(type(actual_error), type(expected_error))
                self.assertEqual(getattr(actual_error, "status_code", None), getattr(expected_error, "status_code", None))
                self.assertIs(actual_error.full_response, full)
                self.assertEqual(actual_error.native_transport_error, transport_error)


if __name__ == "__main__":
    unittest.main()
