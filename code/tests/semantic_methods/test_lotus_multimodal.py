"""An opt-in pinned image-message profile; no model or serving process is used."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from importlib.util import find_spec
import json
import re
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.semantic_methods.lotus import sdk
from src.semantic_methods.lotus.batch import LotusBatchExecutor, lotus_executor


def image_messages(text="listing"):
    return [{"role": "system", "content": "same system"},
            {"role": "user", "content": [
                {"type": "text", "text": text},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,AA=="}},
                {"type": "text", "text": "same instruction"}]}]


class LotusImageProfileTests(unittest.TestCase):
    def test_default_text_profile_remains_strict_and_opt_in_preserves_parts(self):
        messages = image_messages("你好")
        original = deepcopy(messages)
        with self.assertRaisesRegex(ValueError, "supports text messages"):
            sdk._validate_messages(messages)
        sdk._validate_messages(messages, message_profile=sdk.IMAGE_TEXT_PROFILE)
        self.assertEqual(messages, original)
        sdk._validate_messages([dict(role="user", content="text")],
                               message_profile=sdk.IMAGE_TEXT_PROFILE)

    def test_unsupported_media_and_foreign_uri_do_not_gain_capability(self):
        parts = ({"type": "video", "url": "fixture"}, {"type": "input_audio", "data": "fixture"},
                 {"type": "image_url", "image_url": {"url": "https://example.com/image.png"}},
                 {"type": "image_url", "image_url": {"url": None}},
                 {"type": "image_url", "image_url": {"url": "data:image/png;base64,"}},
                 {"type": "image_url", "image_url": {"url": "data:image/png;base64,AA==", "detail": "low"}},
                 {"type": "text", "text": None}, {"type": "text", "text": "valid", "extra": 1})
        for part in parts:
            with self.subTest(part=part), self.assertRaises(ValueError):
                sdk._validate_messages([dict(role="user", content=[part])],
                                       message_profile=sdk.IMAGE_TEXT_PROFILE)

    def test_structured_system_assistant_empty_and_unknown_profiles_are_rejected(self):
        for role in ("system", "assistant"):
            messages = image_messages()
            messages[-1]["role"] = role
            with self.assertRaises(ValueError):
                sdk._validate_messages(messages, message_profile=sdk.IMAGE_TEXT_PROFILE)
        with self.assertRaises(ValueError):
            sdk._validate_messages([dict(role="user", content=[])], message_profile=sdk.IMAGE_TEXT_PROFILE)
        for profile in (None, "video", "auto"):
            with self.assertRaises(ValueError):
                sdk._validate_messages(image_messages(), message_profile=profile)

    def test_image_batch_validates_each_suffix_through_provider_preparation(self):
        data = [(image_messages(str(i)), None) for i in range(3)]
        prepared = []
        def prepare(lm, messages, kwargs, **options):
            sdk._validate_messages(messages, **options)
            prepared.append(deepcopy(messages))
            return messages
        with patch.object(sdk, "prepare_call", side_effect=prepare):
            first = sdk.validate_batch(None, data, {}, message_profile=sdk.IMAGE_TEXT_PROFILE)
        self.assertIs(first, data[0][0])
        self.assertEqual(prepared, [item[0] for item in data])

    def test_text_batch_keeps_existing_one_provider_preparation_fast_path(self):
        data = [([dict(role="user", content=str(i))], None) for i in range(3)]
        with patch.object(sdk, "prepare_call", return_value="first") as prepare:
            self.assertEqual(sdk.validate_batch(None, data, {}), "first")
        prepare.assert_called_once_with(None, data[0][0], {})

    def test_image_profile_automatically_enables_full_batch_prevalidation(self):
        from src.execution_provider.adapters.model_config import FixedModelConfig
        execution = SimpleNamespace(engine=SimpleNamespace(capacity=SimpleNamespace(limits=
            SimpleNamespace(item_input_bytes=1048576, item_result_bytes=1048576))))
        model = FixedModelConfig("http://localhost/v1/chat/completions", "fixture", 2000, "fixture")
        selected = LotusBatchExecutor(execution, model, query_id="fixture", operator_id="map",
                                      message_profile=sdk.IMAGE_TEXT_PROFILE)
        self.assertTrue(selected.prevalidate_batch)
        default = LotusBatchExecutor(execution, model, query_id="fixture", operator_id="map")
        self.assertFalse(default.prevalidate_batch)


PINNED = all(find_spec(name) is not None for name in ("lotus", "litellm", "openai", "pandas", "PIL"))


@unittest.skipUnless(PINNED, "fixed LOTUS/SDK environment unavailable; no dependencies installed")
class PinnedLotusMultimodalPairTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sdk.validate_source(message_profile=sdk.IMAGE_TEXT_PROFILE)

    def frame(self):
        import pandas as pd
        from PIL import Image
        from lotus.dtype_extensions import ImageArray
        image = Image.new("RGB", (8, 8), "red")
        return pd.DataFrame({"image": ImageArray([image] * 4),
                             "listing_text": ["keep-a", "drop-b", "keep-c", "drop-d"]},
                            index=[10, 20, 30, 40])

    def model(self):
        from lotus.models import LM
        return LM("openai/stage-image-fixture", api_base="http://localhost/v1", api_key="fixture",
                  max_tokens=32, max_batch_size=1, timeout=2, num_retries=0, max_retries=0)

    def response(self, body):
        parts = body["messages"][-1]["content"]
        text = "\n".join(part["text"] for part in parts if part["type"] == "text")
        listing = re.search(r"«([^»]+)»", text).group(1)
        content = "Answer: " + (str(listing.startswith("keep")) if "Claim:" in text else listing)
        return dict(id="stage-fixture", object="chat.completion", created=1, model=body["model"],
                    choices=[dict(index=0, message=dict(role="assistant", content=content),
                                  finish_reason="stop")],
                    usage=dict(prompt_tokens=9, completion_tokens=3, total_tokens=12))

    def query(self, frame, *, chain):
        if chain:
            frame = frame.sem_filter("{image} is consistent with {listing_text}.")
        return frame.sem_map("Describe {listing_text} using {image}.", suffix="_decision",
                             return_raw_outputs=True)

    def pair(self, *, chain=False, status=200):
        import httpx
        import lotus
        from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.execution_provider.adapters.native_tasks import build_native_execution
        native_calls, semloom_calls = [], []
        def raw_response(body):
            value = self.response(body) if status == 200 else {"error": {"message": "controlled", "type": "fixture"}}
            return json.dumps(value).encode()
        def native_send(client, request, **options):
            self.assertEqual(str(request.url), "http://localhost/v1/chat/completions")
            body = json.loads(request.read())
            native_calls.append(body)
            return httpx.Response(status, content=raw_response(body), request=request,
                                  headers={"content-type": "application/json"})
        native_lm = self.model()
        with lotus.settings.context(lm=native_lm, enable_cache=False), patch.object(httpx.Client, "send", native_send):
            try:
                native = self.query(self.frame(), chain=chain)
                native_error = None
            except Exception as error:
                native, native_error = None, error
        async def execute(task, endpoint):
            body = json.loads(task.task.payload)
            semloom_calls.append(body)
            if status == 200 and "keep-a" in json.dumps(body):
                await asyncio.sleep(.02)  # Exercise completion association, not performance.
            return encode_full_response(FullModelResponse(status, (("content-type", "application/json"),), raw_response(body)))
        model_config = FixedModelConfig("http://localhost/v1/chat/completions", "stage-image-fixture", 2000, "fixture")
        execution = build_native_execution(model_config, physical=None, execute=execute,
                                           max_tasks=4, max_active_requests=2)
        selected = LotusBatchExecutor(execution, model_config, query_id="image-pair", operator_id="image-method",
                                      message_profile=sdk.IMAGE_TEXT_PROFILE)
        semloom_lm = self.model()
        try:
            with lotus.settings.context(lm=semloom_lm, enable_cache=False), lotus_executor(semloom_lm, selected):
                try:
                    semloom = self.query(self.frame(), chain=chain)
                    semloom_error = None
                except Exception as error:
                    semloom, semloom_error = None, error
            self.assertEqual(semloom_calls, native_calls)
            self.assertEqual(execution.engine.capacity.usage().held_tasks, 0)
            self.assertEqual(execution.engine.jobs.jobs, {})
            self.assertEqual(selected.last_cleanup_errors, ())
        finally:
            self.assertTrue(execution.close())
        if native_error is not None:
            self.assertIs(type(semloom_error), type(native_error))
            self.assertEqual(getattr(semloom_error, "status_code", None), getattr(native_error, "status_code", None))
        else:
            self.assertIsNone(semloom_error)
            self.assertEqual(semloom.index.tolist(), native.index.tolist())
            self.assertEqual(semloom["listing_text"].tolist(), native["listing_text"].tolist())
            self.assertEqual(semloom["_decision"].tolist(), native["_decision"].tolist())
            self.assertEqual(semloom_lm.stats, native_lm.stats)
        return native_calls, native_error

    def test_native_image_text_map_matches_semloom_messages_parser_usage_and_row_order(self):
        calls, error = self.pair()
        self.assertIsNone(error)
        self.assertEqual(len(calls), 4)
        self.assertTrue(all(len([p for p in call["messages"][-1]["content"] if p["type"] == "image_url"]) == 1 for call in calls))

    def test_native_image_filter_then_map_preserves_survivors_and_required_work(self):
        calls, error = self.pair(chain=True)
        self.assertIsNone(error)
        self.assertEqual(len(calls), 6)

    def test_native_image_sdk_http_error_matches_without_network_or_model(self):
        calls, error = self.pair(status=429)
        self.assertIsNotNone(error)
        self.assertEqual(len(calls), 4)


if __name__ == "__main__":
    unittest.main()
