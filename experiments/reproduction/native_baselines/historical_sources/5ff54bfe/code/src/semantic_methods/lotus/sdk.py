"""Use the pinned provider SDK to prepare complete LOTUS calls without submitting them."""

from dataclasses import dataclass, field
from functools import lru_cache
import hashlib
from importlib import import_module
from importlib.metadata import version
import json
from pathlib import Path
import math
from urllib.parse import urlsplit


LOTUS_COMMIT = "b1a85fd7a66fabed8a1585d44d7597d592b4433f"
SOURCE_HASHES = {
    "models/lm.py": "83e843ee8b381757525130ff09b0134fa77d3688a4321a9170e88ce79030e0e3",
    "sem_ops/sem_map.py": "bfa7465c0cb2c0440e9c0cfb9ea3f394afa60b93c593472c2cab55994a456b16",
    "templates/task_instructions.py": "059909938902f3fa44ae1fc9666ce91452266f1e4e8d57c5c81134c24e51a8a3",
    "sem_ops/postprocessors.py": "8f5c33600c849ad9806fcec752ee3d14646cebd5ec441637ff0b271ae6a98956",
    "cache.py": "6341c5e3fc42fdde1359ec9b0a2dcbc3e2902fad53be228c289d8cb2920a477d",
    "settings.py": "1724d0820190e6585fa3504946734eb5964b7b1d8ea727540ac49815f186bf6e",
    "nl_expression.py": "c8864fd77fb0c1afa20453c8227c3ebbd6e124ebe84e083ce393cea9bcc0499e",
    "pricing.py": "bd983188db2c81db95519f68668ac62891fc5970e7b41458be08c29e5279585e",
}
IMAGE_SOURCE_HASHES = {
    "utils.py": "2d8d38af8230f447a354f460e25a8f523f1dc1aa5edebf977a8297ed04458ebe",
    "dtype_extensions/image.py": "f17e136923f4d2d24dd2dc64a80a919b4503a8649276545a65fec22357c5b743",
    "sem_ops/sem_filter.py": "ae20bcb11b8750a9283a988ad2233d2f9a8b755f57a65c47b37029b1e70be7c7",
}
IMAGE_TEXT_PROFILE = "lotus-image-text-v1"
LITELLM_SOURCE_HASHES = {
    "llms/openai/chat/gpt_transformation.py": "5a19feeb4b9957b8d2fb7c941ef0573e4e3b2931ae85db7609f876cff9e07016",
    "utils.py": "8ef5db22a20cd45740788756aabc0e394efe20877311f0eff4ce491c94335d2a",
}
GENERATION_PARAMS = frozenset(
    {
        "temperature",
        "top_p",
        "max_tokens",
        "max_completion_tokens",
        "stop",
        "seed",
        "n",
        "presence_penalty",
        "frequency_penalty",
        "logit_bias",
        "logprobs",
        "top_logprobs",
        "response_format",
        "user",
    }
)
CLIENT_PARAMS = frozenset({"api_base", "api_key", "timeout", "num_retries", "max_retries"})


def check_message_profile(profile):
    if type(profile) is not str or profile not in ("text", IMAGE_TEXT_PROFILE):
        raise ValueError("unsupported LOTUS message profile")
    return profile


@lru_cache(maxsize=4)
def validate_source(*, message_profile="text"):
    """A wheel version alone does not establish the source used by this private seam."""
    check_message_profile(message_profile)
    for package, expected in (("lotus-ai", "1.2.4"), ("litellm", "1.95.0"), ("openai", "2.50.0")):
        if version(package) != expected:
            raise ValueError(f"LOTUS adapter requires {package} {expected}")
    for package, hashes in (("lotus", SOURCE_HASHES), ("litellm", LITELLM_SOURCE_HASHES)):
        root = Path(import_module(package).__file__).parent
        for relative, digest in hashes.items():
            if hashlib.sha256((root / relative).read_bytes()).hexdigest() != digest:
                raise ValueError(f"LOTUS adapter source differs at {package}/{relative}")
    if message_profile == IMAGE_TEXT_PROFILE:
        root = Path(import_module("lotus").__file__).parent
        for relative, digest in IMAGE_SOURCE_HASHES.items():
            if hashlib.sha256((root / relative).read_bytes()).hexdigest() != digest:
                raise ValueError(f"LOTUS image adapter source differs at lotus/{relative}")


@dataclass(frozen=True)
class PreparedLotusCall:
    payload: bytes
    api_base: str
    api_key: str = field(repr=False)
    timeout_s: float
    message_profile: str = "text"


def _validate_utf8(value):
    # Native OpenAI serialization rejects Python surrogate code points before I/O.
    # Validate before the JSON copy can combine a surrogate pair into a scalar.
    if not value.isascii():
        value.encode("utf-8")


def _validate_messages(messages, *, message_profile="text"):
    check_message_profile(message_profile)
    if not isinstance(messages, list) or not messages:
        raise ValueError("LOTUS adapter requires a complete message list")
    for message in messages:
        if (
            type(message) is not dict
            or set(message) != {"role", "content"}
            or message["role"] not in ("system", "user", "assistant")
        ):
            raise ValueError("LOTUS adapter supports text messages with role and content")
        content = message["content"]
        if type(content) is str:
            _validate_utf8(content)
            continue
        if message_profile == "text":
            raise ValueError("LOTUS adapter supports text messages with role and content")
        if message["role"] != "user" or type(content) is not list or not content:
            raise ValueError("LOTUS image content must be a non-empty user content list")
        for part in content:
            if type(part) is not dict:
                raise ValueError("LOTUS image content parts must be dictionaries")
            if part.get("type") == "text":
                if set(part) != {"type", "text"} or type(part["text"]) is not str:
                    raise ValueError("invalid LOTUS image text part")
                _validate_utf8(part["text"])
            elif part.get("type") == "image_url":
                if (
                    set(part) != {"type", "image_url"}
                    or type(part["image_url"]) is not dict
                    or set(part["image_url"]) != {"url"}
                    or type(part["image_url"]["url"]) is not str
                    or not part["image_url"]["url"].startswith("data:image/png;base64,")
                    or not part["image_url"]["url"][len("data:image/png;base64,") :]
                ):
                    raise ValueError("LOTUS image profile requires the native PNG data URI")
                _validate_utf8(part["image_url"]["url"])
            else:
                raise ValueError("unsupported LOTUS image content part")


def validate_batch(
    lm,
    uncached_data,
    all_kwargs,
    *,
    check_stop=None,
    message_profile="text",
    model_config=None,
    call_limits=None,
):
    """Reject an invalid suffix before I/O and retain only its first prepared call.

    Without capacity/configuration guards, text keeps its existing light preflight:
    common parameters use the provider once and suffixes check structure/UTF-8.
    Image messages and explicitly guarded calls use the provider for every row;
    guards check complete request bytes and their sum without caching those bodies.
    Execution still prepares suffix calls in finite blocks.
    """
    check_message_profile(message_profile)
    if not uncached_data:
        return None
    profile_kwargs = {} if message_profile == "text" else {"message_profile": message_profile}
    prepare_suffix = (
        message_profile == IMAGE_TEXT_PROFILE or model_config is not None or call_limits is not None
    )
    first, input_bytes = None, 0
    for index, item in enumerate(uncached_data):
        if check_stop:
            check_stop()
        messages = item[0]
        if index == 0 or prepare_suffix:
            call = prepare_call(lm, messages, all_kwargs, **profile_kwargs)
            if check_stop:
                check_stop()
            if model_config is not None:
                check_model_config(call, model_config)
            if check_stop:
                check_stop()
            if call_limits is not None:
                call_limits.check_prepared(call)
                input_bytes += len(call.payload)
                if input_bytes > call_limits.max_batch_input_bytes:
                    raise ValueError(
                        "LOTUS prepared batch input exceeds the declared byte capacity"
                    )
            if index == 0:
                first = call
            del call
        else:
            _validate_messages(messages)
            if any(type(message["role"]) is not str for message in messages):
                # Preserve the existing JSON rules for unusual equality-compatible roles.
                json.dumps(
                    json.loads(json.dumps(messages)), ensure_ascii=False, allow_nan=False
                ).encode()
        if check_stop:
            check_stop()
    if check_stop:
        check_stop()
    return first


def prepare_call(lm, messages, all_kwargs, *, message_profile="text") -> PreparedLotusCall:
    """Mirror the ordinary OpenAI provider's nonstreaming transformation, before I/O."""
    check_message_profile(message_profile)
    if message_profile == "text":
        validate_source()
    else:
        validate_source(message_profile=message_profile)
    from litellm import OpenAIConfig
    from litellm.utils import get_optional_params
    from litellm.utils import ProviderConfigManager
    from litellm.types.utils import LlmProviders

    if not lm.model.startswith("openai/") or not lm.model[len("openai/") :]:
        raise ValueError("LOTUS adapter requires one explicit openai/ model")
    model = lm.model[len("openai/") :]
    kwargs = dict(all_kwargs)
    unsupported = set(kwargs) - GENERATION_PARAMS - CLIENT_PARAMS
    if unsupported:
        raise ValueError("unsupported LOTUS call parameters: " + ", ".join(sorted(unsupported)))
    if kwargs.get("n", 1) != 1:
        raise ValueError("LOTUS adapter requires one completion choice")
    if kwargs.get("num_retries", 0) != 0 or kwargs.get("max_retries", 0) != 0:
        raise ValueError("LOTUS paired execution requires explicit zero retries")
    if OpenAIConfig.get_config():
        raise ValueError("global LiteLLM OpenAI configuration must be empty")
    _validate_messages(messages, message_profile=message_profile)
    api_base = kwargs.get("api_base")
    if type(api_base) is not str:
        raise ValueError("LOTUS adapter requires an explicit API base")
    url = urlsplit(api_base)
    if (
        url.scheme not in ("http", "https")
        or not url.hostname
        or url.username
        or url.password
        or url.query
        or url.fragment
    ):
        raise ValueError("invalid LOTUS API base")
    api_key = kwargs.get("api_key")
    if type(api_key) is not str or not api_key:
        raise ValueError("LOTUS adapter requires an explicit API key")
    timeout = kwargs.get("timeout")
    if type(timeout) not in (float, int) or not math.isfinite(timeout) or not 0 < timeout <= 120:
        raise ValueError("LOTUS adapter requires a finite timeout of at most 120 seconds")
    generation = {k: v for k, v in kwargs.items() if k in GENERATION_PARAMS}
    if "response_format" in generation and (
        type(generation["response_format"]) is not dict
        or generation["response_format"] != {"type": "text"}
    ):
        raise ValueError("structured Map responses require a separate method adapter")
    config = ProviderConfigManager.get_provider_chat_config(
        model=model, provider=LlmProviders.OPENAI
    )
    if type(config).__name__ != "OpenAIGPTConfig":
        raise ValueError("this model has a different LiteLLM transformation")
    optional = get_optional_params(
        model=model, messages=messages, custom_llm_provider="openai", drop_params=True, **generation
    )
    # The OpenAIChatCompletion nonstreaming path removes these before transform_request.
    optional.pop("stream", None)
    optional.pop("stream_options", None)
    body = config.transform_request(
        model=model,
        messages=json.loads(json.dumps(messages)),
        optional_params=optional,
        litellm_params={"custom_llm_provider": "openai", "api_base": api_base},
        headers={},
    )
    # OpenAI's create SDK merges extra_body into the JSON object, including when empty.
    extra = body.pop("extra_body", {})
    if extra:
        raise ValueError("provider-specific extra body requires a separate adapter")
    return PreparedLotusCall(
        json.dumps(body, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode(),
        api_base.rstrip("/"),
        api_key,
        float(timeout),
        message_profile,
    )


def response_model(body: bytes):
    """Keep usage, logprobs and provider fields in the original SDK response type."""
    from litellm.types.utils import ModelResponse
    from litellm.utils import convert_to_model_response_object

    data = json.loads(body)
    if type(data) is not dict or "error" in data:
        raise ValueError("LOTUS completion is not a successful response object")
    return convert_to_model_response_object(
        response_object=data,
        model_response_object=ModelResponse(),
        response_type="completion",
        hidden_params={"custom_llm_provider": "openai"},
    )


def decode_lotus_response(payload):
    from ...execution_provider.adapters.full_response import decode_full_response

    full = decode_full_response(payload)
    return full, lotus_response(full)


def lotus_response(full):
    import httpx
    from openai import OpenAIError

    if not 200 <= full.status_code < 300:
        error = OpenAIError(f"LOTUS upstream returned HTTP {full.status_code}")
        error.full_response = full
        raise error
    try:
        # The original OpenAI SDK lets HTTPX decode Content-Encoding before parsing.
        # The complete response keeps its original bytes and duplicate headers.
        decoded = httpx.Response(
            full.status_code, headers=list(full.headers), content=full.body
        ).content
        return response_model(decoded)
    except Exception as cause:
        error = ValueError("LOTUS upstream response could not be parsed")
        error.full_response = full
        raise error from cause


def check_model_config(call, model_config):
    if (
        call.api_base + "/chat/completions" != model_config.endpoint_url
        or call.api_key != model_config.bearer_token
        or call.timeout_s * 1000 != model_config.timeout_ms
        or json.loads(call.payload)["model"] != model_config.model_id
    ):
        raise ValueError("LOTUS call differs from the selected execution model configuration")


class LotusResponseReplay:
    """One lazy SDK client for a batch's network-free response interpretation."""

    def __init__(self):
        self._http = self._client = self._active = self._configuration = None
        self._sent = 0

    def _send(self, request):
        import httpx

        if self._active is None:
            raise ValueError("LOTUS SDK parser has no active response")
        prepared, full, transport_error = self._active
        if (
            request.method != "POST"
            or str(request.url) != prepared.api_base + "/chat/completions"
            or json.loads(request.read()) != json.loads(prepared.payload)
            or request.headers.get("authorization") != "Bearer " + prepared.api_key
        ):
            raise ValueError("LOTUS SDK parser request differs from its prepared complete call")
        self._sent += 1
        if self._sent != 1:
            raise ValueError("LOTUS SDK parser attempted to retry its response")
        if transport_error:
            error_type = transport_error.partition(":")[0]
            if error_type in (
                "TimeoutError",
                "ConnectionTimeoutError",
                "SocketTimeoutError",
                "ServerTimeoutError",
            ):
                raise httpx.ReadTimeout(transport_error, request=request)
            raise httpx.ConnectError(transport_error, request=request)
        return httpx.Response(
            full.status_code,
            headers=list(full.headers),
            content=full.body,
            request=request,
            extensions={"http_version": full.http_version.encode()},
        )

    def parse(self, lm, messages, all_kwargs, prepared, full, transport_error=""):
        if prepared.message_profile == "text":
            validate_source()
        else:
            validate_source(message_profile=prepared.message_profile)
        import httpx
        from openai import OpenAI
        from litellm import completion

        configuration = (prepared.api_base, prepared.api_key, prepared.timeout_s)
        if self._active is not None:
            raise ValueError("LOTUS SDK parser is already interpreting a response")
        if self._configuration is not None and configuration != self._configuration:
            raise ValueError("LOTUS SDK parser client configuration changed within its batch")
        if self._client is None:
            owner = self

            class ResponseClient(httpx.Client):
                def send(self, request, *args, **kwargs):
                    return owner._send(request)

            self._http = ResponseClient(transport=httpx.MockTransport(self._send))
            self._client = OpenAI(
                api_key=prepared.api_key,
                base_url=prepared.api_base,
                timeout=prepared.timeout_s,
                max_retries=0,
                http_client=self._http,
            )
            self._configuration = configuration
        self._active = (prepared, full, transport_error)
        self._sent = 0
        try:
            return completion(
                model=lm.model,
                messages=messages,
                drop_params=True,
                client=self._client,
                **all_kwargs,
            )
        except Exception as error:
            error.full_response = full
            error.native_transport_error = transport_error
            raise
        finally:
            self._active = None

    def close(self):
        primary = None
        try:
            if self._client is not None:
                self._client.close()
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                if self._http is not None:
                    self._http.close()
            except BaseException as error:
                if primary is None:
                    raise
                primary.add_note(
                    "LOTUS response HTTP client close also failed: " + type(error).__name__
                )
            finally:
                self._http = self._client = self._active = self._configuration = None


def parse_lotus_response(
    lm, messages, all_kwargs, prepared, full, transport_error="", *, replay=None
):
    """Use the pinned SDK without HTTP; callers may own one replay per batch."""
    if replay is not None:
        return replay.parse(lm, messages, all_kwargs, prepared, full, transport_error)
    owned = LotusResponseReplay()
    primary = None
    try:
        return owned.parse(lm, messages, all_kwargs, prepared, full, transport_error)
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            owned.close()
        except BaseException as error:
            if primary is None:
                raise
            primary.add_note("LOTUS SDK client close also failed: " + type(error).__name__)
