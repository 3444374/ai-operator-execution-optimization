"""Query-private original PZ/HostedVLLM stacks, suspended at the SDK send boundary."""

from copy import copy
from dataclasses import dataclass, field
import hashlib
import ipaddress
from importlib import import_module
from importlib.metadata import version
import json
from pathlib import Path
from types import CodeType, FunctionType, SimpleNamespace
from urllib.parse import quote, urlsplit

from src.baselines.text.frameworks.native_calls import NativeCall, outcome_kind
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from .backend import PalimpzestAdapterError
from .source import validate_source


SDK_HASHES = {
    "main.py": "31e4143cc66b62eb4fdede5f4e33e0b31c661c8e7e2e88a79ee511ce196fbf8c",
    "utils.py": "8ef5db22a20cd45740788756aabc0e394efe20877311f0eff4ce491c94335d2a",
    "types/utils.py": "77d6aef4e97eda0118e8f1d20b91843085b22cc54149eb90435e53d8f0afa41c",
    "llms/hosted_vllm/chat/transformation.py": "844516abd2c51b32fc9d8482320508a2f87a913d7239cee2d757139f2627651d",
    "llms/openai/chat/gpt_transformation.py": "5a19feeb4b9957b8d2fb7c941ef0573e4e3b2931ae85db7609f876cff9e07016",
    "llms/custom_httpx/http_handler.py": "59cd7749ef6b2311433f42a03ee221cdf49a8db2ae2532844dddcca21f4e9e6e",
    "llms/custom_httpx/llm_http_handler.py": "804ff65e3256d78bbaa47287c0180f88173ba3ba5b43c90529f35ec1dff518d6",
    "litellm_core_utils/llm_response_utils/convert_dict_to_response.py": "ffff0d28a7fe9d0d4363f3610b27b5e1f6a639882eb5b6a9a368265ed1d0b20f",
}
PZ_EXECUTION_HASHES = {
    "query/execution/execution_strategy.py": "2b00496d638138e3c1896466df2a4770042259b0acbed5ba527554e2d1e4633f",
    "query/operators/convert.py": "9a95d761d81c1faccfab2a738774a34ea1e3abb3c311b91f759d7f5c49a5fa7c",
    "query/operators/project.py": "457e526c7151c0770c7e2411a1f20f42bf801e0203626ebc89f8856403eeec5a",
    "query/operators/scan.py": "0dafc4654ac0af39406a67876029adaa8fb16cf385bdb4d84a4b6b51c5045127",
    "query/operators/aggregate.py": "020a9ed4f4ff84e60f67ded1a833611109fe935b621a3b8495866d13f6e6c07a",
    "core/elements/records.py": "4aee1df0644da79f90d6749600d9be8de872c39ded1d4976d221a5bb2124f0d2",
    "query/operators/physical.py": "6ee3d98ed3f81ca3557ce08d59d06d0e7452db5edd4ac206ef882a6758e6a32a",
    "core/data/iter_dataset.py": "bbb799356593f38ac7fc3f0b348baad0d0279b6ae85fcd627ca4d8a7e87063b8",
}


@dataclass(frozen=True)
class PreparedRequestRoute:
    """One declared gateway registration; canonical model metadata remains direct."""
    route: GatewayRoute
    effective_endpoint: str
    _registered: bool = field(default=False, init=False, repr=False)

    def __post_init__(self):
        if type(self.route) is not GatewayRoute:
            raise TypeError("PZ effective request route requires an explicit GatewayRoute")
        endpoint = urlsplit(self.effective_endpoint)
        try:
            loopback = ipaddress.ip_address(endpoint.hostname).is_loopback
        except ValueError:
            loopback = endpoint.hostname == "localhost"
        path = "/observe/" + quote(self.route.job_id, safe="") + "/" + quote(self.route.endpoint_id, safe="") + "/v1/chat/completions"
        if (not loopback or endpoint.scheme != "http" or endpoint.path != path
                or endpoint.query or endpoint.fragment or endpoint.username or endpoint.password):
            raise ValueError("PZ effective endpoint differs from its declared loopback gateway registration")

    @classmethod
    def from_gateway(cls, gateway, route):
        if type(gateway) is not ObservationGateway or type(route) is not GatewayRoute:
            raise TypeError("PZ gateway registration requires the existing ObservationGateway")
        if (gateway._routes.get((route.job_id, route.endpoint_id)) != route
                or gateway._preserve_http is not True or gateway._select_upstream is not None):
            raise ValueError("PZ gateway must preserve HTTP on the exact registered canonical route")
        value = cls(route, gateway.endpoint_url(route.job_id, route.endpoint_id))
        object.__setattr__(value, "_registered", True)
        return value

    def describe(self):
        return dict(canonical_endpoint=self.route.upstream_url, effective_endpoint=self.effective_endpoint,
                    gateway_job_id=self.route.job_id, gateway_endpoint_id=self.route.endpoint_id)


def clone_function(function, **bindings):
    """Bind dependencies privately while retaining the exact original code object."""
    def codes(code):
        yield code
        for constant in code.co_consts:
            if isinstance(constant, CodeType):
                yield from codes(constant)
    try:
        compiled = compile(Path(function.__code__.co_filename).read_bytes(),
                           function.__code__.co_filename, "exec", dont_inherit=True)
        expected = next(code for code in codes(compiled) if code.co_qualname == function.__qualname__)
    except (OSError, StopIteration) as error:
        raise PalimpzestAdapterError("PZ private function no longer matches its qualified source") from error
    if expected != function.__code__:
        raise PalimpzestAdapterError("PZ private function code differs from its qualified source")
    cloned = FunctionType(function.__code__, dict(function.__globals__, **bindings),
                          function.__name__, function.__defaults__, function.__closure__)
    cloned.__kwdefaults__ = function.__kwdefaults__
    return cloned


def validate_prepared_runtime():
    validate_source()
    for name, expected in (("greenlet", "3.5.6"), ("litellm", "1.95.0"),
                           ("openai", "2.50.0"), ("httpx", "0.28.1")):
        if version(name) != expected:
            raise PalimpzestAdapterError(f"PZ prepared execution requires {name} {expected}")
    for module, hashes in (("palimpzest", PZ_EXECUTION_HASHES), ("litellm", SDK_HASHES)):
        root = Path(import_module(module).__file__).resolve().parent
        for relative, expected in hashes.items():
            if hashlib.sha256((root / relative).read_bytes()).hexdigest() != expected:
                raise PalimpzestAdapterError("PZ prepared runtime source differs at " + relative)
    if import_module("litellm").cache is not None:
        raise PalimpzestAdapterError("PZ prepared execution requires explicitly disabled LiteLLM cache")


@dataclass
class GenerationAttempt:
    call_id: str
    row_id: int
    source_indices: tuple
    observed: bool = False
    prepared: bool = False
    contract_error: BaseException | None = None


class PreparedSDK:
    """No shared patches and no network transport; the original SDK prepares/finishes."""

    def __init__(self, owner, model_config, original_completion, request_kwargs, validate_response, *, request_route=None):
        import httpx
        from litellm.llms.custom_httpx.http_handler import HTTPHandler, get_default_headers
        self.owner, self.model_config = owner, model_config
        self.endpoint = model_config.endpoint_url if request_route is None else request_route.effective_endpoint
        self.original_completion = original_completion
        self.request_kwargs, self.validate_response = request_kwargs, validate_response
        sdk = self

        class CaptureClient(httpx.Client):
            def send(self, request, *args, **kwargs):
                if self.follow_redirects is not False or args or kwargs.get("stream", False) or set(kwargs) - {"stream"}:
                    sdk.contract_error("PZ prepared execution requires a complete nonstream SDK send")
                # The native-adapted control alone uses this original HTTPX sender.
                sender = super().send
                return sdk.send(request, sender)

        self.client = CaptureClient(trust_env=False, headers=get_default_headers(),
                                    follow_redirects=False, timeout=model_config.timeout_ms / 1000)
        self.handler = HTTPHandler(client=self.client)

    def contract_error(self, message, cause=None):
        error = cause if isinstance(cause, PalimpzestAdapterError) else PalimpzestAdapterError(message)
        attempt = self.owner.current_attempt.get()
        if attempt is not None:
            attempt.contract_error = error
        self.owner.fail(error)
        if error is cause:
            raise error
        raise error from cause

    def observe(self, method, attempt, **kwargs):
        observer = self.owner.observer
        if observer is not None:
            try:
                getattr(observer, method)(attempt.call_id, **kwargs)
            except BaseException as error:
                self.contract_error("PZ prepared observer failed at " + method, error)

    def generator(self, original):
        from palimpzest.query.generators.generators import Generator
        if type(original) is not Generator:
            raise PalimpzestAdapterError("PZ prepared execution requires the original Generator")
        sdk = self
        raw_call = clone_function(Generator.__call__, litellm=SimpleNamespace(completion=self.completion))

        def call(generator, candidate, fields, *args, **kwargs):
            attempt = sdk.owner.begin_generation(candidate, fields)
            token = sdk.owner.current_attempt.set(attempt)
            result, error = None, None
            try:
                result = raw_call(generator, candidate, fields, *args, **kwargs)
                if attempt.contract_error is not None:
                    raise attempt.contract_error
                return result
            except BaseException as failure:
                error = attempt.contract_error or failure
                raise error
            finally:
                try:
                    if attempt.observed:
                        sdk.observe("generator_finished", attempt, result=result, error=error,
                                    row_id=attempt.source_indices)
                finally:
                    sdk.owner.current_attempt.reset(token)

        def parse(generator, *args, **kwargs):
            attempt = sdk.owner.current_attempt.get()
            try:
                answers = Generator._parse_answer(generator, *args, **kwargs)
            except Exception as error:
                sdk.observe("parsed", attempt, error=error, parser_default=True)
                raise
            sdk.observe("parsed", attempt, field_answers=answers)
            return answers

        private_class = type("QueryPrivateGenerator", (Generator,), {"__call__": call, "_parse_answer": parse})
        private = object.__new__(private_class)
        private.__dict__.update(copy(vars(original)))
        private.prompt_factory = copy(original.prompt_factory)
        return private

    def completion(self, **kwargs):
        attempt = self.owner.current_attempt.get()
        if attempt is None:
            self.contract_error("PZ SDK completion escaped its owned original operator stack")
        try:
            self.owner.check_open()
            values = self.request_kwargs(kwargs)
        except Exception as error:
            self.contract_error("PZ SDK request profile failed validation", error)
        model = self.model_config
        if (values.get("model") != "hosted_vllm/" + model.model_id
                or values.get("api_base") != self.endpoint.removesuffix("/chat/completions")
                or values.get("api_key") != (model.bearer_token or "EMPTY")
                or values.get("timeout") != model.timeout_ms / 1000
                or values.get("num_retries") != 0 or values.get("fallbacks") != []
                or set(values) - {"model", "api_base", "api_key", "messages", "temperature",
                                  "timeout", "num_retries", "fallbacks"}):
            self.contract_error("PZ SDK request differs from the selected fixed local profile")
        self.observe("started", attempt, kwargs=kwargs, row_id=attempt.source_indices)
        attempt.observed = True
        response, error = None, None
        try:
            response = self.original_completion(**dict(values, client=self.handler))
            if not attempt.prepared:
                self.contract_error("PZ completion returned without exactly one prepared native HTTP call")
            response = self.validate_response(response)
            return response
        except BaseException as failure:
            error = attempt.contract_error or failure
            if isinstance(error, PalimpzestAdapterError):
                attempt.contract_error = error
                self.owner.fail(error)
            raise error
        finally:
            self.observe("finished", attempt, response=response, error=error)

    def send(self, request, native_sender):
        import httpx
        attempt = self.owner.current_attempt.get()
        try:
            self.owner.check_open()
            body = request.read()
            model = self.model_config
            if (attempt is None or attempt.prepared or request.method != "POST"
                    or str(request.url) != self.endpoint
                    or request.headers.get("authorization") != "Bearer " + (model.bearer_token or "EMPTY")
                    or len(body) > self.owner.limits.max_request_bytes
                    or json.loads(body).get("model") != model.model_id):
                self.contract_error("PZ prepared Request changed identity, model, endpoint or byte allowance")
            attempt.prepared = True
            call = NativeCall(attempt.call_id, attempt.row_id, body, str(request.url),
                              tuple(request.headers.multi_items()), model.timeout_ms / 1000,
                              max_response_bytes=self.owner.limits.max_response_bytes,
                              follow_redirects=self.client.follow_redirects)
            outcome = self.owner.prepared(call, request, native_sender)
        except (ValueError, PalimpzestAdapterError) as error:
            self.contract_error("PZ prepared Request failed its complete-call contract", error)
        if outcome_kind(outcome) == "execution_error":
            self.contract_error("PZ backend reported a prepared-call execution failure")
        if outcome.transport_error:
            kind = outcome.transport_error.partition(":")[0]
            error = httpx.ReadTimeout if kind in (
                "TimeoutError", "ServerTimeoutError", "SocketTimeoutError", "ConnectionTimeoutError") else httpx.ConnectError
            raise error(outcome.transport_error, request=request)
        full = outcome.response
        return httpx.Response(full.status_code, headers=list(full.headers), content=full.body,
                              request=request, extensions={"http_version": full.http_version.encode()})

    def close(self):
        self.handler.close()
