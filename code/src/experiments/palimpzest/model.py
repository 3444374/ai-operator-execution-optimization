"""Bind the old PZ vLLM enum to one explicitly verified local served model."""
from dataclasses import dataclass
import ipaddress
from urllib.parse import urlsplit

from src.baselines.common.private_artifacts import content_digest
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.semantic_methods.palimpzest.backend import PalimpzestAdapterError


PZ_VLLM_MODEL = 'hosted_vllm/qwen/Qwen1.5-0.5B-Chat'
PZ_VLLM_ENUM = 'VLLM_QWEN_1_5_0_5B_CHAT'


@dataclass(frozen=True)
class LocalModelBinding:
    model: FixedModelConfig

    def __post_init__(self):
        if type(self.model) is not FixedModelConfig or self.model.choice_format is not None:
            raise ValueError('PZ requires an explicit ordinary local chat model configuration')
        parsed = urlsplit(self.model.endpoint_url)
        try:
            loopback = ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            loopback = parsed.hostname == 'localhost'
        if (not loopback or parsed.scheme != 'http' or parsed.query
                or parsed.path != '/v1/chat/completions'):
            raise ValueError('PZ local binding requires a loopback /v1/chat/completions endpoint')
        if self.model.model_id.startswith(('hosted_vllm/', 'openai/', 'gemini/')):
            raise ValueError('served model id must be the literal local metadata id, without a provider')

    @property
    def api_base(self):
        return self.model.endpoint_url.removesuffix('/chat/completions')

    @property
    def fingerprint(self):
        return content_digest(dict(endpoint=self.model.endpoint_url, model=self.model.model_id,
                                   timeout_ms=self.model.timeout_ms, enum=PZ_VLLM_MODEL))


@dataclass(frozen=True)
class VerifiedLocalModel:
    binding_fingerprint: str
    metadata_sha256: str
    served_model_id: str

    def summary(self):
        return dict(upstream_enum=PZ_VLLM_ENUM, upstream_model=PZ_VLLM_MODEL,
                    actual_served_model=self.served_model_id,
                    binding_fingerprint=self.binding_fingerprint,
                    metadata_sha256=self.metadata_sha256,
                    binding_scope='controlled fixed-version local transport mapping',
                    litellm_provider='hosted_vllm', explicit_client_retries=0, explicit_fallbacks=[],
                    monetary_cost_usd=None, monetary_cost_status='unavailable',
                    upstream_model_card_is_actual_price=False)


def verify_model_metadata(binding, metadata):
    """No partial names, default model, or remote provider fallback is accepted."""
    if (type(metadata) is not dict or metadata.get('object') != 'list'
            or type(metadata.get('data')) is not list or not metadata['data']):
        raise ValueError('local /v1/models metadata is missing its model list')
    entries = metadata['data']
    if (any(type(item) is not dict or type(item.get('id')) is not str for item in entries)
            or len({item['id'] for item in entries}) != len(entries)):
        raise ValueError('local model metadata has invalid or duplicate model identities')
    selected = [item for item in entries if item['id'] == binding.model.model_id]
    if (len(selected) != 1 or selected[0].get('object') != 'model'
            or selected[0].get('owned_by') != 'vllm'):
        raise ValueError('configured served model is absent from exact local vLLM metadata')
    return VerifiedLocalModel(binding.fingerprint, content_digest(metadata), binding.model.model_id)


def fetch_model_metadata(binding):
    """A bounded read-only SDK request; model requests still use original LiteLLM."""
    import httpx
    headers = ({'Authorization': 'Bearer '+binding.model.bearer_token}
               if binding.model.bearer_token else {})
    with httpx.Client(timeout=binding.model.timeout_ms / 1000, trust_env=False,
                      follow_redirects=False) as client:
        with client.stream('GET', binding.api_base+'/models', headers=headers) as response:
            response.raise_for_status()
            payload = bytearray()
            for chunk in response.iter_bytes():
                payload.extend(chunk)
                if len(payload) > 1_048_576:
                    raise ValueError('local model metadata exceeds its byte allowance')
    import json
    return verify_model_metadata(binding, json.loads(payload))


class LocalCompletion:
    """The bridge owns patching; this callable only guards and translates kwargs."""
    def __init__(self, original, binding, verified):
        if (not callable(original) or type(binding) is not LocalModelBinding
                or type(verified) is not VerifiedLocalModel
                or verified.binding_fingerprint != binding.fingerprint
                or verified.served_model_id != binding.model.model_id):
            raise ValueError('completion requires a matching verified local model binding')
        self.original, self.binding, self.verified = original, binding, verified

    def validate(self, kwargs):
        if (type(kwargs) is not dict or set(kwargs)-{'model', 'api_base', 'messages', 'temperature'}
                or kwargs.get('model') != PZ_VLLM_MODEL
                or kwargs.get('api_base') != self.binding.api_base
                or type(kwargs.get('messages')) is not list or not kwargs['messages']
                or type(kwargs.get('temperature')) not in (int, float)
                or kwargs['temperature'] != 0):
            raise ValueError('completion differs from the pinned PZ local request profile')

    def request_kwargs(self, kwargs):
        """Expose the identical guarded SDK request to the pending-call adapter."""
        self.validate(kwargs)
        return dict(kwargs, model='hosted_vllm/'+self.binding.model.model_id,
                    api_key=self.binding.model.bearer_token or 'EMPTY',
                    timeout=self.binding.model.timeout_ms / 1000, num_retries=0, fallbacks=[])

    def validate_response(self, response):
        # Do not hide server identity changes behind a locally accepted model name.
        response_model = response.get('model') if isinstance(response, dict) else getattr(response, 'model', None)
        if response_model != self.binding.model.model_id:
            raise PalimpzestAdapterError('completion response model differs from the verified served model')
        return response

    def __call__(self, **kwargs):
        return self.validate_response(self.original(**self.request_kwargs(kwargs)))


def bind_local_completion(original, binding, verified):
    return LocalCompletion(original, binding, verified)
