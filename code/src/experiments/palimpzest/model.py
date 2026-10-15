"""Bind the old PZ vLLM enum to one explicitly verified local served model."""
import base64
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import ipaddress
import json
from urllib.parse import urlsplit

from src.baselines.common.private_artifacts import content_digest
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.semantic_methods.palimpzest.backend import PalimpzestAdapterError


PZ_VLLM_MODEL = 'hosted_vllm/qwen/Qwen1.5-0.5B-Chat'
PZ_VLLM_ENUM = 'VLLM_QWEN_1_5_0_5B_CHAT'
METADATA_IDENTITY_PROFILE = 'vllm-0.25.1-model-list-response-identity.v1'
_MAX_METADATA_BYTES = 1_048_576


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
    stable_metadata_sha256: str
    stable_metadata_profile: str = METADATA_IDENTITY_PROFILE

    def stable_identity(self):
        """Repeated GET identity; full dataclass equality still includes raw metadata."""
        return dict(binding_fingerprint=self.binding_fingerprint, served_model_id=self.served_model_id,
                    stable_metadata_sha256=self.stable_metadata_sha256,
                    stable_metadata_profile=self.stable_metadata_profile)

    def summary(self):
        return dict(upstream_enum=PZ_VLLM_ENUM, upstream_model=PZ_VLLM_MODEL,
                    actual_served_model=self.served_model_id,
                    binding_fingerprint=self.binding_fingerprint,
                    metadata_sha256=self.metadata_sha256,
                    stable_metadata_sha256=self.stable_metadata_sha256,
                    stable_metadata_profile=self.stable_metadata_profile,
                    binding_scope='controlled fixed-version local transport mapping',
                    litellm_provider='hosted_vllm', explicit_client_retries=0, explicit_fallbacks=[],
                    monetary_cost_usd=None, monetary_cost_status='unavailable',
                    upstream_model_card_is_actual_price=False)


def _stable_metadata(metadata):
    """Keep the complete response except three per-GET values in this fixed vLLM profile.

    Sentinels preserve field presence; types, list order/cardinality and every
    other field remain bound. This does not replace the full-response digest.
    """
    result = deepcopy(metadata)
    for card in result['data']:
        if 'created' in card:
            if type(card['created']) is not int or card['created'] < 0:
                raise ValueError('invalid response-generated model timestamp')
            card['created'] = '<response-generated-model-created>'
        if 'permission' not in card:
            continue
        if type(card['permission']) is not list:
            raise ValueError('invalid local model permission list')
        for permission in card['permission']:
            if type(permission) is not dict:
                raise ValueError('invalid local model permission entry')
            if 'id' in permission:
                value = permission['id']
                if type(value) is not str or not value.startswith('modelperm-') or len(value) <= len('modelperm-'):
                    raise ValueError('invalid response-generated model permission id')
                permission['id'] = '<response-generated-permission-id>'
            if 'created' in permission:
                if type(permission['created']) is not int or permission['created'] < 0:
                    raise ValueError('invalid response-generated model permission timestamp')
                permission['created'] = '<response-generated-permission-created>'
    return result


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
    stable = content_digest(dict(profile=METADATA_IDENTITY_PROFILE, metadata=_stable_metadata(metadata)))
    return VerifiedLocalModel(binding.fingerprint, content_digest(metadata), binding.model.model_id, stable)


def fetch_model_metadata(binding, *, evidence=None):
    """One bounded GET; optional fresh evidence retains its original parser input.

    The caller persists each observation, including failures, independently. Body
    bytes use the original HTTPX iter_bytes decoding, before any JSON normalization.
    Request authentication headers are never recorded.
    """
    import httpx
    if evidence is not None and (type(evidence) is not dict or evidence):
        raise ValueError('model metadata observation requires a fresh dictionary')
    observation = evidence if evidence is not None else {}
    observation.update(schema='semloom.palimpzest_model_metadata_observation.v1', status='started',
        endpoint=binding.api_base+'/models', binding_fingerprint=binding.fingerprint,
        requested_served_model=binding.model.model_id, http_status=None, response_headers=None,
        raw_body_representation='original HTTPX decoded response bytes before JSON parsing',
        body_complete=False, body_status='unavailable', raw_body_sha256=None, raw_body_bytes=None,
        raw_body_base64=None, partial_body_sha256=None, partial_body_base64=None,
        retained_body_bytes=0, body_bytes_seen=0, metadata_sha256=None, stable_metadata_sha256=None,
        stable_metadata_profile=METADATA_IDENTITY_PROFILE)
    headers = ({'Authorization': 'Bearer '+binding.model.bearer_token}
               if binding.model.bearer_token else {})
    payload, seen, complete, provider_error = bytearray(), 0, False, None
    try:
        with httpx.Client(timeout=binding.model.timeout_ms / 1000, trust_env=False,
                          follow_redirects=False) as client:
            with client.stream('GET', binding.api_base+'/models', headers=headers) as response:
                observation.update(http_status=response.status_code, response_headers=list(response.headers.multi_items()))
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as error:
                    provider_error = error
                for chunk in response.iter_bytes():
                    seen += len(chunk)
                    remaining = _MAX_METADATA_BYTES-len(payload)
                    payload.extend(chunk[:remaining])
                    if len(chunk) > remaining:
                        raise ValueError('local model metadata exceeds its byte allowance')
                complete = True
                if provider_error is not None:
                    raise provider_error
        metadata = json.loads(payload)
        observation['metadata_sha256'] = content_digest(metadata)
        verified = verify_model_metadata(binding, metadata)
        observation.update(status='verified', stable_metadata_sha256=verified.stable_metadata_sha256)
        return verified
    except BaseException as error:
        failure = provider_error if provider_error is not None else error
        observation.update(status='failed', error_type=type(failure).__name__)
        if failure is not error:
            observation['body_observation_error_type'] = type(error).__name__
            failure.add_note('Metadata body observation/cleanup also failed: '+type(error).__name__)
            raise failure from error
        raise
    finally:
        observation.update(body_complete=complete, body_status='complete' if complete else 'partial' if payload else 'unavailable',
                           retained_body_bytes=len(payload), body_bytes_seen=seen)
        if complete or payload:
            prefix = 'raw_body' if complete else 'partial_body'
            observation[prefix+'_sha256'] = hashlib.sha256(payload).hexdigest()
            observation[prefix+'_base64'] = base64.b64encode(payload).decode('ascii')
            if complete:
                observation['raw_body_bytes'] = len(payload)


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
