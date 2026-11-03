"""Bind a native context observation to its requested spec and actual engine."""

from dataclasses import asdict
import hashlib
import json


def _spec_signature(spec):
    value = json.dumps(asdict(spec), sort_keys=True, ensure_ascii=False,
                       allow_nan=False, separators=(',', ':')).encode()
    return hashlib.sha256(value).hexdigest()


def model_context_record(spec, engine_uid, max_model_len):
    if type(engine_uid) is not str or not engine_uid:
        raise ValueError('model context requires an actual engine identity')
    if type(max_model_len) is not int or max_model_len < 1:
        raise ValueError('native model context is unavailable or invalid')
    if spec.max_model_len is not None and max_model_len != spec.max_model_len:
        raise ValueError('native model context differs from explicit max_model_len')
    record = dict(schema='semloom.model_context.v1', engine_uid=engine_uid,
        source='vllm.ModelConfig.max_model_len', requested_max_model_len=spec.max_model_len,
        max_model_len=max_model_len, engine_signature=_spec_signature(spec))
    encoded = json.dumps(record, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    record['observation_signature'] = hashlib.sha256(encoded).hexdigest()
    return record


def checked_model_context(spec, ready):
    if type(ready) is not dict or type(ready.get('model_context')) is not dict:
        raise RuntimeError('effective model context requires the model ready receipt')
    observed = ready['model_context']
    expected = model_context_record(spec, ready.get('engine_uid'), observed.get('max_model_len'))
    if observed != expected:
        raise ValueError('model context changed engine, requested spec or observation identity')
    return dict(observed)


def model_context_limit(spec, ready=None):
    if type(ready) is dict and 'model_context' in ready:
        return checked_model_context(spec, ready)['max_model_len']
    if spec.max_model_len is not None:
        return spec.max_model_len  # Explicit finite checks can also run before startup.
    raise RuntimeError('effective model context is unavailable before model readiness')


def model_context_identity(spec, ready):
    observed = checked_model_context(spec, ready)
    return {name: value for name, value in observed.items() if name not in ('engine_uid', 'observation_signature')}
