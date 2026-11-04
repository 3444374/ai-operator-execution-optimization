"""Observe fixed HF processor and loaded-model identities, never request UUIDs.

Install explicitly in the isolated service process before importing its entry point.
The controller must verify real receipts before allowing the first model POST.
Request identities remain in the existing request/gateway traces.
"""

import hashlib
import json
import os
from pathlib import Path
import threading
import time


_lock = threading.Lock()
_seen = set()


def record(identity):
    encoded = json.dumps(identity, sort_keys=True, default=str)
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    with _lock:
        if digest in _seen:
            return
        if len(_seen) >= 256:
            raise RuntimeError("runtime observer distinct-profile cap reached")
        row = dict(identity, pid=os.getpid(), monotonic_ns=time.monotonic_ns(),
                   epoch_ns=time.time_ns(), profile_sha256=digest)
        raw = (json.dumps(row, default=str) + "\n").encode()
        path = Path(os.environ["Q7_PROCESSOR_RECEIPT"])
        if len(raw) > 8192 or (path.exists() and path.stat().st_size > 262144):
            raise RuntimeError("runtime receipt byte cap reached")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            if os.write(fd, raw) != len(raw):
                raise OSError("incomplete runtime receipt write")
        finally:
            os.close(fd)
        _seen.add(digest)


def observe_factory(original):
    def observed(*args, **kwargs):
        processor = original(*args, **kwargs)
        try:
            record(dict(kind="processor", processor_class=type(processor).__name__,
                image_processor_class=type(processor.image_processor).__name__,
                parameters=processor.image_processor.to_dict(),
                model_path=str(args[0]) if args else None,
                use_fast=kwargs.get("use_fast"), revision=kwargs.get("revision")))
        except Exception:
            pass  # A missing receipt must prevent the first POST at the controller.
        return processor
    return observed


def observe_model_init(original):
    def observed(self, *args, **kwargs):
        returned = original(self, *args, **kwargs)
        try:
            model = self.model_config
            record(dict(kind="loaded_service_model_config",
                model_path=model.model, tokenizer=model.tokenizer,
                dtype=str(model.dtype), max_model_len=model.max_model_len,
                architectures=model.hf_config.architectures,
                generation_config=model.generation_config,
                sampling_defaults=model.get_diff_sampling_param(),
                enable_prefix_caching=self.renderer.config.cache_config.enable_prefix_caching,
                mm_processor_cache_gb=model.multimodal_config.mm_processor_cache_gb))
        except Exception:
            pass
        return returned
    return observed


def install():
    from transformers import Qwen2VLProcessor
    from vllm.entrypoints.openai.models.serving import OpenAIServingModels
    Qwen2VLProcessor.from_pretrained = staticmethod(
        observe_factory(Qwen2VLProcessor.from_pretrained))
    OpenAIServingModels.__init__ = observe_model_init(OpenAIServingModels.__init__)
