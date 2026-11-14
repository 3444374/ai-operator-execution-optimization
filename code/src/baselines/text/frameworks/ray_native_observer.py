"""Finite native API allowance and optional model identity; no success sidecar."""

from functools import wraps
import hashlib
import inspect
import json
import os
from pathlib import Path
import sys
import time


def install(engine_class, budget, emit=None):
    """Charge before delegation and return the original native iterator untouched."""
    original_init, original_generate = engine_class.__init__, engine_class.generate
    signature = inspect.signature(original_generate)

    @wraps(original_init)
    def observed_init(self, *args, **kwargs):
        emit('native_constructor_started')
        original_init(self, *args, **kwargs)
        emit('native_constructor_returned')

    @wraps(original_generate)
    def observed_generate(self, *args, **kwargs):
        arguments = signature.bind(self, *args, **kwargs).arguments
        request_id = arguments['request_id']
        budget.reserve(hashlib.sha256(str(request_id).encode()).hexdigest())
        return original_generate(self, *args, **kwargs)

    if emit is not None:
        engine_class.__init__ = observed_init
    engine_class.generate = observed_generate


def setup():
    """Version-bound Ray actor setup hook, imported before model construction."""
    from vllm.v1.engine.async_llm import AsyncLLM
    from src.experiments.attempt_ledger import AttemptBudget
    from src.experiments.shared_request_budget import SharedClaimedUnit

    spec = json.loads(os.environ['SEMLOOM_RAY_NATIVE_OBSERVER'])
    root = Path(spec['output'])
    unit = SharedClaimedUnit(Path(spec['path']), AttemptBudget(spec['id'], spec['limit']), spec['unit_id'])

    def emit(event, **data):
        if event == 'native_constructor_returned' and spec.get('observe_model_actor_identity'):
            import psutil
            import ray
            context = ray.get_runtime_context()
            data.update(node_id=context.get_node_id(), model_actor_id=context.get_actor_id(),
                        model_actor_create_time=psutil.Process(os.getpid()).create_time())
        value = dict(event=event, monotonic_ns=time.monotonic_ns(), pid=os.getpid(), **data)
        fd = os.open(root / ('native-events-' + str(os.getpid()) + '.jsonl'),
                     os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, (json.dumps(value, default=str) + '\n').encode())
        finally:
            os.close(fd)

    observe_identity = spec.get('observe_model_actor_identity', False)
    install(AsyncLLM, unit, emit if observe_identity else None)
    if observe_identity:
        from importlib.metadata import version
        emit('native_observer_installed', python=sys.executable, prefix=sys.prefix,
             versions={name: version(name) for name in ('ray', 'vllm', 'torch', 'transformers')},
             meaning='model identity only; request completion belongs to the native API')


def summarize(directory):
    """Read optional model identity after the complete query; no request trace."""
    events = [json.loads(line) for path in Path(directory).glob('native-events-*.jsonl')
              for line in path.read_text().splitlines()]
    return dict(schema='semloom.ray_native_model_identity.v1', available=bool(events),
        model_constructor_returned_ns=[row['monotonic_ns'] for row in events
                                      if row['event'] == 'native_constructor_returned'],
        native_versions=[row['versions'] for row in events if row['event'] == 'native_observer_installed'],
        native_interpreters=[dict(python=row['python'], prefix=row['prefix']) for row in events
                             if row['event'] == 'native_observer_installed'],
        engine_accept_ns=None, kernel_seconds=None,
        scope='optional native model identity; no independent per-request terminal, engine acceptance or kernel observation')
