"""Finite native Ray request accounting; no submission or scheduling policy."""

from functools import wraps
import hashlib
import inspect
import json
import os
from pathlib import Path
import sys
import time


def install(engine_class, budget, emit, *, capture_outputs=False):
    """Observe the existing AsyncLLM boundary and charge before each generate."""
    original_init, original_generate = engine_class.__init__, engine_class.generate
    signature = inspect.signature(original_generate)

    @wraps(original_init)
    def observed_init(self, *args, **kwargs):
        emit('native_constructor_started')
        original_init(self, *args, **kwargs)
        emit('native_constructor_returned')

    @wraps(original_generate)
    async def observed_generate(self, *args, **kwargs):
        arguments = signature.bind(self, *args, **kwargs).arguments
        request_id = arguments['request_id']
        attempt = budget.reserve(hashlib.sha256(str(request_id).encode()).hexdigest())
        params = arguments.get('sampling_params')
        emit('native_generate_submitted', attempt=attempt, request_id=request_id,
             sampling={key: getattr(params, key, None) for key in (
                 'max_tokens', 'temperature', 'top_p', 'seed', 'n', 'stop',
                 'presence_penalty', 'frequency_penalty', 'repetition_penalty', 'structured_outputs')})
        terminal = False
        try:
            async for output in original_generate(self, *args, **kwargs):
                if output.finished:
                    terminal = True
                    evidence = dict(outputs=[row.text for row in output.outputs]) if capture_outputs else {}
                    emit('native_generate_terminal', attempt=attempt, request_id=request_id, **evidence)
                yield output
        finally:
            if not terminal:
                emit('native_generate_unconfirmed', attempt=attempt, request_id=request_id)

    engine_class.__init__, engine_class.generate = observed_init, observed_generate


def setup():
    """Version-bound Ray actor setup hook, imported before model construction."""
    from importlib.metadata import version
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

    install(AsyncLLM, unit, emit, capture_outputs=spec.get('capture_outputs', False))
    emit('native_observer_installed', python=sys.executable, prefix=sys.prefix,
         versions={name: version(name) for name in ('ray', 'vllm', 'torch', 'transformers')},
         meaning='API submission/terminal events; engine acceptance and kernel time unobserved')


def summarize(directory, *, offsets=None):
    """Read all events, or only appended events at a dependent-batch checkpoint."""
    events = []
    for path in Path(directory).glob('native-events-*.jsonl'):
        with path.open() as stream:
            if offsets is not None:
                stream.seek(offsets.get(path, 0))
            events.extend(json.loads(line) for line in stream.read().splitlines())
            if offsets is not None:
                offsets[path] = stream.tell()
    submitted = [row for row in events if row['event'] == 'native_generate_submitted']
    terminal = [row for row in events if row['event'] == 'native_generate_terminal']
    return dict(available=bool(events),
        model_constructor_returned_ns=[row['monotonic_ns'] for row in events
                                      if row['event'] == 'native_constructor_returned'],
        first_native_submit_ns=min((row['monotonic_ns'] for row in submitted), default=None),
        last_confirmed_terminal_ns=max((row['monotonic_ns'] for row in terminal), default=None),
        submitted_attempts=[row['attempt'] for row in submitted],
        terminal_attempts=[row['attempt'] for row in terminal],
        terminal_outputs=[dict(attempt=row['attempt'], request_id=row['request_id'], outputs=row['outputs'])
                          for row in terminal if 'outputs' in row],
        unknown_attempts=[row['attempt'] for row in events if row['event'] == 'native_generate_unconfirmed'],
        native_versions=[row['versions'] for row in events if row['event'] == 'native_observer_installed'],
        native_interpreters=[dict(python=row['python'], prefix=row['prefix']) for row in events
                             if row['event'] == 'native_observer_installed'],
        engine_accept_ns=None, kernel_seconds=None,
        scope='AsyncLLM API events, not engine acceptance or kernel timing')
