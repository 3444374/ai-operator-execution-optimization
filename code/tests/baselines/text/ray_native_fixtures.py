"""Software doubles for the public Ray Dataset/LLM seam; never start Ray."""

from contextlib import contextmanager
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import time
from types import ModuleType, SimpleNamespace

from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.shared_request_budget import SharedClaimedUnit
from tests.baselines.text.native_sdk_fixtures import module_entries


@contextmanager
def recorded_processor(replies, *, transform=lambda rows: rows, close_error=None, advance=lambda phase: None):
    state = SimpleNamespace(initialized=False, requests=[], events=[], configs=[])
    ray, data, llm = (ModuleType(name) for name in ('ray', 'ray.data', 'ray.data.llm'))
    ray.__path__ = data.__path__ = []
    ray.__version__ = 'software-fixture-no-Ray'
    ray.data, data.llm = data, llm
    ray.is_initialized = lambda: state.initialized
    def init(**kwargs):
        state.initialized = True
        state.events.append('init')
    def shutdown():
        state.events.append('close')
        advance('close')
        if close_error:
            raise close_error
        state.initialized = False
    ray.init, ray.shutdown = init, shutdown

    class Dataset:
        def __init__(self, read):
            self.read = read
        def materialize(self):
            values = self.read()
            advance('materialize')
            state.events.append('materialize')
            return Dataset(lambda: values)
        def take_all(self):
            state.events.append('take_all')
            return self.read()
    data.from_items = lambda rows: Dataset(lambda: rows)
    def config(**kwargs):
        state.configs.append(deepcopy(kwargs))
        return SimpleNamespace(model_dump=lambda **unused: deepcopy(kwargs))
    llm.vLLMEngineProcessorConfig = config
    class vLLMEngineStage:
        def __init__(self):
            self.map_batches_kwargs = {}
    state.stage = vLLMEngineStage()
    def build(config, *, preprocess, postprocess):
        class Processor:
            def list_stage_names(self):
                return ['engine']
            def get_stage_by_name(self, name):
                return state.stage
            def __call__(self, dataset):
                def read():
                    spec = json.loads(state.stage.map_batches_kwargs['runtime_env']['env_vars']['SEMLOOM_RAY_NATIVE_OBSERVER'])
                    unit = SharedClaimedUnit(Path(spec['path']), AttemptBudget(spec['id'], spec['limit']), spec['unit_id'])
                    events = []
                    output = []
                    for row in dataset.read():
                        request = preprocess(row)
                        state.requests.append(dict(call_id=row['call_id'], **deepcopy(request)))
                        attempt = unit.reserve(hashlib.sha256(row['call_id'].encode()).hexdigest())
                        events.extend(dict(event=event, attempt=attempt, monotonic_ns=time.monotonic_ns())
                                      for event in ('native_generate_submitted', 'native_generate_terminal'))
                        output.append(postprocess(dict(row, **request,
                            generated_text=replies[row['call_id']], request_id='fixture-'+row['call_id'])))
                    Path(spec['output'], 'native-events-software.jsonl').write_text(
                        ''.join(json.dumps(event)+'\n' for event in events))
                    return transform(output[::-1])
                return Dataset(read)
        return Processor()
    llm.build_processor = build
    setup = ModuleType('ray._private.runtime_env.setup_hook')
    def export(env, hook):
        state.hook = hook
        return {**env, 'worker_process_setup_hook': hook}
    setup.export_setup_func_module = export
    modules = {'ray': ray, 'ray.data': data, 'ray.data.llm': llm,
               'ray._private.runtime_env.setup_hook': setup}
    with module_entries(modules):
        yield state
