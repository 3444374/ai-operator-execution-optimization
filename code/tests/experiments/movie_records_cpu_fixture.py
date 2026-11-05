"""Movie source, actual LOTUS formatting and local CPU model-process fixtures."""
from contextlib import ExitStack
from pathlib import Path
import unittest
from unittest.mock import patch

from src.baselines.text.movie_records import MovieRecordSource
from src.baselines.text.movie_tasks import MovieTask
from src.experiments import lotus_records_movie as movie
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.semantic_methods.lotus import engine as methods
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
from tests.experiments import test_movie_records_session as session_fixtures
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.serving.test_native_resource_feedback import CPUFeedbackProcess


class CPUFeedbackWithBirth(CPUFeedbackProcess):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.ready_report=dict(self.ready_report,pid=self.process.pid)


class MovieRecordsCPUFixture(unittest.TestCase):
    def setUp(self):
        self.fixture=session_fixtures.PlanTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.root=Path(self.fixture.manifests['movie-q3']).parent.parent
        self.source=MovieRecordSource.from_manifest(self.fixture.manifests['movie-q3'])
        self.method=MovieTask.from_config('movie-q3').method('openai/cpu-fixture')
        self.engine=EngineSpec('/tmp/CPU','/tmp/model','/tmp/driver',max_num_seqs=4,max_model_calls=4)
        self.resources=RecordsResources(prepare_batch_rows=1,model_batch_rows=1,max_active_batches=4,
            startup_timeout_s=5,query_timeout_s=10)

    def runtime(self):
        stack=ExitStack();self.addCleanup(stack.close)
        # Capture the real method functions before the graph fixture replaces them.
        formatter,binder=methods.LotusFilterMethod,methods.bind_filter_method
        runtime=stack.enter_context(ThreadedRuntime(self.source.rows()))
        ready=[False]
        runtime.ray.init=lambda **kw:ready.__setitem__(0,True)
        runtime.ray.is_initialized=lambda:ready[0]
        runtime.ray.shutdown=lambda:ready.__setitem__(0,False)
        stack.enter_context(patch.object(methods,'LotusFilterMethod',formatter))
        stack.enter_context(patch.object(methods,'bind_filter_method',binder))
        stack.enter_context(patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUFeedbackWithBirth))
        stack.enter_context(patch.object(movie,'verify_checkout',return_value='CPU-upstream'))
        stack.enter_context(patch.object(movie,'evaluate_original',return_value={'CPU_fixture_quality':True}))
        return runtime,stack
