"""Qualification-only native sidecar; original IPC, calls and results delegate unchanged."""
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from unittest.mock import patch

from src.execution_provider.adapters.records_async_model import AsyncCoreModelActor
from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
from src.serving.backends.async_engine_process import AsyncEngineProcess


class CancellationProcess(AsyncEngineProcess):
    def __init__(self, *args, trace_directory, **kwargs):
        self.trace_directory = str(trace_directory)
        super().__init__(*args, **kwargs)

    def _command(self, spec, descriptors):
        child = Path(__file__).resolve().parents[2]/'scripts/experiments/probe_movie_cancellation_child.py'
        return [spec.model_python, '-u', '-B', str(child), self.trace_directory, *map(str, descriptors)]


class CancellationActor(AsyncCoreModelActor):
    def __init__(self, engine, generation, model_batch_rows, limits, trace_directory):
        self.trace_directory = trace_directory
        super().__init__(engine, generation, model_batch_rows, limits)

    def _process_type(self):
        return partial(CancellationProcess, trace_directory=self.trace_directory)


@contextmanager
def instrument(directory):
    arguments = AsyncRecordsActorOwner._model_actor_arguments
    with patch.object(AsyncRecordsActorOwner, '_model_actor_type', return_value=CancellationActor), \
            patch.object(AsyncRecordsActorOwner, '_model_actor_arguments',
                         lambda owner: (*arguments(owner), str(directory))):
        yield
