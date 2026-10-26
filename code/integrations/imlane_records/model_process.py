"""IMLane launch assembly over the existing private engine process component."""
import os

from src.baselines.text.frameworks.daft_engine_process import EngineProcess
from src.baselines.text.frameworks.lotus_chain_engine_process import ChainEngineProcess


def assigned_environment(value, *, check_current=False):
    if (type(value) is not dict or any(type(v) is not str or "\0" in v for v in value.values())
            or set(value) - {"CUDA_VISIBLE_DEVICES", "LD_LIBRARY_PATH"}
            or not value.get("CUDA_VISIBLE_DEVICES", "").strip() or "," in value["CUDA_VISIBLE_DEVICES"]
            or check_current and value["CUDA_VISIBLE_DEVICES"] != os.environ.get("CUDA_VISIBLE_DEVICES")):
        raise ValueError("IMLane model needs the single GPU explicitly assigned to its author worker")
    return dict(value)


class _AuthorModelEnvironment:
    def __init__(self, *args, model_environment, **kwargs):
        self.model_environment = assigned_environment(model_environment, check_current=True)
        super().__init__(*args, **kwargs)

    def _command(self, engine, result_fd):
        # Do not change the embedding author's ABI environment in os.environ.
        # env exec preserves the owned PID and inherited namespace/result FD.
        return ["/usr/bin/env", "-u", "LD_LIBRARY_PATH", "-u", "PYTHONHOME", "-u", "VIRTUAL_ENV",
                *[key + "=" + value for key, value in self.model_environment.items()],
                *super()._command(engine, result_fd)]


class IMLaneEngineProcess(_AuthorModelEnvironment, EngineProcess):
    pass


class IMLaneChainEngineProcess(_AuthorModelEnvironment, ChainEngineProcess):
    pass
