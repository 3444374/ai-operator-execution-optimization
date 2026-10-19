"""IMLane launch assembly over the existing private engine process component."""
import os

from src.baselines.text.frameworks.daft_engine_process import EngineProcess


class IMLaneEngineProcess(EngineProcess):
    def __init__(self, *args, model_environment, **kwargs):
        if (type(model_environment) is not dict
                or any(type(value) is not str or "\0" in value for value in model_environment.values())
                or set(model_environment) - {"CUDA_VISIBLE_DEVICES", "LD_LIBRARY_PATH"}
                or not model_environment.get("CUDA_VISIBLE_DEVICES")
                or "," in model_environment["CUDA_VISIBLE_DEVICES"]
                or model_environment["CUDA_VISIBLE_DEVICES"] != os.environ.get("CUDA_VISIBLE_DEVICES")):
            raise ValueError("IMLane model needs the single GPU explicitly assigned to its author worker")
        self.model_environment = dict(model_environment)
        super().__init__(*args, **kwargs)

    def _command(self, engine, result_fd):
        # Do not change the embedding author's ABI environment in os.environ.
        # env exec preserves the owned PID and inherited namespace/result FD.
        return ["/usr/bin/env", "-u", "LD_LIBRARY_PATH", "-u", "PYTHONHOME", "-u", "VIRTUAL_ENV",
                *[key + "=" + value for key, value in self.model_environment.items()],
                *super()._command(engine, result_fd)]
