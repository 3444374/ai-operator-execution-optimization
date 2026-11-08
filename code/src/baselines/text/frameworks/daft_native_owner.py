"""Close native Daft models through vLLM's public factory/shutdown API.

Daft's experimental local AI expression does not expose its model owner. This
scope is for one query in an isolated caller process: it observes construction,
restores the factory, then closes the actual engines. Scheduling and generation
remain in the original Daft executor. Its daemon loop ends with the process.
"""

import inspect
import math
import time

from src.infrastructure.redact import redact_text


class NativeModelCloseError(RuntimeError):
    def __init__(self, report):
        super().__init__('native Daft model shutdown was not confirmed')
        self.model_close_report = report


class NativeAsyncModelOwner:
    def __init__(self, timeout_s):
        if type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or timeout_s <= 0:
            raise ValueError('native model close timeout must be finite and positive')
        self.timeout_s = timeout_s
        self.models = []
        self.construction_attempts = 0
        self.report = dict(status='not_started', captured_models=0,
            engine_api_shutdown_confirmed=None, gpu_exit_confirmed=None,
            scope='public vLLM factory/shutdown; isolated native Daft query', errors=[])

    def __enter__(self):
        from vllm import AsyncLLMEngine

        self.engine_type = AsyncLLMEngine
        self.original = inspect.getattr_static(AsyncLLMEngine, 'from_engine_args')
        owner = self

        def construct(engine_type, *args, **kwargs):
            owner.construction_attempts += 1
            model = owner.original.__get__(None, engine_type)(*args, **kwargs)
            owner.models.append(model)
            return model

        self.engine_type.from_engine_args = classmethod(construct)
        return self

    def __exit__(self, error_type, primary, traceback):
        # Do not leave a process-global observation hook installed while closing.
        self.engine_type.from_engine_args = self.original
        started = time.monotonic()
        deadline = started + self.timeout_s
        failures = []
        for model in reversed(self.models):
            try:
                model.shutdown(timeout=max(0., deadline - time.monotonic()))
            except BaseException as error:
                failures.append(error)
        unconfirmed = self.construction_attempts - len(self.models)
        uncertain = bool(failures or unconfirmed)
        self.report.update(status='unknown' if uncertain else 'closed' if self.models else 'not_started',
            captured_models=len(self.models),
            construction_attempts=self.construction_attempts, unconfirmed_constructions=unconfirmed,
            engine_api_shutdown_confirmed=False if uncertain else True if self.models else None,
            close_seconds=time.monotonic() - started, timeout_s=self.timeout_s,
            errors=[dict(type=type(error).__name__, message=redact_text(str(error))) for error in failures])
        self.models.clear()
        if uncertain:
            if primary is not None:
                primary.model_close_report = self.report
                primary.add_note('native Daft model cleanup remains unconfirmed; see model_close_report')
            else:
                error = NativeModelCloseError(self.report)
                if failures:
                    raise error from failures[0]
                raise error
        return False
