"""Original native chat calls over the shared synchronous model lifecycle.

Unset capacity parameters belong to vLLM; explicit values pass through unchanged.
This module imports no LOTUS, Ray, Core worker or scheduling policy.
"""
import sys

from src.serving.backends.vllm_engine import VllmEngine
from src.serving.backends.vllm_sync_process import EngineProcess
from src.serving.backends.staged_generation_process import ChainEngineProcess


class ReferenceVllmEngine(VllmEngine):
    def _prepare_native(self, conversations, generation, context):
        return conversations, None

    def _invoke_native(self, conversations, sampling):
        return self._llm.chat(conversations, sampling_params=sampling, use_tqdm=False)


class ReferenceEngineProcess(EngineProcess):
    @staticmethod
    def _command(engine, result_fd):
        return [engine.model_python, '-m', __name__, '--serve', str(result_fd)]


class ReferenceChainEngineProcess(ChainEngineProcess):
    @staticmethod
    def _command(engine, result_fd):
        return [engine.model_python, '-m', __name__, '--serve-chain', str(result_fd)]


def main(arguments):
    if len(arguments) != 2 or arguments[0] not in ('--serve', '--serve-chain'):
        raise SystemExit('native reference worker requires an inherited result descriptor')
    if arguments[0] == '--serve':
        from src.serving.backends.vllm_sync_process import serve
    else:
        from src.serving.backends.staged_generation_process import serve
    serve(int(arguments[1]), engine_factory=ReferenceVllmEngine)


if __name__ == '__main__':
    main(sys.argv[1:])
