"""Native baseline capacity parameters over the shared synchronous lifecycle.

This model-side module imports no LOTUS, Ray or fused worker. Reference graph
owners select it explicitly; current SemLoom construction uses native omission.
"""
import sys

from src.serving.backends.vllm_engine import VllmEngine
from src.serving.backends.vllm_sync_process import EngineProcess
from src.serving.backends.staged_generation_process import ChainEngineProcess


class ReferenceVllmEngine(VllmEngine):
    constructor_policy = 'native_reference_capacity'

    def _capacity_kwargs(self, engine):
        return dict(max_model_len=engine.legacy_max_model_len,
                    max_num_seqs=engine.legacy_max_num_seqs,
                    max_num_batched_tokens=engine.legacy_max_num_batched_tokens)

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
