"""CPU-only live/abort evidence over the existing three-socket protocol.

Explicit gates stand in for model work in this deterministic fixture only.
The GPU entry never installs these gates or modifies generation parameters.
"""
import asyncio
import sys
from tests.execution_provider.section_child_fixture import FixtureEngine
from src.observability.native_model_api_trace import writer
from src.serving.backends.async_engine_worker import serve


class Engine(FixtureEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.emit = writer(sys.argv[1])
        if sys.argv[2] == 'hold':
            self.config['hold'] = list(self.config['responses'])
            for response in self.config['responses'].values():
                response['status'] = 'unknown'

    @staticmethod
    def identity(key, present=True):
        internal = 'cpu-internal-' + key
        return dict(lookup_internal=False, mapped_internal_ids=[internal],
                    present_internal_ids=[internal] if present else [], external_id_by_internal={internal:key})

    async def generate_prepared(self, row, *, request_id, **kwargs):
        self.emit('qualification_request', request_id=request_id,
                  **{name:row[name] for name in ('call_id','query_id','row_id','source_position','call_index')})
        self.emit('native_add_return', request_id=request_id, native_identity=self.identity(request_id))
        result = await super().generate_prepared(row, request_id=request_id, **kwargs)
        if result['status'] == 'unknown':result['error_type'] = 'AbortNotConfirmed'
        self.emit('native_final_observed', request_id=request_id, finished=True, status=result['status'])
        return result

    async def abort(self, request_id):
        self.emit('native_abort_enter', request_id=request_id, internal=False,
                  native_identities={request_id:self.identity(request_id, request_id in self.gates)})
        result = await super().abort(request_id)
        self.emit('native_abort_return', request_id=request_id)
        return result


if __name__ == '__main__':
    asyncio.run(serve(*map(int,sys.argv[3:]),engine_factory=Engine))
