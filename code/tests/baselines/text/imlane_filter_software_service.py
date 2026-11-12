"""No-model service for the real native Filter SQL gate.

Reuse the tested IPC engine fixture. Started provider receipts supply exact
occurrence identity at each native batch position, never a message-hash lookup.
"""
import argparse
import asyncio
from contextvars import ContextVar
import importlib.abc
from pathlib import Path
import sys
from unittest.mock import patch


class NoModels(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.', 1)[0] in {'vllm','torch','transformers','ray','daft','lotus','httpx','requests'}:
            raise AssertionError('model/framework imports forbidden in zero-model service: ' + fullname)


def service(fixture_path, receipts, service_config):
    from integrations.imlane_duckdb import model_ipc_worker as worker
    from src.infrastructure.private_artifacts import read_json, write_private_json
    from tests.baselines.text.test_imlane_duckdb_model_ipc_worker import FakeEngine, FakeSampling
    rows = read_json(fixture_path)['rows']
    by_id = {row['occurrence_id']: dict(row, index=i) for i,row in enumerate(rows)}
    if len(by_id) != len(rows): raise ValueError('fixture must preserve unique occurrences')
    selected = ContextVar('software_occurrence')
    original_row = worker.ModelWorker._row

    async def row_fixture(self, request, messages, call):
        started = read_json(receipts / ('model-batch-' + request['batch_id'] + '.started.json'))
        slot = started['calls'][call['position']]
        assert slot['position'] == call['position']
        row = by_id[slot['occurrence_id']]
        assert messages == row['messages'] == slot['requested_payload']['messages']
        token = selected.set(row)
        try: return await original_row(self, request, messages, call)
        finally: selected.reset(token)

    class Renderer:
        async def render_chat_async(self, conversations, _params):
            row = selected.get()
            assert conversations == [row['messages']]
            return conversations, [dict(type='token', prompt_token_ids=row['token_ids'], fixture_index=row['index'])]

    engine = FakeEngine()
    engine.renderer = Renderer()
    engine.vllm_config.model_config.max_model_len = 32768
    engine.texts = {i:row['generated_text'] for i,row in enumerate(rows)}
    engine.release.set()
    def factory(_config):
        return worker.NativeModel(engine, FakeSampling, 'final-only', object(), 'saved-answer-software', FakeSampling)
    config = read_json(service_config)
    completed = False
    try:
        with patch.object(worker.ModelWorker, '_row', new=row_fixture):
            asyncio.run(worker.serve(config, model_factory=factory))
        completed = True
        return 0
    finally:
        write_private_json(service_config.parent/'software-service-final.json', dict(
            models=0, GPU=False, software_generate=len(engine.calls), completed=completed,
            active_requests=list(engine.active), shutdowns=len(engine.shutdowns),
            socket_exists=Path(config['socket_path']).exists(),
            calls=[dict(occurrence_id=rows[p['fixture_index']]['occurrence_id'],
                        token_ids=p['prompt_token_ids'], generated_text=engine.texts[p['fixture_index']],
                        request_id=request) for p,_sampling,request,_started in engine.calls]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('fixture','receipts','config'): parser.add_argument('--'+name,type=Path,required=True)
    args = parser.parse_args()
    sys.meta_path.insert(0, NoModels())
    return service(args.fixture,args.receipts,args.config)


if __name__ == '__main__': raise SystemExit(main())
