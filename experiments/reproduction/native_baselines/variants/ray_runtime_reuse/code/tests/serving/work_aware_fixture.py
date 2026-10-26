"""Explicit CPU model fixture using the production per-request converter."""
import asyncio
import json
import sys
from types import SimpleNamespace

from src.serving.backends.async_engine_process import serve
from src.serving.backends.vllm_async_engine import VllmAsyncEngine
from src.serving.backends.vllm_engine import _prepared
from tests.serving.resource_feedback_fixture import FixtureEngine as BaseEngine


class FixtureEngine(BaseEngine):
    async def generate_prepared(self,row,**options):
        value=await super().generate_prepared(row,**options)
        prompt=json.loads(value['prompt_token_ids_json'])
        choice=SimpleNamespace(index=0,text=value['text'],token_ids=json.loads(value['output_token_ids_json']),
            finish_reason=value['finish_reason'])
        output=SimpleNamespace(request_id=options['request_id'],finished=True,
            prompt_token_ids=prompt,outputs=[choice],num_cached_tokens=4)
        converter=object.__new__(VllmAsyncEngine)
        converter.engine_uid=self.engine_uid
        converter._llm=SimpleNamespace(get_tokenizer=lambda:None)
        return converter._convert(_prepared([row])[0][0],output,options['request_id'],prompt,options['generation'],
            value['model_start_monotonic'],options['deadline_monotonic'])


if __name__=='__main__':asyncio.run(serve(*map(int,sys.argv[1:]),engine_factory=FixtureEngine))
