"""SemBench field binding and passive receipts; native Map/KV/sampling unchanged."""
import hashlib
import json
from pathlib import Path
import time


def digest(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def install(cfg, unit, directory, native, map_class, estimator_class):
    from vllm.kalypso.sem_ops.prompt_utils import get_prompt, get_data_prompt
    from vllm.kalypso.sem_ops.base import OpName
    from vllm.kalypso.context import RETRY_TASK
    directory = Path(directory)
    allowed = {q['query_id']:q for q in unit['queries']}
    tables = {}
    current_query = [None]
    errors = []
    def event(kind, **fields):
        try:
            with (directory/'map-trace.jsonl').open('a') as out:
                out.write(json.dumps(dict(event=kind,monotonic_ns=time.monotonic_ns(),**fields),ensure_ascii=False)+'\n')
        except Exception as error:
            errors.append(type(error).__name__)
    def table(qid):
        if qid not in allowed:
            raise ValueError('Map query is outside this finite binding')
        if qid not in tables:
            value = json.loads((Path(cfg['map_prompt_root'])/(qid+'.json')).read_text())
            if value['query_id'] != qid or len(value['rows']) != allowed[qid]['expected_calls']:
                raise ValueError('Map occurrence table differs from the declared query')
            if ([r['idx'] for r in value['rows']] != list(range(len(value['rows'])))
                    or len({r['row_id'] for r in value['rows']}) != len(value['rows'])):
                raise ValueError('Duplicate or reordered Map occurrence identity')
            tables[qid] = value
        return tables[qid]
    original_prompt = map_class._build_prompt
    original_run = map_class._run_single
    original_reset = estimator_class.reset
    def prompt(self, ctx):
        qid = ctx.state.raw_request.headers.get('X-Kalypso-Observation')
        current_query[0] = qid
        value = table(qid)
        if type(ctx.state.idx) is not int or not 0 <= ctx.state.idx < len(value['rows']):
            raise ValueError('Invalid native Map occurrence index')
        row = value['rows'][ctx.state.idx]
        if (ctx.input.right_data or ctx.input.data != get_data_prompt(str(row['reviewText']).strip())
                or self.instruction != value['instruction']):
            raise ValueError('Native Map context/instruction differs from its source occurrence')
        data_prompt, _ = original_prompt(self,ctx)
        # Preserve the original task instruction/review inside the author's Map template.
        formatted = self.instruction.format(reviewText=row['reviewText'])
        return data_prompt, get_prompt(formatted,ctx.input.data,op=OpName.SEM_MAP)
    async def run(self,ctx,priority=0):
        qid = ctx.state.raw_request.headers.get('X-Kalypso-Observation')
        token = native.ROW.set(ctx.state.idx)
        retry_before = dict(position=ctx.state.retry_op_position,max_tokens=ctx.state.retry_max_tokens)
        try:
            result = await original_run(self,ctx,priority=priority)
            if result is RETRY_TASK:
                event('map_retry',query_id=qid,idx=ctx.state.idx,retry_before=retry_before,
                    retry_after=dict(position=ctx.state.retry_op_position,max_tokens=ctx.state.retry_max_tokens))
            else:
                text = ctx.output[-1][str(type(self))]
                event('map_result',query_id=qid,idx=ctx.state.idx,text=text,
                    retry_before=retry_before,estimator_samples=len(estimator_class.instance().data.get(self.position, ())))
            return result
        except BaseException as error:
            event('map_error',query_id=qid,idx=ctx.state.idx,error_type=type(error).__name__)
            raise
        finally:
            native.ROW.reset(token)
    def reset(self):
        event('estimator_before_native_reset',query_id=current_query[0],estimator_id=id(self),
            samples={str(k):len(v) for k,v in self.data.items()},
            ratios={str(k):list(v) for k,v in self.data.items()})
        result = original_reset(self)
        event('estimator_after_native_reset',query_id=current_query[0],estimator_id=id(self),samples={str(k):len(v) for k,v in self.data.items()})
        return result
    map_class._build_prompt = prompt
    map_class._run_single = run
    estimator_class.reset = reset
    return dict(errors=errors,semantic_binding='SemBench exact formatted Map instruction inside original author Map prompt template',
        identical_to_LOTUS_message_template=False,original_length_estimation_and_retry_implementation=True,
        original_ratio_update_and_reset=True,original_sampling=True,physical_budget=unit['call_cap'])


def install_response_observation(native, directory):
    """Observe usage and generate identity without changing outcomes or iterators."""
    import inspect
    import os
    from vllm.entrypoints.openai import api_server
    original = api_server.create_chat_completion
    path = Path(directory) / 'uniform-responses.jsonl'
    generate_path = Path(directory) / 'uniform-generate.jsonl'
    bound_clients = set()
    errors = []

    def record(target, value):
        try:
            with target.open('a') as stream:
                stream.write(json.dumps(value, ensure_ascii=False) + '\n')
        except Exception as error:
            errors.append(type(error).__name__)

    def bind_client(client):
        if id(client) in bound_clients:
            return
        generate = client.generate
        signature = inspect.signature(generate)

        def observed_generate(*args, **kwargs):
            try:
                qid, sequence = native.CALL.get()
                call = signature.bind_partial(*args, **kwargs).arguments
                prompt = call.get('prompt', call.get('inputs'))
                params = call.get('sampling_params')
                value = dict(query_id=qid, sequence=sequence, idx=native.ROW.get(),
                    api_pid=os.getpid(), engine_client_id=id(client),
                    request_id=call.get('request_id'), observed_ns=time.monotonic_ns(),
                    prompt_token_ids=prompt.get('prompt_token_ids') if isinstance(prompt, dict) else getattr(prompt, 'prompt_token_ids', None),
                    prompt_type=type(prompt).__name__,
                    sampling={name: getattr(params, name, None) for name in
                        ['max_tokens', 'temperature', 'top_p', 'seed', 'stop',
                         'presence_penalty', 'frequency_penalty', 'repetition_penalty', 'n']},
                    scope='generate API invocation; not add_request or GPU kernel execution')
                record(generate_path, value)
            except Exception as error:
                errors.append(type(error).__name__)
                record(generate_path, dict(observation_error=type(error).__name__))
            # Return the original object without wrapping iteration or close.
            return generate(*args, **kwargs)

        client.generate = observed_generate
        bound_clients.add(id(client))

    async def observed(*args, **kwargs):
        raw = kwargs['raw_request']
        try:
            bind_client(raw.app.state.engine_client)
        except Exception as error:
            errors.append(type(error).__name__)
            record(generate_path, dict(observation_error=type(error).__name__))
        response = await original(*args, **kwargs)
        try:
            qid, sequence = native.CALL.get()
            body = json.loads(response.body)
            record(path, dict(query_id=qid, sequence=sequence, idx=native.ROW.get(),
                api_pid=os.getpid(), engine_client_id=id(raw.app.state.engine_client),
                observed_ns=time.monotonic_ns(), request_id=body.get('id'),
                usage=body.get('usage'), choices=body.get('choices')))
        except Exception as error:
            errors.append(type(error).__name__)
            record(path, dict(observation_error=type(error).__name__))
        return response

    api_server.create_chat_completion = observed
    return errors
