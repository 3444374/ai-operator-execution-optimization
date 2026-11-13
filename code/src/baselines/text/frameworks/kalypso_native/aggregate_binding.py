"""Original SemAgg source binding and passive native reduction observation."""
import json
from pathlib import Path


OPERATOR = "<class 'vllm.kalypso.sem_ops.reducers.SemAgg'>"
PREFIX = 'CONTEXT:\n  {\n    "text": '
SUFFIX = '\n  }\n'


def context_text(messages):
    """Invert precisely the pinned author's get_data_prompt, without parsing text."""
    if (not isinstance(messages, list) or len(messages) != 1 or type(messages[0]) is not dict
            or set(messages[0]) != {'role', 'type', 'content'}
            or messages[0]['role'] != 'user' or messages[0]['type'] != 'text'):
        raise ValueError('Native aggregate requires the original single context message')
    content = messages[0]['content']
    if not isinstance(content, str) or not content.startswith(PREFIX) or not content.endswith(SUFFIX):
        raise ValueError('Native aggregate context wrapper differs from the pinned author')
    return content[len(PREFIX):-len(SUFFIX)]


def response_text(response, model, rows):
    """Preserve native zero/one pass-through and the reduced context index -1."""
    expected = min(1, len(rows))
    if (type(response) is not dict or response.get('model_name') != model
            or type(response.get('num_output_rows')) is not int
            or response['num_output_rows'] != expected or not isinstance(response.get('results'), list)
            or len(response['results']) != expected):
        raise ValueError('Native aggregate returned a different complete relation')
    if not rows:
        return None
    result = response['results'][0]
    outputs = result.get('outputs')
    if (type(result.get('idx')) is not int or result['idx'] != (0 if len(rows) == 1 else -1)
            or result.get('right_input_context') != [] or not isinstance(outputs, list) or len(outputs) != 1
            or type(outputs[0]) is not dict or outputs[0].get('op') != OPERATOR
            or outputs[0].get('value') != result.get('input_context')):
        raise ValueError('Native aggregate lost its final output/context or native index')
    text = context_text(result['input_context'])
    if len(rows) == 1 and text != rows[0]['data'].strip():
        raise ValueError('Native aggregate changed the original single-row pass-through')
    return text


def install(cfg, unit, native, operator_class):
    from vllm.kalypso.sem_ops.prompt_utils import get_data_prompt
    allowed = {q['query_id']: q for q in unit['queries']}
    original_call, original_reduce = operator_class.__call__, operator_class._reduce_chunk

    async def aggregate(self, contexts):
        if not contexts:
            return await original_call(self, contexts)
        request = contexts[0].state.raw_request
        qid = request.headers.get('X-Kalypso-Observation')
        if qid not in allowed:
            raise ValueError('Native aggregate query is outside its finite binding')
        table = json.loads((Path(cfg['map_prompt_root'])/(qid+'.json')).read_text())
        if (table['query_id'] != qid or len(contexts) != allowed[qid]['expected_rows']
                or len(table['rows']) != len(contexts) or self.instruction != table['native_instruction']
                or self.max_tokens != 8192 or self.concurrency != 8):
            raise ValueError('Native aggregate source, instruction or author defaults changed')
        for index, (ctx, row) in enumerate(zip(contexts, table['rows'], strict=True)):
            if (row['idx'] != index or ctx.state.idx != index or ctx.state.raw_request is not request
                    or ctx.input.right_data or ctx.input.data != get_data_prompt(row['data'].strip())):
                raise ValueError('Native aggregate lost its source occurrence')
        return await original_call(self, contexts)

    async def reduce(self, chunk):
        # These are native context indices, including -1 after a reduction.
        # They do not assert the unknown source lineage of generated summaries.
        token = native.ROW.set([ctx.state.idx for ctx in chunk])
        try:
            return await original_reduce(self, chunk)
        finally:
            native.ROW.reset(token)

    operator_class.__call__, operator_class._reduce_chunk = aggregate, reduce
    return 'original SemAgg chunking/reducer; observed native context indices, source lineage unknown after merge'


def audit(text, events, *, rows, cap, model_uid, previous_ids):
    if any(e['event'] == 'native_API_rejected' for e in events):
        raise ValueError('Native aggregate rejected a reducer; its partial return cannot qualify')
    starts = [e for e in events if e['event'] == 'native_API_start']
    ends = [e for e in events if e['event'] == 'native_API_end']
    if not len(starts) == len(ends) <= cap or (rows > 1 and not ends) or (rows <= 1 and ends):
        raise ValueError('Native aggregate reductions are incomplete or exceed their finite upper bound')
    by_sequence = {e['sequence']: e for e in starts}
    ids = {e.get('request_id') for e in ends}
    if (len(by_sequence) != len(starts) or {e['sequence'] for e in ends} != set(by_sequence)
            or len(ids) != len(ends) or ids & previous_ids
            or any(not isinstance(i, str) or not i for i in ids)):
        raise ValueError('Native aggregate request identities differ')
    for end in ends:
        start = by_sequence.get(end['sequence'])
        indices = end['idx']
        if (start is None or start['idx'] != indices or end['status'] != 'succeeded'
                or end['model_uid'] != model_uid or end['max_tokens'] != 8192 or end['pin']
                or not isinstance(indices, list) or len(indices) < 2
                or any(type(i) is not int or not -1 <= i < rows for i in indices)
                or not isinstance(end.get('generated_text'), str)):
            raise ValueError('Native aggregate completion changed owner, context indices or sampling')
    if ends and ends[-1]['generated_text'] != text:
        raise ValueError('Native aggregate final context differs from its last actual generation')
    return ends, ids
