"""Bind original ranking fields and observe native comparisons, without sorting."""
import json
from pathlib import Path
import time


def criterion(instruction,columns):
    # Both documents carry their values separately in the author's template.
    return instruction.format_map({name:name for name in columns})


def install(cfg,unit,directory,native,operator_class):
    from vllm.kalypso.sem_ops.prompt_utils import get_data_prompt
    for name in ('native-trace.jsonl','topk-trace.jsonl'):
        (Path(directory)/name).touch(exist_ok=True)
    allowed={q['query_id']:q for q in unit['queries']};errors=[]
    original_call=operator_class.__call__;original_compare=operator_class._compare
    async def rank(self,contexts):
        if not contexts:return await original_call(self,contexts)
        request=contexts[0].state.raw_request
        qid=request.headers.get('X-Kalypso-Observation')
        if qid not in allowed:raise ValueError('Native TopK query is outside its finite binding')
        table=json.loads((Path(cfg['map_prompt_root'])/(qid+'.json')).read_text())
        count=allowed[qid]['expected_rows']
        if (table['query_id']!=qid or len(table['rows'])!=len(contexts) or len(contexts)!=count or self.k!=count
                or self.instruction!=table['native_instruction'] or self.max_tokens!=5
                or [ctx.state.idx for ctx in contexts]!=list(range(count))):
            raise ValueError('Native full-ranking source or instruction changed')
        for index,(ctx,row) in enumerate(zip(contexts,table['rows'],strict=True)):
            if (row['idx']!=index or ctx.state.raw_request is not request or ctx.input.right_data
                    or ctx.input.data!=get_data_prompt(row['data'].strip())):
                raise ValueError('Native TopK lost its source occurrence')
        return await original_call(self,contexts)
    async def compare(self,first,second):
        qid=first.state.raw_request.headers.get('X-Kalypso-Observation')
        pair=(first.state.idx,second.state.idx)
        token=native.ROW.set(pair)
        try:
            result=await original_compare(self,first,second)
            try:
                with (Path(directory)/'topk-trace.jsonl').open('a') as out:
                    out.write(json.dumps(dict(event='topk_compare',query_id=qid,pair=pair,
                        first_better=result,monotonic_ns=time.monotonic_ns()))+'\n')
            except Exception as error:errors.append(type(error).__name__)
            return result
        finally:native.ROW.reset(token)
    operator_class.__call__=rank;operator_class._compare=compare
    return errors


def audit(order,events,comparisons,*,rows,cap,model_uid,previous_ids):
    """Check actual native terminals; the comparison cap is an upper bound."""
    ends=[e for e in events if e['event']=='native_API_end']
    starts=[e for e in events if e['event']=='native_API_start']
    completed=[e for e in comparisons if e['event']=='topk_compare']
    if not len(starts)==len(ends)==len(completed)<=cap:
        raise ValueError('Incomplete native TopK comparisons or exceeded finite allowance')
    by_sequence={e['sequence']:e for e in starts}
    ids={e.get('request_id') for e in ends}
    if (len(by_sequence)!=len(starts) or len(ids)!=len(ends) or ids&previous_ids
            or any(not isinstance(i,str) or not i for i in ids)):
        raise ValueError('Native TopK request identities differ')
    actual_pairs=[]
    for e in ends:
        start=by_sequence.get(e['sequence'])
        if (start is None or start['idx']!=e['idx'] or e['status']!='succeeded'
                or e['model_uid']!=model_uid or e['max_tokens']!=5 or e['pin']):
            raise ValueError('Native TopK completion changed owner, pair or sampling')
        pair=e['idx']
        if (not isinstance(pair,list) or len(pair)!=2 or pair[0]==pair[1]
                or any(type(i) is not int or not 0<=i<rows for i in pair)):
            raise ValueError('Invalid native TopK comparison occurrence')
        actual_pairs.append(tuple(pair))
    compared=[tuple(e['pair']) for e in completed]
    if (sorted(actual_pairs)!=sorted(compared) or len({frozenset(p) for p in actual_pairs})!=len(actual_pairs)
            or any(type(e['first_better']) is not bool for e in completed)):
        raise ValueError('Native TopK comparison audit is incomplete or duplicated')
    positions={index:rank for rank,index in enumerate(order)}
    if any((positions[e['pair'][0]]<positions[e['pair'][1]])!=e['first_better'] for e in completed):
        raise ValueError('Native ranking disagrees with its completed comparisons')
    return ends,ids
