"""Pair identity and field binding around the author's unchanged Join methods."""
import json
from pathlib import Path
import re


def install(cfg,unit,native,cartesian_class,filter_class):
    from vllm.kalypso.sem_ops.prompt_utils import get_data_prompt,get_prompt
    from vllm.kalypso.sem_ops.base import OpName
    allowed={q['query_id']:q for q in unit['queries']};tables={}
    def table(qid):
        if qid not in allowed:raise ValueError('Join is outside the bound native unit')
        if qid not in tables:
            value=json.loads((Path(cfg['map_prompt_root'])/(qid+'.json')).read_text())
            if (value['query_id']!=qid or not value['left'] or not value['right']
                    or len(value['left'])*len(value['right'])!=allowed[qid]['expected_calls']):
                raise ValueError('Join dimensions differ from the complete pair budget')
            tables[qid]=value
        return tables[qid]
    original_cp=cartesian_class.__call__
    original_prompt=filter_class._build_prompts
    def product(self,ctx):
        value=table(ctx.state.raw_request.headers.get('X-Kalypso-Observation'))
        left=ctx.state.idx;width=len(value['right'])
        if type(left) is not int or not 0<=left<len(value['left']):
            raise ValueError('Unknown native Join left occurrence')
        children=original_cp(self,ctx)
        if len(children)!=width:raise ValueError('Original CartesianProduct fanout differs from bound right table')
        # idx is response/observation metadata, not a native scheduler key.
        for right,child in enumerate(children):child.state.idx=left*width+right
        return children
    def prompt(self,ctx):
        # Native build() estimates this post-CP stage using pre-fanout left
        # contexts. Preserve that estimate, but never submit such a context.
        if not ctx.input.right_data:
            if native.ROW.get() is not None:raise ValueError('Join model request lacks its right context')
            return original_prompt(self,ctx)
        value=table(ctx.state.raw_request.headers.get('X-Kalypso-Observation'))
        idx=ctx.state.idx;width=len(value['right'])
        if type(idx) is not int or not 0<=idx<len(value['left'])*width:
            raise ValueError('Unknown native Join pair identity')
        left,right=value['left'][idx//width],value['right'][idx%width]
        if (ctx.input.data!=get_data_prompt(left['data'].strip())
                or ctx.input.right_data!=get_data_prompt(right['data'].strip())
                or self.instruction!=value['instruction']):
            raise ValueError('Native Join pair differs from its source occurrence')
        fields={**left['fields'],**right['fields']}
        # :left/:right are field names, not Python format specifications.
        formatted=re.sub(r'\{([^{}]+)\}',lambda match:fields[match[1]],self.instruction)
        data_prompt,_=original_prompt(self,ctx)
        return data_prompt,get_prompt(formatted,ctx.input.data+ctx.input.right_data,op=OpName.SEM_JOIN)
    cartesian_class.__call__=product
    filter_class._build_prompts=prompt
