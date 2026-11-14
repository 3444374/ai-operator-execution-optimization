"""Referenced source fields inside original native Filter and Map templates."""
import json
from pathlib import Path


def extract_instruction(output_columns):
    """A task instruction for native SemMap, not a native Extract operator."""
    fields = json.dumps(output_columns, ensure_ascii=False)
    return ('Extract the following fields from the supplied context. Return one JSON object; '
            'the keys and their descriptions are: '+fields+'. '
            'Use null when a value is unknown. Return the JSON object without explanation or Markdown.')


def install_prompt(cfg, unit, operator_class, method_name, op):
    """Bind original fields while leaving the native execution method intact."""
    from vllm.kalypso.sem_ops.prompt_utils import get_prompt, get_data_prompt
    allowed = {q['query_id']:q for q in unit['queries']}
    tables = {}
    current_query = [None]
    def table(qid):
        if qid not in allowed:
            raise ValueError('Native query is outside this finite binding')
        if qid not in tables:
            value = json.loads((Path(cfg['map_prompt_root'])/(qid+'.json')).read_text())
            if value['query_id'] != qid or len(value['rows']) != allowed[qid]['expected_calls']:
                raise ValueError('Native occurrence table differs from the declared query')
            if ([r['idx'] for r in value['rows']] != list(range(len(value['rows'])))
                    or len({r['row_id'] for r in value['rows']}) != len(value['rows'])):
                raise ValueError('Duplicate or reordered Native occurrence identity')
            tables[qid] = value
        return tables[qid]
    original_prompt = getattr(operator_class,method_name)
    def prompt(self, ctx):
        qid = ctx.state.raw_request.headers.get('X-Kalypso-Observation')
        current_query[0] = qid
        value = table(qid)
        if type(ctx.state.idx) is not int or not 0 <= ctx.state.idx < len(value['rows']):
            raise ValueError('Invalid native occurrence index')
        row = value['rows'][ctx.state.idx]
        if (ctx.input.right_data or ctx.input.data != get_data_prompt(row['data'].strip())
                or self.instruction != value['instruction']):
            raise ValueError('Native context/instruction differs from its source occurrence')
        data_prompt, _ = original_prompt(self,ctx)
        # Preserve all original referenced fields inside the author's native template.
        # Extract's field schema is literal JSON; its source is in the context.
        formatted = (self.instruction if allowed[qid]['task'] == 'sembench-extract-map'
                     else self.instruction.format_map(row['fields']))
        return data_prompt, get_prompt(formatted,ctx.input.data,op=op)
    setattr(operator_class,method_name,prompt)
    return current_query
