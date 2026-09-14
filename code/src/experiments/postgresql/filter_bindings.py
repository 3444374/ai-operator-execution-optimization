"""Verify PG Filter inputs and decisions using independent producer row bindings."""
import json
import re

from src.execution_provider.wire import v3
from .map_bindings import parse_pg_bindings


def verify_filter_decisions(lines, raw_inputs, model_id, *, complete, instruction):
    """Match trace inputs to immutable raw rows before reading kept decisions."""
    adapted, decisions = [], {}
    for line in lines:
        match = re.search(r'SEMLOOM_FILTER_BINDING (\{.*\})\s*$',line)
        if match is None:
            continue
        value = json.loads(match.group(1))
        if value.get('phase') in ('before_offer','accepted'):
            adapted.append('LOG: SEMLOOM_MAP_BINDING '+match.group(1))
        elif value.get('phase') == 'decision':
            key = (value['backend_pid'],value['stream'],value['sequence'])
            if value.get('version') != 1 or type(value.get('kept')) is not bool or key in decisions:
                raise ValueError('invalid or duplicate PG Filter decision')
            decisions[key] = value['kept']
        else:
            raise ValueError('unknown Filter trace phase')
    bindings = parse_pg_bindings(adapted)
    plan = v3.SemanticFilterPlan(instruction,model_id)
    expected_spec = v3.semantic_spec_digest(plan)
    rows = {}
    namespaces = {(b.backend_pid,b.stream) for b in bindings}
    if len(namespaces)>1:
        raise ValueError('query audit includes multiple Filter streams')
    for binding in bindings:
        if binding.row_id not in raw_inputs or binding.row_id in rows:
            raise ValueError('Filter producer has an unknown or duplicate row')
        text = raw_inputs[binding.row_id]
        digest = v3.semantic_payload_digest(semantic_spec_sha256=expected_spec,input_value=text,
                          canonical_messages_utf8=v3.canonical_messages(instruction,text))
        if binding.payload_digest != digest:
            raise ValueError('Filter payload differs from its independently bound raw row')
        key = (binding.backend_pid,binding.stream,binding.sequence)
        if key not in decisions:
            raise ValueError('accepted Filter task has no final row decision')
        rows[binding.row_id] = decisions.pop(key)
    if decisions or (complete and set(rows)!=set(raw_inputs)):
        raise ValueError('Filter decision set is incomplete or has orphan records')
    return rows
