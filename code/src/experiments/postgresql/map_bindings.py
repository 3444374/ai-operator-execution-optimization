"""Verify non-NULL Map rows against PostgreSQL's pre-offer row bindings.

Completion events never generate expected sequence IDs. The request-side socket
session binds one observed PG stream to one gateway session for each query.
"""
from dataclasses import dataclass
import hashlib
import json
import re

from src.baselines.text.map_inputs import text_rows_by_id
from src.execution_provider.semantic_map import canonical_messages
from src.execution_provider.wire.map_codec import semantic_payload_digest

_DIGEST = re.compile(r'[0-9a-f]{64}\Z')
_LOG = re.compile(r'LOG:\s+SEMLOOM_MAP_BINDING (\{.*\})\s*$')


@dataclass(frozen=True)
class ProducerBinding:
    backend_pid: int
    stream: int
    sequence: int
    row_id: str
    payload_digest: str


def parse_pg_bindings(lines):
    """Pair pre-offer records with producer acceptance, including rejected offers."""
    pending, accepted = {}, set()
    bindings = []
    for line in lines:
        match = _LOG.search(line)
        if not match:
            continue
        value = json.loads(match.group(1))
        if type(value.get('version')) is not int or value['version'] != 1:
            raise ValueError('unsupported producer trace version')
        for key in ('backend_pid', 'stream', 'offer', 'sequence'):
            if type(value.get(key)) is not int or value[key] < (0 if key == 'sequence' else 1):
                raise ValueError('invalid producer identity')
        offer = (value['backend_pid'], value['stream'], value['offer'])
        sequence = (value['backend_pid'], value['stream'], value['sequence'])
        if value.get('phase') == 'before_offer':
            if offer in pending:
                raise ValueError('duplicate producer offer')
            identity, digest = value.get('row_id'), value.get('payload_digest')
            if (not isinstance(identity, str) or not 1 <= len(identity.encode('utf-8')) <= 256
                    or not isinstance(digest, str) or not _DIGEST.fullmatch(digest)):
                raise ValueError('invalid producer row binding')
            pending[offer] = ProducerBinding(*sequence, identity, digest)
        elif value.get('phase') == 'accepted':
            binding = pending.get(offer)
            if binding is None or binding.sequence != value['sequence'] or sequence in accepted:
                raise ValueError('acceptance lacks a unique prior producer offer')
            accepted.add(sequence)
            bindings.append(binding)
        else:
            raise ValueError('unknown producer trace phase')
    return bindings


def verify_bound_map_results(inputs, predictions, bindings, events, session_events, *, plan):
    """Check one query; identical payloads remain distinct through producer row IDs."""
    inputs, predictions = text_rows_by_id(inputs), text_rows_by_id(predictions)
    if not inputs or inputs.keys() != predictions.keys() or len(bindings) != len(inputs):
        raise ValueError('incomplete non-NULL Map input/result/producer set')
    namespaces = {(r.backend_pid, r.stream) for r in bindings}
    if len(namespaces) != 1:
        raise ValueError('mixed producer queries or streams')
    by_sequence = {r.sequence: r for r in bindings}
    if len(by_sequence) != len(bindings) or set(by_sequence) != set(range(len(inputs))):
        raise ValueError('producer sequences are not unique and contiguous')
    if {r.row_id for r in bindings} != set(inputs):
        raise ValueError('producer row identity mismatch')
    for binding in bindings:
        text = inputs[binding.row_id]
        expected = semantic_payload_digest(semantic_spec_sha256=plan.digest, input_value=text,
                                           canonical_messages_utf8=canonical_messages(plan.instruction, text))
        if expected != binding.payload_digest:
            raise ValueError('producer payload does not match its independently bound row')
    tasks, completed, sessions = set(), set(), set()
    for event in events:
        if event.get('event') not in ('core_map_task', 'core_map_completion'):
            continue
        sequence, session = event.get('sequence'), event.get('session_id')
        if type(sequence) is not int or sequence not in by_sequence or type(session) is not int or session < 1:
            raise ValueError('missing or invalid gateway sequence/session')
        sessions.add(session)
        binding = by_sequence[sequence]
        if event.get('payload_digest') != binding.payload_digest:
            raise ValueError('gateway sequence/payload differs from producer binding')
        if event['event'] == 'core_map_task':
            if sequence in tasks:
                raise ValueError('duplicate request-side task')
            tasks.add(sequence)
            continue
        if sequence not in tasks or sequence in completed:
            raise ValueError('completion has no unique preceding task')
        completed.add(sequence)
        if event.get('response_model_id') != plan.model_id or event.get('finish_reason') != 'stop':
            raise ValueError('completion model or finish mismatch')
        output = predictions[binding.row_id]
        if 'raw_output' in event:
            if event['raw_output'] != output:
                raise ValueError('completion output differs from its producer row result')
        elif event.get('raw_output_sha256') != hashlib.sha256(output.encode('utf-8')).hexdigest():
            raise ValueError('completion output hash differs from its producer row result')
    if completed != set(by_sequence) or len(sessions) != 1:
        raise ValueError('missing completions or mixed gateway sessions')
    session = next(iter(sessions))
    starts = [r for r in session_events if r.get('event') == 'session_start' and r.get('session_id') == session]
    if len(starts) != 1 or starts[0].get('peer_pid') != next(iter(namespaces))[0]:
        raise ValueError('gateway socket is not bound to the observed PG backend')
    return {'matched_rows': len(inputs), 'producer_namespace': list(next(iter(namespaces))),
            'gateway_session': session, 'binding_source': 'PG before-offer row ID plus producer acceptance',
            'independent_sequence_verified': True}
