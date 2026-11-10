"""Replay bounded ordering on immutable captured requests, without an engine."""
from collections import deque
import json
import time

from src.infrastructure.private_artifacts import content_digest
from src.scheduling.core.session_contract import TaskKey
from src.scheduling.organization.prepared_order import (
    PreparedRequestOrder, RequestOrganizationProfile, order_prepared_window,
)
from src.serving.backends.vllm_protocol import IDENTITY_FIELDS


def _common_prefix(rows):
    if not rows:
        return 0
    common = len(rows[0])
    for row in rows[1:]:
        common = min(common, len(row))
        for i in range(common):
            if rows[0][i] != row[i]:
                common = i
                break
    return common


def replay(capture, *, candidate_limit, group_requests, prefix_token_limit, block_tokens, repeats=3):
    if (capture.get('schema') != 'semloom.semantic_request_capture.v1'
            or type(capture.get('requests')) is not list):
        raise ValueError('an original semantic request capture is required')
    config = PreparedRequestOrder('prefix_length', candidate_limit, prefix_token_limit, block_tokens)
    if (type(group_requests) is not int or not 1 <= group_requests <= candidate_limit
            or type(repeats) is not int or repeats < 1):
        raise ValueError('the selected group must fit the finite ready window and repeats must be positive')
    requests = capture['requests']
    digest = content_digest([dict(row={name: item['row'][name] for name in (*IDENTITY_FIELDS, 'messages_json')},
                                  generation=item['generation'], token_ids=item['token_ids']) for item in requests])
    if digest != capture.get('request_digest'):
        raise ValueError('captured request content no longer matches its recorded digest')
    call_ids = [item['row']['call_id'] for item in requests]
    if len(set(call_ids)) != len(call_ids):
        raise ValueError('captured occurrences must retain distinct call identities')
    profiles = []
    for i, item in enumerate(requests):
        ids, generation = item['token_ids'], item['generation']
        if (type(ids) is not list or not ids or any(type(v) is not int or v < 0 for v in ids)
                or item.get('prompt_tokens') != len(ids) or type(generation.get('max_tokens')) is not int
                or generation['max_tokens'] < 1):
            raise ValueError('capture contains inconsistent complete tokens or generation cap')
        row = item['row']
        compatibility = tuple(row[name] for name in ('query_id', 'algorithm_stage', 'model_role')) + (
            json.dumps(generation, sort_keys=True),)
        profiles.append(RequestOrganizationProfile(TaskKey(0, i), compatibility, len(ids),
                                                   generation['max_tokens'], tuple(ids[:prefix_token_limit])))
    arms = []
    for mode in ('fifo', 'length', 'prefix', 'prefix_length'):
        config = None if mode == 'fifo' else PreparedRequestOrder(mode, candidate_limit, prefix_token_limit, block_tokens)
        durations = []
        for _ in range(repeats):
            remaining, ready, groups = deque(profiles), [], []
            elapsed = 0
            while remaining or ready:
                while remaining and len(ready) < candidate_limit:
                    ready.append(remaining.popleft())
                started = time.perf_counter_ns()
                ordered = tuple(p.key for p in ready) if config is None else order_prepared_window(ready, config)
                by_key = {p.key: p for p in ready}
                anchor = by_key[ordered[0]].compatibility
                selected = []
                for key in ordered:
                    if by_key[key].compatibility != anchor or len(selected) == group_requests:
                        break
                    selected.append(key.sequence)
                selected_set = set(selected)
                ready = [p for p in ready if p.key.sequence not in selected_set]
                elapsed += time.perf_counter_ns() - started
                groups.append(selected)
            durations.append(elapsed)
        order = [i for group in groups for i in group]
        if sorted(order) != list(range(len(requests))):
            raise ValueError('replay lost or duplicated a captured occurrence')
        common = [_common_prefix([requests[i]['token_ids'] for i in group]) for group in groups]
        arms.append(dict(mode=mode, selected_groups=groups, cached_profile_choice_ns=durations,
            group_common_prefix_tokens=common,
            within_group_shared_full_block_tokens=sum((len(group)-1) * (prefix // block_tokens) * block_tokens
                for group, prefix in zip(groups, common)),
            adjacent_shared_full_block_tokens=sum((_common_prefix([requests[a]['token_ids'], requests[b]['token_ids']])
                // block_tokens) * block_tokens for a, b in zip(order, order[1:])),
            same_occurrences=True))
    return dict(schema='semloom.prepared_order_replay.v1', source_type='captured_requests_with_simulated_ready_window',
        request_digest=digest, requests=len(requests), complete_initial_operator=capture.get('complete_initial_operator'),
        candidate_limit=candidate_limit, group_requests=group_requests, prefix_token_limit=prefix_token_limit,
        block_tokens_hypothesis=block_tokens, repeats=repeats, arms=arms, model_calls=0,
        scope='Exact complete-request membership and content; synthetic readiness and application groups. Shared content is not KV hits, native batch membership, cascade work saved, or a speedup.',
        timing_scope='Choices over cached profiles only; excludes formatter, tokenization, frame sizing, I/O and GPU work.')
