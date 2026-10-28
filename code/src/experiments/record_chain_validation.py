"""Bounded finite-chain acceptance and captured-result checks; not a scheduler.

Native execution stays in the existing public owner. No retries or model calls
are added by the recorded-answer reference. Input and outputs stay private.
"""
from dataclasses import asdict
import json
import time
from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_chain_token_budget import ChainTokenBudget, OutputTokenEstimate, fingerprint
from src.semantic_methods.driver import SuccessorFirst
from src.semantic_methods.lotus.record_chain import LotusRecordChain, StageSpec, MapMethod, FilterMethod
from src.serving.backends.vllm_engine import _prepared


def make_chain(model_name, generation):
    return LotusRecordChain((
        StageSpec('map0', MapMethod(model_name, 'Label {text}', 'label_0'), generation),
        StageSpec('map1', MapMethod(model_name, 'Label {label_0}', 'label_1'), generation),
        StageSpec('keep', FilterMethod(model_name, 'Is {label_1} positive?'), generation),
    ), query_id='current-main-MMF')


def make_budget(tokenizer, engine, chain, records, active_tokens=None):
    estimates = {s.stage_id: OutputTokenEstimate(s.generation.max_tokens,
        'declared generation cap for functional acceptance; not measured output') for s in chain.stages}
    options = dict(tokenizer_signature=fingerprint(tokenizer.backend_tokenizer.to_str()),
                   template_signature=fingerprint(tokenizer.chat_template), expected_output=estimates)
    # Input-only qualification, before GPU startup; no observed outputs tune W.
    probe = ChainTokenBudget(tokenizer, engine, active_tokens=engine.max_model_len, **options)
    run = chain.start_batch(records)
    if not run.prepare():
        raise ValueError('nonempty first stage required')
    costs = sorted(probe.estimate(row, chain.stages[0].generation)['reserved_tokens'] for row in run.prepared)
    W = 2 * costs[0] - 1 if active_tokens is None else active_tokens
    if not (len(costs) >= 2 and max(costs) <= W < costs[0] + costs[1]):
        raise ValueError('input-only W gate failed; stop before model, no automatic retuning')
    return ChainTokenBudget(tokenizer, engine, active_tokens=W, **options), dict(
        active_tokens=W, initial_reserved_tokens=costs,
        rule='W=2*minimum_initial_reservation-1; require every initial request fits and two smallest do not',
        units='declared input tokens plus generation cap; not RSS, HBM or measured KV')


def run_case(records, chain, engine, resources, *, control_dir, token_budget=None,
             priority=False, actor_owner=None):
    ready, core_events = [], []
    options = {} if token_budget is None else {'token_budget': token_budget}
    if priority:
        options.update(ready_order='successor_first', successor_first=SuccessorFirst(2, 1.0),
                       on_ready_event=lambda event: ready.append(asdict(event)))
    owner = SemLoomRecordChainCore(engine, resources, control_dir=control_dir,
        execution='record_local_sections', actor_owner=actor_owner, **options)
    entered = time.monotonic_ns()
    primary = None
    close = None
    try:
        stream = owner.evaluate(iter(records), chain, expected=records, query_id=chain.query_id)
        owner.core.observe_capacity_blocks = True
        def observe(kind, key):
            record = owner.core.capacity.records.get(key)
            core_events.append(dict(at_ns=time.monotonic_ns(), kind=kind, key=asdict(key),
                usage=asdict(owner.core.capacity.usage()),
                estimated_work=None if record is None else record.task.estimated_work))
        owner.core.sink = observe
        outputs = list(stream)
        iterator_complete_ns = time.monotonic_ns()
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            close = owner.close(timeout=30)
        except BaseException as cleanup:
            if primary is None:
                raise
            primary.add_note('owner close: ' + type(cleanup).__name__)
        finally:
            if primary is not None:
                primary.chain_calls = list(owner.calls)
                primary.chain_close = close
    returned = time.monotonic_ns()
    calls = list(owner.calls)
    if (close['status'] != 'closed' or not close['query_completed'] or
            close['core_usage']['active_work'] != 0 or len(calls) != 3 * len(records) or
            len({c['call_id'] for c in calls}) != len(calls) or
            any(c['status'] != 'succeeded' or c['submission_confirmed'] is not True for c in calls)):
        raise ValueError('normal query/coverage/close acceptance failed')
    blocked = [e for e in core_events if e['kind'] == 'dispatch_capacity_blocked' and
        e['estimated_work'] is not None and token_budget is not None and
        e['usage']['active_requests'] < resources.max_active_batches and
        e['usage']['active_work'] + e['estimated_work'] > token_budget.active_tokens]
    recovered = [b for b in blocked if any(e['kind'] == 'submitted' and e['key'] == b['key'] and
        e['at_ns'] > b['at_ns'] for e in core_events)]
    return dict(records=outputs, calls=calls, columns=list(stream.columns), close=close,
        ready_events=ready, core_events=core_events, W_block_states=len(blocked),
        W_block_states_later_submitted=len(recovered),
        contended_successor_choices=sum(e['action']=='Core_offer_accepted' and
            e['reason']=='successor_first' for e in ready),
        timing=dict(caller_entered_ns=entered, iterator_complete_ns=iterator_complete_ns,
                    caller_returned_ns=returned, full_owner_call_s=(returned-entered)/1e9,
                    iterator_complete_scope='not claimed earliest model result readiness'),
        default_ready_options_omitted=not priority)


def replay_native_answers(records, chain, result, tokenizer=None):
    """Original method reference with recorded native answers, zero new calls."""
    by_id = {c['call_id']: c for c in result['calls']}
    seen = set()
    def infer(prepared, *, generation):
        identities, conversations = _prepared(prepared)
        answers = []
        for identity, conversation in zip(identities, conversations):
            key = identity['call_id']
            if key in seen:
                raise ValueError('reference consumed a call more than once')
            call = by_id[key]
            if any(call.get(k) != v for k, v in identity.items()):
                raise ValueError('native call identity/messages differ from method reference')
            stage = next(s for s in chain.stages if s.stage_id == identity['algorithm_stage'])
            if generation != stage.generation:
                raise ValueError('reference generation differs')
            if tokenizer is not None:
                ids = tokenizer.apply_chat_template(conversation, tokenize=True,
                    add_generation_prompt=True, return_dict=False)
                if ids != json.loads(call['prompt_token_ids_json']):
                    raise ValueError('driver tokenizer/template differs from native prompt IDs')
            seen.add(key); answers.append(call)
        return answers
    expected = chain.run_batch(records, infer)
    if set(by_id) != seen or [r.describe() for r in expected['records']] != [r.describe() for r in result['records']]:
        raise ValueError('reference final records differ')
    actual = {c['call_id']:c for c in result['calls']}
    for call in expected['calls']:
        for field in ('parent_call_ids', 'decision'):
            if call.get(field) != actual[call['call_id']].get(field):
                raise ValueError('reference parent or Filter decision differs')
    return dict(status='passed', recorded_answers_replayed=len(seen), model_calls=0,
                native_prompt_ids_checked=tokenizer is not None)


FIELDS=['query_id','model_role','algorithm_stage','call_id','row_id','source_position','call_index',
        'messages_sha256','prompt_token_ids_json','output_token_ids_json','text','finish_reason',
        'prompt_tokens','completion_tokens','total_tokens','parent_call_ids','decision','status','submission_confirmed']

def compare(root, config):
    cases=[];uids=[];differences=[]
    reference=None;ref_outputs=None
    for case in config['cases']:
        value=json.loads((root/'cases'/case['case_id']/'case.json').read_text())
        if value['status']!='completed' or value['public_git_commit']!=config['public_git_commit']:
            raise ValueError('incomplete or wrong source case: '+case['case_id'])
        calls=value['calls'];by_id={c['call_id']:{f:c.get(f) for f in FIELDS} for c in calls}
        if len(calls)!=48 or len(by_id)!=48:raise ValueError('call coverage differs')
        if any(type(c.get('engine_uid')) is not str or not c['engine_uid'].strip() or c['engine_uid']!=c['engine_uid'].strip() for c in calls):
            raise ValueError('every native engine identity must be a nonempty string')
        engine_uids={c['engine_uid'] for c in calls}
        if len(engine_uids)!=1:raise ValueError('case must have exactly one observed native engine')
        uids.extend(engine_uids)
        for c in calls:
            inp,out=json.loads(c['prompt_token_ids_json']),json.loads(c['output_token_ids_json'])
            if (c['status']!='succeeded' or c['submission_confirmed'] is not True or
                c['finish_reason'] not in ['stop','length'] or c['prompt_tokens']!=len(inp) or
                c['completion_tokens']!=len(out) or c['total_tokens']!=len(inp)+len(out)):
                raise ValueError('native usage or terminal receipt inconsistent')
        if reference is None:reference=by_id;ref_outputs=value['outputs']
        if set(by_id)!=set(reference):raise ValueError('logical identities changed')
        changed=[dict(call_id=k,fields=[f for f in FIELDS if by_id[k][f]!=reference[k][f]])
                 for k in reference if by_id[k]!=reference[k]]
        differences.append(dict(case_id=case['case_id'],changed_calls=changed,
                               final_outputs_identical=value['outputs']==ref_outputs))
        cases.append(dict(case_id=case['case_id'],calls=len(calls),function='passed',
            W_exercised=value['W_exercised'],successor_contention_exercised=value['successor_contention_exercised'],
            full_owner_call_s=value['timing']['full_owner_call_s'],worker_wall_s=value['worker_wall_before_final_write_s'],
            timing_scope='owner vs worker including imports/input/CPU tokenizer/Ray/replay/close; final report write excluded',
            physical_cleanup='separate external supervisor receipt required'))
    if len(set(uids))!=4:raise ValueError('independent case owners reused a native engine identity')
    identical=all(not d['changed_calls'] and d['final_outputs_identical'] for d in differences)
    return dict(status='captured_receipts_compared',public_git_commit=config['public_git_commit'],cases=cases,
        outputs_strictly_comparable=identical,differences=differences,
        performance_qualification='not established: n1 small functional smoke even if outputs identical',
        mechanism_gate='token needs observed W block and later submission; successor needs contended choice; absent means not exercised, no automatic rerun')
