"""Bounded finite-chain acceptance and captured-result checks; not a scheduler.

Native execution stays in the existing public owner. No retries or model calls
are added by the recorded-answer reference. Input and outputs stay private.
"""
from dataclasses import asdict
import json
import math
import time
from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_chain_token_budget import ChainTokenBudget, OutputTokenEstimate, fingerprint
from src.semantic_methods.driver import SuccessorFirst
from src.semantic_methods.budget import row_reservation
from src.semantic_methods.lotus.record_chain import LotusRecordChain, StageSpec, MapMethod, FilterMethod
from src.serving.backends.vllm_engine import _prepared


def make_chain(model_name, generation):
    return LotusRecordChain((
        StageSpec('map0', MapMethod(model_name, 'Label {text}', 'label_0'), generation),
        StageSpec('map1', MapMethod(model_name, 'Label {label_0}', 'label_1'), generation),
        StageSpec('keep', FilterMethod(model_name, 'Is {label_1} positive?'), generation),
    ), query_id='current-main-MMF')


def initial_window(reservations):
    """Largest input-only W that cannot admit two initial requests together."""
    costs = sorted(reservations)
    if len(costs) < 2 or costs[-1] >= costs[0] + costs[1]:
        raise ValueError('no feasible initial W: require max(r) < sum(two smallest); no retuning')
    return costs[0] + costs[1] - 1


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
    W = initial_window(costs) if active_tokens is None else active_tokens
    if not (len(costs) >= 2 and max(costs) <= W < costs[0] + costs[1]):
        raise ValueError('input-only W gate failed; stop before model, no automatic retuning')
    return ChainTokenBudget(tokenizer, engine, active_tokens=W, **options), dict(
        active_tokens=W, initial_reserved_tokens=costs,
        rule='default W=sum(two smallest initial reservations)-1; require every initial request fits and two smallest do not; reject infeasible inputs',
        units='declared input tokens plus generation cap; not RSS, HBM or measured KV')


def run_case(records, chain, engine, resources, *, control_dir, token_budget=None,
             priority=False, actor_owner=None, execution='record_local_sections',
             progression='record', progression_batch_rows=None):
    entered = time.monotonic_ns()
    ready, core_events, driver_modes = [], [], []
    options = {} if token_budget is None else {'token_budget': token_budget}
    if priority:
        options.update(ready_order='successor_first', successor_first=SuccessorFirst(2, 1.0),
                       on_ready_event=lambda event: ready.append(asdict(event)))
    owner = SemLoomRecordChainCore(engine, resources, control_dir=control_dir,
        execution=execution, actor_owner=actor_owner, progression=progression,
        progression_batch_rows=progression_batch_rows, **options)
    constructed = time.monotonic_ns()
    primary = None
    close = None
    try:
        stream = owner.evaluate(iter(records), chain, expected=records, query_id=chain.query_id)
        evaluated = time.monotonic_ns()
        owner.core.observe_capacity_blocks = True
        def observe(kind, key):
            driver = getattr(owner._active, 'driver', None)
            if driver is not None:
                group = getattr(driver, 'completion_batch_rows', None)
                mode = dict(policy='record' if group is None else 'batch_stage', batch_rows=group)
                if mode not in driver_modes:
                    driver_modes.append(mode)
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
        close_entered = time.monotonic_ns()
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
            close['core_usage']['active_work'] != 0 or
            not len(records) <= len(calls) <= len(chain.stages) * len(records) or
            execution == 'record_local_sections' and
            driver_modes != ([{k:close['progression'][k] for k in ('policy', 'batch_rows')}] if calls else []) or
            len({c['call_id'] for c in calls}) != len(calls) or
            any(c['status'] != 'succeeded' or c['submission_confirmed'] is not True for c in calls)):
        raise ValueError('normal query/coverage/close acceptance failed')
    blocked = [e for e in core_events if e['kind'] == 'dispatch_capacity_blocked' and
        e['estimated_work'] is not None and token_budget is not None and
        e['usage']['active_requests'] < resources.max_active_batches and
        e['usage']['active_work'] + e['estimated_work'] > token_budget.active_tokens]
    recovered = [b for b in blocked if any(e['kind'] == 'submitted' and e['key'] == b['key'] and
        e['at_ns'] > b['at_ns'] for e in core_events)]
    pool = getattr(owner._active, 'pool', None)
    return dict(records=outputs, calls=calls, columns=list(stream.columns), close=close,
        execution=close['execution'], progression=close.get('progression'), resources=asdict(owner.resources),
        driver_progression=driver_modes[0] if driver_modes else None,
        capacity=None if pool is None else dict(active_request_limit=owner.core.capacity.limits.active_requests,
            held_task_limit=owner.core.capacity.limits.held_tasks,
            retained_rows=pool.capacity.runs,
            retained_bytes=pool.capacity.bytes,
            row_reservation_bytes=row_reservation(owner.method_limits)),
        lifecycle=dict(model_lifetime=('one_fresh_model_per_query' if actor_owner is None
                                      else 'supplied_model_owner_closed_by_query'),
            cross_query_model_reuse=False, ray_initialized_by_helper=False,
            adapter_uses_ray=execution == 'record_local_sections'),
        ready_events=ready, core_events=core_events, W_block_states=len(blocked),
        W_block_states_later_submitted=len(recovered),
        contended_successor_choices=sum(e['action']=='Core_offer_accepted' and
            e['reason']=='successor_first' for e in ready),
        timing=dict(caller_entered_ns=entered, owner_constructed_ns=constructed,
                    evaluate_returned_ns=evaluated, iterator_complete_ns=iterator_complete_ns,
                    caller_returned_ns=returned, full_owner_call_s=(returned-entered)/1e9,
                    construct_s=(constructed-entered)/1e9, evaluate_s=(evaluated-constructed)/1e9,
                    iterate_to_eof_s=(iterator_complete_ns-evaluated)/1e9,
                    final_close_s=(returned-close_entered)/1e9,
                    iteration_scope='includes lazy model startup in fused and automatic query close in both paths',
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

def compare(root, config, *, config_sha256=None):
    cases=[];uids=[];differences=[]
    reference=None;ref_outputs=None
    matched = config.get('suite') == 'matched-execution'
    if matched:
        from src.semantic_methods.lotus.records import RecordsResources
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
        resources = RecordsResources(**config['resources'])
        engine = json.loads(json.dumps(asdict(EngineSpec.from_dict(config['engine']))))
        generation = json.loads(json.dumps(asdict(GenerationSpec.from_dict(config['generation']))))
        if (resources.max_active_batches != 1 or resources.model_batch_rows != 1 or
                resources.prepare_batch_rows != 1 or resources.cpu_workers != 1 or resources.gpu_workers != 1 or
                [(c['case_id'], c['execution']) for c in config['cases']] !=
                [('fused-count', 'fused'), ('record-local-count', 'record_local_sections')]):
            raise ValueError('matched execution comparison requires the fixed C1/B1 pair')
    progression = config.get('suite') == 'progression'
    if progression:
        from src.semantic_methods.lotus.records import RecordsResources
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
        resources = RecordsResources(**config['resources'])
        group = config.get('progression_batch_rows')
        if (type(group) is not int or not 1 <= group <= resources.max_active_batches <= 4 or
                type(config_sha256) is not str or len(config_sha256) != 64 or
                [(c['case_id'], c.get('progression'), c.get('progression_batch_rows'), c['token'], c['successor'])
                 for c in config['cases']] !=
                [('record-count', 'record', None, False, False), ('batch-stage-count', 'batch_stage', group, False, False)]):
            raise ValueError('progression comparison requires the fixed same-C count/FIFO pair and bound config')
        engine = json.loads(json.dumps(asdict(EngineSpec.from_dict(config['engine']))))
        generation = json.loads(json.dumps(asdict(GenerationSpec.from_dict(config['generation']))))
    for case in config['cases']:
        value=json.loads((root/'cases'/case['case_id']/'case.json').read_text())
        if value['status']!='completed' or value['public_git_commit']!=config['public_git_commit']:
            raise ValueError('incomplete or wrong source case: '+case['case_id'])
        if matched:
            lifecycle = value.get('lifecycle', {})
            runtime = value.get('caller_runtime', {})
            close = value.get('close', {})
            if (value.get('execution') != case['execution'] or value.get('resources') != asdict(resources) or
                    value.get('engine') != engine or value.get('generation') != generation or
                    value.get('input_sha256') != config['input']['sha256'] or
                    lifecycle.get('model_lifetime') != 'one_fresh_model_per_query' or
                    lifecycle.get('ray_initialized_by_helper') is not False or
                    runtime.get('ray_owned') is not (case['execution'] != 'fused') or
                    any(type(runtime.get(k)) not in (int, float) or not math.isfinite(runtime[k]) or runtime[k] < 0
                        for k in ('ray_init_s', 'ray_shutdown_s')) or
                    case['execution'] == 'fused' and any(runtime[k] != 0 for k in ('ray_init_s', 'ray_shutdown_s')) or
                    close.get('status') != 'closed' or close.get('query_completed') is not True or
                    close.get('peak_core_usage', {}).get('active_requests') != 1):
                raise ValueError('matched execution configuration/lifecycle receipt differs: '+case['case_id'])
        if progression:
            close, actual, capacity = value.get('close', {}), value.get('progression', {}), value.get('capacity', {})
            driver = value.get('driver_progression', {})
            expected_mode = dict(policy=case['progression'], batch_rows=case['progression_batch_rows'])
            if (any(type(v) is not dict for v in (close, actual, capacity, driver)) or
                    any(type(mode.get(k)) is not type(v) or mode.get(k) != v
                        for mode in (actual, driver) for k, v in expected_mode.items())):
                raise ValueError('progression policy receipt differs: '+case['case_id'])
            if (value.get('case_id') != case['case_id'] or value.get('suite') != 'progression' or
                    value.get('config_sha256') != config_sha256 or value.get('input_sha256') != config['input']['sha256'] or
                    value.get('execution') != 'record_local_sections' or close.get('execution') != 'record_local_sections' or
                    value.get('resources') != asdict(resources) or value.get('engine') != engine or value.get('generation') != generation or
                    actual != close.get('progression') or
                    close.get('status') != 'closed' or close.get('query_completed') is not True or
                    close.get('cross_query_model_reuse') is not False or close.get('model_batch_rows') != 1 or
                    not close.get('core_usage') or any(close['core_usage'].values()) or
                    any(type(capacity.get(k)) is not int or capacity[k] < 1 for k in
                        ('active_request_limit', 'held_task_limit', 'retained_rows', 'retained_bytes', 'row_reservation_bytes'))):
                raise ValueError('progression source/config/policy/capacity receipt differs: '+case['case_id'])
            if (capacity['active_request_limit'] != resources.max_active_batches or
                    capacity['held_task_limit'] != resources.max_active_batches or
                    capacity['retained_rows'] != resources.max_active_batches or
                    capacity['retained_bytes'] != capacity['retained_rows'] * capacity['row_reservation_bytes'] or
                    group > min(capacity['held_task_limit'], capacity['retained_rows'],
                                capacity['retained_bytes'] // capacity['row_reservation_bytes']) or
                    any(type(close.get('peak_core_usage', {}).get(k)) is not int or
                        not 1 <= close['peak_core_usage'][k] <= capacity[limit]
                        for k, limit in [('active_requests', 'active_request_limit'), ('held_tasks', 'held_task_limit')])):
                raise ValueError('progression group or observed usage exceeds held capacity: '+case['case_id'])
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
        if matched:
            cases[-1].update(execution=value['execution'], resources=value['resources'],
                lifecycle=value['lifecycle'], caller_runtime=value['caller_runtime'], owner_timing=value['timing'])
        if progression:
            cases[-1].update(progression=actual, resources=value['resources'], capacity=capacity)
    if len(set(uids))!=len(config['cases']):raise ValueError('independent case owners reused a native engine identity')
    identical=all(not d['changed_calls'] and d['final_outputs_identical'] for d in differences)
    report = dict(status='captured_receipts_compared',public_git_commit=config['public_git_commit'],cases=cases,
        outputs_strictly_comparable=identical,differences=differences,
        performance_qualification='not established: n1 small functional smoke even if outputs identical',
        mechanism_gate='token needs observed W block and later submission; successor needs contended choice; absent means not exercised, no automatic rerun')
    if matched:
        report.update(comparison_scope='same C1/B1 resources and fresh model per query; complete backend paths',
            isolated_granularity_causality_or_stable_speedup_proven=False,
            remaining_confounds=['synchronous fused versus asynchronous record native API',
                'caller Ray runtime is required only by record path and remains included in worker time',
                'one small pair; cache state and GPU batching are not matched by this check'])
    if progression:
        report.update(suite='progression', comparison_scope='same C, input/model/generation and fresh-query async backend',
            mechanism_gate='driver policy and capacity binding checked; group-stage timing and physical exits require separate evidence')
    return report
