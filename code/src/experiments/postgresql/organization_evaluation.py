"""Check observed token work, finite groups and actual single-Map request dispatch."""
from collections import Counter
from dataclasses import asdict

from src.execution_provider.adapters.map_organization import MapOrganizationConfig


def replay_active_work(events, task_work, *, active_work, max_active_requests=None, capacity_observation_version=None):
    """Reconstruct compute reservations; delivery/HTTP/cancel never release them.

    Scope is the current single-Map, no-retry query profile. The terminal event
    comes from Core after the backend's authoritative terminal has been consumed.
    An uncertain submission remains charged until that event, even after HTTP
    await ends. This is an offline oracle, not another production capacity table.
    """
    if max_active_requests is not None and (type(max_active_requests) is not int or max_active_requests < 1):
        raise ValueError('positive request capacity required for lifecycle audit')
    if capacity_observation_version not in (None,1):
        raise ValueError('unsupported capacity observation version')
    if capacity_observation_version == 1 and max_active_requests is None:
        raise ValueError('capacity blocking audit requires the declared request limit')
    blocked_counts={name:dict(work=0,requests=0,both=0) for name in (
        'core_flow_capacity_blocked','core_dispatch_capacity_blocked')}
    active = {}
    submitted, terminal = set(), set()
    peak_work = peak_requests = snapshots = drained = uncertain = 0
    for event in events:
        kind = event['event']
        if kind in ('core_submitted', 'core_terminal', 'core_uncertain'):
            current = (event['key']['session_id'], event['key']['sequence'])
            if current not in task_work:
                raise ValueError('compute event has no described task identity')
            if kind == 'core_submitted':
                if current in submitted or drained:
                    raise ValueError('duplicate or post-drain compute submission')
                active[current] = task_work[current]
                submitted.add(current)
            elif kind == 'core_terminal':
                if current not in active:
                    raise ValueError('authoritative terminal has no active responsibility')
                del active[current]
                terminal.add(current)
            else:
                if current not in active:
                    raise ValueError('uncertain task has no active responsibility')
                uncertain += 1
        work = sum(active.values())
        count = len(active)
        if work > active_work or (max_active_requests is not None and count > max_active_requests):
            raise ValueError('reconstructed active work/request capacity exceeded')
        peak_work, peak_requests = max(peak_work, work), max(peak_requests, count)
        if kind in blocked_counts:
            current=(event['key']['session_id'],event['key']['sequence'])
            if capacity_observation_version!=1 or current not in task_work or current in submitted:
                raise ValueError('capacity refusal is not a declared pending task observation')
            work_blocked=work+task_work[current]>active_work
            request_blocked=count>=max_active_requests
            if not work_blocked and not request_blocked:
                raise ValueError('capacity refusal has no reconstructed limiting resource')
            reason='both' if work_blocked and request_blocked else 'work' if work_blocked else 'requests'
            blocked_counts[kind][reason]+=1
        mandatory = kind in ('core_submitted', 'core_terminal', 'core_uncertain', 'core_job_drained',*blocked_counts)
        usage = event.get('usage')
        if mandatory and usage is None:
            raise ValueError('missing compute transition usage observation')
        if usage is not None:
            if (not isinstance(usage, dict) or type(usage.get('active_work')) is not int
                    or type(usage.get('active_requests')) is not int
                    or usage['active_work'] != work or usage['active_requests'] != count):
                raise ValueError('usage differs from reconstructed compute responsibility')
            snapshots += 1
        if kind == 'core_job_drained':
            if active or drained or submitted != set(task_work) or terminal != submitted:
                raise ValueError('job drain precedes complete authoritative settlement')
            drained += 1
    if active or submitted != set(task_work) or terminal != submitted or drained != 1:
        raise ValueError('incomplete authoritative compute lifecycle evidence')
    request_only_bound = (sum(sorted(task_work.values(), reverse=True)[:max_active_requests])
                          if max_active_requests is not None else None)
    return dict(peak_active_work=peak_work, peak_active_requests=peak_requests,
                final_active_work=0, final_active_requests=0, snapshots_checked=snapshots,
                submissions=len(submitted), authoritative_terminals=len(terminal), uncertain_events=uncertain,
                request_capacity=max_active_requests, request_only_work_upper_bound=request_only_bound,
                work_limit_can_bind=(request_only_bound > active_work if request_only_bound is not None else None),
                work_limit_binding_observed=(sum(v['work']+v['both'] for v in blocked_counts.values())>0
                                             if capacity_observation_version==1 else None),
                work_only_block_count=(sum(v['work'] for v in blocked_counts.values())
                                       if capacity_observation_version==1 else None),
                dispatch_work_block_count=(blocked_counts['core_dispatch_capacity_blocked']['work']+
                                           blocked_counts['core_dispatch_capacity_blocked']['both']
                                           if capacity_observation_version==1 else None),
                capacity_block_counts=blocked_counts if capacity_observation_version==1 else None,
                binding_observation_scope=('failed flow eligibility checks and selected-member dispatch checks; '
                                           'counts include repeated probes, not blocked wall time'
                                           if capacity_observation_version==1 else
                                           'capacity feasibility only; trace has no declared dispatch-refusal observation'))


def verify_organization(config, events, *, max_active_requests=None, expected_max_new_tokens=None):
    """Audit a completed query; preparation retries are allowed only for identical tasks."""
    if not isinstance(config, MapOrganizationConfig):
        raise ValueError('typed organization configuration required')
    if expected_max_new_tokens is not None and (type(expected_max_new_tokens) is not int or expected_max_new_tokens<1):
        raise ValueError('positive declared generation budget required')
    identity = asdict(config)
    identity.pop('tokenizer_path')
    records = [e for e in events if e['event']=='core_map_organization_config']
    if len(records)!=1 or any(records[0].get(k)!=v for k,v in identity.items()):
        raise ValueError('observed organization configuration differs')
    signature = config.calibration_signature
    if records[0].get('calibration_signature')!=signature:
        raise ValueError('work calibration identity differs from the configuration')
    descriptions, prepare_attempts = {}, 0
    for event in events:
        if event['event']!='core_map_work_described':
            continue
        prepare_attempts += 1
        sequence = event['sequence']
        values = {k:event[k] for k in ('semantic_payload_digest','request_sha256','prompt_tokens',
                  'max_new_tokens','estimated_work','work_unit','calibration_signature')}
        prompt, output = values['prompt_tokens'], values['max_new_tokens']
        if (type(sequence) is not int or sequence<0 or type(prompt) is not int or prompt<1
                or type(output) is not int or output<1 or prompt+output>config.context_tokens
                or values['estimated_work']!=prompt+output or values['work_unit']!='tokens'
                or values['calibration_signature']!=signature):
            raise ValueError('invalid complete-request token work')
        if expected_max_new_tokens is not None and output!=expected_max_new_tokens:
            raise ValueError('work description differs from the declared generation budget')
        if sequence in descriptions and descriptions[sequence]!=values:
            raise ValueError('preparation retry changed a task')
        descriptions[sequence]=values
    accepted = [e for e in events if e['event']=='core_map_task']
    if len(accepted)!=len(descriptions) or {e['sequence'] for e in accepted}!=set(descriptions):
        raise ValueError('described work and accepted tasks differ')
    if any(e['payload_digest']!=descriptions[e['sequence']]['semantic_payload_digest'] for e in accepted):
        raise ValueError('work estimate is bound to a different payload')
    requests = [e for e in events if e['event']=='request']
    if Counter(e['request_bytes_sha256'] for e in requests)!=Counter(d['request_sha256'] for d in descriptions.values()):
        raise ValueError('described request bytes differ from actual POSTs')
    completions = [e for e in events if e['event']=='core_map_completion']
    if (len(completions)!=len(descriptions) or {e['sequence'] for e in completions}!=set(descriptions)
            or any(e['prompt_tokens']!=descriptions[e['sequence']]['prompt_tokens'] for e in completions)):
        raise ValueError('server prompt usage differs from the declared tokenizer')

    def key(value):
        if (not isinstance(value,dict) or any(type(value.get(k)) is not int or value[k]<0
                                            for k in ('session_id','sequence'))):
            raise ValueError('invalid organization task/batch identity')
        return value['session_id'],value['sequence']

    submitted = [e for e in events if e['event']=='core_submitted']
    http = [e for e in events if e['event']=='core_http_started']
    if (len(submitted)!=len(descriptions) or len({key(e['key']) for e in submitted})!=len(submitted)
            or Counter(key(e['key']) for e in submitted)!=Counter(key(e['key']) for e in http)):
        raise ValueError('submitted and HTTP task identities differ')
    if len({key(e['key'])[0] for e in submitted}) > 1:
        raise ValueError('organization audit requires one Map session')
    groups = {}
    for event in submitted:
        sequence = event['key']['sequence']
        member = event.get('member')
        if (sequence not in descriptions or event.get('estimated_work')!=descriptions[sequence]['estimated_work']
                or event.get('work_unit')!='tokens' or not member
                or type(member.get('index')) is not int or type(member.get('size')) is not int
                or key(member['batch_key'])[0] != key(event['key'])[0]):
            raise ValueError('dispatch lost token work or batch membership')
        groups.setdefault(key(member['batch_key']),[]).append(event)
    choices = [e for e in events if e['event']=='core_map_organized']
    queued = []
    for event in events:
        if event['event']=='core_offer' and event['accepted_prefix_count']==1:
            queued.append(key(event['key']))
        elif event['event']=='core_submitted':
            current = key(event['key'])
            if current not in queued:
                raise ValueError('dispatch precedes accepted ownership')
            queued.remove(current)
        elif event['event']=='core_map_organized':
            visible = event['candidates']
            if ([key(c['key']) for c in visible]!=queued[:config.window_rows]
                    or any(c['key']['sequence'] not in descriptions or c['work']!=
                           descriptions[c['key']['sequence']]['estimated_work'] for c in visible)):
                raise ValueError('organization candidates differ from the accepted queued prefix')
    if queued:
        raise ValueError('accepted tasks were never dispatched')
    selected_groups = []
    for event in choices:
        candidates, selected = event['candidates'],event['selected']
        if (not 0<len(candidates)<=config.window_rows or not 0<len(selected)<=config.batch_rows
                or event['mode']!=config.mode or event['calibration_signature']!=signature):
            raise ValueError('organization exceeded its finite candidate/row limit')
        work = {key(c['key']):c['work'] for c in candidates}
        if len(work)!=len(candidates) or len(set(map(key,selected)))!=len(selected):
            raise ValueError('duplicate organization candidate/member')
        expected = sorted(candidates,key=lambda c:c['work']) if config.mode=='length' else candidates
        expected = expected[:config.batch_rows]
        chosen=[]; total=0
        for candidate in expected:
            if chosen and config.mode!='rows' and total+candidate['work']>config.batch_work:
                break
            chosen.append(candidate['key']);total+=candidate['work']
        if selected!=chosen or any(key(k) not in work for k in selected):
            raise ValueError('observed grouping differs from configured control')
        selected_groups.append(tuple(map(key,selected)))
    actual_groups=[]
    for group in groups.values():
        if (any(e['member']['size']!=len(group) for e in group)
                or {e['member']['index'] for e in group} != set(range(len(group)))):
            raise ValueError('incomplete organization batch')
        if [e['member']['index'] for e in group] != list(range(len(group))):
            raise ValueError('actual group submission order differs from member indices')
        actual_groups.append(tuple(key(e['key']) for e in group))
    if actual_groups != selected_groups:
        raise ValueError('selected and dispatched batch memberships/order differ')
    if [key(e['key']) for e in submitted] != [member for group in selected_groups for member in group]:
        raise ValueError('actual submission order differs from organization choices')
    lifecycle = replay_active_work(events,
        {key(e['key']):descriptions[e['key']['sequence']]['estimated_work'] for e in submitted},
        active_work=config.active_work,max_active_requests=max_active_requests,
        capacity_observation_version=records[0].get('capacity_observation_version'))
    order=[e['key']['sequence'] for e in submitted]
    return dict(audit_schema='semloom.map_organization_audit.v2',
                mode=config.mode,rows=len(descriptions),batches=len(groups),prepare_attempts=prepare_attempts,
                peak_active_work=lifecycle['peak_active_work'],active_work_limit=config.active_work,
                compute_lifecycle=lifecycle,
                submitted_http_same_order=[key(e['key']) for e in submitted]==[key(e['key']) for e in http],
                http_order_scope='transport-start order observed separately; not required to equal Core submission order',
                max_candidate_rows=max((len(e['candidates']) for e in choices),default=0),
                submitted_sequences=order,
                max_later_rows_submitted_first=max((sum(other>sequence for other in order[:i])
                                                    for i,sequence in enumerate(order)),default=0),
                http_sequences=[e['key']['sequence'] for e in http],
                batch_sequences=[[sequence for _,sequence in group] for group in actual_groups],
                server_prompt_tokens_verified=True,declared_generation_budget_verified=expected_max_new_tokens is not None,
                calibration_signature=signature)
