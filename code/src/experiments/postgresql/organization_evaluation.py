"""Check observed token work, finite groups and actual single-Map request dispatch."""
from collections import Counter
from dataclasses import asdict

from src.execution_provider.adapters.map_organization import MapOrganizationConfig


def verify_organization(config, events):
    """Audit a completed query; preparation retries are allowed only for identical tasks."""
    if not isinstance(config, MapOrganizationConfig):
        raise ValueError('typed organization configuration required')
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
        return value['session_id'],value['sequence']

    submitted = [e for e in events if e['event']=='core_submitted']
    http = [e for e in events if e['event']=='core_http_started']
    if (len(submitted)!=len(descriptions) or len({key(e['key']) for e in submitted})!=len(submitted)
            or Counter(key(e['key']) for e in submitted)!=Counter(key(e['key']) for e in http)):
        raise ValueError('submitted and HTTP task identities differ')
    groups = {}
    for event in submitted:
        sequence = event['key']['sequence']
        member = event.get('member')
        if (sequence not in descriptions or event.get('estimated_work')!=descriptions[sequence]['estimated_work']
                or event.get('work_unit')!='tokens' or not member):
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
        group.sort(key=lambda e:e['member']['index'])
        if any(e['member']['size']!=len(group) or e['member']['index']!=i for i,e in enumerate(group)):
            raise ValueError('incomplete organization batch')
        actual_groups.append(tuple(key(e['key']) for e in group))
    if Counter(actual_groups)!=Counter(selected_groups):
        raise ValueError('selected and dispatched batch memberships differ')
    usages = [e['usage'] for e in events if 'usage' in e]
    peak = max((u['active_work'] for u in usages),default=0)
    if peak>config.active_work or any(u['active_work']<0 for u in usages):
        raise ValueError('active token work violates fixed capacity')
    order=[e['key']['sequence'] for e in submitted]
    return dict(mode=config.mode,rows=len(descriptions),batches=len(groups),prepare_attempts=prepare_attempts,
                peak_active_work=peak,active_work_limit=config.active_work,
                max_candidate_rows=max((len(e['candidates']) for e in choices),default=0),
                submitted_sequences=order,
                max_later_rows_submitted_first=max((sum(other>sequence for other in order[:i])
                                                    for i,sequence in enumerate(order)),default=0),
                http_sequences=[e['key']['sequence'] for e in http],
                batch_sequences=[[sequence for _,sequence in group] for group in actual_groups],
                server_prompt_tokens_verified=True,calibration_signature=signature)
