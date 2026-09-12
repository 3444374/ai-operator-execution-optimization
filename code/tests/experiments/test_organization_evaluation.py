"""Reject altered token, visibility, membership and actual HTTP evidence."""
from copy import deepcopy
from dataclasses import asdict
import unittest

from src.execution_provider.adapters.map_organization import MapOrganizationConfig
from src.experiments.postgresql.organization_evaluation import verify_organization,replay_active_work


def evidence(*, works=(10,20), active_work=64, context=64):
    config=MapOrganizationConfig('rows',2,2,32,active_work,'model','r1','s1','fixture','a'*64,context)
    fields=asdict(config);fields.pop('tokenizer_path')
    events=[dict(event='core_map_organization_config',**fields,calibration_signature=config.calibration_signature)]
    for i,work in enumerate(works):
        prompt=work-8
        events.extend([
            dict(event='core_map_work_described',sequence=i,semantic_payload_digest=str(i)*64,
                 request_sha256=str(i+2)*64,prompt_tokens=prompt,max_new_tokens=8,estimated_work=prompt+8,
                 work_unit='tokens',calibration_signature=config.calibration_signature),
            dict(event='core_offer',key=dict(session_id=0,sequence=i),accepted_prefix_count=1),
            dict(event='core_map_task',sequence=i,payload_digest=str(i)*64),
        ])
    keys=[dict(session_id=0,sequence=i) for i in range(2)]
    events.append(dict(event='core_map_organized',mode='rows',calibration_signature=config.calibration_signature,
                       candidates=[dict(key=k,work=w) for k,w in zip(keys,works)],selected=keys))
    for i in range(2):
        events.extend([
            dict(event='core_submitted',key=keys[i],estimated_work=works[i],work_unit='tokens',
                 member=dict(batch_key=dict(session_id=0,sequence=0),index=i,size=2),
                 usage=dict(active_work=sum(works[:i+1]),active_requests=i+1)),
            dict(event='core_http_started',key=keys[i]),
            dict(event='request',request_bytes_sha256=str(i+2)*64),
        ])
    for i in range(2):
        usage=dict(active_work=sum(works[i+1:]),active_requests=1-i)
        events.extend([
            dict(event='core_http_finished',key=keys[i]),
            dict(event='core_terminal',key=keys[i],usage=dict(usage)),
            dict(event='core_map_completion',sequence=i,prompt_tokens=works[i]-8),
            dict(event='core_released',key=keys[i],usage=dict(usage)),
        ])
    events.append(dict(event='core_job_drained',usage=dict(active_work=0,active_requests=0)))
    return config,events


class OrganizationEvaluationTests(unittest.TestCase):
    def test_empty_lazy_open_requires_independent_zero_task_count(self):
        config, events = evidence()
        empty = events[:1]
        report = verify_organization(config, empty, expected_task_count=0)
        self.assertEqual(report['rows'], 0)
        self.assertEqual(report['compute_lifecycle']['submissions'], 0)
        self.assertEqual(report['compute_lifecycle']['snapshots_checked'], 0)
        with self.assertRaisesRegex(ValueError, 'lifecycle evidence'):
            verify_organization(config, empty + [dict(event='core_job_opened')], expected_task_count=0)
        report = verify_organization(config, empty + [dict(event='core_job_opened'), events[-1]],
                                     expected_task_count=0)
        self.assertEqual(report['rows'], 0)
        for expected in (None, 1, -1, True):
            with self.subTest(expected=expected), self.assertRaises(ValueError):
                verify_organization(config, empty, expected_task_count=expected)

    def test_expected_zero_cannot_hide_tasks_requests_or_compute(self):
        config, events = evidence()
        for extra in ([events[1]], [next(e for e in events if e['event'] == 'request')],
                      [next(e for e in events if e['event'] == 'core_submitted')],
                      [dict(event='unexpected', usage=dict(active_work=1, active_requests=1))]):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                verify_organization(config, events[:1] + extra, expected_task_count=0)
        with self.assertRaises(ValueError):
            verify_organization(config, events, expected_task_count=0)
        with self.assertRaises(ValueError):
            verify_organization(config, events[:-1], expected_task_count=2)

    def test_complete_trace_and_consistent_preparation_retry(self):
        config,events=evidence()
        events.insert(2,deepcopy(events[1]))
        report=verify_organization(config,events)
        self.assertEqual(report['rows'],2)
        self.assertEqual(report['batches'],1)
        self.assertEqual(report['prepare_attempts'],3)
        self.assertEqual(report['submitted_sequences'],[0,1])

    def test_mutated_evidence_cannot_pass(self):
        mutations=(
            ('core_map_work_described','estimated_work',9),
            ('core_map_completion','prompt_tokens',3),
            ('request','request_bytes_sha256','f'*64),
            ('core_map_organized','candidates',[dict(key=dict(session_id=0,sequence=1),work=20)]),
            ('core_map_organized','selected',[dict(session_id=0,sequence=1)]),
            ('core_submitted','member',dict(batch_key=dict(session_id=0,sequence=0),index=0,size=3)),
            ('core_submitted','usage',dict(active_work=65)),
            ('core_map_organization_config','calibration_signature','invented'),
        )
        for name,field,value in mutations:
            with self.subTest(event=name,field=field):
                config,events=evidence()
                next(e for e in events if e['event']==name)[field]=value
                with self.assertRaises(ValueError):verify_organization(config,events)

    def test_missing_runtime_snapshots_cannot_be_replaced_by_final_zero(self):
        config,events=evidence()
        for event in events:
            if event['event']!='core_job_drained':event.pop('usage',None)
        with self.assertRaisesRegex(ValueError,'missing compute transition'):
            verify_organization(config,events)

    def test_underreported_usage_cannot_hide_two_large_active_tasks(self):
        config,events=evidence(works=(378,378),active_work=512,context=512)
        for event in events:
            if 'usage' in event:event['usage']['active_work']=min(event['usage']['active_work'],378)
        with self.assertRaisesRegex(ValueError,'reconstructed active work/request capacity'):
            verify_organization(config,events,max_active_requests=2)

    def test_underreport_and_request_count_mismatch_are_rejected_within_budget(self):
        for field in ('active_work','active_requests'):
            with self.subTest(field=field):
                config,events=evidence()
                event=next(e for e in events if e['event']=='core_submitted')
                event['usage'][field]-=1
                with self.assertRaisesRegex(ValueError,'usage differs'):
                    verify_organization(config,events,max_active_requests=2)
        config,events=evidence()
        with self.assertRaisesRegex(ValueError,'request capacity'):
            verify_organization(config,events,max_active_requests=1)

    def test_submitted_order_cannot_be_recovered_by_sorting_member_indices(self):
        config,events=evidence()
        indices=[i for i,e in enumerate(events) if e['event']=='core_submitted']
        first,last=indices
        events[first],events[last]=events[last],events[first]
        with self.assertRaisesRegex(ValueError,'actual group submission order'):
            verify_organization(config,events)

    def test_http_start_order_is_observed_independently(self):
        config,events=evidence()
        first,last=[i for i,e in enumerate(events) if e['event']=='core_http_started']
        events[first],events[last]=events[last],events[first]
        report=verify_organization(config,events,max_active_requests=2)
        self.assertEqual(report['submitted_sequences'],[0,1])
        self.assertEqual(report['http_sequences'],[1,0])
        self.assertFalse(report['submitted_http_same_order'])
        self.assertEqual(report['compute_lifecycle']['request_only_work_upper_bound'],30)
        self.assertFalse(report['compute_lifecycle']['work_limit_can_bind'])
        self.assertIsNone(report['compute_lifecycle']['dispatch_work_block_count'])

    def test_terminals_must_be_unique_known_and_complete(self):
        for mutation in ('duplicate','unknown','missing','drain_missing'):
            with self.subTest(mutation=mutation):
                config,events=evidence()
                index=next(i for i,e in enumerate(events) if e['event']=='core_terminal')
                if mutation=='duplicate':events.insert(index+1,deepcopy(events[index]))
                elif mutation=='unknown':events[index]['key']=dict(session_id=0,sequence=99)
                elif mutation=='missing':events.pop(index)
                else:events.pop()
                with self.assertRaises(ValueError):verify_organization(config,events)

    def test_http_cancel_and_delivery_markers_do_not_settle_compute(self):
        for replacement in ('core_http_finished','core_map_completion','core_cancelled','core_released'):
            with self.subTest(replacement=replacement):
                config,events=evidence()
                event=next(e for e in events if e['event']=='core_terminal')
                event['event']=replacement
                # Keep the untrusted claimed decrease, to ensure it cannot release work.
                with self.assertRaises(ValueError):
                    replay_active_work(events,{(0,0):10,(0,1):20},active_work=64,max_active_requests=2)

    def test_uncertain_work_stays_charged_until_late_authoritative_terminal(self):
        _,events=evidence()
        index=next(i for i,e in enumerate(events) if e['event']=='core_terminal')
        events.insert(index,dict(event='core_uncertain',key=dict(session_id=0,sequence=0),
            usage=dict(active_work=30,active_requests=2)))
        report=replay_active_work(events,{(0,0):10,(0,1):20},active_work=64,max_active_requests=2)
        self.assertEqual(report['uncertain_events'],1)
        self.assertEqual(report['authoritative_terminals'],2)
        self.assertEqual(report['peak_active_work'],30)
        events[index]['usage']['active_work']=20
        with self.assertRaisesRegex(ValueError,'usage differs'):
            replay_active_work(events,{(0,0):10,(0,1):20},active_work=64,max_active_requests=2)

    def test_self_consistent_smaller_work_cannot_replace_declared_generation_budget(self):
        config,events=evidence()
        for event in events:
            if event['event']=='core_map_work_described':
                event['max_new_tokens']-=1
                event['estimated_work']-=1
            elif event['event']=='core_submitted':event['estimated_work']-=1
            if 'usage' in event:event['usage']['active_work']-=event['usage']['active_requests']
        with self.assertRaisesRegex(ValueError,'declared generation budget'):
            verify_organization(config,events,max_active_requests=2,expected_max_new_tokens=8)

    def test_capacity_refusal_is_checked_against_reconstructed_work_and_requests(self):
        events=[
            dict(event='core_submitted',key=dict(session_id=0,sequence=0),usage=dict(active_work=10,active_requests=1)),
            dict(event='core_dispatch_capacity_blocked',key=dict(session_id=0,sequence=1),usage=dict(active_work=10,active_requests=1)),
            dict(event='core_terminal',key=dict(session_id=0,sequence=0),usage=dict(active_work=0,active_requests=0)),
            dict(event='core_submitted',key=dict(session_id=0,sequence=1),usage=dict(active_work=20,active_requests=1)),
            dict(event='core_terminal',key=dict(session_id=0,sequence=1),usage=dict(active_work=0,active_requests=0)),
            dict(event='core_job_drained',usage=dict(active_work=0,active_requests=0)),
        ]
        result=replay_active_work(events,{(0,0):10,(0,1):20},active_work=25,max_active_requests=2,
                                  capacity_observation_version=1)
        self.assertTrue(result['work_limit_binding_observed'])
        self.assertEqual(result['work_only_block_count'],1)
        self.assertEqual(result['dispatch_work_block_count'],1)
        with self.assertRaisesRegex(ValueError,'no reconstructed limiting resource'):
            replay_active_work(events,{(0,0):10,(0,1):20},active_work=30,max_active_requests=2,
                               capacity_observation_version=1)


if __name__=='__main__':
    unittest.main()
