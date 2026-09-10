"""Reject altered token, visibility, membership and actual HTTP evidence."""
from copy import deepcopy
from dataclasses import asdict
import unittest

from src.execution_provider.adapters.map_organization import MapOrganizationConfig
from src.experiments.postgresql.organization_evaluation import verify_organization


def evidence():
    config=MapOrganizationConfig('rows',2,2,32,64,'model','r1','s1','fixture','a'*64,64)
    fields=asdict(config);fields.pop('tokenizer_path')
    events=[dict(event='core_map_organization_config',**fields,calibration_signature=config.calibration_signature)]
    for i,prompt in enumerate((2,12)):
        events.extend([
            dict(event='core_map_work_described',sequence=i,semantic_payload_digest=str(i)*64,
                 request_sha256=str(i+2)*64,prompt_tokens=prompt,max_new_tokens=8,estimated_work=prompt+8,
                 work_unit='tokens',calibration_signature=config.calibration_signature),
            dict(event='core_offer',key=dict(session_id=0,sequence=i),accepted_prefix_count=1),
            dict(event='core_map_task',sequence=i,payload_digest=str(i)*64),
        ])
    keys=[dict(session_id=0,sequence=i) for i in range(2)]
    events.append(dict(event='core_map_organized',mode='rows',calibration_signature=config.calibration_signature,
                       candidates=[dict(key=k,work=w) for k,w in zip(keys,(10,20))],selected=keys))
    for i in range(2):
        events.extend([
            dict(event='core_submitted',key=keys[i],estimated_work=(10,20)[i],work_unit='tokens',
                 member=dict(batch_key=dict(session_id=0,sequence=0),index=i,size=2),
                 usage=dict(active_work=(10,30)[i])),
            dict(event='core_http_started',key=keys[i]),
            dict(event='request',request_bytes_sha256=str(i+2)*64),
            dict(event='core_map_completion',sequence=i,prompt_tokens=(2,12)[i]),
        ])
    return config,events


class OrganizationEvaluationTests(unittest.TestCase):
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


if __name__=='__main__':
    unittest.main()
