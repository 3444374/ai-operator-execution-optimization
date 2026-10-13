"""Fixture and independent audit; real-library pair checks are explicitly gated."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from importlib.util import find_spec
import hashlib
import json
import os
from pathlib import Path
import unittest
from urllib.request import Request, urlopen

from src.experiments.query_sharing_lotus import FixtureService, PairFailure, audit, digest, identity, row_text, run_pair


def body(query, sequence):
    return dict(model='lotus-fixture',messages=[dict(role='user',content=row_text(query,sequence))],
                max_tokens=16,temperature=0,top_p=1,n=1)


def traced_pair():
    calls=[]
    for query,start in (('long',1.1),('short',1.3)):
        request=body(query,0)
        raw=json.dumps(dict(model='lotus-fixture',usage=dict(total_tokens=8),
            choices=[dict(finish_reason='stop',message=dict(content='POSITIVE'))]))
        calls.append(dict(query_id=query,sequence=0,body=request,body_sha256=digest(request),
                          response_body=raw,response_sha256=hashlib.sha256(raw.encode()).hexdigest(),
                          arrival=start,service_start=start,service_finish=start+.2))
    return dict(status='passed',run_id='audit-fixture',arm='strong_static',counts=dict(long=1,short=1),
        cancel_at_s=None,release_epoch=1.,delay_s=.2,active_requests=2,calls=calls,
        ready=[dict(pid=1),dict(pid=2)],http_errors=[],exitcodes=dict(long=0,short=0),
        bridge_closed=True,final_core=dict(usage=dict(active_requests=0),jobs=0),
        bridge_events=[dict(owner_thread=3,event='query_open',query_id=q,job_id=q,session_id=i)
                       for i,q in enumerate(('long','short'))],core_events=[],rss_peak_bytes=1024,
        frontends={q:dict(event='finished',began=start,ended=2.,jct_s=2.-start,cpu_s=.01,
            lm_stats=dict(cache_hits=0,operator_cache_hits=0,physical_usage=dict(total_tokens=8)))
                   for q,start in (('long',1.),('short',1.2))})


class LotusPairAuditTests(unittest.TestCase):
    def test_two_worker_service_keeps_complete_rows_and_exact_responses(self):
        with FixtureService(delay_s=.01,max_posts=4) as service:
            service.run_id='unit-service'
            def post(i):
                request=body('long',i)
                with urlopen(Request(service.base+'/chat/completions',json.dumps(request).encode(),
                                     {'Content-Type':'application/json'}),timeout=3) as response:
                    return json.load(response)['choices'][0]['message']['content']
            with ThreadPoolExecutor(max_workers=4) as pool:
                values=list(pool.map(post,range(4)))
            self.assertEqual(values,['POSITIVE','NEGATIVE','POSITIVE','NEGATIVE'])
        self.assertEqual(len(service.calls),4)
        self.assertEqual(service.peak,2)
        self.assertEqual(service.active,0)
        self.assertEqual(service.errors,[])
        self.assertEqual(sorted(identity(c['body'])[1] for c in service.calls),list(range(4)))

    def test_valid_trace_has_overlap_and_complete_query_metrics(self):
        result=audit([traced_pair()])[0]
        self.assertTrue(result['overlap'])
        self.assertEqual(result['count'],2)
        self.assertEqual(result['service_peak'],1)

    def test_body_parity_compares_complete_generation_and_messages(self):
        a,b=traced_pair(),traced_pair()
        b['calls'][0]['body']['messages'][0]['content']+=' Different words.'
        b['calls'][0]['body_sha256']=digest(b['calls'][0]['body'])
        with self.assertRaisesRegex(ValueError,'complete request body differs'):
            audit([a,b])

    def test_missing_duplicate_and_foreign_requests_are_rejected(self):
        changes=(lambda r:r['calls'].pop(),lambda r:r['calls'].append(deepcopy(r['calls'][0])),
                 lambda r:r['calls'][0].update(sequence=7))
        for change in changes:
            with self.subTest(change=change):
                run=traced_pair();change(run)
                with self.assertRaises(ValueError): audit([run])

    def test_early_release_resource_overflow_and_shared_query_identity_are_rejected(self):
        changes=(lambda r:r['calls'][0].update(arrival=.9),
                 lambda r:r['core_events'].append(dict(usage=dict(active_requests=3,active_work=3,held_tasks=3))),
                 lambda r:r['bridge_events'][1].update(job_id='long'),
                 lambda r:r['final_core']['usage'].update(active_requests=1))
        for change in changes:
            with self.subTest(change=change):
                run=traced_pair();change(run)
                with self.assertRaises(ValueError): audit([run])

    def test_three_service_slots_cannot_be_misreported_as_two(self):
        run=traced_pair();run['counts']['long']=2
        third=deepcopy(run['calls'][0]);third.update(sequence=1,body=body('long',1))
        third['body_sha256']=digest(third['body']);third['service_finish']=1.6
        response=json.loads(third['response_body']);response['choices'][0]['message']['content']='NEGATIVE'
        third['response_body']=json.dumps(response)
        third['response_sha256']=hashlib.sha256(third['response_body'].encode()).hexdigest()
        run['calls'][0]['service_finish']=1.6
        run['calls'].append(third)
        with self.assertRaisesRegex(ValueError,'service slot accounting'):
            audit([run])

    def test_generation_response_digest_and_cached_usage_cannot_pass_as_equal_work(self):
        changes=(lambda r:r['calls'][0]['body'].update(max_tokens=17),
                 lambda r:r['calls'][0].update(response_body='{}'),
                 lambda r:r['frontends']['long']['lm_stats'].update(cache_hits=1),
                 lambda r:r['frontends']['long']['lm_stats']['physical_usage'].update(total_tokens=0))
        for change in changes:
            with self.subTest(change=change):
                run=traced_pair();change(run)
                # Keep request digest valid to exercise the parameter-specific check.
                run['calls'][0]['body_sha256']=digest(run['calls'][0]['body'])
                with self.assertRaises(ValueError):audit([run])

    def test_pinned_provider_output_cap_conversion_keeps_complete_body_parity(self):
        a,b=traced_pair(),traced_pair()
        for run in (a,b):
            for call in run['calls']:
                call['body']['max_completion_tokens']=call['body'].pop('max_tokens')
                call['body_sha256']=digest(call['body'])
        self.assertEqual(len(audit([a,b])),2)


@unittest.skipUnless(find_spec('lotus') is not None,'actual pinned LOTUS is unavailable')
class RealLotusPairTests(unittest.TestCase):
    def test_native_and_identical_bridge_static_candidate_match(self):
        runs=[]
        try:
            with FixtureService(delay_s=.01) as service:
                for arm in ('native_shared','strong_static','candidate'):
                    runs.append(run_pair(service,arm,long_rows=32,short_rows=4,delay_s=.08))
        except PairFailure as error:
            runs.append(error.evidence)
            raise
        finally:
            output=os.environ.get('SEMLOOM_PAIR_TEST_OUTPUT')
            if output:
                with Path(output).open('x') as stream:
                    json.dump(dict(source_commit=os.environ.get('SEMLOOM_SOURCE_COMMIT'),runs=runs),stream)
        self.assertEqual([r['count'] for r in audit(runs)],[36]*3)
        self.assertTrue(all(len(r['ready'])==2 for r in runs))


if __name__=='__main__':
    unittest.main()
