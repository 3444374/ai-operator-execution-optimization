"""Differential CPU tests of actual _infer and audit side effects, including damaged results."""
from copy import deepcopy
import json
import random
import tempfile
import unittest
from tests.execution_provider.chain_index_fixture import exercise, fixtures, load, no_runtime_loaded


class CommonKernelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.base,cls.candidate=load('base'),load('candidate')
    def equivalent(self,prepared,results,**kwargs):
        before=deepcopy(results)
        with tempfile.TemporaryDirectory(prefix='semloom-match-test-') as control:
            old=exercise(self.base,prepared,results,control,**kwargs)
            new=exercise(self.candidate,prepared,results,control,**kwargs)
        for k in ['error','returned','audit','snapshots','audit_errors','model_invocations']:
            self.assertEqual(old[k],new[k],k)
        self.assertEqual(results,before)
        if new['error'] is None:self.assertIs(new['returned'],results)
        return new
    def test_valid_sizes_and_reordered_outputs(self):
        for n in [1,3,16,64,257]:
            with self.subTest(n=n):
                p,r=fixtures(n);random.Random(42).shuffle(r);o=self.equivalent(p,r)
                self.assertEqual([x['call_id'] for x in o['audit']],[x['call_id'] for x in p])
    def test_duplicates_missing_and_foreign(self):
        for kind in ['duplicate','missing','foreign','same_call_changed_identity']:
            with self.subTest(kind=kind):
                p,r=fixtures(4)
                if kind=='duplicate':r.append(deepcopy(r[1]))
                elif kind=='missing':r.pop(1)
                elif kind=='foreign':r.append(dict(r[0],call_id='foreign'))
                else:r[1]['source_position']=99
                self.equivalent(p,r)
    def test_malformed_result_identity_and_payloads(self):
        variants=[None,[],{'unhashable':1},True,1.0]
        for value in variants:
            with self.subTest(value=value):
                p,r=fixtures(4);r[1]['source_position']=value;self.equivalent(p,r)
        for bad in [None,'not-object',[1],{}]:
            with self.subTest(result=bad):
                p,r=fixtures(4);r[1]=bad;self.equivalent(p,r)
        p,r=fixtures(4);r[1].pop('call_id');self.equivalent(p,r)
    def test_bool_and_float_equal_identity_duplicate_fallback(self):
        for value in [True,1.0]:
            p,r=fixtures(3);r.append(dict(r[1],source_position=value));self.equivalent(p,r)
    def test_all_identity_fields_participate(self):
        from tests.execution_provider.chain_index_fixture import IDENTITY_FIELDS
        for field in IDENTITY_FIELDS:
            with self.subTest(field=field):
                p,r=fixtures(3)
                r[1][field]=(r[1][field]+10 if type(r[1][field]) is int else r[1][field]+'-other')
                out=self.equivalent(p,r)
                self.assertEqual(out['audit'][1]['status'],'unknown')
                self.assertEqual(out['snapshots'][-1]['raw_results'],[r[1]])
    def test_non_list_container_preserves_legacy_path(self):
        p,r=fixtures(3);self.equivalent(p,tuple(reversed(r)))
    def test_terminal_states_and_known_usage_not_rewritten(self):
        for status in ['failed','unknown','not-a-terminal']:
            p,r=fixtures(3);r[1].update(status=status,submission_confirmed=False)
            self.equivalent(p,r)
    def test_copy_isolation_and_parent_links(self):
        p,r=fixtures(4);parents={(x['row_id'],x['source_position']):('parent:'+str(i),) for i,x in enumerate(p)}
        o=self.equivalent(p,list(reversed(r)),parents=parents)
        for i,call in enumerate(o['audit']):
            self.assertEqual(call['parent_call_ids'],['parent:'+str(i)])
            matched=next(x for x in r if x['call_id']==call['call_id'])
            self.assertIsNot(call['extra'],matched['extra'])
            self.assertIsNot(call['extra']['mutable'],matched['extra']['mutable'])
    def test_engine_and_publisher_failures(self):
        p,r=fixtures(3)
        self.equivalent(p,r,error=RuntimeError('CPU model failure'))
        self.equivalent(p,r,fail_at=1)
        o=self.equivalent(p,r,fail_at=2)
        self.assertIn('call_ledger_write: OSError',o['audit_errors'])
    def test_no_model_call_after_existing_stop(self):
        from pathlib import Path
        p,r=fixtures(3)
        with tempfile.TemporaryDirectory(prefix='semloom-stop-test-') as control:
            Path(control,'stop').touch()
            for m in [self.base,self.candidate]:
                out=exercise(m,p,r,control)
                self.assertEqual(out['error'][0],'InterruptedError');self.assertEqual(out['model_invocations'],0)
    def test_linear_valid_identity_path(self):
        p,r=fixtures(64)
        with tempfile.TemporaryDirectory(prefix='semloom-linear-test-') as control:
            old=exercise(self.base,p,r,control,profile=True);new=exercise(self.candidate,p,r,control,profile=True)
        self.assertGreater(old['dict_get_calls'],40*64+200)
        self.assertLessEqual(new['dict_get_calls'],40*64+200)
    def test_randomized_differential_protocol_cases(self):
        rng=random.Random(73)
        for trial in range(100):
            p,r=fixtures(rng.randint(1,20));rng.shuffle(r)
            if trial%3==0:r.append(deepcopy(r[0]))
            if trial%5==0:r.pop(rng.randrange(len(r)))
            if trial%7==0:r.append(dict(fixtures(1)[1][0],call_id='foreign:'+str(trial)))
            self.equivalent(p,r)
    def test_cold_shared_worker_import_does_not_load_vendor_runtimes(self):
        import os
        import subprocess
        import sys
        program = "from src.execution_provider.method_workers import lotus_chain_batch; import sys; assert not any(n in sys.modules for n in ('torch','vllm','ray','daft','lotus'))"
        run = subprocess.run([sys.executable, '-B', '-c', program], capture_output=True,
                             text=True, timeout=10, env=os.environ.copy())
        self.assertEqual(run.returncode, 0, run.stderr)


class AssociationCollisionTests(unittest.TestCase):
    def test_real_integer_hash_collision_keeps_full_identity_and_order(self):
        import sys
        from tests.execution_provider.chain_index_fixture import IDENTITY_FIELDS
        p, r = fixtures(3)
        for i, (request, result) in enumerate(zip(p, r)):
            # Distinct valid integer identities, identical integer/tuple hashes.
            for field in IDENTITY_FIELDS:
                value = i * sys.hash_info.modulus if field == 'source_position' else ('same' if type(request[field]) is str else 0)
                request[field] = result[field] = value
        self.assertEqual(len({hash(tuple(row[k] for k in IDENTITY_FIELDS)) for row in r}), 1)
        case = CommonKernelTests()
        case.base, case.candidate = load('base'), load('candidate')
        out = case.equivalent(p, list(reversed(r)))
        self.assertEqual([c['source_position'] for c in out['audit']], [r['source_position'] for r in p])

    def test_cancel_during_partial_result_keeps_known_usage_and_unknown(self):
        from pathlib import Path
        from unittest.mock import patch
        from tests.execution_provider import chain_index_fixture as helper
        p, r = fixtures(3)
        outcomes = []
        for module in (load('base'), load('candidate')):
            with tempfile.TemporaryDirectory(prefix='index-partial-') as control:
                obj = helper.worker(module, control, r[:1],
                    parents={(row['row_id'], row['source_position']): ('upstream',) for row in p})
                original = obj.engine.run_prepared
                def partial(rows, *, generation):
                    Path(control, 'stop').touch()
                    return original(rows, generation=generation)
                obj.engine.run_prepared = partial
                with patch.object(helper.implementation, '_associate_declared_results',
                          helper._ORIGINAL_INDEX if module.indexed else helper._reference_associate):
                    returned = obj._infer(p, generation='CPU-GENERATION')
                outcomes.append((obj._audit_calls, json.loads(obj._audit_path.read_text()), returned))
        self.assertEqual(*outcomes)
        calls = outcomes[0][0]
        self.assertEqual([c['status'] for c in calls], ['succeeded', 'unknown', 'unknown'])
        self.assertEqual(calls[0]['total_tokens'], 16)
        self.assertEqual([c['parent_call_ids'] for c in calls], [['upstream']] * 3)


if __name__ == '__main__':
    unittest.main()
