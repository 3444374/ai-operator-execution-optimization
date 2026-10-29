"""Prepared token input reuse over the production Core and CPU model IPC."""
from copy import deepcopy
from dataclasses import replace
import json
import threading
import unittest
from unittest.mock import patch

from src.semantic_methods.lotus import prepared_batching, record_chain
from tests.execution_provider import test_lotus_prepared_batching as fixture


class PreparedReuseTests(unittest.TestCase):
    setUp = fixture.PreparedBatchingTests.setUp
    execute = fixture.PreparedBatchingTests.execute

    def measured(self, policy, **options):
        driver = threading.get_ident()
        counts = {'driver_prepare': 0, 'worker_prepare': 0,
                  'driver_count': 0, 'worker_count': 0}
        prepare = record_chain.LotusRecordMethod.prepare_rows
        count = prepared_batching.chat_token_count

        def preparing(method, rows, **kwargs):
            counts['driver_prepare' if threading.get_ident() == driver else 'worker_prepare'] += len(rows)
            return prepare(method, rows, **kwargs)

        def counting(*args, **kwargs):
            counts['driver_count' if threading.get_ident() == driver else 'worker_count'] += 1
            return count(*args, **kwargs)

        with patch.object(record_chain.LotusRecordMethod, 'prepare_rows', preparing), \
                patch.object(prepared_batching, 'chat_token_count', counting):
            result = self.execute(policy, **options)
        return result, counts

    def test_token_prepares_and_counts_each_request_once_and_keeps_native_inputs(self):
        (owner, records, close, native, error), counts = self.measured(
            'token_budget', read_rows=1, max_rows=None)
        self.assertIsNone(error)
        self.assertEqual(counts, dict(driver_prepare=4, worker_prepare=0,
                                     driver_count=4, worker_count=0))
        self.assertEqual([len(x['calls']) for x in native], [2, 1, 1])
        self.assertEqual(close['status'], 'closed')
        actual = {c['call_id']:c for c in owner.calls}
        native_messages = {c['call_id']:m for batch in native
                           for c,m in zip(batch['calls'],batch['messages'])}

        def replay(prepared, *, generation):
            for row in prepared:
                self.assertEqual(json.loads(row['messages_json']), native_messages[row['call_id']])
            self.assertEqual(generation, self.chain.stages[0].generation)
            return [actual[row['call_id']] for row in prepared]

        reference = self.chain.run_batch(self.rows, replay)
        self.assertEqual([r.describe() for r in records], [r.describe() for r in reference['records']])
        for receipt,batch in zip(owner.request_batches,native):
            expected = sum(prepared_batching.chat_token_count(self.tokenizer,m) for m in batch['messages'])
            self.assertEqual(receipt['input_tokens'],expected)
            self.assertEqual(receipt['output_cap_tokens'],64*len(batch['calls']))

    def test_fixed_and_default_keep_their_original_prepare_and_estimate_paths(self):
        for policy, worker_count in ((None, 0), ('fixed_rows', 4)):
            with self.subTest(policy=policy):
                (owner, records, close, native, error), counts = self.measured(policy)
                self.assertIsNone(error)
                self.assertEqual(counts, dict(driver_prepare=0, worker_prepare=4,
                                             driver_count=0, worker_count=worker_count))
                self.assertEqual([len(x['calls']) for x in native], [4])
                self.assertEqual(len(records),4)
                self.assertEqual(close['status'],'closed')

    def test_reuse_adds_no_row_or_native_sequence_cap(self):
        rows = fixture.fixture.source_rows(column='text',size=80)
        (owner, records, close, native, error), counts = self.measured('token_budget',
            rows=rows, budget=100000, read_rows=1, max_rows=None)
        self.assertIsNone(error)
        self.assertEqual([len(x['calls']) for x in native],[80])
        self.assertEqual(counts,dict(driver_prepare=80,worker_prepare=0,
                                     driver_count=80,worker_count=0))
        self.assertEqual([c['source_position'] for c in native[0]['calls']],list(range(80)))
        self.assertEqual(len(records),80)
        self.assertEqual(close['status'],'closed')

    def test_changed_prepared_identity_or_parameter_contract_never_reaches_native(self):
        run = fixture.Batch.run_rows
        for corruption in ('call_id','order','messages','source','signature','engine','generation',
                           'input_tokens','output_cap_tokens','B','row_cap'):
            observed=[]

            def corrupt(worker, rows, **kwargs):
                rows,kwargs = deepcopy(rows),deepcopy(kwargs)
                self.assertIn('prepared_info',kwargs)
                self.assertTrue(all('prepared' in row for row in rows))
                if corruption=='call_id': rows[0]['prepared']['call_id']='wrong-call'
                elif corruption=='order':
                    rows[0]['prepared'],rows[1]['prepared']=rows[1]['prepared'],rows[0]['prepared']
                elif corruption=='messages': rows[0]['prepared']['messages_json']='{}'
                elif corruption=='source':
                    record=json.loads(rows[0]['record_json']); record['values']['text']='changed'
                    rows[0]['record_json']=json.dumps(record)
                elif corruption=='signature': kwargs['prepared_info']['signature']='wrong-stage-generation'
                elif corruption=='engine':
                    worker.engine_spec=replace(worker.engine_spec, max_num_batched_tokens=8192)
                elif corruption=='generation':
                    chain=worker.chain.describe(); chain['stages'][0]['generation']['max_tokens']=32
                    worker.chain=record_chain.LotusRecordChain.from_dict(chain)
                elif corruption=='input_tokens': kwargs['prepared_info']['input_tokens']=False
                elif corruption=='B': kwargs['prepared_info']['input_tokens']=1_000_000
                elif corruption=='row_cap':
                    worker.prepared_batching=replace(worker.prepared_batching,max_rows=1)
                else: kwargs['prepared_info']['output_cap_tokens']=1
                value=run(worker,rows,**kwargs);observed.append(value)
                return value

            with self.subTest(corruption=corruption), patch.object(fixture.Batch,'run_rows',corrupt):
                owner,records,close,native,error=self.execute('token_budget',max_rows=None,
                    allow_failure=True)
                self.assertIsNotNone(error)
                self.assertFalse(records)
                self.assertFalse(native)
                self.assertTrue(observed)
                self.assertEqual(observed[0]['error_type'],'ValueError')
                self.assertFalse(close['query_completed'])

    def test_complete_wire_payload_is_charged_and_no_prepared_rows_leak_to_metadata(self):
        from src.execution_provider.adapters.lotus_record_chain_core import _ChainTransport
        run, captured = _ChainTransport._run, []

        def observe(transport, task):
            captured.append((task.task.payload,task.task.metadata))
            return run(transport,task)

        rows=fixture.fixture.source_rows(column='text',size=7)
        for row in rows: row.values['text']='short'
        with patch.object(_ChainTransport,'_run',observe):
            owner,records,close,native,error=self.execute('token_budget',max_rows=None,
                rows=rows,budget=100000,capacity=fixture.CoreChainCapacity(item_input_bytes=2048))
        self.assertIsNone(error)
        self.assertEqual(len(records),7)
        self.assertEqual(close['status'],'closed')
        for payload,metadata in captured:
            self.assertLessEqual(len(payload),owner.capacity.item_input_bytes)
            self.assertLessEqual(len(metadata),owner.capacity.metadata_bytes)
            self.assertEqual(set(json.loads(metadata)),{
                'reason','input_tokens','output_cap_tokens','signature'})
            self.assertTrue(all(set(r)=={'record_json','prepared'} for r in json.loads(payload)))
        self.assertEqual(sum(len(b['calls']) for b in native),7)

    def test_reused_method_keeps_input_settings_checks_and_rejects_unsupported_scope(self):
        import lotus
        from src.serving.backends.vllm_engine import _prepared

        prepared=self.chain.methods[0].prepare_rows(self.rows,query_id=self.chain.query_id)
        with patch.object(record_chain.LotusRecordMethod,'prepare_rows',side_effect=AssertionError('formatted twice')):
            invalid=deepcopy(self.rows); invalid[0].values['text']=None
            with self.assertRaisesRegex(ValueError,'present text'):
                self.chain.run_batch(invalid,None,prepared_rows=prepared)
            with lotus.settings.context(enable_cache=True), self.assertRaisesRegex(ValueError,'caching disabled'):
                self.chain.run_batch(self.rows,None,prepared_rows=prepared)
            with self.assertRaisesRegex(TypeError,'allow_nulls'):
                self.chain.run_batch(self.rows,None,allow_nulls=1,prepared_rows=prepared)
        multi=fixture.fixture.chain_for('MM')
        with self.assertRaisesRegex(ValueError,'single Filter or Map'):
            multi.run_batch(fixture.fixture.source_rows(size=2),None,prepared_rows=prepared)
        # Pure shape/identity validation remains part of reuse; no new tokenizer.
        self.assertEqual(len(_prepared(prepared)[0]),4)


if __name__=='__main__':
    unittest.main()
