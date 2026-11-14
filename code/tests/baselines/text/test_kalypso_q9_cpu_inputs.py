"""Frozen cross-Python input/source/identity gates without model dependencies."""
import csv
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.kalypso_native import movie_map_inputs as inputs
from src.baselines.text.frameworks.kalypso_native.native_server import MOVIE_MAP_QUERIES
from tests.baselines.text.test_kalypso_movie_map import unit

ROOT=Path(__file__).resolve().parents[4]


class CPUInputContracts(unittest.TestCase):
    def setUp(self):
        self.prepare(7)
        self.addCleanup(patch.stopall)
        patch.object(inputs,'binding',side_effect=lambda *args:self.binding).start()

    def prepare(self,count,task='movie-q9'):
        directory=tempfile.TemporaryDirectory();self.addCleanup(directory.cleanup)
        self.root=Path(directory.name)
        self.cfg=dict(units=[unit(count,task)])
        self.binding=dict(source='verified-test-source',unit=unit(count,task))
        self.manifest=dict(schema=inputs.SCHEMA,binding=self.binding,model_calls=0,queries=[])
        for spec in unit(count,task)['queries']:
            root=self.root/spec['query_id'];root.mkdir()
            rows=[dict(idx=i,row_id=spec['query_id']+'-row-'+str(i),source_position=i,
                source_index=i*7,reviewId='duplicate',reviewText='review '+str(i),
                fields={'reviewText':'review '+str(i)},data='review '+str(i)) for i in range(count)]
            (root/'prompt.json').write_text(json.dumps(dict(query_id=spec['query_id'],instruction='original',rows=rows)))
            with (root/'input.csv').open('w',newline='') as stream:
                writer=csv.DictWriter(stream,fieldnames=['data']);writer.writeheader()
                writer.writerows(dict(data=r['reviewText']) for r in rows)
            query=MOVIE_MAP_QUERIES[task]
            description=dict(scenario='movie',query='Q'+query,query_id=spec['query_id'],
                sembench_commit='c814e3807e72d4cf876b852b17e77f3cc94575c2',expected_model_calls=count,
                operation=dict(stage=dict(method=dict(instruction='original'))))
            if query=='9':description['ranking']='map'
            (root/'source.json').write_text(json.dumps(description))
            self.manifest['queries'].append(dict(query_id=spec['query_id'],logical_occurrences=count,
                files={role:dict(path=str(path.relative_to(self.root)),sha256=inputs.sha(path))
                    for role,path in dict(csv=root/'input.csv',prompt=root/'prompt.json',source=root/'source.json').items()}))
        self.publish()

    def publish(self):
        path=self.root/'CPU-INPUTS.json';path.write_text(json.dumps(self.manifest))
        self.cfg['native_cpu_inputs']=dict(manifest=str(path),sha256=inputs.sha(path))

    def test_complete_frozen_inputs_preserve_duplicate_business_ID_and_source_occurrences(self):
        for task in MOVIE_MAP_QUERIES:
            for count in (1,7,13):
                self.prepare(count,task)
                manifest,loaded=inputs.load_inputs(self.cfg)
                self.assertEqual([len(q['table']['rows']) for q in loaded],[count,count])
                rows=loaded[0]['table']['rows']
                self.assertEqual({r['reviewId'] for r in rows},{'duplicate'})
                self.assertEqual([r['source_index'] for r in rows],[i*7 for i in range(count)])
                self.assertEqual(len({r['row_id'] for r in rows}),count)

    def test_frozen_query_number_and_ranking_must_match_the_declared_map_task(self):
        self.prepare(7,'movie-q10')
        item=self.manifest['queries'][0]
        path=self.root/item['files']['source']['path']
        original=json.loads(path.read_text())
        for change in (dict(query='Q9'),dict(ranking='topk'),dict(ranking='map')):
            path.write_text(json.dumps(dict(original,**change)))
            item['files']['source']['sha256']=inputs.sha(path);self.publish()
            with self.subTest(change=change),self.assertRaisesRegex(ValueError,'original Map task'):
                inputs.load_inputs(self.cfg)

    def test_source_and_manifest_counts_must_match_the_bound_query_not_history(self):
        item=self.manifest['queries'][0]
        item['logical_occurrences']=256;self.publish()
        with self.assertRaisesRegex(ValueError,'occurrence identities'):inputs.load_inputs(self.cfg)
        item['logical_occurrences']=7
        path=self.root/item['files']['source']['path']
        description=json.loads(path.read_text());description['expected_model_calls']=256
        path.write_text(json.dumps(description));item['files']['source']['sha256']=inputs.sha(path)
        self.publish()
        with self.assertRaisesRegex(ValueError,'original Map task'):inputs.load_inputs(self.cfg)

    def test_single_occurrence_counts_reject_boolean_aliases(self):
        self.prepare(1)
        item=self.manifest['queries'][0]
        item['logical_occurrences']=True;self.publish()
        with self.assertRaisesRegex(ValueError,'occurrence identities'):inputs.load_inputs(self.cfg)
        item['logical_occurrences']=1
        path=self.root/item['files']['source']['path']
        description=json.loads(path.read_text());description['expected_model_calls']=True
        path.write_text(json.dumps(description));item['files']['source']['sha256']=inputs.sha(path)
        self.publish()
        with self.assertRaisesRegex(ValueError,'original Map task'):inputs.load_inputs(self.cfg)

    def test_manifest_hash_and_each_frozen_file_hash_fail_closed(self):
        for target in ('CPU-INPUTS.json','cold/input.csv','cold/prompt.json','cold/source.json'):
            path=self.root/target;original=path.read_bytes();path.write_bytes(original+b' ')
            with self.subTest(target=target),self.assertRaisesRegex(ValueError,'changed'):
                inputs.load_inputs(self.cfg)
            path.write_bytes(original)

    def test_wrong_source_binding_or_query_sequence_fails_before_planning(self):
        self.manifest['binding']=dict(self.binding,source='different');self.publish()
        with self.assertRaisesRegex(ValueError,'source/model/unit'):inputs.load_inputs(self.cfg)
        self.manifest['binding']=self.binding;self.manifest['queries'].reverse();self.publish()
        with self.assertRaisesRegex(ValueError,'sequence'):inputs.load_inputs(self.cfg)

    def test_semantic_instruction_or_occurrence_corruption_is_not_accepted(self):
        item=self.manifest['queries'][0];path=self.root/item['files']['prompt']['path']
        original=json.loads(path.read_text())
        import copy
        for change in (lambda d:d.update(instruction='different'),
                       lambda d:d['rows'][0].update(source_position=1),
                       lambda d:d['rows'][0].update(idx=False),
                       lambda d:d['rows'][0].update(row_id='duplicate')):
            value=copy.deepcopy(original);change(value);path.write_text(json.dumps(value))
            item['files']['prompt']['sha256']=inputs.sha(path);self.publish()
            with self.assertRaises(ValueError):inputs.load_inputs(self.cfg)

    def test_freeze_and_consume_source_proof_checks_real_code_location_commit_tree_and_dirt(self):
        cfg=dict(baseline_source=str(ROOT),adapter_commit='a'*40,adapter_tree='b'*40)
        with patch.object(inputs.subprocess,'check_output',side_effect=['a'*40+'\n'+'b'*40+'\n'+'c'*40+'\n','']):
            proof=inputs.source_binding(cfg)
            self.assertEqual(proof['code_tree'],'c'*40)
            self.assertEqual(len(proof['file_sha256']),len(inputs.SOURCE_FILES))
        for output in (['x'*40+'\n'+'b'*40+'\n'+'c'*40],['a'*40+'\n'+'b'*40+'\n'+'c'*40,' M code/x.py']):
            with patch.object(inputs.subprocess,'check_output',side_effect=output),self.assertRaises(ValueError):
                inputs.source_binding(cfg)
        with self.assertRaisesRegex(ValueError,'different maintained'):
            inputs.source_binding(dict(cfg,baseline_source=str(self.root)))


if __name__=='__main__':unittest.main()
