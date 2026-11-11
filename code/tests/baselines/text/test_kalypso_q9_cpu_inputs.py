"""Frozen cross-Python input/source/identity gates without model dependencies."""
import csv
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.kalypso_native import movie_map_inputs as inputs
from tests.baselines.text.test_kalypso_movie_map import unit

ROOT=Path(__file__).resolve().parents[4]


class CPUInputContracts(unittest.TestCase):
    def setUp(self):
        directory=tempfile.TemporaryDirectory();self.addCleanup(directory.cleanup)
        self.root=Path(directory.name)
        self.cfg=dict(units=[unit()])
        self.binding=dict(source='verified-test-source',unit=unit())
        self.manifest=dict(schema=inputs.SCHEMA,binding=self.binding,model_calls=0,queries=[])
        for spec in unit()['queries']:
            root=self.root/spec['query_id'];root.mkdir()
            rows=[dict(idx=i,row_id=spec['query_id']+'-row-'+str(i),source_position=i,
                source_index=i*7,reviewId='duplicate',reviewText='review '+str(i)) for i in range(256)]
            (root/'prompt.json').write_text(json.dumps(dict(query_id=spec['query_id'],instruction='original',rows=rows)))
            with (root/'input.csv').open('w',newline='') as stream:
                writer=csv.DictWriter(stream,fieldnames=['data']);writer.writeheader()
                writer.writerows(dict(data=r['reviewText']) for r in rows)
            description=dict(scenario='movie',query='Q9',ranking='map',query_id=spec['query_id'],
                sembench_commit='c814e3807e72d4cf876b852b17e77f3cc94575c2',expected_model_calls=256,
                operation=dict(stage=dict(method=dict(instruction='original'))))
            (root/'source.json').write_text(json.dumps(description))
            self.manifest['queries'].append(dict(query_id=spec['query_id'],logical_occurrences=256,
                files={role:dict(path=str(path.relative_to(self.root)),sha256=inputs.sha(path))
                    for role,path in dict(csv=root/'input.csv',prompt=root/'prompt.json',source=root/'source.json').items()}))
        self.publish()
        self.addCleanup(patch.stopall)
        patch.object(inputs,'binding',return_value=self.binding).start()

    def publish(self):
        path=self.root/'CPU-INPUTS.json';path.write_text(json.dumps(self.manifest))
        self.cfg['native_cpu_inputs']=dict(manifest=str(path),sha256=inputs.sha(path))

    def test_complete_frozen_inputs_preserve_duplicate_business_ID_and_source_occurrences(self):
        manifest,loaded=inputs.load_inputs(self.cfg)
        self.assertEqual([len(q['table']['rows']) for q in loaded],[256,256])
        rows=loaded[0]['table']['rows']
        self.assertEqual({r['reviewId'] for r in rows},{'duplicate'})
        self.assertEqual([r['source_index'] for r in rows],[i*7 for i in range(256)])
        self.assertEqual(len({r['row_id'] for r in rows}),256)

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
