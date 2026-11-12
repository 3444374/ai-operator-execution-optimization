"""Original Cars sampling and CSV identities without unrelated media downloads."""
import os
import json
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal

from src.baselines.text import sembench_cars as cars
from src.baselines.text.sembench_text import SemBenchTextQuery
from src.serving.backends.vllm_protocol import GenerationSpec

CHECKOUT=os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')


@unittest.skipUnless(CHECKOUT,'requires pinned public SemBench source')
class CarsInputPreparationTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name);self.source=self.root/'full';self.source.mkdir()
        self.frames=[pd.DataFrame(dict(car_id=list(range(1,10)),year=list(range(2015,2024)))),
            pd.DataFrame(dict(car_id=[1,2,3,4,5,5],image_id=list(range(100,106)),
                image_path=['unchanged-image-'+str(i)+'.jpg' for i in range(6)],damage_status=['no_damage']*6)),
            pd.DataFrame(dict(car_id=[1,2,6,7],audio_id=list(range(200,204)),
                audio_path=['unchanged-audio-'+str(i)+'.wav' for i in range(4)],generic_problem=['engine']*4)),
            pd.DataFrame(dict(car_id=[1,1,2,3,8,9],complaint_id=list(range(300,306)),
                summary=['same','same','engine','other','fire','line\n一'],
                component_class=['ENGINE']*6,crash=[True,False]*3,fire=[False]*6))]
        self.names=('car_data_full.csv','image_data_full.csv','audio_data_full.csv','text_complaints_data_full.csv')
        for name,frame in zip(self.names,self.frames,strict=True):frame.to_csv(self.source/name,index=False)

    def test_original_stratified_selection_full_labels_and_native_text_queries(self):
        module=cars.load_module(CHECKOUT,'sembench_cars_generator','src/scenario/cars/preparation/generate_data.py')
        prefixes=('car_data_','image_car_data_','audio_car_data_','text_complaints_data_')
        projection=(None,['image_path','image_id','car_id'],['audio_path','audio_id','car_id'],
                    ['summary','complaint_id','car_id'])
        for requested in (1,3,7,20):
            with self.subTest(scale=requested):
                expected=(module.scale_down_data(*self.frames,scaling_factor=requested,seed=42)
                          if requested<9 else self.frames)
                output=self.root/('sf'+str(requested))
                with patch.object(cars,'load_module',return_value=module), \
                        patch.object(module,'_download_from_drive',side_effect=AssertionError('unexpected media download')), \
                        patch.object(module,'_download_kaggle_datasets',side_effect=AssertionError('unexpected raw download')):
                    report=cars.prepare_inputs(CHECKOUT,self.source,output,requested)
                scale=report['scale_factor'];self.assertEqual(scale,min(requested,9))
                self.assertEqual(report['selected_rows']['car'],len(expected[0]))
                for name in self.names:
                    self.assertEqual((output/'data/full_data'/name).read_bytes(),(self.source/name).read_bytes())
                for frame,prefix,columns in zip(expected,prefixes,projection,strict=True):
                    actual=pd.read_csv(output/'data'/('sf_'+str(scale))/(prefix+str(scale)+'.csv'))
                    frame=frame if columns is None else frame[columns]
                    assert_frame_equal(actual,frame.reset_index(drop=True))
                for query_id,kind in [('1','filter'),('4','filter'),('10','extract')]:
                    query=SemBenchTextQuery(CHECKOUT,output,'cars',query_id,execution_id='source-only',
                        model_name='openai/local-fixture',generation=GenerationSpec(max_tokens=32),scale_factor=scale)
                    self.assertEqual(query.operation.describe()['kind'],kind)
                self.assertEqual((report['downloads'],report['model_calls']),(0,0))
                with self.assertRaises(FileExistsError):cars.prepare_inputs(CHECKOUT,self.source,output,requested)

    def test_missing_modality_metadata_cannot_be_replaced_by_empty_tables(self):
        (self.source/'audio_data_full.csv').unlink()
        output=self.root/'output'
        with self.assertRaises(FileNotFoundError):cars.prepare_inputs(CHECKOUT,self.source,output,3)
        self.assertFalse(output.exists())

    def test_actual_cli_uses_original_csv_sampler_from_a_caller_directory(self):
        script=Path(__file__).resolve().parents[3]/'scripts/data/prepare_sembench_cars_inputs.py'
        output=self.root/'cli-output'
        result=subprocess.run([sys.executable,'-B',str(script),'--sembench-checkout',CHECKOUT,
            '--source-dir',str(self.source),'--output-dir',str(output),'--scale-factor','3'],
            cwd=self.root,env=dict(os.environ,PYTHONPATH='',CUDA_VISIBLE_DEVICES=''),
            capture_output=True,text=True,timeout=60)
        self.assertEqual(result.returncode,0,result.stderr)
        report=json.loads(result.stdout)
        self.assertEqual((report['downloads'],report['model_calls']),(0,0))
        self.assertEqual(len(report['files']),8)
        self.assertEqual(report['scale_factor'],3)


if __name__=='__main__':unittest.main()
