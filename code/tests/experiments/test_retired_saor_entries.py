"""Retired launchers have no live imports and restore byte-exact from archives."""
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from scripts.experiments.materialize_native_baseline import materialize, read_source

ROOT=Path(__file__).resolve().parents[3]
ARCHIVES=ROOT/'experiments/reproduction/native_baselines'
ENTRIES=('run_saor_project_mechanism.py','run_saor_feeding_ceiling.py','run_saor_feeding_gap_diagnostic.py')


class RetiredEntryTests(unittest.TestCase):
    def test_retired_launchers_restore_original_bytes_in_every_named_projection(self):
        manifest=json.loads((ARCHIVES/'MANIFEST.json').read_text())
        for spec in manifest['variants'].values():
            for entry in ENTRIES:
                name='code/scripts/experiments/'+entry
                self.assertFalse((ROOT/name).exists())
                data=read_source(ROOT,ARCHIVES,spec,name)
                self.assertEqual(hashlib.sha256(data).hexdigest(),spec['code_files_sha256'][name])
                self.assertEqual(spec['source_formats'][name]['decoded_bytes'],len(data))

    def fixture(self,folder):
        repository,archives,output=folder/'repo',folder/'archive',folder/'output'
        repository.mkdir();archives.mkdir()
        data=b'print("historical source")\n'
        archive=gzip.compress(data,mtime=0)
        (archives/'source.py.gz').write_bytes(archive)
        spec=dict(original_source_revision='CPU-fixture',code_projection_sha256='unchanged-source-ID',
            code_files_sha256={'code/source.py':hashlib.sha256(data).hexdigest()},
            overrides={'code/source.py':'source.py.gz'},source_formats={'code/source.py':dict(
                encoding='gzip',decoded_bytes=len(data),archive_sha256=hashlib.sha256(archive).hexdigest())})
        return repository,archives,output,spec,data

    def test_materialization_retains_source_ID_and_plain_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            repository,archives,output,spec,data=self.fixture(Path(temporary))
            (archives/'MANIFEST.json').write_text(json.dumps(dict(variants=dict(fixture=spec))))
            result=materialize(repository,archives,'fixture',output)
            self.assertEqual((output/'code/source.py').read_bytes(),data)
            self.assertEqual(result['code_projection_sha256'],spec['code_projection_sha256'])
            self.assertEqual((result['model_calls'],result['models_started']),(0,0))

    def test_archive_damage_wrong_size_and_unsupported_format_fail_before_any_output(self):
        for change in ('tamper','truncated','small','large','boolean_size','format','escape'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as temporary:
                repository,archives,output,spec,_=self.fixture(Path(temporary))
                archive=archives/'source.py.gz';meta=spec['source_formats']['code/source.py']
                if change=='tamper':archive.write_bytes(archive.read_bytes()+b'changed')
                elif change=='truncated':
                    archive.write_bytes(archive.read_bytes()[:16]);meta['archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest()
                elif change=='small':meta['decoded_bytes']-=1
                elif change=='large':meta['decoded_bytes']+=1
                elif change=='boolean_size':meta['decoded_bytes']=True
                elif change=='format':meta['encoding']='unknown'
                else:spec['overrides']['code/source.py']='../source.py.gz'
                (archives/'MANIFEST.json').write_text(json.dumps(dict(variants=dict(fixture=spec))))
                with self.assertRaises(ValueError):materialize(repository,archives,'fixture',output)
                self.assertFalse(output.exists())

    def test_existing_output_is_never_touched_even_with_bad_sources(self):
        with tempfile.TemporaryDirectory() as temporary:
            repository,archives,output,spec,_=self.fixture(Path(temporary))
            output.mkdir();marker=output/'keep';marker.write_bytes(b'keep')
            (archives/'source.py.gz').write_bytes(b'bad')
            (archives/'MANIFEST.json').write_text(json.dumps(dict(variants=dict(fixture=spec))))
            with self.assertRaises(FileExistsError):materialize(repository,archives,'fixture',output)
            self.assertEqual(marker.read_bytes(),b'keep')
            self.assertEqual(list(output.iterdir()),[marker])


if __name__=='__main__':unittest.main()
