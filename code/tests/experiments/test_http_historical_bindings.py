"""HTTP maintenance must retain exact bytes in every named historical source."""

import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from scripts.experiments.materialize_native_baseline import materialize


ROOT = Path(__file__).resolve().parents[3]
MANIFEST_DIR = ROOT / 'experiments/reproduction/native_baselines'
HTTP_SOURCES = (
    'code/src/execution_provider/adapters/incremental_execution.py',
    'code/src/execution_provider/adapters/map_organization.py',
    'code/src/execution_provider/adapters/model_config.py',
    'code/src/execution_provider/adapters/native_tasks.py',
    'code/src/execution_provider/adapters/openai_compatible_fixed.py',
    'code/src/execution_provider/server.py',
    'code/src/semantic_methods/lotus/batch.py',
    'code/src/semantic_methods/lotus/driver.py',
    'code/src/semantic_methods/lotus/limits.py',
    'code/src/semantic_methods/lotus/sdk.py',
)


class HttpHistoricalBindingTests(unittest.TestCase):
    def test_changed_http_sources_restore_through_original_projection_overrides(self):
        manifest = json.loads((MANIFEST_DIR / 'MANIFEST.json').read_text())
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / 'manifest'
            fixture.mkdir()
            specs = {}
            for variant, spec in manifest['variants'].items():
                expected = {name: spec['code_files_sha256'][name] for name in HTTP_SOURCES}
                overrides = {}
                for name in HTTP_SOURCES:
                    with self.subTest(variant=variant, path=name):
                        self.assertTrue(name in spec['overrides'], 'Missing exact HTTP source: ' + name)
                        source = MANIFEST_DIR / spec['overrides'][name]
                        self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(), expected[name])
                        destination = fixture / spec['overrides'][name]
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(source, destination)
                        overrides[name] = spec['overrides'][name]
                # This is an explicitly scoped fixture, with its own content ID.
                identity = hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest()
                specs[variant] = dict(code_files_sha256=expected, overrides=overrides,
                    code_projection_sha256=identity, original_source_revision=spec['original_source_revision'])
            (fixture / 'MANIFEST.json').write_text(json.dumps(dict(variants=specs)))
            for variant, spec in specs.items():
                with self.subTest(variant=variant):
                    output = Path(directory) / variant
                    report = materialize(ROOT, fixture, variant, output)
                    self.assertEqual(report['files'], len(HTTP_SOURCES))
                    self.assertEqual(report['models_started'], 0)
                    self.assertEqual(report['code_projection_sha256'], spec['code_projection_sha256'])
                    for name, expected in spec['code_files_sha256'].items():
                        self.assertEqual(hashlib.sha256((output / name).read_bytes()).hexdigest(), expected)


if __name__ == '__main__':
    unittest.main()
