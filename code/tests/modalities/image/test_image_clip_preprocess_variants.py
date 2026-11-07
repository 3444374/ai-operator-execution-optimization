from __future__ import annotations

import sys
import importlib
import io
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np


CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
SCRIPTS_ROOT = CODE_ROOT / "scripts"
for path in (CODE_ROOT, SCRIPTS_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from scripts.profiling.profile_image_clip_preprocess_variants import (  # noqa: E402
    _parity,
    _payload_work_units,
)


class ImageClipPreprocessVariantTests(unittest.TestCase):
    def test_payload_work_uses_rows_and_spatial_shape(self) -> None:
        payload = np.zeros((2, 3, 224, 224), dtype=np.float32)

        self.assertEqual(_payload_work_units(payload, 2), 2 * 224 * 224)

    def test_embedding_parity_reports_cosine_and_max_error(self) -> None:
        # Identical vectors need cosine=1 even when their norms are not exactly 1.
        reference = np.asarray([[0.5, 0.0], [0.0, 0.75]], dtype=np.float32)
        candidate = reference.copy()

        mean_cosine, min_cosine, max_abs = _parity(candidate, reference)

        self.assertEqual(mean_cosine, 1.0)
        self.assertEqual(min_cosine, 1.0)
        self.assertEqual(max_abs, 0.0)


class ImageClipProfilerEntryTests(unittest.TestCase):
    def test_package_entrypoints_reach_the_database_connection(self):
        args = SimpleNamespace(model='CPU-fixture-model', pg_dsn='fixture',
            limit=1, warmup=0, repeats=1, batch_sizes='1')
        for name in ('profile_clip_preproc_stages', 'profile_image_clip_preprocess_variants'):
            with self.subTest(entry=name):
                module = importlib.import_module('scripts.profiling.' + name)
                connect = Mock(side_effect=RuntimeError('CPU fixture connect'))
                processor = SimpleNamespace(from_pretrained=Mock(
                    return_value=SimpleNamespace(image_processor=object())))
                with patch.object(module, 'parse_args', return_value=args), \
                     patch.dict(sys.modules, {'psycopg': SimpleNamespace(connect=connect),
                         'torch': SimpleNamespace(),
                         'transformers': SimpleNamespace(CLIPProcessor=processor)}), \
                     patch('sys.stdout', new_callable=io.StringIO):
                    with self.assertRaisesRegex(RuntimeError, 'CPU fixture connect'):
                        module.main()
                connect.assert_called_once_with('fixture')

    def test_connection_error_keeps_the_reason_without_passwords(self):
        module = importlib.import_module('scripts.profiling.profile_image_clip_bottleneck')
        dsn = 'postgresql://<user>:<password>@localhost/<database>'
        connect = Mock(side_effect=RuntimeError('fixture connection refused: ' + dsn))
        with patch.object(module, 'parse_args', return_value=SimpleNamespace(pg_dsn=dsn)), \
             patch.dict(sys.modules, {'psycopg': SimpleNamespace(connect=connect)}), \
             patch('sys.stderr', new_callable=io.StringIO) as error:
            with self.assertRaises(SystemExit) as stopped:
                module.main()
        self.assertEqual(stopped.exception.code, 2)
        self.assertIn('fixture connection refused', error.getvalue())
        self.assertNotIn('<password>', error.getvalue())
        self.assertNotIn(dsn, error.getvalue())


if __name__ == "__main__":
    unittest.main()
