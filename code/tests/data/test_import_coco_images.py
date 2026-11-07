"""Importer resource ownership with local ZIP data and a fake database."""

from contextlib import nullcontext
import io
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch
from zipfile import ZipFile

from scripts.data import import_coco_images as importer


class CocoImportResourceTests(unittest.TestCase):
    def test_zip_and_connection_close_on_success_and_raised_errors(self):
        for case in ('success', 'connect', 'commit', 'load', 'verify'):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'images.zip'
                with ZipFile(path, 'w') as output:
                    output.writestr('train2017/000000000001.jpg', b'fixture')
                archive = ZipFile(path)
                error = RuntimeError('fixture database failure')
                writes = []
                cursor = Mock(spec=['execute', 'executemany', 'fetchone'])
                cursor.fetchone.return_value = (1, 1, 1, 1)

                def execute(query, *_args):
                    if case == 'verify' and str(query).startswith('SELECT count'):
                        raise error

                def insert(_query, rows):
                    if case == 'load':
                        raise error
                    writes.extend(rows)

                cursor.execute.side_effect = execute
                cursor.executemany.side_effect = insert
                connection = Mock(spec=['close', 'cursor', 'commit', 'transaction'])
                connection.cursor.side_effect = lambda: nullcontext(cursor)
                transaction = MagicMock()
                transaction.__exit__.return_value = False
                connection.transaction.return_value = transaction
                connection.commit.side_effect = error if case == 'commit' else None
                connect = Mock(return_value=connection,
                    side_effect=error if case == 'connect' else None)
                database = SimpleNamespace(connect=connect,
                    sql=SimpleNamespace(SQL=lambda value: value, Identifier=str))
                args = SimpleNamespace(zip=str(path), dir=None, pg_dsn='fixture',
                    pattern='*.jpg', limit=0, offset=0, table='image_documents',
                    workload='fixture-workload', batch=1)
                expected = None if case == 'success' else SystemExit if case == 'load' else RuntimeError
                try:
                    with patch.object(importer, 'parse_args', return_value=args), \
                         patch.object(importer, 'ZipFile', return_value=archive), \
                         patch.object(importer, 'get_versions', return_value={'server_version':'fixture'}), \
                         patch.object(importer, 'primary_key_columns', return_value=('workload_name', 'doc_id')), \
                         patch.dict(sys.modules, {'psycopg':database}), \
                         patch('sys.stdout', new_callable=io.StringIO), \
                         patch('sys.stderr', new_callable=io.StringIO):
                        with self.assertRaises(expected) if expected else nullcontext() as stopped:
                            importer.main()
                    if expected is RuntimeError:
                        self.assertIs(stopped.exception, error)
                    elif expected is SystemExit:
                        self.assertEqual(stopped.exception.code, 4)
                    self.assertEqual((archive.fp is None, connection.close.call_count),
                                     (True, 0 if case == 'connect' else 1))
                    if case in ('success', 'load', 'verify'):
                        self.assertEqual(transaction.__exit__.call_args.args[0],
                                         RuntimeError if case == 'load' else None)
                    if case == 'success':
                        self.assertEqual(writes, [(1, 'fixture-workload', b'fixture', 7)])
                finally:
                    archive.close()
