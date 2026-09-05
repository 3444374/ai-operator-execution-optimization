"""Check isolated-cluster configuration and shutdown without a local PG install."""
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from src.experiments.postgresql.runtime_helpers import isolated_pg18_cluster


class ClusterConfigurationTests(unittest.TestCase):
    def test_port_and_owner_are_resolved_once_and_cluster_stops_on_failure(self):
        with tempfile.TemporaryDirectory(prefix='pg config ') as directory:
            root = Path(directory)
            user = SimpleNamespace(pw_name='fixture_owner', pw_uid=123, pw_gid=123)
            connection = MagicMock()
            connection.execute.return_value.fetchone.return_value = ('18.3',)
            driver = MagicMock()
            driver.connect.return_value.__enter__.return_value = connection
            with patch.dict('sys.modules', {'psycopg': driver}), \
                 patch('src.experiments.postgresql.runtime_helpers.os.getuid', return_value=123), \
                 patch('src.experiments.postgresql.runtime_helpers.os.chown'), \
                 patch('src.experiments.postgresql.runtime_helpers.subprocess.run') as run:
                with self.assertRaisesRegex(RuntimeError, 'query failed'):
                    with isolated_pg18_cluster(root/'prefix', root, user, port=55499) as actual:
                        self.assertIs(actual, connection)
                        raise RuntimeError('query failed')
            driver.connect.assert_called_once_with(host=str(root/'socket'), port=55499,
                user='fixture_owner', dbname='postgres', autocommit=True)
            commands = [call.args[0] for call in run.call_args_list]
            self.assertEqual(commands[0][-2:], ['-U', 'fixture_owner'])
            self.assertIn(f"-k '{root / 'socket'}' -p 55499", commands[1][commands[1].index('-o') + 1])
            self.assertEqual(commands[-1][-4:], ['-m', 'fast', '-w', 'stop'])


if __name__ == '__main__': unittest.main()
