"""Execute the real observer wrapper, including exceptional session closure."""
import socket
import struct
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch, Mock
from src.experiments import gateway_observer as observer
from src.experiments.postgresql.provider_session_attribution import session_windows
from src.experiments.postgresql import semmap_resource_gateway_observer as fixture_cli


class ObserverTests(unittest.TestCase):
    def test_fixture_entry_refuses_fixed_model_without_creating_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / 'events.jsonl'
            with patch.object(fixture_cli.server, 'main') as gateway, self.assertRaises(SystemExit) as error:
                fixture_cli.main(['--events', str(events), '--', '--socket', 'unused',
                                  '--fixed-model-config', 'unused-config'])
            self.assertEqual(error.exception.code, 2)
            gateway.assert_not_called()
            self.assertFalse(events.exists())

    def test_peer_and_inode_reads(self):
        connection=Mock()
        connection.getsockopt.return_value=struct.pack("3i",42,100,100)
        with patch.object(socket,"SO_PEERCRED",17,create=True),patch.object(observer.os,"readlink",return_value="socket:[51]"):
            self.assertEqual(observer._peer_credentials(connection),(42,100,100))
            self.assertEqual(observer._accepted_inode(connection),51)
        connection.getsockopt.side_effect=OSError()
        with patch.object(socket,"SO_PEERCRED",17,create=True):
            self.assertIsNone(observer._peer_credentials(connection))

    def test_normal_session_closes_and_records_complete_without_body(self):
        events=[]
        wrapper=observer.SessionObserver(events.append)
        connection=SimpleNamespace(fd=9)
        connection.fileno=lambda:connection.fd
        request=SimpleNamespace(semantic_payload_digest="digest",body="private-input")
        def run(conn):
            result=wrapper.complete(request,lambda _:"private-output")
            conn.fd=-1
            return result
        with patch.object(observer,"_peer_credentials",return_value=(1,2,3)),patch.object(observer,"_accepted_inode",return_value=9):
            self.assertEqual(wrapper.run_session(connection,run),"private-output")
        self.assertEqual([e["event"] for e in events],["session_start","task","task_complete","session_end"])
        self.assertTrue(events[-1]["connection_closed"])
        self.assertNotIn("private-input",str(events))
        self.assertNotIn("private-output",str(events))
        self.assertEqual(len(session_windows(events)),1)

    def test_exception_finally_without_close_does_not_prove_drain(self):
        events=[]
        wrapper=observer.SessionObserver(events.append)
        connection=Mock()
        connection.fileno.return_value=9
        def failed(conn):
            raise RuntimeError("private-error")
        with patch.object(observer,"_peer_credentials",return_value=None),patch.object(observer,"_accepted_inode",return_value=9):
            with self.assertRaises(RuntimeError):
                wrapper.run_session(connection,failed)
        self.assertEqual(events[-1]["termination"],"raised")
        self.assertNotIn("private-error",str(events))
        with self.assertRaisesRegex(ValueError,"session_socket_not_closed"):
            session_windows(events)


if __name__ == "__main__": unittest.main()
