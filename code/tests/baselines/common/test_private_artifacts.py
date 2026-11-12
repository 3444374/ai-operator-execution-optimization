"""Portable artifact semantics used by native queries and historical consumers."""
import hashlib
import json
import os
from pathlib import Path
import socket
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from src.infrastructure.private_artifacts import copy_private_tree, file_identity, json_metric, write_private_json


class ArtifactValueTests(unittest.TestCase):
    def test_upstream_infinite_errors_survive_nested_strict_json(self):
        values = {"errors": (float("inf"), float("-inf"), float("nan")),
                  "rows": [{"finite": 1.5, "missing": None, "valid": True}],
                  "text": "Infinity"}
        normalized = json_metric(values)
        self.assertEqual(json.dumps(normalized, allow_nan=False),
                         '{"errors": ["Infinity", "-Infinity", "NaN"], "rows": '
                         '[{"finite": 1.5, "missing": null, "valid": true}], "text": "Infinity"}')
        self.assertIsInstance(values["errors"], tuple)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "quality.json"
            write_private_json(path, normalized)
            self.assertEqual(json.loads(path.read_text()), normalized)
            with self.assertRaises(ValueError):
                write_private_json(Path(directory) / "raw.json", values)

    def test_file_identity_preserves_empty_binary_and_chunk_crossing_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.bin"
            for payload in (b"", bytes(range(256)), b"x" * (1048576 + 7) + b"\x00\xff"):
                with self.subTest(size=len(payload)):
                    path.write_bytes(payload)
                    expected = {"sha256": hashlib.sha256(payload).hexdigest(), "bytes": len(payload)}
                    self.assertEqual(file_identity(path), expected)
                    self.assertEqual(file_identity(str(path)), expected)
            with self.assertRaises(FileNotFoundError):
                file_identity(Path(directory) / "missing")

    def test_neutral_import_does_not_load_pg_or_execution_libraries(self):
        code_root = Path(__file__).resolve().parents[3]
        script = (
            "import sys; from src.infrastructure.private_artifacts import file_identity, json_metric; "
            "assert not any(name.startswith(('src.experiments.postgresql', 'psycopg', 'ray', 'daft')) "
            "for name in sys.modules)"
        )
        result = subprocess.run([sys.executable, "-B", "-c", script],
                                env={**os.environ, "PYTHONPATH": str(code_root)},
                                text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)


class EvidenceTreeCopyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='evidence-copy-', dir='/tmp')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        self.destination = self.root / 'copy'

    def test_regular_bytes_and_special_node_identities_without_following_or_removing(self):
        nested = self.source / 'nested'
        nested.mkdir()
        payload = bytes(range(256)) * 4096 + b'\x00\xff'
        (nested / 'binary').write_bytes(payload)
        (self.source / 'empty').touch()
        outside = self.root / 'outside'
        outside.mkdir()
        (outside / 'must-not-copy').write_text('external bytes')
        targets = {'relative': 'nested/binary', 'absolute': str(outside),
                   'broken': 'missing', 'cycle': '.'}
        for name, target in targets.items():
            (self.source / name).symlink_to(target)
        pipe = self.source / 'pipe'
        os.mkfifo(pipe)
        socket_path = nested / 'plasma_store'
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(server.close)
        server.bind(str(socket_path))
        identities = {path: path.lstat().st_ino for path in self.source.rglob('*')}

        result = copy_private_tree(self.source, self.destination)
        nodes = {node['path']: node for node in result['entries']}
        self.assertEqual(result['regular_files_verified'], 2)
        self.assertEqual((self.destination / 'nested/binary').read_bytes(), payload)
        self.assertEqual((self.destination / 'empty').read_bytes(), b'')
        for relative, expected in [('nested/binary', payload), ('empty', b'')]:
            node = nodes[relative]
            self.assertEqual(node['sha256'], hashlib.sha256(expected).hexdigest())
            self.assertEqual(node['bytes'], len(expected))
            self.assertEqual(node['storage'], 'copied_and_verified')
            self.assertEqual(stat.S_IMODE((self.destination / relative).stat().st_mode), 0o600)
        for name, target in targets.items():
            self.assertEqual(nodes[name]['type'], 'symlink')
            self.assertEqual(nodes[name]['link_target'], target)
            self.assertEqual(nodes[name]['storage'], 'metadata_only')
            self.assertFalse(os.path.lexists(self.destination / name))
            self.assertEqual(os.readlink(self.source / name), target)
        self.assertEqual(nodes['nested/plasma_store']['type'], 'socket')
        self.assertEqual(nodes['pipe']['type'], 'fifo')
        self.assertFalse((self.destination / 'nested/plasma_store').exists())
        self.assertFalse((self.destination / 'pipe').exists())
        self.assertEqual({p: p.lstat().st_ino for p in identities}, identities)
        self.assertTrue(stat.S_ISSOCK(socket_path.lstat().st_mode))
        self.assertTrue(stat.S_ISFIFO(pipe.lstat().st_mode))
        self.assertEqual((outside / 'must-not-copy').read_text(), 'external bytes')
        write_private_json(self.root / 'ARCHIVE-COPY.json', result)
        self.assertEqual(json.loads((self.root / 'ARCHIVE-COPY.json').read_text()), result)

    def test_existing_destination_nested_destination_and_git_are_rejected(self):
        self.destination.mkdir()
        previous = self.destination / 'previous'
        previous.write_bytes(b'keep')
        with self.assertRaises(FileExistsError):
            copy_private_tree(self.source, self.destination)
        self.assertEqual(previous.read_bytes(), b'keep')
        with self.assertRaisesRegex(ValueError, 'inside its source'):
            copy_private_tree(self.source, self.source / 'nested-copy')
        self.assertFalse((self.source / 'nested-copy').exists())
        checkout = self.root / 'checkout'
        checkout.mkdir(); (checkout / '.git').mkdir()
        with self.assertRaisesRegex(ValueError, 'outside a Git checkout'):
            copy_private_tree(self.source, checkout / 'private-copy')
        self.assertFalse((checkout / 'private-copy').exists())

    def test_missing_source_and_symlink_root_fail_without_following(self):
        with self.assertRaises(FileNotFoundError):
            copy_private_tree(self.root / 'missing', self.destination)
        alias = self.root / 'alias'
        alias.symlink_to(self.source)
        with self.assertRaisesRegex(ValueError, 'not a symlink'):
            copy_private_tree(alias, self.destination)
        self.assertFalse(self.destination.exists())

    def test_read_error_propagates_with_partial_evidence_and_source_preserved(self):
        first = self.source / 'a-first'
        first.write_bytes(b'already copied')
        unreadable = self.source / 'z-unreadable'
        unreadable.write_bytes(b'keep source')
        failure = PermissionError('fixture permission denied')
        original_open = os.open
        def open_file(path, *args, **kwargs):
            if Path(path) == unreadable:
                raise failure
            return original_open(path, *args, **kwargs)
        with patch('src.infrastructure.private_artifacts.os.open', side_effect=open_file):
            with self.assertRaises(PermissionError) as raised:
                copy_private_tree(self.source, self.destination)
        self.assertIs(raised.exception, failure)
        self.assertEqual((self.destination / 'a-first').read_bytes(), b'already copied')
        self.assertEqual(unreadable.read_bytes(), b'keep source')
        self.assertFalse((self.destination / 'z-unreadable').exists())

    def test_verification_mismatch_is_not_reported_as_success_or_deleted(self):
        source = self.source / 'data'
        source.write_bytes(b'original')
        with patch('src.infrastructure.private_artifacts.file_identity',
                   return_value=dict(sha256='wrong', bytes=8)):
            with self.assertRaisesRegex(OSError, 'verification failed'):
                copy_private_tree(self.source, self.destination)
        self.assertEqual(source.read_bytes(), b'original')
        self.assertEqual((self.destination / 'data').read_bytes(), b'original')
