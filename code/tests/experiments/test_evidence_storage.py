"""Evidence restoration rejects corruption and unsafe writes before creating output."""

import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

from src.experiments.evidence_storage import SCHEMA, restore_result


class EvidenceStorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / 'repository'
        self.result = self.repo / 'experiments/results/example'
        self.result.mkdir(parents=True)
        self.output = self.root / 'restored'
        self.archive = self.result / 'raw/evidence.tar.gz'
        self.archive.parent.mkdir()
        self.payload = b'failure evidence\n'
        self.digest = hashlib.sha256(self.payload).hexdigest()
        with tarfile.open(self.archive, 'w:gz') as out:
            member = tarfile.TarInfo('objects/' + self.digest)
            member.size = len(self.payload)
            out.addfile(member, io.BytesIO(self.payload))
        self.entries = [{'path': 'raw/failed.log', 'bytes': len(self.payload), 'sha256': self.digest,
                         'storage': {'kind': 'archive', 'archive': self.archive.relative_to(self.repo).as_posix(),
                                     'member': 'objects/' + self.digest}}]
        self.header = {'schema': SCHEMA, 'result': self.result.relative_to(self.repo).as_posix(),
                       'archives': [{'path': self.archive.relative_to(self.repo).as_posix(),
                                     'bytes': self.archive.stat().st_size,
                                     'sha256': hashlib.sha256(self.archive.read_bytes()).hexdigest()}]}
        self.manifest = self.result / 'raw/storage-manifest.jsonl'

    def write_manifest(self):
        self.header.update(files=len(self.entries), original_bytes=sum(e['bytes'] for e in self.entries))
        self.manifest.write_text('\n'.join(json.dumps(o) for o in (self.header, *self.entries)) + '\n')

    def restore(self):
        self.write_manifest()
        return restore_result(self.manifest, self.output, repository=self.repo)

    def test_aliases_and_external_file_preserve_every_original_path(self):
        self.entries.append({**self.entries[0], 'path': 'other-run/failed.log'})
        kept = self.result / 'summary.csv'
        kept.write_bytes(b'elapsed\n1.5\n')
        self.entries.append({'path': 'summary.csv', 'bytes': kept.stat().st_size,
                             'sha256': hashlib.sha256(kept.read_bytes()).hexdigest(),
                             'storage': {'kind': 'file', 'path': kept.relative_to(self.repo).as_posix()}})
        result = self.restore()
        self.assertEqual(result['files_verified'], 3)
        self.assertEqual((self.output / 'raw/failed.log').read_bytes(), self.payload)
        self.assertEqual((self.output / 'other-run/failed.log').read_bytes(), self.payload)
        self.assertEqual((self.output / 'summary.csv').read_bytes(), kept.read_bytes())

    def test_changed_archive_is_rejected_without_output(self):
        self.archive.write_bytes(self.archive.read_bytes() + b'tampered')
        with self.assertRaisesRegex(ValueError, 'digest differ'):
            self.restore()
        self.assertFalse(self.output.exists())

    def test_entry_digest_mismatch_is_rejected_without_output(self):
        self.entries[0]['sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'digest differ'):
            self.restore()
        self.assertFalse(self.output.exists())

    def test_relative_traversal_and_duplicate_paths_are_rejected(self):
        self.entries[0]['path'] = '../escaped'
        with self.assertRaisesRegex(ValueError, 'invalid evidence path'):
            self.restore()
        self.entries[0]['path'] = 'raw/failed.log'
        self.entries.append(self.entries[0].copy())
        with self.assertRaisesRegex(ValueError, 'duplicate evidence path'):
            self.restore()
        self.assertFalse(self.output.exists())

    def test_sources_cannot_escape_through_symlink(self):
        outside = self.root / 'outside'
        outside.write_bytes(self.payload)
        link = self.result / 'link.log'
        link.symlink_to(outside)
        self.entries[0]['storage'] = {'kind': 'file', 'path': link.relative_to(self.repo).as_posix()}
        with self.assertRaisesRegex(ValueError, 'escapes repository'):
            self.restore()
        self.assertFalse(self.output.exists())

    def test_existing_output_and_output_in_git_are_rejected(self):
        self.output.mkdir()
        with self.assertRaises(FileExistsError):
            self.restore()
        self.output.rmdir()
        (self.root / '.git').mkdir()
        with self.assertRaisesRegex(ValueError, 'outside a Git checkout'):
            self.restore()
        self.assertFalse(self.output.exists())

    def test_directory_path_and_file_parent_conflict_are_rejected(self):
        self.entries[0]['path'] = '.'
        with self.assertRaisesRegex(ValueError, 'invalid evidence path'):
            self.restore()
        self.entries[0]['path'] = 'raw/failed.log'
        self.entries.append({**self.entries[0], 'path': 'raw/failed.log/nested'})
        with self.assertRaisesRegex(ValueError, 'conflicting evidence paths'):
            self.restore()
        self.assertFalse(self.output.exists())

    def test_tar_symlink_is_rejected_even_with_matching_archive_digest(self):
        with tarfile.open(self.archive, 'w:gz') as archive:
            member = tarfile.TarInfo('objects/' + self.digest)
            member.type = tarfile.SYMTYPE
            member.linkname = '/outside'
            archive.addfile(member)
        self.header['archives'][0].update(bytes=self.archive.stat().st_size,
                                         sha256=hashlib.sha256(self.archive.read_bytes()).hexdigest())
        with self.assertRaisesRegex(ValueError, 'non-file'):
            self.restore()
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
