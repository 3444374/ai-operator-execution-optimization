"""Read retained evidence locally from an external copy or its fixed Git blob."""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile


ROOT = Path(__file__).resolve().parents[3]
INDEX = Path(__file__).with_name('retained-evidence.json')


def read_bytes(path):
    path = Path(path)
    index = json.loads(INDEX.read_text())
    try:
        key = path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        key = None
    record = index['files'].get(key)
    if record is None:
        matches = [v for k, v in index['files'].items() if k.endswith('/' + path.name)]
        if len(matches) == 1:
            record = matches[0]
    if path.is_file():
        raw = path.read_bytes()
    elif record is not None:
        process = subprocess.run(['git', '--no-optional-locks', '-C', str(ROOT),
                                  'show', record['commit'] + ':' + record['path']],
                                 capture_output=True, timeout=30)
        if process.returncode:
            raise FileNotFoundError('Restore the retained file from ' + record['url']
                                    + ' into the result directory; no network was attempted.')
        raw = process.stdout
    else:
        raise FileNotFoundError(path)
    if record is not None and (len(raw) != record['bytes']
                              or hashlib.sha256(raw).hexdigest() != record['sha256']):
        raise ValueError('Retained evidence differs from its fixed original: ' + path.name)
    return raw


def read_json(path):
    data = json.loads(read_bytes(path))
    if isinstance(data, dict) and data.pop('units_share_quality', False):
        for unit in data['units']:
            if 'original_quality' in unit:
                raise ValueError('Shared quality must have one recorded representation')
            unit['original_quality'] = data['quality']
    return data


def open_archive(path):
    return tarfile.open(fileobj=io.BytesIO(read_bytes(path)), mode='r:gz')
