"""Compare Filter INSERT lowering on two isolated PG18.3 extension installs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import pwd
import subprocess

p = argparse.ArgumentParser(description=__doc__)
for key in ('root', 'baseline-prefix', 'current-prefix'):
    p.add_argument('--' + key, required=True, type=Path)
a = p.parse_args()
root = a.root / 'insert-diagnostic'
root.mkdir()
socket = root / 'socket'
socket.mkdir()
user = pwd.getpwnam('postgres')
for directory in (root, socket):
    os.chown(directory, user.pw_uid, user.pw_gid)
as_pg = ['runuser', '-u', 'postgres', '--']
data = root / 'data'
subprocess.run(as_pg + [str(a.current_prefix / 'bin/initdb'), '-D', str(data),
    '--no-locale', '-E', 'UTF8'], check=True, stdout=subprocess.DEVNULL)
records = []
for label, prefix in (('baseline', a.baseline_prefix), ('current', a.current_prefix)):
    ctl = as_pg + [str(prefix / 'bin/pg_ctl'), '-D', str(data)]
    subprocess.run(ctl + ['-l', str(root / (label + '.log')), '-o',
        f"-c listen_addresses='' -c shared_preload_libraries=semloom_pg -k {socket} -p 55443",
        '-w', 'start'], check=True, stdout=subprocess.DEVNULL)
    psql = as_pg + [str(prefix / 'bin/psql'), '-XAtq', '-h', str(socket), '-p', '55443',
        '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-v', 'VERBOSITY=verbose']
    try:
        if label == 'baseline':
            subprocess.run(psql, input='CREATE EXTENSION semloom_pg; CREATE TABLE inputs(id int, content text); '
                "INSERT INTO inputs VALUES(1,'database'); CREATE TABLE sink(id int);", text=True, check=True)
        for profile in ('recording', 'exact-v3', 'choice-v4'):
            if profile == 'recording':
                predicate = 'ai_semantic.filter(content)'
            else:
                options = dict(model='golden-model-v1', temperature=0, max_tokens=8)
                if profile == 'choice-v4':
                    options['generation_profile'] = 'semloom.generation.choice.tristate.v1'
                predicate = "ai_semantic.filter(content, 'Classify input.', '" + json.dumps(options) + "'::jsonb)"
            query = 'SELECT id FROM inputs WHERE ' + predicate
            for operation, sql in (('select-plan', 'EXPLAIN (COSTS OFF) ' + query),
                                   ('insert-plan', 'EXPLAIN (COSTS OFF) INSERT INTO sink ' + query),
                                   ('insert-execution', "SET statement_timeout='300ms'; BEGIN; INSERT INTO sink " + query)):
                result = subprocess.run(psql, input=sql, text=True, capture_output=True)
                records.append(dict(install=label, profile=profile, operation=operation,
                    exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr,
                    extension_sha256=hashlib.sha256((prefix / 'lib/semloom_pg.so').read_bytes()).hexdigest()))
    finally:
        subprocess.run(ctl + ['-m', 'fast', '-w', 'stop'], check=True, stdout=subprocess.DEVNULL)
(root / 'comparison.json').write_text(json.dumps(records, indent=2) + '\n')
for record in records:
    print(record['install'], record['profile'], record['operation'], record['exit_code'],
          'Custom Scan' in record['stdout'], record['stderr'].splitlines()[:1])
