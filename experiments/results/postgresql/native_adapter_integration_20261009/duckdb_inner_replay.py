"""Check retained DuckDB inner repair and summary recording fixtures."""
from collections import Counter
import gzip,hashlib,json
from pathlib import Path
root=Path(__file__).parent
identity=json.loads((root/'duckdb-inner-repair-verification.json').read_text())
archive=root/identity['public_archive']['path']
assert hashlib.sha256(archive.read_bytes()).hexdigest()==identity['public_archive']['sha256']
raw={};originals={}
for line in gzip.decompress(archive.read_bytes()).splitlines():
    row=json.loads(line);name=row['path'];text=row['text']
    assert not Path(name).is_absolute() and '..' not in Path(name).parts and name not in raw
    assert hashlib.sha256(text.encode()).hexdigest()==row['sha256']
    raw[name]=text;originals[name]=row['original_sha256']
assert len(raw)==identity['public_archive']['members']==250
def value(name):return json.loads(raw[name])
def lines(name):return [json.loads(line) for line in raw[name].splitlines() if line.strip()]
assert 'Ran 28 tests' in raw['duckdb-inner-fixture02/regression.txt'] and '\nOK' in raw['duckdb-inner-fixture02/regression.txt']
assert 'Ran 4 tests' in raw['duckdb-inner-fixture04/summary-regression.txt'] and '\nOK' in raw['duckdb-inner-fixture04/summary-regression.txt']
posts=queries=0
for number in (3,4):
    base='duckdb-inner-fixture0'+str(number)+'/'
    record=value(base+'result.json');assert record['status']=='passed' and record['real_model_posts']==0
    assert value(base+'source-verification.json')['files']==1028
    if number==4:assert record['source_commit']==identity['source_commit']
    for run in record['runs']:
        assert run['exit_code']==0
        prefix=base+run['arm']+'/'
        inputs=value(prefix+'fixture-inputs.json');requests=lines(prefix+'fixture-requests.jsonl');offset=0
        assert len(requests)==144
        for i,rows in enumerate(inputs):
            path=prefix+'supplier-query-'+str(i)+'/'
            timing=value(path+'persistent-query.json');summary=value(path+'summary.json')
            assert summary['status']=='passed' and timing['query_summary_sha256']==originals[path+'summary.json']
            assert Counter(tuple(row['row']) for row in lines(path+'q0/results.jsonl'))==Counter((row['row_id'],'ok') for row in rows)
            expected=Counter(row['text'] for row in rows);seen=Counter()
            for request in requests[offset:offset+len(rows)]:
                matches=[text for text in expected if text in json.dumps(request)];assert len(matches)==1;seen.update(matches)
            assert seen==expected;offset+=len(rows);queries+=1
            if number==4 and run['arm']=='duckdb-method-semloom':
                diagnostics=summary['identity']['diagnostics']
                assert diagnostics['bridge']['last_error'] is None and diagnostics['bridge']['last_cleanup_error'] is None
                assert diagnostics['executor']['last_cleanup_errors']==[]
                assert not any(diagnostics['executor']['last_close_report']['usage'].values())
        group=value(prefix+'group/group-summary.json');assert group['status']=='passed' and group['cleanup_errors']=={}
        posts+=len(requests)
assert posts==identity['normal_fixture_posts']==576 and queries==identity['normal_queries']==12
assert 'AssertionError' in raw['duckdb-inner-fixture01/owner.log']
assert "KeyError: 'SEMLOOM_SEMA_BINARY'" in raw['duckdb-inner-fixture02/duckdb-adapted-native.txt']
print(str(len(raw))+' members verified;28 DuckDB regressions and4 summary checks,12 normal queries/576 fixturePOST, final source'+identity['source_commit'][:8]+', preserved startup failures, zero modelPOST')
