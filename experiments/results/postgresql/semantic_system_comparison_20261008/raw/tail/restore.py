"""Restore exact public export bytes into a temporary directory and verify hashes."""
from pathlib import Path
import gzip, hashlib, json, shutil, sys


def restore(source, dest):
    source, dest = Path(source), Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((source/'restore-manifest.json').read_text())
    for name, item in manifest.items():
        if Path(name).name != name: raise ValueError('unsafe output name')
        target = dest/name
        if target.exists(): raise ValueError('restore destination already contains '+name)
        if item['kind'] == 'plain':
            shutil.copyfile(source/item['packed_file'], target)
        elif item['kind'] == 'bytes-gzip':
            with gzip.open(source/item['packed_file'],'rb') as src, target.open('xb') as out:
                shutil.copyfileobj(src,out)
        elif item['kind'] == 'dictionary-jsonl-gzip':
            with gzip.open(source/item['dictionary_file'],'rt') as f: strings=json.load(f)
            def decode(v):
                if isinstance(v,list):return [decode(x) for x in v]
                if isinstance(v,dict):
                    if set(v)=={'__tail_string_ref__'}:return strings[v['__tail_string_ref__']]
                    return {k:decode(x) for k,x in v.items()}
                return v
            h=hashlib.sha256();size=count=0
            with gzip.open(source/item['packed_file'],'rt') as src, target.open('xb') as raw:
                with gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as out:
                    for line in src:
                        data=(json.dumps(decode(json.loads(line)),ensure_ascii=False)+'\n').encode()
                        out.write(data);h.update(data);size+=len(data);count+=1
            if (h.hexdigest()!=item['original_uncompressed_sha256'] or size!=item['original_uncompressed_bytes']
                    or count!=item['records']):raise ValueError('uncompressed identity differs: '+name)
        else:raise ValueError('unknown encoding')
        h=hashlib.sha256()
        with target.open('rb') as f:
            for b in iter(lambda:f.read(1048576),b''):h.update(b)
        if target.stat().st_size!=item['original_bytes'] or h.hexdigest()!=item['original_sha256']:
            raise ValueError('restored file identity differs: '+name)
    return dict(status='passed',restored_files=len(manifest),
                original_bytes=sum(v['original_bytes'] for v in manifest.values()))


if __name__=='__main__':print(json.dumps(restore(sys.argv[1],sys.argv[2])))
