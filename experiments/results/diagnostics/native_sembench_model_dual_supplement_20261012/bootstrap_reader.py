"""Preserved original startup reader functions for offline seam replay."""
def save(path,value):
 path=Path(path);tmp=path.with_suffix(path.suffix+'.tmp');tmp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n');tmp.replace(path)
def bootstrap(pid,namespace=None,require_live=True):
 value=json.loads((ROOT/'bootstrap'/f'{pid}.json').read_text())
 if value['pid']!=pid or value['helper_identity']!=harness_identity() or not value['installed'] or 'aiohttp' not in value['hooks']:raise RuntimeError('required effective send bootstrap differs')
 if require_live and start_ticks(pid)!=value['start_ticks']:raise RuntimeError('bootstrap process identity changed')
 if namespace is not None and value.get('namespace')!=namespace:raise RuntimeError('author bootstrap namespace differs')
 return value
def release_cli(child,deadline):
 target=ROOT/'bootstrap'/f'{child.pid}.json'
 until=min(deadline,time.monotonic()+20)
 while True:
  if child.poll() is not None:raise RuntimeError('public CLI exited before bootstrap confirmation')
  if target.exists():
   value=bootstrap(child.pid)
   if value.get('cli_waiting'):break
  if time.monotonic()>=until:raise TimeoutError('public CLI send bootstrap confirmation timed out')
  time.sleep(.02)
 save(target.with_name(f'{child.pid}.release.json'),dict(pid=child.pid,start_ticks=value['start_ticks'],helper_identity=value['helper_identity']))
 return value
