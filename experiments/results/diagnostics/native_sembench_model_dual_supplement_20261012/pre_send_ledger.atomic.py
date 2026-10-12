"""Private pass-through accounting; no request values or retry policy changed."""
from collections import Counter
import hashlib,json,os,sqlite3,time,uuid
from pathlib import Path
from urllib.parse import urlsplit
ROOT=Path(os.environ['MODELRUN_ROOT']);STATE=json.loads((ROOT/'state.json').read_text());DB=ROOT/'sends.sqlite'
_installed=False
_hooks={}
def start_ticks(pid):
 return int((Path('/proc')/str(pid)/'stat').read_text().rsplit(')',1)[1].split()[19])
def harness_identity():
 target=ROOT/'harness-identity.json';expected=json.loads(target.read_text())
 for name,digest in expected['files'].items():
  if hashlib.sha256((ROOT/'validation'/name).read_bytes()).hexdigest()!=digest:raise RuntimeError('run helper source differs from fixed identity')
 return hashlib.sha256(target.read_bytes()).hexdigest()
def initialize():
 with sqlite3.connect(DB,timeout=10) as db:
  db.execute('CREATE TABLE IF NOT EXISTS control (id INTEGER PRIMARY KEY,phase TEXT,slot TEXT,unit TEXT,started REAL,deadline REAL,reserve REAL,deadline_ns INTEGER)')
  db.execute('CREATE TABLE IF NOT EXISTS sends (sequence INTEGER PRIMARY KEY,phase TEXT,slot TEXT,unit TEXT,kind TEXT,pid INTEGER,url TEXT,at_epoch REAL,at_ns INTEGER,body_sha256 TEXT,values_sha256 TEXT,returned_ns INTEGER,status INTEGER,error_type TEXT)')
  db.execute('CREATE TABLE IF NOT EXISTS gateways (url TEXT PRIMARY KEY,pid INTEGER,start_ticks INTEGER,upstream TEXT,helper_identity TEXT)')
  db.execute('CREATE TABLE IF NOT EXISTS fixture_received (sequence INTEGER PRIMARY KEY,received_ns INTEGER)')
def activate(phase,slot,started,seconds,reserve):
 with sqlite3.connect(DB,timeout=10) as db:
  deadline_ns=time.monotonic_ns()+int((started+seconds-time.time())*1e9)
  db.execute('INSERT OR REPLACE INTO control VALUES(1,?,?,?,?,?,?,?)',(phase,slot,'',started,started+seconds,reserve,deadline_ns))
def slot(value):
 with sqlite3.connect(DB,timeout=10) as db:db.execute('UPDATE control SET slot=?,unit=? WHERE id=1',(value,''))
def unit(value):
 with sqlite3.connect(DB,timeout=10) as db:db.execute('UPDATE control SET unit=? WHERE id=1',(value,))
def counts(phase=None):
 with sqlite3.connect(DB,timeout=10) as db:
  sql='SELECT kind,count(*) FROM sends'+(' WHERE phase=?' if phase else '')+' GROUP BY kind'
  rows=dict(db.execute(sql,(phase,) if phase else ()))
 return dict(total=sum(rows.values()),gateway=rows.get('gateway',0),router=rows.get('router',0),model=rows.get('model',0),fixture=rows.get('fixture',0))
def select(method,url):
 if str(method).upper()!='POST':return None
 p=urlsplit(str(url))
 if p.hostname not in ('localhost','127.0.0.1','::1'):raise RuntimeError('run refuses external POST')
 if str(url)==os.environ.get('MODELRUN_FIXTURE_URL'):return 'fixture'
 if str(url)==os.environ.get('MODELRUN_ROUTER_URL'):return 'router'
 if str(url) in json.loads(os.environ.get('MODELRUN_MODEL_ENDPOINTS','[]')):return 'model'
 with sqlite3.connect(DB,timeout=10) as db:
  row=db.execute('SELECT pid,start_ticks FROM gateways WHERE url=?',(str(url),)).fetchone()
 if row is not None:
  if start_ticks(row[0])!=row[1]:raise RuntimeError('registered gateway owner identity changed')
  return 'gateway'
 raise RuntimeError('unidentified localhost POST refused before send')
def reserve(kind,url,body):
 with sqlite3.connect(DB,timeout=10) as db:
  assert_hooks()
  db.execute('BEGIN IMMEDIATE');control=db.execute('SELECT phase,slot,unit,started,deadline,reserve,deadline_ns FROM control WHERE id=1').fetchone()
  if control is None:raise RuntimeError('POST before independent allowance activated')
  phase,case,query,at,deadline,cleanup,deadline_ns=control
  if time.monotonic_ns()>=deadline_ns-int(cleanup*1e9):raise TimeoutError('phase preserves cleanup reserve')
  maximum,post_max=(STATE['minimal_sync_public_cli_software']['max_logical'],STATE['minimal_sync_public_cli_software']['max_posts']) if phase=='software' else STATE['matrix']['stages'][phase][:2]
  total=db.execute('SELECT count(*) FROM sends WHERE phase=?',(phase,)).fetchone()[0]
  logical=db.execute("SELECT count(*) FROM sends WHERE phase=? AND kind='gateway'",(phase,)).fetchone()[0]
  if total>=post_max or kind=='gateway' and logical>=maximum:raise RuntimeError('independent phase allowance exhausted before send')
  if phase!='software':
   all_posts=db.execute("SELECT count(*) FROM sends WHERE phase!='software'").fetchone()[0]
   all_logical=db.execute("SELECT count(*) FROM sends WHERE phase!='software' AND kind='gateway'").fetchone()[0]
   if all_posts>=STATE['matrix']['hard_transmission_total'] or kind=='gateway' and all_logical>=STATE['matrix']['hard_logical_total']:raise RuntimeError('model total allowance exhausted')
   overall=ROOT/'model-start.json'
   if not overall.exists() or time.monotonic_ns()>=json.loads(overall.read_text())['deadline_monotonic_ns']-120_000_000_000:raise TimeoutError('model overall cleanup reserve reached')
  value_digest=None
  if isinstance(body,bytes):
   try:value_digest=hashlib.sha256(json.dumps(json.loads(body),sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()).hexdigest()
   except (ValueError,UnicodeError):pass
  cur=db.execute('INSERT INTO sends(phase,slot,unit,kind,pid,url,at_epoch,at_ns,body_sha256,values_sha256) VALUES(?,?,?,?,?,?,?,?,?,?)',(phase,case,query,kind,os.getpid(),str(url),time.time(),time.monotonic_ns(),hashlib.sha256(body).hexdigest() if isinstance(body,bytes) else None,value_digest));return cur.lastrowid

def finish(seq,response=None,error=None):
 with sqlite3.connect(DB,timeout=10) as db:db.execute('UPDATE sends SET returned_ns=?,status=?,error_type=? WHERE sequence=?',(time.monotonic_ns(),getattr(response,'status',getattr(response,'status_code',None)),type(error).__name__ if error else '',seq))
def claim_fixture(body):
 """The fixture refuses a send without its matching durable outgoing reserve."""
 with sqlite3.connect(DB,timeout=10) as db:
  db.execute('BEGIN IMMEDIATE')
  digest=hashlib.sha256(body).hexdigest()
  row=db.execute("SELECT s.sequence FROM sends s LEFT JOIN fixture_received r ON r.sequence=s.sequence WHERE s.phase='software' AND s.kind='fixture' AND s.body_sha256=? AND r.sequence IS NULL AND s.returned_ns IS NULL ORDER BY s.sequence LIMIT 1",(digest,)).fetchone()
  if row is None:raise RuntimeError('fixture refuses unaccounted transport')
  db.execute('INSERT INTO fixture_received VALUES(?,?)',(row[0],time.monotonic_ns()))
  return row[0]
def assert_hooks():
 if not _installed:raise RuntimeError('run send hooks are not installed')
 for name,(owner,attribute,expected) in _hooks.items():
  if getattr(owner,attribute) is not expected:raise RuntimeError('run send hook was replaced: '+name)
 if not _hooks or 'aiohttp' not in _hooks:raise RuntimeError('required real HTTP client accounting missing')
def _write_bootstrap(target,value):
 temporary=target.with_name(target.name+'.'+uuid.uuid4().hex+'.tmp')
 try:
  temporary.write_text(json.dumps(value)+'\n');temporary.replace(target)
 finally:
  if temporary.exists():temporary.unlink()
def await_cli_release():
 assert_hooks();ticks=start_ticks(os.getpid());marker=ROOT/'bootstrap'/f'{os.getpid()}.json'
 value=json.loads(marker.read_text());value.update(cli_waiting=True,start_ticks=ticks)
 _write_bootstrap(marker,value);release=ROOT/'bootstrap'/f'{os.getpid()}.release.json';until=time.monotonic()+20
 while not release.exists():
  if time.monotonic()>=until:raise TimeoutError('public CLI startup approval handshake timed out')
  time.sleep(.02)
 if json.loads(release.read_text())!={'pid':os.getpid(),'start_ticks':ticks,'helper_identity':value['helper_identity']}:raise RuntimeError('public CLI startup identity mismatch')
 assert_hooks()
def install_gateway_registration():
 from src.observability.request_gateway import ObservationGateway
 original=ObservationGateway.endpoint_url
 def registered(self,*args,**kwargs):
  result=original(self,*args,**kwargs)
  allowed={os.environ.get('MODELRUN_FIXTURE_URL'),os.environ.get('MODELRUN_ROUTER_URL'),*json.loads(os.environ.get('MODELRUN_MODEL_ENDPOINTS','[]'))}
  routes=tuple(self._routes.values())
  if not all(route.upstream_url in allowed for route in routes):raise RuntimeError('gateway upstream is not a registered run endpoint')
  with sqlite3.connect(DB,timeout=10) as db:
   db.execute('INSERT OR REPLACE INTO gateways VALUES(?,?,?,?,?)',(result,os.getpid(),start_ticks(os.getpid()),routes[0].upstream_url,harness_identity()))
  return result
 ObservationGateway.endpoint_url=registered
 _hooks['gateway_registration']=(ObservationGateway,'endpoint_url',registered)
def install():
 global _installed
 if _installed:assert_hooks();return
 identity=harness_identity()
 initialize();versions={}
 try:
  import aiohttp
 except ImportError:aiohttp=None
 if aiohttp:
  versions['aiohttp']=aiohttp.__version__;original=aiohttp.ClientSession._request
  async def aio(self,method,url,*args,**kwargs):
   kind=select(method,url)
   if kind is None:return await original(self,method,url,*args,**kwargs)
   seq=reserve(kind,url,kwargs.get('data'))
   try:r=await original(self,method,url,*args,**kwargs)
   except BaseException as e:finish(seq,error=e);raise
   finish(seq,r);return r
  aiohttp.ClientSession._request=aio
  _hooks['aiohttp']=(aiohttp.ClientSession,'_request',aio)
 try:
  import httpx
 except ImportError:httpx=None
 if httpx:
  versions['httpx']=httpx.__version__;sync_original=httpx.Client.send;async_original=httpx.AsyncClient.send
  def sync(self,request,*args,**kwargs):
   if isinstance(self._transport,httpx.MockTransport):return sync_original(self,request,*args,**kwargs)
   kind=select(request.method,request.url)
   if kind is None:return sync_original(self,request,*args,**kwargs)
   seq=reserve(kind,request.url,request.content)
   try:r=sync_original(self,request,*args,**kwargs)
   except BaseException as e:finish(seq,error=e);raise
   finish(seq,r);return r
  async def asynchronous(self,request,*args,**kwargs):
   if isinstance(self._transport,httpx.MockTransport):return await async_original(self,request,*args,**kwargs)
   kind=select(request.method,request.url)
   if kind is None:return await async_original(self,request,*args,**kwargs)
   seq=reserve(kind,request.url,request.content)
   try:r=await async_original(self,request,*args,**kwargs)
   except BaseException as e:finish(seq,error=e);raise
   finish(seq,r);return r
  httpx.Client.send=sync;httpx.AsyncClient.send=asynchronous
  _hooks['httpx_sync']=(httpx.Client,'send',sync);_hooks['httpx_async']=(httpx.AsyncClient,'send',asynchronous)
 if aiohttp is None:raise RuntimeError('required aiohttp runtime missing')
 install_gateway_registration();_installed=True;assert_hooks()
 marker=ROOT/'bootstrap';marker.mkdir(exist_ok=True)
 _write_bootstrap(marker/f'{os.getpid()}.json',dict(pid=os.getpid(),start_ticks=start_ticks(os.getpid()),versions=versions,installed=True,hooks=list(_hooks),helper_identity=identity,namespace=os.environ.get('IMLANE_NAMESPACE'),scope=os.environ.get('MODELRUN_SCOPE'),model_counter_scope='real localhost sends; MockTransport excluded'))
