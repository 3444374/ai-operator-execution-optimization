"""Owned-channel byte observation; payloads and execution semantics stay native."""
from collections import OrderedDict
import functools
import json
import os
import socket
import threading
import time
import uuid
from src.serving.backends.async_engine_process import AsyncEngineProcess
from record_observer import ROOT, ENABLED, emit, emit_feedback, emit_buffered, ids, core_key, settings, install_cpu_hooks, handoff_profile, close_events

_frames=OrderedDict()
_lock=threading.Lock()
_installed=False
_REAL_SOCKET=socket
_OPS={'open','ready','submit','accepted','terminal','feedback','abort','drain','drained','close','closed','error','producer_quiescent'}


def frame_metadata(value):
    if not isinstance(value,dict):return {}
    return dict(op=value.get('op') if value.get('op') in _OPS else 'other',
                request_id=value.get('request_id') if type(value.get('request_id')) is str else None,
                owner_uid=value.get('owner_uid') if type(value.get('owner_uid')) is str else None)


class ObservedSocket:
    """Delegates precisely one owned socket. Never wraps unrelated connections."""
    def __init__(self,sock,channel):
        self._socket=sock;self.channel=channel;self.socket_id=uuid.uuid4().hex
        self.sent=self.received=self.decoded=0
        self.frame_sequence=0

    def __getattr__(self,name):return getattr(self._socket,name)

    def send(self,data,*args,**kwargs):
        start,cpu=time.monotonic_ns(),time.thread_time_ns()
        try:count=self._socket.send(data,*args,**kwargs)
        except BaseException as error:
            emit('socket_io',channel=self.channel,direction='send',socket_id=self.socket_id,actual_socket_bytes=None,error_type=type(error).__name__)
            raise
        first=self.sent;self.sent+=count
        obj=data.obj if isinstance(data,memoryview) else data
        with _lock:
            frame=_frames.get(id(obj))
            if frame is not None:
                frame['sent']+=count
                if frame['sent']>=frame['bytes']:_frames.pop(id(obj),None)
        emit('socket_io',channel=self.channel,direction='send',socket_id=self.socket_id,
             encoded_frame_id=None if frame is None else frame['id'],actual_socket_bytes=count,
             stream_start=first,stream_end=self.sent,start_ns=start,end_ns=time.monotonic_ns(),
             thread_cpu_start_ns=cpu,thread_cpu_end_ns=time.thread_time_ns(),
             scope='actual successful socket.send return; partial writes retained')
        return count

    def recv(self,*args,**kwargs):
        start,cpu=time.monotonic_ns(),time.thread_time_ns()
        try:data=self._socket.recv(*args,**kwargs)
        except BaseException as error:
            emit('socket_io',channel=self.channel,direction='recv',socket_id=self.socket_id,actual_socket_bytes=None,error_type=type(error).__name__)
            raise
        first=self.received;self.received+=len(data)
        emit('socket_io',channel=self.channel,direction='recv',socket_id=self.socket_id,
             actual_socket_bytes=len(data),stream_start=first,stream_end=self.received,
             start_ns=start,end_ns=time.monotonic_ns(),thread_cpu_start_ns=cpu,thread_cpu_end_ns=time.thread_time_ns(),
             scope='actual socket.recv bytes; zero means EOF; no per-request split of mixed chunks')
        return data

    def frame_decoded(self,value,payload_bytes):
        first=self.decoded;self.decoded+=4+payload_bytes;self.frame_sequence+=1
        emit('frame_decoded',channel=self.channel,socket_id=self.socket_id,
             frame_id=self.socket_id+':recv:'+str(self.frame_sequence),
             frame_start=first,frame_end=self.decoded,payload_bytes=payload_bytes,encoded_frame_bytes=4+payload_bytes,
             **frame_metadata(value),scope='decoded protocol frame span in observed receive stream; not a second wire-byte counter')


class SocketFactory:
    def __init__(self,original,channels):self.original=original;self.channels=channels
    def __getattr__(self,name):return getattr(self.original,name)
    def socket(self,*args,**kwargs):
        value=self.original.socket(*args,**kwargs)
        channel=self.channels.get(kwargs.get('fileno'))
        return ObservedSocket(value,channel) if channel is not None else value


class JsonCodec:
    def __init__(self,original):self.original=original
    def __getattr__(self,name):return getattr(self.original,name)
    def loads(self,data,*args,**kwargs):
        start,cpu=time.monotonic_ns(),time.thread_time_ns()
        try:
            value=self.original.loads(data,*args,**kwargs)
            emit('frame_codec_decode',**frame_metadata(value),payload_bytes=len(data) if isinstance(data,(bytes,bytearray)) else None,
                 start_ns=start,end_ns=time.monotonic_ns(),thread_cpu_start_ns=cpu,thread_cpu_end_ns=time.thread_time_ns(),
                 scope='actual JSON decode bracket; no payload retained')
            return value
        except BaseException as error:
            emit('frame_codec_error',error_type=type(error).__name__);raise


def install_transport(*,child):
    global _installed
    if not ENABLED or handoff_profile() or _installed:return
    _installed=True
    from src.serving.backends import async_engine_process as module
    original=module._frame
    @functools.wraps(original)
    def frame(value,maximum):
        start,cpu=time.monotonic_ns(),time.thread_time_ns()
        packet=original(value,maximum)
        identity=uuid.uuid4().hex
        with _lock:
            _frames[id(packet)]={'id':identity,'bytes':len(packet),'sent':0}
            while len(_frames)>settings()['max_pending_frame_metadata']:
                _frames.popitem(last=False)
                emit('frame_metadata_evicted',unknown='encoded frame metadata capacity; actual socket counts remain observed')
        emit('frame_encoded',encoded_frame_id=identity,encoded_frame_bytes=len(packet),payload_bytes=len(packet)-4,
             **frame_metadata(value),start_ns=start,end_ns=time.monotonic_ns(),
             thread_cpu_start_ns=cpu,thread_cpu_end_ns=time.thread_time_ns(),scope='actual serialized frame bytes; not proof all bytes were sent')
        return packet
    module._frame=frame
    module.json=JsonCodec(module.json)
    if not child:return
    read=module._read_frame
    @functools.wraps(read)
    async def read_frame(sock,*args,**kwargs):
        value,size=await read(sock,*args,**kwargs)
        if isinstance(sock,ObservedSocket):sock.frame_decoded(value,size)
        else:emit('frame_decoded',actual_socket_bytes=None,unknown='owned socket instrumentation unavailable')
        return value,size
    module._read_frame=read_frame
    serve=module.serve
    @functools.wraps(serve)
    async def observed_serve(data_fd,control_fd,result_fd,*args,**kwargs):
        module.socket=SocketFactory(_REAL_SOCKET,{data_fd:'data',control_fd:'control',result_fd:'results'})
        try:return await serve(data_fd,control_fd,result_fd,*args,**kwargs)
        finally:module.socket=_REAL_SOCKET
    module.serve=observed_serve


def observe_feedback(snapshot, **context):
    snapshot=snapshot if type(snapshot)is dict else {}
    context.setdefault('source_clock_domain','native-engine NativeStats.clock seconds')
    context.setdefault('received_clock_domain','observer-process.time.monotonic seconds')
    emit_feedback('native_feedback_frame',engine_uid=snapshot.get('engine_uid'),
        source_sequence=snapshot.get('sequence'),source_observed_at_s=snapshot.get('observed_at_s'),
        **{name:snapshot.get(name)for name in ('running','waiting','kv_usage','preemptions_total',
                                              'finished_total','generation_tokens_total')},**context)


def _prepare_sink_closed(sink):
    buffer=sink.get('feedback_buffer')if type(sink)is dict else None
    return (type(buffer)is dict and sink.get('closed')is True and sink.get('flush_confirmed')is True
        and all(type(sink.get(k))is int and sink[k]==0 for k in ('errors','dropped'))
        and type(sink.get('events_written'))is int and sink['events_written']>=0
        and sink['events_written']==sink.get('events_attempted')
        and buffer.get('closed')is True and buffer.get('drain_complete')is True
        and all(type(buffer.get(k))is int and buffer[k]==0 for k in ('remaining','errors','dropped')))


def install_prepare_close(owner):
    """One bounded observation RPC before each existing prepare-actor kill."""
    if not ENABLED or not callable(getattr(owner,'_kill_actor',None)):return
    original_kill,original_close=owner._kill_actor,owner.close
    @functools.wraps(original_kill)
    def kill(self,actor):
        if ENABLED and any(actor is p for p in self.prepare_actors):
            with self.__dict__.setdefault('_record_prepare_close_lock',threading.Lock()):
                states=self.__dict__.setdefault('_record_prepare_closes',{})
                state=states.setdefault(id(actor),dict(ref=None,receipt=dict(
                    actor_index=next(i for i,p in enumerate(self.prepare_actors)if p is actor),
                    status='unknown',rpc_terminal_status='unsubmitted',dispatch_attempted=False,error_type=None)))
                receipt=state['receipt']
                try:
                    deadline=self._record_prepare_close_deadline
                    if not receipt['dispatch_attempted'] and time.monotonic()<deadline:
                        receipt.update(dispatch_attempted=True,rpc_terminal_status='unknown')
                        state['ref']=actor.close_observation.remote()
                    if state['ref'] is not None:
                        value=self.wait_ready(state['ref'],deadline)
                        state['ref']=None;receipt['rpc_terminal_status']='returned'
                        if (type(value)is not dict or value.get('role')!='prepare'
                                or value.get('status')not in ('closed','unknown')
                                or value['status']=='closed'and not _prepare_sink_closed(value.get('sink'))):
                            raise ValueError('invalid prepare observation receipt')
                        receipt.update({k:value[k]for k in ('status','pid','sink')if k in value})
                        receipt['error_type']=value.get('error_type')
                except Exception as error:
                    receipt.update(status='unknown',error_type=type(error).__name__)
        # Observation failure, timeout or an unknown RPC must never skip cleanup.
        return original_kill(self,actor)
    @functools.wraps(original_close)
    def close(self,*args,**kwargs):
        if not ENABLED or not self.prepare_actors:return original_close(self,*args,**kwargs)
        if not hasattr(self,'_record_prepare_close_deadline'):
            self._record_prepare_close_deadline=(self.close_deadline if self.close_deadline is not None
                else time.monotonic()+self._model_close_timeout_s())
        report=original_close(self,*args,**kwargs)
        report['prepare_observation']=[dict(s['receipt'],ref_retained=s['ref']is not None)
            for s in getattr(self,'_record_prepare_closes',{}).values()]
        return report
    owner._kill_actor,owner.close=kill,close


class ObservedAsyncEngineProcess(AsyncEngineProcess):
    @staticmethod
    def _command(spec,descriptors):
        from pathlib import Path
        return [spec.model_python,'-u','-B',str(Path(__file__).parent/'record_model_bootstrap.py'),*map(str,descriptors)]

    def __init__(self,*args,**kwargs):
        install_transport(child=False)
        emit('IPC_engine_start')
        try:super().__init__(*args,**kwargs)
        finally:
            ready=getattr(self,'ready_report',None) or {}
            emit('IPC_engine_start_return',engine_uid=ready.get('engine_uid'))
            emit_buffered('native_startup_capacity',engine_uid=ready.get('engine_uid'),
                native_capacity=ready.get('native_capacity'),reason='effective_config_receipt'
                if ready.get('native_capacity')is not None else 'runtime_has_no_effective_capacity_receipt')

    def _io(self):
        if handoff_profile():return super()._io()
        replacements={}
        for name,channel in [('_data','data'),('_control','control'),('_results','results')]:
            original=getattr(self,name)
            value=ObservedSocket(original,channel);replacements[original]=value;setattr(self,name,value)
        self._sockets=[replacements.get(s,s) for s in self._sockets]
        return super()._io()

    def _receive(self,value,*,results,encoded_bytes=0):
        if not handoff_profile():
            sock=self._results if results else self._control
            if isinstance(sock,ObservedSocket):sock.frame_decoded(value,encoded_bytes)
            else:emit('frame_decoded',actual_socket_bytes=None,unknown='owned receive instrumentation unavailable')
        if isinstance(value,dict) and value.get('op')=='feedback':
            try:received=time.monotonic()
            except Exception:received=None
            snapshot=value.get('snapshot')
            try:result=super()._receive(value,results=results,encoded_bytes=encoded_bytes)
            except BaseException:
                try:observe_feedback(snapshot,stage='IPC_cache',accepted=False,
                    reason='native_receive_raised',received_at_s=received)
                except Exception:pass
                raise
            try:
                ready=getattr(self,'ready_report',None)
                accepted=isinstance(snapshot,dict) and getattr(self,'_native_feedback',None)is snapshot
                reason=('cached_for_consumer'if accepted else 'malformed_snapshot'if type(snapshot)is not dict
                        else 'engine_not_ready'if ready is None else 'engine_uid_mismatch')
                observe_feedback(snapshot,stage='IPC_cache',accepted=accepted,reason=reason,
                    received_at_s=received,expected_engine_uid=(ready or {}).get('engine_uid'),
                    owner_uid=getattr(self,'_owner_uid',None),session_id=None)
            except Exception:
                emit_buffered('native_feedback_audit_unavailable',reason='IPC_metadata_unavailable')
            return result
        return super()._receive(value,results=results,encoded_bytes=encoded_bytes)

    def submit(self,row,*args,**kwargs):
        emit('IPC_submit_start',identities=ids([row]),logical_messages_bytes=len(row['messages_json'].encode()))
        result=super().submit(row,*args,**kwargs)
        emit('IPC_submit_return',identities=ids([row]),status=result.status,request_id=result.request_id,usage=self.snapshot())
        return result

    def poll(self,*args,**kwargs):
        events=super().poll(*args,**kwargs)
        for event in events:
            emit('IPC_terminal_consumed',identities=ids([event.result or event.identity]),request_id=event.request_id,usage=self.snapshot())
        return events

    def close(self,*args,**kwargs):
        emit('IPC_close_start')
        result=super().close(*args,**kwargs)
        emit('IPC_close_end',status=result.get('status'),scope='original API receipt; process-tree/GPU exit checked by existing supervisor')
        return result


def __getattr__(name):
    # The model interpreter uses install_transport only. Keep driver/Ray/LOTUS
    # actor imports out of that environment, matching the public separation.
    if name not in ('ObservedAsyncCoreModelActor','ObservedPrepareActor'):raise AttributeError(name)
    from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
    from src.execution_provider.adapters.lotus_records_actor_owner import _PrepareActor
    class ObservedAsyncCoreModelActor(AsyncCoreModelActor):
        def __init__(self,*args,**kwargs):
            from record_observer import install_actor
            install_actor();super().__init__(*args,**kwargs)
        def _process_type(self):return ObservedAsyncEngineProcess
        def snapshot(self):
            if not ENABLED:return super().snapshot()
            # The native condition is reentrant. Include this observer producer
            # in the same close gate; zero data readers alone does not cover it.
            with self._condition:
                value=super().snapshot()
                if not getattr(self,'_record_sink_finalized',False):
                    try:
                        ipc=value.get('ipc') or {}
                        observe_feedback(ipc.get('native_feedback'),stage='actor_snapshot',accepted=None,
                            reason='returned_before_Ray_delivery',received_at_s=time.monotonic(),
                            owner_uid=ipc.get('owner_uid'),session_id=value.get('session_id'))
                    except Exception:
                        emit_buffered('native_feedback_audit_unavailable',reason='actor_snapshot_metadata_unavailable')
                return value
        def _run_core(self,rows,deadline,maximum,request_key,generation=None):
            if not handoff_profile():return super()._run_core(rows,deadline,maximum,request_key,generation)
            key=core_key(request_key)
            try:result=super()._run_core(rows,deadline,maximum,request_key,generation)
            except BaseException as error:
                emit('actor_result_raise',key=key,identities=ids(rows),error_type=type(error).__name__)
                raise
            # Native _serve_core still owns its reader guard at this point.
            emit('actor_result_return',key=key,identities=ids(rows),
                 scope='before native reader guard releases; Ray serialization/return still follow')
            return result
        def poll_prepared_batch(self,batch_key):
            if not handoff_profile():return super().poll_prepared_batch(batch_key)
            # Reuse the native first-exposure/ACK frontier, not an observer ledger.
            # Its condition is reentrant; no native ownership fields are changed.
            with self._condition:
                if getattr(self,'_record_sink_finalized',False):return super().poll_prepared_batch(batch_key)
                pending=None
                try:
                    group=tuple(batch_key)if core_key(batch_key)is not None else None
                    pending={key:call.identity for key,call in self._held.items()
                             if call.batch_key==group and not call.poll_exposed}
                    for receipt in(self.last_close_report or {}).get('unconsumed_core_results',()):
                        key=tuple(receipt['request_key'])
                        if (tuple(receipt.get('batch_key',()))==group
                                and self._closed_pending.get(key)is False):
                            pending[key]=receipt['result']
                except Exception as error:emit('handoff_probe_unknown',error_type=type(error).__name__)
                try:result=super().poll_prepared_batch(batch_key)
                except BaseException as error:
                    emit('actor_batch_poll_raise',batch_key=core_key(batch_key),error_type=type(error).__name__)
                    raise
                try:
                    exposed=[]
                    for key,identity in(pending or {}).items():
                        held=self._held.get(key)
                        if ((held is not None and held.poll_exposed)
                                or (self._closed_pending or {}).get(key)is True):
                            members=ids([identity]);exposed.append(dict(key=core_key(key),identities=members))
                            emit('actor_result_return',key=core_key(key),batch_key=core_key(batch_key),
                                 identities=members,scope='first native batch-poll exposure; before Ray return; not ACK or delivery proof')
                    emit('actor_batch_poll_return',batch_key=core_key(batch_key),members=exposed,
                         members_complete=False,scope='newly exposed members only; replay/empty pages are not new completions')
                except Exception as error:emit('handoff_probe_unknown',error_type=type(error).__name__)
                return result
        def _finalize_record_sink(self,report):
            if not ENABLED:return
            with self._condition:
                if getattr(self,'_record_sink_finalized',False):return
                proof=report.get('producer_quiescence')or {}
                if (report.get('status')=='closed'and proof.get('schema')=='semloom.async_actor_quiescence.v1'
                        and proof.get('confirmed')is True and proof.get('input_gate_closed')is True
                        and proof.get('active_readers')==0 and self._readers==0
                        and not getattr(self,'_closed_pending',None)):
                    emit('actor_sink_finalizing',status='closed',metrics=dict(snapshot_gate_closed=True),
                        scope='native producer quiescence; ACKs consumed; snapshot observer gate closed before owner kill; later RPCs outside this sink')
                    close_events();self._record_sink_finalized=True
                else:emit('actor_sink_not_final',status='unknown',scope='native readers, pending batch ACK or producer close not proved')
        def acknowledge_core(self,request_key):
            result=super().acknowledge_core(request_key)
            if ENABLED and result and self.last_close_report is not None:
                self._finalize_record_sink(self.last_close_report)
            return result
        def close(self,*args,**kwargs):
            report=super().close(*args,**kwargs)
            self._finalize_record_sink(report)
            return report
    class ObservedPrepareActor(_PrepareActor):
        def __init__(self,*args,**kwargs):
            self._record_prepare_lock=threading.RLock()
            self._record_prepare_close_report=None
            install_cpu_hooks();super().__init__(*args,**kwargs)
        def prepare(self,*args,**kwargs):
            with self._record_prepare_lock:return super().prepare(*args,**kwargs)
        def close_observation(self):
            with self._record_prepare_lock:
                if self._record_prepare_close_report is not None:return self._record_prepare_close_report
                report=dict(role='prepare',pid=os.getpid(),status='unknown',sink=None,error_type=None)
                try:
                    emit('prepare_sink_finalizing',status='closed',scope='serialized preparation finished; observer only, before owner kill')
                    close_events()
                    sink=json.loads((ROOT/'events'/('events-'+str(os.getpid())+'.status.json')).read_text())
                    report['sink']=sink
                    if _prepare_sink_closed(sink):report['status']='closed'
                except Exception as error:report['error_type']=type(error).__name__
                self._record_prepare_close_report=report
                return report
    globals().update(ObservedAsyncCoreModelActor=ObservedAsyncCoreModelActor,ObservedPrepareActor=ObservedPrepareActor)
    return globals()[name]
