"""Opt-in Daft single-native-thread session; original process-pool stays separate."""
from dataclasses import asdict
import json
import os
from pathlib import Path
import time

from .daft_lotus_records import _DaftRows, _graph, _check, DEFAULT_PREPARED_BATCH_BYTES
from src.baselines.common.private_artifacts import new_private_directory
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


class DaftNativeSession:
    def __init__(self,engine,generation,resources,*,control_dir,deadline_monotonic,
                 engine_type=None,prepare_type=None,model_gpus=1):
        self.engine=EngineSpec.from_dict(asdict(engine))
        self.generation=GenerationSpec.from_dict(asdict(generation))
        self.resources=RecordsResources(**asdict(resources))
        self.control=Path(control_dir);self.deadline=deadline_monotonic
        self.engine_type=engine_type;self.prepare_type=prepare_type;self.model_gpus=model_gpus
        self.infer=None;self.active=None;self.expected=[];self.query_ids=set();self.received=set()
        self.started=False;self.closing=False;self.broken=False
        self.ready_report=None;self.last_close_report=None;self.cleanup=None
        self.last_query_report=None
        self.finish_errors=[]
        if generation.max_tokens>engine.max_model_len:raise ValueError('generation exceeds model context limit')

    def start(self):
        if self.closing:raise RuntimeError('closed Daft session cannot restart')
        if self.ready_report is not None:return self.ready_report
        import daft
        from daft.runners import _get_runner
        from src.infrastructure import daft_runtime
        if daft.__version__!='0.7.21':raise RuntimeError('requires Daft 0.7.21')
        if _get_runner() is not None and daft_runtime._configured!=('native',1):
            raise RuntimeError('single-thread session needs its own compatible Daft worker process')
        daft_runtime.configure_daft_runner('native',num_threads=1)
        new_private_directory(self.control)
        self.started=True
        engine,generation=self.engine,self.generation
        resources,control,deadline=self.resources,self.control,self.deadline
        engine_type=self.engine_type
        dtype=daft.DataType.struct({'engine_result_json':daft.DataType.string()})

        @daft.cls(cpus=1,gpus=self.model_gpus,use_process=False,max_concurrency=None,max_retries=0,on_error='raise')
        class Infer:
            def __init__(self):self.kernel=None

            @daft.method.batch(return_dtype=dtype,batch_size=resources.model_batch_rows)
            def run(self,prepared,query_ids,query_deadlines):
                rows=prepared.to_pylist();ids=query_ids.to_pylist();deadlines=query_deadlines.to_pylist()
                if not rows:return daft.Series.from_pylist([]).cast(dtype)
                query_deadline=min(deadline,*deadlines)
                _check(query_deadline,control)
                if any(row['query_id']!=qid for row,qid in zip(rows,ids,strict=True)):
                    raise ValueError('prepared rows belong to another Daft query')
                size=sum(len(row['messages_json'].encode('utf-8')) for row in rows)
                if len(rows)>resources.model_batch_rows or size>DEFAULT_PREPARED_BATCH_BYTES:
                    raise BufferError('actual Daft inference batch exceeds its logical input allowance')
                if self.kernel is None:
                    # Single instance is required; never load a second model if a
                    # different wrapper instance unexpectedly executes this session.
                    (control/'model-started').touch(mode=0o600,exist_ok=False)
                    from .daft_engine_process import EngineProcess
                    self.kernel=(engine_type or EngineProcess)(engine,generation,control,deadline,
                                                              resources.startup_timeout_s)
                results=self.kernel.run_prepared(rows)
                for result,prepared_row in zip(results,rows,strict=True):
                    for name in ('prepare_worker_pid','prepare_started_ns','prepare_finished_ns'):
                        result[name]=prepared_row[name]
                    result.update(infer_worker_pid=os.getpid(),native_infer_batch_rows=len(rows),
                                  native_infer_message_bytes=size)
                return daft.Series.from_pylist([dict(engine_result_json=json.dumps(row,ensure_ascii=False,allow_nan=False))
                                               for row in results])

            @daft.method.batch(return_dtype=daft.DataType.string(),batch_size=1)
            def finish(self,control_row):
                if not len(control_row):return daft.Series.from_pylist([],dtype=daft.DataType.string())
                report=(dict(status='closed',no_model_started=True) if self.kernel is None else self.kernel.close())
                return daft.Series.from_pylist([json.dumps(report,ensure_ascii=False,allow_nan=False)])

        self.infer=Infer()
        self.ready_report=dict(no_session_model=True,model_startup_scope='lazy first nonempty native query',
            native_threads=1,use_process=False,max_concurrency=None,model_gpus=self.model_gpus,
            model_reuse='same Daft wrapper; original isolated configuration unchanged')
        return self.ready_report

    def evaluate(self,source,method,*,query_id,cancelled=None):
        if not self.started or self.closing or self.broken:raise RuntimeError('Daft session is not reusable')
        if self.active is not None:raise RuntimeError('previous Daft query is still active')
        if query_id in self.query_ids:raise ValueError('Daft session query IDs must be unique')
        from src.semantic_methods.lotus.engine import bind_filter_method,make_lm,validate_generation,validate_lm
        method=FilterMethod.from_dict(asdict(method))
        source.preflight(method)
        validate_generation(method,self.generation);method=bind_filter_method(method)
        validate_lm(make_lm(method,self.generation),method,self.generation)
        self.engine.validate_driver_environment()
        deadline=min(self.deadline,time.monotonic()+self.resources.query_timeout_s)
        _check(deadline,self.control)
        if cancelled is not None and cancelled():raise InterruptedError('Daft query cancelled before graph allocation')
        expected=source.expected(query_id)
        query_control=self.control/('query-'+str(len(self.query_ids)))
        new_private_directory(query_control)
        self.query_ids.add(query_id);self.expected.extend(expected)
        try:
            frame=None if not expected else _graph(source,method,self.engine,self.generation,self.resources,
                query_id,expected,deadline,query_control,runner_threads=1,prepare_type=self.prepare_type,
                session_infer=self.infer)
            rows=_SessionRows(frame,expected,deadline,cancelled,query_control,session=self)
            self.active=rows
            return rows
        except BaseException:
            self.broken=True;self._close_model()
            raise

    def _close_model(self):
        if self.last_close_report is not None and self.last_close_report['status']=='closed':
            return self.last_close_report
        if not self.started:
            return dict(status='closed',cleanup_errors=[],model_started=False)
        if self.cleanup is None:
            if not self.broken:
                try:
                    import daft
                    # Public native UDF call on the retained wrapper, no model
                    # request and no private wrapper-instance access.
                    daft.from_pydict({'finish':[True]}).select(
                        self.infer.finish(daft.col('finish')).alias('report')).to_pydict()
                except BaseException as error:
                    self.broken=True
                    self.finish_errors.append('native_finish: '+type(error).__name__)
            self.cleanup=_DaftRows(None,self.expected,self.deadline,None,self.control)
            self.cleanup._started=(self.control/'model-started').exists()
            self.cleanup.received=set(self.received)
        self.last_close_report=dict(self.cleanup.close())
        if self.finish_errors:
            self.last_close_report.update(status='unknown',cleanup_errors=[
                *self.last_close_report.get('cleanup_errors',[]),*self.finish_errors])
        self.last_close_report.update(scheduler_owner='Daft Native',use_process=False,
            native_threads=1,max_concurrency=None,configuration='native_single_thread_session')
        self.last_query_report=self.last_close_report
        return self.last_close_report

    def close(self):
        self.closing=True
        if self.active is not None:self.active.close()
        return self._close_model()


class _SessionRows(_DaftRows):
    def __init__(self,*args,session,**kwargs):
        super().__init__(*args,**kwargs);self.session=session

    def _watch(self):
        super()._watch()
        if self._failure is not None:
            self.session.broken=True
            (self.session.control/'stop').touch(mode=0o600)

    def __next__(self):
        row=super().__next__()
        self.session.received.add(row['call_id'])
        return row

    def _finish_error(self,error):
        self._failure=error
        super()._finish_error(error)

    def close(self):
        if self.last_close_report is not None:return self.last_close_report
        self._done=True;self._stop.set();errors=[]
        if self._watchdog is not None:
            self._watchdog.join(timeout=0.2)
            if self._watchdog.is_alive():errors.append('stop_watchdog_still_running')
        try:
            close=getattr(self.iterator,'close',None)
            if close is not None:close()
        except BaseException as error:errors.append('iterator_close: '+type(error).__name__)
        complete=(self.received==set(self.expected) and self._failure is None
                  and time.monotonic()<self.deadline and not self.session.closing
                  and not self.session.broken and not errors
                  and not (self.cancelled is not None and self.cancelled()))
        self.session.active=None
        reports=[]
        if not complete:
            self.session.broken=True
            model=self.session._close_model()
            reports=model.get('engine_reports',[])
            errors.extend(model.get('cleanup_errors',[]))
            if model['status']!='closed':errors.append('session_engine_cleanup_unconfirmed')
            for report in reports:errors.extend(self._recover_result_receipts(report))
        self.last_close_report=dict(status='closed' if not errors else 'unknown',cleanup_errors=errors,
            cleanup_scope='native query iterator; model retained until session end' if complete else 'query and session model',
            query_exhausted=self.received==set(self.expected),
            model_retained=complete and (self.session.control/'model-started').exists(),
            engine_reports=reports,gpu_exit_confirmed=None,
            received_occurrences=len(self.received),expected_occurrences=len(self.expected),
            unconsumed_call_results=list(self._result_receipts.values()))
        return self.last_close_report
