"""Optional native query observation adapters; no scheduler, transport, model, or file I/O.

Install only around the original frozen worker in a future packet. HTTP clocks
come from the existing authoritative observer, never a new HTTP client.
"""
from contextlib import contextmanager
from collections import defaultdict
import time


class ObservationStore:
    def __init__(self, clock=time.monotonic_ns):
        self.clock=clock
        self.batches=defaultdict(list)
        self.queries={}
        self.warnings=[]

    def now(self):
        try:
            value=self.clock()
            if type(value) is not int or value<0:
                raise ValueError('invalid monotonic timestamp')
            return value
        except Exception as error:
            self.warnings.append(type(error).__name__)
            return None


def timed_driver_type(base,store):
    """Wrap the existing outer public-policy driver, not its model callback.

    Session query_start remains the authoritative outer submission, covering
    isolated native owner construction before this driver method is entered.
    """
    class TimedDriver(base):
        def query(self,spec,emit):
            entered=store.now()
            report=None
            failure=None
            try:
                report=super().query(spec,emit)
                return report
            except BaseException as error:
                failure=error
                report=getattr(error,'query_report',None)
                raise
            finally:
                try:
                    batches=store.batches.pop(spec['query_id'],[])
                    ended=store.now()
                    receipt=dict(schema='semloom.external_query_clock.v1',
                        query_id=spec['query_id'],driver_entered_ns=entered,
                        API_returned_ns=ended,status='raised' if failure is not None else 'returned',
                        clock='time.monotonic_ns in original worker',
                        complete=entered is not None and ended is not None and ended>=entered,
                        includes_original_policy_scan_cleanup=True,
                        outer_submission='original session query_start_ns; do not substitute driver_entered_ns',
                        owner_shutdown='original separate lifecycle receipt; never subtracted from total cost',
                        native_batch_API=batches)
                    store.queries[spec['query_id']]=receipt
                    if isinstance(report,dict):
                        report['comparison_timing']=receipt
                except Exception as error:
                    store.warnings.append(type(error).__name__)
    return TimedDriver


@contextmanager
def native_observation(native_module,lm_module,store):
    """Retain existing POST clocks and time the unchanged native LM batch call.

    Pass the already-loaded native adapter and lotus.models.lm modules. Nothing
    here reads payloads, makes a request, reserves a call, or changes HTTP options.
    """
    cls=native_module._Calls
    old_receipts,old_batch=cls.receipts,cls.observe_batch

    def receipts(self):
        original=old_receipts(self)
        try:
            for row in original:
                events=self.events.get(row['attempt'],{})
                started=events.get('http_started',{}).get('monotonic_ns')
                response=events.get('http_response',{}).get('monotonic_ns')
                finished=events.get('http_finished',{}).get('monotonic_ns')
                complete=(type(started) is int and type(finished) is int and finished>=started)
                row['external_HTTP_timing']=dict(started_ns=started,response_observed_ns=response,
                    finished_ns=finished,complete=complete,
                    elapsed_s=(finished-started)/1e9 if complete else None,
                    scope='existing observer http_started through http_finished; response read/receipt JSON included',
                    records_per_POST=1)
        except Exception as error:
            store.warnings.append(type(error).__name__)
        return original

    @contextmanager
    def batch(self):
        with old_batch(self):
            original=lm_module.batch_completion

            def observed(*args,**kwargs):
                started=store.now()
                failure=None
                try:
                    return original(*args,**kwargs)
                except BaseException as error:
                    failure=error
                    raise
                finally:
                    ended=store.now()
                    try:
                        qid=self.expected[0]['query_id'] if self.expected else None
                        store.batches[qid].append(dict(kind='LOTUS_native_batch_completion',
                            query_id=qid,started_ns=started,finished_ns=ended,
                            input_records=len(self.expected),status='failed' if failure else 'succeeded',
                            error_type=type(failure).__name__ if failure else None,
                            elapsed_s=(ended-started)/1e9
                                if started is not None and ended is not None and ended>=started else None,
                            scope='one original LM batch_completion invocation, not per-row or per-POST latency'))
                    except Exception as error:
                        store.warnings.append(type(error).__name__)
            lm_module.batch_completion=observed
            try:
                yield
            finally:
                lm_module.batch_completion=original

    cls.receipts,cls.observe_batch=receipts,batch
    try:
        yield
    finally:
        cls.receipts,cls.observe_batch=old_receipts,old_batch


def query_timing(query):
    """Join the exact new return clock to the existing outer submission clock.

    Missing clocks are unavailable, never reconstructed from query_terminal.
    """
    timing=query.get('backend_report',{}).get('comparison_timing')
    start=query.get('query_start_ns')
    ready=query.get('query_result_ready_ns')
    result=dict(submitted_ns=start,result_ready_ns=ready,API_returned_ns=None,
                answer_ready_s=None,API_return_s=None,driver_only_s=None,
                session_terminal_s=None,API_timing_complete=False,reason=None)
    if type(start) is int and type(ready) is int and ready>=start:
        result['answer_ready_s']=(ready-start)/1e9
    terminal=query.get('query_terminal_ns')
    if type(start) is int and type(terminal) is int and terminal>=start:
        result['session_terminal_s']=(terminal-start)/1e9
    if not timing or not timing.get('complete'):
        result['reason']='exact API-return receipt unavailable; session terminal is not substituted'
        return result
    entered,returned=timing.get('driver_entered_ns'),timing.get('API_returned_ns')
    if not all(type(x) is int for x in (start,entered,returned)) or not start<=entered<=returned:
        result['reason']='invalid submission/driver/return clock ordering'
        return result
    if type(ready) is int and not start<=ready<=returned:
        result['reason']='complete result lies outside query API interval'
        return result
    if type(terminal) is int and returned>terminal:
        result['reason']='API return later than original session terminal'
        return result
    result.update(API_returned_ns=returned,API_return_s=(returned-start)/1e9,
                  driver_only_s=(returned-entered)/1e9,API_timing_complete=True)
    return result


def model_API_samples(backend,report):
    """Return actual API units, including failed/unknown timing records.

    Core rows are independent calls. Native POST attempts are independent calls.
    Only original chat(list) rows are grouped by shared engine+interval receipt.
    """
    rows=report.get('calls',{}).get('records',[])
    samples=[]
    if backend=='lotus-native':
        for receipt in report.get('http_receipts',[]):
            timing=receipt.get('external_HTTP_timing',{})
            sent=bool(receipt.get('http_started'))
            status=('succeeded' if receipt.get('http_status')==200 and not receipt.get('error_type') else
                    'failed' if receipt.get('error_type') or receipt.get('http_status') is not None else
                    'unknown' if sent else 'not_sent')
            samples.append(dict(api_id='POST:'+str(receipt['attempt']),unit='HTTP_POST',records=1,
                countable_API_unit=sent,
                status=status,start_ns=timing.get('started_ns'),end_ns=timing.get('finished_ns'),
                elapsed_s=timing.get('elapsed_s') if timing.get('complete') else None))
        return samples
    grouped=defaultdict(list)
    for index,row in enumerate(rows):
        start,end=row.get('model_start_monotonic'),row.get('model_end_monotonic')
        if backend=='semloom-core':
            key=('independent',row.get('call_id',index))
        elif start is None or end is None:
            key=('unresolved_batch_identity',row.get('engine_uid'))
        else:
            key=(row.get('engine_uid'),start,end)
        grouped[key].append(row)
    for key,batch in grouped.items():
        first=batch[0];start=first.get('model_start_monotonic');end=first.get('model_end_monotonic')
        complete=isinstance(start,(int,float)) and isinstance(end,(int,float)) and end>=start
        statuses={x.get('status','unknown') for x in batch}
        status='succeeded' if statuses=={'succeeded'} else 'failed' if 'failed' in statuses else 'unknown'
        samples.append(dict(api_id=repr(key),unit='single_generate' if backend=='semloom-core' else 'native_chat_batch',
            countable_API_unit=key[0]!='unresolved_batch_identity',
            records=len(batch),status=status,start_ns=int(start*1e9) if complete else None,
            end_ns=int(end*1e9) if complete else None,elapsed_s=end-start if complete else None,
            row_ids=[x.get('row_id') for x in batch]))
    return samples


def offline_scan_total(events):
    unique={}
    for event in events:
        key=event['event_id']
        if key in unique and unique[key]!=event:
            raise ValueError('conflicting offline event identity')
        unique[key]=event
    return dict(events=len(unique),calls=sum(x['actual_scan_calls'] for x in unique.values()),
                wall_s=sum(x['actual_scan_wall_s'] for x in unique.values()))
