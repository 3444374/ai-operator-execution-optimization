"""Query clocks and optional native model-call observations.

The caller owns execution and persistence. This module records existing driver
and model receipts without creating requests or an execution loop.
"""
from collections import defaultdict
import time
from src.infrastructure.redact import redact_text



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

    Core rows are independent calls.
    Only original chat(list) rows are grouped by shared engine+interval receipt.
    """
    native_local = backend == 'lotus-native' and report.get('protocol') == 'local_async_model_IPC'
    rows = (report.get('native_transport_receipts', []) if native_local else
            report.get('calls',{}).get('records',[]))
    samples=[]
    if backend=='lotus-native' and not native_local:
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
        if backend=='semloom-core' or native_local:
            key=('independent',row.get('call_id',index))
        elif start is None or end is None:
            key=('unresolved_batch_identity',row.get('engine_uid'))
        else:
            key=(row.get('engine_uid'),start,end)
        grouped[key].append(row)
    for key,batch in grouped.items():
        first=batch[0];start=first.get('model_start_monotonic');end=first.get('model_end_monotonic')
        complete=isinstance(start,(int,float)) and isinstance(end,(int,float)) and end>=start
        countable = key[0]!='unresolved_batch_identity'
        statuses={x.get('status','unknown') for x in batch}
        status='succeeded' if statuses=={'succeeded'} else 'failed' if 'failed' in statuses else 'unknown'
        if native_local:
            submitted = first.get('submission_confirmed')
            countable = submitted is True
            if not countable:
                status = 'not_sent' if submitted is False and status != 'unknown' else 'unknown'
                complete = False  # Unconfirmed submission has no measured model API interval.
        samples.append(dict(api_id=repr(key),unit='single_generate' if backend=='semloom-core' or native_local else 'native_chat_batch',
            countable_API_unit=countable,
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


class QueryClock:
    def __init__(self, *, event_sink=None):
        self.events = []
        self.event_sink, self.event_sink_errors = event_sink, []

    def __call__(self, event):
        self.record(dict(event=event, monotonic_ns=time.monotonic_ns()))

    def record(self, entry):
        """Observe an existing owner timestamp without retiming session/query events."""
        if (type(entry) is not dict or type(entry.get("event")) is not str
                or type(entry.get("monotonic_ns")) is not int or entry["monotonic_ns"] < 0):
            raise ValueError("owner event requires an event name and monotonic timestamp")
        entry = dict(entry)
        self.events.append(entry)
        if self.event_sink is not None:
            try:
                self.event_sink(dict(entry, epoch_ns=entry.get("epoch_ns", time.time_ns())))
            except Exception as error:
                self.event_sink_errors.append(redact_text(type(error).__name__ + ": " + str(error)))

    def result(self, invoked_ns, finished_ns):
        found = [e["monotonic_ns"] for e in self.events if e["event"] == "result_ready"]
        if len(found) != 1 or not invoked_ns <= found[0] <= finished_ns:
            raise ValueError("one complete pre-evaluation result_ready timestamp is required")
        return dict(query_result_seconds=(found[0] - invoked_ns) / 1e9,
            application_e2e_seconds=(finished_ns - invoked_ns) / 1e9, events=self.events,
            scope="worker invocation through complete task result; application includes worker-owned evaluation/close",
            resident_query_seconds=None)
