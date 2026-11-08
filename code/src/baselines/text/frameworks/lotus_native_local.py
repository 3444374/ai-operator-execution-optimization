"""Original LOTUS operators over the existing local asynchronous model IPC.

LOTUS/LiteLLM retain formatting, caching, batch_completion and parsing. Only
the provider transport is local. No Core, policy, HTTP service or retry pool is
constructed here. Native provider registration is scoped to an isolated query.
"""

from collections import deque
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict
import json
import threading
import time
from types import SimpleNamespace
import uuid

from litellm import CustomLLM

from src.infrastructure.deadlines import deadline_expired, remaining_time
from src.semantic_methods.lotus.engine import engine_response, validate_engine_protocol
from src.semantic_methods.lotus.records import CallLedger, identity
from src.semantic_methods.lotus.filter_answers import LOTUS_NATIVE
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_protocol import AsyncEngineLimits, GenerationSpec, result_record, _prepared


_SDK_SCOPE = threading.RLock()


class LocalModelFailure(RuntimeError):
    """The model receipt remains available even when the native SDK raises."""

    def __init__(self, receipt):
        super().__init__("local model request: " + str(receipt.get('error_type') or receipt['status']))
        self.receipt = receipt


class LocalNativeModel:
    """One serving IPC owner. The original LM owns its client concurrency."""

    def __init__(self, engine, generation, resources, concurrency, *, limits=None):
        if type(concurrency) is not int or concurrency < 1:
            raise ValueError('native LM concurrency must be a positive integer')
        self.engine, self.generation, self.resources = engine, generation, resources
        self.concurrency = concurrency
        if limits is None:
            limits = AsyncEngineLimits(max_inflight=concurrency, control_slots=2 * concurrency)
        if type(limits) is not AsyncEngineLimits:
            raise TypeError('native model limits must be AsyncEngineLimits')
        self.limits, self.transport = limits, None
        self.ready_report = self.last_close_report = None
        self.provider_name = 'lotus_local_' + uuid.uuid4().hex
        self._state_lock = threading.RLock()
        self._active_query = None
        self._last_native_binding = None
        self._tainted = self._closing = False

    def start(self):
        with self._state_lock:
            return self._start()

    def _start(self):
        if self._closing or self._tainted:
            raise RuntimeError('native model owner requires cleanup before reuse')
        if self.transport is None:
            started = time.monotonic()
            try:
                self.transport = AsyncEngineProcess(self.engine, self.generation, limits=self.limits,
                    startup_timeout_s=self.resources.startup_timeout_s)
            except BaseException as error:
                # Existing serving startup failures may retain their process
                # owner for another close attempt; do not lose that ownership.
                self.transport = getattr(error, 'async_engine_process', None)
                self._tainted = True
                raise
            self.ready_report = dict(self.transport.ready_report, protocol='local_async_model_IPC',
                scheduler_owner='original LOTUS LM / LiteLLM batch_completion',
                provider_concurrency=self.concurrency, transport_limits=asdict(self.limits),
                startup_seconds=time.monotonic()-started)
        return deepcopy(self.ready_report)

    def close(self, timeout=15):
        with self._state_lock:
            self._closing = True
            binding = self._active_query or self._last_native_binding
            if binding is not None:
                binding.stop()
            if self.transport is None:
                receipt = dict(status='closed', process_exit_confirmed=True,
                    gpu_exit_confirmed=None, cleanup_scope='no model process allocated', cleanup_errors=[])
            else:
                receipt = self.transport.close(timeout=timeout)
            # Physical close/poll are the existing authoritative terminal path.
            # Source occurrences unreturned by the SDK remain unconfirmed.
            self.last_close_report = deepcopy(receipt)
            try:
                accounting = None if binding is None else self._native_accounting(binding)
            except BaseException as error:
                self.last_close_report.update(status='unknown', model_close_status=receipt['status'],
                    native_accounting_status='retained',
                    cleanup_errors=[*receipt.get('cleanup_errors', []), 'native accounting: '+type(error).__name__])
                error.native_close_report = deepcopy(self.last_close_report)
                raise
            if binding is not None and receipt['status'] == 'closed' and binding is not self._active_query:
                self._last_native_binding = None
                binding.release()
            self.last_close_report['native_accounting_status'] = ('not_needed' if binding is None else
                'retained' if self._last_native_binding is not None or self._active_query is not None else 'transferred')
            # The caller owns this final snapshot and its persistence. Keep no
            # raw accounting history in the owner after successful transfer.
            return dict(receipt, native_final_accounting=accounting,
                native_accounting_status=self.last_close_report['native_accounting_status'])

    @staticmethod
    def _native_accounting(binding):
        return dict(binding.report(), schema='semloom.lotus_native_final_accounting.v1',
            query_id=binding.query_id)

    @property
    def last_native_query_report(self):
        """Current retained failure audit; final closed snapshots belong to close's caller."""
        with self._state_lock:
            return None if self._last_native_binding is None else self._native_accounting(self._last_native_binding)

    @contextmanager
    def query(self, lm, expected, budget, *, query_id):
        """Restore only this provider's registration and the original batch hook."""
        import litellm
        import lotus.models.lm as lm_module

        with _SDK_SCOPE:
            with self._state_lock:
                if self._active_query is not None or self._closing or self._tainted:
                    raise RuntimeError('the native model owner has an unfinished query')
                binding = _LocalProvider(self, lm, expected, budget, query_id)
                if expected:
                    self.start()
                self._active_query = binding
            old_kwargs = dict(lm.kwargs)
            original_batch = lm_module.batch_completion
            entry = dict(provider=self.provider_name, custom_handler=binding)
            litellm.custom_provider_map.append(entry)
            lm.kwargs.update(custom_llm_provider=self.provider_name, num_retries=0, max_retries=0)

            def observed(*args, **kwargs):
                responses = original_batch(*args, **kwargs)
                if kwargs.get('custom_llm_provider') == self.provider_name:
                    messages = args[1] if len(args) > 1 else kwargs['messages']
                    binding.bind_native_return(messages, responses)
                return responses

            lm_module.batch_completion = observed
            try:
                yield binding
            except BaseException:
                self._tainted = True
                raise
            finally:
                binding.stop()
                lm_module.batch_completion = original_batch
                lm.kwargs.clear()
                lm.kwargs.update(old_kwargs)
                litellm.custom_provider_map[:] = [item for item in litellm.custom_provider_map if item is not entry]
                for name in ('provider_list', '_custom_providers'):
                    values = getattr(litellm, name)
                    values[:] = [value for value in values if value != self.provider_name]
                with self._state_lock:
                    self._active_query = None
                    if self._tainted or binding._pending:
                        if self.last_close_report is None or self.last_close_report['status'] != 'closed':
                            self._last_native_binding = binding


class _LocalProvider(CustomLLM):
    def __init__(self, owner, lm, expected, budget, query_id):
        super().__init__()
        self.owner, self.lm, self.budget, self.query_id = owner, lm, budget, query_id
        self.expected = {row['row_id']: dict(row) for row in expected}
        if len(self.expected) != len(expected):
            raise ValueError('native query occurrences must have unique execution row IDs')
        self._condition = threading.Condition()
        self._pending, self.receipts, self.bound, self.late_receipts = {}, {}, {}, {}
        self._prompts = deque()
        self._sequence = 0
        self._stopped = False
        self._released = False

    def capture_prompts(self, prompts):
        """Receive the original operator's actual messages before LM execution."""
        self._prompts.extend(deepcopy(prompts))

    def bind_native_return(self, messages, responses):
        # The original SDK's ordered return, not worker arrival order, binds
        # duplicate conversations to their source occurrences.
        if len(messages) != len(responses):
            raise ValueError('native batch changed response cardinality')
        for message, response in zip(messages, responses, strict=True):
            if not self._prompts:
                raise ValueError('native result lacks its original operator prompt')
            prompt = self._prompts.popleft()
            if prompt['messages'] != message:
                raise ValueError('native uncached order differs from the captured operator input')
            request_id = getattr(response, 'id', None)
            with self._condition:
                result = self.receipts.get(request_id)
            if result is None or result['status'] != 'succeeded':
                # Failed or unassociated native returns are not source successes.
                continue
            target = self.expected[prompt['row_id']]
            if target['call_id'] in self.bound:
                raise ValueError('native result occurrence was returned twice')
            transport_identity = {name: result[name] for name in target}
            self.bound[target['call_id']] = dict(result, **target,
                native_transport_identity=transport_identity,
                source_binding='original LOTUS batch_completion ordered return')

    def _generation(self, optional):
        values = asdict(self.owner.generation)
        for name in values:
            if name in optional:
                values[name] = optional[name]
        if 'max_completion_tokens' in optional:
            values['max_tokens'] = optional['max_completion_tokens']
        # LOTUS asks the OpenAI-shaped provider for logprobs=False by default;
        # the existing native engine expresses that as None, not integer False.
        if values['logprobs'] is False:
            values['logprobs'] = None
        return GenerationSpec.from_dict(values)

    def completion(self, model, messages, optional_params, **_kwargs):
        if model != self.lm.model:
            raise ValueError('native provider changed the original LM model identity')
        generation = self._generation(optional_params)
        started = time.monotonic()
        deadline = None if self.owner.resources.query_timeout_s is None else started + self.owner.resources.query_timeout_s
        with self._condition:
            if self._stopped:
                raise RuntimeError('native query is no longer accepting model calls')
            if self._sequence >= len(self.expected):
                raise RuntimeError('native operator emitted more calls than its declared occurrences')
            sequence = self._sequence
            self._sequence += 1
            row = dict(identity(self.query_id, 'native-call-' + str(sequence), sequence),
                messages_json=json.dumps(messages, ensure_ascii=False, allow_nan=False))
            request_identity = _prepared([row])[0][0]
            # The claim occurs once before IPC ownership transfer, including a
            # conservatively charged call that is rejected locally.
            import hashlib
            self.budget.reserve(hashlib.sha256(row['messages_json'].encode()).hexdigest())
        request_id = None
        try:
            while request_id is None:
                with self._condition:
                    self._collect()
                    if self._stopped or deadline_expired(deadline):
                        raise TimeoutError('native query ended before model submission')
                    queued = self.owner.transport.submit(row, generation=generation, deadline_monotonic=deadline)
                    if queued.status == 'queued':
                        request_id = queued.request_id
                        self._pending[request_id] = None
                    elif queued.status != 'backpressure':
                        raise RuntimeError('native model submission rejected: ' + str(queued.reason))
                if request_id is None:
                    self.owner.transport.wait(max(.001, remaining_time(deadline, .05)))
            while True:
                with self._condition:
                    self._collect()
                    event = self._pending.get(request_id)
                    if self._stopped:
                        if event is not None and event.result is not None:
                            self.receipts[request_id] = deepcopy(event.result)
                        raise InterruptedError('native query stopped before consuming its model result')
                    if event is not None:
                        result = event.result
                        if result is None:
                            result = result_record(request_identity, engine_uid=self.owner.ready_report['engine_uid'],
                                start=started, end=time.monotonic(),
                                status='unknown', submitted=True, error_type=event.error_type or 'MissingModelReceipt')
                        self.receipts[request_id] = deepcopy(result)
                        del self._pending[request_id]
                        if not event.engine_terminal_confirmed or result['status'] != 'succeeded':
                            self.owner._tainted = True
                            raise LocalModelFailure(result)
                        validate_engine_protocol(result)
                        return engine_response(result, SimpleNamespace(model=model))
                    if self._stopped or deadline_expired(deadline):
                        self.owner.transport.abort(request_id)
                        raise TimeoutError('native model terminal receipt is still unconfirmed')
                self.owner.transport.wait(max(.001, remaining_time(deadline, .05)))
        except BaseException as error:
            self.owner._tainted = True
            if request_id is None:
                result = result_record(request_identity, engine_uid=self.owner.ready_report['engine_uid'],
                    start=started, end=time.monotonic(),
                    status='failed', submitted=False, error_type='RejectedBeforeModelSubmission')
                result.update(prompt_tokens=0, completion_tokens=0, total_tokens=0)
                self.receipts['not-submitted-' + str(sequence)] = result
            elif request_id not in self.receipts:
                self.receipts[request_id] = result_record(request_identity,
                    engine_uid=self.owner.ready_report['engine_uid'],
                    start=started, end=time.monotonic(),
                    status='unknown', submitted=True, error_type=type(error).__name__)
            raise

    def _collect(self):
        for event in self.owner.transport.poll(self.owner.limits.result_slots):
            if event.request_id in self._pending:
                self._pending[event.request_id] = event
            if event.request_id in self.receipts and event.result is not None:
                self.late_receipts[event.request_id] = deepcopy(event.result)
            elif event.request_id not in self._pending and event.result is not None:
                # A late terminal remains audit evidence; it does not make an
                # already failed native query into a semantic success.
                self.receipts[event.request_id] = deepcopy(event.result)

    def stop(self):
        with self._condition:
            self._stopped = True
            for request_id, event in self._pending.items():
                if event is None:
                    self.owner.transport.abort(request_id)

    def report(self):
        if self._released:
            raise RuntimeError('native final accounting was transferred by close')
        with self._condition:
            if self.owner.transport is not None:
                self._collect()
            for request_id, event in self._pending.items():
                if event is not None and event.result is not None:
                    # A native worker may have exited before collecting its
                    # routed event. Preserve physical usage without promoting
                    # that unreturned occurrence to a semantic success.
                    target = self.late_receipts if request_id in self.receipts else self.receipts
                    target[request_id] = deepcopy(event.result)
        ledger = CallLedger(list(self.expected.values()), max_calls=max(1, len(self.expected)))
        for result in self.bound.values():
            ledger.record(result)
        ledger.mark_unconfirmed('NativeQueryDidNotReturnEveryOccurrence')
        return dict(calls=ledger.report(), native_transport_receipts=deepcopy(list(self.receipts.values())),
            late_native_receipts=deepcopy(list(self.late_receipts.values())),
            source_binding='known native ordered returns only; unassociated calls stay unknown')

    def release(self):
        """Drop only an inactive, physically closed query's original data."""
        with self._condition:
            self._released = True
            self._prompts.clear()
            for values in (self._pending, self.receipts, self.bound, self.late_receipts, self.expected):
                values.clear()
            self.lm = self.budget = self.owner = None


def run(source, checkout, method, generation, engine, resources, *, query_id, profile,
        observer=None, context=None, shared_model=None):
    """Run the original Movie query and evaluator with the local native provider.

    The caller owns its host lease and permanent call allowance. A cold model
    starts inside this function; a session may pass its already-ready model and
    remains responsible for final close. No result clock subtracts those costs.
    """
    if method.answer_parser != LOTUS_NATIVE:
        raise ValueError("lotus-native requires the unmodified LOTUS answer parser")
    import lotus
    import pandas as pd
    from src.baselines.text.movie_records import MovieRecordSource, validate_movie_method
    from src.baselines.text.sembench_movie import (
        run_original_lotus_query, evaluate_original, classification_audit, verify_checkout,
    )
    from src.infrastructure.private_artifacts import json_metric
    from src.semantic_methods.lotus.engine import (
        make_lm, lm_report, validate_filter_source, observe_filter_rows,
    )

    started = time.monotonic()
    report = dict(schema='semloom.lotus_native_local.v1', status='failed', query_id=query_id,
        protocol='local_async_model_IPC', production_core_used=False,
        scheduler_owner='original LOTUS LM / LiteLLM batch_completion',
        native_scheduling_retained=True, original_operators_used=True,
        cache=dict(result_cache_enabled=False, native_cache_implementation_retained=True),
        lifecycle='borrowed model; session closes it' if shared_model is not None else
                  'cold local model; startup included and shutdown separately reported')
    model = binding = lm = None
    failure = None
    emit = observer if observer is not None else lambda _event: None
    try:
        if type(source) is not MovieRecordSource:
            raise TypeError('native Movie requires MovieRecordSource')
        validate_movie_method(method, source.task.name)
        if type(profile) is not dict or set(profile) != {'native_lm_max_batch_size'}:
            raise ValueError('native profile requires only native_lm_max_batch_size')
        concurrency = profile['native_lm_max_batch_size']
        if type(concurrency) is not int or concurrency < 1:
            raise ValueError('native_lm_max_batch_size must be positive')
        budget = context['claimed_budget']
        if budget.reservation.unit_id != query_id or budget.attempts:
            raise ValueError('native query requires its fresh controller-claimed allowance')
        emit('preflight_start')
        verify_checkout(checkout)
        source_info = source.preflight(method)
        report.update(source=source_info, method=method.describe(), generation=asdict(generation),
            engine=asdict(engine), method_source=validate_filter_source(),
            resources=resources.describe(), profile=dict(profile))
        expected = source.expected(query_id)
        if expected and budget.remaining != len(expected):
            raise ValueError('native complete query requires its full declared call allowance')
        lm = make_lm(method, generation)
        lm.max_batch_size = concurrency
        model = shared_model or LocalNativeModel(engine, generation, resources, concurrency)
        if (model.engine != engine or model.generation != generation or model.resources != resources
                or model.concurrency != concurrency):
            raise ValueError('native query differs from the resident model binding')
        decisions = []
        emit('preflight_end')
        with model.query(lm, expected, budget, query_id=query_id) as binding:
            if model.ready_report is not None:
                report['owner_ready'] = deepcopy(model.ready_report)
                if 'inference_calls' in model.ready_report:
                    report['inference_calls'] = model.ready_report['inference_calls']
            def load():
                return pd.DataFrame([dict(reviewId=row['reviewId'], id=row['id'],
                    reviewText=row['reviewText'], _execution_row_id=row['row_id']) for row in source.rows()],
                    columns=['reviewId', 'id', 'reviewText', '_execution_row_id'])
            emit('query_execute_start')
            with lotus.settings.context(lm=lm, enable_cache=False):
                with observe_filter_rows(decisions.extend, binding.capture_prompts):
                    result = run_original_lotus_query(checkout, source.task.query, load)
            report.update(result=source.task.encode_result(result), decisions=[
                dict(row_id=row_id, decision=decision) for row_id, decision in decisions],
                **binding.report(), lm=lm_report(lm))
            source.task.validate_result(report['result'], source.rows(), report['decisions'])
            emit('result_ready')
            evaluated = time.monotonic()
            emit('evaluation_start')
            try:
                labeled = source.labeled_frame()
                report['quality'] = json_metric(evaluate_original(checkout, source.task.query,
                    result, labeled))
                labels = {row['row_id']: label for row, label in
                    zip(source.rows(), labeled['scoreSentiment'], strict=True) if source.task.accepts(row)}
                report['quality']['classification_audit'] = classification_audit(labels,
                    [(row_id, 'POSITIVE' if decision else 'NEGATIVE') for row_id, decision in decisions])
            finally:
                report['evaluation_seconds'] = time.monotonic() - evaluated
                emit('evaluation_end')
        report['status'] = 'completed'
    except BaseException as error:
        failure = error
    finally:
        if model is not None and shared_model is None:
            try:
                report['backend_close'] = model.close(**({} if resources.query_timeout_s is None else dict(timeout=resources.query_timeout_s)))
                emit('owner_closed')
                if report['backend_close']['status'] != 'closed' and failure is None:
                    failure = RuntimeError('native model physical cleanup is unconfirmed')
            except BaseException as error:
                if failure is None:
                    failure = error
                else:
                    failure.add_note('native model close failed: ' + type(error).__name__)
        elif model is not None:
            report['backend_close'] = dict(status='retained', engine_uid=
                None if model.ready_report is None else model.ready_report['engine_uid'])
        if binding is not None:
            final = report.get('backend_close', {}).get('native_final_accounting')
            report.update({key: value for key, value in final.items() if key not in ('schema', 'query_id')}
                          if final is not None else binding.report())
        if lm is not None:
            report['lm'] = lm_report(lm)
        report['query_seconds'] = time.monotonic() - started
    if failure is not None:
        report['status'] = 'failed'
        failure.query_report = report
        raise failure
    return report
