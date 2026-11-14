"""Complete original text queries over sequential native Daft prompt graphs.

Each real LOTUS LM batch uses the existing isolated prepared worker. A consumed
result releases that worker before the next graph; the final original sink
releases the final worker. No model reuse or grouped TopK is implemented here.
"""

from collections import Counter
from concurrent.futures import Future
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Lock, Thread, get_ident
import time

from src.baselines.text.sembench_prepared import make_query_lm, audit_query_batches, result_value
from src.baselines.text.sembench_text import SemBenchTextQuery
from src.baselines.text.frameworks.daft_sembench_native import effective_openai_messages
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.native_daft_query import supervised_run, _save_artifact, native_openai_options
from src.infrastructure.private_artifacts import (
    new_private_directory, write_private_json, read_json, file_sha256, json_metric, content_digest,
)
from src.serving.backends.vllm_protocol import GenerationSpec


def _query(config, generation):
    aggregate = config['scenario'] == 'mmqa' and str(config['query']).lower().removeprefix('q') == '5'
    attached = 'native_openai' in config
    options = {}
    if aggregate:
        from src.baselines.text.sembench_mmqa_aggregate import load_token_counter
        counter = config.get('token_counter')
        fields = {'kind', 'path', 'sha256', 'tokenizer_id', 'tokenizer_revision'}
        if (type(counter) is not dict or set(counter) != fields
                or counter['kind'] != 'huggingface_tokenizer_file'
                or any(type(counter[name]) is not str or not counter[name].strip() for name in fields)):
            raise ValueError('aggregation requires the declared local tokenizer JSON identity')
        engine = config['engine_kwargs']
        context = config.get('native_lm_max_ctx_len')
        if (type(context) is not int or generation.max_tokens >= context
                or (not attached and engine.get('max_model_len') != context)):
            raise ValueError('aggregation requires the declared native context with input space after max_tokens')
        if not attached:
            if Path(counter['path']).resolve() != Path(engine.get('tokenizer', config['model'])).resolve() / 'tokenizer.json':
                raise ValueError('LOTUS counter must be the declared native model/tokenizer tokenizer.json')
            revision = engine.get('tokenizer_revision') or engine.get('revision')
            if revision is not None and revision != counter['tokenizer_revision']:
                raise ValueError('LOTUS counter revision differs from the declared native tokenizer revision')
        if generation.repetition_penalty != 1 or generation.logprobs is not None or generation.response_format is not None:
            raise ValueError('native Q5 retains ordinary generation and the original unstructured parser')
        options = dict(max_ctx_len=context, tokenizer=load_token_counter(counter))
    elif 'token_counter' in config or 'native_lm_max_ctx_len' in config:
        raise ValueError('explicit local token counting currently belongs to native aggregation')
    visual = {name: config[name] for name in ('visual_query', 'image_join', 'manifest', 'manifest_digest')
              if name in config}
    if any(name in visual and type(visual[name]) is not bool for name in ('visual_query', 'image_join')):
        raise ValueError('visual_query and image_join must be booleans')
    if attached and (generation.response_format is not None or generation.logprobs is not None
                     or generation.repetition_penalty != 1):
        raise ValueError('attached queries require plain base generation; original operators own JSON overrides')
    query = SemBenchTextQuery(config['sembench_checkout'], config['data_dir'],
        config['scenario'], config['query'], execution_id=config['query_id'],
        model_name=config['model_name'], generation=generation,
        scale_factor=config.get('scale_factor'), **visual,
        **(dict(lm_options=options) if aggregate else {}))
    if query.visual_inputs is not None and not attached:
        raise ValueError('the native Daft string-prompt provider has no per-row image input')
    if query.operation is not None and query.operation.describe()['kind'] == 'topk':
        raise ValueError('native Daft TopK remains disabled')
    return query


def _audit_attached_batches(batches, model):
    """Check native HTTP receipts after the original complete memory sink."""
    attempts = unresolved = 0
    response_ids, occurrences = [], []
    for index, batch in enumerate(batches):
        if not batch['requests']:
            continue
        api = batch['native']['metadata']['native_api']
        calls = api['requests']
        encoded = [dict(batch_index=index, call_id=row['call_id'],
            original_messages_sha256=content_digest(row['messages']),
            effective_messages_sha256=content_digest(effective_openai_messages(row['messages'])))
            for row in batch['requests']]
        occurrences.extend(encoded)
        expected = Counter(row['effective_messages_sha256'] for row in encoded)
        successful = [call for call in calls if call['error_type'] is None
                      and call['http_status'] is not None and 200 <= call['http_status'] < 300]
        if (Counter(call['messages_sha256'] for call in successful) != expected
                or any(call['messages_sha256'] not in expected or call['model'] != model for call in calls)):
            raise ValueError('native OpenAI attempts differ from the effective complete message batch')
        response_ids.extend(call['response']['id'] for call in successful)
        attempts += api['reserved_attempts']
        unresolved += api['unresolved']
    if len(set(response_ids)) != len(response_ids):
        raise ValueError('native OpenAI completions require distinct response IDs')
    return dict(reserved_attempts=attempts, successful_responses=len(response_ids),
        unresolved_attempts=unresolved, raw_HTTP_call_id_association_available=False,
        message_occurrences=occurrences,
        result_row_association='native Daft call_id projection; HTTP receipt association is per encoded message batch')


def run_query(config, directory, *, model_python, tokenizer=None, supervise=supervised_run,
              monitor=None):
    started = time.monotonic_ns()
    fields = {'schema', 'sembench_checkout', 'data_dir', 'scenario', 'query', 'query_id',
              'model_name', 'model', 'sampling_params', 'engine_kwargs', 'provider_options',
              'query_timeout_s', 'cleanup_s', 'stop_grace_s', 'max_model_calls'}
    optional = {'scale_factor', 'token_counter', 'native_lm_max_ctx_len'}
    attached = 'native_openai' in config
    if attached:
        optional |= {'native_openai', 'visual_query', 'image_join', 'manifest', 'manifest_digest'}
    if (config.get('schema') != 'semloom.native_daft_sembench.v1'
            or set(config) - optional != fields):
        raise ValueError('explicit native Daft SemBench configuration required')
    if attached:
        native_openai_options(config)
    sampling = dict(config['sampling_params'])
    if sampling.pop('n') != 1:
        raise ValueError('the original single-output query requires n=1')
    generation = GenerationSpec.from_dict(sampling)
    budget = AttemptBudget(config['query_id'], config['max_model_calls'])
    directory = Path(directory)
    new_private_directory(directory)
    deadline = started / 1e9 + config['query_timeout_s']
    report = dict(schema='semloom.native_daft_sembench.result.v2', status='failed',
        backend='daft-native-ai', worker_entry_ns=started, source_start_ns=None,
        baseline_name='LOTUS over Daft native AI', semantic_profile='original LOTUS/SemBench query, formatter, algorithms and parser',
        source_to_sink_seconds=None, query_clock_version='lotus_daft.source_to_sink.v1',
        strict_controller_entry_ns=None, model_ready_before_source=False,
        strict_controller_entry_reason='host imports, preflight and lease precede this function',
        clock='time.monotonic_ns on one host', warmup=0, whole_query_retries=0,
        result_cache=False, prefix_cache='native provider/config; effective value observed when exposed',
        lifecycle='one original query; isolated native collect/engine per sequential LM batch',
        timing_scope='fresh original source, original SDK algorithms/token counting, prompt serialization, '
            'prepared packet and native API evidence I/O, IPC/polling, cold native graphs and required intermediate close, '
            'complete original memory sink; final worker close, source audit, evaluation and driver persistence excluded',
        physical_gpu_exit_confirmed=None, performance_claim=False, native_batches=[])
    if attached:
        report.update(provider='Daft builtin OpenAI over attached vLLM-compatible API',
            message_encoding='daft.openai.system_single_user.parts.v1',
            target_renderer_equivalence_verified=None,
            message_encoding_difference='user string becomes one text content part; existing content-part arrays stay unchanged; '
                'target renderer/token equivalence has not been verified',
            model_process_owned=False, model_ready_before_source=None,
            service_startup_in_query_seconds=None,
            lifecycle='one original query; isolated native API-client worker per sequential LM batch; remote service externally owned',
            client_release_boundary='worker process exit after result ACK; native SDK explicit close unobserved',
            timing_scope='fresh original source and required image decode, original SDK methods and complete native Daft '
                'OpenAI graphs, required intermediate worker exits, HTTP attempt accounting/fingerprints/API receipts '
                'and complete original sink; remote READY/startup attribution unobserved; final worker exit, '
                'post-query audits, scoring and persistence excluded')
    ledger = unit = primary = thread = None
    batches = []
    state = {}
    events = Queue(maxsize=1)
    stopped, handoff_lock = Event(), Lock()
    pending = [None]
    try:
        query = _query(config, generation)
        if attached and query.aggregate_task is not None:
            report['message_encoding'] = 'daft.openai.single_user.parts.v1'
        if tokenizer is None and not attached:
            from transformers import AutoTokenizer
            engine = config['engine_kwargs']
            revision = engine.get('tokenizer_revision') or engine.get('revision')
            tokenizer = AutoTokenizer.from_pretrained(engine.get('tokenizer', config['model']),
                local_files_only=True, trust_remote_code=False,
                **(dict(revision=revision) if revision is not None else {}))
        description = report['query'] = query.describe()
        minimum = description.get('minimum_model_calls', description['expected_model_calls'])
        if minimum > budget.limit:
            raise ValueError('physical budget cannot cover the known minimum original requests')
        ledger = CellBudgetLedger.create(directory / 'physical-budget.sqlite', budget,
            deadline_utc=time.time() + max(0, deadline - time.monotonic()))
        ledger.reserve_unit('query', budget.limit)
        unit = ledger.claim_shared_unit('query')
        execute_query = query.bind_original_execution()

        def remaining():
            value = deadline - time.monotonic()
            if value <= 0:
                raise TimeoutError('complete original Daft query exceeded its absolute allowance')
            return value

        def next_event():
            try:
                return events.get(timeout=remaining())
            except Empty as error:
                raise TimeoutError('complete original Daft query exceeded its absolute allowance') from error

        def predict(requests, *, sampling_params):
            if get_ident() != thread.ident:
                raise ValueError('native Daft query bridge supports sequential LM calls; grouped TopK remains disabled')
            if sum(len(batch['requests']) for batch in batches) > budget.limit:
                raise BudgetExhausted('original query exceeds its finite physical request allowance')
            if not requests:
                return dict(predictions=[])
            prepared = []
            for request in requests:
                if attached:
                    prepared.append(request)
                    continue
                if any(isinstance(message['content'], list) and
                        any(part.get('type') != 'text' for part in message['content']) for message in request['messages']):
                    raise ValueError('native Daft string prompts cannot carry original image parts')
                prepared.append(dict(call_id=request['call_id'], prompt=tokenizer.apply_chat_template(
                    request['messages'], tokenize=False, add_generation_prompt=True)))
            reply = Future()
            with handoff_lock:
                if stopped.is_set():
                    raise InterruptedError('native Daft query consumer stopped')
                pending[0] = reply
            events.put(('batch', prepared, sampling_params, reply), timeout=remaining())
            return reply.result(timeout=remaining())

        lm, batches = make_query_lm(query, predict, **query.lm_options)
        if query.aggregate_task is not None:
            report['lotus_lm'] = dict(max_ctx_len=lm.max_ctx_len,
                input_token_budget=lm.max_ctx_len-lm.max_tokens, token_counter=config['token_counter'],
                model_tokenizer_verified=False, scope=(
                    'declared external model context and local counter; remote context/tokenizer match unverified'
                    if attached else 'declared native context/tokenizer only; actual target loading unverified'))

        def original_query():
            import lotus
            try:
                with lotus.settings.context(lm=lm, enable_cache=False):
                    report['source_start_ns'] = time.monotonic_ns()
                    value = execute_query()
                    report['sink_ready_ns'] = time.monotonic_ns()
                    state['result'] = value
                    report['source_to_sink_seconds'] = (report['sink_ready_ns']-report['source_start_ns']) / 1e9
                    remaining()
                event = ('sink', value)
            except BaseException as error:
                event = ('failed', error)
            if not stopped.is_set():
                events.put(event, timeout=max(0, deadline-time.monotonic()))

        report['preflight_finished_ns'] = time.monotonic_ns()
        thread = Thread(target=original_query, name='native-daft-original-query', daemon=True)
        thread.start()
        event = next_event()
        index = 0
        while event[0] == 'batch':
            _, requests, native_sampling, reply = event
            phase = directory if index == 0 else directory / ('batch-' + str(index))
            if index:
                new_private_directory(phase)
            packet, worker_path = phase / 'prepared-prompts.json', phase / 'worker-config.json'
            write_private_json(packet, dict(schema=('semloom.native_daft_messages.v1' if attached
                                                    else 'semloom.native_daft_prompts.v1'), requests=requests))
            worker = {key: config[key] for key in ('model', 'engine_kwargs', 'provider_options', 'cleanup_s', 'stop_grace_s')}
            worker.update(schema='semloom.native_daft_map.v1', sampling_params=native_sampling,
                query_timeout_s=remaining(), requests_path=str(packet), requests_sha256=file_sha256(packet),
                request_budget=dict(path=str(directory/'physical-budget.sqlite'), budget_id=budget.budget_id,
                    limit=budget.limit, unit_id='query'), wait_for_result_release=True)
            if attached:
                worker['native_openai'] = config['native_openai']
            write_private_json(worker_path, worker)
            current = dict(index=index, input_call_ids=[row['call_id'] for row in requests], status='submitted',
                           worker_artifacts=str((phase/'native-worker').relative_to(directory)))
            report['native_batches'].append(current)
            following = None
            raw_result = None

            def accept(raw):
                nonlocal following, raw_result
                if reply.done():
                    raise ValueError('duplicate native Daft result callback')
                raw_result = raw['result']
                current.update(raw_native_ready_ns=raw['result_ready_monotonic_ns'],
                    raw_native_observed_ns=time.monotonic_ns())
                reply.set_result(raw_result)
                following = next_event()
                if following[0] == 'failed':
                    raise following[1]

            current['supervision'] = supervise(worker_path, phase/'native-worker',
                model_python=model_python, monitor=monitor, on_result_ready=accept)
            terminal = read_json(phase/'native-worker'/'worker.json')
            current.update(native_api=terminal.get('native_api'), model_close_receipt=terminal.get('model_close_receipt'),
                           result_release=terminal.get('result_release'))
            if terminal['status'] != 'completed' or following is None:
                raise RuntimeError('native worker did not establish complete predictions and cleanup')
            current['status'] = 'completed'
            if raw_result is not None:
                raw_result['metadata']['model_close_receipt'] = current['model_close_receipt']
            event = following
            index += 1
        if event[0] == 'failed':
            raise event[1]
        report['semantic_audit_started_ns'] = time.monotonic_ns()
        query.verify_source()
        if query.aggregate_task is not None:
            query.aggregate_task.validate_batch_sizes([len(batch['requests']) for batch in batches], state['result'])
        report['semantic'] = audit_query_batches(batches)
        report['logical_requests'] = sum(len(batch['requests']) for batch in batches)
        if (unit.attempts < report['logical_requests'] if attached
                else unit.attempts != report['logical_requests']):
            raise RuntimeError('native physical accounting differs from original LM occurrences')
        if attached:
            report['native_HTTP_attempts'] = unit.attempts
            report['native_retry_policy'] = 'builtin OpenAI client configuration/defaults; no outer query retry'
            report['native_HTTP_audit'] = _audit_attached_batches(batches, config['model'])
            report.update(cost=None, cost_available=False)
        report['semantic_audit_finished_ns'] = time.monotonic_ns()
        report['evaluation_started_ns'] = time.monotonic_ns()
        report['quality'] = json_metric(query.evaluate(state['result']))
        report['evaluation_finished_ns'] = time.monotonic_ns()
        if report['native_batches']:
            last = report['native_batches'][-1]
            report.update(native_api=last['native_api'], model_close_receipt=last['model_close_receipt'],
                raw_native_ready_ns=last['raw_native_ready_ns'], raw_native_observed_ns=last['raw_native_observed_ns'],
                supervision=last['supervision'])
        report['status'] = 'completed'
    except BaseException as error:
        primary = error
        report['error_type'] = type(error).__name__
        if report['native_batches'] and report['native_batches'][-1]['status'] == 'submitted':
            report['native_batches'][-1].update(status='failed', error_type=type(error).__name__)
        raise
    finally:
        with handoff_lock:
            stopped.set()
            if pending[0] is not None and not pending[0].done():
                pending[0].set_exception(primary or InterruptedError('native Daft query stopped'))
        if thread is not None:
            if thread.ident is not None:
                thread.join(timeout=config['stop_grace_s'])
            report['query_thread_closed'] = not thread.is_alive()
            if thread.is_alive():
                error = RuntimeError('native Daft original query thread did not finish')
                if primary is None:
                    primary = error
                else:
                    primary.add_note(str(error))
        if ledger is not None:
            try:
                ledger.close_shared_unit('query')
                report['physical_attempts_reserved'] = unit.attempts
                report['budget'] = ledger.snapshot()
            except BaseException as error:
                if primary is None:
                    primary = error
                else:
                    primary.add_note('native budget close failed: ' + type(error).__name__)
        try:
            report['native_lm_batches'] = [dict(index=index, status=batch['status'], occurrences=len(batch['requests']),
                sampling_params=batch['sampling_params']) for index, batch in enumerate(batches)]
            if 'result' in state:
                report['result'] = result_value(state['result'])
                _save_artifact(directory/'result-ready.json', dict(result=report['result'],
                    semantic=report.get('semantic'), sink_ready_ns=report['sink_ready_ns']), primary=primary, publish=True)
            _save_artifact(directory/'native-lm-batches.json',
                [{key: value for key, value in batch.items() if key != 'native'} for batch in batches], primary=primary)
        except BaseException as error:
            report['status'] = 'failed'
            if primary is None:
                primary = error
            else:
                primary.add_note('native Daft finalization failed: ' + type(error).__name__)
        if primary is not None:
            report['status'] = 'failed'
            report.setdefault('error_type', type(primary).__name__)
            report.pop('quality', None)
        report['final_persistence_started_ns'] = time.monotonic_ns()
        _save_artifact(directory/'driver.json', report, primary=primary)
        _save_artifact(directory/'persistence.json', dict(driver_json_started_ns=report['final_persistence_started_ns'],
            driver_json_finished_ns=time.monotonic_ns(), excludes='this receipt publication'), primary=primary)
        if primary is not None:
            primary.query_report = report
            raise primary
    return report
