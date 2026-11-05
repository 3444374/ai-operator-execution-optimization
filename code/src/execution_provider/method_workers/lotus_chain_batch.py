"""Core-specific prepared-request grouping over the native finite LOTUS worker.

Native baselines use the serving worker directly and never select these policies.
"""

from dataclasses import replace

from src.serving.backends.lotus_chain_worker import (
    FusedChainBatch as NativeFusedChainBatch, _associate_declared_results, _check,
)
from src.serving.backends.vllm_protocol import IDENTITY_FIELDS


class FusedChainBatch(NativeFusedChainBatch):
    prepared_batching = None

    def _configure_batching(self, prepared_batching):
        self.prepared_batching, self._request_batcher = None, None
        self._source_flush_reason = self._source_prepared_group = None
        if prepared_batching is not None:
            from src.semantic_methods.lotus.prepared_batching import PreparedBatching
            self.prepared_batching = PreparedBatching(**prepared_batching)
            if self.engine_spec.chat_template is not None:
                raise ValueError('prepared_batching requires the tokenizer-owned chat template')
            if (any(stage.kind not in ('filter', 'map') for stage in self.chain.stages)
                    or len({stage.model_role for stage in self.chain.stages}) != 1):
                raise ValueError('prepared_batching requires Filter/Map with one model role')
        self._request_batcher_context = None

    def _bind_model_context(self):
        observed = super()._bind_model_context()
        self._request_batcher_context = observed['observation_signature']
        return observed

    def _prepare_requests(self, prepared, generation):
        groups = None
        if self._source_prepared_group is not None:
            if tuple(prepared) != self._source_prepared_group.rows:
                raise ValueError('reused prepared batch changed before inference')
            groups = (self._source_prepared_group,)
        elif self.prepared_batching is not None:
            try:
                if self._request_batcher is None:
                    from transformers import AutoTokenizer
                    from src.semantic_methods.lotus.prepared_batching import PreparedRequestBatcher

                    tokenizer = AutoTokenizer.from_pretrained(
                        self.engine_spec.tokenizer_path or self.engine_spec.model_path,
                        local_files_only=True, trust_remote_code=False)
                    self._request_batcher = PreparedRequestBatcher(self.prepared_batching, tokenizer)
                # Recount actual successor messages after every method preparation.
                groups = self._request_batcher.batches(prepared, generation)
                if self._source_flush_reason is not None:
                    if len(groups) != 1:
                        raise ValueError('source token batch no longer fits its prepared request budget')
                    groups = (replace(groups[0], reason=self._source_flush_reason),)
            except Exception as exc:
                # A local estimate cannot have submitted any of this stage's calls.
                self._raw_stage_id = prepared[0]['algorithm_stage']
                return [dict({name: row[name] for name in IDENTITY_FIELDS}, status='failed',
                    submission_confirmed=False, text=None, error_type='PreparedBatchingEstimateError',
                    cause_type=type(exc).__name__, prompt_tokens=None,
                    completion_tokens=None, total_tokens=None) for row in prepared]
        self._inference_groups = groups
        return None

    def _call_audit(self):
        audit = super()._call_audit()
        if self._inference_groups is not None:
            audit['request_batches'] = self._request_batches
        return audit

    def _make_engine(self, generation, declared, audit):
        from src.serving.backends.vllm_sync_process import _publish

        try:
            return super()._make_engine(generation, declared, audit)
        except Exception:
            if self._inference_groups is not None:
                for call in declared:
                    call.update(status='failed', text=None,
                        error_type='BatchStoppedBeforeSubmission', stop_reason='ModelInitializationFailed')
                try:
                    _publish(self._audit_path, audit)
                except BaseException as recording:
                    self._audit_errors.append('call_ledger_write: ' + type(recording).__name__)
            raise

    def _run_prepared(self, prepared, generation, declared, audit):
        if self._inference_groups is None:
            return super()._run_prepared(prepared, generation, declared, audit)
        return self._infer_groups(self._inference_groups, declared, generation, audit)

    def _begin_batch(self, source_flush_reason, prepared_info):
        self._request_batches = []
        self._source_flush_reason = source_flush_reason
        self._source_prepared_group = None

    def _method_options(self, prepared, prepared_info):
        if prepared_info is None:
            return {}
        self._source_prepared_group = self._reused_group(prepared, prepared_info)
        return dict(prepared_rows=prepared)

    def _batch_receipts(self, envelope, *, failed):
        if failed:
            if self.prepared_batching is not None:
                # A malformed response or interruption may bypass method parsing.
                # Retain native facts beside its unconfirmed declarations.
                native = {call['call_id']: call for call in self._audit_calls}
                for call in envelope['calls']:
                    fact = native.get(call['call_id'])
                    if (fact is not None and call.get('status') == 'unknown'
                            and call.get('submission_confirmed') is not True
                            and call.get('text') is None and call.get('total_tokens') is None):
                        call.update(fact)
                envelope['request_batches'] = list(self._request_batches)
        else:
            self._source_prepared_group = None
            if self.prepared_batching is not None:
                envelope['request_batches'] = list(self._request_batches)
        return envelope

    def _infer_groups(self, groups, declared, generation, audit):
        """Each group invokes the existing native list API, without retries.

        The logical receipt declares an attempt; physical acceptance is known
        only from each response. Unsubmitted suffix rows never become unknown
        model work merely because an earlier group failed or was cancelled.
        """
        from src.serving.backends.vllm_sync_process import _publish
        from src.semantic_methods.lotus.engine import validate_engine_output

        results, offset, stopped = [], 0, None
        for index, group in enumerate(groups):
            if stopped is None:
                try:
                    _check(self.deadline, self.control)
                except (InterruptedError, TimeoutError) as exc:
                    stopped = type(exc).__name__
            if stopped is not None:
                results.extend(dict({name: row[name] for name in IDENTITY_FIELDS},
                    status='failed', submission_confirmed=False, text=None,
                    error_type='BatchStoppedBeforeSubmission', stop_reason=stopped,
                    prompt_tokens=None, completion_tokens=None, total_tokens=None) for row in group.rows)
                offset += len(group.rows)
                continue
            receipt = dict(request_batch_id=f'{self._batch_id}:{self._raw_stage_id}:{index}',
                call_ids=[row['call_id'] for row in group.rows],
                policy=self.prepared_batching.policy, token_budget=self.prepared_batching.token_budget,
                max_rows=self.prepared_batching.max_rows, input_tokens=group.input_tokens,
                output_cap_tokens=group.output_cap_tokens, estimated_tokens=group.estimated_tokens,
                flush_reason=group.reason, oversize=group.oversize,
                cost_scope='local_tokenizer_estimate_not_native_usage',
                submission_scope='synchronous_engine_batch_api',
                submission_confirmation='per_call_ledger',
                model_context_signature=self._request_batcher_context,
                engine_uid=self._model_context['engine_uid'])
            self._request_batches.append(receipt)
            _publish(self._audit_path, audit)
            batch_results = self.engine.run_prepared(list(group.rows), generation=generation)
            results.extend(batch_results)
            declarations = declared[offset:offset + len(group.rows)]
            raw = _associate_declared_results(declarations, batch_results)
            # Keep every confirmed prefix before another process call can fail.
            _publish(self._audit_path, dict(audit, raw_results=raw))
            if (raw or len(batch_results) != len(group.rows) or any(
                    call.get('status') != 'succeeded' or call.get('submission_confirmed') is not True
                    or 'engine_protocol_error' in call
                    or call.get('error_type') == 'NativeTaskUnconfirmed' for call in declarations)):
                stopped = 'PreviousBatchUnconfirmedOrFailed'
            else:
                try:
                    for call in declarations:
                        validate_engine_output(call)
                except ValueError:
                    # Keep native facts; the original method still owns the error,
                    # accounting and parser once the complete stage is returned.
                    stopped = 'PreviousBatchInvalidResponse'
            offset += len(group.rows)
        return results

    def _reused_group(self, prepared, info):
        from src.semantic_methods.lotus.prepared_batching import (
            PreparedRequestBatch, preparation_signature,
        )

        config = self.prepared_batching
        if (config is None or config.policy != 'token_budget' or len(self.chain.stages) != 1
                or type(info) is not dict or set(info) != {
                    'reason', 'input_tokens', 'output_cap_tokens', 'signature'}):
            raise ValueError('prepared reuse requires one token-batched Filter or Map')
        stage = self.chain.stages[0]
        if (info['signature'] != preparation_signature(stage, self.engine_spec, config)
                or info['reason'] != self._source_flush_reason or info['reason'] not in (
                    'token_budget', 'max_rows', 'max_bytes', 'compatibility', 'source_exhausted')
                or type(info['input_tokens']) is not int or info['input_tokens'] < 0
                or type(info['output_cap_tokens']) is not int
                or info['output_cap_tokens'] != len(prepared) * stage.generation.max_tokens
                or not prepared or config.max_rows is not None and len(prepared) > config.max_rows):
            raise ValueError('prepared batch changed its method, parameters or estimated cost')
        oversize = info['input_tokens'] + info['output_cap_tokens'] > config.token_budget
        if oversize and len(prepared) != 1:
            raise ValueError('only one complete oversize request may exceed B')
        return PreparedRequestBatch(tuple(prepared), info['input_tokens'], info['output_cap_tokens'],
                                    info['reason'], oversize)
