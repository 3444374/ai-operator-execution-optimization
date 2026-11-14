"""Text Extract through author SemMap; explicit engineering task adaptation."""

from dataclasses import replace

from .movie_map import MapOperation


class ExtractMapOperation(MapOperation):
    kind = 'extract'
    source_metadata = dict(
        policy='original text Extract relation; engineering JSON instruction over author SemMap',
        native_retry_policy='author SemMap length branch; no transport retry',
        stage_generation_scope='query metadata only; native SemMap owns sampling without JSON-format forwarding',
    )

    def restore_relation(self, operation, texts):
        from lotus.sem_ops.postprocessors import extract_postprocess
        from src.semantic_methods.lotus.record_method import LotusRecordMethod

        # Keep original string materialization, actual keys and label writes;
        # missing/empty output follows the original query's error behavior.
        parsed = extract_postprocess(texts, LotusRecordMethod(operation.stage).lm, False).outputs
        completed = [replace(row, index_updates=values)
                     for row, values in zip(operation.records, parsed, strict=True)]
        return operation.result_frame(completed)

    def report_metadata(self, source):
        return dict(super().report_metadata(source),
            baseline_name='Kalypso author SemMap with project JSON Extract task adaptation',
            semantic_operator='SemMap over original text Extract inputs; original relation and evaluator',
            prompt_scope='project output-field JSON instruction inside author Map template; not LOTUS prompt',
            parser_identity='LOTUS 1.2.4 extract_postprocess and original label-based result writes',
            author_original_semantic_method=False,
            native_extract_operator=False, native_JSON_format_forwarded=False,
            task_adaptation='sembench-extract-map; engineering variant, not author-original Extract',
        )
