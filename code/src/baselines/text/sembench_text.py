"""Original single-operator SemBench text queries and their input boundary.

The upstream query still owns selection and relational postprocessing. An
execution adapter binds its one accessor; exact Join and TopK require the
explicit original LOTUS resident owner, separate from prepared Filter/Map/Extract.
"""

from contextlib import contextmanager
from dataclasses import dataclass, replace
from numbers import Integral
from pathlib import Path
import threading

from src.infrastructure.private_artifacts import file_identity
from src.semantic_methods.lotus.record_types import (
    ExtractMethod, MapMethod, RecordOccurrence, StageSpec,
)
from src.semantic_methods.lotus.records import FilterMethod

from .sembench_source import SEMBENCH_COMMIT, verify_checkout


_OPERATOR_SCOPE = threading.RLock()
MOVIE_QUERY_IDS = ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10')


@dataclass(frozen=True)
class TextOperation:
    frame: object
    stage: StageSpec
    records: tuple[RecordOccurrence, ...]

    @property
    def generation(self):
        return self.stage.generation

    def describe(self):
        return dict(kind=self.stage.kind, stage=self.stage.describe(),
                    input_rows=len(self.records), expected_model_calls=len(self.records),
                    input_columns=list(self.frame.columns),
                    model_columns=list(self.stage.method.input_columns if self.stage.kind == 'extract'
                                       else self.stage.method.resolved_input_columns),
                    source_index_dtype=str(self.frame.index.dtype),
                    source_index_name=self.frame.index.name,
                    model_cell_representation='native DEFAULT per-cell text; original dataframe unchanged')

    def result_frame(self, records):
        """Restore original non-model columns and native label-based writes."""
        ordered = sorted(records, key=lambda row: row.source_position)
        if self.stage.kind == 'filter':
            return self.frame.iloc[[row.source_position for row in ordered]].copy(deep=True)
        result = self.frame.copy(deep=True)
        if self.stage.kind == 'map':
            result[self.stage.method.output_column] = [row.values[self.stage.method.output_column] for row in ordered]
        else:
            # Ordinary Extract writes actual keys (including collisions), not
            # a declared-schema projection. Reuse the recorded native updates.
            for row in ordered:
                for key, value in row.index_updates.items():
                    if key not in result.columns:
                        result[key] = None
                    result.loc[row.source_index, key] = value
        return result


@dataclass(frozen=True)
class TopKOperation:
    """The author's full quick ranking; requests are comparisons, not rows."""
    frame: object
    model_name: str
    generation: object
    instruction: str
    records: tuple[RecordOccurrence, ...]

    def describe(self):
        count = len(self.records)
        return dict(kind='topk', method='quick', K=count, input_rows=count,
            expected_model_calls=count * (count - 1) // 2,
            call_count_kind='upper_bound; one request per native binary comparison',
            input_columns=list(self.frame.columns), model_columns=['reviewText'],
            instruction=self.instruction, result_cache=False, cascade=False,
            pivot='original numpy.random.randint; actual RNG state recorded')


@dataclass(frozen=True)
class JoinOperation:
    """The author's exact self Join; every ordered source pair is a request."""
    frame: object
    model_name: str
    generation: object
    instruction: str
    records: tuple[RecordOccurrence, ...]

    def describe(self):
        count = len(self.frame)
        return dict(kind='join', method='exact', input_rows=count, right_input_rows=count,
            expected_model_calls=count * count, call_count_kind='exact ordered Cartesian pairs',
            input_columns=list(self.frame.columns), model_columns=['reviewText:left', 'reviewText:right'],
            instruction=self.instruction, result_cache=False, cascade=False,
            source_pair_order='left-major; original positions, including same IDs and text')


def _operation(kind, frame, args, kwargs, *, model_name, generation, query_id):
    """Translate only the argument shapes used by the selected original queries."""
    import lotus
    from lotus.types import SerializationFormat

    if lotus.settings.serialization_format != SerializationFormat.DEFAULT:
        raise ValueError('this record bridge uses original LOTUS DEFAULT text serialization')
    if kind == 'join':
        if (len(args) != 1 or not args[0].equals(frame) or set(kwargs) != {'join_instruction'}
                or set(lotus.nl_expression.parse_cols(kwargs['join_instruction']))
                    != {'reviewText:left', 'reviewText:right'}):
            raise ValueError('only original Movie Q5/Q6/Q7 exact self Join is bound')
        if generation.response_format is not None or generation.logprobs is not None:
            raise ValueError('ordinary Join uses the original Boolean answers without helper sampling')
        records = []
        rows = list(frame[['reviewText']].iterrows())
        for left, (left_index, left_row) in enumerate(rows):
            for right, (right_index, right_row) in enumerate(rows):
                records.append(RecordOccurrence(query_id + f'-pair-{left}-{right}', len(records),
                    f'{int(left_index)}:{int(right_index)}',
                    {'reviewText:left': str(left_row['reviewText']),
                     'reviewText:right': str(right_row['reviewText'])}))
        return JoinOperation(frame.copy(deep=True), model_name, generation,
                             kwargs['join_instruction'], tuple(records))
    if kind == 'topk':
        if (len(args) != 1 or kwargs != dict(K=len(frame), method='quick', return_stats=True)
                or lotus.nl_expression.parse_cols(args[0]) != ['reviewText']):
            raise ValueError('only original Movie Q9 quick TopK with K=N is bound')
        if (generation.response_format is not None or generation.logprobs is not None
                or generation.repetition_penalty != 1):
            raise ValueError('ordinary TopK uses plain binary answers without helper sampling')
        records = tuple(RecordOccurrence(query_id + '-row-' + str(i), i,
            int(label) if isinstance(label, Integral) else label, {'reviewText': str(row['reviewText'])})
            for i, (label, row) in enumerate(frame[['reviewText']].iterrows()))
        return TopKOperation(frame.copy(deep=True), model_name, generation, args[0], records)
    if kind in ('filter', 'map'):
        if len(args) != 1 or kwargs:
            raise ValueError('this original query profile uses one ordinary instruction without overrides')
        method = (FilterMethod(model_name, args[0]) if kind == 'filter' else
                  MapMethod(model_name, args[0], output_column='_map'))
    else:
        if (len(args) != 2 or set(kwargs) - {'extract_quotes', 'return_raw_outputs'}
                or kwargs.get('extract_quotes', False) is not False
                or kwargs.get('return_raw_outputs', False) is not False):
            raise ValueError('the selected original Extract uses ordinary JSON fields without quotes/raw outputs')
        method = ExtractMethod(model_name, tuple(args[0]), args[1])
        generation = replace(generation, response_format={'type': 'json_object'})
    stage = StageSpec(kind, method, generation).bind()
    columns = (method.input_columns if kind == 'extract' else stage.method.resolved_input_columns)
    records = []
    # DEFAULT df2text formats each cell from the projected row Series. Preserve
    # that representation (including CSV NaN -> "nan") at the JSON wire seam;
    # never fill missing source values or change the original relation.
    for position, (label, row) in enumerate(frame[list(columns)].iterrows()):
        label = int(label) if isinstance(label, Integral) else label
        values = {name: str(row[name]) for name in columns}
        records.append(RecordOccurrence(query_id + '-row-' + str(position), position, label, values))
    return TextOperation(frame.copy(deep=True), stage, tuple(records))


@contextmanager
def _operators(handler):
    """One isolated query scope; original methods remain available to the adapter."""
    import lotus
    import pandas as pd

    originals = []
    with _OPERATOR_SCOPE, lotus.settings.context(enable_cache=False):
        try:
            for kind in ('filter', 'map', 'extract', 'topk', 'join'):
                cls = type(getattr(pd.DataFrame(), 'sem_' + kind))
                original = cls.__call__
                originals.append((cls, original))

                def call(accessor, *args, _kind=kind, _original=original, **kwargs):
                    return handler(_kind, accessor._obj, args, kwargs,
                                   lambda: _original(accessor, *args, **kwargs))

                cls.__call__ = call
            yield
        finally:
            for cls, original in reversed(originals):
                cls.__call__ = original


class _Captured(BaseException):
    pass


class SemBenchTextQuery:
    """A finite frozen input snapshot and the unchanged original query/evaluator."""

    def __init__(self, checkout, data_dir, scenario, query_id, *, execution_id,
                 model_name, generation, scale_factor=None, ranking=None):
        import pandas as pd
        from .sembench_medical import FULL_SCALE_FACTOR, TEXT_QUERY_IDS as MEDICAL_QUERY_IDS, text_files
        from .sembench_mmqa import TEXT_QUERY_IDS as MMQA_QUERY_IDS

        query = str(query_id).lower().removeprefix('q')
        if not ((scenario == 'movie' and query in MOVIE_QUERY_IDS)
                or (scenario == 'mmqa' and query in MMQA_QUERY_IDS)
                or (scenario == 'medical' and query in MEDICAL_QUERY_IDS)):
            raise ValueError('this entry supports Movie Q1-Q10 (Q5-Q7 native Join, Q9 Map or TopK), twelve ordinary MMQA text queries '
                             'and Medical Q1/Q4/Q10')
        if scenario != 'medical' and scale_factor is not None:
            raise ValueError('scale_factor applies only to the Medical authored CSV filenames')
        if ranking is not None and (scenario != 'movie' or query != '9' or ranking not in ('map', 'topk')):
            raise ValueError('ranking=map/topk applies only to Movie Q9')
        if type(execution_id) is not str or not execution_id.strip():
            raise ValueError('each execution needs a nonempty independent query_id')
        self.checkout = verify_checkout(checkout)
        self.scenario, self.query, self.query_id = scenario, query, execution_id
        self.ranking = ranking or 'map'
        self.model_name, self.generation = model_name, generation
        self.data_dir = Path(data_dir).resolve()
        self.evaluation_files = set()
        if scenario == 'movie':
            names = ['Reviews.csv'] + (['Movies.csv'] if query == '10' else [])
            self.evaluation_files.add('Movies.csv')
        elif scenario == 'medical':
            self.scale_factor = FULL_SCALE_FACTOR if scale_factor is None else scale_factor
            self.symptoms_file, self.patients_file, self.labels_file = text_files(self.scale_factor)
            names = [self.symptoms_file, self.labels_file]
            if query == '4':
                names.append(self.patients_file)
            self.evaluation_files.add(self.labels_file)
        elif query == '1':
            names = ['ben_piazza.csv', 'ben_piazza_text_data.csv']
        elif query.startswith('3') or query == '4':
            names = ['lizzy_caplan_text_data.csv']
        else:
            names = ['tampa_international_airport.csv']
        self.frames, self.files = {}, {}
        for name in names:
            path = self.data_dir / name
            before = file_identity(path)
            self.frames[name] = pd.read_csv(path)
            if file_identity(path) != before:
                raise ValueError('SemBench input changed while its snapshot was read')
            self.files[name] = dict(before, rows=len(self.frames[name]),
                                    columns=list(self.frames[name].columns))
        self.operation = None

        def capture(kind, frame, args, kwargs, invoke):
            self.operation = _operation(kind, frame, args, kwargs, model_name=model_name,
                generation=generation, query_id=execution_id)
            raise _Captured()

        with _operators(capture):
            try:
                self._original()
            except _Captured:
                pass

    def _load(self, name, **kwargs):
        if name not in self.frames or kwargs not in ({}, {'sep': ',', 'quotechar': '"'}):
            raise ValueError('unexpected input requested by the selected original query')
        frame = self.frames[name].copy(deep=True)
        if self.scenario == 'movie' and name == 'Reviews.csv':
            # Evaluation labels never enter the operator's input relation.
            frame = frame[['reviewId', 'id', 'reviewText']]
        return frame

    def _original(self):
        if self.scenario == 'movie':
            from .sembench_movie import run_original_lotus_query
            ranking = dict(ranking=self.ranking) if self.query == '9' else {}
            return run_original_lotus_query(self.checkout, int(self.query),
                lambda: self._load('Reviews.csv'), **ranking)
        if self.scenario == 'medical':
            from .sembench_medical import run_original_query
            return run_original_query(self.checkout, self.query, self._load,
                data_dir=self.data_dir, scale_factor=self.scale_factor)
        from .sembench_mmqa import run_original_query
        return run_original_query(self.checkout, self.query, self._load)

    def describe(self):
        operation = None if self.operation is None else self.operation.describe()
        return dict(schema='semloom.sembench_text_query.v1', sembench_commit=SEMBENCH_COMMIT,
            scenario=self.scenario, query='Q' + self.query, query_id=self.query_id,
            files=self.files, operation=operation,
            **(dict(ranking=self.ranking) if self.scenario == 'movie' and self.query == '9' else {}),
            **(dict(scale_factor=self.scale_factor, scale_unit='patient rows; model rows are query-specific')
               if self.scenario == 'medical' else {}),
            expected_model_calls=0 if operation is None else operation['expected_model_calls'],
            policy='original exact ordinary operator; result cache disabled; no warmup or retry',
            label_scope=('original Medical patient labels used only for evaluation'
                         if self.scenario == 'medical' else
                         'original evaluation only; Movie labels excluded from execution input'),
            original_selection_and_postprocessing=True)

    def execute(self, execute_operator):
        """Run the original query once around the selected native operator owner."""
        import pandas as pd

        # Preflight reads do not replace the query's real CSV scan. Gold-only
        # label relations stay outside execution, as in the original benchmark.
        for name, identity in self.files.items():
            if name in self.evaluation_files:
                continue
            path = self.data_dir / name
            expected_identity = dict(sha256=identity['sha256'], bytes=identity['bytes'])
            if file_identity(path) != expected_identity:
                raise ValueError('SemBench input changed after preflight')
            self.frames[name] = pd.read_csv(path)
            if file_identity(path) != expected_identity:
                raise ValueError('SemBench input changed during query scan')
        used = False
        operator_report = None

        def dispatch(kind, frame, args, kwargs, invoke):
            nonlocal used, operator_report
            if used or self.operation is None:
                raise ValueError('the query emitted an undeclared additional semantic operator')
            actual = _operation(kind, frame, args, kwargs, model_name=self.model_name,
                                generation=self.generation, query_id=self.query_id)
            if actual.describe() != self.operation.describe() or not frame.equals(self.operation.frame):
                raise ValueError('the original operator input or arguments changed after preflight')
            used = True
            operator_report = execute_operator(actual, invoke)
            if kind == 'topk':
                return operator_report['frame'], operator_report['native_stats']
            return operator_report['frame']

        try:
            with _operators(dispatch):
                result = self._original()
        except BaseException as error:
            if operator_report is not None:
                error.operator_report = operator_report
            raise
        if self.operation is not None and not used:
            raise ValueError('the original query omitted its declared semantic operator')
        return result, operator_report

    def evaluate(self, result):
        if self.scenario == 'movie':
            from .sembench_movie import evaluate_original
            return evaluate_original(self.checkout, int(self.query), result,
                self.frames['Reviews.csv'], labeled_movies=self.frames.get('Movies.csv'))
        if self.scenario == 'medical':
            from .sembench_medical import evaluate_original
            return evaluate_original(self.checkout, self.query, result,
                self.frames[self.labels_file], self.frames[self.symptoms_file],
                scale_factor=self.scale_factor)
        from .sembench_mmqa import evaluate_original
        return evaluate_original(self.checkout, self.query, result)
