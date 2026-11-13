"""Original SemBench queries and explicit prepared-operator input boundaries.

The upstream query still owns selection and relational postprocessing. An
execution adapter may bind one accessor or the complete original function. Image Join is explicitly
selected; it does not broaden the text record graphs' modality support.
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
    columns: tuple[str, ...]

    def describe(self):
        count = len(self.records)
        return dict(kind='topk', method='quick', K=count, input_rows=count,
            expected_model_calls=count * (count - 1) // 2,
            call_count_kind='upper_bound; one request per native binary comparison',
            input_columns=list(self.frame.columns), model_columns=list(self.columns),
            instruction=self.instruction, result_cache=False, cascade=False,
            pivot='original numpy.random.randint; actual RNG state recorded')


@dataclass(frozen=True)
class JoinOperation:
    """The author's exact Join; every ordered left/right pair is a request."""
    frame: object
    right_frame: object
    model_name: str
    generation: object
    instruction: str
    records: tuple[RecordOccurrence, ...]
    columns: tuple[str, ...]
    strategy: object = None

    def describe(self):
        left_count, right_count = len(self.frame), len(self.right_frame)
        return dict(kind='join', method='exact', input_rows=left_count, right_input_rows=right_count,
            expected_model_calls=left_count * right_count, call_count_kind='exact ordered Cartesian pairs',
            input_columns=list(self.frame.columns), right_input_columns=list(self.right_frame.columns),
            model_columns=list(self.columns),
            model_columns_scope='declared instruction references; native_join_inputs reports actual SDK projection',
            instruction=self.instruction, result_cache=False, cascade=False,
            **(dict(strategy=self.strategy.name) if self.strategy is not None else {}),
            source_pair_order='left-major; original positions, including same IDs and text')


def _operator_arguments(args, kwargs, parameters, optional=()):
    """Bind ordinary accessor arguments without changing their task values."""
    if len(args) > len(parameters):
        raise ValueError('unsupported positional semantic operator arguments')
    values = dict(zip(parameters, args))
    for name, value in kwargs.items():
        if name in values or name not in (*parameters, *optional):
            raise ValueError('duplicate or unsupported semantic operator argument: ' + name)
        values[name] = value
    if any(name not in values for name in parameters):
        raise ValueError('missing required semantic operator arguments')
    return values


def _operation(kind, frame, args, kwargs, *, model_name, generation, query_id):
    """Translate only the argument shapes used by the selected original queries."""
    import lotus
    from lotus.types import SerializationFormat

    if lotus.settings.serialization_format != SerializationFormat.DEFAULT:
        raise ValueError('this record bridge uses original LOTUS DEFAULT serialization')
    if kind == 'join':
        import pandas as pd
        from lotus.types import ReasoningStrategy
        values = _operator_arguments(args, kwargs, ('other', 'join_instruction'), ('strategy',))
        strategy = values.get('strategy')
        if strategy not in (None, ReasoningStrategy.ZS_COT):
            raise ValueError('this Join binding supports ordinary or native zero-shot CoT strategy')
        if not isinstance(values['other'], pd.DataFrame):
            raise ValueError('ordinary exact Join requires one right frame and its original instruction')
        right_frame = values['other']
        columns = lotus.nl_expression.parse_cols(values['join_instruction'])
        left = {column: column[:-5] for column in columns if column.endswith(':left')}
        right = {column: column[:-6] for column in columns if column.endswith(':right')}
        for column in columns:
            if column in left or column in right:
                continue
            if column in frame.columns and column not in right_frame.columns:
                left[column] = column
            elif column in right_frame.columns and column not in frame.columns:
                right[column] = column
        if (not columns or len(left) + len(right) != len(columns)
                or not left or not right
                or any(column not in frame.columns for column in left.values())
                or any(column not in right_frame.columns for column in right.values())):
            raise ValueError('ordinary exact Join requires unambiguous references to present left/right source columns')
        if generation.response_format is not None or generation.logprobs is not None:
            raise ValueError('ordinary Join uses the original Boolean answers without helper sampling')
        records = []
        right_rows = list(right_frame[list(right.values())].iterrows())
        for left_position, (left_index, left_row) in enumerate(frame[list(left.values())].iterrows()):
            for right_position, (right_index, right_row) in enumerate(right_rows):
                records.append(RecordOccurrence(query_id + f'-pair-{left_position}-{right_position}', len(records),
                    f'{left_index}:{right_index}',
                    {**{label: str(left_row[column]) for label, column in left.items()},
                     **{label: str(right_row[column]) for label, column in right.items()}}))
        return JoinOperation(frame.copy(deep=True), right_frame.copy(deep=True), model_name, generation,
                             values['join_instruction'], tuple(records), tuple(left) + tuple(right), strategy)
    if kind == 'topk':
        if len(args) != 1 or kwargs != dict(K=len(frame), method='quick', return_stats=True):
            raise ValueError('ordinary quick TopK with K=N and original statistics is required')
        columns = tuple(lotus.nl_expression.parse_cols(args[0]))
        if not columns or any(column not in frame.columns for column in columns):
            raise ValueError('TopK instruction must reference present source columns')
        if (generation.response_format is not None or generation.logprobs is not None
                or generation.repetition_penalty != 1):
            raise ValueError('ordinary TopK uses plain binary answers without helper sampling')
        records = tuple(RecordOccurrence(query_id + '-row-' + str(i), i,
            int(label) if isinstance(label, Integral) else label,
            {column: str(row[column]) for column in columns})
            for i, (label, row) in enumerate(frame[list(columns)].iterrows()))
        return TopKOperation(frame.copy(deep=True), model_name, generation, args[0], records, columns)
    if kind in ('filter', 'map'):
        values = _operator_arguments(args, kwargs, ('user_instruction',),
                                     ('suffix',) if kind == 'map' else ())
        method = (FilterMethod(model_name, values['user_instruction']) if kind == 'filter' else
                  MapMethod(model_name, values['user_instruction'], output_column=values.get('suffix', '_map')))
    else:
        values = _operator_arguments(args, kwargs, ('input_cols', 'output_cols'),
                                     ('extract_quotes', 'return_raw_outputs'))
        if (values.get('extract_quotes', False) is not False
                or values.get('return_raw_outputs', False) is not False):
            raise ValueError('the selected original Extract uses ordinary JSON fields without quotes/raw outputs')
        method = ExtractMethod(model_name, tuple(values['input_cols']), values['output_cols'])
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


def capture_first_operator(execute_query):
    """Inspect the original source frontier without inventing an operator result."""
    selected = []

    def capture(kind, frame, args, kwargs, _invoke):
        selected.append((kind, frame, args, kwargs))
        raise _Captured()

    with _operators(capture):
        try:
            execute_query()
        except _Captured:
            pass
    frontier, = selected
    return frontier


class SemBenchTextQuery:
    """A finite frozen input snapshot and the unchanged original query/evaluator."""

    def __init__(self, checkout, data_dir, scenario, query_id, *, execution_id,
                 model_name, generation, scale_factor=None, ranking=None, image_join=False,
                 lm_options=None):
        from .sembench_medical import FULL_SCALE_FACTOR, TEXT_QUERY_IDS as MEDICAL_QUERY_IDS, text_files
        from .sembench_mmqa import TEXT_QUERY_IDS as MMQA_QUERY_IDS
        from .sembench_ecomm import TEXT_QUERY_IDS as ECOMM_QUERY_IDS, TEXT_FILE
        from .sembench_cars import FULL_SCALE_FACTOR as CARS_SCALE, TEXT_QUERY_IDS as CARS_QUERY_IDS, text_files as cars_files
        from src.baselines.image.sembench_mmqa import IMAGE_QUERY_IDS

        query = str(query_id).lower().removeprefix('q')
        if image_join and not (scenario == 'mmqa' and query in IMAGE_QUERY_IDS):
            raise ValueError('the original image Join binding supports MMQA Q2a/Q2b/Q7')
        if not ((scenario == 'movie' and query in MOVIE_QUERY_IDS)
                or (scenario == 'mmqa' and query in (*MMQA_QUERY_IDS, '5')) or image_join
                or (scenario == 'medical' and query in MEDICAL_QUERY_IDS)
                or (scenario == 'ecomm' and query in ECOMM_QUERY_IDS)
                or (scenario == 'cars' and query in CARS_QUERY_IDS)):
            raise ValueError('this entry supports Movie Q1-Q10 (Q5-Q7 native Join, Q9 Map or TopK), ordinary MMQA text queries and native Q5 aggregation '
                             'plus Medical/Cars Q1/Q4/Q10 and E-Commerce Q1/Q3/Q5/Q7')
        if scenario not in ('medical', 'cars') and scale_factor is not None:
            raise ValueError('scale_factor applies only to the Medical/Cars authored CSV filenames')
        if ranking is not None and (scenario != 'movie' or query != '9' or ranking not in ('map', 'topk')):
            raise ValueError('ranking=map/topk applies only to Movie Q9')
        if type(execution_id) is not str or not execution_id.strip():
            raise ValueError('each execution needs a nonempty independent query_id')
        self.checkout = verify_checkout(checkout)
        self.scenario, self.query, self.query_id = scenario, query, execution_id
        self.ranking = ranking or 'map'
        self.model_name, self.generation = model_name, generation
        self.data_dir = Path(data_dir).resolve()
        self.visual_task = None
        self.aggregate_task = None
        self.lm_options = dict(lm_options or {})
        self.evaluation_files = set()
        if scenario == 'mmqa' and query == '5':
            from lotus.models import LM
            from . import sembench_mmqa_aggregate as aggregate
            from src.baselines.sembench_lotus import OriginalLotusQuery

            if self.lm_options.get('max_ctx_len') is None or self.lm_options.get('tokenizer') is None:
                raise ValueError('native aggregation requires the declared model context and local token counter')
            source = aggregate.input_identity(self.data_dir)
            lm = LM(model_name, **self.lm_options, **generation.kwargs())
            plan = aggregate.leaf_plan(self.checkout, self.data_dir, lm)
            if aggregate.input_identity(self.data_dir) != source:
                raise ValueError('MMQA input changed while preparing the native aggregation')
            self.aggregate_inputs = dict(source=source, native_plan=plan, work=aggregate.describe_work(plan),
                native_lm_max_ctx_len=lm.max_ctx_len, native_input_token_budget=lm.max_ctx_len-lm.max_tokens)
            self.aggregate_task = OriginalLotusQuery(self.checkout, scenario, query,
                inputs=self.aggregate_inputs, data_dir=self.data_dir)
            self.frames, self.files = {}, {aggregate.INPUT_FILE: source}
            self.operation = None
            self._run_original = self.aggregate_task.execute
            return
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
        elif scenario == 'cars':
            self.scale_factor = CARS_SCALE if scale_factor is None else scale_factor
            cars, complaints, full_cars, full_complaints = cars_files(self.scale_factor)
            names = [complaints, full_complaints]
            self.evaluation_files.add(full_complaints)
            if query != '1':
                names = [cars, complaints, full_cars, full_complaints]
                self.evaluation_files.add(full_cars)
        elif scenario == 'ecomm':
            names = [TEXT_FILE]
        elif image_join:
            from src.baselines.image.sembench_mmqa import describe_inputs
            from src.baselines.sembench_lotus import OriginalLotusQuery
            inputs = describe_inputs(self.checkout, self.data_dir, query)
            self.visual_task = OriginalLotusQuery(self.checkout, scenario, query,
                inputs=inputs, data_dir=self.data_dir)
            names = [Path(self.visual_inputs['table']['path']).name]
        elif query == '1':
            names = ['ben_piazza.csv', 'ben_piazza_text_data.csv']
        elif query.startswith('3') or query == '4':
            names = ['lizzy_caplan_text_data.csv']
        else:
            names = ['tampa_international_airport.csv']
        self.frames, self.files = {}, {}
        for name in names:
            if self.visual_task is not None:
                table = self.visual_inputs['table']
                self.files[name] = {key: table[key] for key in ('sha256', 'bytes', 'rows', 'columns')}
                continue
            path = self.data_dir / name
            before = file_identity(path)
            self.frames[name] = self._read_file(path)
            if file_identity(path) != before:
                raise ValueError('SemBench input changed while its snapshot was read')
            self.files[name] = dict(before, rows=len(self.frames[name]),
                                    columns=list(self.frames[name].columns))
        self._run_original = self._bind_original()
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

    @staticmethod
    def _read_file(path):
        import pandas as pd

        return pd.read_parquet(path) if path.suffix == '.parquet' else pd.read_csv(path)

    @property
    def visual_inputs(self):
        return None if self.visual_task is None else self.visual_task.inputs

    def _load(self, name, **kwargs):
        if name not in self.frames or kwargs not in ({}, {'sep': ',', 'quotechar': '"'}):
            raise ValueError('unexpected input requested by the selected original query')
        return self.frames[name].copy(deep=True)

    def _bind_original(self):
        if self.scenario == 'movie':
            from .sembench_movie import bind_original_lotus_query
            ranking = dict(ranking=self.ranking) if self.query == '9' else {}
            return bind_original_lotus_query(self.checkout, int(self.query),
                lambda: self._load('Reviews.csv'), **ranking)
        if self.scenario == 'medical':
            from .sembench_medical import bind_original_query
            return bind_original_query(self.checkout, self.query, self._load,
                data_dir=self.data_dir, scale_factor=self.scale_factor)
        if self.scenario == 'cars':
            from .sembench_cars import bind_original_query
            return bind_original_query(self.checkout, self.query, self._load, data_dir=self.data_dir,
                                       scale_factor=self.scale_factor)
        if self.scenario == 'ecomm':
            from .sembench_ecomm import bind_original_query
            return bind_original_query(self.checkout, self.query, self._load, data_dir=self.data_dir)
        if self.visual_task is not None:
            return self.visual_task.execute
        from .sembench_mmqa import bind_original_query
        return bind_original_query(self.checkout, self.query, self._load)

    def _original(self):
        return self._run_original()

    def describe(self):
        if self.aggregate_task is not None:
            return dict(schema='semloom.sembench_text_query.v1', sembench_commit=SEMBENCH_COMMIT,
                scenario=self.scenario, query='Q'+self.query, query_id=self.query_id,
                files=self.files, operation=None, algorithm='native_hierarchical_aggregate',
                **self.aggregate_inputs['work'], aggregate_inputs=self.aggregate_inputs,
                policy='original LOTUS token counting, long-context strategy, hierarchy and parser; '
                       'result cache disabled; no warmup or retry',
                original_selection_and_postprocessing=True)
        operation = None if self.operation is None else self.operation.describe()
        return dict(schema='semloom.sembench_text_query.v1', sembench_commit=SEMBENCH_COMMIT,
            scenario=self.scenario, query='Q' + self.query, query_id=self.query_id,
            files=self.files, operation=operation,
            **(dict(visual_inputs=self.visual_inputs) if self.visual_inputs is not None else {}),
            **(dict(ranking=self.ranking) if self.scenario == 'movie' and self.query == '9' else {}),
            **(dict(scale_factor=self.scale_factor, scale_unit='patient rows; model rows are query-specific')
               if self.scenario == 'medical' else {}),
            **(dict(scale_factor=self.scale_factor, scale_unit='authored Cars sample; model rows follow original complaint joins')
               if self.scenario == 'cars' else {}),
            expected_model_calls=(None if self.visual_inputs is not None and self.query == '2b'
                                  else 0 if operation is None else operation['expected_model_calls']),
            **(dict(maximum_model_calls=self.visual_inputs['maximum_model_calls'])
               if self.visual_inputs is not None else {}),
            policy='explicit exact ordinary-operator profile; not the official runner default; '
                   'result cache disabled; no warmup or retry',
            label_scope=('original Medical patient labels used only for evaluation'
                         if self.scenario == 'medical' else
                         'original full Cars labels used only for evaluation'
                         if self.scenario == 'cars' else
                         'original Parquet retained; only authored text columns enter model messages'
                         if self.scenario == 'ecomm' else
                         'original CSV relation retained; only operator-declared fields enter model messages'),
            original_selection_and_postprocessing=True)

    def execute(self, execute_operator):
        """Run the original query once around the selected native operator owner."""
        result, operator = self.bind_execution()(execute_operator)
        try:
            self.verify_source()
        except BaseException as error:
            if operator is not None:
                error.operator_report = operator
            raise
        return result, operator

    def verify_source(self):
        """Audit execution-file identities; gold-only snapshots are not query input."""
        if self.aggregate_task is not None:
            self.aggregate_task.verify_inputs()
            return
        if self.visual_task is not None:
            self.visual_task.verify_inputs()
            return
        for name, identity in self.files.items():
            if name in self.evaluation_files:
                continue
            path = self.data_dir / name
            expected_identity = dict(sha256=identity['sha256'], bytes=identity['bytes'])
            if file_identity(path) != expected_identity:
                raise ValueError('SemBench input changed after preflight')

    def bind_execution(self, *, multiple=False):
        """Check source before timing and bind a real scan of the original query.

        The caller records its full sink, then calls verify_source outside that
        interval. execute() retains the combined checked entry for other callers.

        multiple=True calls execute_operator(operation, invoke) for each actual
        operator in original query order and returns (result, reports). Each
        callback returns its native report containing frame, plus native_stats
        for TopK. Later inputs come from the original query's intermediate
        relations. The default keeps the existing single-operator return shape.
        """
        if self.aggregate_task is not None:
            raise ValueError('native aggregation needs the complete original query LM boundary')
        self.verify_source()
        if multiple:
            return lambda execute_operator: self._execute_bound(execute_operator, multiple=True)
        return self._execute_bound

    def bind_original_execution(self):
        """Bind the full original function, including its actual later stages.

        The query owns intermediate relations and further semantic calls. A
        backend supplies its LM prediction seam for the whole query; unlike the
        prepared single-operator entry this boundary never invents a frontier
        result or limits the number of operators.
        """
        self.verify_source()
        return self._execute_original_bound

    def _read_execution_files(self):
        if self.visual_task is not None or self.aggregate_task is not None:
            # Its original source reader runs inside the actual original query.
            return
        for name in self.files:
            if name in self.evaluation_files:
                continue
            path = self.data_dir / name
            self.frames[name] = self._read_file(path)

    def _execute_original_bound(self):
        self._read_execution_files()
        return self._original()

    def _execute_bound(self, execute_operator, *, multiple=False):
        self._read_execution_files()
        used = False
        operator_report = None
        reports = []

        def dispatch(kind, frame, args, kwargs, invoke):
            nonlocal used, operator_report
            if (used and not multiple) or self.operation is None:
                raise ValueError('the query emitted an undeclared additional semantic operator')
            actual = _operation(kind, frame, args, kwargs, model_name=self.model_name,
                                generation=self.generation,
                                query_id=(self.query_id if not used else self.query_id + '-operator-' + str(len(reports))))
            if not used and (actual.describe() != self.operation.describe() or not frame.equals(self.operation.frame)
                    or kind == 'join' and not actual.right_frame.equals(self.operation.right_frame)):
                raise ValueError('the original operator input or arguments changed after preflight')
            used = True
            operator_report = execute_operator(actual, invoke)
            reports.append(operator_report)
            if kind == 'topk':
                return operator_report['frame'], operator_report['native_stats']
            return operator_report['frame']

        try:
            with _operators(dispatch):
                result = self._original()
        except BaseException as error:
            error.query_operator_reports = reports
            if operator_report is not None and not hasattr(error, 'operator_report'):
                error.operator_report = operator_report
            raise
        if self.operation is not None and not used:
            raise ValueError('the original query omitted its declared semantic operator')
        return result, reports if multiple else operator_report

    def evaluate(self, result):
        if self.aggregate_task is not None:
            return self.aggregate_task.evaluate(result)
        if self.scenario == 'movie':
            from .sembench_movie import evaluate_original
            return evaluate_original(self.checkout, int(self.query), result,
                self.frames['Reviews.csv'], labeled_movies=self.frames.get('Movies.csv'))
        if self.scenario == 'medical':
            from .sembench_medical import evaluate_original
            return evaluate_original(self.checkout, self.query, result,
                self.frames[self.labels_file], self.frames[self.symptoms_file],
                scale_factor=self.scale_factor)
        if self.scenario == 'cars':
            from .sembench_cars import evaluate_original
            return evaluate_original(self.checkout, self.query, result, self.frames, scale_factor=self.scale_factor)
        if self.scenario == 'ecomm':
            from .sembench_ecomm import evaluate_original, TEXT_FILE
            return evaluate_original(self.checkout, self.query, result, data_dir=self.data_dir,
                                     source_identity=self.files[TEXT_FILE])
        if self.visual_task is not None:
            return self.visual_task.evaluate(result)
        from .sembench_mmqa import evaluate_original
        return evaluate_original(self.checkout, self.query, result)
