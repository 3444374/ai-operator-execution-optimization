"""Independent image Filter semantics over the pinned Animals source and score.

This task owns no SDK, model, scheduler or clock. Native runners supply strict
JSON Boolean replies; the original relation steps and official score are kept.
"""

import base64
from dataclasses import dataclass, field
import hashlib
import io
import json
from pathlib import Path

from src.baselines.text.sembench_native_text import TextFilterTask, COMPARISON_IDENTITY
from src.infrastructure.private_artifacts import json_metric
from .sembench_animals import (
    AnimalsDataset, SOURCE_FILES, SEMBENCH_COMMIT, evaluate_original, verify_animals_checkout,
)


PROMPT_PROFILE = 'semloom.native_animals_filter.json_bool.v1'
_DECISIONS = TextFilterTask('The supplied photograph contains a zebra.')
_EXECUTION_COLUMNS = ('ImagePath', 'City', 'StationID', 'row_id')


def _system(animal):
    subject = 'a zebra' if animal == 'zebra' else 'an impala'
    return (f'Decide whether the supplied photograph contains {subject}. Return exactly '
        f'one JSON object with one Boolean key: {{"keep": true}} if {subject} is present, '
        'or {"keep": false} otherwise. Do not add other keys, explanations or Markdown.')


@dataclass(frozen=True)
class AnimalsFilterSource:
    manifest_bytes: bytes
    frame: object
    image_bytes: tuple[bytes, ...]
    image_uris: tuple[str, ...]


@dataclass(frozen=True)
class AnimalsEvaluation:
    manifest_bytes: bytes
    manifest: dict
    frame: object
    source_sha256: dict


@dataclass
class AnimalsFilterTask:
    """Original Animals image conditions followed by the complete relation sink.

    Prepare labels and extra identity checks before timing. Each read_source
    freshly reads the manifest and actual image bytes, converts RGB and encodes
    PNG inside the query. Evaluation and source verification follow full sink.
    """
    checkout: object
    manifest: object
    manifest_digest: str
    query: object = 'Q1'
    execution_id: str = 'native-animals-filter'
    _evaluation: AnimalsEvaluation | None = field(default=None, init=False, repr=False)

    def __post_init__(self):
        query = str(self.query).lower().removeprefix('q')
        if query not in ('1', '3', '7', '10'):
            raise ValueError('the independent single-image Animals Filter supports Q1/Q3/Q7/Q10')
        if type(self.execution_id) is not str or not self.execution_id.strip():
            raise ValueError('each execution requires a nonempty occurrence identity')
        self.query, self.checkout, self.manifest = query, Path(self.checkout), Path(self.manifest)

    @property
    def response_format(self):
        return {'type': 'json_object'}

    def _dataset(self):
        return AnimalsDataset(self.checkout, self.manifest, self.manifest_digest)

    def _filters(self):
        return ('zebra', 'impala') if self.query == '7' else ('zebra',)

    def prepare_evaluation(self):
        import pandas as pd

        raw = self.manifest.read_bytes()
        value = self._dataset().read()
        if json.loads(raw) != value:
            raise ValueError('Animals manifest changed while preparing its frozen reference')
        sources = verify_animals_checkout(self.checkout, int(self.query))
        if (value['role'] == 'official_animals_sf64'
                and value['provenance'].get('generator_sha256') != sources[SOURCE_FILES[2]]):
            raise ValueError('Animals generation source is not the pinned upstream function')
        frame = pd.DataFrame([{k: row[k] for k in ('ImagePath', 'City', 'StationID', 'Species')}
                              for row in value['rows']])
        self._evaluation = AnimalsEvaluation(raw, value, frame, sources)
        return self._evaluation

    def read_source(self):
        import pandas as pd
        from PIL import Image

        if self._evaluation is None:
            raise ValueError('prepare_evaluation must freeze Animals inputs before query execution')
        raw = self.manifest.read_bytes()
        if raw != self._evaluation.manifest_bytes:
            raise ValueError('Animals manifest changed after preparation')
        rows = json.loads(raw)['rows']
        image_bytes, image_uris = [], []
        for _ in self._filters():
            # The original Q7 replaces ImageArray before its second full Filter,
            # so each condition retains its own real image read and encoding.
            for row in rows:
                encoded = Path(row['ImagePath']).read_bytes()
                with Image.open(io.BytesIO(encoded)) as image:
                    rgb = image.convert('RGB')
                    png = io.BytesIO()
                    rgb.save(png, format='PNG')
                image_bytes.append(encoded)
                image_uris.append('data:image/png;base64,' + base64.b64encode(png.getvalue()).decode('utf-8'))
        frame = pd.DataFrame([{k: row[k] for k in _EXECUTION_COLUMNS} for row in rows])
        return AnimalsFilterSource(raw, frame, tuple(image_bytes), tuple(image_uris))

    def model_request_count(self, source):
        return len(source.frame) * len(self._filters())

    def _call_ids(self, source):
        if self.query == '7':
            return [self.execution_id + '-' + animal + '-row-' + str(i)
                    for animal in self._filters() for i in range(len(source.frame))]
        return [self.execution_id + '-row-' + str(i) for i in range(len(source.frame))]

    def requests(self, source):
        return [dict(call_id=key, messages=[dict(role='system', content=_system(
                self._filters()[i // len(source.frame)])),
            dict(role='user', content=[dict(type='text', text='Evaluate the supplied photograph.'),
                dict(type='image_url', image_url=dict(url=uri))])])
            for i, (key, uri) in enumerate(zip(self._call_ids(source), source.image_uris, strict=True))]

    def finish(self, source, predictions):
        import pandas as pd

        decisions = _DECISIONS.decisions(self._call_ids(source), predictions)
        if self.query == '7':
            count = len(source.frame)
            cities = []
            for offset in (0, count):
                selected = source.frame.iloc[[i for i, keep in enumerate(decisions[offset:offset+count]) if keep]]
                cities.append(set(selected['City']))
            return pd.DataFrame([dict(city=city) for city in sorted(cities[0] & cities[1])], columns=['city'])
        selected = source.frame.iloc[[i for i, keep in enumerate(decisions) if keep]]
        if self.query == '1':
            return pd.DataFrame([{'count(*)': len(selected)}])
        columns = ['city'] if self.query == '3' else ['city', 'stationID']
        if len(selected) == 0:
            return pd.DataFrame(columns=columns)
        groups = ['City'] if self.query == '3' else ['City', 'StationID']
        counts = selected.groupby(groups).size().reset_index(name='count')
        first = counts.sort_values('count', ascending=False).iloc[0]
        return pd.DataFrame([{output: first[key] for output, key in zip(columns, groups, strict=True)}])

    def evaluate(self, source, frame, *, checkout=None, evaluation=None):
        evaluation = self._evaluation if evaluation is None else evaluation
        if evaluation is None:
            raise ValueError('Animals evaluation requires the frozen pre-query reference')
        return evaluate_original(self.checkout if checkout is None else checkout,
            frame, evaluation.frame, int(self.query))

    def verify_source(self, source):
        if self.manifest.read_bytes() != source.manifest_bytes:
            raise ValueError('Animals manifest changed after its execution snapshot')
        current = self._dataset().read()
        for raw, row in zip(source.image_bytes, current['rows'] * len(self._filters()), strict=True):
            if hashlib.sha256(raw).hexdigest() != row['image']['sha256']:
                raise ValueError('actual Animals image bytes differed from the frozen input')

    def describe(self, source):
        count = self.model_request_count(source)
        return dict(schema='semloom.native_animals_filter_task.v1', query='Q' + self.query,
            comparison_identity=COMPARISON_IDENTITY, prompt_profile=PROMPT_PROFILE,
            response_format=self.response_format, answer_parser='strict unique JSON Boolean keep',
            source=dict(manifest_digest=self.manifest_digest, role=self._evaluation.manifest['role'],
                manifest_sha256=hashlib.sha256(source.manifest_bytes).hexdigest(), rows=len(source.frame)),
            expected_model_calls=count, logical_model_requests=count, model_columns=['Image'],
            input_modality='image',
            filter_conditions=list(self._filters()),
            execution_shape='independent image/condition requests with native batching; not LOTUS sequential LM batches',
            label_scope='Species frozen before query; official post-query evaluation only',
            image_processing='fresh encoded bytes -> PIL RGB -> PNG data URI per Filter pass; no resize',
            preparation_scope='extra original-byte/RGB identity checks before query; actual read/decode/encode inside',
            native_scheduling_owner='selected framework', author_original_semantic_method=False,
            lotus_sdk_required=False, sembench_commit=SEMBENCH_COMMIT,
            original_source_sha256=self._evaluation.source_sha256)

    @staticmethod
    def encode_result(frame):
        return json_metric(frame.to_dict(orient='split'))
