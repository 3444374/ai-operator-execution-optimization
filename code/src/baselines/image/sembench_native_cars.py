"""Independent Cars image predicates with the authored joins, LIMITs and score.

Project JSON semantics do not import or reproduce LOTUS prompts/parsers. The
selected native framework owns execution; this task retains source and sink.
"""

import base64
from dataclasses import dataclass
import hashlib
import io
from pathlib import Path

from src.baselines.text.sembench_cars import FULL_SCALE_FACTOR, visual_files
from src.baselines.text.sembench_native_text import COMPARISON_IDENTITY, TextFilterTask
from src.baselines.text.sembench_source import SEMBENCH_COMMIT
from src.infrastructure.private_artifacts import json_metric


PROMPT_PROFILE = 'semloom.native_cars_visual_filter.json_bool.v1'
_CONDITIONS = {
    '3': 'The vehicle or vehicle parts in the supplied image are not damaged.',
    '8': 'The vehicle in the supplied image has both a puncture and paint scratches.',
}
_DECISIONS = TextFilterTask('Independent Cars visual Boolean predicate.')


@dataclass(frozen=True)
class CarsVisualSource:
    raw_files: dict
    frames: dict
    relation: object
    image_bytes: tuple
    image_uris: tuple


@dataclass(frozen=True)
class CarsVisualGold:
    raw_files: dict
    frames: dict


@dataclass(frozen=True)
class CarsVisualTask:
    """Q3/Q8: one native image predicate after the original ordinary join."""
    data_dir: object
    query: object = 'Q3'
    execution_id: str = 'native-cars-visual'
    scale_factor: int = FULL_SCALE_FACTOR

    def __post_init__(self):
        query = str(self.query).lower().removeprefix('q')
        if query not in _CONDITIONS:
            raise ValueError('independent Cars visual bindings are Q3 and Q8')
        if type(self.execution_id) is not str or not self.execution_id.strip():
            raise ValueError('each execution requires a nonempty occurrence identity')
        visual_files(self.scale_factor)
        object.__setattr__(self, 'query', query)
        object.__setattr__(self, 'data_dir', Path(self.data_dir))

    @property
    def response_format(self):
        return {'type': 'json_object'}

    def prepare_evaluation(self):
        import pandas as pd

        _, _, cars, images = visual_files(self.scale_factor)
        raw_files, frames = {}, {}
        for name in (cars, images):
            raw_files[name] = (self.data_dir/name).read_bytes()
            frames[name] = pd.read_csv(io.BytesIO(raw_files[name]))
            required = ({'car_id','transmission','vin'} if self.query == '3' else {'car_id'}) if name == cars else (
                {'car_id','damage_status'} | ({'image_id'} if self.scale_factor != FULL_SCALE_FACTOR else set()))
            if not required <= set(frames[name].columns):
                raise ValueError('Cars visual evaluation requires original gold columns: '+', '.join(sorted(required)))
        return CarsVisualGold(raw_files, frames)

    def read_source(self):
        import pandas as pd
        from PIL import Image

        cars, images, _, _ = visual_files(self.scale_factor)
        raw_files, frames = {}, {}
        for name in (cars, images):
            raw_files[name] = (self.data_dir/name).read_bytes()
            frames[name] = pd.read_csv(io.BytesIO(raw_files[name]))
        # Sample image IDs are also the original evaluator's membership input.
        if self.scale_factor != FULL_SCALE_FACTOR and 'image_id' not in frames[images].columns:
            raise ValueError('Cars sample images require original image_id membership')
        relation = frames[cars].merge(frames[images], on='car_id', how='inner')
        if self.query == '3':
            relation = relation[relation['transmission'] == 'Manual']
        image_bytes, image_uris = [], []
        for path in relation['image_path']:
            raw = Path(path).read_bytes()
            with Image.open(io.BytesIO(raw)) as image:
                png = io.BytesIO()
                image.convert('RGB').save(png, format='PNG')
            image_bytes.append(raw)
            image_uris.append('data:image/png;base64,'+base64.b64encode(png.getvalue()).decode('utf-8'))
        return CarsVisualSource(raw_files, frames, relation, tuple(image_bytes), tuple(image_uris))

    def model_request_count(self, source):
        return len(source.relation)

    def _call_ids(self, source):
        return [self.execution_id+'-row-'+str(position) for position in range(len(source.relation))]

    def requests(self, source):
        system = (_CONDITIONS[self.query]+' Return exactly one JSON object with one Boolean key "keep": '
            'true only when the condition holds, false otherwise. Do not add other keys, explanations or Markdown.')
        return [dict(call_id=key, messages=[dict(role='system',content=system), dict(role='user',content=[
            dict(type='text',text='Evaluate the supplied photograph.'),
            dict(type='image_url',image_url=dict(url=uri))])])
            for key,uri in zip(self._call_ids(source),source.image_uris,strict=True)]

    def finish(self, source, predictions):
        decisions = _DECISIONS.decisions(self._call_ids(source), predictions)
        selected = source.relation.iloc[[position for position,keep in enumerate(decisions) if keep]]
        return selected[['vin']].head(10) if self.query == '3' else selected['car_id'].head(100)

    def evaluate(self, source, result, *, checkout, evaluation=None):
        from src.baselines.text.sembench_cars import evaluate_original

        if evaluation is None:
            raise ValueError('Cars visual evaluation requires frozen pre-query full gold')
        frames = {name:frame.copy(deep=True) for name,frame in (evaluation.frames | source.frames).items()}
        quality = evaluate_original(checkout,self.query,result.copy(deep=True),frames,scale_factor=self.scale_factor)
        quality['gold_source'] = {name:dict(sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw),
            rows=len(evaluation.frames[name])) for name,raw in evaluation.raw_files.items()}
        return quality

    def verify_source(self, source):
        for name,raw in source.raw_files.items():
            if (self.data_dir/name).read_bytes() != raw:
                raise ValueError('Cars visual source changed after its execution snapshot: '+name)
        for path,raw in zip(source.relation['image_path'],source.image_bytes,strict=True):
            if Path(path).read_bytes() != raw:
                raise ValueError('actual Cars image bytes differ from this execution snapshot')

    def describe(self, source):
        count = self.model_request_count(source)
        return dict(schema='semloom.native_cars_visual_task.v1',scenario='cars',query='Q'+self.query,
            input_modality='image',scale_factor=self.scale_factor,scale_unit='original CSV paths only; no row cap',
            comparison_identity=COMPARISON_IDENTITY,prompt_profile=PROMPT_PROFILE,response_format=self.response_format,
            expected_model_calls=count,logical_model_requests=count,images_per_occurrence=1,model_columns=['image_path'],
            source={name:dict(sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw),rows=len(source.frames[name]))
                    for name,raw in source.raw_files.items()},
            image_processing='fresh bytes -> PIL RGB -> PNG data URI per image occurrence; no resize',
            relation_postprocessing=('ordinary cars/images inner merge, Manual selection, full Filter, vin table head10'
                if self.query == '3' else 'ordinary cars/images inner merge, full Filter, car_id Series head100'),
            label_scope=('frozen full labels at authored full scale' if self.scale_factor == FULL_SCALE_FACTOR else
                'frozen full labels; original car_id/image_id sample membership from this execution snapshot'),
            native_scheduling_owner='selected framework',author_original_semantic_method=False,
            lotus_sdk_required=False,sembench_commit=SEMBENCH_COMMIT)

    @staticmethod
    def encode_result(result):
        import pandas as pd

        if isinstance(result,pd.Series):
            return json_metric(dict(kind='series',name=result.name,index=result.index.tolist(),
                                    data=result.tolist(),dtype=str(result.dtype)))
        return json_metric(result.to_dict(orient='split'))


def cars_visual_task(data_dir, query='Q3', execution_id='native-cars-visual', *, scale_factor=FULL_SCALE_FACTOR):
    return CarsVisualTask(data_dir,query,execution_id,scale_factor)
