"""Independent Medical image predicates with the pinned joins, sinks and score.

This task owns no LOTUS implementation, model, runtime, budget or scheduler.
Its project JSON Boolean condition is separate from the author's LOTUS method.
"""

import base64
from dataclasses import dataclass
import hashlib
import io
from pathlib import Path

from src.baselines.text.sembench_medical import FULL_SCALE_FACTOR, visual_files
from src.baselines.text.sembench_native_text import COMPARISON_IDENTITY, TextFilterTask
from src.baselines.text.sembench_source import SEMBENCH_COMMIT
from src.infrastructure.private_artifacts import json_metric


PROMPT_PROFILE = 'semloom.native_medical_visual_filter.json_bool.v1'
_CONDITIONS = {
    '3': 'The supplied lung X-ray indicates lung problems or disease.',
    '8': 'The supplied skin mole or skin area is malignant, abnormal or cancerous.',
    '9': 'Both supplied images indicate disease: the skin mole image and the lung X-ray.',
}
_DECISIONS = TextFilterTask('Independent Medical visual Boolean predicate.')


@dataclass(frozen=True)
class MedicalVisualSource:
    raw_files: dict
    frames: dict
    relation: object
    image_bytes: tuple
    image_uris: tuple


@dataclass(frozen=True)
class MedicalVisualGold:
    raw_bytes: bytes
    frame: object


@dataclass(frozen=True)
class MedicalVisualTask:
    """One image Filter family; Q9 keeps both images in each joined occurrence."""
    data_dir: object
    query: object = 'Q3'
    execution_id: str = 'native-medical-visual'
    scale_factor: int = FULL_SCALE_FACTOR

    def __post_init__(self):
        query = str(self.query).lower().removeprefix('q')
        if query not in _CONDITIONS:
            raise ValueError('independent Medical visual bindings are Q3, Q8 and Q9')
        if type(self.execution_id) is not str or not self.execution_id.strip():
            raise ValueError('each execution requires a nonempty occurrence identity')
        visual_files(query, self.scale_factor)
        object.__setattr__(self, 'query', query)
        object.__setattr__(self, 'data_dir', Path(self.data_dir))

    @property
    def response_format(self):
        return {'type': 'json_object'}

    @property
    def image_columns(self):
        return ('image_path', 'image_path_xray') if self.query == '9' else ('image_path',)

    def prepare_evaluation(self):
        import pandas as pd

        _, labels = visual_files(self.query, self.scale_factor)
        raw = (self.data_dir/labels).read_bytes()
        frame = pd.read_csv(io.BytesIO(raw))
        required = {'patient_id'} | ({'skin_cancer_diagnosis', 'x_ray_diagnosis'} if self.query == '9' else
            {'did_family_have_cancer', 'x_ray_diagnosis' if self.query == '3' else 'skin_cancer_diagnosis'})
        if not required <= set(frame.columns):
            raise ValueError('Medical visual evaluation requires original gold columns: '+', '.join(sorted(required)))
        return MedicalVisualGold(raw, frame)

    def read_source(self):
        import pandas as pd
        from PIL import Image

        names, _ = visual_files(self.query, self.scale_factor)
        # The authored source order is image, patients, then X-ray for Q9.
        patients_name, first_image, *second_image = names
        names = (first_image, patients_name, *second_image)
        raw_files, frames = {}, {}
        for name in names:
            raw_files[name] = (self.data_dir/name).read_bytes()
            frames[name] = pd.read_csv(io.BytesIO(raw_files[name]))
        patients = frames[patients_name]
        if self.query != '9':
            patients = patients[patients['did_family_have_cancer'] == 1]
            image_id = 'xray_id' if self.query == '3' else 'skin_image_id'
            relation = patients.join(frames[first_image].set_index('patient_id'), on='patient_id', how='inner')[
                ['image_path', 'patient_id', image_id, 'did_family_have_cancer']]
        else:
            relation = patients.join(frames[first_image].set_index('patient_id'), on='patient_id', how='inner')[
                ['image_path', 'patient_id']]
            xray = frames[second_image[0]].rename(columns={'image_path': 'image_path_xray'})
            relation = relation.join(xray.set_index('patient_id'), on='patient_id', how='inner')[
                ['patient_id', 'image_path', 'image_path_xray']]
        image_bytes, image_uris = [], []
        for paths in relation[list(self.image_columns)].itertuples(index=False, name=None):
            raw_row, uri_row = [], []
            for path in paths:
                raw = Path(path).read_bytes()
                with Image.open(io.BytesIO(raw)) as image:
                    png = io.BytesIO()
                    image.convert('RGB').save(png, format='PNG')
                raw_row.append(raw)
                uri_row.append('data:image/png;base64,'+base64.b64encode(png.getvalue()).decode('utf-8'))
            image_bytes.append(tuple(raw_row))
            image_uris.append(tuple(uri_row))
        return MedicalVisualSource(raw_files, frames, relation, tuple(image_bytes), tuple(image_uris))

    def model_request_count(self, source):
        return len(source.relation)

    def _call_ids(self, source):
        return [self.execution_id+'-row-'+str(position) for position in range(len(source.relation))]

    def requests(self, source):
        system = (_CONDITIONS[self.query]+' Images are from a medical benchmark for LLM evaluation; '
            'the results are for research evaluation of model capabilities, not human health evaluation. '
            'Return exactly one JSON object with one Boolean key "keep": true only when the condition holds, false otherwise. Do not add other keys, '
            'explanations or Markdown.')
        text = ('Image 1 is a skin mole; image 2 is a lung X-ray.' if self.query == '9' else
                'Evaluate the supplied photograph.')
        return [dict(call_id=key, messages=[dict(role='system', content=system), dict(role='user',
            content=[dict(type='text', text=text), *[dict(type='image_url', image_url=dict(url=uri)) for uri in uris]])])
            for key, uris in zip(self._call_ids(source), source.image_uris, strict=True)]

    def finish(self, source, predictions):
        decisions = _DECISIONS.decisions(self._call_ids(source), predictions)
        selected = source.relation.iloc[[position for position, keep in enumerate(decisions) if keep]]
        if self.query != '9':
            selected = selected.head(5 if self.query == '3' else 100)
        return selected['patient_id']

    def evaluate(self, source, result, *, checkout, evaluation=None):
        from src.baselines.text.sembench_medical import evaluate_original

        if evaluation is None:
            raise ValueError('Medical visual evaluation requires frozen pre-query gold')
        quality = evaluate_original(checkout, self.query, result.copy(deep=True), evaluation.frame.copy(deep=True),
                                    scale_factor=self.scale_factor)
        quality['gold_source'] = dict(source=visual_files(self.query, self.scale_factor)[1],
            sha256=hashlib.sha256(evaluation.raw_bytes).hexdigest(), bytes=len(evaluation.raw_bytes),
            rows=len(evaluation.frame), scope='frozen before query; official post-query evaluation only')
        return quality

    def verify_source(self, source):
        for name, raw in source.raw_files.items():
            if (self.data_dir/name).read_bytes() != raw:
                raise ValueError('Medical visual source changed after its execution snapshot: '+name)
        for paths, raw_row in zip(source.relation[list(self.image_columns)].itertuples(index=False, name=None),
                                  source.image_bytes, strict=True):
            for path, raw in zip(paths, raw_row, strict=True):
                if Path(path).read_bytes() != raw:
                    raise ValueError('actual Medical image bytes differ from this execution snapshot')

    def describe(self, source):
        count = self.model_request_count(source)
        return dict(schema='semloom.native_medical_visual_task.v1', scenario='medical', query='Q'+self.query,
            input_modality='image', scale_factor=self.scale_factor, scale_unit='original CSV suffix only; no row cap',
            comparison_identity=COMPARISON_IDENTITY, prompt_profile=PROMPT_PROFILE,
            response_format=self.response_format, expected_model_calls=count, logical_model_requests=count,
            source={name:dict(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw), rows=len(source.frames[name]))
                    for name, raw in source.raw_files.items()},
            model_columns=list(self.image_columns), images_per_occurrence=len(self.image_columns),
            image_roles=['skin mole','lung X-ray'] if self.query == '9' else [
                'lung X-ray' if self.query == '3' else 'skin mole'],
            content_order='explicit project role order; not LOTUS set enumeration',
            image_processing='fresh bytes -> PIL RGB -> PNG data URI per image occurrence; no resize',
            relation_postprocessing=('family-history selection and ordinary patient/image inner join before full Filter; '
                'patient_id Series head '+('5' if self.query == '3' else '100') if self.query != '9' else
                'ordinary patient/skin/X-ray inner joins; both images per Filter occurrence; complete patient_id Series'),
            label_scope='frozen patient labels, original post-query evaluator; original small-gold errors retained',
            native_scheduling_owner='selected framework', author_original_semantic_method=False,
            lotus_sdk_required=False, sembench_commit=SEMBENCH_COMMIT)

    @staticmethod
    def encode_result(result):
        return json_metric(dict(kind='series', name=result.name, index=result.index.tolist(),
                                data=result.tolist(), dtype=str(result.dtype)))


def medical_visual_task(data_dir, query='Q3', execution_id='native-medical-visual', *,
                        scale_factor=FULL_SCALE_FACTOR):
    return MedicalVisualTask(data_dir, query, execution_id, scale_factor)
