"""Compose bounded Map organization with verified local tokenizer work estimates."""

from dataclasses import asdict, dataclass, replace
import hashlib
import json
from pathlib import Path
import time

from ...modalities.text.contracts import build_text_work_descriptor, text_work_calibration_signature
from ...modalities.text.tokenization import chat_token_count
from ...scheduling.organization.session_window import WorkWindowOrganizer
from .incremental_execution import build_fixed_model_execution, prepare_map_task
from .model_config import MAX_MODEL_RESPONSE_BYTES


@dataclass(frozen=True)
class MapOrganizationConfig:
    mode: str
    window_rows: int
    batch_rows: int
    batch_work: int
    active_work: int
    model_id: str
    model_revision: str
    serving_revision: str
    tokenizer_path: str
    tokenizer_sha256: str
    context_tokens: int

    def __post_init__(self):
        if self.mode not in ('rows', 'work', 'length'):
            raise ValueError('unsupported Map organization mode')
        for name in ('window_rows', 'batch_rows', 'batch_work', 'active_work', 'context_tokens'):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError('organization limits must be positive integers')
        if self.batch_rows > self.window_rows or self.window_rows > 256:
            raise ValueError('organization rows must fit a window of at most 256')
        if self.context_tokens > 1048576 or self.active_work < self.context_tokens:
            raise ValueError('active work must admit every supported complete request')
        if any(not isinstance(getattr(self, name), str) or not getattr(self, name)
               for name in ('model_id', 'model_revision', 'serving_revision', 'tokenizer_path')):
            raise ValueError('organization requires model and tokenizer identity')
        if (not isinstance(self.tokenizer_sha256, str) or len(self.tokenizer_sha256) != 64
                or any(c not in '0123456789abcdef' for c in self.tokenizer_sha256)):
            raise ValueError('organization requires a tokenizer SHA-256')

    @classmethod
    def load(cls, path):
        path = Path(path)
        if path.stat().st_size > 16384:
            raise ValueError('organization configuration is too large')
        return cls(**json.loads(path.read_text()))

    @property
    def calibration_signature(self):
        return text_work_calibration_signature(
            model_revision=self.model_revision, serving_revision=self.serving_revision,
            protocol='semloom-map-v6',
            cost_model_revision=f'prompt-plus-max-new-v1:{self.tokenizer_sha256}:{self.context_tokens}',
        )


def tokenizer_fingerprint(directory):
    """Hash local tokenizer inputs, excluding weights; independent of the directory name."""
    directory = Path(directory)
    names = ('config.json', 'tokenizer_config.json', 'tokenizer.json', 'vocab.json',
             'vocab.txt', 'merges.txt', 'special_tokens_map.json', 'added_tokens.json',
             'tokenizer.model', 'spiece.model', 'chat_template.jinja', 'chat_template.json')
    files = [directory / name for name in names if (directory / name).is_file()]
    templates = directory / 'chat_templates'
    if templates.is_dir():
        files.extend(p for p in templates.rglob('*') if p.is_file())
    if not files or not (directory / 'tokenizer_config.json').is_file():
        raise ValueError('missing local tokenizer configuration')
    digest = hashlib.sha256()
    for path in sorted(files):
        digest.update(str(path.relative_to(directory)).encode() + b'\0')
        with path.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1048576), b''):
                digest.update(chunk)
        digest.update(b'\0')
    return digest.hexdigest()


def organization_factory(config, *, tokenizer=None):
    """Validate tokenizer before accepting sockets; the optional tokenizer is a test seam."""
    if tokenizer_fingerprint(config.tokenizer_path) != config.tokenizer_sha256:
        raise ValueError('tokenizer files differ from the declared identity')
    if tokenizer is None:
        from transformers import AutoTokenizer
        tokenizer = AutoTokenizer.from_pretrained(
            config.tokenizer_path, local_files_only=True, trust_remote_code=False, use_fast=True,
        )
    if tokenizer_fingerprint(config.tokenizer_path) != config.tokenizer_sha256:
        raise ValueError('tokenizer changed while loading')
    signature = config.calibration_signature

    def build(model_config, *, observer=None, **kwargs):
        if model_config.model_id != config.model_id or kwargs.get('max_jobs', 1) != 1:
            raise ValueError('Map organization requires the declared model and a single Job')
        if kwargs.get('max_tasks', 1) < config.window_rows:
            raise ValueError('organization window exceeds held task capacity')
        organizer = WorkWindowOrganizer(
            config.batch_rows, None if config.mode == 'rows' else config.batch_work,
            shortest_first=config.mode == 'length', candidate_window_rows=config.window_rows,
        )

        def organize(candidates):
            selected = organizer(candidates)
            if observer:
                visible = candidates[:config.window_rows]
                observer(dict(event='map_organized', mode=config.mode, calibration_signature=signature,
                              candidates=[dict(key=asdict(c.key), work=c.task.estimated_work) for c in visible],
                              selected=[asdict(key) for key in selected]))
            return selected

        def prepare(request, sequence):
            if request.protocol_version != 6 or request.model_id != config.model_id:
                raise ValueError('token work supports only the declared Map v6 model')
            max_new = request.generation_constraints.get('max_tokens')
            if type(max_new) is not int or not 0 < max_new < config.context_tokens:
                raise ValueError('generation budget cannot fit model context')
            started = time.monotonic()
            prompt_tokens = chat_token_count(tokenizer, list(request.canonical_messages),
                                             max_tokens=config.context_tokens - max_new)
            if prompt_tokens + max_new > config.context_tokens:
                raise ValueError('complete request exceeds model context; no truncation is authorized')
            prompt_bytes = sum(len(m['content'].encode('utf-8')) for m in request.canonical_messages)
            work = build_text_work_descriptor(
                prompt_tokens=prompt_tokens, estimated_output_tokens=max_new, prompt_bytes=prompt_bytes,
                result_bytes_upper=MAX_MODEL_RESPONSE_BYTES, calibration_signature=signature,
            )
            task = prepare_map_task(request, sequence, describe_work=lambda _: work)
            if observer:
                observer(dict(event='map_work_described', sequence=sequence,
                              semantic_payload_digest=request.semantic_payload_digest,
                              request_sha256=hashlib.sha256(task.payload).hexdigest(),
                              prompt_tokens=prompt_tokens, max_new_tokens=max_new,
                              estimated_work=work.primary.units, work_unit='tokens',
                              calibration_signature=signature, preparation_seconds=time.monotonic()-started))
            return task

        execution = build_fixed_model_execution(
            model_config, observer=observer, organize=organize, active_work=config.active_work,
            work_unit='tokens', **kwargs,
        )
        try:
            if observer:
                identity = asdict(config)
                identity.pop('tokenizer_path')
                observer(dict(event='map_organization_config', **identity, calibration_signature=signature,
                              capacity_observation_version=1))
        except BaseException:
            execution.close()
            raise
        return replace(execution, prepare_task=prepare)

    return build
