"""Finite CPU-only media preparation comparisons using the original LOTUS producer.

No inference, semantic operator, answer cache, server or GPU is invoked. Event
collection, preparation and policy costs remain inside each measured arm.
"""
import hashlib
import os
from pathlib import Path
import time
import uuid

from src.infrastructure.private_artifacts import new_private_directory, write_private_json
from src.infrastructure.redact import redact_text


def run_study(config, output):
    required = {'schema', 'images', 'passes', 'capacity_bytes', 'repeats', 'input_kind'}
    if (type(config) is not dict or set(config) != required
            or config['schema'] != 'semloom.media_retention_study.v1'
            or config['input_kind'] not in ('fixture', 'real_input')):
        raise ValueError('an explicit CPU media preparation study is required')
    if (type(config['images']) is not list or not config['images']
            or any(type(config[key]) is not int or config[key] <= 0
                   for key in ('passes', 'repeats', 'capacity_bytes'))):
        raise ValueError('images and positive consumer, repeat and capacity settings are required')
    from src.baselines.image.lotus_media_observation import LotusMediaObservation

    output = Path(output)
    new_private_directory(output)
    write_private_json(output / 'config.json', config)
    rows = []
    for position, source in enumerate(config['images']):
        path = Path(source).resolve(strict=True)
        raw = path.read_bytes()
        rows.append(dict(row_id=str(position), ImagePath=str(path),
                         image=dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())))
    stages = tuple('prepare-' + str(i) for i in range(config['passes']))
    profile = dict(model='not_invoked_cpu_preparation', generation=dict(temperature=0),
                   service_preprocess='not_invoked')
    arms, reference = [], None
    study_id = uuid.uuid4().hex
    order = ('disabled', 'keep_until_last', 'lru', 'consumer_rebuild_value')
    for repeat in range(config['repeats']):
        for name in order[repeat % len(order):] + order[:repeat % len(order)]:
            events, outputs = [], []
            run_id = f'repeat-{repeat}-{name}'
            event_error = None
            def record(event):
                # The finite source/consumer bounds also bound this private list.
                events.append(dict(event, run_id=study_id + ':' + run_id, observer_session_id=study_id, pid=os.getpid(),
                                   _evidence=dict(file=run_id + '.events.json', entry=len(events))))
            owner = LotusMediaObservation(rows, stages,
                retention_bytes=config['capacity_bytes'], retention_enabled=name != 'disabled',
                profile=profile, record=lambda event: None, representation_observer=record,
                retention_policy=name if name in ('consumer_rebuild_value', 'lru') else None)
            start = time.monotonic_ns()
            status = 'succeeded'
            try:
                with owner.observe():
                    for stage in stages:
                        owner.begin(stage, [row['row_id'] for row in rows])
                        for row in rows:
                            value = owner.module.fetch_image(row['ImagePath'], 'base64')
                            outputs.append(hashlib.sha256(value.encode()).hexdigest())
                        owner.finish()
                owner.close()
            except BaseException as error:
                status = 'failed'
                event_error = dict(type=type(error).__name__, message=redact_text(str(error)))
                raise
            finally:
                elapsed = time.monotonic_ns() - start
                from .representation_diagnostics import diagnose_representations
                diagnostic = diagnose_representations(events, capture_issues=[])
                # This list is a directly owned, synchronously written CPU study,
                # not an engine collector's independent close-receipt proof.
                diagnostic['capture_basis'] = 'finite_local_list_and_exclusive_private_write'
                summary = dict(arm=name, repeat=repeat, status=status, error=event_error,
                    prepare_consume_wall_ns=elapsed, snapshot=owner.snapshot(),
                    outputs_sha256=outputs, diagnostic=diagnostic)
                write_private_json(output / (run_id + '.events.json'), events)
                write_private_json(output / (run_id + '.json'), summary)
                arms.append(summary)
            if reference is None:
                reference = outputs
            elif outputs != reference:
                raise ValueError('representation outputs changed between matched CPU arms')
    result = dict(schema='semloom.media_retention_study.result.v1', study_id=study_id, input_kind=config['input_kind'],
                  source_type='CPU_original_LOTUS_preparation', model_calls=0, gpu=False,
                  matched_outputs=True, rows=len(rows), passes=len(stages), repeats=config['repeats'],
                  capacity_bytes=config['capacity_bytes'], arms=arms,
                  scope='Preparation and declared URI consumers only. No semantic query, device cache or end-to-end performance conclusion.')
    write_private_json(output / 'result.json', result)
    return result
