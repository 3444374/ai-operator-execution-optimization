"""Small native/Direct FP32 qualification; private artifacts, shared lease.

Run as a module with explicit asset, artifact and source identities. This is an
independent backend check, not a LOTUS query or a performance campaign.
"""
import argparse
import gc
import hashlib
import io
import json
import os
from pathlib import Path
import time
import traceback
import weakref
import uuid
from unittest.mock import patch

import numpy as np

from src.baselines.common.redact import redact_json_values, redact_text
from src.baselines.image.frameworks import daft_clip as native
from src.infrastructure.runner_lease import (
    acquire_runner_lease, HOST_SCOPE_DIR_NAME, HOST_SCOPE_FINGERPRINT,
)
from src.modalities.image.clip import ClipImagePreprocessor, ClipTensorActor
from src.modalities.image.contracts import ImageEmbeddingBatch


def record(event, **fields):
    with Path(os.environ['SEMLOOM_DAFT_CLIP_GPU_TRACE']).open('a') as handle:
        handle.write(json.dumps(dict(event=event, pid=os.getpid(), **fields)) + '\n')


class ObservedActor(ClipTensorActor):
    def __init__(self, *args, **kwargs):
        started = time.perf_counter()
        super().__init__(*args, **kwargs)
        self.model_instance_id = uuid.uuid4().hex
        parameters, buffers = tuple(self._model.parameters()), tuple(self._model.buffers())
        finalizer = weakref.finalize(self._model, record, 'model_module_destroyed',
                                    model_instance_id=self.model_instance_id)
        finalizer.atexit = False
        for kind, tensors in [('parameter', parameters), ('buffer', buffers)]:
            for index, tensor in enumerate(tensors):
                finalizer = weakref.finalize(tensor, record, 'model_tensor_destroyed',
                    model_instance_id=self.model_instance_id, kind=kind, tensor_index=index)
                finalizer.atexit = False
        record('model_loaded', elapsed_s=time.perf_counter() - started,
               device=self._device, dtype=str(self._dtype), parameters=len(parameters),
               buffers=len(buffers), model_instance_id=self.model_instance_id)

    def embed(self, batch):
        result = super().embed(batch)
        record('model_batch_completed', rows=len(batch.doc_ids), service_s=result.service_s)
        return result

    def __del__(self):
        if hasattr(self, 'model_instance_id'):
            record('model_destroyed', model_instance_id=self.model_instance_id)


def check_model_release(events):
    """Confirm exactly-once destruction of the registered native model objects."""
    loads = [x for x in events if x['event'] == 'model_loaded']
    owners = [(x['pid'], x['model_instance_id']) for x in loads]
    if not owners or len(set(owners)) != len(owners):
        raise RuntimeError('native model load identity missing or duplicated')
    expected = {(x['pid'], x['model_instance_id'], kind, index)
                for x in loads for kind in ('parameter', 'buffer')
                for index in range(x[kind + 's'])}
    tensors = [(x['pid'], x['model_instance_id'], x['kind'], x['tensor_index'])
               for x in events if x['event'] == 'model_tensor_destroyed']
    for event in ('model_destroyed', 'model_module_destroyed'):
        observed = [(x['pid'], x['model_instance_id']) for x in events if x['event'] == event]
        if len(observed) != len(owners) or set(observed) != set(owners):
            raise RuntimeError('native model object destruction unconfirmed')
    if len(tensors) != len(expected) or set(tensors) != expected:
        raise RuntimeError('native model parameter/buffer destruction unconfirmed')
    return dict(models_loaded=len(owners), models_destroyed=len(owners),
                model_modules_destroyed=len(owners),
                parameters_loaded=sum(x['parameters'] for x in loads),
                parameters_destroyed=sum(x['kind'] == 'parameter' for x in events
                                         if x['event'] == 'model_tensor_destroyed'),
                buffers_loaded=sum(x['buffers'] for x in loads),
                buffers_destroyed=sum(x['kind'] == 'buffer' for x in events
                                      if x['event'] == 'model_tensor_destroyed'))


def digest_bytes(value):
    return hashlib.sha256(value).hexdigest()


def run(args, report):
    import daft
    from daft.functions import decode_image
    import torch
    from PIL import Image

    if native.metadata.version('transformers') != '4.57.6':
        raise RuntimeError('qualification requires Transformers 4.57.6')
    from src.modalities.image.clip import configure_torch_thread_pools
    configure_torch_thread_pools(1, 1)
    report['hardware'] = dict(gpu=torch.cuda.get_device_name(0),
                            torch_cuda=torch.version.cuda, gpu_count=torch.cuda.device_count())
    files = sorted(Path(args.image_root).glob('*.jpg'))[:9]
    if len(files) != 9:
        raise ValueError('qualification requires nine source JPEG files')
    images, source_ids = [], []
    source = []
    for path in files:
        encoded = path.read_bytes()
        with Image.open(io.BytesIO(encoded)) as image:
            image = image.convert('RGB')
            output = io.BytesIO()
            image.save(output, format='PNG')
        images.append(output.getvalue())
        source_ids.append(path.stem)
        source.append(dict(id=path.stem, source_sha256=digest_bytes(encoded),
                           png_sha256=digest_bytes(images[-1])))
    ids = tuple(source_ids)
    report['inputs'] = source
    report['input_sha256'] = digest_bytes(json.dumps(source, sort_keys=True).encode())
    processor = ClipImagePreprocessor(args.processor, local_files_only=True)
    direct_pixels = processor.preprocess(images)
    frame = daft.from_pydict({'image': images})
    frame = frame.with_column('_rgb', decode_image(frame['image'], on_error='raise', mode=daft.ImageMode.RGB))
    decoded = frame.to_pydict()['_rgb']  # explicitly capped nine-image qualification
    native_pixels = native._load_processor(args.processor)(
        images=decoded, return_tensors='np', input_data_format='channels_last')['pixel_values']
    report['pixels'] = dict(direct_sha256=digest_bytes(direct_pixels.tobytes()),
                           native_sha256=digest_bytes(native_pixels.tobytes()),
                           max_abs=float(np.max(np.abs(direct_pixels - native_pixels))),
                           shape=list(direct_pixels.shape))
    if not np.array_equal(direct_pixels, native_pixels):
        raise ValueError('native and Direct effective pixel tensors differ')

    started = time.perf_counter()
    direct = ClipTensorActor(args.model, processor_revision=args.processor, dtype='float32',
                             normalize=True, local_files_only=True,
                             torch_intraop_threads=1, torch_interop_threads=1)
    report['direct_model_load_s'] = time.perf_counter() - started
    chunks = []
    started = time.perf_counter()
    for position in range(0, 9, args.batch_size):
        batch_ids = ids[position:position + args.batch_size]
        payload = direct_pixels[position:position + args.batch_size]
        batch = ImageEmbeddingBatch(batch_ids, payload, 'preprocessed_tensor',
                                    len(batch_ids) * 224 * 224, 'pixels')
        chunks.append(direct.embed(batch).embeddings)
    expected = np.concatenate(chunks)
    report['direct_prepared_tensor_batch_s'] = time.perf_counter() - started
    direct.synchronize()
    del direct, chunks
    gc.collect()
    torch.cuda.synchronize()
    report['direct_memory_after_release'] = torch.cuda.memory_allocated()

    started = time.perf_counter()
    with patch.object(native, 'ClipTensorActor', ObservedActor):
        executor = native.DaftClipExecutor(model_path=args.model, processor_path=args.processor,
                                           dtype='float32', batch_size=args.batch_size, model_workers=1,
                                           gpus_per_worker=1, device='cuda', input_kind='encoded_bytes')
    report['native_constructor_s'] = time.perf_counter() - started
    report['identity'] = executor.identity
    report['runs'] = []
    report['comparison'] = []
    all_outputs = []
    for iteration in range(4):
        started = time.perf_counter()
        output = executor.embed_batch(ids, images)
        wall_s = time.perf_counter() - started
        actual = output.embeddings
        if output.doc_ids != ids:
            raise ValueError('output IDs did not preserve the complete input batch')
        cosine = np.sum(expected.astype(np.float64) * actual.astype(np.float64), axis=1)
        cosine /= np.linalg.norm(expected.astype(np.float64), axis=1) * np.linalg.norm(actual.astype(np.float64), axis=1)
        error = np.max(np.abs(expected - actual), axis=1)
        norm_error = np.abs(np.linalg.norm(actual, axis=1) - 1)
        report['runs'].append(dict(iteration=iteration, phase='first_query' if iteration == 0 else 'reused_instance',
                                   wall_s=wall_s, service_s=output.service_s, rows=len(ids),
                                   max_abs=float(error.max()), min_cosine=float(cosine.min()),
                                   max_norm_error=float(norm_error.max())))
        for identifier, absolute, similarity in zip(ids, error, cosine, strict=True):
            report['comparison'].append(dict(iteration=iteration, id=identifier,
                                             max_abs=float(absolute), cosine=float(similarity)))
        if error.max() > 1e-5 or cosine.min() < .99999 or norm_error.max() > 1e-5:
            raise ValueError('native/Direct numerical tolerance failed')
        all_outputs.append(actual)
    report['output_sha256'] = dict(direct=digest_bytes(expected.tobytes()),
                                   native=[digest_bytes(x.tobytes()) for x in all_outputs])
    np.savez_compressed(Path(args.output) / 'embeddings.npz', direct=expected,
                        native=np.stack(all_outputs), ids=np.asarray(ids))
    report['embeddings_npz_sha256'] = native._digest(Path(args.output) / 'embeddings.npz')
    report['accuracy_passed'] = True
    report['cleanup_passed'] = False
    started = time.perf_counter()
    executor.close()
    gc.collect()
    torch.cuda.synchronize()
    report['close_s'] = time.perf_counter() - started
    report['native_memory_after_close'] = torch.cuda.memory_allocated()
    trace = [json.loads(line) for line in Path(os.environ['SEMLOOM_DAFT_CLIP_GPU_TRACE']).read_text().splitlines()]
    report['model_events'] = trace
    released = check_model_release(trace)
    children = {x['pid'] for x in trace if x['pid'] != os.getpid()}
    remaining_children = []
    for pid in children:
        try:
            os.kill(pid, 0)
            remaining_children.append(pid)
        except ProcessLookupError:
            pass
    report['cleanup'] = dict(graphs_fully_consumed=4, **released,
                             udf_process_location='driver' if not children else 'child',
                             remaining_owned_children=remaining_children)
    # Process-global allocations are recorded without attributing their residue.
    if remaining_children or released['parameters_loaded'] != 398:
        raise RuntimeError('owned worker exit or fixed model parameter identity unconfirmed')
    report['cleanup_passed'] = True
    report['status'] = 'passed'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('model', 'processor', 'image-root', 'artifact-root', 'output', 'source-hash', 'base-sha'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument("--batch-size", type=int, default=32)
    args = parser.parse_args()
    if args.batch_size <= 0:
        parser.error("batch size must be positive")
    output = Path(args.output)
    output.mkdir(exist_ok=False, parents=True)
    os.environ['SEMLOOM_DAFT_CLIP_GPU_TRACE'] = str(output / 'model-trace.jsonl')
    report = dict(schema='daft.clip.native.qualification.v1', source_hash=args.source_hash,
                  baseline_sha=args.base_sha, dtype='float32', rows=9, batch_size=args.batch_size,
                  input_kind='encoded_bytes',
                  tolerance=dict(max_abs=1e-5, min_cosine=.99999, max_norm_error=1e-5),
                  tail_percentiles='unavailable: nine-image backend qualification, four batch calls',
                  status='not_started')
    lease = None
    try:
        lease = acquire_runner_lease(Path(args.artifact_root) / HOST_SCOPE_DIR_NAME,
            config_fingerprint=HOST_SCOPE_FINGERPRINT, repository_commit=args.base_sha,
            recover_stale=False)
        report['shared_lease_acquired'] = True
        run(args, report)
    except BaseException as error:
        report.update(status='failed', error_type=type(error).__name__,
                      error=redact_text(str(error)), traceback=redact_text(traceback.format_exc()))
        raise
    finally:
        release_error = None
        if lease is not None and report['status'] == 'passed':
            try:
                lease.release()
                report['shared_lease_released'] = True
            except BaseException as error:
                release_error = error
                report['shared_lease_released'] = 'unconfirmed'
                report['lease_release_error'] = redact_text(str(error))
        elif lease is not None:
            report['shared_lease_released'] = False
        (output / 'report.json').write_text(json.dumps(redact_json_values(report), indent=2) + '\n')
        print(json.dumps(dict(status=report['status'], lease_released=report.get('shared_lease_released'),
                              report=str(output / 'report.json'))))
        if release_error is not None:
            raise release_error


if __name__ == '__main__':
    main()
