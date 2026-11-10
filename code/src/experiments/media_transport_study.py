"""Matched CPU processes for image preparation position and materialization.

The worker returns exact RGB pixels' digest. Encoded source and decoded RGB are
real transferred representations; none of these timings describe H2D or a model.
"""
import hashlib
import importlib.metadata
import multiprocessing as mp
import os
from multiprocessing.connection import wait
from multiprocessing.reduction import ForkingPickler
from pathlib import Path
import time

from src.infrastructure.private_artifacts import new_private_directory, write_private_json
from src.modalities.image.representations import consume_representation, decode_rgb, prepare_representation


def _worker(connection):
    import psutil
    from PIL import Image
    Image.init()
    process = psutil.Process()
    connection.send(dict(ready=True))
    try:
        while True:
            packet = connection.recv_bytes()
            if not packet:
                return
            index, value = ForkingPickler.loads(packet)
            started = time.perf_counter_ns()
            size, pixels = consume_representation(value)
            prepare_ns = time.perf_counter_ns() - started
            digest = hashlib.sha256(pixels).hexdigest()
            connection.send(dict(index=index, size=size, pixels_sha256=digest,
                received_bytes=len(packet), worker_prepare_ns=prepare_ns,
                worker_pid=os.getpid(),
                worker_sampled_rss_bytes=process.memory_info().rss))
    finally:
        connection.close()


def _run_arm(paths, references, *, kind, materialization, workers, timeout_s):
    import psutil
    context, hosts, children = mp.get_context('spawn'), [], []
    created = time.perf_counter_ns()
    deadline = time.monotonic() + timeout_s
    parent_process = psutil.Process()
    events = []
    try:
        for _ in range(workers):
            host, worker = context.Pipe()
            process = context.Process(target=_worker, args=(worker,))
            process.start(); worker.close()
            hosts.append(host); children.append(process)
        for host in hosts:
            if not host.poll(max(0, deadline - time.monotonic())):
                raise TimeoutError('image worker did not become ready')
            host.recv()
        started = time.perf_counter_ns()

        def prepared():
            for index, path in enumerate(paths):
                begin = time.perf_counter_ns()
                raw = path.read_bytes()
                value = prepare_representation(raw, kind)
                events.append(dict(index=index, source_bytes=len(raw), payload_bytes=len(value['payload']),
                    parent_prepare_ns=time.perf_counter_ns() - begin,
                    prepared_at_ns=time.perf_counter_ns() - started))
                yield index, value

        values = list(prepared()) if materialization == 'stage' else prepared()
        ready, pending = list(hosts), {}
        source = iter(values)
        eof = False
        parent_rss_samples = [parent_process.memory_info().rss]
        while not eof or pending:
            while ready and not eof:
                try:
                    index, value = next(source)
                except StopIteration:
                    eof = True
                    break
                begin = time.perf_counter_ns()
                packet = ForkingPickler.dumps((index, value))
                events[index]['serialize_ns'] = time.perf_counter_ns() - begin
                events[index]['sent_bytes'] = len(packet)
                host = ready.pop(0)
                events[index]['worker_slot'] = hosts.index(host)
                begin = time.perf_counter_ns()
                host.send_bytes(packet)
                events[index]['send_ns'] = time.perf_counter_ns() - begin
                pending[host] = index
                parent_rss_samples.append(parent_process.memory_info().rss)
                del packet, value
            if pending:
                done = wait(tuple(pending), timeout=max(0, deadline - time.monotonic()))
                if not done:
                    raise TimeoutError('image transport arm exceeded its operation time')
                for host in done:
                    response = host.recv()
                    index = pending.pop(host)
                    if (response['index'] != index or tuple(response['size']) != references[index][0]
                            or response['pixels_sha256'] != references[index][1]
                            or response['received_bytes'] != events[index]['sent_bytes']):
                        raise ValueError('transferred representation changed occurrence, pixels or byte accounting')
                    events[index].update(response, completed_at_ns=time.perf_counter_ns() - started)
                    ready.append(host)
        elapsed = time.perf_counter_ns() - started
        return dict(representation=kind, materialization=materialization, workers=workers,
                    pipeline_wall_ns=elapsed, startup_ns=started-created, events=events,
                    parent_max_sampled_rss_bytes=max(parent_rss_samples),
                    actual_frame_bytes=sum(e['sent_bytes'] for e in events), matched_pixels=True)
    finally:
        for host, process in zip(hosts, children):
            if process.is_alive():
                try:
                    host.send_bytes(b'')
                except (BrokenPipeError, EOFError, OSError):
                    pass
            host.close()
        for process in children:
            process.join(max(0, deadline-time.monotonic()))
            if process.is_alive():
                process.terminate(); process.join()


def run_study(config, output):
    if (config['schema'] != 'semloom.media_transport_study.v1'
            or not config['images'] or any(type(config[k]) is not int or config[k] < 1
                                         for k in ('workers', 'repeats', 'timeout_s'))):
        raise ValueError('explicit images, CPU workers, repeats and operation time are required')
    output = Path(output)
    new_private_directory(output)
    write_private_json(output / 'config.json', config)
    paths = [Path(p).resolve(strict=True) for p in config['images']]
    references, inputs = [], []
    for path in paths:
        raw = path.read_bytes()
        size, pixels = decode_rgb(raw)
        references.append((size, hashlib.sha256(pixels).hexdigest()))
        inputs.append(dict(file=path.name, source_sha256=hashlib.sha256(raw).hexdigest(),
                           source_bytes=len(raw), size=size, rgb_bytes=len(pixels)))
    del raw, pixels
    modes = [('encoded', 'stream'), ('rgb', 'stream'), ('encoded', 'stage'), ('rgb', 'stage')]
    arms = []
    for repeat in range(config['repeats']):
        for kind, materialization in modes[repeat % 4:] + modes[:repeat % 4]:
            arm = _run_arm(paths, references, kind=kind, materialization=materialization,
                           workers=config['workers'], timeout_s=config['timeout_s'])
            arm['repeat'] = repeat
            write_private_json(output / f'{repeat}-{kind}-{materialization}.json', arm)
            arms.append(arm)
    report = dict(schema='semloom.media_transport_study.result.v1', inputs=inputs, arms=arms,
                  pillow_version=importlib.metadata.version('Pillow'), model_calls=0, gpu=False,
                  worker_ready_scope='Pillow decoders initialized before ready; no image is decoded as warmup',
                  scope='Real local CPU processes, image decode and IPC. Sampled RSS is not a peak guarantee. No GPU transfer, semantic query or model inference.')
    write_private_json(output / 'result.json', report)
    return report
