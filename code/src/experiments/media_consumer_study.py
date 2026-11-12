"""Exercise real image preparation under explicitly constructed consumer traces."""
from collections import defaultdict
import hashlib
import importlib.metadata
import io
from pathlib import Path
import time

from src.infrastructure.private_artifacts import new_private_directory, write_private_json
from src.modalities.image.shared_media import SharedMediaRepresentation, media_key


def run_study(config, output):
    if (config['schema'] != 'semloom.media_consumer_study.v1' or not config['images']
            or type(config['repeats']) is not int or config['repeats'] < 1):
        raise ValueError('images and positive repeats are required for the consumer study')
    from PIL import Image
    from lotus.dtype_extensions.image import fetch_image
    output = Path(output); new_private_directory(output)
    write_private_json(output / 'config.json', config)
    sources = [Path(p).read_bytes() for p in config['images']]
    digests = [hashlib.sha256(raw).hexdigest() for raw in sources]
    profile = dict(model='not_invoked_cpu_preparation', generation=dict(temperature=0), service_preprocess='not_invoked',
                   media_recipe='LOTUS-RGB-PNG-base64', lotus=importlib.metadata.version('lotus-ai'),
                   pillow=importlib.metadata.version('Pillow'))

    def produce(raw):
        with Image.open(io.BytesIO(raw)) as image:
            return fetch_image(image, 'base64')

    values = [produce(raw) for raw in sources]
    sizes = [len(value) for value in values]
    expected = [hashlib.sha256(value.encode()).hexdigest() for value in values]
    del values
    # Capacity is selected from representation sizes before policy timing.
    capacities = sorted({max(max(sizes), sum(sizes)//3), max(max(sizes), sum(sizes)//2)})
    n = len(sources)
    traces = dict(round_robin=[(i, True) for _ in range(3) for i in range(n)],
                  record_local=[(i, True) for i in range(n) for _ in range(3)],
                  uneven_consumers=[(i, True) for i in range(n)] + [(i, True) for i in range(n) for _ in range(i+1)],
                  revealed_drop=[(i, not (stage == 1 and i % 2)) for stage in range(3) for i in range(n)])
    arms = []
    policies = ('disabled', 'keep_until_last', 'lru', 'consumer_rebuild_value')
    for trace_name, trace in traces.items():
        plan = defaultdict(list)
        for position, (index, _) in enumerate(trace):
            plan[media_key(digests[index], profile)].append(str(position))
        plan = {key: tuple(consumers) for key, consumers in plan.items()}
        for capacity in capacities:
            for repeat in range(config['repeats']):
                for policy in policies[repeat % 4:] + policies[:repeat % 4]:
                    events, outputs = [], []
                    owner = SharedMediaRepresentation(plan, capacity_bytes=capacity,
                        max_entries=n, max_item_bytes=max(sizes), max_consumers=len(trace), observer=events.append,
                        retention_enabled=policy != 'disabled',
                        retention_policy=policy if policy in ('lru', 'consumer_rebuild_value') else None)
                    started = time.perf_counter_ns()
                    try:
                        for position, (index, consumed) in enumerate(trace):
                            if not consumed:
                                owner.abandon_unstarted(digests[index], profile, str(position))
                                continue
                            value = owner.borrow(sources[index], digests[index], profile, str(position), produce)
                            observed = hashlib.sha256(value.encode()).hexdigest()
                            if observed != expected[index]:
                                raise ValueError('retention changed the original prepared representation')
                            outputs.append(observed)
                            del value
                            owner.finish(digests[index], profile, str(position))
                        snapshot = owner.snapshot()
                        if snapshot['active_borrows'] or snapshot['pending_consumers'] or snapshot['retained_bytes']:
                            raise ValueError('completed trace left representation responsibility')
                    finally:
                        owner.close()
                    arm = dict(trace=trace_name, capacity_bytes=capacity, repeat=repeat, policy=policy,
                               prepare_consume_wall_ns=time.perf_counter_ns()-started,
                               outputs_sha256=outputs, snapshot=snapshot, events=events)
                    name=f'{trace_name}-{capacity}-{repeat}-{policy}.json'
                    write_private_json(output / name, arm)
                    arms.append({k:v for k,v in arm.items() if k != 'events'} | {'events_file':name})
    report = dict(schema='semloom.media_consumer_study.result.v1', profile=profile, source_sha256=digests,
                  uri_bytes=sizes, capacities=capacities, traces=traces, arms=arms, model_calls=0, gpu=False,
                  scope='COCO images with constructed physical consumer order/drop traces, not observed semantic query outcomes. Source bytes are preloaded outside each measured arm.')
    write_private_json(output / 'result.json', report)
    return report
