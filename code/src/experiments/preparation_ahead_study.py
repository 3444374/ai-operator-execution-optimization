"""Prepare the next known image while auditing the current consumed RGB output.

Masks/probabilities are declared synthetic branch traces. Decode and checksum work
are real CPU operations. There is no artificial sleep or simulated model call.
"""
import hashlib
from pathlib import Path
import time

from src.infrastructure.private_artifacts import new_private_directory, write_private_json
from src.modalities.image.preparation_ahead import ImagePreparationAhead, preparation_value, prepare_rgb


def run_study(config, output):
    if (config['schema'] != 'semloom.preparation_ahead_study.v1' or not config['images']
            or type(config['repeats']) is not int or config['repeats'] < 1):
        raise ValueError('images and positive repeats required')
    output = Path(output); new_private_directory(output)
    write_private_json(output / 'config.json', config)
    sources = [Path(p).read_bytes() for p in config['images']]
    calibration, expected = [], []
    for raw in sources:
        prepared = prepare_rgb(raw)
        begin = time.perf_counter_ns()
        expected.append(hashlib.sha256(prepared.pixels).hexdigest())
        calibration.append(dict(prepare_ns=prepared.wall_ns, sink_ns=time.perf_counter_ns()-begin))
    del prepared
    n = len(sources)
    traces = dict(dense=([i % n for i in range(2*n)], .8), sparse=([i % n for i in range(2*n)], .3))
    arms = []
    policies = ('on_demand', 'fixed_ahead', 'probability_half', 'expected_cost')
    for trace, (indices, probability) in traces.items():
        selected = [i % 5 != 0 if trace == 'dense' else i % 3 == 0 for i in range(len(indices))]
        for repeat in range(config['repeats']):
            for policy in policies[repeat % 4:] + policies[:repeat % 4]:
                owner = ImagePreparationAhead(max_pending=1, workers=1)
                decisions, outputs = [], []
                on_demand_cpu_ns, sink_ns = 0, 0
                overlap_ns = sum(c['sink_ns'] for c in calibration) // n
                start = time.perf_counter_ns()
                try:
                    for position, index in enumerate(indices):
                        prepared = None
                        if position in owner.pending:
                            if selected[position]:
                                prepared = owner.take(position)
                            else:
                                owner.discard(position)
                        elif selected[position]:
                            prepared = prepare_rgb(sources[index])
                            on_demand_cpu_ns += prepared.cpu_ns
                        if position+1 < len(indices):
                            following = indices[position+1]
                            value = preparation_value(probability, calibration[following]['prepare_ns'], overlap_ns, 0)
                            choose = (policy == 'fixed_ahead' or policy == 'probability_half' and probability >= .5
                                      or policy == 'expected_cost' and value['prepare'])
                            submitted = owner.submit(position+1, sources[following]) if choose else False
                            decisions.append(dict(position=position+1, probability=probability, submitted=submitted, **value))
                        if prepared is not None:
                            begin = time.perf_counter_ns()
                            digest = hashlib.sha256(prepared.pixels).hexdigest()
                            overlap_ns = time.perf_counter_ns()-begin
                            sink_ns += overlap_ns
                            if digest != expected[index]:
                                raise ValueError('early preparation changed consumed pixels')
                            outputs.append(dict(position=position, pixels_sha256=digest))
                        del prepared
                finally:
                    owner.close()
                arm = dict(trace=trace, policy=policy, repeat=repeat, wall_ns=time.perf_counter_ns()-start,
                           on_demand_cpu_ns=on_demand_cpu_ns, sink_ns=sink_ns,
                           preparation=owner.snapshot(), outputs=outputs, decisions=decisions,
                           consumer_mask=selected, matched_pixels=True)
                write_private_json(output / f'{trace}-{repeat}-{policy}.json', arm)
                arms.append(arm)
    result = dict(schema='semloom.preparation_ahead_study.result.v1', calibration=calibration,
                  source_sha256=[hashlib.sha256(x).hexdigest() for x in sources], arms=arms, model_calls=0, gpu=False,
                  scope='Real CPU decode/checksum overlap; synthetic demand masks and probabilities. Assumes one spare CPU preparation worker. No semantic branch accuracy, model interference or query speedup conclusion.')
    write_private_json(output / 'result.json', result)
    return result
