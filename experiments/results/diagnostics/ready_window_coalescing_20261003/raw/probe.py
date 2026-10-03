"""Local transport-only probe; batch preparation and remote calls are simulated."""
import argparse
import asyncio
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time
from unittest.mock import patch


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def run_case(pattern, wait_ms, phase, repeat):
    from src.execution_provider.adapters.model_config import FixedModelConfig
    from src.execution_provider.adapters.ray_map_transport import RayMapConfig, RayMapTransport, _RemoteResult
    from tests.execution_provider.test_ray_map_transport import FakeRay, task
    from tests.execution_provider.test_ready_window_coalescing import PreparedTable

    capacity, batch_rows = 64, 16
    count = 256 if pattern == 'burst' else 128
    preparation_seconds, remote_seconds = .0005, .010
    windows, events, issued, delivered, offered, latencies = [], [], [], [], {}, []
    first_result = None
    peak_active = 0
    active_remote = 0

    def batches(rows, limits, *, batch_rows, backend):
        assert len(rows) <= limits.rows
        assert sum(len(row[2]) + 24 for row in rows) <= limits.bytes
        windows.append(len(rows))
        time.sleep(preparation_seconds)
        for offset in range(0, len(rows), batch_rows):
            yield PreparedTable(rows[offset:offset + batch_rows])

    async def execute(table, index, template, *args):
        nonlocal active_remote, peak_active
        sequence = template.key.sequence
        assert table['sequence'][index].as_py() == sequence
        assert table['payload'][index].as_py() == f'payload-{sequence}'.encode()
        issued.append(sequence)
        active_remote += 1
        peak_active = max(peak_active, active_remote)
        try:
            await asyncio.sleep(remote_seconds + (sequence % 7) * .0003)
            return _RemoteResult(template.key, f'result-{sequence}'.encode(), 1, 2, None)
        finally:
            active_remote -= 1

    physical = RayMapConfig('fixture-cluster', 1, batch_rows, 4096, 8192, ready_wait_ms=wait_ms)
    transport = RayMapTransport(FixedModelConfig('http://localhost/fixture', 'model', 1000),
                                capacity, events.append, physical=physical, ray_api=FakeRay(execute))
    pending = set()
    began = time.perf_counter()

    async def request(sequence):
        nonlocal first_result
        result = await transport.execute(task(sequence, f'payload-{sequence}'.encode()), 'model')
        ended = time.perf_counter()
        assert result == f'result-{sequence}'.encode()
        delivered.append(sequence)
        latencies.append(ended - offered[sequence])
        if first_result is None:
            first_result = ended - began

    with patch('src.execution_provider.adapters.ray_map_transport.iter_payload_batches', batches):
        try:
            for sequence in range(count):
                if pattern == 'sparse' and sequence:
                    await asyncio.sleep(.004)
                elif pattern == 'burst' and sequence and sequence % 8 == 0:
                    await asyncio.sleep(.00025)
                else:
                    await asyncio.sleep(0)
                completed = {operation for operation in pending if operation.done()}
                for operation in completed:
                    operation.result()
                pending.difference_update(completed)
                if len(pending) == capacity:
                    completed, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    for operation in completed:
                        operation.result()
                offered[sequence] = time.perf_counter()
                pending.add(asyncio.create_task(request(sequence)))
            await asyncio.gather(*pending)
            elapsed = time.perf_counter() - began
        finally:
            await transport.close()
    assert sorted(issued) == sorted(delivered) == list(range(count))
    assert active_remote == 0 and peak_active <= capacity
    assert not transport.rows and not transport.blocks and transport.used_bytes == 0
    peak_bytes = max((event['object_bytes'] for event in events), default=0)
    assert peak_bytes <= physical.object_bytes
    waits = [event for event in events if event['event'] == 'ray_ready_wait']
    return dict(pattern=pattern, ready_wait_ms=wait_ms, phase=phase, repeat=repeat,
                rows=count, capacity=capacity, physical=asdict(physical),
                preparation_seconds=preparation_seconds, remote_seconds=remote_seconds,
                elapsed_seconds=elapsed, first_result_seconds=first_result,
                mean_row_seconds=statistics.mean(latencies), max_row_seconds=max(latencies),
                windows=windows, preparation_windows=len(windows),
                object_batches=sum(event['event'] == 'ray_block_put' for event in events),
                object_peak_bytes=peak_bytes, remote_peak=peak_active,
                ready_wait_seconds=sum(event['elapsed_ns'] for event in waits) / 1e9,
                ready_wait_reasons={reason: sum(event['stage'] == reason for event in waits)
                                   for reason in sorted({event['stage'] for event in waits})},
                simulated_calls=len(issued), http_requests=0, model_requests=0,
                exactly_once=True, resources_drained=True)


async def main(repo, output):
    source_paths = ['code/src/execution_provider/adapters/ray_map_transport.py',
                    'code/src/data/materializers/payloads.py',
                    'code/tests/execution_provider/test_ray_map_transport.py',
                    'code/tests/execution_provider/test_ready_window_coalescing.py']
    report = dict(started_utc=datetime.now(timezone.utc).isoformat(),
                  reference_commit=subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip(),
                  python=platform.python_version(), platform=platform.platform(),
                  source_sha256={path: digest(repo / path) for path in source_paths},
                  probe_sha256=digest(Path(__file__)), scope='local_simulated_transport', runs=[])
    deadline = time.monotonic() + 300
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise FileExistsError('probe output already exists')
    try:
        for pattern in ('burst', 'sparse'):
            for phase, repeat in [('warmup', 0), ('measurement', 1), ('measurement', 2), ('measurement', 3)]:
                for wait_ms in ((0, 1) if repeat % 2 == 0 else (1, 0)):
                    if time.monotonic() >= deadline:
                        raise TimeoutError('local probe overall deadline')
                    run = await asyncio.wait_for(run_case(pattern, wait_ms, phase, repeat),
                                                 min(30, deadline - time.monotonic()))
                    report['runs'].append(run)
                    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
                    print(json.dumps({key: run[key] for key in ('pattern', 'ready_wait_ms', 'phase', 'repeat',
                        'elapsed_seconds', 'first_result_seconds', 'preparation_windows', 'simulated_calls')},
                        ensure_ascii=False), flush=True)
        report['completed_utc'] = datetime.now(timezone.utc).isoformat()
        report['passed'] = True
    except BaseException as error:
        report['passed'] = False
        report['error_type'] = type(error).__name__
        raise
    finally:
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    arguments = parser.parse_args()
    sys.path.insert(0, str(arguments.repo / 'code'))
    asyncio.run(main(arguments.repo, arguments.output))
