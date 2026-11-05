#!/usr/bin/env python3
"""Sample an already running local service into bounded private JSONL; no inference."""
import argparse
import json
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.baselines.common.private_artifacts import open_private_text, write_private_json

METRICS = re.compile(r'^vllm:(?:num_requests_(?:running|waiting)|kv_cache_usage_perc|'
                     r'prefix_cache_(?:queries|hits)_total|prompt_tokens_total|generation_tokens_total)(?:\{| )')


def main():
    import psutil
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--port', type=int, required=True)
    p.add_argument('--pid', type=int, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--seconds', type=int, required=True)
    args = p.parse_args()
    if not 1 <= args.seconds <= 7200 or not 1 <= args.port <= 65535:
        raise ValueError('invalid duration or port')
    process = psutil.Process(args.pid)
    stopping = False
    def stop(*_):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    errors = samples = 0
    deadline = time.monotonic() + args.seconds
    with open_private_text(args.output) as output:
        while not stopping and time.monotonic() < deadline:
            before = time.monotonic()
            row = dict(monotonic_ns=time.monotonic_ns())
            try:
                with urllib.request.urlopen(f'http://127.0.0.1:{args.port}/metrics', timeout=2) as response:
                    payload = response.read(2*1048576+1)
                if len(payload) > 2*1048576:
                    raise ValueError('service metrics too large')
                row['metrics'] = [line for line in payload.decode().splitlines() if METRICS.match(line)]
                row['service_processes'] = [dict(pid=proc.pid, rss_bytes=proc.memory_info().rss)
                                            for proc in [process, *process.children(recursive=True)]]
                if samples % 5 == 0:
                    row['gpu'] = subprocess.check_output(['nvidia-smi', '--query-gpu=index,utilization.gpu,memory.used,power.draw',
                                                          '--format=csv,noheader,nounits'], text=True, timeout=3).splitlines()
            except (OSError, ValueError, psutil.Error, subprocess.SubprocessError) as error:
                row['error_type'] = type(error).__name__
                errors += 1
            output.write(json.dumps(row) + '\n')
            samples += 1
            time.sleep(max(0, min(1-(time.monotonic()-before), deadline-time.monotonic())))
    write_private_json(args.output.with_suffix('.summary.json'), dict(samples=samples, sample_errors=errors,
                       interval_seconds=1, gpu_interval_samples=5, inference_requests=0))


if __name__ == '__main__':
    main()
