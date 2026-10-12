#!/usr/bin/env python3
"""Exercise the original IMLane Coordinator and executors with deterministic Arrow data."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

OWNED = Path(__file__).resolve().parent
sys.path.insert(0, str(OWNED.parents[1]))
from src.baselines.common.redact import redact_text
try:
    import psutil
except ModuleNotFoundError:
    print(redact_text('Missing psutil resource sampler; run prepare.py in the target environment.'), file=sys.stderr)
    raise SystemExit(1)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def resources(prefix: str) -> dict[str, int]:
    return {p.name: p.stat().st_size for p in Path('/dev/shm').iterdir()
            if p.name.startswith(prefix) or p.name.startswith('.' + prefix)}


def spawn(command: list[str], env: dict[str, str], log: Path, *, stdin=False):
    process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               stdin=subprocess.PIPE if stdin else subprocess.DEVNULL,
                               text=True, start_new_session=True, cwd=log.parent)
    def save_output():
        with log.open('w') as stream:
            stream.write(json.dumps({'command': [redact_text(arg) for arg in command]}) + '\n')
            for line in process.stdout:
                stream.write(redact_text(line)); stream.flush()
    writer = threading.Thread(target=save_output, daemon=True)
    writer.start()
    return process, writer


def stop(process, writer, *, owner=False) -> dict:
    forced = False
    if process.poll() is None:
        if owner:
            try: process.stdin.write('\n'); process.stdin.flush()
            except (BrokenPipeError, OSError): pass
        else:
            process.send_signal(signal.SIGTERM)
        try: process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            forced = True
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
    writer.join(timeout=5)
    return {'returncode': process.returncode, 'forced': forced}


def check_runtime(root: Path, runtime: str, repeats: int) -> dict:
    token = uuid.uuid4().hex[:12]
    prefix = 'imlane-fixture-' + token
    out = root / 'artifacts' / (runtime + '-' + token)
    out.mkdir(parents=True)
    binary_prefix = root / 'prefix-patched'
    python = root / 'venv/bin/python'
    info = json.loads(subprocess.check_output([str(python), '-c',
        'import sys,sysconfig,pyarrow,json; print(json.dumps(dict(site=sysconfig.get_path("purelib"),'
        'lib=pyarrow.get_library_dirs()[0],base=sys.base_prefix)))'], text=True))
    cpp_runtime = Path(subprocess.check_output(
        ['g++', '-print-file-name=libstdc++.so.6'], text=True).strip()).resolve()
    env = dict(os.environ)
    env.update(CUDA_VISIBLE_DEVICES='', OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1',
               PATH=str(python.parent) + os.pathsep + env['PATH'],
               PYTHONPATH=info['site'] + os.pathsep + str(OWNED),
               LD_LIBRARY_PATH=os.pathsep.join([str(binary_prefix / 'lib'), info['lib'],
                   str(root / 'source/third_party/fmt/dist/lib'),
                   str(root / 'source/third_party/boost/dist/lib'), str(cpp_runtime.parent), info['base'] + '/lib']),
               TMPDIR=str(root / 'tmp'), IMLANE_NAMESPACE=prefix,
               IMLANE_GATEWAY_HOST='127.0.0.1',
               RAY_DEDUP_LOGS='0', RAY_USAGE_STATS_ENABLED='0')
    env.pop('PYTHONHOME', None)
    env.pop('RAY_ADDRESS', None)
    service = owner = fixture = None
    service_writer = owner_writer = None
    port = free_port()
    report = {'runtime': runtime, 'namespace': prefix, 'gateway_host': '127.0.0.1',
              'repeats': repeats, 'real_model_requests': 0, 'runs': [],
              'resources_before': resources(prefix), 'rss_peak_bytes': 0,
              'shared_memory_peak_bytes': 0, 'processes_seen': {},
              'cpu_affinity': sorted(os.sched_getaffinity(0)), 'cuda_visible_devices': '',
              'ray_object_store_capacity_bytes': 134217728 if runtime == 'ray' else None}
    report['cpp_runtime'] = {'path': str(cpp_runtime), 'sha256': digest(cpp_runtime)}
    monitored = threading.Event()
    owned_processes = {}
    def sample():
        while not monitored.wait(0.1):
            rss = 0
            for base in (service, owner, fixture):
                if base is None: continue
                try: processes = [psutil.Process(base.pid), *psutil.Process(base.pid).children(recursive=True)]
                except psutil.Error: continue
                for proc in processes:
                    try:
                        created = proc.create_time()
                        owned_processes[proc.pid] = created
                        rss += proc.memory_info().rss
                    except psutil.Error: pass
            report['rss_peak_bytes'] = max(report['rss_peak_bytes'], rss)
            report['shared_memory_peak_bytes'] = max(report['shared_memory_peak_bytes'], sum(resources(prefix).values()))
    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    try:
        command = [str(binary_prefix / 'bin/imlane'), '--working_dir', str(out / 'state'),
                   '--n_executors', '2', '--runtime', runtime, '--gateway_port', str(port)]
        if runtime == 'ray':
            ray_temp = root / 'ray' / token
            ray_temp.mkdir(parents=True)
            short = Path('/root/autodl-tmp') / ('i-r-' + token)
            short.symlink_to(ray_temp, target_is_directory=True)
            address_file = out / 'ray-address.json'
            owner_code = (
                'import sys,ray,json; '
                'ctx=ray.init(address="local",num_cpus=2,num_gpus=0,include_dashboard=False,'
                'object_store_memory=134217728,_node_ip_address="127.0.0.1",_temp_dir=sys.argv[1]); '
                'open(sys.argv[2],"w").write(json.dumps(dict(address=ctx.address_info["gcs_address"]))); '
                'sys.stdin.readline(); ray.shutdown()')
            owner, owner_writer = spawn([str(python), '-c', owner_code, str(short), str(address_file)], env, out / 'ray-owner.log', stdin=True)
            until = time.monotonic() + 60
            while not address_file.exists():
                if owner.poll() is not None: raise RuntimeError('owned Ray cluster exited before ready')
                if time.monotonic() > until: raise TimeoutError('owned Ray cluster startup exceeded 60 seconds')
                time.sleep(0.1)
            command += ['--ray_address', json.loads(address_file.read_text())['address']]
            report['ray_temp_link'] = str(short)
        service, service_writer = spawn(command, env, out / 'coordinator.log')
        until = time.monotonic() + 60
        while True:
            if service.poll() is not None: raise RuntimeError('IMLane Coordinator exited before ready')
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=0.1): break
            except OSError:
                if time.monotonic() > until: raise TimeoutError('IMLane Coordinator startup exceeded 60 seconds')
                time.sleep(0.1)
        report['resources_ready'] = resources(prefix)
        for repeat in range(repeats):
            started = time.monotonic()
            try:
                fixture = subprocess.Popen([str(binary_prefix / 'bin/imlane_fixture_cpp'), str(port),
                    str(OWNED / 'fixture_udf.py')], env=env, text=True, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, start_new_session=True, cwd=out)
                stdout, _ = fixture.communicate(timeout=120)
                output = redact_text(stdout)
                (out / f'fixture-{repeat}.log').write_text(output)
                if fixture.returncode: raise RuntimeError(f'fixture exited with status {fixture.returncode}')
                result = json.loads(output.strip().splitlines()[-1])
                if not result['passed']: raise RuntimeError('fixture reported a failed check')
                parents = {service.pid, owner.pid if owner is not None else service.pid}
                for pid in result['worker_pids']:
                    proc = psutil.Process(pid)
                    if pid in {fixture.pid, service.pid} or not parents.intersection(p.pid for p in proc.parents()):
                        raise RuntimeError('Arrow UDF ran outside the owned backend processes')
                    owned_processes[pid] = proc.create_time()
                result['backend_process_identity_validated'] = True
                report['runs'].append({**result, 'fixture_seconds': time.monotonic() - started})
                if report['rss_peak_bytes'] > 8 * 1024 ** 3: raise RuntimeError('fixture exceeded the 8 GiB RSS allowance')
                if report['shared_memory_peak_bytes'] > 2 * 1024 ** 3: raise RuntimeError('fixture exceeded the 2 GiB shared-memory allowance')
            except subprocess.TimeoutExpired as error:
                os.killpg(fixture.pid, signal.SIGKILL)
                fixture.wait(timeout=5)
                partial = error.stdout or b''
                if isinstance(partial, bytes): partial = partial.decode(errors='replace')
                (out / f'fixture-{repeat}.log').write_text(redact_text(partial) + '\nFixture exceeded 120 seconds.\n')
                raise TimeoutError('fixture exceeded 120 seconds') from None
        report['normal_checks_passed'] = True
    except Exception as error:
        report['normal_checks_passed'] = False
        report['error'] = redact_text(f'{type(error).__name__}: {error}')
    finally:
        if service is not None: report['coordinator_shutdown'] = stop(service, service_writer)
        if owner is not None: report['ray_owner_shutdown'] = stop(owner, owner_writer, owner=True)
        time.sleep(0.5)
        monitored.set(); sampler.join(timeout=2)
        survivors = []
        for pid, created in owned_processes.items():
            try:
                proc = psutil.Process(pid)
                if proc.create_time() == created and proc.status() != psutil.STATUS_ZOMBIE:
                    survivors.append(pid)
            except psutil.Error: pass
        report['processes_seen'] = {str(pid): created for pid, created in owned_processes.items()}
        report['owned_survivors'] = survivors
        report['resources_after'] = resources(prefix)
        report['passed'] = bool(report['normal_checks_passed'] and not survivors and not report['resources_after']
            and not report.get('coordinator_shutdown', {}).get('forced', True)
            and report.get('coordinator_shutdown', {}).get('returncode') == 0
            and not report.get('ray_owner_shutdown', {}).get('forced', False)
            and report.get('ray_owner_shutdown', {}).get('returncode', 0) == 0)
        if survivors:
            for pid in survivors:
                try:
                    proc = psutil.Process(pid)
                    if proc.create_time() == owned_processes[pid]: proc.terminate()
                except psutil.Error: pass
        # Only this test's known namespace may be removed after failed author cleanup.
        if report['resources_after']:
            for name in report['resources_after']:
                (Path('/dev/shm') / name).unlink(missing_ok=True)
        report['resources_after_owned_cleanup'] = resources(prefix)
        (out / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
        report['artifact'] = out.name
        report['result_sha256'] = digest(out / 'result.json')
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--runtime', choices=('cpython', 'ray', 'both'), default='both')
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args(argv)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    if not 1 <= args.repeats <= 4: parser.error('--repeats must be between 1 and 4')
    root = args.root.resolve()
    if root.is_relative_to(OWNED.parents[2]): parser.error('--root must be outside the repository')
    results = [check_runtime(root, runtime, args.repeats) for runtime in
               (('cpython', 'ray') if args.runtime == 'both' else (args.runtime,))]
    summary = {'passed': all(item['passed'] for item in results), 'runtimes': results,
               'real_model_requests': 0, 'owned_sha256': {p.name: digest(p) for p in OWNED.iterdir() if p.is_file()}}
    path = root / 'artifacts' / ('verification-' + str(time.time_ns()) + '.json')
    path.write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps({'passed': summary['passed'], 'results': [dict(runtime=x['runtime'], passed=x['passed'],
          normal_checks_passed=x['normal_checks_passed'], artifact=x['artifact'], error=x.get('error')) for x in results],
          'summary': path.name, 'real_model_requests': 0}))
    return 0 if summary['passed'] else 1


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as error:
        print(redact_text(f'{type(error).__name__}: {error}'), file=sys.stderr)
        raise SystemExit(1)
