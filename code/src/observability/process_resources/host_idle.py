"""Host resource boundaries, distinct from the mere presence of Python.

The caller owns the host lease and observed-child cleanup. This reader neither
signals processes nor proves CPU isolation. Unclassified Python is retained as
evidence and prevents time-window-only attribution of leftover shared memory.
"""
import csv
import hashlib
import math
import os
from pathlib import Path
import re
import subprocess
import time


SCHEMA = 'semloom.host_resource_boundary.v1'


def _python_entrypoint(argv):
    """Read the executed module/script, never script arguments or inline code."""
    args = iter(argv[1:])
    for arg in args:
        if arg == '-c':
            return None
        if arg == '-m':
            return next(args, None)
        if arg in ('-W', '-X', '--check-hash-based-pycs'):
            next(args, None)
        elif not arg.startswith('-'):
            return arg
    return None


def _native_workload(name, argv):
    executable = Path(argv[0]).name.lower() if argv else ''
    if any(marker in identity for identity in (name.lower(), executable)
           for marker in ('ray::', 'raylet', 'gcs_server', 'vllm', 'imlane')):
        return 'native workload executable'
    if 'python' not in name.lower() and 'python' not in executable:
        return None
    entrypoint = _python_entrypoint(argv)
    if entrypoint is None:
        return None
    if entrypoint in ('ray', 'vllm') or entrypoint.startswith(
            ('ray.', 'vllm.', 'src.experiments.', 'integrations.imlane')):
        return 'native workload module'
    if re.fullmatch(r'(?:run_(?:sembench|kalypso|native|imlane|movie_records)[\w]*|'
                    r'qualification_(?:unit|controller)|native_server)\.py', Path(entrypoint).name):
        return 'native workload script'
    return None


def gpu_snapshot(cfg):
    def query(fields, selector):
        result = subprocess.run(
            ['nvidia-smi', selector + '=' + fields, '--format=csv,noheader,nounits'],
            check=True, capture_output=True, text=True, timeout=10)
        return list(csv.reader(result.stdout.strip().splitlines(), skipinitialspace=True))
    gpus = query('index,name,uuid,memory.total,memory.used', '--query-gpu')
    if len(gpus) != 1 or len(gpus[0]) != 5:
        raise RuntimeError('GPU inventory is unavailable or no longer single-device')
    index, name, gpu_uuid, total, used = [value.strip() for value in gpus[0]]
    if (index != '0' or name != cfg['gpu_expected_name']
            or int(total) != cfg['gpu_expected_total_mib']):
        raise RuntimeError('GPU hardware differs from the frozen qualification identity')
    compute = []
    for values in query('gpu_uuid,pid,used_gpu_memory', '--query-compute-apps'):
        if len(values) != 3:
            raise RuntimeError('NVIDIA compute inventory has an unsupported row')
        device, pid, memory = [value.strip() for value in values]
        compute.append(dict(gpu_uuid=device, pid=int(pid), used_memory_mib=int(memory)))
    return dict(index=0, name=name, uuid=gpu_uuid, total_mib=int(total), used_mib=int(used),
                compute=compute, compute_inventory_complete=True)


def snapshot(api, cfg, label, owned=()):
    psutil = api.psutil
    own_process = psutil.Process(os.getpid())
    excluded = {(own_process.pid, own_process.create_time()), *owned}
    excluded.update((item.pid, item.create_time()) for item in own_process.parents())
    allowed = {(item['pid'], item['create_time']): item['cmdline_sha256']
               for item in cfg.get('allowed_idle_processes', [])}
    observed_allowed, rows, foreign, unclassified, errors = set(), [], [], [], []
    for process in psutil.process_iter(['pid', 'create_time', 'name', 'cmdline', 'status']):
        try:
            info = process.info
            pid, created, name, argv = (info[k] for k in ('pid', 'create_time', 'name', 'cmdline'))
            if (type(pid) is not int or type(created) not in (int, float)
                    or not math.isfinite(created) or type(name) is not str or type(argv) is not list
                    or type(info['status']) is not str):
                raise RuntimeError('incomplete psutil process identity')
            key = (pid, created)
            cmd_sha = hashlib.sha256(' '.join(argv).encode('utf-8')).hexdigest()
            row = dict(pid=pid, create_time=created, name=name, cmdline_sha256=cmd_sha,
                       status=info['status'])
            rows.append(row)
            if key in allowed and cmd_sha == allowed[key]:
                observed_allowed.add(key)
                continue
            if key in excluded or info['status'] in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD):
                continue
            reason = _native_workload(name, argv)
            if reason:
                foreign.append(dict(row, reason=reason))
            elif 'python' in name.lower() or (argv and 'python' in Path(argv[0]).name.lower()):
                unclassified.append(row)
        except psutil.NoSuchProcess:
            continue
        except Exception as error:
            # Identity details may contain private argv; retain type and failed PID only.
            errors.append(dict(error_type=type(error).__name__, pid=process.pid))
    return dict(schema=SCHEMA, label=label, captured_epoch_s=time.time(),
                policy_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                inventory_complete=not errors, inventory_errors=errors, processes=rows,
                foreign_work=foreign, unclassified_python=unclassified,
                idle_allowlist_unverified=[dict(pid=p, create_time=c)
                    for p, c in sorted(set(allowed) - observed_allowed)],
                cmdline_digest_format='space-joined-argv-v1', gpu=gpu_snapshot(cfg),
                scope='resource boundary only; unclassified Python impact and CPU isolation unproven')


def assert_idle(value, cfg):
    if value.get('schema') != SCHEMA:
        raise RuntimeError('host resource snapshot policy is missing or historical')
    if not value['inventory_complete'] or value['idle_allowlist_unverified']:
        raise RuntimeError('process inventory or exact idle-service identity is unconfirmed')
    if value['foreign_work']:
        raise RuntimeError('native workload process remains; nothing was signalled')
    gpu = value['gpu']
    if not gpu['compute_inventory_complete'] or gpu['compute']:
        raise RuntimeError('GPU compute processes remain; nothing unknown may be signalled')
    if gpu['used_mib'] > cfg['gpu_idle_max_mib']:
        raise RuntimeError('GPU memory has not returned to the configured idle baseline')
