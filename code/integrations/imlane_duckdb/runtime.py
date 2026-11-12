"""Own only the author coordinator/executors for native DuckDB Q3.

The caller holds the host lease and owns the DuckDB query/deadline. Only process
identity, namespace and cleanup facilities are shared with the records owner;
this module never uses its records protocol or a SemLoom execution graph.
"""

import json
import math
import os
from pathlib import Path
import socket
import subprocess
import threading
import time

from integrations.imlane_records import owner
from src.infrastructure.private_artifacts import file_identity, require_outside_git
from src.infrastructure.redact import redact_text

CODE = Path(__file__).resolve().parents[2]
PORT = 7086
_PROBE = (
    "import json,sys,sysconfig,pyarrow;print(json.dumps(dict("
    "executable=sys.executable,version=list(sys.version_info[:3]),base=sys.base_prefix,"
    "site=sysconfig.get_path('purelib'),arrow=pyarrow.__version__,"
    "arrow_library=pyarrow.get_library_dirs()[0])))"
)


def _python_environment(python):
    return json.loads(subprocess.check_output(
        [str(python), '-I', '-B', '-c', _PROBE], text=True, timeout=15))


def _check_port():
    # REUSEADDR allows a previous closed TCP connection's TIME_WAIT, while
    # listen still rejects an existing coordinator. The caller's lease must
    # cover this check and the subsequent author process launch.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind(('127.0.0.1', PORT))
            probe.listen(1)
        except OSError as error:
            raise RuntimeError('author DuckDB requires an unused default coordinator port 7086') from error


class DuckDBScope(owner.AuthorScope):
    def __init__(self, args, report, *, kernel):
        require_outside_git(Path(args.output))
        report.setdefault('cleanup', [])
        super().__init__(args, 'duckdb-cpython', owner, report, kernel=Path(kernel).resolve())
        self.author = Path(args.author_root).resolve()
        self.port = PORT
        self._failure = None
        self.state = dict(namespace=self.namespace, port=PORT, runtime='cpython', status='preparing')
        report.setdefault('coordinator_scopes', []).append(self.state)

    def _environment(self):
        author = self.author
        # Keep the venv invocation path: resolving its python symlink would
        # silently run the base interpreter and lose the selected environment.
        python = Path(self.args.method_python).absolute()
        author_python = author / 'venv/bin/python'
        author_info, method_info = (_python_environment(value) for value in (author_python, python))
        if (author_info['arrow'] != '16.1.0' or method_info['arrow'] != '16.1.0'
                or author_info['version'][:2] != method_info['version'][:2]
                or Path(author_info['base']).resolve() != Path(method_info['base']).resolve()):
            raise ValueError('native CPython UDF must share the author Python ABI and Arrow 16.1 environment')
        identity_path = author / 'artifacts/build-patched/build-identity.json'
        identity = json.loads(identity_path.read_text())
        head = subprocess.check_output(
            ['git', '-C', str(author / 'source'), 'rev-parse', 'HEAD'], text=True, timeout=15).strip()
        if head != owner.AUTHOR or identity['upstream_commit'] != owner.AUTHOR or identity['mode'] != 'patched':
            raise ValueError('native DuckDB scope requires the declared patched 072f author environment')
        cpp = Path(subprocess.check_output(
            ['g++', '-print-file-name=libstdc++.so.6'], text=True, timeout=15).strip()).resolve()
        temporary = self.root / 'tmp'
        temporary.mkdir()
        paths = [str(self.kernel.resolve().parent), str(CODE), method_info['site']]
        if os.environ.get('PYTHONPATH'):
            paths.append(os.environ['PYTHONPATH'])
        libraries = [str(cpp.parent), str(author / 'prefix-patched/lib'), method_info['arrow_library'],
            str(author / 'source/third_party/nanoarrow/dist/lib'),
            str(author / 'source/third_party/fmt/dist/lib'),
            str(author / 'source/third_party/boost/dist/lib'), str(Path(method_info['base']) / 'lib')]
        if getattr(self.args, 'duckdb_library', None) is not None:
            libraries.insert(0, str(Path(self.args.duckdb_library).resolve().parent))
        if os.environ.get('LD_LIBRARY_PATH'):
            libraries.append(os.environ['LD_LIBRARY_PATH'])
        env = dict(os.environ, CUDA_VISIBLE_DEVICES='', PYTHONDONTWRITEBYTECODE='1',
            PATH=str(python.parent) + os.pathsep + os.environ.get('PATH', ''),
            PYTHONPATH=os.pathsep.join(paths), LD_LIBRARY_PATH=os.pathsep.join(libraries),
            TMPDIR=str(temporary), IMLANE_NAMESPACE=self.namespace, IMLANE_GATEWAY_HOST='127.0.0.1')
        env.update(self.args.workload_environment)
        env.pop('PYTHONHOME', None)
        env.pop('RAY_ADDRESS', None)
        self.state.update(author_commit=head, author_build_identity=file_identity(identity_path),
            author_python=author_info, method_python=method_info, kernel=file_identity(self.kernel),
            module_name=self.kernel.stem, cuda_visible_devices='',
            inherited_thread_environment={name: os.environ.get(name) for name in
                ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS')})
        return env

    def check(self):
        if self._failure is not None:
            raise self._failure
        self.args.stop_budget.check()
        if self.service is not None and self.service.poll() is not None:
            raise RuntimeError('author DuckDB coordinator exited: ' + str(self.service.returncode))

    def _sample(self):
        try:
            while not self.stop_sampler.wait(.1):
                self.snapshot()
                self.check()
        except BaseException as error:
            if self.stop_sampler.is_set():
                return
            self._failure = error
            self.state['monitor_error'] = dict(type=type(error).__name__, error=redact_text(str(error)))
            self.report.setdefault('sampler_errors', []).append(self.state['monitor_error'])
            self.args.stop_budget.stopped.set()
            try:
                self.state['abort_errors'] = self.abort_owned()
            except BaseException as cleanup:
                error.add_note('author abort also failed: ' + redact_text(str(cleanup)))

    def __enter__(self):
        try:
            self.args.stop_budget.check()
            count = getattr(self.args, 'executor_count', None)
            if count is not None and (type(count) is not int or count < 1):
                raise ValueError('executor_count must be an explicit positive integer')
            timeout = getattr(self.args, 'startup_timeout_s', 30)
            if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
                raise ValueError('coordinator startup timeout must be finite positive seconds')
            _check_port()
            self.env = self._environment()
            prefix = self.author / 'prefix-patched'
            command = [str(prefix / 'bin/imlane'), '--runtime', 'cpython',
                       '--working_dir', str(self.root / 'state')]
            if count is not None:
                command.extend(['--n_executors', str(count)])
            self.state.update(executor_count_declared=4 if count is None else count,
                executor_count_source='author_default' if count is None else 'explicit',
                startup_timeout_s=timeout, command=command)
            started = time.monotonic()
            self.service, self.writer = owner.spawn(command, self.env, self.root / 'coordinator.log')
            self.track_root(self.service)
            self.sampler = threading.Thread(target=self._sample, daemon=True)
            self.sampler.start()
            until = started + timeout
            while True:
                self.check()
                try:
                    with socket.create_connection(('127.0.0.1', PORT), timeout=.1):
                        break
                except OSError:
                    if time.monotonic() >= until:
                        raise TimeoutError('author DuckDB coordinator startup deadline exceeded')
                    time.sleep(.05)
            self.state.update(status='tcp_ready', startup_seconds=time.monotonic() - started,
                dbend_handshake='performed by the native DuckDB driver Setup, not this TCP probe')
            return self
        except BaseException as error:
            self.state.update(status='failed', error=redact_text(str(error)))
            try:
                self.close()
            except BaseException as cleanup:
                error.add_note('author startup cleanup also failed: ' + redact_text(str(cleanup)))
            raise

    def close(self):
        # Stop the liveness monitor before the inherited close deliberately
        # terminates the coordinator; the inherited final identity scan remains.
        self.stop_sampler.set()
        return super().close()
