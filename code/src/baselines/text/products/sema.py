"""Pinned author binary executing its own SQL semantic projection."""
import csv
from contextlib import contextmanager
import hashlib
import io
import os
from pathlib import Path
import selectors
import subprocess
import threading
import time
import uuid

from src.baselines.common.private_artifacts import open_private_text
from src.baselines.common.redact import redact_text

UPSTREAM_COMMIT = '3f2c7182bdaa26c1e8925f486585da25337e687e'
BINARY_SHA256 = '15a534667a668a152524a8756fe5d66c0fa9c9822e73d26029e1a71693a56ec3'


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def projection_instruction(instruction):
    # The author API parses a JSON scalar. Make that serialization visible in
    # its SQL instruction instead of rewriting requests or repairing outputs.
    return (instruction + '\nEncode the selected label as one JSON string in double quotes, '
            'for example "POSITIVE" or "NEGATIVE". Do not output a JSON object or any other text.'
            '\nInput: {review_text}')


def run_projection(values, plan, model, *, binary, root, num_threads=8):
    binary, root = Path(binary), Path(root)
    if hashlib.sha256(binary.read_bytes()).hexdigest() != BINARY_SHA256:
        raise ValueError('Sema binary differs from the pinned author artifact')
    source = root / 'sema-source.csv'
    with open_private_text(source, newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(('row_id', 'review_text'))
        writer.writerows((v['source_example_id'], v['input_text']) for v in values)
    instruction = projection_instruction(plan.instruction)
    statement = '\n'.join((
        'SET llm_url=' + literal(model.endpoint_url) + ';',
        'SET llm_model=' + literal(model.model_id) + ';',
        'SET llm_api_key=' + literal(model.bearer_token or 'local-fixture') + ';',
        'SET threads=' + str(num_threads) + ';',
        'SET semantic_batch_size=1;',
        'CREATE TABLE input(row_id VARCHAR, review_text VARCHAR);',
        'COPY input FROM ' + literal(str(source)) + ' (HEADER, DELIMITER \',\');',
        'SELECT row_id,s' + literal(instruction) + ' AS output FROM input;'))
    # SQL travels over stdin so endpoint credentials never enter process arguments.
    try:
        result = subprocess.run([str(binary), '-csv', '-noheader'], input=statement,
            capture_output=True, text=True, timeout=model.timeout_ms / 1000 + 10)
    except subprocess.TimeoutExpired as error:
        for name, value in (('stdout', error.stdout), ('stderr', error.stderr)):
            with open_private_text(root / ('sema-' + name + '.txt')) as stream:
                text = value.decode(errors='replace') if isinstance(value, bytes) else value or ''
                stream.write(redact_text(text))
        raise
    with open_private_text(root / 'sema-stdout.txt') as stream:
        stream.write(result.stdout)
    with open_private_text(root / 'sema-stderr.txt') as stream:
        stream.write(redact_text(result.stderr))
    if result.returncode:
        raise RuntimeError('native Sema projection failed; exit=' + str(result.returncode))
    rows = list(csv.reader(io.StringIO(result.stdout)))
    if any(len(row) != 2 for row in rows):
        raise ValueError('native Sema projection returned an unexpected CSV schema')
    yield from rows


class _PreparedProjection:
    """Own one author CLI session; SQL execution stays inside the author binary."""

    completion_protocol = 'native_select_followed_by_three_column_sql_marker'

    def __init__(self, binary, root, statement, timeout_seconds):
        self._root = root
        self._statement = statement
        self._timeout_seconds = timeout_seconds
        self._buffer = b''
        self._stdout = []
        self._stderr = []
        self._stdout_eof = False
        self._ready = False
        self._active = False
        self._closed = False
        self._cancelled = threading.Event()
        self._terminate_lock = threading.Lock()
        self._process = subprocess.Popen(
            [str(binary), '-csv', '-noheader', '-batch', '-bail'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self._selector = selectors.DefaultSelector()
        self._selector.register(self._process.stdout, selectors.EVENT_READ, 'stdout')
        self._selector.register(self._process.stderr, selectors.EVENT_READ, 'stderr')

    @property
    def pid(self):
        return self._process.pid

    @property
    def returncode(self):
        return self._process.poll()

    def _send(self, statement):
        if self._cancelled.is_set():
            raise RuntimeError('native Sema session was canceled')
        if self._closed or self._process.poll() is not None:
            raise RuntimeError('native Sema session has already stopped')
        self._process.stdin.write((statement + '\n').encode('utf-8'))
        self._process.stdin.flush()

    def _read_available(self, timeout):
        for key, _ in self._selector.select(timeout):
            chunk = os.read(key.fileobj.fileno(), 65536)
            if not chunk:
                self._selector.unregister(key.fileobj)
                if key.data == 'stdout':
                    self._stdout_eof = True
            elif key.data == 'stdout':
                self._stdout.append(chunk)
                self._buffer += chunk
            else:
                self._stderr.append(chunk)

    def _lines(self, deadline):
        while True:
            if self._cancelled.is_set():
                raise RuntimeError('native Sema statement was canceled')
            if b'\n' in self._buffer:
                line, self._buffer = self._buffer.split(b'\n', 1)
                yield (line + b'\n').decode('utf-8')
            elif self._stdout_eof:
                if self._buffer:
                    line, self._buffer = self._buffer, b''
                    yield line.decode('utf-8')
                code = self._process.wait(timeout=1)
                raise RuntimeError('native Sema statement ended before its completion marker; exit=' + str(code))
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('native Sema statement exceeded its completion deadline')
                self._read_available(remaining)

    @staticmethod
    def _marker(value):
        return ('__sema_complete__', uuid.uuid4().hex, str(value))

    @staticmethod
    def _marker_select(marker):
        return 'SELECT ' + ','.join(literal(value) for value in marker) + ';'

    def prepare(self, statement, row_count):
        if self._active or self._closed or self._cancelled.is_set():
            raise RuntimeError('native Sema cannot prepare input during execution or after stopping')
        marker = self._marker(row_count)
        # COUNT belongs to preparation and proves that COPY finished before entry.
        ready = ('SELECT ' + literal(marker[0]) + ',' + literal(marker[1]) +
                 ',CAST(COUNT(*) AS VARCHAR) FROM input;')
        self._send(statement + '\n' + ready)
        reader = csv.reader(self._lines(time.monotonic() + self._timeout_seconds))
        for row in reader:
            if tuple(row) != marker:
                raise ValueError('native Sema preparation returned an unexpected readiness record')
            self._ready = True
            return

    def replace_source(self, values, root, endpoint_url):
        """Replace the raw relation and observer URL in this author SQL session."""
        values = tuple(values)
        source = Path(root) / 'sema-source.csv'
        with open_private_text(source, newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(('row_id', 'review_text'))
            writer.writerows((v['source_example_id'], v['input_text']) for v in values)
        setup = ('SET llm_url=' + literal(endpoint_url) + ';\nDELETE FROM input;\nCOPY input FROM '
                 + literal(str(source)) + " (HEADER, DELIMITER ',');")
        self.prepare(setup, len(values))

    def execute(self):
        """Submit one native SELECT and stop only after its statement marker."""
        if not self._ready or self._closed:
            raise RuntimeError('native Sema session is not ready')
        if self._active:
            raise RuntimeError('native Sema already has an active projection')
        self._active = True
        completed = False
        try:
            marker = self._marker('query')
            self._send(self._statement + '\n' + self._marker_select(marker))
            reader = csv.reader(self._lines(time.monotonic() + self._timeout_seconds))
            for row in reader:
                if tuple(row) == marker:
                    completed = True
                    return
                if len(row) != 2:
                    raise ValueError('native Sema projection returned an unexpected CSV schema')
                yield row[0], row[1]
        finally:
            if not completed:
                self.close()
            self._active = False

    def _terminate(self):
        with self._terminate_lock:
            if self._process.poll() is None:
                try:
                    self._process.terminate()
                except ProcessLookupError:
                    pass

    def cancel(self):
        """Signal the owned child from an HTTP observer without joining its reader.

        The author may retry HTTP errors without raising SQL errors. The caller
        owns HTTP failure observation; cancellation preserves the native parser.
        """
        self._cancelled.set()
        self._terminate()

    def close(self):
        if self._closed:
            return
        self._closed = True
        process = self._process
        if (self._active or not self._ready) and process.poll() is None:
            self._terminate()
        try:
            process.stdin.close()
        except BrokenPipeError:
            pass
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            self._terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
        # Drain both pipes after exit so errors and partial output are retained.
        self._stdout.append(process.stdout.read())
        self._stderr.append(process.stderr.read())
        self._selector.close()
        process.stdout.close()
        process.stderr.close()
        with open_private_text(self._root / 'sema-prepared-stdout.txt') as stream:
            stream.write(b''.join(self._stdout).decode('utf-8', errors='replace'))
        with open_private_text(self._root / 'sema-prepared-stderr.txt') as stream:
            stream.write(redact_text(b''.join(self._stderr).decode('utf-8', errors='replace')))


@contextmanager
def prepare_projection(values, plan, model, *, binary, root, num_threads=8):
    """Import the bounded input before yielding a reusable native SQL session.

    Each execute iterator submits SQL when consumption begins. Its final normal
    SELECT marker confirms that all preceding output has reached the consumer.
    """
    binary, root = Path(binary), Path(root)
    if hashlib.sha256(binary.read_bytes()).hexdigest() != BINARY_SHA256:
        raise ValueError('Sema binary differs from the pinned author artifact')
    if type(num_threads) is not int or num_threads < 1 or model.timeout_ms <= 0:
        raise ValueError('native Sema requires positive threads and timeout')
    values = tuple(values)
    source = root / 'sema-source.csv'
    with open_private_text(source, newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(('row_id', 'review_text'))
        writer.writerows((v['source_example_id'], v['input_text']) for v in values)
    setup = '\n'.join((
        'SET llm_url=' + literal(model.endpoint_url) + ';',
        'SET llm_model=' + literal(model.model_id) + ';',
        'SET llm_api_key=' + literal(model.bearer_token or 'local-fixture') + ';',
        'SET threads=' + str(num_threads) + ';',
        'SET semantic_batch_size=1;',
        'CREATE TABLE input(row_id VARCHAR, review_text VARCHAR);',
        'COPY input FROM ' + literal(str(source)) + ' (HEADER, DELIMITER \',\');'))
    statement = ('SELECT row_id,s' + literal(projection_instruction(plan.instruction)) +
                 ' AS output FROM input;')
    session = _PreparedProjection(binary, root, statement, model.timeout_ms / 1000 + 10)
    try:
        session.prepare(setup, len(values))
        yield session
    finally:
        session.close()
