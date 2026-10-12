"""Deterministic real startup-function replay; no SDK, network, model or GPU."""
import argparse
import ast
import json
import os
import tempfile
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace


def load_functions(path, names, namespace):
    tree = ast.parse(path.read_text())
    selected = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert {node.name for node in selected} == names
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), 'exec'), namespace)


def replay(ledger, support, writer_shape):
    with tempfile.TemporaryDirectory(prefix='bootstrap-replay-') as directory:
        root = Path(directory)
        (root / 'bootstrap').mkdir()
        marker = root / 'bootstrap' / f'{os.getpid()}.json'
        release = marker.with_name(f'{os.getpid()}.release.json')
        expected = dict(pid=os.getpid(), start_ticks=123, helper_identity='fixture-identity', installed=True, hooks=['aiohttp'])
        marker.write_text(json.dumps(expected) + '\n')
        namespace = dict(ROOT=root, Path=Path, json=json, os=os, time=time, uuid=uuid, harness_identity=lambda: 'fixture-identity', start_ticks=lambda pid: 123, assert_hooks=lambda: None)
        load_functions(support, {'bootstrap', 'release_cli', 'save'}, namespace)
        names = {'await_cli_release'}
        if '_write_bootstrap' in ledger.read_text():
            names.add('_write_bootstrap')
        load_functions(ledger, names, namespace)
        writer_paused, continue_writer, reader_done = threading.Event(), threading.Event(), threading.Event()
        output, writer_error = {}, []
        original = Path.write_text

        def paused_write(path, text, *args, **kwargs):
            if path.parent == marker.parent and path.name.startswith(marker.name):
                with path.open('w', encoding='utf-8') as handle:
                    if writer_shape == 'truncated':
                        handle.write(text[:len(text) // 2])
                        handle.flush()
                    writer_paused.set()
                    assert continue_writer.wait(2), 'controlled writer did not resume'
                    if writer_shape == 'truncated':
                        handle.write(text[len(text) // 2:])
                    else:
                        handle.write(text)
                return len(text)
            return original(path, text, *args, **kwargs)

        def write_child_marker():
            try:
                namespace['await_cli_release']()
            except BaseException as error:
                writer_error.append(type(error).__name__ + ': ' + str(error))

        def read_parent_marker():
            try:
                value = namespace['release_cli'](SimpleNamespace(pid=os.getpid(), poll=lambda: None), time.monotonic() + 2)
                output['released_waiting_marker'] = value.get('cli_waiting') is True
            except BaseException as error:
                output.update(error_type=type(error).__name__, error=str(error))
            finally:
                reader_done.set()

        Path.write_text = paused_write
        writer = threading.Thread(target=write_child_marker)
        reader = threading.Thread(target=read_parent_marker)
        try:
            writer.start()
            assert writer_paused.wait(2), 'actual marker write not exercised'
            output['target_bytes_during_write'] = len(marker.read_bytes())
            reader.start()
            output['reader_finished_while_writer_paused'] = reader_done.wait(.15)
            continue_writer.set()
            reader.join(2)
            if 'error_type' in output:
                namespace['save'](release, dict(pid=os.getpid(), start_ticks=123, helper_identity='fixture-identity'))
            writer.join(2)
            assert not reader.is_alive() and not writer.is_alive(), 'bounded startup replay did not settle'
            assert not writer_error, writer_error
        finally:
            continue_writer.set()
            Path.write_text = original
        output.update(writer_shape=writer_shape, model_calls=0, http_posts=0, gpu=0)
        return output


def refusals(support):
    rows = []
    for case in ('dead-child', 'wrong-helper', 'wrong-birth', 'persistent-bad-json'):
        with tempfile.TemporaryDirectory(prefix='bootstrap-refusal-') as directory:
            root = Path(directory)
            (root / 'bootstrap').mkdir()
            marker = root / 'bootstrap' / f'{os.getpid()}.json'
            value = dict(pid=os.getpid(), start_ticks=123, helper_identity='fixture-identity', installed=True, hooks=['aiohttp'], cli_waiting=True)
            if case == 'wrong-helper': value['helper_identity'] = 'wrong'
            if case == 'wrong-birth': value['start_ticks'] = 456
            marker.write_text('{' if case == 'persistent-bad-json' else json.dumps(value))
            namespace = dict(ROOT=root, Path=Path, json=json, time=time, harness_identity=lambda: 'fixture-identity', start_ticks=lambda pid: 123)
            load_functions(support, {'bootstrap', 'release_cli', 'save'}, namespace)
            error_type = None
            try:
                namespace['release_cli'](SimpleNamespace(pid=os.getpid(), poll=lambda: 1 if case == 'dead-child' else None), time.monotonic() + 1)
            except (RuntimeError, json.JSONDecodeError) as error:
                error_type = type(error).__name__
            assert error_type and not marker.with_name(f'{os.getpid()}.release.json').exists()
            rows.append(dict(case=case, rejected=True, error_type=error_type))
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ledger', type=Path, required=True)
    parser.add_argument('--support', type=Path, required=True)
    parser.add_argument('--expected', choices=['old-fails', 'fixed-passes'], required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    started = time.monotonic()
    rows = [replay(args.ledger, args.support, shape) for shape in ('empty', 'truncated') for _ in range(3)]
    if args.expected == 'old-fails':
        passed = all(row.get('error_type') == 'JSONDecodeError' and row['reader_finished_while_writer_paused'] for row in rows)
    else:
        passed = all(row.get('released_waiting_marker') and not row['reader_finished_while_writer_paused'] for row in rows)
    result = dict(passed=passed, expected=args.expected, rows=rows, refusals=refusals(args.support), elapsed_seconds=time.monotonic()-started, model_calls=0, http_posts=0, gpu=0, seam='actual await_cli_release and release_cli functions; controlled pause after write-open/truncate or partial-write; real filesystem', historical_root_cause_scope='reproduced source mechanism; old supervisor did not save read bytes or full traceback, so exact original read remains an inference')
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))
    raise SystemExit(not passed)


if __name__ == '__main__': main()
