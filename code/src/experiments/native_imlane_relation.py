"""Complete original relation sinks while the native IMLane author is owned.

Native drivers publish query.json atomically before their destructors. Consume
that relation immediately; process exit, receipt audit and author close remain
required for success, but do not define the full-query result-ready timestamp.
"""

from dataclasses import replace
import math
import os
from pathlib import Path
import time

from integrations.imlane_duckdb.runtime import DuckDBScope
from integrations.imlane_records import owner
from src.infrastructure.private_artifacts import open_private_text, read_json, write_private_json

ACK_PROTOCOL = 'semloom.imlane.relation_ack.v1'


def release_relation(path, native_ready_ns, sink_ready_ns, *, outcome):
    """Publish one complete or abort ACK; never reuse an earlier completion."""
    path = Path(path)
    pending = path.with_name(path.name + '.pending')
    with open_private_text(pending) as stream:
        stream.write(f'{ACK_PROTOCOL}\n{outcome} {native_ready_ns} {sink_ready_ns}\n')
    os.link(pending, path)
    pending.unlink()


def run_relation_query(args, report, *, kernel, command, window, query_started_ns,
                       sink, audit, check):
    """One native query under the existing outer lease and resource supervisor.

    ``sink(native)`` returns the complete original in-memory result, using only
    the native relation. ``audit(native, result)`` validates saved responses and
    quality afterwards. ``check`` observes the caller's existing model service;
    this function does not create a model, lease, scheduler or retry policy.
    """
    result_path = Path(args.output) / 'query.json'
    release_path = Path(args.output) / 'sink-release.ack'
    report['status'] = 'running'
    scope = None
    result = None
    try:
        if result_path.exists() or release_path.exists():
            raise ValueError('native relation query requires fresh relation and ACK paths')
        with DuckDBScope(args, report, kernel=kernel) as scope:
            handoff_ready, released, primary = None, False, None
            try:
                process, writer = owner.spawn(command, scope.env, Path(args.output) / 'query.log')
                scope.children.append((process, writer))
                scope.track_root(process)
                while not result_path.exists():
                    window.check()
                    scope.check()
                    check()
                    if process.poll() is not None:
                        raise RuntimeError('native driver exited before publishing a complete relation')
                    time.sleep(.05)
                native = read_json(result_path, max_bytes=result_path.stat().st_size)
                if native['status'] != 'query_returned':
                    raise RuntimeError('native driver published a failed query')
                native_ready = native['result_ready_monotonic_ns']
                if (native.get('result_clock') != 'CLOCK_MONOTONIC' or type(native_ready) is not int
                        or not query_started_ns <= native_ready <= time.monotonic_ns()):
                    raise ValueError('native relation has no matching monotonic readiness event')
                if native.get('sink_handoff') != dict(protocol=ACK_PROTOCOL, release_path=str(release_path)):
                    raise ValueError('driver must retain its DuckDB relation until the matching sink ACK')
                handoff_ready = native_ready
                report.update(query_started_monotonic_ns=query_started_ns,
                    native_relation_ready_monotonic_ns=native_ready)
                window.check()
                scope.check()
                check()
                if process.poll() is not None:
                    raise RuntimeError('native driver exited without waiting for the original sink')
                result = sink(native)
                ready = time.monotonic_ns()
                report['full_sink_observed_monotonic_ns'] = ready
                report['native_alive_after_sink'] = process.poll() is None
                if not report['native_alive_after_sink']:
                    raise RuntimeError('native driver exited before the original sink completed')
                window.accept_ready(ready, clock='CLOCK_MONOTONIC')
                report.update(result_ready_monotonic_ns=ready,
                    source_to_sink_seconds=(ready - query_started_ns) / 1e9,
                    result_clock='CLOCK_MONOTONIC',
                    result_scope='source start through complete original relation sink, including native '
                                 'relation publication/transport; receipt audit and close follow readiness')
                release_relation(release_path, native_ready, ready, outcome='complete')
                released = True
                report['sink_release'] = dict(outcome='complete', published_monotonic_ns=time.monotonic_ns())
                while process.poll() is None:
                    window.check()
                    scope.check()
                    check()
                    time.sleep(.05)
                writer.join(1)
                report['driver_returncode'] = process.returncode
                report['driver_exit_observed_monotonic_ns'] = time.monotonic_ns()
                if process.returncode:
                    raise RuntimeError('native driver failed after publishing its relation')
                report['audit_started_monotonic_ns'] = time.monotonic_ns()
                audited = audit(native, result)
                report['audit_finished_monotonic_ns'] = time.monotonic_ns()
                window.check()
                scope.check()
                check()
            except BaseException as error:
                primary = error
                raise
            finally:
                deadline = window.begin_cleanup()
                if handoff_ready is not None and not released:
                    try:
                        release_relation(release_path, handoff_ready, 0, outcome='abort')
                        report['sink_release'] = dict(outcome='abort', published_monotonic_ns=time.monotonic_ns())
                        process.wait(timeout=max(0.001, deadline - time.monotonic()))
                        writer.join(1)
                        report['driver_returncode'] = process.returncode
                        report['driver_exit_observed_monotonic_ns'] = time.monotonic_ns()
                    except BaseException as cleanup:
                        if primary is None:
                            raise
                        primary.add_note('relation abort/reap also failed: ' + type(cleanup).__name__)
        report['status'] = 'completed'
        return result, audited
    except BaseException as error:
        report['status'] = 'failed'
        error.query_report = report
        error.query_result = result
        raise
    finally:
        if scope is not None:
            report['author_cleanup'] = owner.resource_state(scope)


def persist_relation_observation(directory, report, result, *, primary=None):
    """Save the observed original sink and report after execution, even on failure.

    Call from the worker's finally block. A write failure cannot replace the
    query's primary error; saving an observed sink never promotes a failed query.
    """
    failure = primary
    names = ['author.json'] if result is None else ['full-sink-observed.json', 'author.json']
    for name in names:
        try:
            if name == 'author.json':
                payload = report
            else:
                frames = result if isinstance(result, dict) else {'result': result}
                payload = dict(status=report['status'],
                    full_sink_observed_monotonic_ns=report['full_sink_observed_monotonic_ns'],
                    result_ready_monotonic_ns=report.get('result_ready_monotonic_ns'),
                    native_alive_after_sink=report['native_alive_after_sink'],
                    frames={label: frame.to_dict(orient='tight') for label, frame in frames.items()})
            write_private_json(Path(directory) / name, payload)
        except BaseException as error:
            report['status'] = 'failed'
            if failure is None:
                failure = error
                failure.query_report = report
                failure.query_result = result
            else:
                failure.add_note(f'{name} persistence also failed: {type(error).__name__}')
    if primary is None and failure is not None:
        raise failure


def finish_mmqa_filters(native, bindings):
    """Run each original MMQA title sink from one joint native Filter relation.

    ``bindings`` maps a query label to ``(SemBenchTextQuery, source_rows)``.
    Source rows follow that query's operation.records order and explicitly carry
    the native ``occurrence_id`` and native ``source_position``. This supports
    both a single query and the existing six-query Q3b-g native batch.
    """
    if (native['return_type'] != 'TINYINT'
            or native['full_filter_materialized_before_title_sink'] is not True):
        raise ValueError('the complete native TINYINT Filter relation is required')
    expected = {}
    for label, (query, sources) in bindings.items():
        if (query.scenario != 'mmqa' or not query.query.startswith('3')
                or query.operation.stage.kind != 'filter'):
            raise ValueError('this sink binds original MMQA Q3 title Filters only')
        for record, source in zip(query.operation.records, sources, strict=True):
            occurrence = source['occurrence_id']
            if occurrence in expected:
                raise ValueError('duplicate native Filter occurrence binding')
            expected[occurrence] = (label, record, source, query)
    if native['source_rows'] != len(expected) or native['eligible_rows'] != len(expected):
        raise ValueError('native Filter source cardinality differs from the frozen bindings')
    selected = {label: [] for label in bindings}
    seen = set()
    for value in native['accepted_occurrences']:
        occurrence = value['occurrence_id']
        if occurrence in seen or occurrence not in expected:
            raise ValueError('duplicate or unknown native Filter result occurrence')
        seen.add(occurrence)
        label, record, source, query = expected[occurrence]
        if (value['source_position'] != source['source_position']
                or value['title'] != str(query.operation.frame.iloc[record.source_position]['title'])):
            raise ValueError('native Filter result differs from its source occurrence')
    for occurrence, (label, record, _source, _query) in expected.items():
        if occurrence in seen:
            selected[label].append(record)
    results = {}
    for label, (query, _sources) in bindings.items():
        def original_operator(operation, _invoke):
            return dict(frame=operation.result_frame(selected[label]))
        results[label], _ = query.execute(original_operator)
    return results


def finish_movie_scores(native, query, source_rows):
    """Apply the original Movie Q10 grouped sink to actual native DOUBLE scores."""
    if (query.scenario != 'movie' or query.query != '10'
            or query.operation.stage.kind != 'map' or native['return_type'] != 'DOUBLE'
            or native['full_projection_materialized'] is not True):
        raise ValueError('this sink requires a complete native Movie Q10 DOUBLE Map projection')
    values = {row['occurrence_id']: row for row in native['mapped_occurrences']}
    if (len(values) != len(native['mapped_occurrences']) or len(values) != len(source_rows)
            or native['source_rows'] != len(source_rows)
            or set(values) != {row['occurrence_id'] for row in source_rows}):
        raise ValueError('native Map projection changed occurrence coverage')
    completed = []
    for record, source in zip(query.operation.records, source_rows, strict=True):
        value = values[source['occurrence_id']]
        if any(value[key] != source[key] for key in ('id', 'reviewId', 'source_position')):
            raise ValueError('native Map result differs from its source occurrence')
        score = value['score']
        if type(score) not in (int, float) or not math.isfinite(score) or not 1 <= score <= 5:
            raise ValueError('native Movie score violated its existing numeric contract')
        completed.append(replace(record, values={**record.values, query.operation.stage.method.output_column: score}))

    def original_operator(operation, _invoke):
        return dict(frame=operation.result_frame(completed))
    result, _ = query.execute(original_operator)
    return result
