"""Combine existing budget/HTTP and passive session observers for this run only."""
import argparse
import json
from pathlib import Path
import runpy
import sys
import time

from src.execution_provider import server
from src.experiments.postgresql.semmap_resource_gateway_observer import SessionObserver


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--legacy-observer', type=Path, required=True)
    parser.add_argument('--session-events', type=Path, required=True)
    parser.add_argument('--releases', type=Path, required=True)
    args, remaining = parser.parse_known_args()
    original_adapter, original_session = server.OpenAICompatibleFixedAdapter, server._run_session
    with args.session_events.open('x', buffering=1) as handle:
        observer = SessionObserver(lambda event: handle.write(json.dumps(event) + '\n'))
        class ObservedAdapter(original_adapter):
            def complete(self, request):
                return observer.complete(request, super().complete)
        def held_session(connection, **keywords):
            release = args.releases / f'session-{observer.current_session}'
            deadline = time.monotonic() + 5
            while not release.exists():
                if time.monotonic() >= deadline:
                    connection.close()
                    raise TimeoutError('real_observation_barrier_timeout')
                time.sleep(.001)
            return original_session(connection, **keywords)
        def observed_session(connection, **keywords):
            return observer.run_session(connection, held_session, **keywords)
        server.OpenAICompatibleFixedAdapter, server._run_session = ObservedAdapter, observed_session
        sys.argv = [str(args.legacy_observer)] + remaining
        try:
            runpy.run_path(str(args.legacy_observer), run_name='__main__')
        finally:
            server.OpenAICompatibleFixedAdapter, server._run_session = original_adapter, original_session


if __name__ == '__main__':
    main()
