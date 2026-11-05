"""Record fixture session evidence, optionally holding a fault handshake.

The event stream contains identities and counts, never model bodies. A release
barrier is test-only and bounded; real-model checks use choice_gateway_observer.
"""
import argparse
import json
from pathlib import Path
import time

from src.execution_provider import server
from src.experiments.gateway_observer import SessionObserver, ObservedAdapter


def main(argv=None):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--release", type=Path)
    args, gateway_args = parser.parse_known_args(argv)
    gateway_args = gateway_args[1:] if gateway_args[:1] == ["--"] else gateway_args
    options = server.parse_args(gateway_args)
    if options.fixed_model_config:
        parser.error("fixed models require the budgeted choice_gateway_observer entry point")
    with args.events.open("x", encoding="ascii", buffering=1) as handle:
        observer = SessionObserver(lambda value: handle.write(json.dumps(value) + "\n"))

        def session_wrapper(run):
            def observed(connection, **keywords):
                def held(connection, **keywords):
                    if args.release:
                        deadline = time.monotonic() + 5.0
                        while not args.release.exists():
                            if time.monotonic() >= deadline:
                                connection.close()
                                raise TimeoutError("fixture_barrier_timeout")
                            time.sleep(.001)
                    return run(connection, **keywords)
                return observer.run_session(connection, held, **keywords)
            return observed

        return server.main(
            gateway_args,
            adapter_wrapper=lambda adapter: ObservedAdapter(adapter, observer.complete),
            session_wrapper=session_wrapper,
        )


if __name__ == "__main__":
    raise SystemExit(main())
