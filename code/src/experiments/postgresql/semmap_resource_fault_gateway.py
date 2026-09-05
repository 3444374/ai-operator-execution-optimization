"""Test-only handshake barrier for observable single-query fault fixtures.

This is fault injection, separate from the passive gateway observer. The driver
releases a held connection after observing both ends, or cancels at a fixed
five-second deadline. Production server and protocol code are unchanged.
"""
import argparse
from pathlib import Path
import sys
import time


def main():
    parser=argparse.ArgumentParser(add_help=False)
    parser.add_argument('--release',type=Path,required=True)
    args, remaining=parser.parse_known_args()
    from src.execution_provider import server
    from src.experiments.postgresql import semmap_resource_gateway_observer as observer
    original=server._run_session
    def held(connection, **keywords):
        deadline=time.monotonic()+5.
        while not args.release.exists():
            if time.monotonic()>=deadline:
                connection.close()
                raise TimeoutError('fixture_barrier_timeout')
            time.sleep(.001)
        return original(connection, **keywords)
    server._run_session=held
    sys.argv=[sys.argv[0]]+remaining
    try:
        return observer.main()
    finally:
        server._run_session=original


if __name__=='__main__':
    raise SystemExit(main())
