"""Compatibility CLI for the fixture observer's bounded --release barrier."""
import argparse
from src.experiments.postgresql import semmap_resource_gateway_observer


def main(argv=None):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--release', required=True)
    args, remaining = parser.parse_known_args(argv)
    return semmap_resource_gateway_observer.main(['--release', args.release, *remaining])


if __name__ == "__main__":
    raise SystemExit(main())
