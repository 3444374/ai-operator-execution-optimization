"""Recompute the two retained sync CPU checks without model or Ray execution."""
import argparse
from contextlib import ExitStack
import importlib.abc
import json
from pathlib import Path
import sys
from unittest.mock import patch

from src.infrastructure.private_artifacts import require_outside_git, write_private_json


class NoModel(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] == 'vllm':
            raise AssertionError('CPU evidence check attempted the actual model runtime')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('check', choices=('native-pipeline', 'ack-order'))
    parser.add_argument('--source-root', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if (args.check == 'native-pipeline') != (args.source_root is not None):
        parser.error('--source-root is required only for native-pipeline')
    require_outside_git(args.output)
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('CPU evidence output already exists')
    guard = NoModel()
    with ExitStack() as cleanup:
        sys.meta_path.insert(0, guard)
        cleanup.callback(sys.meta_path.remove, guard)
        if args.check == 'native-pipeline':
            from tests.serving.sync_native_source_fixture import check_native_pipeline
            report = check_native_pipeline(args.source_root)
        else:
            import ray
            import ray._private.worker
            def no_runtime(*args, **kwargs):
                raise AssertionError('CPU evidence check attempted actual Ray initialization')
            cleanup.enter_context(patch.object(ray, 'init', no_runtime))
            cleanup.enter_context(patch.object(ray._private.worker, 'init', no_runtime))
            from tests.execution_provider.ack_order_fixture import check_ack_order
            report = check_ack_order()
    write_private_json(args.output, report)
    print(json.dumps(dict(check=args.check, status='passed', model_calls=0, real_Ray_runs=0, GPU_runs=0)))


if __name__ == '__main__':
    main()
