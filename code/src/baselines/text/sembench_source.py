"""Load original queries and evaluators from the selected public SemBench checkout."""

import importlib.util
from pathlib import Path
import subprocess
import sys


SEMBENCH_COMMIT = 'c814e3807e72d4cf876b852b17e77f3cc94575c2'


def verify_checkout(checkout):
    root = Path(checkout).resolve()
    # Trust only this caller-selected public archive, never a global Git path.
    git = ['git', '-c', 'safe.directory=' + str(root), '-C', str(root)]
    commit = subprocess.check_output(git + ['rev-parse', 'HEAD'], text=True, timeout=10).strip()
    dirty = subprocess.check_output(git + ['status', '--porcelain', '--untracked-files=no',
        '--', 'src', 'files/movie/query', 'files/mmqa/query'], text=True, timeout=10)
    if commit != SEMBENCH_COMMIT or dirty:
        raise ValueError('SemBench query/evaluator checkout does not match its pinned source')
    return root


def load_module(checkout, name, relative_path):
    root = verify_checkout(checkout)
    source = root / 'src'
    for package in ('runner', 'evaluator'):
        module = sys.modules.get(package + '.generic_' + package)
        if module is not None and not Path(module.__file__).resolve().is_relative_to(source):
            raise RuntimeError('another benchmark namespace is already loaded; use an isolated process')
    sys.path.insert(0, str(source))
    try:
        spec = importlib.util.spec_from_file_location(name, root / relative_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(source))
