"""Load original queries and evaluators from the selected public SemBench checkout."""

from contextlib import contextmanager, nullcontext
import importlib
import importlib.util
from pathlib import Path
import subprocess
import sys


SEMBENCH_COMMIT = 'c814e3807e72d4cf876b852b17e77f3cc94575c2'


@contextmanager
def _runner_namespace(source):
    """The pinned MMQA runner spells its public runner import as src.runner.

    SemLoom already owns src. Alias only the verified upstream runner packages
    during that import, without replacing src or editing the original query.
    """
    names = ('runner', 'runner.generic_lotus_runner',
             'runner.generic_lotus_runner.generic_lotus_runner')
    previous = {}
    try:
        for name in names:
            module = importlib.import_module(name)
            locations = ([module.__file__] if getattr(module, '__file__', None) else list(module.__path__))
            if not locations or any(not Path(path).resolve().is_relative_to(source) for path in locations):
                raise RuntimeError('another benchmark runner namespace is loaded; use an isolated process')
            alias = 'src.' + name
            previous[alias] = sys.modules.get(alias)
            if previous[alias] is not None and previous[alias] is not module:
                raise RuntimeError('src.runner is already owned by another module')
            sys.modules[alias] = module
        yield
    finally:
        for alias, module in previous.items():
            if module is None:
                sys.modules.pop(alias, None)
            else:
                sys.modules[alias] = module


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
        namespace = (_runner_namespace(source) if relative_path ==
            'src/scenario/mmqa/runner/lotus_runner/lotus_runner.py' else nullcontext())
        with namespace:
            spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(source))
