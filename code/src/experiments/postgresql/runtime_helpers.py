"""Shared PostgreSQL runtime helpers for isolated qualification runners.

Behavior-preserving extraction from choice_resource_checks: the same
child-process supervision, isolated PG18.3 cluster lifecycle, and
path-wait semantics, now owned by a neutral module so SemMap
qualification stops importing choice experiment code. The two
experiments keep entirely separate threshold policies.
"""
from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import subprocess
import time


try:
    import pwd
except ImportError:  # Windows checkouts import this module for unit tests only.
    pwd = None


def wait_for_path(path, process):
    """Wait until ``path`` exists or the owning child exits."""
    for _ in range(500):
        if path.exists():
            return
        if process.poll() is not None:
            raise RuntimeError("child_exited_before_ready")
        time.sleep(0.02)
    raise RuntimeError('test process did not become ready')


@contextmanager
def owned_child_process(command, root, name, env, user):
    """Run a child under the target user, logging to <name>.log, terminating on exit."""
    with (root / (name + '.log')).open('x') as log:
        # setuid to the target user only when the caller is someone else;
        # an unprivileged caller cannot setuid, and runuser is root-only.
        if user is None or pwd is None or os.getuid() == user.pw_uid or not hasattr(os, 'chown'):
            process = subprocess.Popen(command, env=env, stdout=log,
                                       stderr=subprocess.STDOUT)
        else:
            process = subprocess.Popen(command, env=env, stdout=log,
                                       stderr=subprocess.STDOUT,
                                       user=user.pw_uid, group=user.pw_gid,
                                       extra_groups=[])
        try:
            yield process
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise RuntimeError(
                        'owned test process required forced termination')


@contextmanager
def isolated_pg18_cluster(prefix, root, user):
    """Initdb + start an isolated PG18.3 cluster with semloom_pg preloaded."""
    import psycopg
    data = root / 'data'
    socket_dir = root / 'socket'
    socket_dir.mkdir()
    if pwd is not None and hasattr(os, 'chown'):
        os.chown(socket_dir, user.pw_uid, user.pw_gid)
    # runuser is root-only; a runner already executing as the cluster user
    # (required on hosts where /proc fd inspection needs target-user
    # identity) must invoke the PostgreSQL tools directly.
    pg = [] if pwd is not None and os.getuid() == user.pw_uid else [
        'runuser', '-u', user.pw_name, '--']
    with (root / 'cluster.log').open('x') as log:
        subprocess.run(
            pg + [str(prefix / 'bin/initdb'), '-D', str(data),
                  '--no-locale', '-E', 'UTF8'],
            check=True, stdout=log, stderr=subprocess.STDOUT)
        ctl = pg + [str(prefix / 'bin/pg_ctl'), '-D', str(data)]
        subprocess.run(
            ctl + ['-l', str(root / 'postgres.log'), '-o',
                   f"-c listen_addresses='' "
                   f"-c shared_preload_libraries=semloom_pg "
                   f"-k {socket_dir} -p 55446",
                   '-w', 'start'],
            check=True, stdout=log, stderr=subprocess.STDOUT)
        try:
            with psycopg.connect(
                    host=str(socket_dir), port=55446, user='postgres',
                    dbname='postgres', autocommit=True) as connection:
                assert connection.execute(
                    'SHOW server_version').fetchone()[0] == '18.3'
                connection.execute('CREATE EXTENSION semloom_pg')
                yield connection
        finally:
            subprocess.run(
                ctl + ['-m', 'fast', '-w', 'stop'],
                check=True, stdout=log, stderr=subprocess.STDOUT)
