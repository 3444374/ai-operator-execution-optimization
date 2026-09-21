"""Observe two overlapping, task-free sessions against the canonical gateway."""
import hashlib
import json
from pathlib import Path
import select
import socket
import subprocess
import sys
import tempfile
import time

repo = Path('<test-worktree>')
sys.path.insert(0, str(repo/'code'))
from src.execution_provider.generation_profile import GenerationProfile
from src.execution_provider.wire import v4
from src.execution_provider.wire.framing import encode_frame, read_frame

profile = GenerationProfile('semloom.generation.choice.tristate', 1, 'CHOICE', ('TRUE','FALSE','UNKNOWN'))
plan = v4.SemanticFilterPlan('Classify input.', 'golden-model-v1', profile)
with tempfile.TemporaryDirectory(prefix='choice-session-', dir='/private/tmp') as directory:
    path = Path(directory)/'gateway.sock'
    process = subprocess.Popen([sys.executable, str(repo/'code/scripts/services/run_execution_provider_gateway.py'),
                                '--socket', str(path), '--test-max-sessions', '2'],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    first = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    second = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        for _ in range(200):
            if path.exists():
                break
            assert process.poll() is None
            time.sleep(0.01)
        first.settimeout(2)
        second.settimeout(2)
        first.connect(str(path))
        first.sendall(encode_frame(v4.build_open_message(plan)))
        assert read_frame(first)['type'] == 'opened'
        second.connect(str(path))
        second.sendall(encode_frame(v4.build_open_message(plan)))
        ready, _, _ = select.select([second], [], [], 0.2)
        blocked = not ready
        first.close()
        assert read_frame(second)['type'] == 'opened'
        second.close()
        out, err = process.communicate(timeout=5)
        assert process.returncode == 0, err.decode()
        assert not path.exists()
        result = dict(first_opened=True, second_waited_while_first_open=blocked,
                      second_opened_after_first_close=True, wait_observation_seconds=0.2,
                      tasks_sent=0, model_requests=0, gateway_exit=process.returncode,
                      socket_removed=True, source_commit=subprocess.check_output(
                          ['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
                      source_sha256={name:hashlib.sha256((repo/name).read_bytes()).hexdigest() for name in (
                          'code/src/execution_provider/server.py',
                          'code/src/execution_provider/adapters/semantic_session.py')})
        print(json.dumps(result, indent=2))
    finally:
        first.close()
        second.close()
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
