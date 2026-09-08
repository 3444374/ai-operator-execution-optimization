"""PG v6 fixture: concurrent tasks complete out of order and retain cancelled work."""

import argparse
import asyncio
import json
from pathlib import Path
import signal
import socket
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from src.execution_provider.adapters.incremental_session import IncrementalMapSessionAdapter
from src.execution_provider.adapters.openai_compatible_fixed import FixedModelConfig
from src.execution_provider.wire.framing import read_frame, encode_frame

p = argparse.ArgumentParser()
p.add_argument("--socket", required=True)
p.add_argument("--events", type=Path, required=True)
p.add_argument("--fault", choices=("ack-version", "sequence", "payload"))
a = p.parse_args()
stop = threading.Event()
pairs = {}


def observe(event):
    with a.events.open("a") as output:
        output.write(json.dumps(event) + "\n")


async def execute(task, endpoint):
    value = json.loads(task.task.payload)["messages"][1]["content"]
    pair = pairs.setdefault(task.key.session_id, asyncio.Event())
    observe({"event": "model_start", "sequence": task.key.sequence, "input": value})
    if value == "slow":
        await asyncio.wait_for(pair.wait(), 2)
        await asyncio.sleep(0.05)
    elif value == "fast":
        pair.set()
    elif value == "cancel":
        await asyncio.sleep(0.4)
    observe({"event": "model_end", "sequence": task.key.sequence, "input": value})
    return json.dumps(
        {
            "model": "model",
            "choices": [{"message": {"content": "mapped:" + value}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 2, "completion_tokens": 1},
        }
    ).encode()


class FaultConnection:
    def __init__(self, connection):
        self.connection = connection

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def sendall(self, frame):
        message = json.loads(frame[4:])
        if a.fault == "ack-version" and message["type"] == "accepted":
            message["protocol_version"] = 5
        elif message["type"] == "completion":
            if a.fault == "sequence":
                message["sequence"] = "1000"
            elif a.fault == "payload":
                message["semantic_payload_digest"] = "0" * 64
        self.connection.sendall(encode_frame(message))


adapter = IncrementalMapSessionAdapter(
    FixedModelConfig("http://localhost/v1/chat/completions", "model", 3000),
    max_tasks=2,
    execute=execute,
    observer=observe,
)
listener = socket.socket(socket.AF_UNIX)
listener.bind(a.socket)
listener.listen(4)
listener.settimeout(0.05)


def stopping(*_):
    stop.set()
    adapter.request_stop()


signal.signal(signal.SIGTERM, stopping)
signal.signal(signal.SIGINT, stopping)
try:
    while not stop.is_set():
        try:
            conn, _ = listener.accept()
        except TimeoutError:
            continue
        conn.settimeout(4)
        try:
            adapter.serve_connection(
                conn,
                lambda connection: adapter.run_incremental(
                    FaultConnection(connection), read_frame(connection)
                ),
                stop.is_set,
            )
        finally:
            conn.close()
finally:
    adapter.close()
    listener.close()
    Path(a.socket).unlink(missing_ok=True)
