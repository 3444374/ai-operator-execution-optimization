#!/usr/bin/env python3
"""Count fixture sessions and tasks without recording payloads or outputs."""

import argparse
import json
from pathlib import Path
import sys

from src.execution_provider import server


parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--events", type=Path, required=True)
args, gateway_args = parser.parse_known_args()
args.events.touch(exist_ok=False)
handle = args.events.open("a", encoding="ascii", buffering=1)
task_count = 0
session_count = 0


def record(value):
    handle.write(json.dumps(value, separators=(",", ":")) + "\n")


original_adapter = server.GoldenCompletionAdapter


class ObservedGoldenAdapter(original_adapter):
    def complete(self, request):
        global task_count
        task_count += 1
        record({
            "event": "task",
            "task": task_count,
            "payload_digest": request.semantic_payload_digest,
        })
        return super().complete(request)


original_session = server._run_session


def observed_session(connection, **keywords):
    global session_count
    session_count += 1
    current = session_count
    record({"event": "session_start", "session": current})
    try:
        return original_session(connection, **keywords)
    finally:
        record({"event": "session_end", "session": current})


server.GoldenCompletionAdapter = ObservedGoldenAdapter
server._run_session = observed_session
sys.argv = [sys.argv[0]] + (gateway_args[1:] if gateway_args[:1] == ["--"] else gateway_args)
try:
    raise SystemExit(server.main())
finally:
    handle.close()
    server.GoldenCompletionAdapter = original_adapter
    server._run_session = original_session
