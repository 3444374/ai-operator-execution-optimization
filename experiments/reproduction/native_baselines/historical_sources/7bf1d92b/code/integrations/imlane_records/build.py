"""Compile only records clients against the existing checked author prefix."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

OWNED = Path(__file__).resolve().parent
sys.path.insert(0, str(OWNED.parents[1]))
from src.baselines.common.private_artifacts import require_outside_git, write_private_json
from src.baselines.common.redact import redact_text

AUTHOR = "072f8257db80569e42c9f5727d839587df529652"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--checks", action="store_true")
    args = parser.parse_args(argv)
    root, output = args.root.resolve(), args.output.resolve()
    require_outside_git(root)
    require_outside_git(output)
    if output.exists() or output.with_suffix(".build.json").exists():
        raise ValueError("records build refuses to overwrite a prior result")
    identity = json.loads((root / "artifacts/build-patched/build-identity.json").read_text())
    if identity["upstream_commit"] != AUTHOR or identity["mode"] != "patched":
        raise ValueError("records bridge requires the checked public author build")
    source, prefix = root / "source", root / "prefix-patched"
    expected = dict(identity["original_sha256"], **identity["patched_sha256"])
    # The cleanup behavior below is an inline public API and must be pinned too.
    if expected.get("include/dbend/dbend_context.hpp") != "f524d36e40dcfa6540a1105df65e875d6b34cf04fce3fdf631631b5d1c3fca9b":
        raise ValueError("records DBEnd inline API identity differs")
    for name, digest in expected.items():
        if hashlib.sha256((source / name).read_bytes()).hexdigest() != digest:
            raise ValueError("checked IMLane source changed after build")
    for name, digest in identity["binaries_sha256"].items():
        if hashlib.sha256((prefix / name).read_bytes()).hexdigest() != digest:
            raise ValueError("checked IMLane binary changed")
    arrow = identity["python_arrow"]
    if arrow["arrow"] != "16.1.0":
        raise ValueError("records build keeps the author Arrow 16.1 ABI")
    selected = "records_checks.cpp" if args.checks else "imlane_records.cpp"
    includes = [source / "include", Path(arrow["include"]), source / "third_party/msgpack/include",
                source / "third_party/concurrentqueue/include", source / "third_party/libuv/include",
                source / "third_party/json/single_include", source / "third_party/spdlog",
                source / "third_party/fmt/dist/include", source / "third_party/boost/dist/include"]
    libraries = [prefix / "lib", Path(arrow["lib"]), source / "third_party/fmt/dist/lib",
                 source / "third_party/boost/dist/lib"]
    command = ["g++", "-std=c++17", "-O2", str(OWNED / selected), "-o", str(output),
               *["-I" + str(p) for p in includes], *["-L" + str(p) for p in libraries],
               "-Wl,-rpath," + ":".join(map(str, libraries)),
               "-ldbendrt_cxx", "-larrow", "-lfmt", "-lboost_system", "-pthread"]
    output.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(command, capture_output=True, text=True, timeout=120)
    record = dict(upstream_commit=AUTHOR, returncode=result.returncode, author_modified=False,
                  compiler_output=redact_text(result.stdout + result.stderr), real_model_requests=0,
                  sources_sha256={p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                  for p in (OWNED / selected, OWNED / "record_dbend_adapter.hpp")},
                  binary_sha256=hashlib.sha256(output.read_bytes()).hexdigest() if output.is_file() else None)
    write_private_json(output.with_suffix(".build.json"), record)
    if result.returncode:
        print(record["compiler_output"], file=sys.stderr)
        return 1
    print(json.dumps({"built": True, "binary_sha256": record["binary_sha256"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
