"""Check the author ABI and fixed method source before an owner can start."""

import json
from pathlib import Path
import subprocess


def validate_author_build(author_root, bridge_binary):
    """Check the existing patched author and this bridge without starting them."""
    from integrations.imlane_records import owner

    author, binary = Path(author_root), Path(bridge_binary)
    build = json.loads((author / "artifacts/build-patched/build-identity.json").read_text())
    if build.get("upstream_commit") != owner.AUTHOR or build.get("mode") != "patched":
        raise ValueError("the checked author build identity differs")
    expected = dict(build["original_sha256"], **build["patched_sha256"])
    if (any(owner.digest(author / "source" / path) != sha for path, sha in expected.items())
            or any(owner.digest(author / "prefix-patched" / path) != sha
                   for path, sha in build["binaries_sha256"].items())):
        raise ValueError("the author's checked source or binaries changed")
    built = json.loads(binary.with_suffix(".build.json").read_text())
    source = Path(__file__).parent
    if (built.get("upstream_commit") != owner.AUTHOR or built.get("binary_sha256") != owner.digest(binary)
            or built.get("sources_sha256") != {path.name: owner.digest(path) for path in (
                source / "imlane_records.cpp", source / "record_dbend_adapter.hpp")}):
        raise ValueError("the bridge binary is not bound to these reviewed v2 source bytes")
    return dict(author_commit=owner.AUTHOR, bridge_binary_sha256=built["binary_sha256"])


def preflight_method(method_python, author_root, code_root, *, chain=False, cascade=False):
    if chain and cascade:
        raise ValueError("choose one finite method source profile")
    method_python, author_root = Path(method_python), Path(author_root)
    probe = ("import json,sys,pyarrow; print(json.dumps(dict(base=sys.base_prefix,"
             "version=list(sys.version_info[:2]),arrow=pyarrow.__version__)))")
    probes = [json.loads(subprocess.check_output(
        [str(python), "-I", "-B", "-c", probe], text=True, timeout=15))
        for python in (author_root / "venv/bin/python", method_python)]
    if (any(item["arrow"] != "16.1.0" or item["version"] != [3, 12] for item in probes)
            or Path(probes[0]["base"]).resolve() != Path(probes[1]["base"]).resolve()):
        raise ValueError("the isolated method copy must retain the author's Python and Arrow ABI")
    validators = [("record_chain", "validate_chain_source") if chain else
                  ("engine", "validate_filter_source")]
    if cascade:
        validators.append(("cascade", "validate_cascade_source"))
    for module, function in validators:
        command = ("import sys;sys.path.insert(0,sys.argv[1]);from src.semantic_methods.lotus."
                   + module + " import " + function + ";" + function + "()")
        subprocess.run([str(method_python), "-I", "-B", "-c", command, str(code_root)],
                       check=True, capture_output=True, timeout=15)
    return probes
