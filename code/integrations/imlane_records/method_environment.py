"""Check the author ABI and fixed method source before an owner can start."""

import json
from pathlib import Path
import subprocess


def preflight_method(method_python, author_root, code_root, *, chain=False):
    method_python, author_root = Path(method_python), Path(author_root)
    probe = ("import json,sys,pyarrow; print(json.dumps(dict(base=sys.base_prefix,"
             "version=list(sys.version_info[:2]),arrow=pyarrow.__version__)))")
    probes = [json.loads(subprocess.check_output(
        [str(python), "-I", "-B", "-c", probe], text=True, timeout=15))
        for python in (author_root / "venv/bin/python", method_python)]
    if (any(item["arrow"] != "16.1.0" or item["version"] != [3, 12] for item in probes)
            or Path(probes[0]["base"]).resolve() != Path(probes[1]["base"]).resolve()):
        raise ValueError("the isolated method copy must retain the author's Python and Arrow ABI")
    module, function = (("record_chain", "validate_chain_source") if chain else
                        ("engine", "validate_filter_source"))
    command = ("import sys;sys.path.insert(0,sys.argv[1]);from src.semantic_methods.lotus."
               + module + " import " + function + ";" + function + "()")
    subprocess.run([str(method_python), "-I", "-B", "-c", command, str(code_root)],
                   check=True, capture_output=True, timeout=15)
    return probes
