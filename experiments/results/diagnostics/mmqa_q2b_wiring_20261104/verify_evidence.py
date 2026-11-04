"""Check published evidence copies and optionally the independently retained originals."""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import tarfile


def check(raw, expected, label):
    if len(raw) != expected["bytes"] or hashlib.sha256(raw).hexdigest() != expected["sha256"]:
        raise ValueError("evidence bytes or digest differ: " + label)


def relative_path(root, name):
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("evidence member must be relative")
    return root.joinpath(*path.parts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-root", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    mapping = json.loads((root / "evidence-map.json").read_text())
    published = originals = 0
    for artifact_id, artifact in mapping["artifacts"].items():
        archive = relative_path(root, artifact["path"])
        check(archive.read_bytes(), artifact["identity"], artifact_id)
        with tarfile.open(archive) as stream:
            expected_members = set()
            for entry in mapping["entries"]:
                copy = entry.get("published_copy")
                if copy is None or copy["artifact_id"] != artifact_id:
                    continue
                member = stream.getmember(copy["member"])
                if not member.isfile():
                    raise ValueError("published member is not a regular file")
                check(stream.extractfile(member).read(), copy["identity"], copy["member"])
                expected_members.add(copy["member"])
                published += 1
            if set(stream.getnames()) != expected_members:
                raise ValueError("published member inventory differs")
    if args.original_root is not None:
        for entry in mapping["entries"]:
            name = entry["original_relative_path"]
            check(relative_path(args.original_root, name).read_bytes(), entry["original_identity"], name)
            originals += 1
    entries = {entry["original_relative_path"]: entry for entry in mapping["entries"]}
    validation = json.loads((root / "validation.json").read_text())
    references = [run["log"] for run in validation["cpu_runs"].values()]
    references += [validation["cli_supervised_fixture"][key]
                   for key in ("original_preflight", "summary", "footer", "supervisor")]
    repair = validation["review_repair"]
    references += [repair["tests_log"], repair["privacy_probe"]["log"]]
    references += [repair["cli_supervised_fixture"][key]
                   for key in ("original_preflight", "summary", "footer", "supervisor")]
    references.append(validation["author_local_cli_checks"]["log"])
    for reference in references:
        source = entries[reference["evidence_original"]]["original_identity"]
        if source != {key: reference[key] for key in ("sha256", "bytes")}:
            raise ValueError("validation reference differs from original evidence mapping")
    print(json.dumps(dict(published_files_verified=published, original_files_verified=originals,
                          validation_references_verified=len(references))))


if __name__ == "__main__":
    main()
