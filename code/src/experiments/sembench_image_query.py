"""Read-only preparation for original finite multimodal SemBench queries."""

import argparse
import json
from pathlib import Path
import sys

from src.infrastructure.private_artifacts import new_private_directory, write_private_json
from src.infrastructure.redact import redact_text


def check(checkout, scenario, query, *, data_dir=None, manifest=None, manifest_digest=None, scale_factor=None):
    """Inspect actual inputs; do not allocate a framework, model or call budget."""
    if scale_factor is not None and scenario not in ('cars', 'medical'):
        raise ValueError('visual scale_factor selects only the authored Cars/Medical CSV paths')
    if scenario == "animals":
        if manifest is None or manifest_digest is None or data_dir is not None:
            raise ValueError("Animals requires manifest and manifest_digest, without data_dir")
        from src.baselines.image.sembench_animals import AnimalsDataset
        query_id = int(str(query).lower().removeprefix("q"))
        inputs = AnimalsDataset(Path(checkout), Path(manifest), manifest_digest).preflight(query_id)
    elif scenario == "mmqa":
        if data_dir is None or manifest is not None or manifest_digest is not None:
            raise ValueError("MMQA requires data_dir, without Animals manifest arguments")
        from src.baselines.image.sembench_mmqa import describe_inputs
        inputs = describe_inputs(checkout, data_dir, query)
    elif scenario == "ecomm":
        if data_dir is None or manifest is not None or manifest_digest is not None:
            raise ValueError("E-Commerce requires data_dir, without Animals manifest arguments")
        from src.baselines.text.sembench_ecomm import describe_visual_inputs
        inputs = describe_visual_inputs(checkout, data_dir, query)
    elif scenario == 'cars':
        if data_dir is None or manifest is not None or manifest_digest is not None:
            raise ValueError('Cars requires data_dir, without Animals manifest arguments')
        from src.baselines.text.sembench_cars import describe_visual_inputs, FULL_SCALE_FACTOR
        inputs = describe_visual_inputs(checkout, data_dir, query,
            scale_factor=FULL_SCALE_FACTOR if scale_factor is None else scale_factor)
    elif scenario == 'medical':
        if data_dir is None or manifest is not None or manifest_digest is not None:
            raise ValueError('Medical requires data_dir, without Animals manifest arguments')
        from src.baselines.text.sembench_medical import describe_visual_inputs, FULL_SCALE_FACTOR
        inputs = describe_visual_inputs(checkout, data_dir, query,
            scale_factor=FULL_SCALE_FACTOR if scale_factor is None else scale_factor)
    else:
        raise ValueError("visual preparation supports animals, mmqa, ecomm, cars and medical")
    return dict(schema="semloom.sembench_image_query_check.v1", status="prepared_cpu_only",
        scenario=scenario, query=inputs["query"], inputs=inputs,
        execution_enabled=False, model_calls=0, requests_reserved=0, requests_claimed=0,
        native_lotus_query="original query/evaluator and native SDK service entry wired; service qualification pending",
        baseline_states={
            "lotus-native": "run_sembench_lotus_image_query.py binds the original SDK; actual VLM service not checked here",
            "ray-data": ("native Animals VLM Filter entry wired; this input/service is not qualified by preflight"
                         if scenario == "animals" else "this visual task is not wired to the maintained native Ray entry"),
            "daft": "visual SemBench binding pending; native provider support must be checked separately from CLIP",
            "imlane": "current author bridge visual binding not established",
            "kalypso": "current author bridge visual binding not established"},
        results={name: None for name in ("lotus-native", "ray-data", "daft", "imlane", "kalypso")},
        pending=["qualify the configured native VLM service, original PNG requests and JSON Extract responses",
            "freeze model/revision/dtype/processor/max_pixels/generation and actual ready configuration",
            "use existing host supervisor for startup, query deadline and physical cleanup"],
        qualification_scope="file/pixel identities and one complete query's declared work; no model quality or performance")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sembench-checkout", required=True)
    parser.add_argument("--scenario", choices=("animals", "mmqa", "ecomm", "cars", "medical"), required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--data-dir")
    parser.add_argument("--scale-factor", type=int)
    parser.add_argument("--manifest")
    parser.add_argument("--manifest-digest")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(argv)
    try:
        report = check(args.sembench_checkout, args.scenario, args.query,
            data_dir=args.data_dir, manifest=args.manifest, manifest_digest=args.manifest_digest,
            scale_factor=args.scale_factor)
        output = Path(args.output_dir).resolve()
        new_private_directory(output)
        write_private_json(output / "check.json", report)
        print(json.dumps(dict(status=report["status"], scenario=args.scenario,
            query=report["query"], model_calls=0, execution_enabled=False)))
        return 0
    except Exception as error:
        print(redact_text(type(error).__name__ + ": " + str(error)), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
