"""Original MMQA Q3a Filter and title sink in driver; unchanged Daft AI worker."""
import argparse
from collections import Counter
from dataclasses import replace
import json
from pathlib import Path
import time

from src.infrastructure.private_artifacts import (
    read_json, write_private_json, publish_private_json, file_sha256,
    content_digest, json_metric,
)
from src.experiments.native_daft_query import supervised_run, _save_artifact


from mmqa_semantics import prepare, finish, frame_value


def run_query(config, directory, *, model_python, tokenizer=None, invoke=None):
    from transformers import AutoTokenizer
    started = time.monotonic_ns()
    directory = Path(directory)
    directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    report = dict(status='failed', baseline_name='Daft native AI / project MMQA Q3a binding',
        semantic_operator='Original title/text Comedy Filter -> original title DataFrame',
        execution_backend='Daft prompt(provider=vllm-prefix-caching); original LOTUS semantics in driver',
        started_monotonic_ns=started, warmup=0, whole_query_retries=0)
    primary = None
    try:
        if tokenizer is None:
            tokenizer = AutoTokenizer.from_pretrained(config['model'], local_files_only=True,
                                                      trust_remote_code=False)
        query, method, prepared, requests = prepare(config, tokenizer)
        report['prompts_prepared_monotonic_ns'] = time.monotonic_ns()
        report['logical_rows'] = len(requests)
        report['query_description'] = query.describe()
        if invoke is None:
            packet = directory/'prepared-prompts.json'
            write_private_json(packet, dict(schema='semloom.native_daft_prompts.v1', requests=requests))
            worker = {k:config[k] for k in ('model','sampling_params','engine_kwargs','provider_options',
                                          'query_timeout_s','cleanup_s','stop_grace_s')}
            worker.update(schema='semloom.native_daft_map.v1', requests_path=str(packet),
                          requests_sha256=file_sha256(packet))
            worker_path = directory/'worker-config.json'
            write_private_json(worker_path, worker)
            report['supervision'] = supervised_run(worker_path, directory/'native-worker', model_python=model_python)
            terminal = read_json(directory/'native-worker/worker.json')
            if terminal['status'] != 'completed':
                raise RuntimeError('Native worker incomplete; retain partial results')
            raw = read_json(directory/'native-worker/result-ready.json')
            native = raw['result']
            report['native_api'] = terminal['native_api']
            report['model_close_receipt'] = terminal['model_close_receipt']
            report['native_raw_result_ready_ns'] = raw['result_ready_monotonic_ns']
        else:
            native = invoke(requests)
        result, semantic, _ = finish(query, method, prepared, native['predictions'])
        ready = time.monotonic_ns()
        report.update(result_ready_monotonic_ns=ready, cold_full_result_seconds=(ready-started)/1e9,
                      result=frame_value(result), returned_rows=len(result),
                      timing_scope='Original CSV/preparation, fresh native AI model/graph, all25 outputs, worker close/wait, original Filter parser and title sink; evaluation separate')
        publish_private_json(directory/'result-ready.json', dict(result=frame_value(result),
            semantic=semantic, result_ready_monotonic_ns=ready))
        report['evaluation_started_ns'] = time.monotonic_ns()
        report['quality'] = json_metric(query.evaluate(result))
        report.update(status='completed', evaluation_finished_ns=time.monotonic_ns())
    except BaseException as error:
        primary = error
        report['error_type'] = type(error).__name__
        raise
    finally:
        report['finished_monotonic_ns'] = time.monotonic_ns()
        _save_artifact(directory/'driver.json', report, primary=primary)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--model-python', required=True)
    args = parser.parse_args()
    run_query(read_json(args.config), args.output_dir, model_python=args.model_python)


if __name__ == '__main__':
    main()
