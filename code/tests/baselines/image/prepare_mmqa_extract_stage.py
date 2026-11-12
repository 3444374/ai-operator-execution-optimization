"""Prepare original Extract messages using an explicit reference Join fixture.

This zero-model component diagnostic does not run or qualify a model Join, return
a completed query, or produce query quality. The original SDK formats both stages.
"""
import argparse
from copy import deepcopy
import json
from pathlib import Path
import socket
from unittest.mock import patch

from src.baselines.image.sembench_mmqa import describe_inputs, run_original_query
from src.infrastructure.private_artifacts import file_identity, new_private_directory, write_private_json


class _ExtractCaptured(BaseException):
    pass


def prepare(checkout, data_dir, output):
    import lotus
    from lotus.models import LM
    from lotus.sem_ops.sem_join import SemJoinDataframe
    from litellm.types.utils import ModelResponse

    inputs = describe_inputs(checkout, data_dir, '2b')
    truth = Path(checkout) / 'files/mmqa/query/natural_language/q2b.json'
    reference = {(row[0], row[1]) for row in json.loads(truth.read_text())['ground_truth']}
    selected, decisions, captured = [], [], {}
    original_join = SemJoinDataframe.__call__
    lm = LM('openai/reference-fixture', max_tokens=8192, temperature=0, top_p=1, seed=0, stop=[])

    def reference_join(accessor, other, *args, **kwargs):
        if decisions:
            raise ValueError('this component fixture requires the original single Q2b Join')
        for path in accessor._obj['image_filepath']:
            for row_id in other['ID']:
                pair = (int(row_id), Path(path).name)
                keep = pair in reference
                decisions.append(keep)
                if keep:
                    selected.append(pair)
        if not selected:
            raise ValueError('no official positive reference pairs occur in this input')
        return original_join(accessor, other, *args, **kwargs)

    def software(data, options, *_args):
        if options.get('response_format') == {'type': 'json_object'}:
            captured.update(messages=[deepcopy(message) for message, _ in data],
                            generation=deepcopy(options))
            raise _ExtractCaptured()
        if len(data) != len(decisions):
            raise ValueError('original Join changed its ordered Cartesian request count')
        return [ModelResponse(id='reference-' + str(i), model=lm.model,
            choices=[dict(index=0, finish_reason='stop', message=dict(role='assistant',
                content='Reasoning: explicit reference fixture.\nAnswer: ' + str(keep)))],
            usage=dict(prompt_tokens=0, completion_tokens=0, total_tokens=0))
            for i, keep in enumerate(decisions)]

    connect = socket.socket.connect

    def offline(sock, address):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            raise RuntimeError('reference-stage preparation forbids network and model endpoints')
        return connect(sock, address)

    lm._process_uncached_messages = software
    with lotus.settings.context(lm=lm, enable_cache=False), \
            patch.object(SemJoinDataframe, '__call__', reference_join), \
            patch.object(socket.socket, 'connect', offline):
        try:
            run_original_query(checkout, data_dir, '2b')
        except _ExtractCaptured:
            pass
    if len(captured.get('messages', [])) != len(selected):
        raise ValueError('the original Extract did not consume every reference Join occurrence')
    if describe_inputs(checkout, data_dir, '2b') != inputs:
        raise ValueError('original inputs changed during reference-stage preparation')
    report = dict(status='prepared_reference_Extract_component_only', inputs=inputs,
        preparation_source=file_identity(Path(__file__)),
        reference=file_identity(truth), reference_pairs=[list(pair) for pair in selected],
        software_Join_responses=len(decisions), model_calls=0,
        proposed_Extract_model_calls=len(selected), original_Extract=captured,
        generation_scope='software LM kwargs and native JSON constraint; bind actual model/service configuration separately',
        Join_reply_source='official positive pairs as explicit software fixture; not actual model answers',
        completed_query=False, query_quality=None, full_query_model_qualified=False)
    output = Path(output)
    new_private_directory(output)
    write_private_json(output / 'extract-stage.json', report)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('sembench-checkout', 'data-dir', 'output-dir'):
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args()
    report = prepare(args.sembench_checkout, args.data_dir, args.output_dir)
    print(json.dumps({key: report[key] for key in
        ('status', 'model_calls', 'proposed_Extract_model_calls', 'full_query_model_qualified')}))
