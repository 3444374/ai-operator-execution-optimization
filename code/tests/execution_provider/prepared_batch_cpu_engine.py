"""Production synchronous IPC server with deterministic CPU inference only."""
from dataclasses import asdict
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import patch

from src.serving.backends import staged_generation_process, vllm_engine
from src.serving.backends.vllm_engine import _prepared, result_from_output


class Engine:
    def __init__(self, spec, generation):
        self.config = json.loads(Path(spec.model_path).read_text())
        self.log = Path(spec.model_path + '.batches')
        self.count = 0
        self.uid = 'CPU-prepared-batch-' + str(os.getpid())

    def ready(self):
        return dict(engine_uid=self.uid, physical_gpu=False)

    def run_prepared(self, rows, *, generation, deadline_monotonic):
        identities, conversations = _prepared(rows)
        index = self.count; self.count += 1
        with self.log.open('a') as stream:
            stream.write(json.dumps(dict(index=index, calls=identities,
                messages=conversations, generation=asdict(generation))) + '\n')
        if self.config.get('fail_batch') == index:
            raise RuntimeError('CPU native batch outcome unconfirmed')
        results = []
        for row in identities:
            text = (('generated ' * (1 + row['source_position'] * 2)).strip()
                    if row['algorithm_stage'] == 'label' else 'True')
            results.append(dict(row, status='succeeded', text=text, prompt_tokens=1,
                completion_tokens=1, total_tokens=2, prompt_token_ids_json='[0]',
                output_token_ids_json='[1]', finish_reason='stop', submission_confirmed=True,
                engine_uid=self.uid, engine_request_id='CPU-' + row['call_id']))
        if self.config.get('cancel_after') == index:
            Path(self.config['stop_file']).touch()
        if self.config.get('corrupt_batch') == index:
            results.append(dict(results[0]))
        if self.config.get('invalid_success_batch') == index:
            invalid = self.config['invalid_success']
            if invalid == 'converter_missing_usage':
                output = SimpleNamespace(request_id='CPU-missing-usage', prompt_token_ids=None,
                    outputs=[SimpleNamespace(index=0, text='generated', token_ids=[1], finish_reason='stop')],
                    finished=True)
                results[0] = result_from_output(identities[0], output, engine_uid=self.uid,
                    start=1, end=2, generation=generation, get_tokenizer=lambda: None)
            else:
                results[0].update(invalid)
        return results

    def close(self):
        return dict(status='closed', engine_api_shutdown_confirmed=True,
                    gpu_exit_confirmed=None, cleanup_errors=[])


if __name__ == '__main__':
    with patch.object(vllm_engine, 'VllmEngine', Engine):
        staged_generation_process.serve(int(sys.argv[1]))
