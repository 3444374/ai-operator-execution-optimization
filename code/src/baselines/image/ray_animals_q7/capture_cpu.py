def main(query_id=7):
    """Capture original image Filters and their full sink without any model."""
    import base64
    from copy import deepcopy
    import gzip
    import hashlib
    from io import BytesIO
    import json
    import os
    from pathlib import Path
    from unittest.mock import patch
    from PIL import Image
    import httpx
    from .binding import run
    from src.baselines.image.sembench_animals import AnimalsDataset, run_original_query
    assert os.environ.get('CUDA_VISIBLE_DEVICES') == ''
    root = Path(os.environ['Q7_OUTPUT'])
    root.mkdir(parents=True, exist_ok=False)
    manifest = Path(os.environ['Q7_MANIFEST'])
    value = json.loads(manifest.read_text())
    dataset = AnimalsDataset(Path(os.environ['Q7_CHECKOUT']), manifest, value['sha256'])
    count = len(value['rows'])
    source = dataset.preflight(query_id)
    expected_passes = source['native_filter_passes']
    messages = []
    occurrences = []
    masks = []
    answers = ['True', 'False', 'no verdict', 'False then True']

    def execute(rows):
        stage = len(masks)
        assert 0 <= stage < expected_passes
        for item, source in zip(rows, value['rows'], strict=True):
            message = deepcopy(item['messages'])
            messages.append(message)
            content = [part for msg in message if isinstance(msg['content'], list) for part in msg['content']]
            images = [part['image_url']['url'] for part in content if part.get('type') == 'image_url']
            text = ' '.join((part.get('text', '') for part in content if part.get('type') == 'text'))
            assert 'Species' not in text and source['ImagePath'] not in text
            assert len(images) == 1 and images[0].startswith('data:image/png;base64,')
            png = base64.b64decode(images[0].split(',', 1)[1])
            with Image.open(BytesIO(png)) as image:
                rgb = hashlib.sha256(image.convert('RGB').tobytes()).hexdigest()
            assert rgb == source['image']['rgb_sha256']
            occurrences.append(dict(row_id=source['row_id'], call_id=item['call_id'], stage=stage, source_image=source['ImagePath'], source_sha256=source['image']['sha256'], RGB_sha256=rgb, PNG_sha256=hashlib.sha256(png).hexdigest()))
        from lotus.models import LM
        from lotus.sem_ops.postprocessors import filter_postprocess
        mask = filter_postprocess([answers[(i + stage) % 4] for i in range(count)], LM('openai/CPU-parser', cache={}), default=True).outputs
        assert len(mask) == count and all((type(x) is bool for x in mask))
        masks.append(mask)
        return [dict(call_id=row['call_id'], generated_text=answers[(i + stage) % 4]) for i, row in reversed(list(enumerate(rows)))]

    def forbidden(*args, **kwargs):
        raise AssertionError('HTTP forbidden in CPU capture')
    with patch.object(httpx.Client, 'request', forbidden), patch.object(httpx.AsyncClient, 'request', forbidden):
        result, quality, passes, timing = run(dataset, execute, query_id=query_id)
        # Direct original-SDK execution is independent of the binding's
        # reversed occurrence results and reconciliation.
        import lotus
        import pandas as pd
        from lotus.models import LM
        from lotus.types import LMOutput
        reference_passes=[]
        class ReferenceLM(LM):
            def __call__(self, batch, *args, **kwargs):
                stage=len(reference_passes)
                assert batch==messages[stage*count:(stage+1)*count]
                reference_passes.append(stage)
                return LMOutput(outputs=[answers[(i+stage)%4] for i in range(count)])
        with lotus.settings.context(lm=ReferenceLM('openai/native-ray-binding',cache={}),enable_cache=False):
            reference=run_original_query(dataset.checkout,dataset.frame,query_id)
        pd.testing.assert_frame_equal(result,reference)
    assert len(messages)==source['expected_model_calls'] and len(passes)==len(reference_passes)==expected_passes
    with gzip.open(root / 'MESSAGES-PRIVATE.json.gz', 'wt') as out:
        json.dump(messages, out)
    (root / 'OCCURRENCES.json').write_text(json.dumps(occurrences, indent=2) + '\n')
    (root / 'CPU-NATIVE-CAPTURE.json').write_text(json.dumps(dict(status='passed', scope='software fixture only', preflight=source, passes=passes, masks=masks, complete_original_sink_match=True, timing=timing, result=result.to_dict(orient='split'), quality=quality, actual_model_calls=0, images=len(messages)), indent=2) + '\n')
if __name__ == '__main__':
    import argparse
    from src.baselines.image.sembench_animals import IMAGE_QUERY_IDS
    parser = argparse.ArgumentParser(description='Zero-model Animals Filter qualification; environment: adjacent README.md')
    parser.add_argument('--query',type=int,choices=IMAGE_QUERY_IDS,default=7)
    args=parser.parse_args()
    main(args.query)
