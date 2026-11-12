"""Execute the pinned upstream encoder-cache state machine on CPU request stubs."""
import ast
from collections import OrderedDict
from collections.abc import Mapping
import hashlib
import logging
from pathlib import Path
from types import SimpleNamespace

from src.research.encoder_retention import prioritize_freeable_encoder


def probe(source, expected_sha256):
    raw = Path(source).read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected_sha256:
        raise ValueError('upstream encoder source does not match the selected revision')
    tree = ast.parse(raw)
    definition = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'EncoderCacheManager')
    future = ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)
    module = ast.fix_missing_locations(ast.Module(body=[future, definition], type_ignores=[]))
    namespace = dict(OrderedDict=OrderedDict, Mapping=Mapping, logger=logging.getLogger(__name__))
    exec(compile(module, str(source), 'exec'), namespace)
    manager_type = namespace['EncoderCacheManager']

    class Request:
        def __init__(self, key):
            self.request_id = 'request-' + key
            self.mm_features = [SimpleNamespace(identifier=key)]
        def get_num_encoder_embeds(self, input_id):
            return 4

    arms = []
    for name, hints, keep_active in [('native', {}, False), ('future_value', {'A':100, 'B':1}, False),
                                    ('active_reference', {'A':0, 'B':100}, True)]:
        manager = manager_type(cache_size=8)
        a, b, c = Request('A'), Request('B'), Request('C')
        for request in (a, b):
            if not manager.can_allocate(request, 0, 4, 0):
                raise AssertionError('initial native allocation unexpectedly rejected')
            manager.allocate(request, 0)
            if request is b or not keep_active:
                manager.free_encoder_input(request, 0)
        before = tuple(manager.freeable)
        prioritize_freeable_encoder(manager, hints)
        selected_order = tuple(manager.freeable)
        if manager.can_allocate(c, 0, 3, 0) or manager.freed:
            raise AssertionError('hint changed the native encoder compute budget')
        if not manager.can_allocate(c, 0, 4, 0):
            raise AssertionError('native reclaimable capacity was lost')
        manager.allocate(c, 0)
        evicted = manager.get_freed_mm_hashes()
        expected = ['A'] if name == 'native' else ['B']
        if evicted != expected or keep_active and manager.cached['A'] != {'request-A'}:
            raise AssertionError('wrong reclaimable entry or active reference changed')
        if manager.num_freeable_slots != manager.num_free_slots + sum(manager.freeable.values()):
            raise AssertionError('native capacity accounting changed')
        arms.append(dict(arm=name, before=before, selected_order=selected_order, evicted=evicted,
                         active_a_preserved=keep_active and manager.cached['A'] == {'request-A'},
                         free_slots=manager.num_free_slots, reclaimable_slots=manager.num_freeable_slots))
    return dict(schema='semloom.encoder_retention_probe.v1', upstream_sha256=digest, arms=arms,
                source_type='pinned_native_class_with_CPU_request_stubs', model_calls=0, gpu=False,
                scope='Native bookkeeping and selected victims only. No physical encoder tensors, real consumer hints, process integration or performance result.')
