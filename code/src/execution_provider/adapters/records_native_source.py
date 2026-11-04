"""Bind native feedback to its ready owner and conservatively map source age."""
import math


RESOURCE_OWNER_SCHEMA = 'semloom.model_resource_owner.v1'
NATIVE_AGE_SCHEMA = 'semloom.native_sample_age.v1'
PRIVATE_CLOCK = 'private_child_and_model_actor.monotonic'


def _number(value):
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def backend_manages_request_memory(ready):
    if type(ready) is not dict or type(ready.get('engine_uid')) is not str or not ready['engine_uid']:
        return False
    resource = ready.get('resource_owner')
    return (type(resource) is dict and resource.get('schema') == RESOURCE_OWNER_SCHEMA
        and resource.get('engine_uid') == ready['engine_uid']
        and resource.get('request_memory_owner') == 'vllm'
        and resource.get('native_feedback_clock') == PRIVATE_CLOCK
        and ready.get('batch_mode') == 'independent_final_results' and ready.get('closed') is False)


def native_sample_age(native, ready, *, captured_at_s):
    """Called beside the private child on its model actor's host clock.

    No source timestamp is interpreted in the driver's clock. Missing source
    identity/time leaves the age unknown and cannot qualify modern feedback.
    """
    if (not backend_manages_request_memory(ready) or type(native) is not dict
            or native.get('engine_uid') != ready['engine_uid']
            or type(native.get('sequence')) is not int or native['sequence'] < 1
            or not _number(native.get('observed_at_s')) or not _number(captured_at_s)
            or native['observed_at_s'] > captured_at_s):
        return None
    return dict(schema=NATIVE_AGE_SCHEMA, engine_uid=native['engine_uid'],
        source_sequence=native['sequence'], source_observed_at_s=native['observed_at_s'],
        captured_at_s=captured_at_s, age_s=captured_at_s-native['observed_at_s'],
        clock_domain=PRIVATE_CLOCK)


class NativeSourceClock:
    """One native identity per bound query; repeated reads retain mapped time."""
    def __init__(self, engine_uid, session_id):
        if type(engine_uid) is not str or not engine_uid or type(session_id) is not int or session_id < 0:
            raise ValueError('a validated engine and Core session are required')
        self.engine_uid, self.session_id = engine_uid, session_id
        self._sequence = None
        self._source_at = self._mapped_at = None
        self._rejected = False

    def observed_at(self, native, packet, *, issued_at_s, received_at_s):
        if (not all(_number(v) for v in (issued_at_s, received_at_s)) or issued_at_s > received_at_s
                or type(packet) is not dict or packet.get('session_id') != self.session_id
                or type(packet.get('session_id')) is not int or type(native) is not dict
                or native.get('engine_uid') != self.engine_uid
                or type(native.get('sequence')) is not int or native['sequence'] < 1
                or not _number(native.get('observed_at_s'))):
            raise ValueError('native source identity or driver RPC timing is invalid')
        sequence, source_at = native['sequence'], native['observed_at_s']
        if self._sequence is not None:
            if sequence < self._sequence:
                raise ValueError('native source sequence moved backwards')
            if sequence == self._sequence and (self._rejected or source_at != self._source_at):
                self._rejected = True
                raise ValueError('native source identity changed or was already rejected')
        age = packet.get('native_sample_age')
        valid = (type(age) is dict and age.get('schema') == NATIVE_AGE_SCHEMA
            and age.get('engine_uid') == self.engine_uid and age.get('source_sequence') == sequence
            and type(age.get('source_sequence')) is int and age.get('source_observed_at_s') == source_at
            and age.get('clock_domain') == PRIVATE_CLOCK
            and _number(age.get('captured_at_s')) and _number(age.get('age_s'))
            and age['captured_at_s'] >= source_at
            and age['captured_at_s']-source_at == age['age_s'])
        mapped = None if not valid else issued_at_s-age['age_s']
        if not _number(mapped):
            if self._sequence is None or sequence > self._sequence:
                self._sequence, self._source_at, self._mapped_at = sequence, source_at, None
            self._rejected = True
            raise ValueError('native source age is unknown or invalid')
        if sequence == self._sequence:
            return self._mapped_at
        self._sequence, self._source_at, self._mapped_at = sequence, source_at, mapped
        self._rejected = False
        return mapped
