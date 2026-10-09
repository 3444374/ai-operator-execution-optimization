"""Complete-response observation around native graph HTTP kernels."""
import base64
from dataclasses import dataclass
import hashlib
import json
import time

from src.baselines.text.frameworks.prepared_map import RESPONSE_FIELD
from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.ray_map_transport import _clock_domain
from src.experiments.native_http_observer import NativeSessionFactory, ObservedSession, ObservedResponse
from src.experiments.request_identity import RAY_IDENTITY_FIELD


@dataclass(frozen=True)
class PreparedSessionFactory(NativeSessionFactory):
    calls: tuple
    max_response_bytes: int = 1048576

    def __call__(self):
        return PreparedObservedSession(self)


class PreparedObservedSession(ObservedSession):
    async def __aenter__(self):
        await super().__aenter__()
        self.session = _RawSession(self.session)
        return self

    async def post(self, url, *, data, headers):
        request = json.loads(data)
        identity = request.get(RAY_IDENTITY_FIELD)
        call = next((c for c in self.config.calls if identity == dict(
            row_id=c['row_id'],source_position=c['source_position'])),None)
        if call is None:
            raise ValueError('native HTTP kernel lost the prepared call occurrence')
        request.pop(RAY_IDENTITY_FIELD)
        from src.baselines.common.private_artifacts import content_digest
        if content_digest(request) != call['request_values_sha256']:
            raise ValueError('native HTTP kernel changed the prepared request values')
        fields = dict(call_id=call['call_id'], row_id=call['row_id'], stage_id=call['stage_id'],
                      clock_domain=_clock_domain())
        self.events.record(dict(event='executor_submit',monotonic_ns=time.monotonic_ns(),**fields))
        response = await super().post(url,data=data,headers=headers)
        self.events.record(dict(event='prepared_http_binding',attempt=response.attempt,**fields))
        return PreparedObservedResponse(response.response,self.events,response.attempt,
            response.identity,fields,self.config.max_response_bytes)


class _RawSession:
    def __init__(self, session):
        self.session = session

    async def post(self, *args, **options):
        # HTTP metadata and response bytes must describe the same representation.
        return await self.session.post(*args,auto_decompress=False,**options)

    async def close(self):
        await self.session.close()


class PreparedObservedResponse(ObservedResponse):
    def __init__(self, response, events, attempt, identity, fields, maximum):
        super().__init__(response,events,attempt,identity)
        self.fields,self.maximum,self.wire = fields,maximum,None

    async def _read_complete(self):
        if self.wire is not None:
            return self.wire
        buffer = bytearray()
        async for chunk in self.response.content.iter_chunked(4096):
            if len(buffer) + len(chunk) > self.maximum:
                raise ValueError('native complete response exceeds its declared byte allowance')
            buffer.extend(chunk)
        body = bytes(buffer)
        full = FullModelResponse(self.status,
            tuple((name.decode('ascii'),value.decode('latin-1')) for name,value in self.response.raw_headers),
            body,'HTTP/'+str(self.response.version.major)+'.'+str(self.response.version.minor))
        wire = encode_full_response(full)
        if len(wire) > self.maximum:
            raise ValueError('native response and metadata exceed the result allowance')
        self.wire = wire
        self.events.record(dict(event='http_body_read',attempt=self.attempt,
            monotonic_ns=time.monotonic_ns(),response_bytes_sha256=hashlib.sha256(body).hexdigest(),
            response_wire=base64.b64encode(self.wire).decode(),status=self.status,**self.fields))
        return self.wire

    async def json(self):
        wire = await self._read_complete()
        return {RESPONSE_FIELD:base64.b64encode(wire).decode(),RAY_IDENTITY_FIELD:self.identity}

    async def __aexit__(self, *error):
        # Native Ray may inspect a failure status before asking for JSON.
        # Preserve that complete HTTP error while keeping its original exception.
        failure = error[1]
        try:
            await self._read_complete()
        except BaseException as observation:
            if failure is None:
                raise
            failure.add_note('Complete native response observation also failed: '+type(observation).__name__)
        finally:
            await super().__aexit__(*error)


def bind_native_http_events(events):
    bindings = {e['attempt']:e for e in events if e.get('event') == 'prepared_http_binding'}
    if len(bindings) != sum(e.get('event') == 'prepared_http_binding' for e in events):
        raise ValueError('duplicate native HTTP attempt binding')
    result = []
    for event in events:
        if event.get('event') == 'http_started' and 'attempt' in event:
            if event['attempt'] not in bindings:
                raise ValueError('native HTTP start lacks its prepared call binding')
            binding = bindings[event['attempt']]
            event = dict(event, **{k:binding[k] for k in ('call_id','row_id','stage_id','clock_domain')})
        result.append(event)
    return result
