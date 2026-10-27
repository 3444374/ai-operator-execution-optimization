"""Explicit prepared-message estimates over the existing Core work capacity.

W bounds declared reservations, not actual KV bytes or model compute. The input
estimate uses the caller's pinned tokenizer and one full chat template. Native
renderer parity still needs separate verification on the model environment.
"""

from dataclasses import asdict, dataclass
import json
import hashlib
from types import MappingProxyType

from src.modalities.text.tokenization import chat_token_count
from src.planning.work import StageWork, WorkDescriptor
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, _prepared
def fingerprint(value, *, maximum_bytes=None):
    digest, size = hashlib.sha256(), 0
    for piece in json.JSONEncoder(sort_keys=True, ensure_ascii=False,
                                 allow_nan=False, separators=(",", ":")).iterencode(value):
        encoded = piece.encode()
        size += len(encoded)
        if maximum_bytes is not None and size > maximum_bytes:
            raise ValueError('token fixture exceeds its serialized byte bound')
        digest.update(encoded)
    return digest.hexdigest()


def _digest(value):
    return type(value) is str and len(value) == 64 and all(c in '0123456789abcdef' for c in value)


def _positive(value):
    return type(value) is int and 0 < value <= (1 << 64)-1



@dataclass(frozen=True)
class OutputTokenEstimate:
    tokens: int
    provenance: str

    def __post_init__(self):
        if (type(self.tokens) is not int or self.tokens < 0
                or type(self.provenance) is not str or not self.provenance
                or len(self.provenance.encode()) > 256):
            raise ValueError("expected output needs a nonnegative count and explicit provenance")


def audit_chain_call(expected, result=None):
    """Keep the existing chain namespace and reuse records' usage validation."""
    from src.semantic_methods.lotus.record_chain import RecordOccurrence, stage_identity
    from src.semantic_methods.lotus.records import CallLedger, identity
    from src.serving.backends.vllm_engine import IDENTITY_FIELDS
    record = RecordOccurrence(expected["row_id"], expected["source_position"], 0, {})
    declared = stage_identity(expected["query_id"], record, expected["algorithm_stage"],
        model_role=expected["model_role"], call_index=expected["call_index"])
    if {name:expected[name] for name in IDENTITY_FIELDS} != declared:
        raise ValueError("prepared identity differs from its method's stage identity")
    if result is None:
        return
    if type(result) is not dict or any(result.get(name) != declared[name] for name in IDENTITY_FIELDS):
        raise ValueError("result differs from its prepared chain identity")
    # Normalization is local to validation. Original call IDs are never rewritten
    # in the method, Core, actor, IPC, native result or durable receipt.
    canonical = identity(expected["query_id"], expected["row_id"], expected["source_position"],
        expected["call_index"], model_role=expected["model_role"], algorithm_stage=expected["algorithm_stage"])
    CallLedger([canonical], 1).record(dict(result, call_id=canonical["call_id"]))


def verify_cost(row, generation, cost, *, engine):
    """Associate an estimate with exactly the messages/identity/options submitted.

    This validates transfer and units; it is not proof of native token IDs.
    """
    _prepared([row])
    keys = {"scope", "input_tokens", "expected_output_tokens", "output_cap",
            "reserved_tokens", "output_estimate_provenance", "tokenizer_signature",
            "template_signature", "engine_signature", "prepared_sha256"}
    if (type(cost) is not dict or set(cost) != keys
            or cost["scope"] != "driver_tokenizer_estimate_not_native_usage"
            or not _positive(cost["input_tokens"]) or not _positive(cost["output_cap"])
            or type(cost["expected_output_tokens"]) is not int
            or not 0 <= cost["expected_output_tokens"] <= cost["output_cap"]
            or type(cost["reserved_tokens"]) is not int
            or not _positive(cost["reserved_tokens"])
            or cost["reserved_tokens"] != cost["input_tokens"] + cost["output_cap"]
            or cost["output_cap"] != generation.max_tokens
            or type(cost["output_estimate_provenance"]) is not str
            or not cost["output_estimate_provenance"]
            or len(cost["output_estimate_provenance"].encode()) > 256
            or any(not _digest(cost[name]) for name in
                   ("tokenizer_signature", "template_signature", "engine_signature", "prepared_sha256"))
            or cost["engine_signature"] != fingerprint(asdict(engine))
            or cost["prepared_sha256"] != fingerprint(dict(row=row, generation=asdict(generation)))):
        raise ValueError("prepared token estimate changed identity, messages, generation or units")


class ChainTokenBudget:
    """Per-request estimation; no source scan, extra inference, batching or ledger.

    Expected output is a declared/calibrated diagnostic, independently of the
    conservative generation-cap reservation. Completion never refunds tokens
    directly: the existing Core terminal/uncertain protocol owns that operation.
    """

    def __init__(self, tokenizer, engine, *, active_tokens, tokenizer_signature,
                 template_signature, expected_output):
        if type(engine) is not EngineSpec or not _positive(active_tokens):
            raise ValueError("a model configuration and positive W are required")
        if not _digest(tokenizer_signature) or not _digest(template_signature):
            raise ValueError("tokenizer and template need explicit SHA256 signatures")
        if (type(expected_output) is not dict or not expected_output
                or any(type(k) is not str or not k or type(v) is not OutputTokenEstimate
                       for k, v in expected_output.items())):
            raise ValueError("each configured stage needs an explicit output estimate")
        self.tokenizer, self.engine, self.active_tokens = tokenizer, engine, active_tokens
        self.tokenizer_signature, self.template_signature = tokenizer_signature, template_signature
        self.expected_output = MappingProxyType(dict(expected_output))
        self.signature = fingerprint(dict(engine=asdict(engine), tokenizer=tokenizer_signature,
            template=template_signature,
            output={k:asdict(v) for k,v in self.expected_output.items()}))

    def estimate(self, row, generation):
        _, conversations = _prepared([row])
        expected = self.expected_output[row["algorithm_stage"]]
        if expected.tokens > generation.max_tokens:
            raise ValueError("expected output cannot exceed the generation cap")
        prepared_sha = fingerprint(dict(row=row, generation=asdict(generation)))
        count = chat_token_count(self.tokenizer, conversations[0])
        cost = dict(scope="driver_tokenizer_estimate_not_native_usage", input_tokens=count,
            expected_output_tokens=expected.tokens, output_cap=generation.max_tokens,
            reserved_tokens=count + generation.max_tokens,
            output_estimate_provenance=expected.provenance,
            tokenizer_signature=self.tokenizer_signature, template_signature=self.template_signature,
            engine_signature=fingerprint(asdict(self.engine)), prepared_sha256=prepared_sha)
        verify_cost(row, generation, cost, engine=self.engine)
        if cost["reserved_tokens"] > min(self.active_tokens, self.engine.max_model_len):
            raise ValueError("one prepared request exceeds W or the model context capacity")
        return cost

    def validate(self, row, generation, cost):
        verify_cost(row, generation, cost, engine=self.engine)
        expected = self.expected_output[row["algorithm_stage"]]
        if (cost["tokenizer_signature"] != self.tokenizer_signature
                or cost["template_signature"] != self.template_signature
                or cost["expected_output_tokens"] != expected.tokens
                or cost["output_estimate_provenance"] != expected.provenance
                or cost["reserved_tokens"] > min(self.active_tokens, self.engine.max_model_len)):
            raise ValueError("prepared estimate does not belong to this token configuration")

    def describe_work(self, identity, stage, request):
        value = json.loads(request.payload)
        cost = value["token_cost"]
        self.validate(value["row"], GenerationSpec.from_dict(value["generation"]), cost)
        if request.estimated_work != cost["reserved_tokens"]:
            raise ValueError("Core work must equal the prepared token reservation")
        return WorkDescriptor((StageWork("reservation", request.estimated_work, "tokens"),
            StageWork("input_estimate", cost["input_tokens"], "input_tokens"),
            StageWork("expected_output", cost["expected_output_tokens"], "output_tokens"),
            StageWork("output_cap", cost["output_cap"], "output_tokens")),
            "reservation", self.signature)
