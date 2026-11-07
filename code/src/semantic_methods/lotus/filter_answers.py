"""Filter answer semantics shared by physical execution backends."""

from dataclasses import dataclass
import re


LOTUS_NATIVE = "lotus-native"
ANSWER_FIELD = "answer-field-v1"
FILTER_PARSERS = (LOTUS_NATIVE, ANSWER_FIELD)

_ANSWER = re.compile(r"^[ \t]*Answer[ \t]*:[ \t]*([^\r\n]*)\r?$",
                     re.IGNORECASE | re.MULTILINE)
_BARE = re.compile(r"(True|False)[.!。]?", re.IGNORECASE)


@dataclass
class FilterAnswers:
    raw_outputs: list[str]
    outputs: list[bool]
    explanations: list[str]


def parse_filter_answers(texts, model, default=True, *, output_tokens=("True", "False"),
                         answer_parser=LOTUS_NATIVE):
    """Interpret a validated Filter policy; keep the installed native choice intact.

    The corrected format is one Answer line containing a boolean, or a bare
    boolean. Explanation words are never answers. Multiple Answer lines and
    unrecognized text use the method's explicit default.
    """
    if answer_parser == LOTUS_NATIVE:
        from lotus.sem_ops.postprocessors import filter_postprocess

        return filter_postprocess(texts, model, default, output_tokens=output_tokens)

    texts = list(texts)
    outputs, explanations = [], []
    for text in texts:
        matches = list(_ANSWER.finditer(text))
        if matches:
            answer = _BARE.fullmatch(matches[0][1].strip()) if len(matches) == 1 else None
        else:
            answer = _BARE.fullmatch(text.strip())
        outputs.append(default if answer is None else answer[1].lower() == "true")
        explanations.append(text[:matches[0].start()].strip() if len(matches) == 1 else text)
    return FilterAnswers(texts, outputs, explanations)
