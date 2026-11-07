"""Observed native LOTUS image sources; model/input identities are checked separately."""

from .source import IMAGE_FILES, LOTUS_COMMIT as LOTUS_COMMIT, source_report


def validate_image_source():
    evidence = source_report("lotus-native-image-v1", IMAGE_FILES)
    return dict(evidence, commit=None, version=evidence["versions"]["lotus-ai"],
                source_sha256=evidence["source_hashes"])
