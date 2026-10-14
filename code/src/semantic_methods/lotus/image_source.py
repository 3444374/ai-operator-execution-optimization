"""Verify the pinned native implementation without changing its source."""
import hashlib
from importlib.metadata import version
from pathlib import Path

LOTUS_COMMIT = "b1a85fd7a66fabed8a1585d44d7597d592b4433f"
LOTUS_IMAGE_HASHES = {
    "models/rm.py": "b80e484e38183ba4e9c2738b1239407842902820186f41408ea55394f48e368a",
    "models/sentence_transformers_rm.py": "9c5687c5647b11e98e81a0da2d2328648806944d26ff92c070bb1ff313b23742",
    "dtype_extensions/image.py": "f17e136923f4d2d24dd2dc64a80a919b4503a8649276545a65fec22357c5b743",
    "dtype_extensions/__init__.py": "757c8a2f23cebad55f16ccc9675849f3e942461d104dba13694fe9e530b7ffcc",
    "sem_ops/sem_index.py": "0f37ac28e5a313c3d61d9a45a1b4fe11b1d618836e4d64dc9c5f034e22a05820",
    "sem_ops/sem_search.py": "6c2900f9cdd1c811ec7e6487c5bb4053e056a6996a28a57abd715ea4ba6925de",
    "sem_ops/load_sem_index.py": "01f30d66d2308c58013bfc0f2e29d4e72e8e6182e83f409bbeea7d2dae67ac3b",
    "vector_store/faiss_vs.py": "694322929f9535ff0ee86ad41adf50d8a3b44eb3da61f2afbb24dc6243074181",
    "cache.py": "6341c5e3fc42fdde1359ec9b0a2dcbc3e2902fad53be228c289d8cb2920a477d",
    "settings.py": "1724d0820190e6585fa3504946734eb5964b7b1d8ea727540ac49815f186bf6e",
}


def validate_image_source():
    if version("lotus-ai") != "1.2.4":
        raise ValueError("native image retrieval requires LOTUS 1.2.4")
    import lotus
    root = Path(lotus.__file__).parent
    for path, digest in LOTUS_IMAGE_HASHES.items():
        if hashlib.sha256((root / path).read_bytes()).hexdigest() != digest:
            raise ValueError("native LOTUS image source differs at " + path)
    return {"commit": LOTUS_COMMIT, "version": "1.2.4", "source_sha256": LOTUS_IMAGE_HASHES}
