"""Finite immutable source bytes shared by the stage A fixture arms."""

from dataclasses import dataclass
import hashlib
from io import BytesIO

from PIL import Image


@dataclass(frozen=True)
class ImageSourceSnapshot:
    encoded: bytes
    content_digest: str
    rgb_bytes: int

    def fetch(self, fetch_image, image_type="base64"):
        # LOTUS already accepts PIL images; reuse its RGB/PNG transform rather
        # than reopening a mutable path after validating the source version.
        with Image.open(BytesIO(self.encoded)) as image:
            return fetch_image(image, image_type)


def read_image_snapshot(path, expected_sha256, encoded_bytes_upper_bound):
    if type(encoded_bytes_upper_bound) is not int or encoded_bytes_upper_bound < 1:
        raise ValueError("snapshot requires a positive encoded byte limit")
    with path.open("rb") as source:
        encoded = source.read(encoded_bytes_upper_bound + 1)
    if len(encoded) > encoded_bytes_upper_bound:
        raise BufferError("image source exceeds its encoded snapshot limit")
    digest = hashlib.sha256(encoded).hexdigest()
    if digest != expected_sha256:
        raise ValueError("historical image source changed")
    with Image.open(BytesIO(encoded)) as image:
        if getattr(image, "n_frames", 1) != 1:
            raise ValueError("historical image is not single-frame")
        rgb_bytes = image.width * image.height * 3
    return ImageSourceSnapshot(encoded, digest, rgb_bytes)
