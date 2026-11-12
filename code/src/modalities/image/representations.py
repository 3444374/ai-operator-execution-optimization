"""Lossless host representations for explicit preparation-location experiments."""
import io


def decode_rgb(encoded):
    from PIL import Image
    with Image.open(io.BytesIO(encoded)) as source:
        rgb = source.convert('RGB')
        return rgb.size, rgb.tobytes()


def prepare_representation(encoded, kind):
    """Return caller-owned bytes and shape; no cache or scheduling is hidden here."""
    if kind == 'encoded':
        return dict(kind=kind, payload=encoded)
    if kind == 'rgb':
        size, pixels = decode_rgb(encoded)
        return dict(kind=kind, size=size, payload=pixels)
    raise ValueError('representation must be encoded or rgb')


def consume_representation(value):
    if value['kind'] == 'encoded':
        return decode_rgb(value['payload'])
    if value['kind'] == 'rgb':
        return tuple(value['size']), value['payload']
    raise ValueError('unknown image representation')
