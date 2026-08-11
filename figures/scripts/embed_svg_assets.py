#!/usr/bin/env python3
"""Embed local SVG image references as data URIs.

This keeps a curated SVG portable when it is copied into PowerPoint or Word.
Only local relative ``href``/``xlink:href`` image references are rewritten;
fragments, existing data URIs, and network URLs are left unchanged.
"""

from __future__ import annotations

import argparse
import base64
import mimetypes
import os
from pathlib import Path
import re


HREF_RE = re.compile(r'(?P<attr>(?:xlink:)?href)="(?P<ref>[^"]+)"')
SKIPPED_PREFIXES = ("#", "data:", "http://", "https://", "mailto:")


def embed_file(svg_path: Path) -> int:
    source = svg_path.read_text(encoding="utf-8")
    embedded = 0

    def replace(match: re.Match[str]) -> str:
        nonlocal embedded
        ref = match.group("ref")
        if ref.startswith(SKIPPED_PREFIXES):
            return match.group(0)

        asset_path = (svg_path.parent / ref).resolve()
        if not asset_path.is_file():
            raise FileNotFoundError(f"{svg_path}: missing referenced asset {ref}")

        mime_type, _ = mimetypes.guess_type(asset_path.name)
        if mime_type is None:
            raise ValueError(f"{svg_path}: unknown MIME type for {ref}")
        payload = base64.b64encode(asset_path.read_bytes()).decode("ascii")
        embedded += 1
        return f'{match.group("attr")}="data:{mime_type};base64,{payload}"'

    output = HREF_RE.sub(replace, source)
    if embedded:
        temporary = svg_path.with_suffix(svg_path.suffix + ".tmp")
        temporary.write_text(output, encoding="utf-8")
        os.replace(temporary, svg_path)
    return embedded


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Embed relative SVG image assets as data URIs in place."
    )
    parser.add_argument("svg", nargs="+", type=Path, help="SVG file(s) to make portable")
    args = parser.parse_args()

    total = 0
    for svg_path in args.svg:
        if svg_path.suffix.lower() != ".svg":
            raise ValueError(f"expected .svg input: {svg_path}")
        count = embed_file(svg_path)
        total += count
        print(f"{svg_path}: embedded {count} asset(s)")
    print(f"embedded assets: {total}")


if __name__ == "__main__":
    main()
