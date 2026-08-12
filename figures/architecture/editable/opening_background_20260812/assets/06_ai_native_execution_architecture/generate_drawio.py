#!/usr/bin/env python3
"""Generate the clean editable Draw.io source from the reviewed publication SVG.

The SVG remains the deterministic publication view. This generator expresses the
same visible structure as native Draw.io rectangles, text and connectors, with
each icon retained as an independent SVG asset.
"""

from __future__ import annotations

import base64
import html
import re
from pathlib import Path
from xml.etree import ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
SVG = ROOT / "06_ai_native_execution_architecture.svg"
OUT = ROOT / "06_ai_native_execution_architecture.drawio"
ASSETS = ROOT / "assets" / "06_ai_native_execution_architecture"

NS = {"s": "http://www.w3.org/2000/svg"}


def esc(s: str) -> str:
    return html.escape(s, quote=True)


def style_of(el: ET.Element) -> str:
    value = el.attrib.get("style", "")
    classes = el.attrib.get("class", "")
    class_map = {
        "title": "font-size:40px;font-weight:700;fill:#111827",
        "section": "font-size:28px;font-weight:700",
        "cardtitle": "font-size:25px;font-weight:700",
        "body": "font-size:23px;fill:#374151",
        "small": "font-size:22px;fill:#6B7280",
        "pill": "font-size:22px;font-weight:700",
    }
    for name in classes.split():
        value += ";" + class_map.get(name, "")
    for key in ("font-size", "font-weight", "fill", "text-anchor"):
        if key in el.attrib:
            value += f";{key}:{el.attrib[key]}"
    return value


def prop(style: str, key: str, default: str) -> str:
    matches = re.findall(rf"(?:^|;)\s*{re.escape(key)}\s*:\s*([^;]+)", style)
    return matches[-1].strip() if matches else default


def text_content(el: ET.Element) -> str:
    direct = (el.text or "").strip()
    tspans = el.findall("s:tspan", NS)
    if not tspans:
        return direct
    pieces = []
    if direct:
        pieces.append(direct)
    pieces.extend("".join(t.itertext()).strip() for t in tspans)
    return "&#xa;".join(p for p in pieces if p)


def rgba_fill(el: ET.Element, default: str = "#FFFFFF") -> str:
    fill = el.attrib.get("fill", default)
    if fill == "none":
        return "none"
    return fill


def geometry(x: float, y: float, w: float, h: float) -> str:
    return f'<mxGeometry x="{x:g}" y="{y:g}" width="{w:g}" height="{h:g}" as="geometry" />'


def main() -> None:
    doc = ET.parse(SVG)
    svg = doc.getroot()
    cells: list[str] = []
    cid = 2

    def add_vertex(value: str, style: str, x: float, y: float, w: float, h: float) -> None:
        nonlocal cid
        cells.append(
            f'<mxCell id="{cid}" value="{esc(value)}" style="{esc(style)}" vertex="1" parent="1">'
            f'{geometry(x, y, w, h)}</mxCell>'
        )
        cid += 1

    def add_edge(style: str, x1: float, y1: float, x2: float, y2: float) -> None:
        nonlocal cid
        cells.append(
            f'<mxCell id="{cid}" value="" style="{esc(style)}" edge="1" parent="1">'
            f'<mxGeometry relative="1" as="geometry"><mxPoint x="{x1:g}" y="{y1:g}" as="sourcePoint"/>'
            f'<mxPoint x="{x2:g}" y="{y2:g}" as="targetPoint"/></mxGeometry></mxCell>'
        )
        cid += 1

    # Visible shapes in painter order. SVG paths used for arrows become native connectors.
    for el in svg.iter():
        tag = el.tag.rsplit("}", 1)[-1]
        if tag == "rect":
            x = float(el.attrib.get("x", 0)); y = float(el.attrib.get("y", 0))
            w = float(el.attrib.get("width", 0)); h = float(el.attrib.get("height", 0))
            if w == 1600 and h == 900:
                continue
            fill = rgba_fill(el); stroke = el.attrib.get("stroke", "none")
            sw = el.attrib.get("stroke-width", "1")
            dashed = "dashed=1;dashPattern=8 5;" if el.attrib.get("stroke-dasharray") else ""
            rx = float(el.attrib.get("rx", 0)); arc = min(30, int(100 * rx / max(1, min(w, h))))
            style = (
                f"rounded={1 if rx else 0};arcSize={arc};whiteSpace=wrap;html=1;"
                f"fillColor={fill};strokeColor={stroke};strokeWidth={sw};shadow=0;{dashed}"
            )
            add_vertex("", style, x, y, w, h)
        elif tag == "text":
            value = text_content(el)
            if not value:
                continue
            st = style_of(el)
            size = float(prop(st, "font-size", "23px").replace("px", ""))
            weight = prop(st, "font-weight", "400")
            color = prop(st, "fill", "#374151")
            anchor = prop(st, "text-anchor", "start")
            align = {"middle": "center", "end": "right"}.get(anchor, "left")
            x = float(el.attrib.get("x", 0)); y = float(el.attrib.get("y", 0))
            lines = max(1, value.count("&#xa;") + 1)
            h = max(size * 1.45 * lines, size + 10)
            # Wide boxes keep Draw.io text editable; clamp them to the page for
            # centered/right aligned labels near the edges.
            preferred = 720 if size >= 28 else 520
            if anchor == "middle":
                w = min(preferred, max(120, 2 * min(x, 1600 - x) - 16))
            elif anchor == "end":
                w = min(preferred, max(120, x - 16))
            else:
                w = min(preferred, max(120, 1600 - x - 16))
            if align == "center":
                tx = x - w / 2
            elif align == "right":
                tx = x - w
            else:
                tx = x
            ty = y - size * 1.15
            style = (
                "text;html=1;whiteSpace=wrap;overflow=hidden;strokeColor=none;fillColor=none;"
                f"fontFamily=Microsoft YaHei;fontSize={size:g};fontColor={color};"
                f"fontStyle={1 if weight in ('700', 'bold') else 0};align={align};verticalAlign=middle;spacing=0;"
            )
            add_vertex(value.replace("&#xa;", "\n"), style, tx, ty, w, h)
        elif tag == "image":
            name = el.attrib.get("data-icon")
            if not name:
                continue
            asset = ASSETS / f"{name}.svg"
            encoded = base64.b64encode(asset.read_bytes()).decode("ascii")
            image_data = f"data:image/svg+xml;base64,{encoded}"
            x = float(el.attrib["x"]); y = float(el.attrib["y"])
            w = float(el.attrib["width"]); h = float(el.attrib["height"])
            style = f"shape=image;imageAspect=0;aspect=fixed;image={image_data};"
            add_vertex("", style, x, y, w, h)
        elif tag == "path" and "marker-end" in el.attrib:
            d = el.attrib.get("d", "")
            start = re.search(r"M\s*(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)", d)
            if not start:
                continue
            x1, y1 = float(start.group(1)), float(start.group(2))
            x2, y2 = x1, y1
            waypoints: list[tuple[float, float]] = []
            tail = d[start.end():]
            for cmd, a, b in re.findall(r"([HVL])\s*(-?\d+(?:\.\d+)?)(?:\s+(-?\d+(?:\.\d+)?))?", tail):
                if cmd == "H": x2 = float(a)
                elif cmd == "V": y2 = float(a)
                elif cmd == "L" and b: x2, y2 = float(a), float(b)
                waypoints.append((x2, y2))
            color = el.attrib.get("stroke", "#165DCC")
            sw = el.attrib.get("stroke-width", "3")
            dashed = "dashed=1;dashPattern=8 6;" if el.attrib.get("stroke-dasharray") else ""
            style = f"endArrow=block;endFill=1;html=1;rounded=0;strokeColor={color};strokeWidth={sw};{dashed}"
            nonlocal_cid = cid
            points_xml = ""
            if len(waypoints) > 1:
                points = waypoints[:-1]
                points_xml = '<Array as="points">' + ''.join(
                    f'<mxPoint x="{px:g}" y="{py:g}"/>' for px, py in points
                ) + '</Array>'
            cells.append(
                f'<mxCell id="{nonlocal_cid}" value="" style="{esc(style)}" edge="1" parent="1">'
                f'<mxGeometry relative="1" as="geometry"><mxPoint x="{x1:g}" y="{y1:g}" as="sourcePoint"/>'
                f'<mxPoint x="{x2:g}" y="{y2:g}" as="targetPoint"/>{points_xml}</mxGeometry></mxCell>'
            )
            cid += 1

    xml = """<?xml version="1.0" encoding="UTF-8"?>
<mxfile host="app.diagrams.net" modified="2026-08-12T00:00:00.000Z" agent="Codex drawio-reconstruction" version="24.7.17" type="device">
  <diagram id="06-ai-native-execution" name="Page-1">
    <mxGraphModel dx="1600" dy="900" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="1600" pageHeight="900" math="0" shadow="0">
      <root><mxCell id="0"/><mxCell id="1" parent="0"/>\n""" + "\n".join(cells) + """
      </root>
    </mxGraphModel>
  </diagram>
</mxfile>
"""
    OUT.write_text(xml, encoding="utf-8")
    print(OUT)


if __name__ == "__main__":
    main()
