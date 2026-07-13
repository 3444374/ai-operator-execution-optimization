import json
from pathlib import Path

data = json.loads(Path("projects/opening_defense_20260712/analysis/template.slide_library.json").read_text(encoding="utf-8"))

for slide in data["slides"]:
    slots = slide.get("slots", [])
    tables = slide.get("tables", [])
    charts = slide.get("charts", [])
    print(
        f"slide {slide['slide_index']} type={slide.get('page_type')} "
        f"slots={len(slots)} tables={len(tables)} charts={len(charts)}"
    )
    for slot in slots[:10]:
        text = (slot.get("text") or "").replace("\n", " ")
        print(f"  {slot.get('slot_id')} role={slot.get('role')} text={text[:80]}")
    for table in tables:
        print(f"  table {table.get('table_id')} rows={table.get('row_count')} cols={table.get('col_count')}")
        for cell in (table.get("cells") or [])[:8]:
            text = (cell.get("text") or "").replace("\n", " ")
            print(f"    r{cell.get('row')} c{cell.get('col')}: {text[:60]}")
