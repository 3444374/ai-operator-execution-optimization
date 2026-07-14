# projects/

本目录是 PPT 自动生成工程的工作目录。当前只有 2026-07-12 开题答辩 PPT 的生成项目。

## 目录结构

```
projects/opening_defense_20260712/
├── analysis/             ← 学校 PPT 模板结构解析、填充计划
│   ├── build_fill_plan.py
│   ├── check_report.json
│   ├── fill_plan.json
│   ├── summarize_template.py
│   └── template.slide_library.json
├── validation/           ← PPTX 版式检查、边界审计、内容读回
│   ├── fix_bounds.py
│   ├── inspect_bounds.py
│   ├── pptx_audit.json / .md
│   ├── pptx_audit_fixed.json / .md
│   ├── readback.md
│   ├── readback_files/
│   └── validate_report.json
└── exports/              ← 生成的 PPTX 导出
    ├── opening_defense_20260712_200753.pptx
    ├── opening_defense_20260712_201257.pptx
    └── opening_defense_20260712_fixed.pptx
```

## 与正式材料的关系

- 正式 PPTX 放在 `opening/slides/opening_defense_20260712.pptx`。
- 正式图资产放在 `figures/`。
- 本目录的工具链用于生成和验证，不是最终交付目录。

## 注意

本目录的 PPTX 和脚本是旧版（2026-07-12），当前内容和形式已作废。后续基于新报告重做 PPT 时，工具链经验可参考，但应重新生成。
