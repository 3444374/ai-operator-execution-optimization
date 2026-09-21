# 旧 PPT 生成工程归档

本目录保留 2026-07-12 开题答辩 PPT 的自动生成与验证工程。该版本的内容和形式已经被
`../` 中的后续材料取代；这里不再接受新的开题内容修改。

## 归档内容

```text
opening_defense_20260712/
├── analysis/       # 模板结构解析与填充计划
├── validation/     # 版式检查、边界审计与内容读回
└── exports/        # 当时生成的 PPTX（2026-09-21 迁出 git，见下）
```

2026-09-21 经用户确认：`exports/` 的三份 PPTX 二进制成品从 git 跟踪移除，移除前已逐文件
SHA-256 与 git blob 核对一致，副本归档于本地
`C:\Users\ays\Desktop\results\projects_opening_defense_20260712_exports\`；历史内容同时保留在
git 历史对象中。analysis/ 与 validation/ 的脚本和报告继续保留在 git，供工具链参考。

当前开题入口：

- 正文：`../report/opening_report.md`
- 答辩内容：`../opening_defense_outline_20260808.md`
- 当前幻灯片与设计说明：`../slides/`
- 图资产：`../../figures/opening_figure_set/`

除非需要复现 2026-07-12 的生成过程，否则不要运行或修改本目录脚本。归档中的脚本、校验报告
和读回图片共同构成历史记录，普通清理不删除；PPTX 成品按上方注记存放。
