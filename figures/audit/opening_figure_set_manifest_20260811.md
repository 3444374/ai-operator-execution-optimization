# 开题专用图集清单与审计（2026-08-11）

## 1. 目的

解决 `figures/data/report_main/` 图过多、文件名无法直接映射到开题页面的问题。依据
`opening/opening_defense_outline_20260808.md` 与
`figures/audit/opening_required_data_figures_20260810.md`，建立
`figures/opening_figure_set/` 作为开题材料的单一选图入口。

本次只复制和重命名，不移动或删除权威源，不改实验数据、统计值、图面内容或报告引用。

## 2. 选图结果

- 主讲概念图：5 张，分别承担研究空白、总体闭环、研究内容一、研究内容二、已有基础与后续计划。
- 主讲数据图：9 张，分别承担文本 baseline、work/state/capacity 动机、图像 stage 动机、文本
  原生多 Job 干扰、数据组织 regime、共享调度权衡、代价决策质量、图像 baseline、图像多 Job
  干扰。
- 答辩备份数据图：2 张，只用于单 Job 请求延迟和状态指纹追问。
- 主讲图按照答辩页码命名为 `P05`--`P19`；备份图命名为 `B01`--`B02`。

详细的页码、任务与源文件映射见 `figures/opening_figure_set/README.md`。

## 3. 文件与格式合同

| 分组 | PNG | SVG | Draw.io | 说明 |
|---|---:|---:|---:|---|
| 主讲 | 14 | 14 | 5 | 概念图和数据图均有 PNG/SVG；仅概念图需要 Draw.io |
| 备份 | 2 | 2 | 0 | 数据图由冻结结果脚本生成 |

- 概念图 SVG 使用的独立 icon 同步保存在 `main_svg/assets/`，并由
  `figures/scripts/embed_svg_assets.py` 转为 SVG 内嵌 data URI；因此 SVG 可单文件导入
  PowerPoint/Word，assets 目录仅保留给后续编辑和追溯。
- PNG 用于快速预览和兼容性；PPT/Word 正式排版优先使用 SVG。
- Draw.io 副本保留独立文字、卡片、连接器和 icon；修改后仍须回写权威源目录。
- 权威结果、绘图脚本和逐图审计仍位于原目录，本图集不是第二套事实源。

## 4. 明确排除

以下内容不进入主讲或备份图集：

1. static–dynamic phase-change 示意结果：缺少同上限正式结果；
2. DuckDB 多 Job：只有准备合同，没有正式结果；
3. Daft Built-in 图像 60K×2：容量边界与主排名不一致；
4. ShareGPT database-E2E 三臂性能图：bounded direct 欠供给且 DuckDB cap 语义失败；
5. 跨框架绝对 short JCT：Project 与原生路径的时间边界不一致；
6. 已被当前九张数据图取代的旧版、诊断版或重复图。

## 5. 质检项目

- [x] 14 张主讲 PNG 与 14 张主讲 SVG 一一对应；
- [x] 2 张备份 PNG 与 2 张备份 SVG 一一对应；
- [x] 5 张概念图均提供可编辑 Draw.io；
- [x] 文件名可从页码和中文用途直接识别；
- [x] 概念图 SVG 的相对 icon 资产已复制；
- [x] 原权威文件未移动、未删除；
- [x] 不可比或无正式证据的图未进入图集；
- [x] `figures/README.md`、`PROJECT_INDEX.md`、`PROJECT_OUTLINE.md`、根 `README.md` 与
  `PROJECT_LOG.md` 已登记新入口。

最终检查结果：主讲/备份数量为 `14/2`，Draw.io 数量为 `5`；16 张 PNG 均可读取，概念图为
1600×900，数据图保持原始高分辨率 3273–3994 px 宽；全部主 SVG、资产 SVG 与 Draw.io 通过
`xmllint --noout`；所有 `href="assets/..."` 相对引用均可在图集内解析；文档通过
`git diff --check`。图集共 87 个文件、约 5.9 MiB，其中多出的文件为可编辑概念图的独立 icon
与构建资产。未解决项为无。
