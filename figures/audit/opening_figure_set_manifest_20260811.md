# 开题专用图集清单与审计（2026-08-11）

## 1. 目的

解决 `figures/data/report_main/` 图过多、文件名无法直接映射到开题页面的问题。依据
`opening/opening_defense_outline_20260808.md` 与
`figures/audit/opening_required_data_figures_20260810.md`，建立
`figures/opening_figure_set/` 作为开题材料的单一选图入口。

2026-08-12 增补 P02–P04 三张背景/相关工作概念图。增补同样不移动或删除权威源，不改实验
数据或统计值；PPT 主讲入口因此扩展为 17 张，报告正文仍引用其中 P05–P19 的 14 张研究/证据图。

2026-08-16 按背景叙事顺序修订图资产但不重生成 PPT：P02 暂不修改；P03 删除本课题的
Work Unit、状态反馈、credit/准入/路由等设计，改为大众化外部 AI 算子六阶段流程；P04 只作
相关工作分层；P05 只陈述研究空白。P06–P08 仅中性化标题/标签，实验数值与图形几何不变。

2026-08-17 仅做版面拆分：P07 的 panel a 与 panels b–c 分为 P07A/P07B，P08 的 panel a 与
panels b–c 分为 P08A/P08B。原合成图仍保留在 `data/report_main/`；拆分图的数据、坐标、注释和
结论不变，仅把底注改成实验配置；独立页不再显示原组合图的 a/b/c 面板编号。本轮未重新生成 PPT。

## 2. 选图结果

- PPT 主讲概念图：8 张；除原有 5 张外，新增数据库 AI 外部执行链路、传统/外部 AI 执行假设
  对照和相关工作分层三张背景图。
- 主讲数据图：11 张；其中 work/state/capacity 与图像 stage 两张合成图各拆成两个 16:9 面板，其余分别承担文本 baseline、文本
  原生多 Job 干扰、数据组织 regime、共享调度权衡、代价决策质量、图像 baseline、图像多 Job
  干扰。
- 答辩备份数据图：2 张，只用于单 Job 请求延迟和状态指纹追问。
- 主讲图按照答辩页码命名为 `P02`--`P19`（第 10 页无图）；备份图命名为 `B01`--`B02`。

详细的页码、任务与源文件映射见 `figures/opening_figure_set/README.md`。

## 3. 文件与格式合同

| 分组 | PNG | SVG | Draw.io | 说明 |
|---|---:|---:|---:|---|
| 主讲 | 19 | 19 | 8 | 概念图和数据图均有 PNG/SVG；仅概念图需要 Draw.io |
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

- [x] 19 张主讲 PNG 与 19 张主讲 SVG 一一对应；
- [x] 2 张备份 PNG 与 2 张备份 SVG 一一对应；
- [x] 8 张概念图均提供可编辑 Draw.io；
- [x] 文件名可从页码和中文用途直接识别；
- [x] 概念图 SVG 的相对 icon 资产已复制；
- [x] 原权威文件未移动、未删除；
- [x] 不可比或无正式证据的图未进入图集；
- [x] `figures/README.md`、`PROJECT_INDEX.md`、`PROJECT_OUTLINE.md`、根 `README.md` 与
  `PROJECT_LOG.md` 已登记新入口。

最终检查结果：主讲/备份数量为 `19/2`，Draw.io 数量为 `8`；21 张 PNG 均可读取，概念图为
1600×900，数据图保持原始高分辨率 3273–3994 px 宽；全部主 SVG、资产 SVG 与 Draw.io 通过
`xmllint --noout`；所有 `href="assets/..."` 相对引用均可在图集内解析；文档通过
`git diff --check`。拆分版均保留 PNG/SVG 成对输出；未解决项为无。
