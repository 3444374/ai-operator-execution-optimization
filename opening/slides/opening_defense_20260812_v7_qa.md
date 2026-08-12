# 开题答辩 PPT v7 QA

日期：2026-08-12

成品：`opening_defense_20260812_v7.pptx`

构建脚本：`build_opening_defense_v7_artifact_tool.mjs`

内容依据：`../opening_defense_outline_20260808.md`、`../claim_matrix.md`

## 1. 内容与模板

- 使用 `opening_defense_20260807_v6.pptx` 的学校模板、页眉、页脚、标题区和安全内容区，重组为冻结的 20 页主讲结构；没有切换或修改主工作区 `main`。
- 生成工具为 `@oai/artifact-tool`，没有使用旧 `build_ppt.py` 或 python-pptx 全量覆盖模板。
- 每页只保留一个 take-away；第 4、10 页改为独立文字页，避免结构图与数据图混放或图文重叠。
- 20/20 页 speaker notes 均含 `汇报讲稿`、`答辩提示` 和 `[Sources]`。

## 2. 图文一致性

- 完整逐页映射见 `opening_defense_20260812_v7_figure_content_audit.md`。
- 第 5–19 页只使用 `figures/opening_figure_set/main_png` 的 14 张主讲图，与页面结论一一对应；数据图的比较范围均与 Claim Matrix 一致。没有直接引用旧 `report_main` 或旧 architecture PNG。
- 第 2、3 页当前是逻辑完整但视觉偏空的中文文字页。建议补 AI-Native 演进/控制点图与传统/AI 执行链路对照图；第 4 页相关工作分层图属于可选增强。中文绘图提示词见图文审计文件。
- 第 6 页保持两条比较轨：左侧 database-E2E 产品轨含 Project，右侧官方 Chat graph 轨不混入不同计时边界的 Project 数值。
- 第 17 页严格分开 12K 结构诊断与 120K matched-resource 正式比较；第 18 页包含 Daft Built-in、Ray Data、Project static/shared 的图像四 Job 归一化干扰。

## 3. 程序化门禁

| 检查 | 结果 |
|---|---|
| 幻灯片数 | 20 |
| 空 placeholder | 0 |
| notes 缺失 | 0 |
| template fidelity | pass，0 issues |
| 每页 PNG 渲染 | 20/20 成功 |
| 全页 contact sheet | 已人工复核，无重叠、裁切或页眉页脚侵占 |

模板一致性命令使用 `check_template_fidelity.mjs`，输入为 starter frame map、starter/final layout 和最终 PPTX；结果为 `status=pass`、`issueCount=0`。

## 4. 数据与主张边界

- 文本/图像四 Job 图均为路径内部 `concurrent JCT / isolated JCT`，不用于跨框架绝对排名。
- 数据组织图只说明压力状态下 locality/cache-hit/吞吐的关联和策略排名变化，不声称某一策略全局最优。
- Project shared 图呈现效率—隔离—公平权衡，不声称动态方案全面优于静态方案。
- 代价估计图展示 20 个 context 的决策损失分布、pairwise accuracy、平均和最坏 regret；只支持文本配置选择的初步可行性。
- 图像 Project shared 的 snapshot 为 observe-only，不写成动态收益。

## 5. 文件完整性

- PPTX SHA-256：`d00667debead8a62b64ea6cfabaaa2ec3ebb109ab1464f3567064500fc8485cd`
- 构建脚本 SHA-256：`e33324b0ae181cfdcb518678fecff0f4eb739624bd68d6283b193fc1192184a8`
- 构建脚本会在独立临时目录自行执行模板检查、starter 生成、20 页渲染、layout、notes 和 contact sheet 门禁，不依赖上一次会话的临时缓存。

## 6. 仍需人工填写

- 封面报告人、指导教师信息目前保留空线，避免自行猜测个人信息。
- 本轮已完成程序化渲染和逐页图片检查；正式答辩前仍建议在用户使用的 PowerPoint/WPS 环境打开一次，确认本机字体替换与投影比例。
