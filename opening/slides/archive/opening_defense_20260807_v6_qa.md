# 开题答辩 PPT v6 QA（2026-08-07）

## 交付对象

- PPTX：`opening/slides/opening_defense_20260807_v6.pptx`
- 构建器：`opening/slides/build_opening_defense_v6_artifact_tool.mjs`
- 设计合同：`opening/slides/opening_defense_v6_design.md`
- 模板：`opening/slides/opening_defense_20260720_v5.pptx`
- 精确数据：`experiments/results/opening_database_e2e_text_20260807/raw/headline_summary.json`

## 模板与内容审计

- 页数：28；沿用 v5 学校模板、master、页眉、标题和结论栏。
- 映射方式：先用 `prepare_template_starter_deck.mjs` 复制已声明的 v5 source slide，再定点改写文本和继承图片帧；没有运行旧 `build_ppt.py`。
- 证据页：1 页统一文本三臂边界；4 页核心证据图；后续方法页只解释两项研究内容、共同代价组件和跨模态边界。
- 四级 claim：已证明、条件性、待验证、不能声称在第 12 页和 speaker notes 中明确区分。
- 数据口径：第 7 页从 `headline_summary.json` 读取，不手写 ShareGPT 结果；SQuAD/ShareGPT project feeding 均标为未过门。

## 自动 QA

| 检查 | 结果 |
|---|---|
| PPTX export | 通过 |
| slide count | 28/28 |
| speaker notes | 28/28 含“汇报讲稿”“答辩备注”“[Sources]” |
| empty placeholders | 0 |
| headline source | 指向冻结 `headline_summary.json` |
| `slides_test.py` 画布溢出 | 通过；`No overflow detected` |
| PPTX XML 可解析 | 通过构建器 export/import 与 placeholder 审计 |

## 视觉 QA 与修复记录

逐页 PNG contact sheet 与关键页原分辨率检查覆盖 1–28 页。首轮渲染暴露两类问题：长正文文本仍保留 v5 内容；imported image 的 `replace` 没有落到最终 export。构建器随后改为直接设置完整文本，并在继承图片帧中加入白色 cover 与新图片对象。二次检查又发现 contain 留白使旧底图边缘透出，加入 cover 后消失。

重点检查页：

- 第 7 页：统一三臂数值完整，无截断；4,936/6,144 cap 语义失败和 feeding 门同页呈现。
- 第 8–11 页：四张核心图可读，结论栏与图内门槛一致。
- 第 14、16、17、19、20、25 页：架构/机制图完整落入模板安全区；无旧底图边缘。
- 第 26–28 页：计划、风险与结束页无 overflow。

## Office 应用级检查（2026-08-08）

- WPS Office：应用成功启动，但自动打开文件后没有出现文档窗口，因此不计为通过。
- Microsoft PowerPoint：同一 PPTX 已真实打开并识别为 28 页；首页与左侧缩略图正常渲染，
  未出现文件修复提示、字体替换告警或页面损坏提示。
- PowerPoint 当前未激活 Microsoft 365，只限制编辑和保存；不影响本次只读兼容性检查。
- 程序化逐页渲染、关键页原分辨率检查、`slides_test.py` 与 PowerPoint 真实打开证据共同
  覆盖版面和文件兼容性门禁。

结论：v6 PPTX 的本地内容与兼容性检查通过。飞书正文和四图已同步并回读；整个开题材料
只待平级知识库目录恢复并完成镜像后，才标记为最终发布冻结。
