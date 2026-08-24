# Palimpzest 精读笔记论文原图选择与裁剪审计

日期：2026-08-24

## 范围与版本边界

- 使用位置：`research/精读文献笔记/palimpzest_cidr2025/palimpzest_cidr2025.md`。
- 本地来源：`research/reference/palimpzest_cidr2025.pdf`，题名 *A Declarative System for Optimizing AI Workloads*，arXiv:2405.14696v2，2024-05-29。
- 该工作后来以 *Palimpzest: Optimizing AI-Powered Analytics with Declarative Query Processing* 发表于 CIDR 2025；本笔记的方法、图号、页码和实验结论严格对应附件的 29 页 arXiv 版本，不把正式会议版题名当作附件题名。
- 本地 PDF SHA256 为 `F853718E273A6330AA4FDE3CE79FBE23BF457D90C18D4D1F009DE2ADACA5DEAF`；已核对首页题名、版本、页数、正文图号和英文 caption。
- 这些 PNG 是论文原图的局部裁剪，只服务 Markdown 精读讲解；不是项目自制图，也不是本项目实验结果。

## 选择结果

| 论文图 | PDF 页码 | 精读正文位置 | 选择理由 |
|---|---:|---|---|
| Figure 1 | 3 | §4.4 | 总览声明式程序、逻辑/物理候选、sentinel sampling、代价估计、Policy 选择和执行之间的完整数据流。 |
| Figure 2 | 5 | §3.2 | 用三个正例把 Legal Discovery、Real Estate Search 和 Medical Schema Matching 的输入、语义判断与输出形式放在同一视图中。 |
| Figure 3 | 6 | §4.1 | 展示 Schema、自然语言 Filter、Policy 与 lazy `Execute()` 如何组成 Legal Discovery 的声明式程序。 |
| Figure 4 | 9 | §4.1.1 | 左侧给出 PDF、图片、邮件的 Convert 代码，右侧给出完整关系算子表，适合建立语言抽象与实现边界。 |
| Figure 5 | 14 | §6.2.2 | 展示 Real Estate Search 的文本/图片 Schema、UDF Filter、语义 Filter 与 `depends_on`，是理解跨模态重排机会的关键代码图。 |
| Figure 6 | 15 | §6.3.2 | 直接展示三个工作负载中实测 runtime/cost–F1 候选点、单模型 baseline 与 Pareto frontier，承担“候选空间有价值”的主要实验证据。 |
| Figure 7 | 17 | §6.4.1 | 将三个 Policy 所选计划与全 GPT-4 baseline 的 runtime、cost、F1 并列，便于和 Table 1 的 7/9 约束满足情况一起解读。 |

未加入附录 Figure 8–9：Figure 8 的负例与 Figure 2 和正文中的 workload 定义重复，Figure 9 是 Medical Schema Matching 的长程序清单，其 `oneToMany` 和 Schema 转换步骤已经在 §3.2.3、§6.2.3 转写。Algorithm 1 和各 Table 同样由正文逐步解释或转录，不再用截图重复文字信息。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `research/精读文献笔记/palimpzest_cidr2025/figures/fig1_system_overview.png` | 2142×1188 | `6A6516E1F9C273EC7BE5EE9CE6B89BE9A0B7DC61DE5BCF1887478DFEC77AF1FF` |
| `research/精读文献笔记/palimpzest_cidr2025/figures/fig2_workload_examples.png` | 2142×2367 | `F867962B5FC7171ED142A9F4D750D1F751F27011A75ECD368FFB59A629520ADD` |
| `research/精读文献笔记/palimpzest_cidr2025/figures/fig3_legal_discovery_program.png` | 2106×720 | `51ED27C3A0C05832F9F960339BBB5EA79EE11E430D7E49B70647E254A64E47A9` |
| `research/精读文献笔记/palimpzest_cidr2025/figures/fig4_code_and_relational_algebra.png` | 2124×1359 | `495D448B9823BAD3F64018FF12B4E03BB051EFCEC3080627D33541E49060A72D` |
| `research/精读文献笔记/palimpzest_cidr2025/figures/fig5_real_estate_program.png` | 2358×1359 | `0D49CB915C7791912850D5951E2996E9DD592FB72FE0720512F9E5BA92FF954E` |
| `research/精读文献笔记/palimpzest_cidr2025/figures/fig6_plan_tradeoff_frontiers.png` | 2142×1157 | `EF4A10095FD26088A8583DB369DEF35D56E1B0C7B5C66DB8ACC5DCF026742732` |
| `research/精读文献笔记/palimpzest_cidr2025/figures/fig7_policy_selected_plans.png` | 2142×1359 | `3DAEC227C0AB7573D7C4E0F6A7FE6D750286B8703D6388134468B1F2B2C96F32` |

提取方式：使用 PyMuPDF 对相应 PDF 页面作 4.5× raster render，再按 Figure 与英文 caption 的完整边界裁剪。没有重绘、锐化、替换颜色、修改坐标、删除图内元素或拼接不同页面。

## 视觉与论证 QA

- 7 张裁剪件已按原始分辨率逐张预览；代码、Schema、算子名称、流程编号、图例、坐标轴、单位、数值与英文 caption 完整可读，没有混入相邻页眉或无关正文。
- Figure 1–5 分别按架构示意、接口示例或候选算子空间解读，不把它们当作性能证明；Figure 4 的完整算子表也不外推为所有 AI 算子均完成同等实验验证。
- Figure 6 的点是实际执行值而非优化器估计值，且各工作负载坐标尺度不同；正文只在各自面板内比较。Figure 7 必须与 Table 1 联读，明确优化器只满足 9 个约束中的 7 个。
- Figure 5 说明 `depends_on` 提供合法重排和跳过图像调用的机会，不单独证明视觉路径一定是瓶颈，也不单独证明重排收益。
- PNG 仅用于精读笔记显示。若将来进入正式论文、开题报告或 PPT，应从相应发表版本取得矢量图，重新核对图号、题名和实验是否与本 arXiv 版本一致。

审计结论：7 张论文原图裁剪件适合进入当前 Palimpzest 精读笔记；0 个 critical、0 个 major、0 个视觉阻断项。
