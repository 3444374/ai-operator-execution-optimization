# Parrot 精读笔记论文原图选择与裁剪审计

日期：2026-08-25

## 范围与证据来源

- 使用位置：`research/精读文献笔记/parrot_osdi2024/parrot_osdi2024.md`。
- 权威来源：`research/reference/parrot_osdi2024.pdf`，题名 *Parrot: Efficient Serving of LLM-based Applications with Semantic Variable*，OSDI 2024，论文印刷页 929–945。
- 本地文件共 18 页，其中 PDF p.1 为 USENIX 封面、PDF p.2–18 为 17 页论文正文；SHA256 `CE334DF62516EA037233B77650CD5303C6AE254E0CF7C267F86A92F69F2386FF`。该文件是正式 proceedings PDF，对应预印本为 `arXiv:2405.19888v1`。
- 使用 Poppler 以 324 DPI 渲染原页，再按 Figure 边界裁切并保留原论文 caption；没有重绘、改色、锐化、拼接或修改图内数据。
- PNG 只服务 Markdown 精读讲解，不是项目自制图，也不是本项目实验结果。

## 选择结果

| 论文图 | PDF 页码（印刷页） | 精读正文位置 | 选择理由 |
|---|---:|---|---|
| Figure 1 | 2（929） | §2.2 | 四种应用拓扑是理解“请求并非独立”的总入口。 |
| Figure 2 | 3（930） | §2.2 | 用 `api → code → test/review` 显示 Semantic Variable 的生产者—消费者依赖。 |
| Figure 3 | 4（931） | §3.1 | 同时给出外部延迟占比观测与传统/Parrot 连续请求路径对照。 |
| Figure 4 | 4（931） | §3.2 | 用同一 Map-Reduce DAG 解释单请求目标与应用完成时间目标的错位。 |
| Figure 5 | 5（932） | §3.3 | 静态、准静态和动态 prompt 结构是 prefix sharing 的直接动机。 |
| Figure 6 | 5（932） | §5.1 | 唯一完整连接 API、Manager 分析、调度与 LLM Engine 的系统架构图。 |
| Figure 7 | 6（933） | §5.2.1 | 将 SemanticFunction、输入/输出占位符、future 与性能目标落实到代码接口。 |
| Figure 8 | 6（933） | §5.3 | 把 request/variable DAG 与四个跨请求分析 primitive 对齐。 |
| Figure 9 | 7（934） | §6.2.2 | 展示从最终输出反向推导请求类别与 task group 的规则。 |
| Figure 10 | 10（937） | §10.1 | 解释 latency-oriented vLLM baseline 的 token-capacity 校准依据。 |
| Figure 11 | 11（938） | §10.2.1 | Chain Summary 在输出长度和 chunk size 两个变量下的主结果。 |
| Figure 12 | 11（938） | §10.2.1 | Chain Summary 在背景请求与多应用并发下的竞争结果。 |
| Figure 13 | 11（938） | §10.2.1 | 逐应用完成时间差可检查平均收益是否掩盖受损应用。 |
| Figure 14 | 12（939） | §10.2.2 | Map-Reduce 对输出长度和 chunk size 的完整敏感性结果。 |
| Figure 15 | 12（939） | §10.3.1 | 同时呈现 batch size、prefix sharing、TPOT 与 OOM 的关系。 |
| Figure 16 | 13（940） | §10.3.1 | 在两个 batch size 下隔离输出长度增加时的 TPOT 趋势。 |
| Figure 17 | 13（940） | §10.3.2 | 用完整系统和两项消融展示多 GPTs 的容量拐点、placement 与 kernel 作用。 |
| Figure 18 | 13（940） | §10.4 | 同时给出多智能体 workflow 的端到端延迟和 KV cache 内存。 |
| Figure 19 | 14（941） | §10.5 | 三个不同单位的 panel 是理解混合 workload 目标分离的关键结果。 |

论文正文共有 Figure 1–19。十九张图分别承担应用结构、问题动机、API/分析/架构机制、baseline 校准或六类独立结果问题，没有仅重复已转录表格与算法的 Figure，因此全部保留。Table 1–3、Algorithm 1 与公式已在笔记中按字段或步骤转写，不再截图。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `fig1_application_workflows.png` | 1147×689 | `0A45B09D39854920A5ED53196D86CA135010EB0552C05A201215ADA13A6A3F73` |
| `fig2_semantic_variable_dependencies.png` | 1147×751 | `395C5B282FC05673D36392471DE9EE961AADF0FF58A13309AF085F73DCD08F62` |
| `fig3_dependent_request_overhead.png` | 2340×698 | `A09C782694D6E720E986E39FC6D43BDE9C7186DC54FEAC30CFA9905DE2AFBCA2` |
| `fig4_application_centric_scheduling.png` | 1147×674 | `966DE4990105639C9C8E6CD56DEB2BD60CB5A51E19B8C15A8C8724AE4D818BB1` |
| `fig5_prompt_structure.png` | 1170×504 | `D5B7FC7AE095D8B4A911928FFD57153C8380FB9CD729C8EF59C2343ECEE3EB65` |
| `fig6_system_overview.png` | 1147×661 | `DFF6E6843BA536B2C466FC7267DE771B65948B672D077470ED42B81EEF8E06A9` |
| `fig7_semantic_function_example.png` | 1103×1201 | `25781F634AA42CAFD099804BA23C57E6DC640AE33AED2FF779C1C2E4EA3F037C` |
| `fig8_analysis_primitives.png` | 1147×621 | `62A1253D2B54F541691C6BE639B129D7DAA0D407789690E98367C2DDCF974CE8` |
| `fig9_performance_objective_deduction.png` | 1147×463 | `BAD16C4FDDEB69D88E41990B7D4A5920923C38494892F73AB9B65DF1785982D0` |
| `fig10_vllm_capacity_calibration.png` | 1147×810 | `C2D7ACFBA0A9F088BB168205F3611D5C613E1070E61258A26E3798F769D6B8CB` |
| `fig11_chain_summary_sensitivity.png` | 1103×720 | `B1E572DF7D3DF0F602C4ECF6E57B07C89CB812C1765D2A97625194728A9577E1` |
| `fig12_chain_summary_contention.png` | 1147×720 | `4D9F9A34F0125AC601DA6EA34040A9B25A6108503ED86715CE52F509544B4A51` |
| `fig13_per_application_latency_savings.png` | 1147×675 | `B266E52995707AEF2B47A53D868A0FB44F72669F455E9DFCECA731CE2C2AE0F0` |
| `fig14_map_reduce_summary.png` | 1103×720 | `226CD3FE21C8A31C723EDBB811BE56D7055A4FF59C6BDABA413C30EE8729B0C0` |
| `fig15_bing_copilot_batch_size.png` | 1147×674 | `9CEBF86BD94C414B35C8BE8279668FC2BCF3DD2BFC705016D86D39477C41A3A6` |
| `fig16_bing_copilot_output_length.png` | 1112×674 | `C0508267110EFF88AEF7A7626211601647D25461EE86D870C9AEC848D3EF3EAB` |
| `fig17_multiple_gpts_request_rate.png` | 1112×720 | `2BD2B184E2168EC9555859FF2EDF5310E34F539841AF9B66507078B340E28C35` |
| `fig18_multi_agent_latency_memory.png` | 1228×1260 | `2532FE7CE6F43AB7890A762051F7E4F8034071E73F1663C472343F060F0DC13C` |
| `fig19_mixed_workloads.png` | 1147×720 | `D46D1AA9C493CD7444D77AF9A663FFEA3B8AFAF84B9E6E2FB8D18D23A318316B` |

所有输出均位于 `research/精读文献笔记/parrot_osdi2024/figures/`。Markdown 共 19 个本地图片引用，逐一解析到同名 PNG。

## 视觉与论证 QA

- 19 张 PNG 已按原始分辨率逐张预览；坐标轴、图例、节点、倍率、OOM 标记和原论文 caption 均完整，未混入相邻正文、下一节标题或页脚。
- Figure 1–9 作为应用结构、动机或系统机制图解读，不冒充实验收益；Figure 10 明确只是 baseline capacity 校准。
- Figure 11–19 覆盖 Chain、Map-Reduce、Bing、GPTs、Multi-agent 与 Mixed workloads 六类结果；图注均说明横纵轴或 panel 读法，并把单 A100、4×A6000、固定模型/数据/到达分布、注入网络延迟、无质量评价或无置信区间等限制写在相邻位置。
- Figure 18 的约 48 GB 虚线按论文图中的可用 KV cache 阈值描述，不误写为 A100 物理总显存；Figure 19 的三个 panel 使用不同单位，不跨 panel 比较柱高。
- 当前 PNG 适合 Markdown 精读。如果进入正式报告、PPT 或论文正文，应从 proceedings PDF 重新导出矢量或针对版面重裁，并另行检查版权与引用格式。

审计结论：Parrot 正文 Figure 1–19 均有独立讲解价值，已在对应关键段落插入；0 个 critical、0 个 major、0 个视觉阻断项。
