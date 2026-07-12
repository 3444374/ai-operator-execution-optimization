# 开题工作区

本目录用于同步准备开题报告、开题汇报 PPT 和飞书进度汇报。当前阶段先建立材料组织方式和写作规则，后续再把实验结果、文献和项目总结提炼进报告与 PPT。

## 当前汇报主线

建议主线：

```text
数据库 AI 算子正在出现
  -> 传统数据库执行链路无法覆盖模型服务、batch、调度、写回等新成本
  -> 本项目构建可控端到端链路并拆分阶段耗时
  -> 初步实验发现 batch、模型服务路由、writeback 都会影响端到端性能
  -> 后续研究模型服务感知的 batch、调度、backpressure 和写回协同优化
```

一句话口径：

> 本课题关注数据库 AI 算子触发后的外部执行链路优化，重点研究 PostgreSQL fetch、Arrow / batch、Ray task / actor、模型服务调用、fan-in 和 writeback 等阶段的瓶颈定位与协同优化。

## 目录结构

| 路径 | 作用 |
|---|---|
| `AGENTS.md` | 本目录长期规则 |
| `work_rules.md` | 开题工作的任务组织和目标管理规则 |
| `ppt_rules.md` | 开题 PPT 制作规则 |
| `outline.md` | 开题报告、PPT、飞书汇报的统一内容骨架 |
| `report/` | 开题报告正文与 Word 版本材料 |
| `slides/` | PPT 源稿、讲稿备注、PPTX 输出 |
| `feishu/` | 飞书进度汇报稿 |
| `literature/` | 文献清单、精读笔记、CCF-A 优先候选 |
| `assets/` | 图、SVG、表格、流程图、模板素材说明 |
| `logs/` | 非实验类 project log |

## 当前需要维护的材料

| 材料 | 主文件 | 状态 |
|---|---|---|
| 开题报告 | `report/opening_report.md` | 待起草 |
| 开题 PPT | `slides/opening_ppt.md` | 待起草 |
| 飞书进度汇报 | `feishu/progress_update.md` | 待起草 |
| 文献精读清单 | `literature/reading_list.md` | 待补 |
| 答辩问答 | `qa_bank.md` | 初版已建 |
| 材料同步日志 | `logs/project_log.md` | 初版已建 |

## 与项目其他目录的关系

- 实验事实优先来自 `motivation/results/`。
- 实验讲解和术语口径参考 `learning/experiment_walkthrough.md`。
- 文献与外部系统证据参考 `research/`。
- 当前方向和阶段计划参考 `overview/current_direction_and_plan.md`。
- 开题材料中的实验结论必须回到真实 CSV / 报告，不能只引用聊天结论。

## 下一步

1. 把旧开题 PPT、Word 模板和报告样例迁移或授权读取后，整理成可复用结构。
2. 从当前实验报告中提炼 3 到 5 个开题可用事实。
3. 建立 CCF-A 优先的核心文献候选清单。
4. 起草 `opening_report.md` 和 `opening_ppt.md`。
5. 将 PPT、报告、飞书版口径统一后，再生成正式 PPTX / DOCX。
## 飞书同步目标

后续需要写入飞书的主要目标：

| 飞书文档 | 链接 | 用途 |
|---|---|---|
| 开题报告与开题汇报 | https://my.feishu.cn/wiki/GCxowlVJbinzgRkoHDmc06cSn9J?from=from_copylink | 承载开题报告正文，并同步开题汇报 PPT 的核心内容和最终口径 |
| 动机测试与可行性测试 | https://my.feishu.cn/wiki/R2MywYu12i2PtWk84Vzcbp9Lnme?from=from_copylink | 承载动机实验、可行性实验、阶段画像和实验结论边界 |

同步规则见 `feishu/README.md`。

