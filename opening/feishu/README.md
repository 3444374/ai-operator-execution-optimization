# 飞书同步入口

本目录维护开题相关内容同步到飞书的本地源稿、同步计划和记录。原则是：本地 Markdown 先形成稳定口径，再同步到飞书文档；不要直接在飞书里临时改出和本地项目不一致的版本。

## 线上文档

| 飞书文档 | 链接 | 承载内容 | 本地来源 |
|---|---|---|---|
| 开题报告与开题汇报 | https://my.feishu.cn/wiki/GCxowlVJbinzgRkoHDmc06cSn9J?from=from_copylink | 开题报告正文、开题 PPT 内容摘要、最终汇报口径 | `opening/report/`、`opening/slides/`、`opening/outline.md` |
| 动机测试与可行性测试 | https://my.feishu.cn/wiki/R2MywYu12i2PtWk84Vzcbp9Lnme?from=from_copylink | 动机实验、可行性实验、阶段画像、实验结论边界 | `motivation/results/`、`feasibility/results/`、`learning/experiment_walkthrough.md` |

## 同步规则

- 飞书是发布面，本地 Markdown 是源稿。
- 同步前先检查本地报告、PPT、飞书版、实验报告口径是否一致。
- 写入飞书前要区分“事实、推断、待验证、不能声称”。
- 开题报告飞书文档中可以同步 PPT 大纲和关键页摘要，但正式 PPTX 仍由 `opening/slides/` 维护。
- 动机测试与可行性测试飞书文档只写真实实验和明确边界，不把聊天中的临时判断当作正式结论。

## 工具使用

- 普通飞书文档 / wiki 文档：使用 `lark-doc`。
- 如果飞书文档内嵌 Base / 多维表格，再切到 `lark-base`。
- 如果后续需要直接生成或编辑飞书幻灯片，再使用 `lark-slides`。
- 每次实际写入飞书后，在 `opening/logs/project_log.md` 记录同步时间、同步目标和本地来源文件。

## 实验内容记录标准

飞书上的实验相关内容记录标准与项目本地实验报告保持一致，但讲解方式要参考 `learning/experiment_walkthrough.md`：先让读者看懂实验在整条链路里的位置，再解释变量和结果，最后给出结论边界。

每个正式实验同步到飞书时，至少包含：

1. 实验问题：这次实验想验证什么。
2. 实验位置：实验处在数据库 AI 算子链路的哪一段。
3. 实验设置：脚本、命令、数据规模、模型、endpoint、executor、batch、repeat、warm-up。
4. 链路拆分：例如 `PostgreSQL fetch -> Arrow / batch -> Ray task / actor -> HTTP 模型服务 -> fan-in -> writeback`。
5. 变量解释：解释 `rows`、`executor`、`strategy`、`calls`、`e2e_s`、`operator_wall_s`、`model_request_wall_s`、`writeback_s` 等字段，不默认读者懂。
6. 原始结果入口：CSV、Markdown 报告、图表路径。
7. 结果解释：严格区分本地实验事实、合理推断、待验证问题、不能声称的结论。
8. 对开题的含义：说明它支撑哪个研究问题、技术路线或后续实验。

建议小节模板：

```text
### 实验名称

实验问题：

链路位置：

实验设置：

关键变量：

实验数据：

结果怎么读：

当前能说明：

当前不能说明：

下一步：
```

## 图表规则

- 能用图解释清楚的实验结果，优先放图。
- 图表只放图表头、坐标、图例、颜色含义和必要数值。
- 图外的解释放在正文，不把长句塞进图里。
- 图表来源必须能追溯到本地 CSV 或报告。
- 飞书文档中引用图时，同时保留本地图片路径或生成脚本路径。
