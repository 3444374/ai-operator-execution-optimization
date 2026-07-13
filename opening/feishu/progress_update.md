# 飞书进度汇报

## 本周完成

- 建立并补全开题材料工作区：报告、PPT 源稿、飞书进度汇报、文献清单、答辩问答和同步日志。
- 按学校开题报告模板重排 `opening/report/opening_report.md`，当前结构包括：课题背景、目的和意义；国内外研究现状；研究目标与研究内容；研究方案与可行性分析；进度安排；预期成果；主要参考文献。
- 补充 `opening/report/opening_report.md` 的进度安排细节、预期关键技术指标和主要参考文献初稿。
- 新增 `opening/feishu/opening_report_wiki.md`，作为“开题报告与开题汇报”飞书 wiki 的本地源稿。
- 起草 `opening/slides/opening_ppt.md`，形成 16 页 PPT 内容源稿，并为每页补充汇报讲稿和答辩备注。
- 从真实 GPU-backed `AI_EMBED` 实验中提炼开题可用事实：
  - 1024 行下逐行 endpoint 调用比 batch 调用慢约 `13.4x`。
  - 4096 行单 endpoint coalesced 场景中 Python / Ray task / Ray actor 差距不大。
  - 16384 行 Ray actor/coalesced 下 external operator 与 PostgreSQL writeback 都接近端到端时间的一半。
  - 两 endpoint 场景下 Ray task/actor 能降低 external operator wall time，但端到端收益仍受 writeback 限制。

## 当前判断

- 汇报主线应围绕数据库 AI 算子的模型服务感知批处理执行与写回协同优化，而不是只讲 Ray。
- 当前更适合作为开题题目的口径是：面向数据库 AI 算子的模型服务感知批处理执行与写回协同优化研究。
- Ray 是可控执行机制和后续调度 substrate，不是论文唯一研究对象。
- 开题阶段可把 `AI_EMBED` 作为第一组真实执行路径 baseline，但后续必须保留 `AI_FILTER/AI_CLASSIFY` 和 `AI_COMPLETE`，避免把题目收窄成 RAG ingestion 工程。

## 遇到的问题

- 当前真实 embedding 实验使用 PostgreSQL 18.4 本地预演和 JSON text 写回，不能代表 PostgreSQL 18.3 或 384 维 pgvector 写回性能。
- 双 endpoint 实验仍是本地同一 GPU 上的两个 endpoint，不能代表多 GPU 或 Ray Serve / vLLM 最终结论。
- 参考文献仍需要进一步核验作者、会议 / 期刊、年份和学校要求的格式。
- 学校 Word 模板和 PPTX 模板当前仍在外部目录；本阶段先维护 Markdown 和飞书源稿，最后再生成 DOCX。

## 下周计划

- 补全 `opening/feishu/opening_report_wiki.md`，再同步到飞书“开题报告与开题汇报”wiki。
- 补 384 维 pgvector 写回实验，比较 JSON text 与真实 pgvector 写回。
- 设计 driver fan-in 后统一写回、Ray worker 各自写回、vectorizer-like queue worker 写回三种对照。
- 将 PPT 源稿中的关键结果转成图表：fine vs coalesced、16K 阶段拆分、single/multi endpoint Ray 对比。
- 继续核验核心参考文献，补齐正式参考文献格式。

## 需要确认

- 学校开题报告对参考文献数量、外文比例、格式和 PPT 页数是否有硬性要求。
- 导师是否认可当前题目口径：“面向数据库 AI 算子的模型服务感知批处理执行与写回协同优化研究”。
- 企业侧 / 达梦平台中的 AI 算子更接近 SQL UDF、外部 worker、表函数、批处理服务，还是模型服务调用。
