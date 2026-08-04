# Archived all-phase cost-estimation evidence

本目录保存 2026-08-04 审计前的历史结果。旧 loader 只筛选 `status=ok`，没有筛
`phase=formal`，因此 warmup 被混入模型拟合、候选 repeat 均值和 selection metrics。
这些文件只用于追溯问题和复核修复影响，不再作为项目当前实验结论，也不得与
formal-only 结果混表。

当前权威入口为上级目录的：

- `ce_context_loo_20260804.md`；
- `ce_context_loo_formal_only_23feature_20260804.json.gz`。

旧文件未删除是为了保留审计链；任何引用都必须显式标注 `all-phases / invalid for
formal performance claims`。

`README_full_allphases_audit.md` 是审计前的完整长报告，其中同时保留了旧 283-row
headline 和方法演进。它不是当前入口；上级目录的新 `README.md` 才是权威摘要。
