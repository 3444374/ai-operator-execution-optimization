# 实验设计参考

本目录保存跨多个实验复用的规则、检查清单和设计依据。它们不等于待执行实验，也不单独产生
结果。

| 文件 | 作用 | 状态 |
|---|---|---|
| `bounded_output_duckdb_comparison_protocol_20260805.md` | DuckDB bounded-output 产品语义与比较口径 | 持续参考 |
| `experiment_report_honesty_checklist.md` | 报告身份、指标和证据完整性检查 | 持续参考 |
| `literature_driven_pipeline_optimization_guide.md` | 从文献到最小实验的设计流程 | 持续参考 |
| `sink_writeback_coordination.md` | PostgreSQL/pgvector 写回工程备忘 | 历史设计参考；写回当前只是统一工程 baseline |
| `strategy_design_implementation_reference.md` | 早期信号—变量—代码映射 | 历史工程参考；当前代码以 `../../../code/README.md` 为准 |
| `strategy_design_literature_basis.md` | 策略设计的文献边界与反证条件 | 持续参考，术语以根总纲为准 |

所有当前 baseline 身份和准入仍以顶层 `../baseline_reference.md` 为唯一总入口。
