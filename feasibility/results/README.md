# Feasibility Results

本目录保存可行性 benchmark、环境验证和连接验证结果。

规则：

- 系统组件 microbenchmark 放在这里。
- PG18.4 连接/环境验证放在这里。
- GPU/CUDA/模型服务 smoke test 可以放在这里，但只能证明环境或服务可用。
- 数据库 AI 算子端到端动机测试、系统画像、瓶颈定位和可优化点分析放在 `motivation/results/`。
- GPU-backed E2E profile 不放这里，放 `motivation/results/`。

## 阅读顺序

本目录按“是否能让后续实验继续跑”来读：

1. `pg18_4_connection_validation.md`：确认本地 PostgreSQL 18.4 + pgvector 链路可用。
2. `pgai_sql_smoke_20260714.md`：确认 pgai SQL embedding 触发面和 pgvector 写回可用。
3. `pg18_4_connection_smoke_*.csv`、`pg18_4_script_dryrun.csv`：确认脚本和小规模 smoke run 可用。
4. `ray_*`、`arrow_serialization.csv`、`shuffle_simulation.csv`：组件级 benchmark，只用于判断是否存在可观测系统信号。
5. 组件级 benchmark（`ray_*`、`arrow_serialization.csv`、`shuffle_simulation.csv`）：只用于判断是否存在可观测系统信号，作为历史组件参考。
6. `vllm_clip_pooling_gate_20260804/`：当前 AutoDL 软件组合上的 CLIP pooling
   capability blocker；两次 600 秒门禁均未返回 embedding，不含性能结果。
7. `cost_profile_cacheon_gate_20260805/`：已提交 main 上的双 4090 cache-on + shared-Ray
   两运行门禁；验证 cache 声明/命中观测、exactly-once 和双 endpoint，不作性能排名。
8. `duckdb_ai_semantic_gate_20260805/`：DuckDB community `ai` 的双 endpoint
   capability 与 ShareGPT fixed-cap 语义门禁；证明当前环境可执行，但该 workload
   会把截断作为行级错误，因此不进入正式吞吐排名。
9. `request_equivalence_gate_20260805/`：三方请求等价门禁（canonical = DuckDB
   `ai_completion_request_json` = 项目 `build_completion_request_body`）+ 单请求 vLLM
   prompt-token delta 交叉核验；PASSED（37=37 prompt tokens），证明三臂送进模型的请求一致。
10. `squad_v11_dev_import_20260805/`：SQuAD v1.1 dev（10570 行）importer provenance
    （canonical SHA256 fail-closed、content_hash、多答案 JSONB、prompt 模板 hash）；
    bounded-output 主对比轨的数据源合同。
11. `squad_capability_256_v4_20260805/`：DuckDB-ai 256 行 capability gate（canonical：
    SQuAD-normalize 分桶 + `sample_manifest.jsonl` + /version 修复后重跑）。EM 81.64% / F1 89.82%，
    attribution=attributable，integrity=verified；当前 canonical 256 单臂样本。v2/v3 保留作历史。
12. `squad_capability_full_10570_20260805/`：DuckDB-ai **全量 10570** gate（`--mode full` +
    `--strict-attribution` + fail-closed）。**fail-closed FAILURE 维持**：10569/10570 成功、1 NULL
    （full-set query、扩展并发 32 下的单次机制未定生成尾部事件，被 truncation-as-error 转 NULL）；
    exactly-once/三 hash 一致/归因==10570 全通过；EM 80.32%/F1 89.36%。状态：`capability_gate_status=failure`、
    `comparison_admission=eligible_with_documented_failure`、`formal_run_gate_passed=false`。
13. `squad_truncation_diag_572700c8_20260805/`：full gate 失败行的定点诊断（direct + DuckDB ×
    cap{64,128,256} × 3）。cap=64 孤立重放 3×3 全 `stop`/46 token/文本一致 → 截断不可复现，推翻
    「确定性 rambling」；记为偶发、机制未定。诊断专用，不回灌 cap=64。

如果后续新增 GPU 环境验证，建议命名为：

```text
gpu_model_service_smoke.md
gpu_model_service_smoke.csv
```

这些文件只能说明 GPU 模型服务能跑通；端到端瓶颈和优化结论仍应进入 `motivation/results/`。

## 文件

| 文件 | 内容 |
|---|---|
| `ray_small_task.csv` | Ray small task 实验结果 |
| `ray_object_transfer.csv` | Ray object transfer 实验结果 |
| `arrow_serialization.csv` | Arrow RecordBatch serialization 结果 |
| `shuffle_simulation.csv` | 本地 shuffle simulation 结果 |
| `ray_many_objects.csv` | Ray many-object fan-in 结果 |
| `ray_arrow_fanout_fanin.csv` | Arrow RecordBatch fan-out/fan-in 结果 |
| `pg18_4_connection_validation.md` | PG18.4 本地连接与环境验证报告 |
| `pgai_sql_smoke_20260714.md` | pgai SQL embedding 触发面与 pgvector 写回冒烟验证 |
| `pg18_4_connection_smoke_256_rows.csv` | 首次 256 行 PG18.4 链路冒烟 CSV |
| `pg18_4_connection_smoke_runs.csv` | PG18.4 连接冒烟补充运行 CSV |
| `pg18_4_script_dryrun.csv` | 画像脚本 dry-run 展开验证 CSV |
| `image_staged_resource_gate_20260802/` | 2×4090 上 Daft/Ray Data staged 256-row 显式 source+stage+model CPU 资源门禁；报告、45 列摘要和原始 CSV/manifest 均已归档；只证明可运行和输出等价，不作性能排名 |
| `vllm_clip_pooling_gate_20260804/` | vLLM 0.25.1 CLIP pooling 两次离线 capability gate；保存退出码、超时、环境、完整日志与七步报告；结论是当前环境 blocked，不是服务性能排名 |
| `cost_profile_cacheon_gate_20260805/` | 双 4090 上 cost-profile cache-on 主合同的 1 warmup + 1 formal 提交后门禁；2/2、0 incident、共享 Ray、缓存命中与 exactly-once 通过，不作性能排名 |
| `duckdb_ai_semantic_gate_20260805/` | DuckDB v1.5.4 + community `ai` v0.4.14 的 4/64-row capability/语义门禁；保存最小 raw 证据，结论是另建 bounded-output 产品轨，不把失败的 ShareGPT fixed-cap 数据用于排名 |

PG18.4 系统画像与瓶颈定位实验已经移动到：

```text
motivation/results/pg18_4_fake/system_profile.md
motivation/results/pg18_4_fake/system_profile.csv
```

## 报告生成

```bash
python feasibility/benchmarks/analyze_results.py \
  --results-dir feasibility/results
```

## Trigger Surface Validation Files

```text
trigger_surface_validation_20260714.md
pg18_4_post_migration_health_20260714.csv
pgai_sql_profile_20260714.csv
trigger_surface_comparison_20260714.csv
trigger_surface_pgai_sql_20260714.csv
```

These files validate that the existing PG18.4 job-table chain and the isolated
pgai SQL trigger surface can both run small embedding workloads. They are
feasibility results, not GPU-backed or PostgreSQL 18.3 performance conclusions.

## 2026-07-14 pgai SQL scale file

```text
pgai_sql_scale_20260714.csv
```

This is a feasibility-side SQL trigger-surface timing file. It is not a
GPU-backed result and is not used as PostgreSQL 18.4 or PostgreSQL 18.3
performance evidence.
