# Feasibility Results

本目录保存可行性 benchmark、环境验证和连接验证结果。

[待准备尾块追加](map_queued_preparation_20261008/README.md)：原逐行补充记录、可关闭原型、本地高/低成本对照及服务器147项检查；后续PG与8224次模型验证仍慢于即时默认，全部证据归同一报告。

[文本Map准备分段原型](map_input_preparation_20261004/README.md)：单Job输入借用/阶段持有、本地替身与服务器实际库检查；默认与模型请求保持原用途，失败和补测来源归同一报告。

[真实分批成本与供给形态](map_preparation_shape_20261004/README.md)：固定窗口回放、单／双Job内部对照及选取前就绪快照，模型及PG0；重复、观察干扰、失败和恢复来源归同一报告。

[gateway多查询干扰](gateway_isolation_20261004/README.md)：本地干扰隔离与服务器Linux/真实Ray/本机HTTP检查；源码、失败、紧凑共享证据和复算入口归该报告，不作为SQL或GPU结果。

[同机共享预付计数](mapped_request_budget_20261003/README.md)：本地组件及Linux/真实Ray SDK观察API核对；完整消费未获净收益，正式默认保持，PG/模型仍pending。

[Core／Map观察成本隔离](map_observation_isolation_20261003/README.md)：真实执行循环与厂商／服务替身的有限控制，区分观察引起的时序变化与真实执行优化。

[批量持久记账](request_budget_batch_20261003/README.md)：组件、无模型查询及真实模型对照；真实负结果保留，默认同步保持。
[就绪窗口聚合](ready_window_coalescing_20261003/README.md)：本地受控原型、同脚本复现及模拟取舍；当前不启用，实际库与PG/GPU收益待验证。

2026-10-02格式检查：[历史dry-run CSV](pg18_4_script_dryrun.csv)的表头为12列，末尾两条写回记录为14列。
原始输出保留；读取时按输出版本处理附加字段，不能按单一12列结构直接聚合。这份记录只说明脚本预演。

规则：

- 系统组件 microbenchmark 放在这里。
- PG18.4 连接/环境验证放在这里。
- GPU/CUDA/模型服务 smoke test 可以放在这里，但只能证明环境或服务可用。
- 数据库 AI 算子端到端动机测试、系统画像、瓶颈定位和可优化点分析放在 `experiments/results/motivation/`。
- GPU-backed E2E profile 不放这里，放 `experiments/results/motivation/`。

## 阅读顺序

本目录按“是否能让后续实验继续跑”来读：

1. `pg18_4_connection_validation.md`：确认本地 PostgreSQL 18.4 + pgvector 链路可用。
2. `pgai_sql_smoke_20260714.md`：确认 pgai SQL embedding 触发面和 pgvector 写回可用。
3. `pg18_4_connection_smoke_*.csv`、`pg18_4_script_dryrun.csv`：确认脚本和小规模 smoke run 可用。
4. `ray_*`、`arrow_serialization.csv`、`shuffle_simulation.csv`：组件级 benchmark，只用于判断是否存在可观测系统信号。
5. 上述历史dry-run文件的附加字段按本页格式说明读取。
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
14. `squad_database_e2e_duckdb_ai_20260805/`：**database-E2E runner 单臂实测**（DuckDB-ai，全 10570）。
    scan→construct→operator→统一 sink 计时墙：wall 93.9s，scan+construct+sink <1%（**adapter 占 wall 99.27%**，
    含 setup+DuckDB 执行+HTTP+排队+模型服务，不归因给模型）；`correct_rows/s` 90.42（主 headline）、sunk 10570；
    状态 `single_run_valid=false` / `formal_run_gate_passed=false`（单次恒 false） / `comparison_admission=pending_formal_repeat`
    （1 偶发截断 → fail-closed）。机器原始文件不变，README §8 含 codex 订正。单臂测量，非排名。
15. `squad_database_e2e_direct_client_20260805/`：**database-E2E runner 单臂实测**（direct_client，全 10570）。
    同一计时墙：wall 91.9s，adapter 占 99.26%；`correct_rows/s` 92.29、sunk 10570、EM 80.22%；finish_reason
    `{stop:10569, length:1}`——1 行截断返回 **partial text**（非 error）→ 0 error/0 NULL → `single_run_valid=true`。
    与 DuckDB-ai 臂核心差异：同一 source row（`572700c8…`）两次独立 full 触顶 cap=64，DuckDB-ai 转 NULL→failure，
    direct 返回 partial text→success（**截断的产品语义差异，非吞吐差异**）。机器原始文件不变，README §8 含 codex 订正
    （truncation_count/per-row latency 列需正式 rerun 落盘）。单臂测量，非排名；`project_static` 臂待补。

如果后续新增 GPU 环境验证，建议命名为：

```text
gpu_model_service_smoke.md
gpu_model_service_smoke.csv
```

这些文件只能说明 GPU 模型服务能跑通；端到端瓶颈和优化结论仍应进入 `experiments/results/motivation/`。

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
experiments/results/motivation/pg18_4_fake/system_profile.md
experiments/results/motivation/pg18_4_fake/system_profile.csv
```

## 报告生成

```bash
python code/scripts/benchmarks/analyze_results.py \
  --results-dir experiments/results/diagnostics
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

<a id="早期组件检查的读法"></a>

## 早期组件检查的读法

[小任务](ray_small_task.csv)、[对象传输](ray_object_transfer.csv)、
[Arrow序列化](arrow_serialization.csv)与[shuffle模拟](shuffle_simulation.csv)分别观察组件成本；
[对象数量](ray_many_objects.csv)及[批次fan-out/fan-in](ray_arrow_fanout_fanin.csv)在固定总量下解释
任务与对象数量的影响。读取结果时保持工作量、warm-up、重复和计时定义一致。
CPU/fake与数据库预演说明对应环境中的现象，系统瓶颈需从实际模型路径的独立画像核对。
