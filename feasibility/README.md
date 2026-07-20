# Feasibility Directory

本目录是项目的可行性验证和环境验证入口，用来回答：

- 某个组件或链路是否有可观察系统信号；
- 本地环境、数据库连接、脚本 dry-run 是否可用；
- 哪些结果只能作为 feasibility evidence，不能作为端到端动机或性能结论。

当前可行性验证优先服务外部执行链路：Ray/Arrow/object/fan-in、外部 worker、writeback 和 backpressure。本目录不承载最终 GPU-backed 链路性能结论；它只负责证明组件、环境和脚本可用。GPU 可用性、驱动、模型服务 smoke test 可以放这里；真正的 GPU-backed 端到端系统画像应放到 `motivation/results/`。

当前实验主线、动机测试结论和开题论证优先看 `motivation/README.md`、`motivation/plans/workloads.md`、`motivation/plans/integration.md` 和 `motivation/results/README.md`。

进入本目录前先读 `AGENTS.md`。如果要运行组件 benchmark，优先读 `benchmarks/README.md`；如果要看结果，优先读 `results/README.md`。

## 目录分工

| 路径 | 作用 |
|---|---|
| `AGENTS.md` | 本目录稳定规则和边界 |
| `README.md` | 本目录动态入口和文件索引 |
| `benchmarks/` | Ray、Arrow、shuffle、fan-in 等组件级 benchmark 脚本 |
| `benchmarks/README.md` | benchmark 运行命令和脚本说明 |
| `results/` | 组件 benchmark、环境验证、连接验证和自动报告 |
| `results/README.md` | 结果文件命名、结果索引和报告生成方式 |

## 结果放置规则

- Ray / Arrow / shuffle / object fan-in 等组件级结果放在 `feasibility/results/`。
- PG18.4 连接验证、smoke test、dry-run 结果放在 `feasibility/results/`。
- GPU 环境可用性、CUDA/驱动/模型服务 smoke test 放在 `feasibility/results/`。
- 数据库 AI 算子端到端动机测试、系统画像、瓶颈定位和优化收益分析放在 `motivation/results/`。
- GPU-backed E2E profile、Ray vs non-Ray、GPU vs CPU 的端到端 baseline 放在 `motivation/results/`，因为它们会影响课题主线判断。

## 常用入口

benchmark 脚本说明：

```text
feasibility/benchmarks/README.md
```

结果索引：

```text
feasibility/results/README.md
```

PG18.4 连接验证：

```text
feasibility/results/pg18_4_connection_validation.md
```

## 运行示例

```bash
python feasibility/benchmarks/analyze_results.py \
  --results-dir feasibility/results
```

## 更新要求

完成本目录下的 benchmark、环境验证或结果整理后，检查：

- `README.md` 是否需要更新入口、命令或文件索引；
- `benchmarks/README.md` 是否需要更新脚本说明和运行命令；
- `results/README.md` 是否需要登记新结果；
- `learning/experiment_walkthrough.md` 是否需要补初学者讲解；
- 根 `README.md`、`PROJECT_INDEX.md` 是否需要同步更新当前进展或阅读路径。
