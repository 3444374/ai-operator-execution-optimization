# 历史设计与实施记录

本目录的文件按原日期保存设计选择和实施步骤。**归档不表示计划内容已实现，也不授予实验运行权限。**
正文中的旧命令、skill 名称和旧路径只描述当时的工作；实际操作从现行源码、目标计划和运行手册核对。

## 文件与现行核对入口

以下 38 份均为历史稿；每行的现行入口用于核对该主题今天的实现或结果。

### 设计记录

| 历史文件 | 现行核对 |
|---|---|
| [2026-07-25-accelerated-arrival-replay-design.md](designs/2026-07-25-accelerated-arrival-replay-design.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-25-adaptive-admission-controller-design.md](designs/2026-07-25-adaptive-admission-controller-design.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-25-adaptive-flush-window-design.md](designs/2026-07-25-adaptive-flush-window-design.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-25-ai-operator-execution-infra-design.md](designs/2026-07-25-ai-operator-execution-infra-design.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-25-runtime-scheduling-strategy-suite-design.md](designs/2026-07-25-runtime-scheduling-strategy-suite-design.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-26-multi-endpoint-routing-readiness-design.md](designs/2026-07-26-multi-endpoint-routing-readiness-design.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-26-output-aware-bfd-design.md](designs/2026-07-26-output-aware-bfd-design.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-26-ray-vllm-execution-tuning-design.md](designs/2026-07-26-ray-vllm-execution-tuning-design.md) | [证据台账](../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| [2026-07-26-row-cap-aware-packing-and-observation-design.md](designs/2026-07-26-row-cap-aware-packing-and-observation-design.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-28-dual-gpu-experiment-correctness-design.md](designs/2026-07-28-dual-gpu-experiment-correctness-design.md) | [证据台账](../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| [2026-07-29-daft-ray-baseline-advantage-validation-design.md](designs/2026-07-29-daft-ray-baseline-advantage-validation-design.md) | [证据台账](../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| [2026-07-29-same-condition-official-baselines-design.md](designs/2026-07-29-same-condition-official-baselines-design.md) | [证据台账](../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| [2026-07-29-saturated-ray-actor-pool-replenishment-design.md](designs/2026-07-29-saturated-ray-actor-pool-replenishment-design.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-08-13-saor-native-system-matched-comparison-design.md](designs/2026-08-13-saor-native-system-matched-comparison-design.md) | [实验状态](../../../experiments/plans/archive/进度汇总_20261001.md) |
| [2026-08-23-opening-report-cost-estimation-enhancement-design.md](designs/2026-08-23-opening-report-cost-estimation-enhancement-design.md) | [开题归档](../opening/README.md) |

### 实施记录

| 历史文件 | 现行核对 |
|---|---|
| [2026-07-17-daft-postgres-entry-existing-writeback.md](plans/2026-07-17-daft-postgres-entry-existing-writeback.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-25-accelerated-arrival-replay-implementation.md](plans/2026-07-25-accelerated-arrival-replay-implementation.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-25-adaptive-controller-family-implementation.md](plans/2026-07-25-adaptive-controller-family-implementation.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-25-adaptive-flush-window-implementation.md](plans/2026-07-25-adaptive-flush-window-implementation.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-25-arrival-replay-flush-runtime-implementation.md](plans/2026-07-25-arrival-replay-flush-runtime-implementation.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-25-ray-static-wiring-implementation.md](plans/2026-07-25-ray-static-wiring-implementation.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-25-request-lifecycle-scenario-runner-implementation.md](plans/2026-07-25-request-lifecycle-scenario-runner-implementation.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-25-scheduling-foundation-implementation.md](plans/2026-07-25-scheduling-foundation-implementation.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-26-output-aware-bfd-implementation.md](plans/2026-07-26-output-aware-bfd-implementation.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-26-ray-execution-foundation-implementation.md](plans/2026-07-26-ray-execution-foundation-implementation.md) | [证据台账](../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| [2026-07-26-row-cap-aware-packing-and-observation-implementation.md](plans/2026-07-26-row-cap-aware-packing-and-observation-implementation.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-26-single-gpu-text-closure-implementation.md](plans/2026-07-26-single-gpu-text-closure-implementation.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-26-vllm-ray-tuning-experiments.md](plans/2026-07-26-vllm-ray-tuning-experiments.md) | [证据台账](../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| [2026-07-28-dual-gpu-experiment-correctness-implementation.md](plans/2026-07-28-dual-gpu-experiment-correctness-implementation.md) | [证据台账](../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| [2026-07-29-daft-ray-baseline-advantage-validation-implementation.md](plans/2026-07-29-daft-ray-baseline-advantage-validation-implementation.md) | [证据台账](../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| [2026-07-29-official-baseline-matrix-implementation.md](plans/2026-07-29-official-baseline-matrix-implementation.md) | [证据台账](../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| [2026-07-29-same-condition-project-runtime-comparison-implementation.md](plans/2026-07-29-same-condition-project-runtime-comparison-implementation.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-07-29-saturated-ray-execution-foundation-implementation.md](plans/2026-07-29-saturated-ray-execution-foundation-implementation.md) | [证据台账](../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| [2026-07-29-shared-vllm-fairness-implementation.md](plans/2026-07-29-shared-vllm-fairness-implementation.md) | [证据台账](../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| [2026-07-29-slo-aware-ewma-flush-implementation.md](plans/2026-07-29-slo-aware-ewma-flush-implementation.md) | [代码状态](../../../code/INFRA_STATUS.md) |
| [2026-08-13-saor-native-system-matched-comparison-implementation.md](plans/2026-08-13-saor-native-system-matched-comparison-implementation.md) | [实验状态](../../../experiments/plans/archive/进度汇总_20261001.md) |
| [2026-08-23-opening-report-cost-estimation-enhancement-plan.md](plans/2026-08-23-opening-report-cost-estimation-enhancement-plan.md) | [开题归档](../opening/README.md) |
| [2026-08-23-opening-report-minimal-figure-corrections.md](plans/2026-08-23-opening-report-minimal-figure-corrections.md) | [开题归档](../opening/README.md) |

## 保留原路径的记录

[代码结构迁移计划](../../../code/ARCHITECTURE_REFACTOR_PLAN.md)被历史 source manifest 以原路径和摘要记录，
因此继续留在 `code/`；当前结构看[代码说明](../../../code/README.md)。

当前研究方向看[项目总纲](../../../PROJECT_OUTLINE.md)，实验结果看[证据台账](../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)。

## 历史方法工程参考

- [策略工程映射](designs/策略工程映射.md)：原信号、变量与接口设计；当前实现从源码核对。
- [写回协调方案](designs/写回协调方案.md)：早期写回推演与对照要求，按原日期解释。
