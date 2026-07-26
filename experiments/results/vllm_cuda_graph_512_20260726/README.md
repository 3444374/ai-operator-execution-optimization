# vLLM Eager vs CUDA Graph（512 请求，2026-07-26）

## 1. 实验设置

本实验回答一个部署层基线问题：在不改变 Daft 数据组织、Ray 提交策略和
请求 workload 的前提下，移除 vLLM `--enforce-eager`、启用默认
CUDA Graph / `torch.compile` 后，单 RTX 5070 上的稳态端到端性能是否改善。
该实验用于选定后续调度实验的 vLLM baseline，不是本文提出的上游调度策略。

共同链路为 PostgreSQL 18.4 + pgvector 0.8.2 → Daft native organizer →
Ray task → vLLM 0.25.1 / Qwen2.5-1.5B。每轮输入 512 个相同且顺序一致的
ShareGPT/BurstGPT 文档，token budget=6144，static K=8，fixed 50ms，
2 个客户端 model workers，最大输出 512 tokens，prefix cache 关闭。

唯一服务变量：

| Arm | vLLM execution | Metadata |
|---|---|---|
| eager | `--enforce-eager` | `enforce_eager=true`, `compilation_mode=disabled` |
| graph | 移除 `--enforce-eager` | `enforce_eager=false`, `compilation_mode=default` |

配置自动 diff 确认，除 experiment ID 和上述两个 metadata 字段外，gate
与 512 配置的所有负载和调度参数分别完全相同。

## 2. 实验设计与运行

先运行每个服务的 64-request 真实门禁，再运行 1 次 512-request warm-up
和 3 次 formal。graph 服务使用与 eager 相同的 Docker image、端口、
GPU memory utilization 和只读模型挂载；旧 eager 容器被停止并重命名为
`ai-operator-vllm-qwen-eager-backup`，未删除。

graph 运行命令：

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  code\scripts\run_ai_operator_scenarios.py `
  --config experiments\results\vllm_cuda_graph_512_20260726\scenario_config_graph.json `
  --profiler code\scripts\postgres_ai_operator_profile.py `
  --python-executable D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  --output-dir experiments\results\vllm_cuda_graph_512_20260726\graph `
  --health-url http://localhost:8000/health `
  --metrics-url http://localhost:8000/metrics
```

原始数据见 `eager/runs.csv`、`graph/runs.csv` 及各轮 request/submission/
flush/resource trace；绘图汇总见 `comparison_summary.csv`。两侧容器配置见
`eager_service.json` 和 `graph_service.json`，带 Docker 时间戳的 graph
启动证据见 `graph_startup_evidence.json`。

## 3. 严谨性自检

- eager 与 graph 的三轮 formal 每轮均为 512/512 完成；两侧 manifest
  最终均 completed，graph 为 0 incident。
- 六轮 formal 均为 137 个 packing batches / submissions，prompt token
  delta 均为 63,970；对应重复的 512 个 `doc_id` 集合和顺序完全一致。
- graph 每轮 512 个 request ID、doc ID、actual output token 和 finish
  reason 均完整且唯一覆盖；四个数据库 job 1132–1135 均为 `finished`。
- graph 每轮保留 230–241 条 resource samples，MFU、FLOP、功耗、能耗字段
  均为有效值；vLLM 日志未发现 OOM、CUDA Graph failure 或 EngineCore failure。
- request ID 包含数据库 job ID，因此 eager 与 graph 的 request ID 字符串
  不应相等；配对依据是相同顺序的 `doc_id`。
- `temperature=0` 不保证不同执行模式逐 token 完全相同。对应重复仅
  231/236/232 个文档的 output-token count 相同，finish reason 一致数为
  470/466/470；graph 的 generation token 均值比 eager 高 0.57%。
  因而吞吐比较使用各轮实际观测 token，不能把输出写成逐请求完全相同。
- 两侧均出现 Windows Ray shutdown 阶段的非致命日志：eager formal 3，
  以及 graph warm-up、formal 1、formal 2 的 stderr 含有
  `access violation` 文本。对应运行进程 exit code 仍为 0，manifest、
  exactly-once trace、vLLM success delta 和数据库状态均完整。该 shutdown
  噪声不改变本次完成数据，但仍需单独修复，不能称任一侧日志完全干净。

## 4. 实验数据

以下为 3 次 formal 的均值 ± 样本标准差：

| Metric | eager | CUDA Graph | 均值比值变化 |
|---|---:|---:|---:|
| E2E (s) | 282.756 ± 23.291 | 79.854 ± 1.847 | -71.76% |
| rows/s | 1.819 ± 0.150 | 6.414 ± 0.150 | +252.59% |
| observed tokens/s | 812.234 ± 67.446 | 2875.684 ± 65.103 | +254.05% |
| request P50 (s) | 175.962 ± 1.487 | 32.260 ± 2.024 | -81.67% |
| request P95 (s) | 255.545 ± 20.252 | 56.917 ± 1.679 | -77.73% |
| request P99 (s) | 259.822 ± 22.474 | 57.630 ± 1.938 | -77.82% |
| SLO violation ratio (180s) | 48.112% ± 1.856% | 0% | -48.112 pp |
| GPU utilization | 95.236% ± 0.342% | 92.130% ± 0.302% | -3.11 pp |
| MFU estimate | 4.025% ± 0.334% | 14.511% ± 0.337% | +10.49 pp |
| Energy (J/run) | 22851.3 ± 953.5 | 12744.2 ± 163.5 | -44.23% |
| J / 1k observed tokens | 99.954 ± 4.168 | 55.517 ± 0.669 | -44.46% |

graph formal 1/2/3 的 E2E 分别为 77.748/81.199/80.615s，observed
tokens/s 为 2950.127/2829.402/2847.523，MFU 为
14.898%/14.278%/14.358%。这三轮没有单次异常值驱动均值。

## 5. 结果解释

**事实**：在当前单 GPU、当前 vLLM 版本和 512-request 负载上，默认
CUDA Graph 配置相对 eager 显著缩短稳态 E2E 和 request tail，同时提高
实际 token 吞吐和 MFU，并降低每千 observed tokens 的能耗。GPU utilization
略低而吞吐明显更高，说明仅看 utilization 不能判断执行效率。

**推断**：主要收益与 eager 模式禁用了编译/图重放有关，但本实验只做了
服务级二元对照，不能把总收益拆分为 `torch.compile`、piecewise graph、
full decode graph 各自的贡献。

## 6. 对课题的含义与结论边界

后续 Ray task/actor、capacity、prefix cache 和上游调度实验应以 graph
服务作为当前本地 steady-state baseline，否则 eager 的部署层损失会掩盖
上游策略差异。该收益属于 vLLM 部署配置选择，不应包装为论文的方法贡献。

本结果不能外推到其他 GPU、模型、prompt/output 分布、多 GPU 或多 endpoint；
也不能声称生产环境一定有 3.5× 提升。当前只有一次服务启动和 n=3 formal，
且 eager arm 曾发生一次运行前 schema-preflight incident；两侧均有
非致命 Windows Ray shutdown stderr 噪声。

## 7. 启动成本与下一步

根据 `graph_startup_evidence.json` 中保存的 Docker 时间戳，graph 容器从
Created 到 application startup complete 为 148.512s。vLLM 日志记录：
模型加载 23.139s、`torch.compile` 13.51s、graph capture 4s、engine
初始化 72.26s；graph pool 实占 0.48 GiB。上述一次性启动成本未计入
steady-state E2E。

下一步按调优计划在 graph baseline 上做 Ray task vs endpoint-local actor
的 64-request correctness 门禁和 512-request 重复；同时把 Windows Ray
shutdown stderr 问题作为独立基础设施缺陷处理，不与策略性能结论混合。
