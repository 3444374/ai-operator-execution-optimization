# AIMD、EWMA-AIMD 与 PID 准入控制实验（2026-07-26）

## 1. 实验设置

问题是：在当前单 GPU、单 endpoint、单 workload 链路上，动态 K_max 控制是否比静态并发上限更有效。

真实链路为 PostgreSQL 18.4 + pgvector 0.8.2 → Daft native organizer → Ray task → vLLM 0.25.1/Qwen2.5-1.5B → RTX 5070 12GB。vLLM 使用 CUDA Graph、0.75 GPU memory utilization、MFU metrics，关闭 prefix cache；未使用 fake backend和数据库写回。服务证据见 `service.json`。

共同设置为 512 条 ShareGPT/BurstGPT 请求、arrival replay scale 0.0005、ChatML、temperature 0、自然 EOS 上限 512、token budget 6144、fixed 50ms flush。控制器共同使用初始窗口 8、范围 4–16；只改变 admission 控制律。

## 2. 实验设计

实验分三层：

1. `gate/`：64 请求，static K=8、AIMD、EWMA-AIMD、PID 各一次真实门禁。
2. `formal_512/`：四策略各 1 次 warm-up + 3 次随机交错 formal。
3. `mechanism_control_512/`：发现三个控制器几乎都升到 16 后，追加 AIMD 与 static K=16 各 1 次 warm-up + 3 次随机交错 formal，隔离“动态反馈”与“更高并发”。

复现命令：

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  code\scripts\run_ai_operator_scenarios.py `
  --config experiments\results\adaptive_admission_controller_20260726\<config.json> `
  --profiler code\scripts\postgres_ai_operator_profile.py `
  --python-executable D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  --output-dir experiments\results\adaptive_admission_controller_20260726\<run-directory> `
  --health-url http://localhost:8000/health `
  --metrics-url http://localhost:8000/metrics `
  --idle-timeout-s 420
```

三个配置分别是 `scenario_config_gate.json`、`scenario_config_512.json` 和 `scenario_config_mechanism_control.json`；每轮展开后的精确命令保存在对应 `manifest.json`。

## 3. 严谨性自检

- 三组 runner 共 28/28 runs 完成，0 incident。
- 64 门禁有 256/256 completed request。
- 两组 512 formal 共 18 runs、9,216/9,216 completed request；每轮 512 个唯一 request ID 和 doc ID，均无负时间。
- 所有 formal run 的 prompt-token delta 均为 63,970，vLLM success delta 均为 512，结果 schema 均为 201 列。
- 动态 formal 共有 2,322 个 fresh control samples，窗口始终在 4–16 内，没有 missing/stale observation。
- 实际 generation tokens 存在约 1% 自然波动，因此吞吐使用每轮实际 observed tokens 归一化。
- 多轮 Windows Ray shutdown stderr 含非致命 `access violation` 文本；static 与 adaptive 两侧都出现，profiler exit=0、manifest、请求审计和数据库 finished 状态完整。不能声称 stderr 完全干净。
- 每个策略只有 3 次 formal；结果用于机制筛选，不作跨模型、跨硬件或统计显著性结论。

## 4. 实验数据

四策略矩阵：

| 策略 | E2E mean ± sd (s) | tokens/s mean ± sd | request P99 mean (s) | MFU mean | admission limit mean |
|---|---:|---:|---:|---:|---:|
| Static K=8 | 82.079 ± 2.095 | 2814.9 ± 60.9 | 59.957 | 14.19% | 8.000 |
| AIMD 4–16 | 55.777 ± 3.051 | 4117.1 ± 249.8 | 33.397 | 21.16% | 15.927 |
| EWMA-AIMD 4–16 | 55.861 ± 3.280 | 4115.7 ± 247.3 | 33.489 | 21.13% | 15.832 |
| PID 4–16 | 57.196 ± 0.392 | 4009.3 ± 39.9 | 35.027 | 20.51% | 15.783 |

相对 static K=8，AIMD/EWMA-AIMD/PID 的 E2E 分别降低 32.04%/31.94%/30.32%，tokens/s 分别增加 46.26%/46.21%/42.43%。但三个控制器平均窗口都接近上限 16。

必要的机制对照：

| 策略 | E2E mean ± sd (s) | tokens/s mean ± sd | request P99 mean ± sd (s) | energy J/1k tokens | MFU |
|---|---:|---:|---:|---:|---:|
| Static K=16 | 58.943 ± 0.569 | 3899.3 ± 36.4 | 37.132 ± 0.463 | 39.513 | 19.80% |
| AIMD 4–16 | 59.334 ± 0.614 | 3872.2 ± 39.7 | 37.106 ± 0.446 | 39.659 | 19.78% |

在随机交错的机制对照中，AIMD 相对 static K=16：E2E +0.66%、tokens/s -0.69%、P99 -0.07%、SLO goodput -0.66%、每千 token 能耗 +0.37%、MFU -0.13%。差异均小于或接近重复波动。

绘图入口为 `comparison_summary.csv`、`formal_512/summary_long.csv`、`mechanism_control_512/summary_long.csv`；时序图使用各目录的 `.control.csv`，请求尾延迟使用 `.requests.csv`。

## 5. 结果解释

**事实**：三种动态控制器在这个持续 backlog 的单作业 workload 上都迅速把窗口升到接近 16，因此显著优于 static K=8。

**事实**：加入同并发上限的 static K=16 后，简单静态策略与 AIMD 不可分辨，并在 E2E、吞吐、goodput、能耗和 MFU 上有极小的数值优势。

**推断**：当前动态策略的收益主要来自选择了更高并发，而不是对队列/KV 信号的在线反馈。该 workload 没有提供足够的阶段变化让动态控制展示降载价值；AIMD 总共只有极少 downshift。

**不能声称**：

- 不能声称 AIMD、EWMA 或 PID 优于最佳静态 K_max。
- 不能把单作业 static K=16 推荐为共享 vLLM 默认；既有双作业干扰证据仍支持 K=8 作为共享服务 guardrail。
- 不能据此淘汰动态控制器；突发阶段变化、多租户干扰和真实多 endpoint 环境仍未验证。
- 不能把 CUDA Graph 的部署收益计入上游 admission 方法贡献。

## 6. 对课题的含义

这组实验补齐了控制器代码之后缺失的真实 GPU 矩阵，也给出了负机制结论：复杂控制律不能只和较低的静态窗口比较，必须加入其实际收敛上限作为机制 control。当前单作业默认应优先使用经 workload 筛选的静态窗口；动态 admission 仅在共享服务或负载阶段变化中继续验证。

## 7. 下一步

最有价值的后续不是继续调 PID 参数，而是重跑真实 shared-vLLM foreground/background：比较 static K=8、static K=16 和 AIMD，报告 foreground E2E/P99 与 background tokens/s。若 AIMD 仍不能同时满足 foreground 保护和至少 90% background throughput，则将动态 K_max 降为 discussion 候选。UCB 在 epoch reward 归因闭环前仍不接入。
