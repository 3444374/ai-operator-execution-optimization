# Shared-vLLM adaptive admission experiment

Date: 2026-07-26

## 1. 实验设置

本实验验证单个 vLLM 服务被前台延迟敏感任务和后台吞吐任务同时使用时，
typed AIMD admission controller 是否能优于静态并发上限。

- GPU：NVIDIA GeForce RTX 5070 12 GB；
- vLLM：0.25.1，CUDA Graph，Qwen2.5-1.5B，prefix cache 关闭；
- 数据库：PostgreSQL 18.4 + pgvector 0.8.2 本地同构预演环境；
- 执行链路：PostgreSQL → Daft → Ray task → shared vLLM；
- workload：`sharegpt_burstgpt`，按 `arrival_time` 排序并压缩重放；
- batching：sequential token budget 6144，row cap 64；
- completion：ChatML，temperature 0，`max_tokens=512`，记录真实 token IDs；
- flush：固定 50 ms；
- 前台：128 请求，静态 K=8；
- 后台：512 请求，分别使用静态 K=8、静态 K=16、AIMD 4–16（初始 8）。

服务启动证据见 `service.json`，完整场景配置见 `scenario_config.json`。
用户追问后追加 shared-service adaptive-flush 分支，配置见
`adaptive_flush_config.json`。

## 2. 实验设计

先使用前台 32、后台 128、每组一次的门禁，确认双进程链路、独立 trace 和
exactly-once。门禁通过后，正式实验进行三轮配对重复。每轮先运行一次前台
solo baseline，再用固定随机种子交错三个共享服务场景，避免把 GPU 热状态和
运行时间漂移固定绑定到某个策略。

正式运行命令：

```powershell
.conda\pg-ai-profile\python.exe code\scripts\experiments\run_kmax_interference_experiment.py `
  --repeats 3 --random-seed 20260804 `
  --foreground-rows 128 --background-rows 512 `
  --ray-batch-rows 64 --completion-max-tokens 512 `
  --background-static-kmax 8,16 --include-aimd `
  --controller-min-window 4 --controller-max-window 16 `
  --controller-initial-window 8 --ramp-up-s 1.5 `
  --trace-dir experiments\results\shared_vllm_adaptive_admission_20260726\formal_512\traces `
  --small-output experiments\results\shared_vllm_adaptive_admission_20260726\formal_512\foreground.csv `
  --bulk-output experiments\results\shared_vllm_adaptive_admission_20260726\formal_512\background.csv `
  --overwrite
```

追加的 adaptive-flush 分支保持相同规模和 admission 参数，只把 flush 从
fixed-50 改为 queue-adaptive 25–50 ms，并对 K=8 和 AIMD 各重复三次。该分支
与 fixed-50 连续分块运行，不是完整随机交错的 2×2 factorial，因此只把明显
超过运行噪声的差异解释为机制证据。

## 3. 严谨性自检

- 门禁 7/7 个进程成功，512/512 请求 exactly-once，0 失败。
- 正式实验 21/21 个进程成功；12 个前台运行、9 个后台运行。
- 21 个 request trace 共 6144/6144 请求，0 失败、0 重复请求键。
- 每个进程有独立 request/submission/resource/flush trace；三个 AIMD 后台
  运行各有独立 control trace。
- 三轮策略顺序分别为 K16→K8→AIMD、K8→AIMD→K16、
  AIMD→K16→K8。
- 本轮外层 runner 已按 `scenario_config.json` 的 seed 交错，但当时没有把
  外层 seed/scenario ID 转发给子 profiler，所以原始主 CSV 的
  `random_seed=0`、`scenario_id=manual`；唯一的 `experiment_id` 和显式顺序
  仍可完整还原运行。runner 已在实验后修复，后续 CSV 会直接记录外层字段。
- Ray 在 Windows 进程退出阶段仍打印已知的非致命 shutdown 栈；所有 profiler
  与 runner 退出码均为 0，trace 和主 CSV 完整。
- 并发进程读取的是同一组 vLLM Prometheus 累计量，两个进程各自记录的
  `tokens_per_s` 会重叠，不能当成每个 job 的独立吞吐。因此本文用 request
  trace 中的真实 `total_tokens` 计算后台 job 吞吐和共享场景总吞吐。
- 预配置的 180 s SLO 对当前规模过宽，violation 均为 0；本文不把该字段作为
  策略胜负依据，而报告原始 P95/P99 和相对 solo slowdown。
- adaptive-flush 门禁 5/5 个进程、352/352 请求成功；正式分支 15/15 个
  进程、4224/4224 请求成功，0 失败、0 重复请求键。

## 4. 实验数据

均值 ± 样本标准差，n=3：

| 后台策略 | 前台 E2E (s) | 前台 request P99 (s) | 后台真实 tokens/s | 场景总真实 tokens/s | MFU | 平均 admission limit |
|---|---:|---:|---:|---:|---:|---:|
| static K=8 | 40.214 ± 0.554 | 23.003 ± 0.473 | 2596.6 ± 25.5 | 3301.0 ± 32.5 | 16.49% | 8.000 |
| static K=16 | 55.743 ± 1.278 | 38.307 ± 1.396 | 3603.1 ± 22.4 | 4625.0 ± 22.1 | 23.10% | 16.000 |
| AIMD 4–16 | 56.423 ± 0.779 | 39.065 ± 0.670 | 3550.8 ± 13.7 | 4572.3 ± 20.1 | 22.84% | 15.953 |

AIMD 相对 static K=8：

- 前台 E2E `+40.31%`，前台 request P99 `+69.83%`；
- 后台真实 tokens/s `+36.75%`，场景总 tokens/s `+38.52%`。

AIMD 相对同上限 static K=16：

- 前台 E2E `+1.22%`，前台 request P99 `+1.98%`；
- 后台真实 tokens/s `-1.45%`，场景总 tokens/s `-1.14%`。

三个 AIMD control trace 共记录 774 次决策，只有 12 次 increase、0 次
decrease。每轮窗口都从 8 快速升至 16，均值均为 15.953；vLLM
`waiting` 始终为 0，虽然 `running` 最大达到 84–91。

逐轮数据见 `formal_512/paired_runs.csv`，汇总及相对变化见
`formal_512/comparison_summary.csv`。

### Adaptive flush 对照

| Admission | Flush | 前台 E2E (s) | 前台 request P99 (s) | 后台真实 tokens/s | 场景总真实 tokens/s |
|---|---|---:|---:|---:|---:|
| static K=8 | fixed 50 ms | 40.214 | 23.003 | 2596.6 | 3301.0 |
| static K=8 | adaptive 25–50 ms | 39.570 | 22.315 | 2569.1 | 3261.3 |
| AIMD | fixed 50 ms | 56.423 | 39.065 | 3550.8 | 4572.3 |
| AIMD | adaptive 25–50 ms | 56.476 | 39.138 | 3553.9 | 4583.8 |

adaptive flush 相对同 admission 的 fixed-50：

- static K=8：前台 E2E `-1.60%`、P99 `-2.99%`，但后台 tokens/s
  `-1.06%`、场景总 tokens/s `-1.20%`；
- AIMD：四项差异均小于 `0.3%`。

全部 2948 条 adaptive flush 决策中，2636 条选择 50 ms，312 条选择
25 ms；`running_pressure` 占 2630 条。它在共享压力下约 89.4% 的时间选择
最大窗口，因此行为接近 fixed-50。

两种 flush 的统一汇总见 `admission_flush_comparison.csv`，adaptive 分支逐轮
数据见 `adaptive_flush_formal_512/paired_runs.csv`。

## 5. 结果解释

**事实**：static K=8 相比 static K=16，将前台 E2E 降低约 27.9%，将前台
P99 降低约 40.0%，代价是后台吞吐降低约 27.9%。这复验了共享服务中
admission guardrail 的吞吐—前台尾延迟权衡。

**事实**：当前 AIMD 没有优于任一静态端点。它相对 K=8 明显伤害前台；相对
K=16，前台延迟、后台吞吐和总吞吐均略差。

**机制证据**：AIMD 没有发生一次 multiplicative decrease。当前拥塞条件依赖
vLLM waiting 或高 KV usage；本实验的延迟竞争发生时 waiting 仍为 0，控制器
因而迅速饱和到 K=16。`running` 升高只使控制器 hold，不会触发 decrease。

**推断**：当前反馈信号不能识别“尚未进入 vLLM waiting 队列、但已经显著伤害
并发前台任务”的软拥塞。继续在相同稳态 workload 上调 PID/EWMA 参数不能解决
观测信号缺失。

**Adaptive flush 事实**：它确实根据运行压力切换窗口，但绝大多数决策选择
50 ms；在 AIMD 下与 fixed-50 几乎完全一致，在 K=8 下只表现为约 1–3% 的
延迟改善和约 1% 的吞吐损失。结合该分支的时间分块边界，当前不能声称
adaptive flush 优于 fixed-50。

**不能声称**：

- 不能声称所有动态 admission controller 都无效；
- 不能外推到多 endpoint、多 GPU、多租户或不同模型；
- 不能把背景进程记录的 Prometheus `tokens_per_s` 当作 job 独享吞吐；
- 不能从三次重复声称微小的 AIMD/K16 差异具有统计显著性。

## 6. 对课题的含义

本实验强化了“K_max 是共享 vLLM 的必要上游 admission guardrail”这一结论，
但否定了当前 AIMD 实现能提供额外收益。当前单 endpoint 共享服务默认继续使用
static K=8 + fixed 50 ms，而不是 AIMD、static K=16 或 adaptive flush。
动态控制器保留为可扩展机制，不作为已验证贡献。

## 7. 下一步

1. 不继续在当前稳态 workload 上搜索 AIMD/EWMA/PID 参数。
2. 不为约 1–3% 的 adaptive-flush 分块差异追加大规模参数搜索；需要正式主张时
   再做完整随机化 2×2 factorial。
3. 若继续动态控制，只测试能观察前台 SLO/排队延迟或服务时间膨胀的控制信号，
   并先做 fatal-flaw gate。
4. 以 static K=8 + fixed 50 ms 为共享服务基线，扩展不同 foreground size、arrival offset
   和多 job 数量，验证 guardrail 的边界与公平性。
5. 主线可转向尚未完成的 length-align + token-budget 正式重复和多模态泛化。
