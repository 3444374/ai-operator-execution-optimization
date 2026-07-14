# Learning Figures

本目录保存学习讲解文档引用的项目级图表。后续 learning、开题、中期汇报和毕业论文如需要复用这些图，应从根目录 `figures/` 引用，不再在 `learning/figures/` 维护单独副本。

## 当前图表

| 文件 | 说明 | 数据来源 |
|---|---|---|
| `cpu_gpu_coalesced_e2e_20260712.svg` | CPU/GPU endpoint 在 coalesced 模式下的端到端时间对比 | `motivation/results/cpu/` 与 `motivation/results/gpu/` |
| `fine_vs_coalesced_e2e_20260712.svg` | fine 与 coalesced 调用粒度的端到端时间对比 | `motivation/results/cpu/` 与 `motivation/results/gpu/` |
| `gpu_embed_1024_granularity_e2e_20260712.svg` | 1024 行真实 GPU embedding 下 fine/coalesced 端到端对比 | `motivation/results/gpu/ai_embed_chain_breakdown_20260712.csv` |
| `gpu_embed_4096_executor_e2e_20260712.svg` | 4096 行 coalesced 下 Python、Ray task、Ray actor 对比 | `motivation/results/gpu/ai_embed_chain_breakdown_20260712.csv` |
| `gpu_embed_16384_stage_breakdown_20260712.svg` | 16384 行 Ray actor/coalesced 下 operator、writeback、db fetch、Arrow 阶段拆分 | `motivation/results/gpu/ai_embed_chain_breakdown_20260712.csv` |

## 生成方式

GPU embedding 链路拆分图由下面脚本生成：

```powershell
.conda\pg-ai-profile\python.exe figures\scripts\make_chain_breakdown_figures.py
```

图表解释写在：

```text
learning/experiment_walkthrough.md
```
