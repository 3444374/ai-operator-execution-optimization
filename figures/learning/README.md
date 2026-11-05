# Learning Figures

本目录保存学习讲解文档使用的图表，项目入口见[图资产说明](../README.md)。复用时引用这里的原件，并按下列记录区分历史用途。

## 当前图表

| 文件 | 说明 | 数据来源 |
|---|---|---|
| `cpu_gpu_coalesced_e2e_20260712.svg` | CPU/GPU endpoint 在 coalesced 模式下的端到端时间对比 | `experiments/results/motivation/cpu/` 与 `experiments/results/motivation/gpu/` |
| `fine_vs_coalesced_e2e_20260712.svg` | fine 与 coalesced 调用粒度的端到端时间对比 | `experiments/results/motivation/cpu/` 与 `experiments/results/motivation/gpu/` |
| `gpu_embed_1024_granularity_e2e_20260712.svg` | 1024 行真实 GPU embedding 下 fine/coalesced 端到端对比 | `experiments/results/motivation/gpu/ai_embed_chain_breakdown_20260712.csv` |
| `gpu_embed_4096_executor_e2e_20260712.svg` | 4096 行 coalesced 下 Python、Ray task、Ray actor 对比 | `experiments/results/motivation/gpu/ai_embed_chain_breakdown_20260712.csv` |
| `gpu_embed_16384_stage_breakdown_20260712.svg` | 16384 行 Ray actor/coalesced 下 operator、writeback、db fetch、Arrow 阶段拆分 | `experiments/results/motivation/gpu/ai_embed_chain_breakdown_20260712.csv` |

## 生成方式

GPU embedding 链路拆分图由下面脚本生成：

```powershell
.conda\pg-ai-profile\python.exe figures\scripts\make_chain_breakdown_figures.py
```

图表解释写在：

```text
experiments/results/README.md#历史实验解读
```


## 历史请求流程示意

[普通 PNG](request_shape_evolution.png)与[两倍 PNG](request_shape_evolution@2x.png)
描绘旧 PG profiler、token 切分与 HTTP metrics 轮询，保留为历史学习素材，不能说明当前 SemLoom 实现或性能。
[生成脚本](../scripts/generate_request_shape_evolution.py)已标注历史用途；本次未覆盖原导出。
[来源与保留说明](../audit/request_shape_evolution_history_20261105.md)记录替代关系和检查范围。
