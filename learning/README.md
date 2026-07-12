# Learning Notes

本目录用于把项目实验、代码和术语讲成学习材料。

正式 CSV、严谨结果报告和论文式结论仍放在：

```text
feasibility/results/
motivation/results/
```

`learning/` 负责回答更基础的问题：

- 这个实验为什么要做？
- 数据从哪里来，经过哪些系统，再写到哪里？
- Ray / Arrow / pgvector / batch / actor / fan-in / backpressure / writeback 是什么意思？
- 每个参数在控制什么？
- 每个结果字段怎么读？
- 这个结果对课题下一步有什么用？
- 这个实验不能证明什么？

## 阅读顺序

1. `experiment_walkthrough.md`：按项目推进顺序讲解已经完成的实验。
2. `figures/README.md`：学习用实验图表清单。

## 当前重点章节

| 章节 | 内容 |
|---|---|
| 第 9 节 | GPU-backed 真实 embedding 画像 |
| 第 10 节 | CPU/GPU 对比，以及 `model_service_s` 为什么不能直接当阶段占比 |
| 第 13 节 | 真实 embedding 链路拆分：当前开题动机最应优先学习的一组结果 |

## 当前重点图表

图表放在：

```text
learning/figures/
```

当前新增的 GPU 链路拆分图：

- `gpu_embed_1024_granularity_e2e_20260712.svg`
- `gpu_embed_4096_executor_e2e_20260712.svg`
- `gpu_embed_16384_stage_breakdown_20260712.svg`

## 更新规则

每次完成新实验、代码实现或功能测试后，都要同步检查：

- `learning/experiment_walkthrough.md` 是否需要新增讲解；
- `learning/figures/` 是否需要新增图；
- 本 README 的阅读入口是否需要更新。

学习材料可以讲得更通俗，但不能改变正式实验事实。
