# Learning Notes

本目录用于把项目实验和代码讲成“学习材料”。

正式结果、CSV、严谨结论仍放在：

```text
feasibility/results/
motivation/results/
```

本目录负责回答更基础的问题：

- 这个实验为什么要做？
- Ray / Daft / Lance / Arrow / pgvector / batch / actor / fan-in 这些词是什么意思？
- 每个术语在当前链路的哪一步，数据从哪里来、到哪里去？
- 数据在系统里怎么流动？
- 每个参数在控制什么？
- 结果数字怎么读？
- 这个结果对课题下一步有什么用？
- 这个实验不能证明什么？

## 阅读顺序

1. `experiment_walkthrough.md`：按时间线讲解目前已经做过的实验。

## 后续规则

每次完成新实验、代码实现或功能测试后，都应该同步更新这里的学习材料。面向论文或汇报的严谨报告继续放在 `motivation/results/` 或 `feasibility/results/`。
