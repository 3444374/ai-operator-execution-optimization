# motivation/results/fake_cpu/AGENTS.md

本文件继承根、`motivation/AGENTS.md` 与 `motivation/results/AGENTS.md`，只增加 fake/CPU 历史预研
结果规则。

## 作用

- 追溯早期 task/object/fan-in/backpressure 信号。
- 调试计时边界和脚本框架。
- 为 GPU-backed E2E 消融设计提供假设来源。

## 边界

- 不能作为真实 GPU-backed 链路瓶颈归因。
- 不能替代生产式 GPU-backed 主动机实验。
- 新主线结果不要继续放这里。
