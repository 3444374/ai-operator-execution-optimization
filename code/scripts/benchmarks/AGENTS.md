# 组件测量与历史模拟脚本

继承代码规则。脚本与输出入口见[README.md](README.md)。

- 保存组件测量和历史fake/CPU模拟的命令入口，共享辅助函数维护在`common.py`。
- 组件输出进入`experiments/results/diagnostics/`，动机模拟进入`experiments/results/motivation/fake_cpu/`。
- 新结果保留实际环境与用途，运行授权按根及实验规则执行；可复用实现仍进入`code/src/`。
