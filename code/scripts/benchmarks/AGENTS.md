# 组件测量与历史模拟脚本

继承代码规则。脚本与输出入口见[README.md](README.md)。

- 保存真实组件测量入口，共享辅助函数维护在`common.py`；早期模拟从 README 指定的 Git 提交恢复。
- 组件输出进入`experiments/results/diagnostics/`，动机模拟进入`experiments/results/motivation/fake_cpu/`。
- 新结果保留实际环境与用途，运行授权按根及实验规则执行；可复用实现仍进入`code/src/`。
