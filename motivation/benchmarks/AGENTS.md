# motivation/benchmarks/AGENTS.md

继承上级规则。脚本入口见 [README.md](README.md)。

- 只保存服务于动机、系统画像或消融的脚本；组件测试和连接 smoke 进入 `feasibility/`，可复用实现进入 `code/`。
- 默认输出指向 `results/motivation/` 中对应类型；历史 fake/CPU 脚本在 README 标明证据用途。
- 新 GPU-backed 脚本记录 DB fetch、Arrow build、submit/put、queue wait、model service、fan-in、writeback。
- 脚本移动或改名时更新受影响的 README、计划和运行命令。
