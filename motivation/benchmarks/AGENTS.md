# motivation/benchmarks/AGENTS.md

本文件继承根 `AGENTS.md` 与 `motivation/AGENTS.md`，只增加动机实验脚本规则。

## 可以放

- 服务于数据库 AI 算子动机、系统画像或消融的脚本。
- 历史 fake/CPU 动机脚本，但必须在 README 中标注不能外推为 GPU-backed 结论。

## 不放

- 纯组件可行性脚本。应放 `feasibility/benchmarks/`。
- 正式工程模块。应放 `code/`。
- 只验证环境连通性的 smoke 脚本。

## 规则

- 默认输出路径应指向 `motivation/results/` 下的具体结果层级。
- 脚本移动或改名后要更新 README、计划文档和运行命令。
- 新 GPU-backed 脚本应记录 DB fetch、Arrow build、submit/put、queue wait、model service、fan-in 和 writeback 阶段。
