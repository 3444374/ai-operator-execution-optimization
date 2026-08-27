# data/AGENTS.md

本文件继承根 `AGENTS.md`，只增加本地 workload 资产、许可与导入规则。当前资产清单见 `README.md`；
原始数据默认被 Git 忽略。

## 规则

- Git 只保存数据来源、哈希、schema、导入方式和最小元数据，不提交原始 payload。
- 下载模型或数据、切换机器或准备 GPU 实验前，先读 `deploy/runtime/AGENTS.md` 和
  `deploy/runtime/README.md`，按 README 选择 `--groups` 和 `--json-out` 运行只读 preflight，再使用
  受许可的显式下载命令。
- 下载完成不代表数据库 workload 已就绪；继续执行 importer，并通过行数、schema、哈希和
  exactly-once 检查。
- 不把某台机器的本地文件存在状态写成跨机器事实；README 只描述预期资产和可验证合同。
