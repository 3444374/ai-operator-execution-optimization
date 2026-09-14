# data/AGENTS.md

继承根规则。资产清单见 [README.md](README.md)。

- Git 只保存来源、哈希、schema、导入方式和最小元数据，原始 payload 不提交。
- 下载、换机器或准备 GPU 运行时，按根 §5 和 `deploy/runtime/README.md` 完成只读 preflight，再执行获准的下载。
- 数据库 workload 需继续完成 importer，并核对行数、schema、哈希和 exactly-once；下载完成不代表可用于查询。
- README 记录预期资产与验证要求，单台机器的文件存在状态记录到该机器的报告。
