# deploy/runtime/AGENTS.md

本目录维护跨机器的运行时合同，继承根 `AGENTS.md` 与 `deploy/AGENTS.md`。

- machine profile 只描述可验证能力，不写实验最优参数。
- `assets.json` 只声明可信来源、目标和完整性门槛；受许可资产必须 `manual` fail closed。
- `check` 默认只读；安装、下载和数据库导入必须是相互独立的显式动作。
- runtime env 示例不得包含密钥；实际 env 永远放仓库外。
- 新增能力/资产必须有单元测试、README 使用方式和 `PROJECT_INDEX.md` 入口。
- 不把 driver 与 vLLM 依赖合并，不自动安装驱动/CUDA，不把下载成功当 workload 正确。
