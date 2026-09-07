# deploy/runtime/AGENTS.md

本目录维护跨机器的运行时合同，继承根 `AGENTS.md` 与 `deploy/AGENTS.md`。

- machine profile 只描述可验证能力，不写实验最优参数。
- 默认按观测硬件自动选择 profile；显式 `--machine-profile` 只用于诊断/覆盖，报告必须标注选择来源。
- 多机器轮流运行时，每台机器保留自己的仓库外 runtime env；禁止把路径、密钥或本机缓存提交到 Git。
- 性能参数按“机器 + 模型/版本 + 服务配置 + 协议 + workload 分布”独立校准并冻结；签名变化时旧校准失效。
- `assets.json` 只声明可信来源、目标和完整性门槛；受许可资产必须 `manual` fail closed。
- `check` 默认只读；安装、下载和数据库导入必须是相互独立的显式动作。
- runtime env 示例不得包含密钥；实际 env 永远放仓库外。
- 新增能力/资产时同步使用说明与受影响入口，核对来源、目标路径和完整性检查；新增或改变
  解析、校验、安装行为时补充对应测试，单纯资产元数据变更复用已有校验。
- 不把 driver 与 vLLM 依赖合并，不自动安装驱动/CUDA，不把下载成功当 workload 正确。
