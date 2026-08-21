# SAOR native-system matched 冻结 calibration 绑定（2026-08-19）

> **性质**：当前五臂 native-system matched comparison 复用的四份 selection identity 合同；
> 三份 native selection 分别对应 Daft Native、Daft Ray、Ray Data，一份 Project selection 由
> frozen-static 与 SAOR 共同复用。历史八臂口径已停止指导执行。
> Project 合同继续使用通用 calibration loader；native 合同由 matched runner 逐文件 SHA、adapter、
> concurrency 和 batch size 校验，不把 vendor default 或单次 screen 夸大成三类性能门全部通过。

## 文件与来源

| 文件 | 臂 | 来源 |
|---|---|---|
| `project_frozen_selection_20260808.json` | 当前 Project frozen-static 与 SAOR 两臂共同 selection（FIFO/DRR/VTC 仅历史消融） | 由服务器 selection 衍生并显式补齐既有 token-budget 6144 身份，SHA `55c09b95…a2c9`；selection：token-budget6144/K128/W65536/actors8/concurrency32/cpus0.25 |
| `daft_native_selection_20260808.json` | daft_native | C1/B1 vendor-default control，SHA `c8c31f99…cd6e`；只声明配置身份已核验，不声明搜索到性能最优点 |
| `daft_ray_selection_20260808.json` | daft_ray | C1/B1 vendor-default control，SHA `e866f945…3a23`；证据强度同上 |
| `ray_data_http_selection_20260808.json` | ray_data_http | C8/B16，SHA `98aa11bd…d60`；来自一次 C4/C8/C16 development screen，只冻结已测峰值，不声明统计最优 |

## 边界

- matched config 为每臂冻结 `calibration_path + calibration_sha256`；native runner 再把合同中的
  `adapter/concurrency_per_endpoint/batch_size` 与实际 executor 配置逐字段比较。
- 运行签名约束：这批 selection 绑定 2026-08-08 的 2×4090 + Qwen2.5-7B + chat_completions +
  prefix-cache ON + 8192/256 服务签名；硬件/模型/协议/workload 签名变化时全部失效，须重新校准。
- Ray Data C8/B16 只控制官方 Ray Data HTTP Processor graph 暴露的 concurrency/batch 参数；
  Daft 两臂保持 C1/B1 vendor control。三臂均由框架自身拥有 batching、backpressure 与 scheduler，
  不注入 Project organizer、K/W、credit、router 或 bounded-ready。
