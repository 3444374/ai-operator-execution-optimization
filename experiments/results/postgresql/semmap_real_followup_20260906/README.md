# SemMap 有限真实模型复查（2026-09-06）

内部工程验证记录。当前状态：独立服务就绪；首轮准备因引用旧预算校验脚本停止，账本仍25/32。
已将历史成功辅助脚本改为完整SHA绑定，准备在新目录执行，尚未新增推理请求。
实施依据为 [Map 合同 §8.4.4](../../../plans/postgresql_semmap_generation_contract.md)。

复用原32次持久账本，现场初值25，本次最多7次；不重置、不自动重试。固定单RTX4090、
Qwen2.5-7B-Instruct、BF16、vLLM0.25.1，验证SELECT/INSERT、NULL、取消/拒绝与恢复。
这不是质量或性能实验，也不包含正式fixture3×2000资源运行。

本目录的 `real_gateway.py` 与 `real_checks.py` 是本次可复查实验驱动，复用现有生产gateway、
历史32次账本实现、SessionObserver及修复后的资源采集/判定。原始模型payload与服务日志只保存在
服务器；公开部分将单列允许字段摘要、源码身份与原始/公开文件哈希。
