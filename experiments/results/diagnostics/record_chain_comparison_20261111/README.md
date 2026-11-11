# 有限链回执比较的退出状态

问题来自 [PR #99](https://github.com/3444374/ai-operator-execution-optimization/pull/99) 的验收边界：
`compare` 已在 JSON 报告记录 `outputs_strictly_comparable=false`，但 CLI 仍退出0，
标准输出也不包含这个字段。只检查进程退出码的调用方可能把输出差异误判为通过。

## 修改与证据

基底为 `c552eddf90c9dd84a195f586a5a4114897c5fdc9`。复用原配置解析、回执读取、逐字段比较和私有报告写入，
只修正 CLI 的验收行为：**先保存完整差异，输出严格一致标记；不一致退出1，一致退出0。**
现有状态名 `captured_receipts_compared`、报告 schema 和差异明细保持；它表示比较完成，不表示性能通过。

新增检查通过真实 CLI `main`、原 JSON 读取／写入和比较函数验证三种情况：

| 临时回执 | 修改前 | 修改后 |
|---|---|---|
| 全部字段及最终关系相同 | 退出0，stdout缺少一致标记 | 退出0，stdout标记true |
| 一个输出token改变，长度不变 | JSON记录差异，但退出0 | 保存差异后退出1，stdout标记false |
| 最终关系改变 | JSON记录差异，但退出0 | 保存差异后退出1，stdout标记false |

先运行新增反例得到2个失败子场景和1个stdout字段缺失错误；修复后同组4项通过。
完整 `tests.experiments.test_record_chain_validation` 在已有隔离driver中33项通过，无跳过。
此前系统Python运行同模块时因缺LOTUS／Transformers等依赖有6项错误；失败保留，没有安装依赖或改用例绕过。
复现命令为 `PYTHONPATH=code "$DRIVER_PYTHON" -m unittest tests.experiments.test_record_chain_validation -v`。

[checks.json.gz](checks.json.gz)保存各轮完整脱敏输出、原件摘要与最终源码摘要；
仓库外原件按运行标识 `semloom-pr-followup-20261111` 保留。
本次只有临时回执、固定响应和 CPU 子进程检查，模型、GPU、服务器运行与下载均为0。

## 保留的边界

旧提交的61项服务器 CPU 检查保持其原来源，不能替代本次修改的服务器资格。
该入口每个case仍创建并关闭模型与Ray；修正退出码不会把冷查询转换为常驻暖查询。
现有Core的常驻模式绑定rolling refill，不能在两臂之一打开或通过从冷时间减启动来构造暖比较。
未来常驻对照须匹配两臂生命周期、输入、资源及完整输出，再独立校准和重复。
严格一致通过也不证明GPU饥饿、稳定吞吐或完整查询加速；报告原有性能资格限制保持。
