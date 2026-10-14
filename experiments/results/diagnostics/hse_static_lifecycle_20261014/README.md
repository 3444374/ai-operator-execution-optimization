# HSE static集成切片

本分支从`3e7bd54b`构造可顺序挑选的A／B／C；已发布`47f7800c`与原修复分支保持。
A仅修正确性，B历史扫描优化、C ready优先均已独立加入；生产行为和测试依赖如下。
[原完整审查与失败证据](https://github.com/3444374/ai-operator-execution-optimization/blob/47f7800ce5fbe280c5075165453b2c6dec5437f4/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md)属于47f的完整组合，不能当作当前A含B／C的证据。

| 片 | 精确代码／测试 | 依赖与默认影响 |
|---|---|---|
| A正确性 | staged_execution的提交前owner登记、payload至完成证明持有、异常确认／未知池保护、局部引用清理和admission峰值；stage_broker的lease时间原子校验、永久超大ready拒绝；10项lifecycle＋broker时间／超大块／临时FIFO三项 | 从3e直接应用；保留source→CPU提交→model提交→wait、FIFO和全终态历史扫描；CLI默认direct_dependency保持；正常HSE无新增确认RPC |
| B历史扫描 | 仅stage_broker的活动集合／终态计数及其转换、snapshot和不变量；broker不重扫历史及retry/cancel计数两项 | 可在A上独立加入；不改HSE顺序；保留默认历史与duplicate检测；当前已包含 |
| C ready优先 | 将A已修正的model提交循环提取为helper；source.next前及原model派发位置调用；1项顺序测试的局部source／GPU fixture | 依赖A的owner/ref登记与公共fixture；不依赖B；当前已包含 |

A不引入新worker、模型、source、batch、输出schema或资源选点。逻辑byte/work额度不代表全部
RSS、Arrow／Ray副本、pinned对象或device内存上限。临时FIFO等待仍保留，不增加fit-aware策略。
A和A+B保留source先行；加入C后已有ready model先于source.next派发。A的异常归还仍等待串行CPU ready／GPU synchronize完成证明；确认失败保留lease与ObjectRef，
后续同池HSE拒绝。共享Core和broker本地检查见[逐片机器记录](integration_checks.json)。

[CLI／pool factory固定源码交接](https://github.com/3444374/ai-operator-execution-optimization/blob/ed20923876d668d6b11b0e4005cca2c7caa89c12/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md#integration-handoff)列出47f上的外层异常清理调用点。
这些文件由image owner独占，并正在修正部分构建清理及连接容量说明；本分支不重复修改。
[零模型实际Ray包](https://github.com/3444374/ai-operator-execution-optimization/blob/ed20923876d668d6b11b0e4005cca2c7caa89c12/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md#ray-validation-package)在ed209238独立发布，26成员、4 CPU／0 GPU／512 MiB
object store、5项真实Ray用例。其生产模块匹配完整47f；不能借此认领A集成HEAD的实库验证。
当前模型owner持有共同lease，HSE等待主会话通知，未占锁／启动Ray／上传文件。
健康运行没有总墙钟截断，单操作60秒、故障确认30秒、自有actor退出30秒分别计时。

## B的独立成本证据

B仅维护活动集合与终态缓存，所有fail／retry／cancel／release更新保持原计数语义。
[CPU原值](broker_cost.json)和[复算入口](probe_broker.py)复用47f原件，B的broker源码与该测量版逐字节相同。
固定单活动窗口、累计1k／2k／4k块各三次，中位耗时基点0.071473／0.228983／0.793945秒，
B为0.030907／0.061171／0.123499秒。只解释全历史扫描成本下降，不提供Ray、GPU、RSS或端到端收益。
A+B的31项定向通过，C顺序测试在A+B上按预期失败；加入C后该测试通过。

## C的独立派发行为

C只提取已有model提交循环并在source.next前调用；owner/ref登记等A修复保持。
调度额度、FIFO、batch和正常完成同步保持；仅显式HSE受影响，CLI默认仍direct_dependency。
A+B+C四个生产／测试文件逐字节等于47f原完整交付；累计32项定向通过，共享回归另行记录。
真实GPU收益和物理副本／内存、完整吞吐／延迟仍未验证，不能把顺序测试当作性能结论。

独立审查以A broker与C HSE内存装配运行11项lifecycle＋原staged4项，15／15通过，
证明C不需要B。另一审查者确认model提交循环提取前后相同；source在提前派发后抛错时，
原错误、未确认model额度和同池拒绝仍保持。两者均为本地受控CPU检查，不新增实库资格。
