# HSE static集成切片

本分支从`3e7bd54b`构造可顺序挑选的A／B／C；已发布`47f7800c`与原修复分支保持。
A仅修正确性，B历史扫描优化、C ready优先分别待加入；生产行为和测试依赖如下。
[原完整审查与失败证据](https://github.com/3444374/ai-operator-execution-optimization/blob/47f7800ce5fbe280c5075165453b2c6dec5437f4/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md)属于47f的完整组合，不能当作当前A含B／C的证据。

| 片 | 精确代码／测试 | 依赖与默认影响 |
|---|---|---|
| A正确性 | staged_execution的提交前owner登记、payload至完成证明持有、异常确认／未知池保护、局部引用清理和admission峰值；stage_broker的lease时间原子校验、永久超大ready拒绝；10项lifecycle＋broker时间／超大块／临时FIFO三项 | 从3e直接应用；保留source→CPU提交→model提交→wait、FIFO和全终态历史扫描；CLI默认direct_dependency保持；正常HSE无新增确认RPC |
| B历史扫描 | 仅stage_broker的活动集合／终态计数及其转换、snapshot和不变量；broker不重扫历史及retry/cancel计数两项 | 可在A上独立加入；不改HSE顺序；保留默认历史与duplicate检测；当前未包含 |
| C ready优先 | 将A已修正的model提交循环提取为helper；source.next前及原model派发位置调用；1项顺序测试的局部source／GPU fixture | 依赖A的owner/ref登记与公共fixture；不依赖B；当前未包含 |

A不引入新worker、模型、source、batch、输出schema或资源选点。逻辑byte/work额度不代表全部
RSS、Arrow／Ray副本、pinned对象或device内存上限。临时FIFO等待仍保留，不增加fit-aware策略。
A的异常归还仍等待串行CPU ready／GPU synchronize完成证明；确认失败保留lease与ObjectRef，
后续同池HSE拒绝。共享Core和broker本地检查见[逐片机器记录](integration_checks.json)。

[CLI／pool factory固定源码交接](https://github.com/3444374/ai-operator-execution-optimization/blob/ed20923876d668d6b11b0e4005cca2c7caa89c12/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md#integration-handoff)列出47f上的外层异常清理调用点。
这些文件由image owner独占，部分构建清理及连接容量说明已在`2f116d4a`完成，并以
[图像集成`1b90c1ea`的固定SDK记录](../image_file_sdk_20261014/README.md)交付；本切片不重复修改。
[零模型实际Ray包](https://github.com/3444374/ai-operator-execution-optimization/blob/ed20923876d668d6b11b0e4005cca2c7caa89c12/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md#ray-validation-package)在ed209238独立发布，26成员、4 CPU／0 GPU／512 MiB
object store、5项真实Ray用例。其生产模块匹配完整47f；不能借此认领A集成HEAD的实库验证。
原A交接时尚未取得实际窗口；当前集成的独立A-only资格见[下方记录](#integration-a-qualification)，不借组合结果。
健康运行没有总墙钟截断，单操作60秒、故障确认30秒、自有actor退出30秒分别计时。


<a id="integration-a-qualification"></a>

## 图像集成上的独立 A-only 资格

固定测量源码 `45da0a5451bfcf785d98b9ab5336054bdbaf199f`，图像基点 `1b90c1ea39e12c9fff2e2265aae003e25aa9d618`。
A 的两份生产文件和两份回归与 `d307bb92` 逐字节一致；实际 Ray 测试入口逐字取 `ed209238`。
B 历史扫描优化与 C ready 优先均未引入，保留 source→CPU→model→wait、FIFO、完整历史与
CLI 的 direct_dependency 默认。其余生产源码以及 native N/S、IMLane、图像 reader／CLI 保持图像基点。

[独立机器记录](integration_a_verification.json)绑定本轮源码、版本、逐成员摘要和退出证明；
[19成员公开原件](raw/integration-a-sdk.tar.gz)保存实际用例、预检、两个driver、观察器和本地回归。
179 个 Git 输入全部与固定源码逐字节核验。原
[组合五项Ray资格](https://github.com/3444374/ai-operator-execution-optimization/blob/f735fa6a7f223ff55addd69c69907ffaabcf722e/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md#actual-ray-checks)
仍只属于47f／A+B+C，没有更名或复用为本轮。

| 本轮检查 | 结果与范围 |
|---|---|
| 本地受影响组合 | 63 run、62通过、1个真实Daft/Arrow适配因缺库跳过、0失败；原A的29定向检查另列 |
| 独立A-only实际Ray | 5通过／0失败／0错误／0跳过：多返回与尾批、prepare／模拟model确认后归还、未确认引用和额度保留及同池拒绝、submit确认丢失后的串行fence |
| 共享准备数据适配 | 1通过／0跳过，分别使用真实Daft和Arrow表核对合并buffer、行身份及释放；Ray transport为替身，没有HTTP／模型调用 |

Ray driver 为 Python 3.12.3、NumPy 1.26.4、Arrow 16.1.0、Ray 2.56.1、psutil 7.0.0；
共享数据driver为 NumPy 1.26.4、Arrow 24.0.0、Daft 0.7.21、Ray 2.56.1、psutil 7.2.2。
均复用现有环境，没有安装依赖。Ray请求4 CPU／0 GPU、512 MiB object store、同时至多3个
CPU fixture actor；这些声明不构成OS CPU配额、总RSS／pinned／device内存上界。
正常检查没有总墙钟截断；单操作无完成60秒、异常确认30秒、自有actor退出30秒分别计时。
无benchmark warm-up，不作性能比较。

本轮 `2026-10-14T07:26:44.382326+00:00` 开始、`2026-10-14T07:27:31.694777+00:00` 完成，controller执行
47.312秒；Ray unittest 36.762秒、数据适配 unittest
3.078秒分别保留，不能相加为方法JCT。两条预期的prepare错误通知原样保留于脱敏日志。
12个fixture actor的PID／birth退出核对通过；27个自有子进程均退出，没有强制清理。
`2026-10-14T07:32:32.055281+00:00` 独立复查27个出生身份及controller均未存活、自有lease已释放；
没有移除他人的lease。模型／GPU／HTTP调用均0。

fence仅适用于独占同一pool、同提交者的同步串行actor、restart／task retry均0。
CPU模拟model的synchronize不证明CUDA完成；弱引用记录被回收也不证明actor退出。
真实CLIP质量／性能、device完成／退出、完整CLI／pool factory／sampler进程树停止、
全物理内存与副本上界、PG／Daft source接入仍未由这些检查覆盖。B／C没有默认启用或新增收益证据。

完整私有原件保留：320成员、599251字节，SHA-256
`5e0d58c93808108aba6871ca10e806684a1eb97b44795d2c0506577e8a92ea52`；公开19成员副本逐项映射原始与脱敏摘要。
最初只读探测包装器返回1、未保留stderr，原因未确认，记录该限制；当时没有上传或执行SDK。
随后只读确认外部owner持lease时未启动；其释放后由本controller原子取得共同lease，未抢占窗口。

<a id="literal-gpu-handle-guard"></a>

## 借用池的重复 GPU handle 入口校验

在已发布 `58462a0d` 之后单独补 HSE 入口：同一 Python handle 对象重复出现在
`gpu_actors` 中时，在 source 读取及 actor RPC 前抛出 `ValueError`。不静默去重或改变额度，
broker、标准 pool builder、默认 direct_dependency 和本节之前的固定测量源码保持。
[两项专属回归](../../../../code/tests/modalities/image/test_hse_pool_resource_identity.py)修前一项因读取
source 而失败，另一项正常；修后连同旧 HSE 生命周期、部分确认及 broker 回归共31项通过、0跳过。
两个不同替身 actor 的四行输出仍恰好一次。这里只核对重复 Python 对象；反序列化或包装后仍指向
同一 Ray actor 的不同 handle、实际设备身份与共享物理 GPU 未识别。新增服务器／模型／GPU／HTTP
执行均0，新 guard没有借用上方 `45da0a54` 的实际 SDK 资格。
