# 暂停 PG 与退役 HTTP 实验的准确来源

本目录保存所列旧 PG／HTTP 实验的准确来源，不表示现用 PG provider 或数据库接口已退役。
原实验仍可按完整源码恢复，**暂停并非永久放弃**。
旧 Movie HTTP 与旧固定矩阵另见[恢复记录](../native_baselines/README.md#movie-http-retirement)。

[MANIFEST.json](MANIFEST.json)逐项记录退出当前入口的源码／测试／部署配置路径、字节数和 SHA256。
源树为 [`7bf1d92baf2611925c608cfe76b4a766855bd77e`](https://github.com/3444374/ai-operator-execution-optimization/tree/7bf1d92baf2611925c608cfe76b4a766855bd77e)，
包含退役前的 PG C／SQL、build、测试、Python 接线、旧 HTTP 对照、相连启动脚本及部署源。
在新的隔离 checkout 中选择该精确提交即可恢复整套依赖；不要把存档目录加入当前 PYTHONPATH，
不要只取单文件并与新 owner 或协议混用。恢复源码不授权启动数据库、服务、模型或恢复旧预算。

后续清理也移除残留的 PG C 编译／静态检查及其夹具；原件与上述完整来源树一致，
恢复 PG 时使用该树中的配套测试。仍有用途的 Python plan、generation 和 wire 合约测试继续维护。

原生 baseline 依赖审查又清除了两份只针对已退役接口的残留测试；它们在五份历史投影的
3010 个绑定中均无引用，字节与上述 `7bf1d92b` 完整来源树相同。恢复时从该树取配套实现与测试，
不把旧 HTTP 适配或 project_static 调度重新加入当前 baseline：

| 原路径 | SHA256 |
|---|---|
| `code/tests/baselines/text/test_official_runtime_adapters.py` | `fbfbea403db419bd1bcb20b8c6d53c3deb40ed35aa466b35004e45d2f64881e9` |
| `code/tests/baselines/text/test_project_static.py` | `b39d62860e9f5bd43e6d079d8cf72d5cb89b4b48d9c6557fb6d484f0c268fc85` |

| 处理 | 内容 |
|---|---|
| 退出当前执行入口 | 旧 Movie HTTP、固定五后端矩阵、prepared-call HTTP 图、PG／HTTP 查询／画像及旧部署配置 |
| 暂停 PG | 扩展、SQL、部署源、实验接线、旧测试均以精确 Git 树保存，不触碰数据库数据／容器／卷 |
| 保留当前共享能力 | 私有文件／脱敏、LOTUS 方法、CLIP workers、Core／IPC 和结果责任 |
| 保留研究 | SAOR 调度算法、离线分析、方案及历史结果；依赖旧 HTTP／PG 命令的启动胶水从原完整 Git 树恢复 |
| 恢复原生 LOTUS 对照 | 新普通 Movie session 使用原 LOTUS LM + 本地模型 IPC，真实模型仍未验证 |

五份具名历史投影继续使用原 ID、原 expected SHA 和原来源，只为受影响文件增加精确旧字节 override。
历史成绩、失败、unknown、成本和私有原件不改写。此清理是工程职责和入口收敛，不是查询提速或质量修复。


<a id="retired-http-observation"></a>
## HTTP 观测代理及专属回环测试

`code/src/observability/request_gateway.py` 和 `code/tests/observability/test_request_gateway.py` 已退出维护树。
其原用途包括五臂观测、旧 PG supplier／persistent adapter 及 Animals 实验，现用 src／scripts／研究包没有代理实现消费者。
两份原件分别为 38,221 和 49,736 字节，与本索引的准确 7bf1 源树逐字节相同，已在 MANIFEST 追加大小与 SHA256；
恢复旧组件时使用该完整树中的配套调用方和测试，不建立现行兼容入口。

trace 摘要及其纯 CPU 用例继续维护；HTTPX／native observer、request identity 和 Unix execution-provider 接口未移除。
五份原生历史投影继续使用其既有旧来源，没有覆盖为当前文件或改写实验结果。
[代码与检查](../../results/diagnostics/records_runtime_refactor_20261104/README.md#retired-http-observation)
说明恢复、当前调用和未覆盖的模型场景。


<a id="retired-choice-http-observers"></a>
## 旧 Choice HTTP 观测与专属测试（2026-11-06）

本批六份删除文件来自准确的
[`5539a19ff85f9dd96524f10c8c1197b399a04fb8`完整树](https://github.com/3444374/ai-operator-execution-optimization/tree/5539a19ff85f9dd96524f10c8c1197b399a04fb8)，
不属于上方 `7bf1d92b` 的来源表，不把两个版本混写。
旧 Choice CLI、`SessionObserver`／`ObservedAdapter` 和 `ExpectedRequests` 只有彼此及专属测试消费；
其标准库 POST／异步 POST 包装也随旧 CLI 退役。恢复旧机制应选择完整5539树及配套测试，
不要把旧模块或全局 HTTP 包装加入当前 PYTHONPATH，也不要据恢复源码重用原预算或运行许可。

| 原路径 | 字节 | SHA256 |
|---|---:|---|
| `code/src/experiments/choice_gateway_observer.py` | 15153 | `b7a3a99bb758ec31433b148058aa053b6897de7ed4fd2e44927fcc15c78ed736` |
| `code/src/experiments/gateway_observer.py` | 4450 | `98ea566daeddad686d3c44d9cc4c00e214485ec178e71e6bda0beb79db1199e2` |
| `code/src/experiments/expected_requests.py` | 1929 | `b527a80abe783f0891b38a1c0c820e6323c6ed76ebf90454b438a13429203fc8` |
| `code/tests/experiments/test_choice_http_observer.py` | 5816 | `7c2f696ff8d4e03cedd15a964ec2378f7cdb1392b515e81337f676ff43aee6e1` |
| `code/tests/experiments/test_gateway_session_isolation.py` | 3882 | `8d5d7b01f095616fe87a769dc971c74838eb6f9b39315b0b6d396615a27b90f8` |
| `code/tests/experiments/test_expected_requests.py` | 6151 | `aeb0532d63f04d3531088dfa19c1760ceac7de5c5b36671ceed3f4683cbd4cd0` |

仍被 map probe 使用的请求守卫已原函数体迁至 `src.experiments.attempt_ledger.guard_remote_request`；
两个现用 probe 的来源列表直接记录 `attempt_ledger.py`。原 Choice 模块没有兼容转发壳。
`AttemptLedger`、Cell／shared／mapped预算、request identity、原生HTTPX观察和PG provider／server／异步传输继续维护，
不是本批删除对象。服务器仅核对当前helper、e7e、0657共65份py/json/sh与17份顶层Python无旧引用，
这个范围不代表全部仓外材料；历史完整源码副本不按活跃launcher处理。

五份原生投影中只有 `gateway_observer.py` 需要新增当前来源，保存方式见
[本次历史来源](../native_baselines/README.md#retired-choice-observer-sources)。
既有旧来源、原3010条expected、投影ID和版本保持；原结果、失败和性能材料没有复制或改写。


<a id="retired-fixed-http-launchers"></a>
## 旧 Fixed HTTP 隔离诊断与研究服务入口

本批五份文件来自准确的
[`f526e6bd4701b23d9d47e44d22590593debabb9c` 完整树](https://github.com/3444374/ai-operator-execution-optimization/tree/f526e6bd4701b23d9d47e44d22590593debabb9c)，
与上方7bf／5539来源分开。gateway isolation只为旧固定HTTP网关组织隔离／取消诊断；
研究服务CLI只装配同一HTTP网关，其图像分支没有独立执行能力。两份入口和专属测试直接退出当前树。
恢复旧诊断、分析和结果复算时使用完整f526及配套依赖，不把旧服务入口拼接到当前单查询路径。
原结果、失败、unknown和负结果继续保留，不因源码退役改写。

| 原路径 | 字节 | SHA256 |
|---|---:|---|
| `code/src/experiments/gateway_isolation_probe.py` | 33302 | `c09490018aeec96b02fa9c88358f238e129337306d3698b3e104a15f20bf9a80` |
| `code/scripts/profiling/gateway_isolation_probe.py` | 157 | `a0cd8c44f3c2dc622e0d9eed9102aacddb1dab1b06fcca48a82085e572c833d4` |
| `code/tests/experiments/test_gateway_isolation_probe.py` | 3818 | `fe2ee078c9779cf65c3d4fdc865ffea16377306adea055e1bcac2fd9ed71ce02` |
| `code/src/research/multitask/semloom/gateway.py` | 3204 | `406f45e32ecf680eee43e7ad671197d76b5275c1d380a73ada919323155efa91` |
| `code/tests/research/multitask/test_shared_query_gateway.py` | 6280 | `e20783e3107b5565557df2c1a3a0ba2d162f66da806993488d960b613ca7cdf4` |

共享的 `SyntheticTable`／`WORK`、`IncrementalExecution`、Core任务／资源／租约／取消、
typed image provider、SAOR策略和共享额度函数未删除；普通records仍使用本地IPC／原生模型。
这批只删除旧入口，fixed HTTP工厂、服务选项和共用网关装配留待后续清理，不表示整条旧路径已清空。
五份历史投影的10条相关来源继续保持原字节；只有同名CLI的五条来源需要新增准确f526压缩原件，
其余既有来源不变，见[来源读取说明](../native_baselines/README.md#fixed-http-launcher-source)。


### 同一旧 HTTP 服务选择的退役

随后从provider服务命令移除 `--fixed-model-config`、`--incremental-map`、
`--map-transport-config` 和 `--organization-config`，以及只为它们服务的工厂分派和旧路由专测。
这些参数不再进入配置解析，也不静默改选Golden；原默认Golden夹具与typed image仍使用既有UDS入口。
需要重现这些旧CLI路由时，从上方完整f526树恢复 `server.py`、原测试和依赖，不拼接新服务源码。
本批仍未删除全部内部HTTP运输／Map装配；它们与共享Core、image、opaque task/session的后续清理分别处理。
五条受影响历史读取继续使用已有原件，没有新增归档或改写expected／投影身份。
