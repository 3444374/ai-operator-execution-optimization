# 暂停 PG 与退役 HTTP 实验的准确来源

本目录保存所列旧 PG／HTTP 实验的准确来源，PG 相关工作当前不作为主线或验收前提。
原实验仍可按完整源码恢复，是否恢复以后另行决定。
旧 Movie HTTP 与旧固定矩阵另见[恢复记录](../native_baselines/README.md#movie-http-retirement)。

[MANIFEST.json](MANIFEST.json)逐项记录退出当前入口的源码／测试／部署配置路径、字节数和 SHA256。
未注明单项来源时，源树为 [`7bf1d92baf2611925c608cfe76b4a766855bd77e`](https://github.com/3444374/ai-operator-execution-optimization/tree/7bf1d92baf2611925c608cfe76b4a766855bd77e)，
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

<a id="retained-m1-config"></a>
## 原 PG campaign 示例配方

[m1_screening.example.json](configs/m1_screening.example.json)从现用配置目录移至本目录，原字节保持。
它由已归档的`postgresql/m1_campaign.py`读取，包含旧PG分组、固定模型配置和查询期限，
不作为普通records配置。文件大小、摘要与原路径记录在[清单](MANIFEST.json)的`retained_path`项中，
其字节与`7bf1d92b`完整来源树一致；恢复时应与该树中的campaign、模型适配和配套测试一起使用。
当前CLI不加载本目录，PG及原研究记录继续保留。


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


<a id="retired-http-map-owner"></a>
## 旧 HTTP Map owner 与注册连接退役

这批旧实现及专测来自准确
[`e5e65fdb6db14f5f422ed4e3983e2206fbc75dfd` 完整树](https://github.com/3444374/ai-operator-execution-optimization/tree/e5e65fdb6db14f5f422ed4e3983e2206fbc75dfd)，
删除前逐文件核对与该公开来源相同。恢复旧Map／注册协议与其研究结果时使用完整树，
不把旧模块或测试夹具接回当前图像／records入口。原始结果、失败与负结果没有改写。

| 原路径 | 字节 | SHA256 |
|---|---:|---|
| `code/src/execution_provider/multiplexed_gateway.py` | 25964 | `55d498c179b9b8ee51577ba091f26ed65d5707518d5e86df30f24cd59e7620af` |
| `code/src/execution_provider/connection_capacity.py` | 2211 | `0ed6188a5b6f2ae1aa38b78ca01ac34bd5ebcca5e6f125def33874649615bbb9` |
| `code/src/execution_provider/query_registration.py` | 5375 | `f90a05125b781bacc579d844136ba61ace7f22a7fca2aa7c9c900f6e7b49ce28` |
| `code/tests/execution_provider/test_multisession_gateway.py` | 14425 | `865e67d9a3fdf4de426f82dcd59dc49bd5ebbc8065e4a6cf21ece722ea1a83ae` |
| `code/tests/execution_provider/test_query_registration.py` | 8498 | `8812f8e1d28d81840e58f306ff35ef0626d8141c45ff8dd6367bf53df4cdbffc` |
| `code/tests/execution_provider/test_query_lifecycle.py` | 5933 | `ccae265f058803816df21f3a2cbe473726a33e6d7a7b65fb5ba40a9aacbfa42f` |
| `code/tests/execution_provider/test_incremental_session.py` | 8866 | `e887493a2cc75f40b4edb233df25e8256cebddea04be2bf0c4b2fd817a8ab883` |
| `code/tests/execution_provider/test_incremental_window_one.py` | 5152 | `050694930c4f4141693f5642f4143413318e97333d7e93d2da7d8b84ba053312` |
| `code/tests/execution_provider/test_map_organization.py` | 6578 | `da818b1f762869df5850cc52965bfe099a62d86ffc19513037d46986bcb243c5` |
| `code/tests/execution_provider/test_organized_response_capacity.py` | 4457 | `288a83af5d74e356decbf3c7ceaf726f7c67db9b61db4bb8ed33078eb03cade6` |

ImageGateway现在直接持有现用v7图像连接；帧处理在image_connection，连接消息与分类计数在既有gateway_runtime。
图像验证先于Job登记，因此旧query_open／stream_join不能进入图像登记分支。
直接调用旧私有_register时的错误不代表当前图像入口存在该缺陷；没有为此增加多查询协议。
Golden、当前图像v7与通用UDS帧读写、Core／Job／任务／租约／取消保持。
旧专测中唯一通用的dispatch关闭检查迁入现用test_map_gateway，方法内容相同；图像真实UDS测试保留并改为当前所属导入。
本机只执行纯对象图像与既有软件fixture，真实图像UDS／PG／Ray仍待单独核对。

五份具名投影的35条相关来源读取前后相同。六份原来直接读取维护文件的源码各增加五条准确旧源映射，
原3010条expected、投影身份、来源版本与已有映射不变；本批没有重跑完整3010构造。
HTTP工厂／传输和混合IncrementalExecution仍需后续清理，不把这批写成所有HTTP内容已清空。


<a id="retired-fixed-http-adapters"></a>
## 固定HTTP工厂、传输和旧Map观察probe

准确来源仍为上节的公开
[`e5e65fdb6db14f5f422ed4e3983e2206fbc75dfd` 完整树](https://github.com/3444374/ai-operator-execution-optimization/tree/e5e65fdb6db14f5f422ed4e3983e2206fbc75dfd)。
以下源码／专测不再留兼容壳。恢复旧HTTP实验时使用该树中的整套依赖及配套测试，不把旧模块拼回当前入口。
SyntheticTable／WORK的现用消费者也全部随旧装配退役，没有为它们新造fixture模块。
两个原诊断raw脚本及其原结果、失败、负结果没有编辑；旧复算应在上述完整来源中进行。

| 原路径 | 字节 | SHA256 |
|---|---:|---|
| `code/src/execution_provider/adapters/model_config.py` | 4549 | `78d6c4406b5e0788fe64fa4a3d7845b9b73200c8547e695c288ea56c7096ad36` |
| `code/src/execution_provider/adapters/openai_compatible_fixed.py` | 13626 | `66a611592c2b153c1ebd09e2c4006568a70c8fa09982b6d304dd3d9270ff7792` |
| `code/src/execution_provider/adapters/async_fixed_model.py` | 6200 | `3e86526be43decdbc96811442c769efecc9b697255e820d17bdf045bb83a8e4b` |
| `code/src/execution_provider/adapters/full_response.py` | 3991 | `ef0bdfeb99bd5aab1c1b4002ee1f61469ee270966437d71eac66b2ddbd99ebde` |
| `code/src/execution_provider/adapters/ray_map_transport.py` | 30562 | `0cfb5a0262d2857c6705d51147db7a93bd4af2cabeca967089466d1b08f33dff` |
| `code/src/execution_provider/adapters/map_preparation.py` | 16065 | `890af84238be5d6e0a4829286a3c50c4d10f4708c20f9e76c6a557b85537c03e` |
| `code/src/execution_provider/adapters/map_organization.py` | 8182 | `8279088919fcc53ef210781efe27fa8ba8a178c9b689a3a667bc87ddcaaea1ac` |
| `code/src/execution_provider/adapters/incremental_session.py` | 6809 | `0b0baea54597b414abc6b46226cc7e08fd2e4af461f5960817982c3c92f6aeb9` |
| `code/src/execution_provider/adapters/completion_response.py` | 2021 | `b6da1a0f59d8833a60f39a5d787af460d92b622273ce3c7de0d95148e9455cb0` |
| `code/src/experiments/map_observation_probe.py` | 27178 | `d9633f79ac297c84eafdf542abe089a52af5774d6cd978914655346a73357586` |
| `code/scripts/profiling/map_observation_probe.py` | 160 | `bf6872d7ab0f00f30d27d5b0c61a9fc1adc22057a5c9b29801c4a44dab774c03` |
| `code/src/execution_provider/wire/v6.py` | 1578 | `6b6396d8380c5158e11d4761e895ed8c2c8666ba43cfe7bfb1692e6823d0520f` |
| `code/tests/execution_provider/test_ray_map_transport.py` | 11339 | `851f0d2f62405e6371eacd03c100dd4014243daf586667a8cd6c29914135de13` |
| `code/tests/execution_provider/test_ray_map_worker_pool.py` | 12712 | `522d6ce89ff791f987fa71a9d3d767c4cc1c3060ed30bfe2b473a48ade2e2c65` |
| `code/tests/execution_provider/test_ray_map_payload_failure.py` | 10775 | `ee7e874938722f99907c24073eaae17a89e3d51bfd1bb1cc3767c24599278dfb` |
| `code/tests/execution_provider/test_ray_map_errors.py` | 7546 | `f6543cd6174a3d455380e3693e9874593a034adbb93453dac574f04b38195771` |
| `code/tests/execution_provider/test_ray_map_timing.py` | 6391 | `8e1ffce00ad22166e73be940d2176bdcc4f6a03c364ae5964009652de5e213a1` |
| `code/tests/execution_provider/test_ray_map_failure_integration.py` | 12611 | `12f0ec8f679d604e56f28e51a6a7f3452185a6cc049c2254e98da8ade228d3de` |
| `code/tests/execution_provider/test_native_ray_integration.py` | 7821 | `e2e2020cfb5b280697c4d9dfba986c52095808775b53e14cd9c5395d2e3ca712` |
| `code/tests/execution_provider/test_map_preparation.py` | 35577 | `446f2a6dd31bc79b16e1101b66ad9194ab3c28df748d4c10c37f8176d62ead61` |
| `code/tests/execution_provider/test_full_response.py` | 6068 | `1050c288afecfd0f93301519d1030c7a7b92e7dbfb6b0d1da20e95b432804713` |
| `code/tests/execution_provider/test_http_configuration.py` | 11374 | `dd9b28981ec0a9aaf8f70896eeec9b6d5f564a6ddc9d68e6bc779ad743f8860f` |
| `code/tests/execution_provider/test_httpx_request_cookies.py` | 8855 | `6949068bf14dcb69317eb16d9db6e42c3f8dd8afa57d788dbba89caae0df0207` |
| `code/tests/execution_provider/test_async_transport.py` | 5200 | `96b512bf3d80972b9c0fad5b60f4624c5a0bdd9a6fd4203cb143e4c78ff0af42` |
| `code/tests/execution_provider/test_async_request_guard.py` | 4071 | `7b21fc39db9eabf228dce8626bbcfb1712ba867b2ba0e92ec05a3a985a325e4f` |
| `code/tests/execution_provider/test_native_request_capacity.py` | 18333 | `4c2e7a019258a4aaeae91a832e6772743ed125e46348b41dc8d88a624d00dbe7` |
| `code/tests/postgres/test_semloom_fixed_model_adapter.py` | 22806 | `bcb5e5780e1ffd488952845b0c09a4406f35adb8efd5fd93fd5e0f09b47a60c3` |
| `code/tests/experiments/test_map_observation_probe.py` | 11036 | `51451e12938cff1b11b88a64b8f68152fce5f3a71962112a096da0652a2372a2` |
| `code/tests/experiments/test_map_preparation_probe.py` | 3777 | `7ca8041a33d9fe79bc93d987d237e5dd315c8f9305910c27a6f5129460b72cba` |
| `code/tests/experiments/test_ray_return_wait_probe.py` | 3892 | `ab4680ebce72d212d1bbfd03e4c6dd54d5db4b3474d897fada057df7dca96bc4` |
| `code/tests/execution_provider/transport_accounting.py` | 1994 | `13108e27b3756a40001f901e1e53cd8fb9a22659544a869bb3c1080c72752019` |

IncrementalExecution原有的Job分配／flow分区、NativeQueryJob和NativeTaskSession保留；
其现用Core测试直接传递opaque bytes，不再借HTTP envelope作为无关夹具。
空release与等待推进另由现有Core／MethodDriver行为测试覆盖；不为删除的HTTP工厂保留运输线程专测。
原v3／v4／v5 Golden和v7 Image保持；只删除无现用入口的Fixed／v6身份与分派，不增加新协议或多查询功能。
纯向量、通用payload materializer、StageBroker、任务／额度／租约／取消和独立记账继续维护。
原生LOTUS VLM/MMQA、Kalypso与Ray官方HTTP连接及native HTTPX观察保持，不反向接入SemLoom执行器。

删除前49个所选CPU方法中48成功，旧别名检查因scheduler已不再导出SharedCreditPolicy而报错；原记录保留。
删除该过时要求并迁移真实消费者后，51项纯CPU全部成功，额外两项为现有空release／等待行为；
没有运行真实HTTP／UDS／Ray／PG／模型或本机SDK收集。修改后的selected-profile socket用例仍待服务器核对。
五份历史投影的85条相关来源保持，九份新增旧源及45条读取位置见
[来源读取记录](../native_baselines/README.md#fixed-http-adapter-sources)；没有重跑完整3010构造。
离线校准读取器仍保留旧profile身份，用于解释历史产物，不恢复HTTP运行入口。


<a id="retired-unused-aiohttp-observer"></a>
## 无现用调用的aiohttp观察包装

native_http_observer中的NativeSessionFactory、ObservedSession、ObservedResponse三类只由三项专测调用，
当前VLM／MMQA消费者只用同文件的observe_native_httpx。此次只删除这三类、专属依赖和三项专测；
HTTPX观察函数、MappedUnit／预算和余下四项行为保持。没有更换或包装第三方原生连接。

删除前两份完整文件与准确公开
[e5e65fdb来源](https://github.com/3444374/ai-operator-execution-optimization/tree/e5e65fdb6db14f5f422ed4e3983e2206fbc75dfd)相同，
旧实验恢复使用完整树，不向维护模块加回旧导出。

| 修改前完整路径 | 字节 | SHA256 |
|---|---:|---|
| `code/src/experiments/native_http_observer.py` | 5976 | `2a95cd57e93019b5c85c55a51e2ab8e1a3970d6f7b6c6ea1e5c4773717328a3a` |
| `code/tests/experiments/test_observer_mapped_budget.py` | 7798 | `1cddae575dd5b9417e0683132b6f55da4d292a978f74bbc0e4f30babc4c5414d` |

五份投影共十条相关来源读取前后一致：旧实现原已读存档，未改的HTTPX测试仍读原来源；
因此不新增压缩副本或MANIFEST映射，也没有重跑3010完整构造。
保留的七项纯CPU／HTTPX Mock在修改前后均通过，没有实际网络、模型、Ray或UDS运行。

<a id="pg-image-isolation"></a>
## PG图像读取与实验的独立归档

本批18个文件来自完整的[94db来源树](https://github.com/3444374/ai-operator-execution-optimization/tree/94db4a3b914b0841ced21f02fe7c8216ab031323)，
单项`source_commit`优先于本清单的默认来源，不能把这批与7bf1源码混用。原件复用
[独立压缩目录](../native_baselines/historical_sources/pg_image_94db4a3b)，每项记录原字节数、原摘要及gzip摘要。
当前Python搜索路径不包含该目录；恢复旧实验须使用完整94db树及其配套测试，不启动数据库或模型。

| 移出活动目录的部分 | 原职责 |
|---|---|
| `modalities/image/source.py`、`baselines/image/frameworks/ray_data.py` | 固定PG表读取、元数据与PG来源的原生Ray图 |
| `experiments/image_clip`、`experiments/image_multijob` | 依赖PG输入的六臂与多Job实验装配 |
| `run_image_clip_e2e.py`、`run_image_clip_matrix.py`、两个图像多Job CLI | 上述PG实验的公开运行入口 |
| `build_image_multijob_manifest.py`及两个专测文件 | PG切片准备与对应驱动检查 |

共享`ImageSourceConfig`及分片函数逐字迁入[现有中性图像类型](../../../code/src/modalities/image/contracts.py)，
[文件图像源](../../../code/src/modalities/image/file_source.py)直接调用它们。公共CLIP workers、HSE、通用Daft图、
LOTUS文件检索及其他非PG图像入口保留。混合测试中移出已归档实现的九个方法及两条旧CLI名称断言；仍在使用的COCO导入器主键测试迁入数据测试目录。
原定义仍由完整94db树恢复，公共行为检查保持。

这批之后，COCO导入与三个PG画像工具按[图像工具原件](#pg-image-data-tools)归档；文本SQL导入及旧SQuAD/PG诊断按[文本工具原件](#pg-text-data-tools)归档。

<a id="pg-image-data-tools"></a>
## COCO数据库导入与PG画像工具

本批七个文件来自完整的[e5c36c9来源树](https://github.com/3444374/ai-operator-execution-optimization/tree/e5c36c9be6a2f50620dc7b8cf161978346c638a5)。
它们只服务PG图像输入，未被当前JPEG清单、LOTUS文件检索或普通records调用。逐项原件位于
[独立来源目录](sources/pg_image_tools_e5c36c9b)，原路径、字节数、原摘要及压缩摘要记录在本目录MANIFEST。

| 移出的原路径 | 原职责 |
|---|---|
| `code/scripts/data/import_coco_images.py` | ZIP或目录图像写入PG，事务与主键检查 |
| `deploy/autodl/image_documents_workload_key.sql` | PG图像主键迁移 |
| `code/scripts/profiling/profile_image_clip_bottleneck.py` | PG读取、CLIP阶段测量及共用取数函数 |
| `code/scripts/profiling/profile_clip_preproc_stages.py`、`profile_image_clip_preprocess_variants.py` | 复用上述PG取数的画像入口 |
| `code/tests/data/test_import_coco_images.py`、`code/tests/modalities/image/test_image_clip_preprocess_variants.py` | 上述工具的资源、主键、数值与入口检查 |

共1370行Python及55行SQL移至可恢复原件；没有删除原始图像、ZIP或研究结果。
恢复使用完整e5c36c9树及其依赖，不能将压缩代码目录加入当前Python搜索路径。
当前文件图像准备方式见[数据入口](../../../data/README.md#image-workload-clip-current-multimodal-workload)；公共COCO文件选择、CLIP与LOTUS入口保持。

<a id="pg-text-data-tools"></a>
## PG文本数据准备与旧诊断

本批十二文件来自完整的[c2c2d35来源树](https://github.com/3444374/ai-operator-execution-optimization/tree/c2c2d3513915a2a7b1a8ec10935a9e38d4af5e0a)，共4167行Python移出活动目录。
原件保存在[配套来源目录](../native_baselines/historical_sources/pg_text_tools_c2c2d351)；逐项原路径、字节数与原／压缩摘要见本目录MANIFEST。
恢复旧工具使用完整来源树及其依赖，当前任务不加载这些压缩文件。

| 移出的原路径 | 原职责 |
|---|---|
| `code/scripts/data/import_ai_complete_workload.py`、`import_bounded_output_workload.py`、`import_squad_workload.py` | PG文本表准备、包装与写入 |
| `code/scripts/baselines/duckdb_ai_sentence_count_gate.py`、`squad_capability_gate.py`、`squad_truncation_diagnostic.py` | 从PG取数后运行旧对照／诊断 |
| `code/src/baselines/common/squad_identity.py` | 仅供上述SQuAD PG诊断使用的摘要及服务归属函数 |
| `code/tests/data/test_import_ai_complete_workload.py`、`test_import_squad_workload.py` | 两个导入器的专测 |
| `code/tests/baselines/test_duckdb_ai_sentence_count_gate.py`、`test_squad_capability_gate.py`、`test_squad_truncation_diagnostic.py` | 旧PG取数诊断的专测 |

现行`map_workload_tools`、SQuAD／ShareGPT文件输入、SQuAD质量与容量函数，以及原生DuckDB adapter保持。
`test_squad_pg_map_pilot.py`虽沿用旧名称，九项方法实际检查文件任务、消息、分组、身份及EM／F1评价，不访问PG，故保留；既有输入身份也不改名。
三份直接来源新增十五条准确读取映射，其他来源与投影身份保持，详见[读取记录](../native_baselines/README.md#pg-text-data-tools-source)。
旧本机数据库状态只从[原数据说明](https://github.com/3444374/ai-operator-execution-optimization/blob/c2c2d3513915a2a7b1a8ec10935a9e38d4af5e0a/data/README.md#local-import)回查；原始语料、派生输入和研究结果未移动或删除。
