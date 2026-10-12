# LOTUS 原生臂观测准备核查

本次基于发布版 `1e96935610069d17430c6fb75fe37a75659aa84e` 的独立本地工作树。
证据是源码核查、确定性替身回归及局部观测成本；真实模型、GPU、SSH 和依赖安装均为 0。
仓库没有 `.agents/skills`；本机该目录只有本任务无关的飞书技能，未调用。

## 确认的重复准备与计时

发布版 `_LotusBatchObservation.__call__` 在原 LOTUS 臂先逐条 `prepare_call` 构造预期 HTTP 值，
然后才调用原 `_process_uncached_messages`；预期 payload 没有交给原执行器复用。
固定版本 [LOTUS LM 源码](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/models/lm.py#L237)
仍将整批消息交给 LiteLLM `batch_completion`，保留原 `max_batch_size` 请求池，自行准备实际出站请求。
替换臂的 `on_prepared` 观察的是执行器已准备的实际 payload。

[`record_prepared_execution`](../../../../code/src/experiments/postgresql/ready_query_recording.py)在原 API 提交前开始计时，
以统一消费者 EOF／异常结束。重复准备在 `execute()` 中发生，同时计入 `ready_query_seconds` 和 `full_query_seconds`。
`lotus_batch_return` 继续在完整 SDK 列表返回后、观测序列化及原 LM 统计前记录，不代表服务端推理结束。

## 模式与证据语义

默认 `lotus_observation_mode='qualification'` 保留原预期构造和完整请求值多重集合核对。
显式 `performance` 只在原请求池前省去预期构造；同批就绪时间、独立行／阶段／调用身份、原参数对象、
批次调用、完整 SDK 返回、解析、质量评价和两 Map 原生阶段等待保持。
替换臂的两种模式都通过 `on_prepared` 观察实际 payload；增量方法继续观察自己的请求。

原生性能模式的 `request_values_sha256=null`，`request_values_scope` 说明原 SDK 编码前未构造完整值；
摘要 `method_http_values_audit.status=unavailable`。`compare_methods` 拒绝把两个缺少值的样本判成内容相等。
共同代理继续记录 `protocols.jsonl`、`upstream-response-bodies.jsonl` 中的真实请求／完整响应，
核对正文未改、模型身份、非流式语义、实际 POST 数及总额度；发送前持久扣记和关闭后停止消费保持。
查询与失败摘要记录所选模式，LOTUS identity 另记值来源与计时说明，常驻启动及查询元数据也记录模式。

单查询、常驻 CLI 使用 `--lotus-observation-mode performance`；`PersistentAdapterGroup` 接受同名 Python 参数。
SemBench 私有 JSON 可增加 `"lotus_observation_mode": "performance"`，省略仍为资格模式，预检和各单元保存选项。
阶段名称不自动切换观测。实际性能运行应先完成同配置资格检查；未构造预期值的性能样本不能用于完整值对拍。
没有更改 prompt、cache、LIMIT／COUNT、MovieQuery 行容量、IMLane 或共享 `native_calls.py`。
查询内观测仍计入完整实际耗时，未扣除任何估算预处理时间。

## 回归与局部成本

[`regression.json`](regression.json)保存源码 SHA-256、逐项结果、脱敏输出及跳过原因：
**82 项中 57 通过、25 跳过、0 失败、0 错误**，跳过未计为通过。
新增 11 项覆盖默认兼容、原参数对象、重复输入的出现身份、两阶段结果／依赖、准备量、代理正文／POST 数、
准确输出评价、总额度不足零发送、额外 POST 发送前拒绝、账本关闭、原异常对象／取消／超时／异常值、
缺失响应、模式验证和常驻元数据。旧观测测试 patch 更新为现有 `parse_lotus_response`，生产 parser 未改。

查询级检查使用 JSON-only 供应商及响应替身，经真实 localhost 代理、SQLite 额度和原查询记录器执行，
**累计 16 次替身 POST，模型请求为 0**。第一次沙箱监听拒绝、旧组合三项进程采样拒绝及一项陈旧测试符号错误，
均在 `prior_attempts` 保留；允许的本地替身运行通过自动审查后执行。

[`probe.py`](probe.py)使用 2048 字符消息、JSON-only 准备及相同原生编码／返回替身，事件与响应写入内存，HTTP 为 0。
每规模、每模式 1 次 warmup 后交错测量 7 次；[`observation-substitute.json`](observation-substitute.json)保存全部原值和构造次数。

| 行数 | 资格模式中位数 | 性能模式中位数 | 两个中位数之差 |
|---:|---:|---:|---:|
| 128 | 3.095375 ms | 1.081959 ms | 2.013416 ms |
| 2000 | 48.176708 ms | 16.700666 ms | 31.476042 ms |

原生编码替身每次均处理 N 条；额外预期准备由 N 降为 0，完整结果和出现记录仍各 N 条。
每个样本直接测量完整区间，差值只描述两模式局部扰动，不用于扣减查询时间。
**这些数值不是实际 LOTUS／LiteLLM 准备开销、模型性能、厂商调度或服务器收益，不能校正历史模型测量。**

复核入口（输出要求新路径；回归需要 localhost 监听和本机进程采样权限）：

```bash
PYTHONPATH=code python3 experiments/results/diagnostics/lotus_observation_modes_20261012/verify.py --output /tmp/lotus-observation-regression-new.json
PYTHONPATH=code python3 experiments/results/diagnostics/lotus_observation_modes_20261012/probe.py --output /tmp/lotus-observation-substitute-new.json
```

## 整合位置与待验证

下列行号以固定发布版为准，全部采用局部 patch，未覆盖整个文件：

- `supplier_adapter_query.py`：61 后模式定义；109–110 未构造值；132–155 批观测；227、280、294 后为
  `_lotus_rows` 参数／元数据；464–466、504 后、614、720–721 为入口参数、摘要、传递及完整值核对。
  Movie 类型／数据／容量检查和原评价逻辑保持。
- `sembench_query_cli.py`：20 后、39–42、131 后、160、167 后仅增加模式配置／验证／记录。
  两文件与 task-9 的 Movie 预检可能相邻，应手工保留其容量与输入形状改动。
- `persistent_native_adapter_query.py`、`native_adapter_query.py` 只传递模式及元数据；
  `native_adapter_metrics.py` 只拒绝缺少值的内容相等声明。状态／README／台账／日志为局部新增记录。

待验证：固定 LOTUS 1.2.4／LiteLLM 1.95.0／OpenAI 2.50.0 两模式差分；实际 Daft／Ray Data／作者 IMLane；
官方 Movie Q1／Q2／Q3 evaluator 对拍；指定服务器、真实模型、GPU、正式规模及饱和性能。
实际库检查可沿用 `test_supplier_adapter_libraries`，通过新增可选 `SEMLOOM_LOTUS_OBSERVATION_MODE=performance` 传递模式；
默认资格模式保持。未改动或验证 task-9／task-10 的扩容和客户端寿命候选。

<a id="publisher-integration"></a>
## 已发布容量上的整合回归

发布者以已推送并核验的38e82a9a为基线，整合固定候选84132349；两份重叠源码只按局部函数接入。
`supplier_adapter_query.py`和`sembench_query_cli.py`逐字匹配独立三方合并结果，其余13个容量源／测试文件保持已发布摘要。
MovieQuery、CLI和公共执行器继续读取同一显式行容量，旧配置128、显式2000与桥未知责任的跨对象保护保持。

首个整合候选83a924db本机组合170项中156通过、14因缺LOTUS／OpenAI／LiteLLM依赖跳过，0失败／错误；目标64项全部通过、0跳过。
本轮新增服务器／POST／模型／GPU0，未使用服务器CPU16–23；三个localhost代理用例未重跑，保留交付方16次替身POST来源。
原回归源码摘要、82项逐项计数、失败来源、全部局部成本样本及中位数已离线核对，原两份JSON证据和复核脚本字节保持。
详见[整合机器记录](publisher_integration_verification.json)和[组合回归输出](publisher_integration_tests.txt)。

默认资格模式和显式性能模式分别记录，未构造的预期完整值仍为unavailable，查询计时无估算减法。
48.176708与16.700666毫秒保持原JSON-only替身的局部测量，不表示实际LOTUS或模型收益。
实际SDK模式差分、厂商图、官方Movie评价、最新作者完整Q1与模型／性能检查仍待各自验证。

有限需求复审指出常驻失败侧车缺少所选观察模式，失败摘要已保留模式；现已补齐`persistent-query.json`，LOTUS写配置值，其他臂写null。
新增无网络失败用例核对两模式与LOTUS／DuckDB／固定Map六种组合，原异常对象、group停止状态和摘要指纹保持。
修后组合171项中157通过、14缺依赖跳过，0失败／错误；14项受影响跟进回归全部通过、0跳过，新增服务器／POST／模型／GPU仍为0。
修后源码与命令见[修后机器记录](publisher_repair_verification.json)，输出见[修后回归](publisher_repair_tests.txt)；首轮记录保留。
固定候选916e1f54的标准与需求两轴有限跟进剩余必修均为0；审阅者只读，完整7303文件隐私扫描无违规。
