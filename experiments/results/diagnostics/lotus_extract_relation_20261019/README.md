# Extract 后继的关系完成边界

从最新已发布 `integration/data-execution@d2915206c945e3c5db658a6533f3e6fed9b8cc6a`
建立独立职责分支 `feat/lotus-extract-relations`。本轮只新增关系值模块、专属测试与本报告；
主集成确认共享内核、现有方法／结果账目、原生接线与末端 guard 继续由其拥有。
首轮报告是后继接线的 CPU 准备证据；当前显式 Core profile 的后续接线见末节。
Ray Data／Daft 原生图仍不支持非末端 Extract。

## 实施条款与职责

当前[有限链计划](../../../plans/数据组织.md#lotus-operator-extension)要求字段碰撞、索引和原错误保持，
允许阶段式正确性控制，同时避免每算子固定 collect。这里的屏障由重复标签跨行写入这个真实依赖决定，
不能以每个物理 batch 的完成替代整个 Extract 关系完成；没有计划冲突，也没有新增通用 DAG／Join。

| 验收对象 | 本职责实现或准备 | 公共 owner 接线责任 |
|---|---|---|
| 关系完成 | [record_relation.py](../../../../code/src/semantic_methods/lotus/record_relation.py) 只接已验证前缀的发生项、schema 和调用快照，并要求显式成功源 EOF | 使用现有 `ChainBatchResults` 核对完整源覆盖、阶段成功及原生 EOF；一个不完整行列表本身不能证明覆盖 |
| 跨批重复 index／覆盖 | 按 `source_position` 逐键累积实际 `index_updates`，广播到同标签的全部发生项后清空这些待应用更新 | 在后继开始前形成这个完整关系，不在单 batch 内局部完成后放行 |
| 原值／空关系 | 直接保留有限 JSON、源列、实际键与 `None`，显式保留空关系有序 schema；不按声明补输出列 | 由 source／完整结果提供有序 schema；不通过 DataFrame `to_dict()` 引入 NaN／pd.NA |
| 跨行父依赖 | 每个发生项带同 index 组的全部 Extract call IDs，保留原来源身份和字段覆盖者 | 给现有阶段循环提供父调用延续入口；不能重置后只留下本发生项父调用 |
| usage／错误 | `call_snapshot` 为原已知调用的独立副本，不计作新工作；本函数异常保留相同调用快照 | 原共享账目继续拥有全部调用、LM 累计快照、raw／unknown 和主异常；失败不发布后继关系 |
| 后继 Filter／Map | 通过现有 `LotusRecordChain` 和原 formatter／parser 做 CPU 阶段控制 | 保留现有 Extract 末端 guard，直到关系屏障、依赖、空／nullable 与模型关闭合同接通 |

`CompletedExtractRelation.records` 保留 `row_id`、`source_position`、`source_index` 与 `source_refs`；
数据里的同名键仍只是数据。`parent_call_ids` 以 `(row_id, source_position)` 为键，
同标签组父依赖是保守的完整组，不宣称逐字段最小 lineage。该值目前是进程内合同，尚未增加 native wire。
函数不生成提示、不解析模型、不拥有 stage loop、线程池、模型实例或其关闭时点。

## 固定来源与独立表征

固定 LOTUS 1.2.4／`b1a85fd7a66fabed8a1585d44d7597d592b4433f`、LiteLLM 1.95.0、
OpenAI SDK 2.50.0、pandas 2.3.3；沿用已有 Python 3.12.6 隔离 CPU 环境，未安装依赖。
明确 macOS CPU profile 的只读 `core,semantic-benchmarks` preflight 通过；未启动模型、HTTP、Ray 或 GPU。

原 `sem_extract.py:240–248` 复制输入表后，按关系行顺序对实际输出键执行 `.loc[index_label, key]`；
新字段先建空列，重复标签的后写覆盖所有同标签行。原 parser 的缺键、额外键与字符串化均不加强。
原 Extract 来源 SHA256 为 `0f58eda86d645b85b8659591d61366a86809a4d189dbec6c491fe5b4628c163a`。
独立[11 项来源探针](raw/source_audit.txt)核对了跨批反例、字段覆盖、空 schema、原 accessor 后继和 usage；
其中“native”指原 LOTUS pandas accessor，不是新原生 backend 或模型资格。

不能直接把装配后的 DataFrame 转回 JSON 发生项：数字列的空值可能变成 NaN；
`convert_dtypes()` 还会将原提示里的 `«None»` 改成 `«<NA>»`。
因此新边界直接应用原已解析字段到有限 JSON 记录，不补值、不将整列 `str()` 化。

## CPU 验证与接线剩余项

专属 [CPU 测试](../../../../code/tests/semantic_methods/test_lotus_record_relation.py) 11 项、9 个 subTest 全部通过，
无失败、错误或跳过。测试直接使用共享 `ChainBatchResults` 逆序接受两批 Extract 结果，
在覆盖核验后装配；完整关系与原 LOTUS 全表 Extract 相同，原已有列被覆盖而缺键仍保留旧值。
字符串字段的后继 Filter／Map 复用既有阶段循环，其实际 messages 和最终关系与原 accessor 一致。
稀疏 `None` 当前仍在后继准备前报原有文本检查错误，前缀 usage 保留；不是已支持 nullable 消费。
完整命令、[CPU preflight](preflight.json)、源／日志 SHA、先前 10 项运行和独立来源探针见
[验证 JSON](verification.json)。先前运行是补充源列覆盖前的同一套件，不与本轮重复相加。
测试专项独立复审、Standards／Spec 两轴均无阻断；Spec 轴独立重跑同 11 项全部通过。
阶段式控制只证明关系语义；不代表引擎复用、跨阶段重叠、GPU 性能或新 native 图已经取得资格。

共享接线尚待主集成：

- 原生 owner 在整个 Extract 源 EOF、覆盖和解析成功后才调用边界，并给后继提供完整关系；
- `LotusRecordChain.run_batch` 当前重置父依赖，需最小延续接口消费本模块的跨行父调用；
- `LotusRecordMethod.prepare_rows` 当前拒绝非字符串。原 formatter 可以格式化稀疏字段的 `None`，
  但新关系不能靠填值或字符串化绕过检查；nullable 消费范围及提示表示须单独明确；
- 模型按所有前后继消费者关闭，前缀 `expected` 数量和前缀 EOF 不能单独决定回收；
- 公共 README、Infra 和台账登记由主集成与共享接线一起完成，本职责不抢改其维护文件。

现有非末端 Extract guard 保持。真实模型／GPU 由主集成统一排程，本轮调用为零。

## 后续共享合同与显式 Core 接线

主集成以 `a903e51c` 接纳共享合同、`3fcb7a88` 接纳 Core 接线，`4566939a`
为 Ray／Daft 增加资源分配前的拒绝门。上述首轮待项是历史状态；当前
[`LotusRecordChain(relation_barriers=True)`](../../../../code/src/semantic_methods/lotus/record_chain.py)
仅由 [Core owner](../../../../code/src/execution_provider/adapters/lotus_record_chain_core.py) 消费。
默认末端 Extract 规则保持，直接 `run_batch` 仍拒绝非末端 Extract，不增加通用 DAG。

Core 收齐整个前缀，在真实源 EOF、成功覆盖及解析核验后，等待 delivery lease、Core 额度、
transport 输入／结果全部归零，关闭前段 session，再应用本模块的原 `.loc` 广播。
下一段使用新 session，沿用同一 query、worker 和私有 EngineProcess；全局 stage ID 保持唯一。
后继消费显式有序 schema、同标签 Extract 父调用组及实际 `None`；只在派生关系阶段允许存在的
字符串／None，不填值、字符串化或放宽 Movie 源规则。空关系的 schema 继续保留。

原共享方法循环与 formatter／parser 继续执行；前缀结果不会提前发布给最终消费者。
调用、raw、LM 统计和失败审计按阶段保留并去重，最后消费者 EOF／错误／显式 close 才回收模型，
前缀 expected 数量不触发提前关闭。未知调用保持原 Core 责任，物理清理成功不会把它改为成功。

一个 OfferedTask 是当前段的一批有限原始记录 bytes；work 上界为该批行数乘当前段算子数，
不等于同时运行的 GPU 请求数。当前仅一个在途 Core 任务、一个同步 fused worker，尚无 AsyncLLM
跨批持续供给。每任务逻辑输入／结果额度为 2／8 MiB；驱动端仍保留完整 expected 源值、源迭代器、
一批 pending 输入、必要的完整 Extract 关系和调用／raw／统计历史，因此这些额度不封顶全查询 RSS。

[派生验证摘要](core_verification.json)分列共享 129 项、Core 组合 55 项及 native 前置门 2 项；
组合套件含共享 8 项及 helper 11 项，不能重复相加。Core 新增 13 项核原 LOTUS 关系／提示、
跨批重复索引、nullable／空 schema、父调用、unknown／早停、源 close 原错误和同 PID 私有 CPU
pipe 延续；外层监督确认 3 个已观察 owned 出生身份退出，remaining／cleanup 均为空。
首轮 fixture 指令误命中、过渡运行及旧默认集合引用修正均保留原件。模型／GPU 调用均为零，
这项 CPU 接线不继承任何冻结普通 Filter 模型资格；新 profile 的真实模型与各 native 屏障仍待验证。
