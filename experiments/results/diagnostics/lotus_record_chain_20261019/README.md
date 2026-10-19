# LOTUS 普通记录方法与有限链 CPU 检查

本切片从 `integration/data-execution@c0be45dd` 的独立职责分支 `feat/lotus-finite-chain` 推进。
它实现选定普通文本 Filter→Map→Extract，并以 Map→Filter 验收同一套方法和原生接线的复用；三步是首个验收形状。
本轮固定响应 CPU 检查通过，实际库和真实 GPU 资格分开记录。没有启动模型、HTTP serving 或 GPU 单元。

## 计划条款与实施对应

| 既有要求 | 实现 | 验收 |
|---|---|---|
| [三行有限链](../../../plans/数据组织.md#lotus-operator-extension)：Filter `3→3` 后关系过滤 `3→2`，Map／Extract `2→2` | [record_chain.py](../../../../code/src/semantic_methods/lotus/record_chain.py) 的普通方法与显式有限链 | 固定响应核对七次逐发生项评价；物理提交批次独立记录 |
| 下游实际使用生成列，保留源列和发生项 | `RecordOccurrence.values` 与来源、调用身份分离；Map 新 `summary` 进入 Extract 原准备函数 | messages 与原 accessor 逐阶段比较；被过滤行没有后继，同文不合并 |
| Map／Extract 原 formatter／parser、字段碰撞和索引 | 调用固定 LOTUS 原函数；Extract 仅请求原 `json_object`，不增加 strict schema | 空白、额外／缺失键、字符串化、JSON 错误、数据字段碰撞、稀疏／重复标签差分 |
| [原生执行所有权](../../../plans/系统架构.md#lotus-data-execution) | [Ray Data 链](../../../../code/src/baselines/text/frameworks/ray_data_lotus_chain.py) 的原生 callable-class `map_batches` | 单 action、原生批次与 actor 池推进；全丢弃批仍交账本，不逐阶段回 driver collect |
| [模型环境和寿命](../../../plans/系统架构.md#model-engine-execution) | 单 Ray GPU actor 独占隔离模型子进程，逐调用生成参数由唯一引擎负责人扩展公共接口 | CPU 替身核对参数隔离、实例复用、错误与 OS 退出；真实 GPU 单元由集成 owner 排程 |
| [同模式对照](../../../plans/reference/对照规范.md#lotus-algorithm-execution) | 当前为同步、融合、单机 callable-class 接线 | 不记为官方 vLLM Processor、异步持续供给、完整 baseline 矩阵或性能改进 |

本职责新增语义与链专属模块／测试；不修改公共 `records.py`、`vllm_engine.py`、
`lotus_records_movie.py` 或既有单 Filter 后端。公共逐调用 generation 与 JSON object 映射提交由集成 owner 提供，
作为单独依赖保留来源，不在职责实现里改共享 `_sampling` 或复制模型执行内核。

## 原生图与最终结果的职责

调用方交付 `record_json` 原生 Dataset，每行序列化一项 `RecordOccurrence`；源列和来源元数据不变成模型消息。
一个 Ray Data callable-class actor 按原生批次运行原 Filter 准备／模型／parser，只有幸存行进入 Map，
再将 Map 新列交给 Extract。三个方法都在图内准备和解释结果，隔离模型子进程跨输入批复用。
这是首版融合布局：原生输入批、各阶段的有限引擎提交和引擎内部计算批不同，不把七次评价写成七个 batch。
模型子进程及 IPC 成本属于本适配；没有将所有 baseline 交给统一线程池或调度器。

结果端验证源发生项、逐阶段身份、父调用关系、覆盖和终态，收集调用账本及 worker 累计统计快照。
最终 `records_to_frame()` 仅做关系装配：按 `source_position` 恢复顺序，保留稀疏索引，
按原 Extract accessor 的 `.loc[label, key]` 应用图内已解析字段。重复标签的后写广播在此保留；
数据中的 `row_id`／`call_id` 键不能改写内部发生项和调用身份。这个最终装配不调用模型或 parser。
全丢弃关系仍可用结果携带的列名保留源 schema 和 Map 的空输出列。

`StageSpec.stage_id`／`model_role` 是开放名称，评价序号与记录发生项分开；
`source_refs` 保留命名来源，`values` 保留分组／关联列，调用结果的 `parent_call_ids` 是列表。
当前共用 runner 组合已支持的有限线性节点，两种组合分别验收；这些字段不表示分支、汇合、子查询或全局聚合已经实现。
本切片的 Extract 位于末端；若后继要消费重复标签装配后的列，必须先实现真实依赖，不能当成独立逐行流水。

## 已核对的原行为和来源

固定 LOTUS `b1a85fd7a66fabed8a1585d44d7597d592b4433f`／1.2.4、LiteLLM 1.95.0、OpenAI SDK 2.50.0。
原 `sem_map` 输出不 strip；普通 `sem_extract` 固定 `response_format={"type":"json_object"}`。
原 Extract parser 只捕获 `JSONDecodeError` 并返回空字典；接受额外键、缺键和字段覆盖，值统一 `str()`；
合法非 object JSON 在 `.items()` 抛 `AttributeError`。不将格式请求解释成更强的 schema 校验。
原 Extract 纯文本提示末尾 `\n\nNone` 也保留。准备、解析、LM 统计与相关 accessor 按实际安装字节核验。

新增核验：`sem_ops/sem_extract.py` SHA256
`0f58eda86d645b85b8659591d61366a86809a4d189dbec6c491fe5b4628c163a`；
`types.py` SHA256 `73b0ebb4a8a9606cdec6df27353d57fb28ce6e2f5800c539caee892eb4f2cc6b`。
其余准备、Map、parser 与 LM 摘要复用既有来源表，不重定义 LOTUS 算法。
工程分工沿[架构 §8.8](../../../plans/系统架构.md#pgml-engineering-reference)：新增语义和关系处理，复用模型调用／清理，
不安装 pgml、不复制公司材料；本轮没有重新访问公司源码。

<a id="composition-reuse"></a>
## Map→Filter 复用性增量

首个 FME 交付为 `62001fb9`，以下增量从该提交继续。直接组合探针发现三项真实共用限制，
所以本轮包含接口修正，不能报告为“仅加测试”。

| 复现的限制 | 最小共用修正 | 验收 |
|---|---|---|
| 旧 `FilterMethod` 只接受 Q3／`reviewText` 指令 | 新链内 `TextFilterMethod` 接受显式列引用，仍使用原 Filter formatter／parser；旧合同不动 | Map 摘要的实际内容决定 Filter，原评论不在 Filter messages 内 |
| 构造器只接受三步，列初始化假定第二步 Map | `LotusRecordChain` 组合已知节点，原三步类只保留示例校验；原执行循环不复制 | 两种组合的 `run_batch` 是同一个方法；JSON 往返保留节点参数与身份 |
| Ray 结果核验假定首阶段 Filter、末阶段保行 | 同一接线按 `StageSpec.kind` 核验每阶段输入与关系选择 | 终态 Filter 的 false 评价仍入账，最终关系只包含 true 发生项 |

Map→Filter 的三条输入均先 Map，再 Filter，为 `3+3=6` 次独立评价，最终两条发生项；
重复原文本仍独立，不以输出内容去重。原 accessor 差分逐阶段核对提示、参数、LM 统计与最终 DataFrame。
语义增量独立复查 18 项通过，完整[脱敏输出](raw/test_lotus_record_chain_composition.txt)另存；
首提交的 15 项记录保留原身份，不与重跑重复相加。
Extract 仍限制在末端，因为原重复标签 `.loc` 更新在关系装配时生效；没有实现其后的跨标签依赖。

原生返回值采用单次消费 iterator。首次完成后再次迭代为 EOF，不新建模型工作；
重复读取已消费记录或 DataFrame 不触发执行。重新 `evaluate` 使用新 query ID，表示新的查询及模型评价。
内部 Ray Dataset 不作为可重复 action 的公共接口，不承诺隐式模型结果缓存。
调用方若提前停止消费，须调用 `stream.close()`；只有成功 EOF、完整覆盖与清理确认后才标记查询完成。
模型实例必须保持到当前原生批的全部后继完成；不能因为首阶段评价数已达到输入数就关闭。

固定 LOTUS 1.2.4 实际导出 `LazyFrame`，其 `sem_map`／`sem_filter` AST 节点调用原 pandas accessor，
`LazyFrameRun.execute` 顺序走节点并使用内容缓存。独立表征使用显式 `InMemoryCache`：
关闭 LM 缓存时，同一内容缓存重复 execute 不增加响应调用；新内容缓存重新运行两个阶段。
该结果只适用于这里固定的源码与测试条件，不把 LOTUS 的关系缓存暗接到新原生路径，也没有升级依赖。
源码摘要另列于[增量验证 JSON](composition_verification.json)。

本轮原生检查 24 项全部通过，无失败、错误或跳过：17 项协议、2 项真实私有 CPU 传输、5 项真实 Ray CPU。
新增真实图直接验收 Map→Filter 的生成列消费；原 FME、取消／unknown／退出和空源核验继续通过。
重复消费与关闭顺序的新增事件测试分别确认无额外模型工作、引擎在末阶段完成后关闭；这两项生命周期实现没有改变。
增量两轴独立审查及方法专项复审无代码阻断，报告登记项已补齐；新增测试不与首轮重跑相加。

报告元数据的 `layout` 从首切片 `fused_filter_map_extract` 改为 `fused_record_methods`，
新增 `stage_kinds` 逐项记录实际组成，以适配共用接线。首切片日志与摘要保留旧来源，
关系、调用身份、错误和清理语义不因此变化。

当前 Ray 融合布局在每个原生批内同步逐阶段完成；下一阶段等待当前阶段的有限提交返回。
这证明有限依赖和原生逐批推进，没有证明跨阶段重叠、AsyncLLM 持续供给或 GPU 吞吐改善。

## 后续复杂任务候选

已只读核实 SemBench `c814e3807e72d4cf876b852b17e77f3cc94575c2` 的
[Movie runner](https://github.com/SemBench/SemBench/blob/c814e3807e72d4cf876b852b17e77f3cc94575c2/src/scenario/movie/runner/lotus_runner/lotus_runner.py)：
`_execute_q7`（327–388 行）对 Reviews 自连接，提示消费 `reviewText:left`／`reviewText:right`，
排除同一业务 `reviewId` 后投影左右评论。它需要保留左右来源和配对身份，适合作为实际需求驱动的后续扩展，当前未实现。
同文件 `_execute_q8`（390–432 行）在 Map 情感分类后执行全局 `value_counts`，
计数依赖完整分类结果，也不能宣称为无依赖逐行阶段。

## 验证与剩余项

公共生成接口依赖为 `640446822c430d21f08d1f9bde75e61198912aa3`，其 parent 为本切片基线；
以 fast-forward 保留原提交，职责实现不修改该依赖的三个文件。
首 FME 提交不使用类型补丁的[正式 CPU 验证](verification.json)按独立解释器运行，原 LOTUS 三步方法差分 15 项，
共享引擎及受影响旧 records／Filter／Ray／Daft／静态／Movie 路径 118 项，合计 133 项通过。
原生 Ray 专属 21 项另列：15 项协议检查、2 项真实私有 CPU 传输、4 项真实 Ray CPU 用例。
后者覆盖原 LOTUS 的完整三步链、合法非 object JSON 的原解析错误及已知 usage、
取消后的 unknown／OS 退出、空期望下的真实空／非空源验证。无 GPU 资格由这些检查转授。

初次把模型隔离测试与 LOTUS 测试放在同一解释器，触发有意设置的 LOTUS import guard；
macOS 沙箱也拒绝 psutil 的 sysctl 进程枚举。该次 118 项的 1 failure／46 errors／6 skips 摘要保留在验证 JSON，
改为独立解释器并允许 CPU 子进程观测后，118 项旧路径均通过。
独立审查还修复了差分测试误用 `max_tokens` 参数键的问题，原 LM 实际使用 `max_completion_tokens`。
首个 FME 提交前另对固定 `64044682` 的职责 diff 完成 Standards／Spec 两轴只读审查，无阻断；
私有进程 `serve` 与旧传输的分派重复作为非阻断复用建议交公共 owner，不在本职责改写旧后端。
Map→Filter 复用探针确认 Q3 指令与三阶段构造器的现有限制，后续最小共用接口增量单独提交。

当前使用已有隔离 Python 3.12.6 CPU 环境，不安装或修改共享依赖；macOS 没有自动匹配的 GPU profile，
使用明确的 CPU profile 完成只读 `core,semantic-benchmarks` 检查，Ray 2.56.1／Arrow 24.0.0／pandas 2.3.3 已确认。
测试启用本地 LiteLLM 价格表和离线模型开关，固定响应替代推理。

GPU／真实 vLLM、分阶段布局、Daft／IMLane／SemLoom 新链、生产 Core、原全局级联、图像及复杂任务仍待独立实现或验证。
本切片不产生 GPU 性能、质量、AsyncLLM 或完整系统对照结论。
