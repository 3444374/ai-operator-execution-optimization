# IMLane 普通 Map／Extract、关系链与固定 API 模型验证

当前固定 `4fc53086` 的查询 API 已完成下方[有限模型检查](#fixed-api-model-20261019)；
后续 `8d35ddea` 的公开 selector 仍只有本机 CPU 检查，两者按各自来源解释。

职责分支 `feat/imlane-records-chain` 固定基点 `aebbbcd8`，选择性接纳共享
`empty_prototype` 提交 `f977c979`。新增
[IMLane 通用入口](../../../../code/integrations/imlane_records/chain.README.md)复用原方法、
有限模型进程、结果账本及 Extract 关系控制；不修改作者调度、Lane、C++、旧 HTTP，
不增加线程池或 SemLoom 调度。Map／Extract 是公共链的单阶段值。

## 实际本地结果

新增八项和原有限链九项，共 17 个不同测试通过；定向复跑不相加。
普通 Map、普通 Extract、ME、EF、MEFM、EEF 六种组合分别与 LOTUS 1.2.4 accessor 的实际
messages 和最终 DataFrame 严格差分。输入为非 Q3 `title/body`，Index `17,17,29` 保留重复标签，
业务 `row_id/call_id`、额外 Extract 键和缺失键保持原语义；后继 grouped parents 不去重来源。
两块 pending 乱序就绪仍保留调用及发生项关联。

ME／EF 另用真实 CPU 子进程和三个独立 engine 身份检查装配及关闭；它们只返回声明响应，
没有模型推理。已知失败保留两项终态及 prompt 10／completion 4，阻断 Filter 后继。
作者异常退出即使物理资源已结清仍保持 unknown；已确认模型 API 证据保留。
模型关闭 unknown 保留 owner，取消前拒绝分配且单 query backend 不复用。

首次 r1 整轮失败原件保留：原 SDK 测试响应缺少 `engine_request_id`，失败工厂函数受 Python
descriptor 绑定影响，另复现公共空 Map 新列为 float64、旧组装为 object 的真实差异。
前两项只修测试 scaffold，r2 六项通过；r3 四项针对乱序 ready、准确失败身份和关闭证据补验通过。
原九项在 r1 通过且相关源码未改，没有重复执行。

空 dtype 不在 IMLane 猜测。公共 owner 的 `f977c979` 增加最终零行 DataFrame 原型，r4 两项
空源及 Filter→Map→Extract 空结果通过，严格保留 dtype／Index／列序断言，未增加模型调用。
本地监督分别观察 r1／r2／r3 的四个出生身份和 r4 的一个出生身份全部退出，无信号、残留或
cleanup error；这些观察不证明未观察后代或 GPU 退出。

私有作者 qualifier 的三个实际独占保存 API 回归及一个实际终态 SDK replay 回归另行通过，
不增加上述生产测试计数。保存采用逐查询快照和单独最终报告，不覆盖失败证据。

## 作者与模型待验范围

作者 CPU 材料固定 ME／EF 各三行，最多 12 次 CPU fixture 评价，300 秒含 30 秒清理，
8 CPU／RSS 8 GiB／SHM 2 GiB 观察限额，复用既有 Arrow 16.1 方法环境和 v2 二进制。
source、作者 HEAD／status／diff／源码／二进制、现有账本和独立进程／SHM／GPU清点均需核同。
新作者 CPU 尚未执行，不能用历史 echo、Filter 或级联资格填补。

真实模型配置保持禁用，拟用 ME／EF 各两行、新永久额度最多八次、每段新实例、无暖身和自动重试。
只重放实际终态给原 SDK，不产生第二套模型调用；每段独立 GPU 结清后才允许后继。
机器身份／当前 runtime、tokenizer／template 内容、永久账本与当前 slot 配置由唯一服务器 owner
绑定后才可启用，当前没有新模型／GPU请求。作者 Ray、多 executor、公共 selector、质量和性能另验。

精确源码与各次运行 SHA、失败原件定位见 [qualification.json](qualification.json)。

## 集成后的关闭异常复核

本次选择职责提交 `7a4524b7` 的十个所属文件，共享 `f977c979` 不重复接纳。
原17项含真实CPU child的检查继续绑定原职责来源；本轮另以当前依赖做16项无child组合核验，
不把两组数字相加。所有原作者scope／真实模型／GPU待验项保持。

独审发现公共 `NativeExtractRelationRows.close` 在阶段关闭抛错时，只保留异常名，
遗漏阶段已保存的模型API和作者物理状态。共享修补 `200330ce`、`eaa1c1d7` 在失败时
收集该报告副本、维持unknown和原错误；API的True／False字面值、已知usage和资源证据分列。
重复close不重计调用，后继仍被阻断。读取报告失败、坏元数据及None／非字典返回不跳过其余阶段清理。

5项新回归与11项旧原生合同共16项NoPopen通过；IMLane组合16项及独立author失败／model unknown
公开字段反例通过。最初公开报告缺失RED及中间坏报告反例保留，修补未认证作者环境或GPU。
精确源／测试和独审摘要见 [close_report_verification.json](close_report_verification.json)。
冻结的作者CPU包需由唯一执行owner重新绑定最终共享源码，不能沿用旧包哈希认证本修补。

## 旧冻结作者 CPU 回执补记

后续只读核验定位到 `imlane-chain-author-cpu-20261019-r1` 的实际成功回执，修正上方
“尚未执行”的阶段状态；旧准备阻断原件保留。该单元固定职责源码 `7a4524b7`，只执行一次，
99.924秒，ME／EF各6、共12次CPU fixture，分别3／2个输出；三个作者scope／模型fixture实例
正常关闭。独立50个观察出生身份及13个作者Linux start ticks均退出，SHM／RPC结清，
正常lease释放，GPU1MiB且compute空；作者源／二进制与八旧账本保持。模型推理次数为0。

原包SHA `3c71f3aeaffcc6ce2a6be1ea73f11c0b4c4762cfa9b832dffcaadf1d6efb5c1f`，
1077源码／1083普通成员及55个证据成员逐项核同。
[紧凑来源回执](author_cpu_old_source.json)保留原report／独立witness／归档SHA，原件由唯一
服务器owner保管。它与级联r3的44次fixture是不同查询／来源，不合并计数。

旧包已含 `f977c979` 空prototype，但共享关系controller仍为 `17604899…`；最终4fc为
`424979a1…`，唯一生产差异是关闭失败报告修补，另有文档和新增合同测试。
因此旧作者成功只认证旧包；4fc及后续公开selector需要新源码绑定。精确差异已生成只读交接，
不修改旧配置或冻结Async82e模型包。

## 公开入口与固定来源

新增 [chain-check／chain-run](../../../../code/integrations/imlane_records/chain.README.md#公开-ordinary-chain-入口)
显式选择 `imlane-author-local-model-chain`，使用已有IMLaneChainBackend和完整Extract关系屏障，
单阶段Filter／Map／Extract与有限组合共用原LOTUS loop。通用source不要求Q3列，不附加COUNT
或SemBench质量指标；旧Movie和级联命令保持各自身份。该入口尚无真实作者／模型／GPU资格。

25项入口CPU核验实际使用固定LOTUS1.2.4、公共方法／批内核和内存FixtureBackend，严格差分
Map／Extract／ME／EF消息及关系，重复Index／grouped parents、空prototype、knownusage、
primary／metadata错误和取消均覆盖。CLI层ABI／owner明确替身；不启动作者或模型child。
45项owner合同使用真实临时CellBudgetLedger、JSON／哈希和lease解析，作者／ABI／GPU为替身，
验证现有unit一次claim、不退款、失败后收尾和逐已闭scope GPU门。另17项既有lease／IMLane兼容
CPU通过，旧真实CPU child用例未重跑。三组零fail／error／skip；定向复跑不累加。

原普通RED保留：DataFrame默认to_json截断float64业务值；外部lease先hash再重新读取会接受
另一版本。入口现以原标量构建split JSON，float64及uint64经JSON往返精确保持；lease在同一份
有界raw bytes上计算SHA并复用原schema解码。两项回归转绿。初始测试scaffold失败与中间WIP
运行也保留，不能当最终固定源码结果。

启用执行只复用现有有限supervisor、原作者owner和caller既有正常lease；单query／deployment／
executor／GPU，pending≤2，显式最终空prototype及永久unit上界。新入口不恢复stale lease，
不创建／重置预算或自动重试；known API／usage和独立物理／GPU证据分列。最终查询JSON写失败
也保持失败和原错误，不能冒称结果已保存。

最终源SHA、完整脱敏CPU记录恢复入口、原失败SHA与两轴审阅见
[public_selector_verification.json](public_selector_verification.json)。这是当前本机合同资格；
新源码作者环境、实际模型／GPU和性能由唯一执行owner另行冻结核验，既有成功单元不重跑。

<a id="fixed-api-model-20261019"></a>
## 固定查询 API 的单次模型检查

原始实验 `imlane-chain-model-4fc53086-20261019-r1` 使用 `4fc53086` 的1078项公开源码投影，
直接调用 `IMLaneChainBackend.evaluate`；原作者 DBEnd／Coordinator／Lane／CPython executor
拥有调度和传输，v2桥与作者源码／二进制未改。此前 `7a4524b7` 的作者CPU成功保持原来源。
新增五项关闭报告测试先在既有driver通过；模型11文件、模板和采样参数核同后才建立新账本。

| 两行合成查询 | 实际调用 | 模型实例及上限 | 最终行数 |
|---|---:|---|---:|
| Map→Extract | Map 2、Extract 2 | 一个实例，4次 | 2 |
| Extract→Filter | Extract 2、Filter 2 | 两个实例，各2次 | 0 |

8次均成功，原始usage为738输入／126输出、共864 tokens。两个查询都把实际终态回放给原
LOTUS开发包（SDK），没有新增模型调用；消息、最终关系、重复命名Int64索引、字段顺序及
Extract后继的分组父调用核同。Filter返回空关系，其字段及dtype与原SDK一致；这不代表语义质量通过。
Qwen2.5-7B-Instruct／vLLM0.25.1／单4090，生成与批配置、精确来源和计数见
[紧凑回执](model_qualification.json)。运行起止为2026-10-19 12:42:58.277108–12:47:31.463077 UTC，
273.185968秒包括准备、加载和退出；只有一次样本，不能作性能比较。

三个实例的API关闭字面值均为true，三次分段及两次查询GPU退出检查通过。独立只读复核确认
247个已观察出生身份、88个作者start_ticks身份退出，GPU为1MiB且计算进程为空；共享内存为空，
RPC活跃连接0，9个非活动目录项保留。三个作者资源组正常退出，原helper释放自有正常lease，
没有清理信号或错误。九个旧账本、118份旧证据、作者12源码／3二进制及HEAD/status/diff保持。
100ms采样的常驻内存及共享内存最大值为8,922,361,856／134,258,688字节；不表示完整瞬时峰值或硬上限。

独立永久账本总额8、唯一unit只领取一次，实际按4／2／2完成；无暖身、退款、转移或自动重试。
已成功的旧CPU和模型单元未重跑。原disabled配置与准备失败原件保留，两份终态独审通过。
完整原件8,618,517字节／1236成员留在仓库外，SHA256为
`5ad35a706fbcda0fe71b7dd000d00e005964f3f524ba2c25bb7bb877e77b1fc6`；
向实验维护者提供运行ID和该摘要可获取原归档及完整成员索引，关键原件SHA见紧凑回执。

本次只认证上述查询API。公开selector的实际作者执行、作者Ray、多executor、PostgreSQL接入、
语义质量和性能仍另验；不把4fc模型结果转授后续公开入口或级联。
