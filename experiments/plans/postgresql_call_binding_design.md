# PG调用与结果绑定：近期详细设计

更新日期：2026-09-07
状态：`current / design-specified / implementation-started`
受众：PG扩展实施者。所属工作为[主设计A1–A2](postgresql_ai_semantic_operator_architecture_20260827.md#implementation-sequence)。
本文件唯一定义近期调用/绑定的实现方案；总体分工、远期SQL与研究方向仍由主设计拥有。

原设计代码依据为`66887463`。共同调用与tuple绑定在19326609完成，§11的V1载体与一个Filter→一个生成Map
已在开发分支实现并通过[验证](../results/postgresql/filter_map_binding_20260907/README.md)：
PG18.3回归1、TAP1910项，本地115、Linux138项，611源码哈希一致。模型请求0，尚未合入main。
前序基础与setrefs/OFFSET原型见[绑定验证](../results/postgresql/semantic_binding_20260907/README.md)。
§8–10保留当时实施记录；当前格式和行为以§3–5与§11为准。

## 1. 首批范围与选定方案

分两次可审阅变更：A1先提取共同调用记录与绑定校验，不改变现有支持形状/输出；A2a再支持**一个
顶层关系Filter接一个顶层生成型Map**。Filter可为recording/exact/choice，Map为三参生成路径，使用
现有v2/v3/v4/v5和同步provider。保留已验证双Filter路径。多个独立Map、嵌套Map属于A2b后续设计，
不是A2a顺便放开的形状；不允许两个Filter再接Map、OR/NOT、Map嵌入普通函数、Join/CTE/聚合/重扫。

A2a支持现有单表、非继承SELECT及直接INSERT ... SELECT限制的交集；Map排序、RETURNING、ON CONFLICT、
OVERRIDING及整个Map的SQL包装函数保持拒绝。普通输入表达式和普通投影仍按原范围处理。

选择继续使用现有两处hook：Filter在base relation路径阶段构造；Map在`UPPERREL_FINAL`阶段构造，
因此此切片中的LIMIT/OFFSET先决定Map需要处理的行，Map不移动到Filter或LIMIT前。
不引入统一SQL DAG、新的CustomScan类型、一次组合RPC、Core接线或新公开SQL函数。
接口先让两个已有builder共享调用/绑定知识，而不是搬进一个巨大的通用builder。

## 2. 源码复核与取舍

| 当前符号/文件 | 已核对事实 | 拟议改动 |
|---|---|---|
| `extension.c:semloom_planner` | Map原SQL来源检查在standard_planner前；嵌套规划/ERROR恢复状态 | 保留来源检查与PG_TRY/FINALLY，不用优化后的表达式代替原调用授权 |
| `sem_filter_call.c:semloom_filter_calls` | 从baserestrictinfo提取至多两个顶层调用 | 复用为Filter收集器，输出共同调用描述；类型和参数检查仍归Filter |
| `sem_map_path.c:semloom_supported_marker` | 只接受一个直接输出Map，依赖query source检查 | 复用为Map收集器；A2a仍一个Map，不删除全部guard |
| `sem_map_path.c:semloom_add_sem_map_paths` | `UPPERREL_FINAL`构造现有Map路径 | 保持位置，识别合法Filter child，不回到普通base path绕过Filter |
| `sem_map_path.c:semloom_plan_path` | Map结果位置与逻辑marker描述保持一致，child把marker替为输入 | 保留该身份原则，A2a改为显式输入位置和独立结果位置 |
| `sem_pump.c:semloom_pump_next` | child/scan列数必须相等，Map覆盖input_column位置 | legacy分支保持；新binding使用明确透传映射与独立输出列 |
| `sem_plan_spec`、`pg_semantic_runtime`、`ai_provider_port.h` | 语义解码与同步借用/复制规则已经明确 | 继续复用，不更改messages/digest/语义schema/wire/错误类别 |

来源与采用遵守主设计§8.7–8.8：沿用固定版本的调用识别、列映射与共享生命周期经验；本次未重新
读取公司或pgml源码，不将其实现复制为自有设计。本切片的具体PG行为最终以REL_18_3测试为准。

## 3. 对象、接口与寿命

### 3.1 规划期共同描述

`planner/semantic_call.{h,c}`已提供内部`SemloomSemanticCall`；Filter和Map收集器共同消费它。
不建立全局registry；对象由对应PlannerInfo使用的规划内存拥有。

| 字段 | 定义 |
|---|---|
| `call_key` | 当前规划上下文内的(query_level, placement_kind, occurrence_ordinal)，不是持久/跨进程ID |
| `function_oid`, `operator_kind` | 通过扩展成员身份校验的函数与本算子种类 |
| `placement_kind` | BASE_FILTER或FINAL_MAP；此切片不引入任意层级搜索 |
| `input_expr`, `marker_expr` | marker按出现复制；input通过semloom_call_input从同一marker读取，不以equal或指针相同合并出现 |
| `source_locator` | Filter restriction列表位置或Map TargetEntry.resno；只在该规划阶段解释 |
| `semantic_fields` | 已经由算子验证的参数/语义字段，复用现有spec builder |
| `result_type`, `typmod`, `collation` | 交给PG绑定/结果投影的类型描述 |

Filter与Map的key有不同placement命名空间；path候选复制同一描述保留key。key只辅助节点关联/校验，
不充当PG Var/列号，不加入语义摘要。不同规划调用或prepared重规划无需得到相同key。
A2a没有Map数据依赖另一语义值，故不先增加通用dependencies列表；Filter→Map的关系依赖由child树表达。

不得把PG改写前的指针/列表序号拿到改写后当同一对象。原SQL检查只判断来源/位置许可；共同描述在
相应hook已规范化的表达式处建立，复制进入Path/Plan后不再反向检索原Query以猜输入位置。

### 3.2 绑定值与Plan存储

在`planner/semantic_binding.{h,c}`集中绑定构造、严格解码与校验；执行层消费解码值，不重算列位置。

```text
SemanticBindingV1:
  input: 新Map由custom_exprs中的唯一text表达式提供；legacy使用child列号
  passthrough: [(child_resno, scan_resno), ...]
  result_resno: Filter为空；生成Map为独立新增scan列
  result_type/typmod/collation由PG child/scan TupleDesc提供，不在binding中重复保存
```

A2a Map结果列追加在child列之后，不能覆盖输入列；最终TargetEntry用现有PG投影只输出用户要求的列。
Filter只透传存活行，不新增语义布尔列。临时输入列可作为resjunk保留在内部，不泄漏到SELECT/INSERT结果。
透传目的位置唯一、范围合法；结果位置不与输入/透传混同；所有scan列必须有定义，不允许未初始化slot。
公共binding只验证位置、映射覆盖、引用与类型一致性；text输入和text/boolean结果由当前算子合同
检查，不把TEXTOID写成所有语义算子的永久要求。多个输入/结果或新类型需要对应版本和编码验证，
沿用列映射与节点生命周期；FINAL_MAP也是当前placement，不限制未来方法必须位于FINAL。

A2a首先仅在新组合的Map节点使用V1，Filter child和现有单算子仍保留legacy。新Map的`custom_private`采用可复制PG Node构成的带版本命名envelope：`carrier_version=1`、`call_key`、
`function_oid`、`semantic_fields`、`binding`，字段缺失/重复/未知或类型错误均拒绝。
binding固定为[result_resno, passthrough]；新Map不携带Filter成本字段。
这是**PG内部carrier格式**，不改变语义schema或wire。语义解码复用原字段解码器；
legacy视图只在原路径适配，新Map不生成虚构的输入列号。
旧路径保留旧格式；legacy适配保留原来Map覆盖输入的绑定模式，V1严格禁止这种别名，不将宽松模式暴露为新配置。
decoder仅接受明确的legacy或V1形式，不能遇到未知V1字段后尝试legacy。
新旧carrier格式的判别和解码集中一处，不让pump/各算子分别推测list长度。

`custom_exprs`保留需要setrefs/对象依赖处理的marker与表达式，`custom_scan_tlist`声明实际scan输出，
不把PG Expr序列化为JSON。新Map仍以marker描述它的逻辑结果；custom_scan_tlist描述不直接求值。
所有runtime句柄、Socket、MemoryContext和状态只在每次Exec初始化时创建。
[PG CustomScan计划接口](https://www.postgresql.org/docs/18/custom-scan-plan.html)

## 4. 规划与执行的确定时序

1. planner_hook校验原始Map调用来源和固定参数规则；将控制权交回既有PG规划链。
2. base hook收集/验证Filter描述并构造其Path；透传下游所需原始Vars，不提前求值Map输入。
3. final hook验证完整形状只属于A2a许可组合，从已有output path构造Map。保留Filter子树及LIMIT位置；
   新child target只承载原始Vars及已有结果，普通输出表达式按PG实际选择的位置保留，
   不强制把它们移到LIMIT/OFFSET上方。去除本Map拥有的marker计算；不能沿用旧的递归
   marker→完整input替换，把Map输入函数留在LIMIT下。必要的内部占位列使用typed NULL而不求值，
   它不是SQL语义结果，不能被最终投影引用。由V1绑定恢复最终marker到新结果列的对应。
   Map以原生ExprState在取得LIMIT之后的一行时计算输入，生成独立结果绑定。
   CUSTOMPATH_SUPPORT_PROJECTION只声明投影能力，不作为通用求值屏障；需以最终Plan和调用次数
   证明位置。重建本候选所需Path/target，不能原地修改其它候选共享对象。无法维持的路径在规划时拒绝。
4. Plan callback从选中Path描述编码binding/spec，不再按表达式相等扫描所有结果列猜归属。
5. Exec初始化先校验新carrier/类型、函数身份与权限，创建节点私有方法/runtime，再初始化child；
   无任务时不连接provider。新Map选择显式使用PG原生ACL/object hook设施，每个marker出现检查一次；
   legacy Map原检查保持。权限入口不依赖custom_exprs中marker经setrefs后仍是FuncExpr，
   普通输入/输出表达式仍由PG正常初始化；不能为检查而执行marker或重复Invoke hook。
6. 下游Map拉取一行 → Filter驱动ordinary child直到存活行 → LIMIT/OFFSET处理 → Map输入ExprState求值。
   Map input为NULL时本地产生NULL结果，否则复用lazy open/drive，校验并复制completion至tuple context。
7. pump按passthrough写入scan slot，写独立result_resno，再交PG投影/INSERT。消费该行之前不拉下一行。
   每个节点一份不可变spec、一个私有runtime/sequence和按需打开的provider session。

Filter与Map的连接可以同时存在，但本切片没有跨节点异步派发/预取。input不得因为生成结果而被改写；
借用child Datum只到下一次child pull/reset，结果在离开drive后按既有规则复制。模型配置不逐行重建。
无排序时仅保持实际child交付次序，不承诺SQL天然按id排序；测试可以在客户端比较无序结果。

## 5. 中断、资源与兼容

| 事件 | 必须行为 |
|---|---|
| Filter错误 | Map不取得该行、不求值其输入、不发其任务；PG错误终止语句 |
| Map错误/用户取消 | 当前语句停止继续pull；PG现有ERROR/context cleanup关闭全部已打开节点连接 |
| LIMIT 0 | 两个节点均零模型任务，允许计划/权限初始化；不连接gateway |
| 同步LIMIT早停 | Map只处理上层实际要求的行；Filter可为寻找存活行消费更多child，不预取下一存活行 |
| 本地关闭时远端仍计算 | 保持当前未知终态规则；本地取消不等于远端取消，不自动重试 |
| INSERT失败 | 无部分提交；已经发生的模型调用不回滚；后续独立查询可重新执行 |

借用的透传值不复制成每个算子一整份行；新结果和规范消息按需分配。目标局部内存为
`O(sum(存活节点的当前输入/消息/完成上限) + 当前child/scan slot)`，不随已处理行数累计；
PG其它执行节点内存、gateway/模型和历史观测分别计量，不将该式当作总RSS上限。
每次pull前确认上一行的借用已结束，每行错误路径清理scratch。节点数增加不暗中扩大模型请求容量。

A1全部保持66887463的SQL、wire/digest、错误、NULL及默认值。A2a只有矩阵内组合由原拒绝变为支持；
其它拒绝继续保留。新carrier只有新路径产生，旧prepared/计划复制检查继续运行；不承诺运行中热替换
共享库或跨版本执行旧私有内存。部署验证使用新隔离实例，不把升级操作隐含在本设计中。

## 6. 可判定验收与实施拆分

下面F/M是已注册Filter/生成Map调用的简写，instruction/options使用各自已定义的固定合法值。
确定性表`(id, decision, body)`为`(1,TRUE,'alpha'), (2,FALSE,'bad'), (3,TRUE,NULL), (4,UNKNOWN,'bad')`；
Filter fixture按decision返回指定判断，Map fixture把alpha映为`A`。这些是task-digest绑定的fixture，不是
自然语言质量测试。recording Filter另用其合法小写输入；测试适配输入，不修改旧parser。

| 用例 | 明确预期 |
|---|---|
| `SELECT id, body, M(body) ... WHERE F(decision)` | 无序结果为(1,alpha,A)、(3,NULL,NULL)；Filter4请求，Map1请求；body未覆盖 |
| `SELECT id ... M(CASE WHEN id=2 THEN (1/(id-id))::text ELSE body END)`同Filter | 仍成功；第2行Map输入从未求值；其它输入相同 |
| 同一输入既是普通输出又是Map输入 | 普通列/原输入保持，结果写独立列；常量输入也不能混同 |
| `LIMIT 0` | 两节点零任务和零provider连接 |
| 所有输入TRUE/非NULL的fixture加LIMIT 1 | 结果一行；Filter、Map各1任务，无下一行预取；不假定返回哪个id |
| 全TRUE输入，`SELECT tick(body), M(tick(body)) ... OFFSET 3 LIMIT 1`，tick为计数后返回原值的VOLATILE函数 | Filter4请求、Map1请求；普通输出tick按PG18.3基线求值4次，Map输入tick在OFFSET/LIMIT后1次，共5次；不合并两个出现 |
| 新Map经过setrefs、generic prepared再撤权 | 最终表达式形状留证，marker EXECUTE撤销仍报42501，object hook每个出现一次，错误前零模型请求 |
| Filter全部FALSE/UNKNOWN/NULL | Map零任务；NULL按原规则零Filter请求 |
| Map中途错误的INSERT | 目标表新增0行，两个节点关闭；下一次合法查询成功 |
| generic prepared反复执行、撤销函数权限、RLS隐藏bad行 | 每次运行状态独立；撤权42501；隐藏行无模型请求；不靠EXPLAIN摘要替代调用审计 |
| 两Filter旧路径、单Map/Filter、v2–v5 | 原合同回归通过；新增内部carrier缺字段/越界列在provider打开前拒绝 |
| 三调用、OR、嵌套Map、排序等不在本切片范围 | 规划期稳定拒绝；fixture不收到任何任务 |

A1先完成共同描述/legacy适配与绑定单测；A2a先用最小PG原型验证权限与FINAL投影，
再接新carrier/pump并放开一个Filter→一个Map。原型使用公开合成fixture，不需要模型运行。
每步分别保存失败反例、实际计划与请求记录；共享路径最终跑完整PG18.3回归/TAP及相关Python/C合同。
B2/Core、真实质量、正式资源不是A2a的前置，也不由此获得新证据。实际实现及逐项证据以§11与其结果记录为准。

若final-stage输入投影或setrefs身份在指定形状下不能稳定表达，记录最小SQL/Plan/版本反例并暂停该形状；
不能由实现者悄悄变更求值位置、复用输入结果或加入外部SQL重写。实现不确定性需要这种实际验证，
不影响本文件已经选定的职责、绑定方向和兼容策略。

## 7. 补充审查的依据与实施判断

补充资料提出setrefs可能将marker匹配为INDEX_VAR，导致期望的FuncExpr权限初始化消失；这是
待复现的具体风险，不能写成已发现现有路径漏洞。A2a选择显式PG权限入口，仍须保留表达式依赖、
函数身份重验证及撤权测试；不能仅保留一个私有OID而丢掉计划依赖。
公开依据为[CustomScan计划表达式职责](https://www.postgresql.org/docs/18/custom-scan-plan.html)、
[投影能力标志](https://www.postgresql.org/docs/18/custom-scan-path.html)与
[SELECT输出求值规则](https://www.postgresql.org/docs/18/sql-select.html#SQL-SELECT-LIST)。
实现时核对REL_18_3的set_customscan_references、fix_upper_expr_mutator、ExecInitFunc和
create_projection_path；官方接口说明与补充资料不替代锁定版本的小原型。

## 8. A1首个实现步骤：分离Map调用分析

实施基线为main `b4b93b2e`，工作分支`codex/semantic-call-binding`。本步只把Map调用/来源分析
提取到`planner/sem_map_call.{c,h}`，对应已有`sem_filter_call`；现有marker计数与身份解析继续复用。
路径/Plan构造、spec、pump、权限、SQL与wire不变，查询借用指针只在原有规划阶段使用。
后续再引入共同SemanticCall和V1 binding；本步不生成尚无消费者的字段、空类或通用builder。

来源核对：2026-09-07只读查看用户参考副本`4601bf7`的`sem_map_op.c:sem_map_validate_query`，
该副本有未提交改动，观察到目标列表检查和单调用限制；保留自有成员OID/原始参数检查、分阶段
调用分析，不复制公司实现。参考只作职责比较，不作行为真值或向服务器上传的材料。
这一步不涉及模型API/客户端或批接口，pgml固定来源表保持原记录，不重复读取无关实现。

可验证目标：移动的函数体保持等价，只有内部collector命名改变；所有既有115项Python/C合同和
PG18.3严格编译、regression1项、8个TAP共1808项仍通过。fixture已覆盖来源、常量、撤权、
NULL/LIMIT、复制计划及双Filter；无新行为时不添加同义断言。服务器使用新数据目录、独立PG前缀
和合成fixture，先core preflight；任一失败保留输出并停止完成声明，模型请求预算为0。
该步骤不完成A1全部绑定，不开放Filter→Map，不替代A2a权限/投影原型或B1/B2动态验证。
实际验证：本地115项、Linux138项、PG18.3 regression1项及TAP1808项全部通过，模型请求0；
首次本地沙箱端口拒绝导致16项setup错误，按相同测试重跑通过，原失败保留在上述验证记录。

## 9. A1共同调用与tuple绑定实施

基线为`11e89b08`。两个collector统一使用SemloomSemanticCall，按规划层级/placement/出现序号
标识调用，各自持有marker副本。函数OID、input及结果type/typmod/collation从同一marker读取，
不再复制一套可互相矛盾的字段；算子固定参数仍归各自语义校验。来源列表位置仅在对应阶段解释。

semantic_binding集中校验列范围、目的列唯一、完整覆盖、类型/typmod/collation及dropped列。
旧pump通过legacy适配保持同列数/覆盖输入；pump改为从child读input，按独立result位置写结果。
新绑定内部Node表示选为`[input_resno, result_resno或0, [[child_resno,scan_resno],...]]`，
外层carrier版本仍归A2a。类型描述以child/scan的PG TupleDesc为事实，不在绑定中复制类型OID。
通用列绑定只计列与类型；后续新增的projected Map适配单独检查text结果，不收紧通用接口。

新增test-only PG caller直接调用生产绑定接口，验证copyObject/源内存释放、重排、独立结果、
二进制载荷、NULL、legacy与错误输入。既有PG全套和相关Python/C必须继续通过，错误不放宽。
沿用§8的工程参照判断；本步采用自有PG Node/TupleDesc/slot接口，不复制参考系统实现。
服务器core preflight后使用新隔离前缀/数据目录，fixture-only、零模型请求；失败轮次单独保留。
共同调用/绑定通过后再验证A2a投影与权限；该顺序不把基础单测成功当作已经开放组合。

## 10. PG18.3前置原型的实际发现

2026-09-07受控8行诊断：普通`SELECT tick(body), tick(body) OFFSET 3 LIMIT 1`的计数为8；
旧单Map路径中普通tick为4、返回NULL的Map输入tick为4，Map为NULL不发模型请求。说明OFFSET
跳过行仍可触发普通输出求值，不能把资料中“两个tick总共2次”当作PG18.3基线。第一个Filter
诊断没有启动所需recording provider而连接失败；随后显式LOAD并启动recording gateway，
重复诊断确认单Filter后的两个普通tick也是8次，原失败保留。

因此A2a继续选择让Map输入在OFFSET/LIMIT之后求值，这是该语义载体的显式选型；普通输出
表达式则保持PG对应查询的实际位置和次数，不能一起搬到上方。相同VOLATILE函数在普通输出
和Map输入的两个出现不能因结果列匹配而复用。将用分别计数的函数与最终Plan验证这两个条件。

新增生产set_plan_references的test-only调用验证：独立结果位于scan列2时，targetlist与
custom_exprs中的相同marker均被改写为INDEX_VAR列2，同时保留函数依赖。因此新Map使用显式
PG ACL/object hook的决定得到直接机制依据；这项检查不是新组合路径的实际执行/撤权资格。
共同调用/tuple绑定的全回归及这些原型在各自结果记录中保存；A2a外层carrier和组合仍待实现。

本轮最终验证：本地115项、Linux138项、PG18.3回归1项及9个TAP共1848项通过，605项非Markdown源码哈希一致。
完整结果见[绑定验证](../results/postgresql/semantic_binding_20260907/README.md)；模型请求0，测试服务均停止。

## 11. A2a载体接入的具体选择

在19326609基础上接入一个Filter→一个生成Map。独立Result投影可能把相同VOLATILE输入匹配到
已计算的普通输出，因此选择由Map CustomScan持有一个PG原生ExprState，在取得LIMIT/OFFSET
之后的一行时求值；不自行解释SQL表达式，也不把整行交给外部服务。输入表达式放custom_exprs，
原始依赖Vars透传；已计算的普通非Var输出在Map scan描述中使用其INDEX_VAR列身份，避免新输入
与它做整表达式匹配。普通输出继续在原计划的位置计算，Map输出追加独立列。占位NULL只替代
本Map在child计划中的未执行marker，不能成为用户结果。仅修改选中候选新生成的Plan，不修改共享Path。

由此将A2a的绑定确定为**表达式输入**，不再强制物化child输入列：carrier_version=1包含且仅包含
call_key、function_oid、semantic_fields、binding四个命名字段及版本字段；binding为[result_resno,
passthrough映射]，custom_exprs恰好一个text输入表达式。函数OID用PG OID Const表示，语义字段
使用独立于旧列绑定的公共decoder。legacy载体仍走原格式，Filter成本仍只属于legacy Filter字段。
共同tuple绑定中input_column=0只表示这个显式表达式模式，不能作为普通列输入解码成功；不生成
虚构的输入列号，也不改语义摘要或wire。新Map显式检查PG marker权限，input ExprState原生检查自身函数权限。

首批测试使用现有Filter fixture加独立Map向量，分别验证recording/exact/choice Filter、原输入保留、
NULL、丢弃行不求值、LIMIT/OFFSET、prepared、INSERT失败回滚和拒绝范围。先验证数据绑定，再扩展
实际撤权、RLS、snapshot和取消。每轮测试保存独立源码清单和失败；没有完整通过前不声明组合资格。

实际结果：四轮PG测试分别1875、1889、1908、1910项通过，全部回归通过；
最后45项组合测试及19项carrier测试覆盖上述行为，详见[结果记录](../results/postgresql/filter_map_binding_20260907/README.md)。

## 12. 2026-09-08真实组合小规模验证

在用户明确允许真实模型测试后，计划以当前已提交代码和独立PG18.3实例验证同步Filter→Map。
使用已缓存Qwen2.5-7B-Instruct，先重新核对权重/配置/tokenizer哈希及实际vLLM版本，单GPU、
模型长4096、max_num_seqs=4、batched_tokens=4096、BF16、eager、关闭prefix cache；只监听localhost。
生成参数保持Filter temperature0/max_tokens8，Map temperature0/max_tokens128。四条公开合成输入
含TRUE/FALSE判定、NULL Map输入及NULL Filter输入；每次3个Filter请求加1个Map请求。
v3 SELECT、v4 SELECT、v4 INSERT各4次，总预算12次，由现有持久AttemptLedger限制；GET健康检查
不生成内容，无额外warmup。LIMIT0及NULL Filter查询必须零请求；INSERT用独立审计连接核对。
核对计划中两个算子实际计数、完整结果/NULL、响应模型、原始生成内容与数据库结果一致及token usage。
任一失败停止，不自动重试或扩大预算；每次尝试单独目录保留。结束核对数据库/gateway/model退出、
模型端口关闭及GPU回到空闲。本项不验证新session真实Backend、性能、质量或正式资源资格。

实际结果：上述12次请求全部通过；[真实组合记录](../results/postgresql/filter_map_real_20260908/README.md)
保存三阶段结果、独立写入审计、请求账本及清理。没有借此声称新session的PG桥接已完成。
