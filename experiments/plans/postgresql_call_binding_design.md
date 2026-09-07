# PG调用与结果绑定：近期详细设计

更新日期：2026-09-07
状态：`current / design-specified / implementation-pending`
受众：PG扩展实施者。所属工作为[主设计A1–A2](postgresql_ai_semantic_operator_architecture_20260827.md#implementation-sequence)。
本文件唯一定义近期调用/绑定的实现方案；总体分工、远期SQL与研究方向仍由主设计拥有。

代码依据为`66887463`，主设计依据为`440f7cea`及本次修订。已完成源码复核和本设计，尚未实现下列
新对象、carrier格式或Filter→Map；没有运行PG/模型实验。下列名称为拟议内部接口，不是已存在符号。

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

拟在`planner/semantic_call.{h,c}`提供内部`SemanticCall`值与校验；`sem_filter_call`和Map收集器消费它。
不建立全局registry；对象由对应PlannerInfo使用的规划内存拥有。

| 字段 | 定义 |
|---|---|
| `call_key` | 当前规划上下文内的(query_level, placement_kind, occurrence_ordinal)，不是持久/跨进程ID |
| `function_oid`, `operator_kind` | 通过扩展成员身份校验的函数与本算子种类 |
| `placement_kind` | BASE_FILTER或FINAL_MAP；此切片不引入任意层级搜索 |
| `input_expr`, `marker_expr` | 当前阶段的PG Expr，按调用出现复制，不以equal或指针相同合并出现 |
| `source_locator` | Filter restriction列表位置或Map TargetEntry.resno；只在该规划阶段解释 |
| `semantic_fields` | 已经由算子验证的参数/语义字段，复用现有spec builder |
| `result_type`, `typmod`, `collation` | 交给PG绑定/结果投影的类型描述 |

Filter与Map的key有不同placement命名空间；path候选复制同一描述保留key。key只辅助节点关联/校验，
不充当PG Var/列号，不加入语义摘要。不同规划调用或prepared重规划无需得到相同key。
A2a没有Map数据依赖另一语义值，故不先增加通用dependencies列表；Filter→Map的关系依赖由child树表达。

不得把PG改写前的指针/列表序号拿到改写后当同一对象。原SQL检查只判断来源/位置许可；共同描述在
相应hook已规范化的表达式处建立，复制进入Path/Plan后不再反向检索原Query以猜输入位置。

### 3.2 绑定值与Plan存储

拟在`planner/semantic_binding.{h,c}`集中绑定构造、严格解码与校验；执行层消费解码值，不重算列位置。

```text
SemanticBindingV1:
  input_resno: child中本次算子输入（1-based，类型为text）
  passthrough: [(child_resno, scan_resno), ...]
  result_resno: Filter为空；生成Map为独立新增scan列
  result_type/typmod/collation
```

A2a Map结果列追加在child列之后，不能覆盖输入列；最终TargetEntry用现有PG投影只输出用户要求的列。
Filter只透传存活行，不新增语义布尔列。临时输入列可作为resjunk保留在内部，不泄漏到SELECT/INSERT结果。
透传目的位置唯一、范围合法；结果位置不与输入/透传混同；所有scan列必须有定义，不允许未初始化slot。

A2a首先仅在新组合的Map节点使用V1，Filter child和现有单算子仍保留legacy。新Map的`custom_private`采用可复制PG Node构成的带版本命名envelope：`carrier_version=1`、`call_key`、
`semantic_fields`、`binding`及可选`cost_fields`，字段缺失/重复/未知或类型错误均拒绝。
这是**PG内部carrier格式**，不改变语义schema或wire。语义解码复用原字段解码器；如需旧`[fields,input]`
视图，在一个适配函数中由binding.input_resno构造，不重复存两个权威输入位置。
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
   新child target只承载原始依赖和普通输出，去除本Map拥有的marker计算；不能沿用旧的递归
   marker→完整input替换，把Map输入函数留在LIMIT下。必要的内部占位列使用typed NULL而不求值，
   它不是SQL语义结果，不能被最终投影引用。由V1绑定恢复最终marker到新结果列的对应。
   在紧邻Map且位于LIMIT之上的projection计算Map输入，生成独立结果绑定。若PG将投影下推跨越Filter/LIMIT，
   用当前已有projection capability控制避免该转换；无法维持的路径在规划时拒绝，禁止改写最终executor补救。
4. Plan callback从选中Path描述编码binding/spec，不再按表达式相等扫描所有结果列猜归属。
5. Exec初始化先校验新carrier/类型、函数身份与权限，创建节点私有方法/runtime，再初始化child；
   无任务时不连接provider。新carrier走PG原生Expr初始化检查；legacy Map手动检查保持，避免同一节点重复Invoke hook。
6. 下游Map拉取一行 → Filter驱动ordinary child直到存活行 → LIMIT/OFFSET处理 → Map输入投影求值。
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
| Filter全部FALSE/UNKNOWN/NULL | Map零任务；NULL按原规则零Filter请求 |
| Map中途错误的INSERT | 目标表新增0行，两个节点关闭；下一次合法查询成功 |
| generic prepared反复执行、撤销函数权限、RLS隐藏bad行 | 每次运行状态独立；撤权42501；隐藏行无模型请求；不靠EXPLAIN摘要替代调用审计 |
| 两Filter旧路径、单Map/Filter、v2–v5 | 原合同回归通过；新增内部carrier缺字段/越界列在provider打开前拒绝 |
| 三调用、OR、嵌套Map、排序等不在本切片范围 | 规划期稳定拒绝；fixture不收到任何任务 |

A1先完成共同描述/legacy适配与绑定单测；A2a先接新carrier/pump，再放开一个Filter→一个Map。
每步分别保存失败反例、实际计划与请求记录；共享路径最终跑完整PG18.3回归/TAP及相关Python/C合同。
B2/Core、真实质量、正式资源不是A2a的前置，也不由此获得新证据。本轮没有给上述待实现用例写“通过”。

若final-stage输入投影或setrefs身份在指定形状下不能稳定表达，记录最小SQL/Plan/版本反例并暂停该形状；
不能由实现者悄悄变更求值位置、复用输入结果或加入外部SQL重写。实现不确定性需要这种实际验证，
不影响本文件已经选定的职责、绑定方向和兼容策略。
