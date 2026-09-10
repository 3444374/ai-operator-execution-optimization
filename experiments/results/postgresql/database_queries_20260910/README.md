# 数据库输入与公共查询接入验证

受众：内部工程与实验审计。对应[数据执行计划的工作包 B](../../../plans/data_organization_batching.md#工作包-b-的接入规格已完成代码与受控验证)。
研究对象是 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化；本轮补齐研究内容一、二
共同需要的数据库源、查询语义、原生参照和计量入口，没有测试新的组织或调度算法。

代码和受控验证完成，新增真实模型请求 **0**。本报告不提供真实语义质量、吞吐、强静态容量平台或
方法收益结论。新能力默认关闭；真实模型验证需要另行确定模型、服务、配置、重复与请求额度。
本包交付后暂停，不自动进入 PG 总量预算、组织、多 Job 借用或图像接入。

## 已实现的范围

| 路径 | 执行与语义所有者 | 本轮检查 |
|---|---|---|
| PG Map | PostgreSQL SemanticPlanSpec、ordinary child、incremental provider | 原始列、消息集合、独立 producer 绑定、结果和责任回收 |
| PG-source direct Map | PG 只读源；有界 Python direct 执行参照 | 读取/转换进入查询时间；慢首行时已完成结果继续占用 C 窗口；输入序/完成序与提前关闭 |
| Ray Data Map | Ray 2.56.1 `read_sql`→`HttpRequestProcessorConfig` | SQL 探测/COUNT、消息构造、原生批次/actor、实际出站和结果消费均在查询时间内 |
| 原始 Movie Q1/Q2/Q3 | SemBench 原始 LOTUS 方法、LOTUS1.2.4 prompt/parser | 原始过滤和 head(5)/COUNT；原 evaluator，另补行级判断、重复执行与额外工作审计 |
| PG Movie Q1/Q2/Q3 | PG Filter 语义与普通 PG Aggregate/LIMIT | 同任务的独立 PG 语义参照；逐行真假审计不从 COUNT 数值反推 |

PG-source direct 是执行参照，不称为第三方原生系统。Movie-derived Map 使用独立二值情感指令，
不混称为 SemBench 原始任务。Map 三臂核对完全相同的模型消息；PG Filter 与原生 LOTUS 使用各自
声明的 prompt/parser，分别评价质量，不能只按耗时给出等语义性能排名。

`semloom_pg.enable_predicate_prefetch` 默认 OFF。仅检查已有 SeqScan/Result 中白名单内置整数/布尔
比较、确定性 text 等值、NULL/布尔组合，不移动或改写谓词。LIMIT、易抛错表达式、domain 和
其他 plan 形状保守按需执行；SQuAD 的 SQL 字符串拼接仍按输入表达式回退。RLS 不可见行未出站；
security-barrier view 仍明确不支持，未因本包开放。

`semloom_pg.enable_filter_count` 默认 OFF。显式开启后只接入单表、单目标内置 `COUNT(*)`；
GROUP BY/HAVING、DISTINCT、aggregate FILTER、其他聚合及 INSERT 聚合仍拒绝。
Filter 行 ID trace 是测试专用设置，保留 Var 时登记列 SELECT 权限；数据库拥有最终保留/丢弃判断。

## 原生版本、资源与计时

- SemBench 固定提交 `c814e3807e72d4cf876b852b17e77f3cc94575c2`，调用原始 Q1/Q2/Q3 方法与 evaluator。
  上游 `requirements/lotus.txt` 为 LOTUS1.1.3，本轮为1.2.4，明确属于版本适配；没有声称复现全部上游环境。
- Ray2.56.1、Python3.12、pandas2.3.3、Arrow24、LOTUS1.2.4；额外 evaluator 依赖安装在独立 venv。
  旧 driver 和模型服务环境不变。环境检查使用 `core,text,semantic-benchmarks` 并保存前后报告。
- 受控查询固定 C=2、PG/window=4、核心输入128MiB、结果64MiB、PG窗口64MiB，Map最多128生成token，
  Filter最多8。原生 Ray 自身配置 `RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY=1`，每个 actor
  一批；actor/task 重试与重启为0。不能用 actor 数推断真实 HTTP 并发，必须核对事件。
- Ray120行最终原生统计为 **2 个 ReadSQL task，每个60行**。规划期间还发生单 reader 探测；日志中
  探测的 fallback 不代表最终数据读取只执行一片。两行源测试实际单片回退，分别保留证据。
- LOTUS 原生读取全量 DataFrame；Ray reader 物化各实际分片。报告其真实留存行为，不声称等同于
  direct 的 C 行流式留存。独立源表使多连接读取同一不可变数据；未声称跨任意可变表共享 PG snapshot。
- 所有新查询臂使用同一持久共享 POST 计数：一次领取，worker 发送前计数，不退款；计数开销留在查询时间。
  它不含 SemLoom 执行策略。已有容量工具的单进程计数模式保持兼容，两套计时不可直接混排。
- CLI 在独立进程中运行查询，分别限制准备、查询和评价/清理时间。超时先关闭该单元的后续 POST，
  再终止自有 worker 与按 PID/创建时间确认的子进程；不停止共享 Ray 集群。保留首个错误、部分结果、
  清理错误和 supervisor 报告。取消请求不等于远端模型计算已停止，异常时远端完成保持 unknown。

## 真实数据准备

来源：[Kaggle Clapper Movie Reviews v4](https://www.kaggle.com/datasets/andrezaza/clapper-massive-rotten-tomatoes-movies-and-reviews)，
官方卡片许可 CC0。保留源 archive、两个 CSV、失败下载断点和各次日志；完整 archive 为159286131字节，
SHA256 `92d95672cd39260fca12e6e64d814f7d324ad0600823e0d9207ed1fb52ead707`。

未使用仓库 demo CSV。对原始 v4 数据运行上述 SemBench 提交中未修改的
`src/scenario/movie/preparation/generate_data.py`，scale factor=2000；包括其原有 random_state=42
和换行替换。得到116个电影、2000条评论；`taken_3`120行，其中标签POSITIVE为14行；全表POSITIVE1485行。

上游输出有1865个不同 `reviewId`，135次重复出现均保留。新 source schema v2 用独立执行行号关联任务，
另存原始 `review_id`；原始 evaluator 仍读取原始 reviewId，逐行审计使用执行行号。
原评分的集合去重行为原样保留并单列重复原始 ID，不能据此判定重复执行。
首次把原始 reviewId 当唯一键的准备失败也保留，没有删行后重命名为原任务。

原始字段与标签分文件保存。PG 只包含 source_position、row_id、movie_id、review_text、review_id；
2000行 COPY→读回 SHA256 为 `88eceb0f234a54fd38e874b49e627406434716c6b4096c0925befa0b1213eade`，
唯一执行行号与不可修改触发器通过。准备清单指纹与具体检查见 [validation.json](validation.json)。
SQuAD 入口通过原始 context/question 和已有合格选择清单重建，不拆解格式化 prompt 反推原始字段；
本轮完整原生查询测试使用 Movie，真实 SQuAD 新 runner 比较仍待执行。

## 验证与保留的失败

| 检查 | 结果 |
|---|---|
| PG18.3 构建 | `-O2 -Werror` 构建、安装通过；未改 PostgreSQL core |
| PG regression | 1/1通过 |
| PG TAP | 完整17文件2101项中2100通过；一条事件断言误判晚到清理；修正后该文件28/28通过，其余16文件无变更 |
| PG Python 合同/协议 | Linux116/116通过 |
| 受影响 runner/预算/源/观测/超时 | 本地及Linux65/65通过 |
| 原始源集成 | 真实PG/direct/Ray及超时通过；schema新增列的旧断言修正后单项通过，首次结果保留 |
| 全查询集成 | 5个测试、多个子场景通过，实际1111次本地fixture POST；真实模型0次 |
| CLI 清理 | direct、LOTUS、独立Ray正常退出；故意挂起/忽略TERM/关闭预算失败的自有worker均被回收 |

最终120行合成输入包含重复原始reviewId，执行行号唯一。Map三臂每臂120次；Q3两臂各90次；
Q1/Q2 PG各13次得到5条通过行，原生LOTUS分别120/90次；另有两个预期失败与三个CLI子进程场景。
空选择两臂0次。通过的查询实际HTTP峰值≤2、最终0；逐行无错误分类，COUNT与实际保留行一致。
这些确定性响应只验证控制与关联，不是模型准确率。

失败及修复分开保留：首轮Ray目录权限错误、fixture遗漏SDK的`max_completion_tokens`别名；
第二轮发现Ray每actor默认4个异步批次导致HTTP峰值8，改用Ray原生配置后通过；PG测试整文件比较
将前一查询的`connection_closed/job_drained`误当新请求，改为检查连接建立、offer、task、模型提交事件。
未改放行阈值；重复源ID失败保留。各次fixture计数、清理状态和日志指纹在validation中，不将多次运行合成一次成功。

## 入口与后续

统一入口为 [database_queries.py](../../../../code/scripts/experiments/database_queries.py)，
支持 prepare-movie、prepare-squad、install、run；参数示例见[脚本说明](../../../../code/scripts/README.md#数据库原始输入与公共查询)。
完整请求、响应、原始数据、PG日志与失败材料只保存在仓库外；Git只保留本报告与计数/指纹。
专用PG正常停止，任务进程/模型服务均无残留，临时根目录ACL恢复，两张GPU各1MiB。
717份非Markdown代码文件与服务器验收版本逐文件SHA一致。完整私有证据归档130438815字节，
SHA256 `90306e25e1621672dd69791183c298113f9561eb354a89f16650a6cf5b2b08ba`；原数据archive另行保留。
实现提交为`4cefe84e`；随后只补充清理和验收指纹，717份执行代码保持不变。

下一次真实验证先单独确定新额度，使用已准备的不可变2000行源，在同一服务下分别核对三臂Map与
Movie Q3，再执行Q1/Q2。先评价各自的消息/解析语义与质量，再讨论耗时；本轮没有开启该运行。
PG总量预算、有限窗口组织、多Job计算借用和CLIP接入仍属于后续独立工作包。
