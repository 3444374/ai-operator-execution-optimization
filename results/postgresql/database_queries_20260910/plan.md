> 本文件是该次验证的运行前声明/实施规格原文，2026-09-21 从当时的主题计划（`docs/plans/data_organization_batching.md`）按“执行说明与测试材料放在一起”的原则移入。其中请求额度、停止条件和“本轮/下一步”均为当时记录；未使用的旧额度不构成今天继续运行的授权。

### 工作包 B 的接入规格（已完成代码与受控验证）

本包已完成代码、受控查询验证和真实Movie原始数据导入；真实模型比较待新额度。交付后曾暂停；用户随后授权继续C、D，E/F不自动启动。来源仍采用主架构§8.7–8.8已固定的公开接口，
保留自有PG生命周期、消息程序与清理；本轮没有读取、复制或上传公司材料。

- 原始输入：`QueryInputs`只读取顺序、执行行ID和context/question或movie/review/reviewId列；标签留在评价侧。
  一次安装的新表通过COPY往返SHA、唯一键和行/字节上限核对，并拒绝后续数据修改。PG-source direct
  在计时内打开只读repeatable-read事务、逐行读源并构造消息；完成未消费结果仍占用同一个C窗口。
- `sem_prefetch.c`白名单以PG18.3内置操作符/类型为依据，不重写qual；新WHERE预取默认关闭。
  LIMIT保守执行；SQuAD在SQL内拼接原始列仍按输入表达式回退，不能冒充已扩大该形状窗口。
- 源码发现`sem_filter_path.c`仍拒绝所有aggregate，Q3不能仅靠runner接入。新增显式开启的单表、
  单目标内置`COUNT(*)`，保持PG原有Aggregate→SemFilter→ordinary child；GROUP BY/HAVING、
  DISTINCT、aggregate FILTER、其他聚合和INSERT聚合仍不支持。旧默认能力不变。
- Filter逐行审计使用测试专用ID列：planner保留原始Var并登记PG列权限；先记录row-ID/sequence/payload，
  再记录PG实际保留/丢弃判断。COUNT输出不参与生成输入绑定。trace元数据不改变SemanticPlanSpec或wire。
  验证重复文本、NULL、RLS不可见行、无ID列权限、失败及LIMIT；错误不能被后续清理替换。
- SemBench固定`c814e3807e72d4cf876b852b17e77f3cc94575c2`，原始Movie Q3→Q1/Q2方法和evaluator不改写；
  原生LOTUS加载PG原始列，仍由其程序过滤、调用LM和取head(5)。本包选LOTUS1.2.4；上游requirements
  锁定1.1.3，故明确登记软件适配，不称完整上游环境复现。模型/prompt/parser和实际出站分别记录。
- Ray固定2.56.1，使用`read_sql`与`HttpRequestProcessorConfig`访问同一已部署endpoint；native
  SQL探测/COUNT、转换和消息构造在JCT内。记录请求的分片配置与实际执行图；小表单reader回退不称多分片。
  native reader按分片materialize的内存与LOTUS全表DataFrame均按系统实际行为报告。120行受控执行实际2个ReadSQL task，每个60行；规划单reader探测另计。
- 新共享POST计数只做预算/观测：一个单元只领取一次，每次worker出站前持久化计数，无退款；不含
  SemLoom调度策略。所有新查询臂采用同一种计数模式，持久化开销留在JCT内。重试与缓存显式关闭。
- Movie-derived Map使用独立二值情感指令，标签仍不出站；原COUNT指标外补FP/FN，LIMIT补重复执行行号、
  有效性、行数及额外工作。暂无真实模型新额度；当前受控HTTP结果不登记真实质量或性能。

本包[完整报告](../../results/postgresql/database_queries_20260910/README.md)保存版本、原生身份、失败及检查。
Movie v4原始数据由未修改的SemBench generator生成2000行；1865个原始reviewId的135次重复均保留，
执行行号独立，原始评价字段不改。CLI默认监督自有worker，取消先关闭后续POST，再清理自有进程；
远端模型结束未知时保持unknown。Ray原生异步批次配置固定为1，避免每actor默认4批导致实际HTTP超出C。
