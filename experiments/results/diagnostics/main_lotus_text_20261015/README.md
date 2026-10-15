# main LOTUS 文本接入资格

本批将原 LOTUS 文本方法和四臂 Movie 入口接入 main 底稿；实际模型质量、性能及最终组合资格仍待
统一 main SHA 上的实验。本页记录本批软件资格，不替换任何历史模型结果。

## 固定来源与必要依赖

底稿 `72f3f71523f8664547c705bf2bf008acdd6f4b19`，实现来源
`671a22dd4285319209396ece3572bb89ff5ab74e`。首个完整代码候选
`80187839e242971ace00adf1e17c2e4e0e681fd7` 的54个文件与 Linux 执行副本逐项 SHA-256 相同。
后续精简和对应复验在[机器摘要](checks.json)中独立记录，不将旧源码检查借给新组合。

| 依赖 | 必要性与范围 |
|---|---|
| 已发布 Core／native task／完整 HTTP 基础 | 保留取消、unknown、结果关联和资源责任；原统计修复不回退 |
| `semantic_methods/lotus` | 固定 SDK 源码检查、完整 uncached batch、响应重放、容量及显式两阶段库 API |
| Daft／Ray Data complete-call graph | vendor 自己拥有并行、调度和缓冲；原 SDK 继续解析完整结果 |
| 共享 Daft runner | 原生 Daft 后接 SemLoom payload 准备原会二次初始化；两个真实 Daft 用例修后通过 |
| Movie 数据／查询／evaluator 与持久 owner | 唯一原数据身份、发生项关联、Q1/Q2 LIMIT及Q3 COUNT、每查询独立预算和观测 |
| 网关与记录器 | 完整 HTTP、池复用但逐请求 cookie 隔离、先持久导出再确认回收、首错误与清理责任 |

按职责裁去通用 runner 的 Sema、DuckDB-AI、IMLane、Ray Core reference、局部诊断和两阶段查询入口；
`prepared_map`只保留实际消费者需要的配置合同。独审另要求删去无消费者的副本路由 hook。
没有合入整个 integration 祖先、历史模型产物、新 HSE 策略、Animals大输入或 PZ Stage2。
详细计时默认关闭，原 LOTUS 查询关闭重试／结果缓存；负收益 completion-order 不启用。

工程参照沿[架构 §8.7–8.8](../../../plans/系统架构.md#company-engineering-reference)：
有效请求只有一个来源；原方法拥有 prompt/parser，执行 adapter 只换完整请求的执行 owner；
连接寿命与执行额度分开，错误与取消不伪装成合法空结果。本切片依据公开 LOTUS／vendor API 与
固定自有源码，没有重新访问或复制公司材料。

## 已执行与保留失败

环境、命令、逐阶段结果、源码摘要及原件恢复身份见[机器摘要](checks.json)。
本机原 SDK／HTTP／预算／清理／归档214项中191通过、23项显式 runtime 条件跳过；
另两个 Daft 组合用例从失败变为通过。Linux预检`core,text,semantic-benchmarks`通过，nvcc为可选缺项。
Linux原数据预检与实际公开`--data-preflight`通过：固定 SemBench `c814e380`、2000行、
原 CSV／manifest／查询／evaluator 摘要及全部四臂声明一致，预检模型调用0，未创建执行输出。

| Linux检查 | 实际结果与范围 |
|---|---|
| 单元／SDK／HTTP／预算／清理 | 220项中213通过、7项native graph条件跳过；对应真实图另行运行 |
| 原数据准备、调度、执行层旧路径 | 63通过；432通过；192项中179通过、13项PG图像／Ray条件跳过 |
| 实际Daft／Ray Data HTTP图与原LOTUS SDK | 12项与8项通过，完整响应、错误、取消及unknown责任不变 |
| 实际SemLoom Ray与四臂同driver组合 | 各1项通过，单Map与显式增量TwoMap、完整请求值和清理核对 |
| 原Movie软件关系与原evaluator | 四臂各Q1/Q2/Q3，共12个成功查询、80次fixture POST；原关系／分类审计按发生项核对 |
| 持久HTTP与完整查询错误路径 | 三个replacement臂分别2查询复用；三个原／native臂的HTTP解析和两个native错误对照通过 |
| 独审精简后的复验 | `57945e80`全部54文件与Linux逐项相同，定向64项及实际SemLoom原Movie三查询通过；ruff通过 |

所有模型响应均来自有限 localhost fixture；Ray进程由本次测试自己创建、配置零GPU并自行关闭，
没有加入其他owner的Ray cluster。以上计数有重叠，不汇总成独立样本数。
原件运行身份为`main-convergence-20261015/lotus-text`，单一归档SHA-256为
`97b7fbffbc3ba936c59c1193da4799e87900351802eec97e1dd2c541bd97d778`（650,466字节），
已在服务器与本机独立核对；按运行身份和摘要向项目artifact owner取回`raw-qualification.tar.gz`。
归档保存原失败、控制器、环境／原数据预检、源码清单、原始fixture和每阶段日志，不需要重跑旧模型恢复证据。

保留了范围精简后的初次参数／import错误、首次完整SDK检查，以及两个控制器错误：
native graph测试缺少Ray导入时并发声明；Core／LOTUS测试完成计算后，外部控制器把观测目录写成文件路径。
对应修订只改外部测试配置，生产源码未借机放宽检查；首次输出不覆盖。

## 资格边界

软件 fixture 覆盖的是 SDK／执行图／Core 组合、完整请求与响应、原 evaluator 调用、关系结果和生命周期。
不据此声称 Movie 原数据的真实模型质量、GPU收益、PG接入、图像检索、Animals VLM或PZ完成。
固定 SemBench 的上游LOTUS1.1.3与选用1.2.4分别记录，不能称整个上游环境原样复现。
显式nullable Movie聚合deadline依赖调用方的有限故障／取消／清理监督；本CLI不隐式安装监督器。
字节合同不证明总RSS有界，单个数据／模型／服务配置上的旧结果不升级为新main资格。

公开 API、四臂 owner 和容量以[LOTUS模块](../../../../code/src/semantic_methods/lotus/README.md)为准。
后续成熟接点完成后，另固定同一个 main SHA、依赖、数据和配置做整合测试；本批基础不是最终比较结论。
