# 查询级Job归属与生命周期验证

状态：实现、受控验证和真实模型复测完成；主干尚未合并。
基线`c2bf4c5e`，开发分支`codex/query-job-attribution`。

## 目的与范围

验证同一次PG查询执行的普通Filter/Map流确实进入同一Engine Job并共用预算；
另一个查询拥有不同Job。对应外部多Job执行的数据库接入基础，不是调度算法或性能实验。
[设计与请求预算](../../../plans/postgresql_query_job_design.md)给出范围、来源和停止条件。

PG继续拥有SQL、表达式和事务；新增query-job模式通过Linux内核peer核验和活控制连接登记归属。
Job预算由外部执行层分割，Filter保持逐项等待，Map窗口1。旧同步路径及独立增量Map保留。
choice Filter、重扫、任意SQL组合、方法状态总预算、动态借用和多服务模型路由不在本次完成范围。

## 实现及失败记录

- 共用非阻塞UDS连接与FD释放；QueryJobOwner利用查询内存与ResourceOwner清理。
- 网关将Job登记与session加入分开，复用一个控制循环；流正常结束保留Job，查询结束才取消组。
- 已登记查询的控制连接和声明流需要连接预算，Job存储份额能为每个流预留一个最大请求/结果。
- 最后复核补充runtime关闭幂等性：实际C函数的受控调用在修复前重复计数，修复后每个流只结束一次。
  第二轮24次通过后发现该问题；补检查并以独立第三轮24次复核最终代码，同时校正完成返回类型标注。
- 两项原静态检查仍在旧文件内寻找建连实现，迁移到共享连接模块；没有降低非阻塞和层次检查。
- 首轮新增TAP在预期INSERT错误处由psql遇错退出；第二轮保留的stderr导致后续query_safe误判。
  修正测试脚本，显式断言预期错误后清理测试客户端stderr；没有修改SQL错误行为。两轮失败记录保留。

## 验证结果

| 检查 | 结果 |
|---|---|
| Linux调度/提供者/PG Python/观测辅助 | 350 / 46 / 116 / 5项通过，共517项 |
| PG18.3回归与完整TAP | 1 / 2005项通过；新增查询专项42项 |
| 源码/测试/配置清单 | 652份哈希与服务器一致 |
| 真实模型复测 | 24/24次POST，12个查询Job；15次Filter、9次Map完成 |
| 首轮真实模型 | 4次POST后失败，非空条件返回UNKNOWN；与复测分开保留 |

受控测试覆盖错误peer、错误/重复流、不足预算、先后连接不改变Job、取消后未确认远端资源保留、
其它Job继续推进。PG专项覆盖双Filter、Filter→Map、prepared重复执行、同backend两个游标、
子事务错误后外层游标继续、backend断连、NULL/LIMIT0/EXPLAIN零工作。

真实复测核对了11个双流Job和1个单流Job，相同Filter payload仍有两个独立流身份。
全部查询Job结束时held_tasks和active_requests为零；模型/网关/PG自有进程与Raylet均清理，
模型监听端口关闭，GPU回到两卡各1 MiB。代码与测试652份哈希在运行前后均一致。

### 首次真实运行为何失败

第二个Filter的条件是“The input text is nonempty.”，输入为TRUE，模型返回UNKNOWN。
PG按原语义没有保留该行，LIMIT继续读取下一行，最终4次POST后预期输出断言失败。
发送的prompt和任务关联均与计划一致，没有把UNKNOWN重写为TRUE或放宽解析器。

复测将双Filter限定为两个相同的“The input text equals TRUE.”调用：先检查计划含两个节点，
再验证相同payload未混淆流身份。其余生命周期场景及严格输出断言保持；新目录、新24次账本；关闭修复前后两轮各24次通过，分别为intermediate与final记录。
因此只能说修订后的工程用例通过，不能说非空条件判断错误已解决，更不能把累计52次称作全部通过。

## 设置、来源与不能声称

真实模型由独立控制器启动，沿用已验证Qwen2.5-7B-Instruct与vLLM服务参数，运行前重新核对9份
模型文件和扩展哈希。合成输入为TRUE；要求明确预期输出、固定24次请求账本及完成后清理。
没有性能对照/消融，不报告吞吐、公平性、近似语义质量或GPU显存管理结论。该结果不能代表任意
SQL形状或所有部署都可直接使用query-job；其它平台的peer校验尚未实现。

## 下一步

在同一核心内补方法状态总预算；更大窗口、choice Filter和多服务路由分别扩展与验证。

## 原始材料

- [关闭幂等性修复前](raw/close-before.log.gz)、[修复后](raw/close-after.log.gz)：直接执行C关闭函数，检查每个流只计一次。
- [最终模型汇总](raw/final-run-summary.json)、[控制器与清理](raw/final-prepare-controller-summary.json)、[失败运行](raw/initial-prepare-controller-summary.json)。
- [模型请求与执行事件](raw/final-run-gateway-events.jsonl.gz)、[独立请求账本](raw/final-prepare-ledger.jsonl.gz)；首轮对应initial前缀，均未覆盖。
- [完整PG检查](raw/pg-close.log.gz)、[新增查询专项](raw/pg-close-query.log.gz)；pg-first/second保留两次测试脚本失败，pg-third是修复后的专项。
- [652份源码清单](raw/query-job-final3-manifest.json)、[归档校验](raw/SHA256SUMS.json)、[运行后核对](raw/postflight.json)。
- raw中的日志、事件与验证脚本先脱敏后压缩；runtime settings、配置凭据和查询令牌不归档。
