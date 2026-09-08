# 多Job共享执行与网关复用验证

2026-09-08，内部工程验证。对应[多Job设计](../../../plans/semloom_multisession_design.md)，
源码基线`b6c341c9`加本次变更；最终644份非Markdown源码/测试/配置文件hash与最后两次真实运行一致；前两次通过结果保留各自manifest。
研究内容是PostgreSQL内置AI语义算子的外部分布式物理执行与调度优化；本轮只验证执行基础的
正确性与资源归属，不是调度性能、语义质量或GPU显存资格。

## 实现及复用

- 复用一个SessionEngine、任务账本、组织器、任务策略和异步backend。新增可信Job句柄，
  同一Job多流合计检查存储/执行额度；按Job轮转，再在Job内轮转流。
- Job资源策略由执行组装入口注入；默认静态份额，不借用其它Job的空闲存储。网关申请预算，
  Engine校验、计费和释放。受控非均分3/1份额验证无需修改协议收发。
- 单Job和多Job增量服务使用同一个控制循环；删除生产入口的专用单会话增量循环。
  同步Filter v3/v4、Map v5、recording/golden保留协议处理，共享连接接收、线程与socket清理工具。
  独立单流适配API保留兼容消费者，没有删除仍被引用的协议、同步调度器或历史证据。
- 每连接一个命令/结果槽，取消通知独立；结果lease直到发送完成或发送线程结束才归还。
  连接数和每连接5MiB逻辑序列化空间共同有界；这不是Python对象、socket内核缓存或RSS精确计费。
- 有身份的未知远端结果只失败所属流并保留额度；其他Job可用剩余容量。共享身份冲突隔离全池。
  PG C实现未修改；PG仍只拥有本地行缓冲、语义与查询生命周期，没有新增PG调度器。

## 验证

| 路径 | 结果及能说明的内容 |
|---|---|
| 本地受控 | 新增15项通过；完整provider套件40项中39通过/1平台跳过（与新增7项重叠） |
| Linux | 调度346、provider40、PG合同115、observer13、runtime helper1，共515项通过 |
| PG18.3 | 回归1/1，TAP1963/1963；原扩展二进制hash一致，无需改PG代码 |
| 单Job真实模型 | 独立12/12 POST，默认统一循环下窗口2；同步参照、SELECT/INSERT、拒绝写入、取消、恢复、LIMIT通过 |
| 多Job真实模型 | 独立18/18 POST，两PG查询共享Engine，不同Job真实HTTP同时在途，峰值2；结果与同步参照一致 |
| 多Job资源 | 14个增量任务提交/权威终态一一对应，9个Job归零；取消A返回57014，B完成；失败INSERT23514留下0行 |
| 清理与身份 | 644份源码一致，9项模型文件重哈希；测试模型/PG/gateway及Raylet均无残留，模型端口关闭，两GPU均回到1MiB |

受控测试还覆盖A两流/B一流不因流数增加获得双倍机会、A输入满/B推进、A真实阻塞发送/B推进、
关闭时迟到结果、伪造Job句柄、局部未知结果与共享身份错误。请求机会轮转不代表token或GPU时间公平。
PG既有窗口夹具也改用统一控制循环；逆序完成、65项输入窗口、volatile输入回退、取消、协议错误均通过。
真实链路是PG → v6 → 有界代理 → 共享Engine/数据组织 → async HTTP → 模型 → 原PG行。

模型为Qwen2.5-7B-Instruct，revision `a09a35458c702b33eeacc393d103063234e8bc28`，vLLM0.25.1；
单GPU、BF16、max-model-len4096、max-num-seqs4、max-num-batched-tokens4096、FCFS、
显存比例0.8、eager、关闭prefix cache。temperature0，普通输出最多128 tokens、取消任务256。
服务参数相同，多Job运行使用重新分配的localhost端口。源码、模型和服务身份记录见归档。

输入是四个公开合成短句及NULL；取消使用固定天文长输出指令。前后两组各单Job12次、多Job18次，总计60次请求；每组请求构成见
设计中的预定账本；没有模型请求重试。结果关联、独立INSERT回读、零调用控制及取消断言由
归档中的两份`real_check.py`执行，controller独立检查模型来源、账本上限和清理。
未做性能排名或算法消融；同步Map作为语义参照，受控不等份额策略作为可替换接口检查。

## 失败与修正记录

1. 初始lint发现一个未使用import，删除后通过，原日志保留。
2. 本地一次unittest模块路径写错，未运行产品测试；更正命令后执行。
3. 公共异步backend重构一度改变独立单流适配器的未知结果处理，回归捕获；改为显式区分
   单流兼容和登记Job服务模式，完整provider及真实默认单Job路径通过。
4. 第一次PG检查目录不归postgres用户写入，数据库未启动；改用已授权隔离目录，完整检查通过。
5. 多Job第一次controller在启动前发现本地端口无法绑定，未创建账本、未启动模型、POST为0。
   原目录保留；新目录重新预检并选空闲localhost端口，18次验证成功。未覆盖失败日志或放宽断言。

6. 最终复核修正同步accept等待期间的旧连接计数，补充回归；随后补跑捕获慢客户端关闭的
   间歇性排空失败。诊断记录显示任务账本已空，但重复cancel自唤醒使退出线程缺少推进机会。
   每连接仅中断一次后，原用例连续50次、完整provider40项通过；等待期限和断言未放宽。
   中间预检准备未启动模型；使用最终644份源码重新执行完整PG检查及12/18请求验证。
   先前成功模型记录、失败测试、三次诊断及修正前源文件均保留，累计60 POST，无模型请求重试。

### 代码问题与回归入口

| 问题与触发条件 | 修复位置及验证 |
|---|---|
| 独立单流backend遇到未知结果时，重构曾改变原来的全Engine隔离语义 | `incremental_execution.py`显式选择登记Job错误模式；`test_unknown_result_quarantines_engine`保留旧行为，多Job timeout用例验证局部失败和保留额度 |
| 同步accept等待时旧连接退出，等待前的计数可能误拒绝新连接 | `gateway_runtime.py`在accept后调用当前连接计数；`test_capacity_is_rechecked_after_blocked_accept`覆盖这个顺序 |
| 停止时每轮重新cancel，通知自身造成忙循环，慢发送/读帧线程可能来不及退出 | `multiplexed_gateway.py`的每连接中断仅执行一次；`test_slow_send_retains_lease_while_other_job_runs`原失败复现与修复后50次检查，保持原超时不变 |

源码回归入口：[网关生命周期测试](../../../../code/tests/execution_provider/test_gateway_sessions.py)、
[多会话隔离测试](../../../../code/tests/execution_provider/test_multisession_gateway.py)、
[单流兼容测试](../../../../code/tests/execution_provider/test_incremental_window_one.py)。
失败、原因、改动及回归结果保存在本报告；项目日志只保存摘要，不另建竞争的Bug状态文档。

## 复核入口与剩余工作

[摘要](summary.json)、[压缩证据](evidence.tar.gz)、[逐文件hash索引](evidence-index.json)。
110份原始日志/脚本/账本/身份材料先明文脱敏、秘密扫描，再以固定mtime压缩；无需展开大量结果文件。
解包到临时目录后按索引核对SHA256；`multijob-final2-manifest.json`对照最终仓库内容，较早manifest对应归档中的修正前源文件。主要复现命令：

```sh
PYTHONPATH=code python -m unittest discover -s code/tests/scheduling -t code
PYTHONPATH=code python -m unittest discover -s code/tests/execution_provider -t code
PYTHONPATH=code python -m unittest discover -s code/tests/postgres -t code
```

PG按平台runbook使用隔离PG18.3执行`make installcheck`；真实driver/controller使用仓库外runtime配置，
新建账本后运行，不复用这里的已消费账本，不在仓库保存服务器地址/凭据或模型路径。

仍待完成：PG同查询多算子可信Job归属、动态份额/借用、方法状态的总内存预算、GPU实际资源模型、
多成员物理请求与Filter异步接入。独立核心已支持同Job多流，但本轮PG每条连接仍登记一个独立Job。
后续数据执行策略接入现有执行层接口，不需要在PG插件重建调度器。
