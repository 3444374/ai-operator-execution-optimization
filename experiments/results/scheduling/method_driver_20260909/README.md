# 有界多行方法驱动验证

状态：受控与真实模型检查通过。基于main 96b1f169，实现提交f7de79c3已快进合入main。
本记录对应[实施计划](../../../plans/bounded_method_driver.md)，用于方法执行正确性，不用于质量或吞吐排名。

## 目的与实现

复用MethodRun与共享SessionEngine，增加MethodDriver及服务级MethodBudgetPool。方法驱动独占一个
新session，方法自行决定零/一/两阶段请求；驱动负责行/调用/阶段身份、最终结果及每行最坏payload
预留。所有driver固定配额合计受一个池限制；最终结果交付后仍计费，消费者release后才允许新行。
这是payload计费模型，不是Python RSS测量或GPU内存管理；可信回调的临时分配与隐藏状态不受此接口
自动计量。方法失败诊断仅保留有界类型名，不保存任意长异常文本。

TaskInfo使方法任务实际进入现有WorkWindowOrganizer；默认工作量为方法声明值，可注入describe_work
提供已有WorkDescriptor。没有新建调度器、模型客户端、PG协议或gateway。直接任务路径保持可用。

## 设置与验证

- Linux完整调度回归361项通过；最终方法/续体/session专项55项通过（与完整回归有重叠，不相加）。
- 新增12项driver测试：零/一/两阶段、重排身份、单槽、慢消费者、固定共池、回调/后端错误、迟到完成、
  长流、EOF、组织路径与诊断大小。原单行方法及session测试保留。
- 本机完整发现因缺pyarrow有1个模块加载失败，不能称本机完整套件通过。Linux已有依赖完成补验。
- 首轮模型检查：0 POST，因驱动缺少组织器所需TaskInfo失败；模型已启动后正常清理，证据保留。
- 第二轮：同一个Engine、两个Job/driver、8行合成输入，共10次真实POST通过。每driver最多2行，
  服务共享4行预留，执行并发上限2；零/一/两阶段各自产生0/1/2次完整HTTP请求。
- 最终两阶段小检查：1行、2次POST通过。整个切片累计12次POST，不重试、不放宽输出断言。

模型Qwen2.5-7B-Instruct，revision a09a35458c702b33eeacc393d103063234e8bc28，9个模型文件在每轮前
核对。单vLLM实例，BF16、TP1、max_model_len4096、max_num_seqs4、FCFS、eager、关闭prefix caching。
执行链为独立producer → 方法driver → 同一session/组织器 → 有界异步HTTP → 真实vLLM；模型阶段为
严格复制TRUE，检查文本完全相等、模型身份、stop与usage。无数据组织消融、性能对照或自然语言质量结论。

## 观察及不能声称的内容

真实测试验证接入及多阶段资源交接；受控测试覆盖无法可靠用真实模型制造的乱序/失败/迟到时序。
一个driver等待消费不会使用另一个driver已获固定份额。EOF只有在方法不再产生任务时才seal。
取消仅清理方法自有数据，未知远端责任继续由Engine持有至权威完成。不是即时GPU取消保证。

PG方法桥接、跨行校准、并行分叉、LOTUS方法迁移与动态预算借用均未实现。已有PG Filter→Map组合仍
属于PG节点组合，不是本驱动的多阶段方法。ShareGPT真实数据SQL切片已确定选样、评价和静态对照，
本轮未运行；后续从该切片取得实际负载与质量观察，再决定算法和调度实验。

## 原始记录与清理

[首次失败](raw/method-real1-summary.json)、[失败时驱动源码](raw/before-driver.py.gz)、
[失败日志](raw/method-real1-driver.log.gz)保留组织元数据缺失的实际路径。
[双Job运行](raw/method-real2-summary.json)峰值4个活动行、101504 bytes payload预留，达到设定上限；
[最终两阶段运行](raw/method-final-real-summary.json)峰值1行、25376 bytes预留。两轮结束时方法配额、
核心held_tasks/input/result/active_requests/active_work均为0，backend正常关闭。
这些数值来自固定行预留公式，不是进程RSS观测。

[最终专项](raw/method-postreview.log.gz)、[完整调度测试](raw/scheduling-final.log.gz)、
[最终源码清单](raw/method-final-real-source.json)及[运行后核对](raw/postflight.json)记录版本和回收。
服务器最终669份源码/配置/文档与运行清单一致，后续仅更新说明文档；实际实现和测试未再改变。
[10次请求账本](raw/method-real2-ledger.jsonl.gz)与[2次请求账本](raw/method-final-real-ledger.jsonl.gz)
独立计数。首次ruff导入/格式检查失败和本机缺依赖也有记录，没有隐去失败后重跑。

三轮模型均停止并关闭端口；最终无本轮模型/driver/Raylet残留，两卡各1 MiB。没有启动PG数据库，
因此本轮不新增PG回归或SQL方法桥接的证据。raw文件在脱敏后压缩，真实runtime配置和凭据不归档；
[归档校验](raw/SHA256SUMS.json)用于逐文件核对。

## 主干集成

按用户授权，main从96b1f169快进合入f7de79c3，无冲突、无实现改写。集成时656份实现/测试/配置
与最终模型验证清单一致，44份原始材料校验通过；随后仅更新集成状态文档，没有重复启动模型。
验证范围仍为上述受控检查和12次真实POST，不新增PG方法桥接或真实数据质量/性能结论。
