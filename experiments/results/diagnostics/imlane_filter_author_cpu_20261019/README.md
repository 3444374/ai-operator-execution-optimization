# IMLane 作者 CPython 的普通 Filter 无模型资格

本轮在集成 owner 放行的 CPU 窗口，以 `0190e8cd` 的公共源码闭包加显式解释器 fixture 修正，
完成固定作者 `072f8257` 的 Arrow16.1／CPython 实际检查。作者 DBEnd、Coordinator、Lane、
执行器和已有 fixture 补丁保持原字节，公共 CLI、共享 engine 和旧 HTTP 配置未修改。
[本机 CPU 记录](../imlane_filter_cpu_20261019/README.md)和[原 echo 记录](../imlane_records_echo_20261016/README.md)
保持各自来源；新结果不回填到旧运行。

通过范围是作者 CPython 的记录传输、原 LOTUS 方法、固定响应 CPU 子进程及资源责任。
真实模型请求与 GPU 请求均为 **0**；模型质量、GPU 生命周期、作者 Ray 后端和性能仍未验证。
机器可读结果、二进制及原件摘要见 [qualification.json](qualification.json)。协议与资源两份
独立审阅均核对本地归档、实际结果及来源后接受，没有重测或再次探测远端。

## 实际检查

| 环节 | 结果与边界 |
|---|---|
| 新 C++ 检查与桥 | 两个二进制编译成功；实际执行 9 个输出反例、future 单次消费、独立输出内存、v2 已知失败 usage 与版本协商检查 |
| 新 helper 的 7 个 echo scope | 健康 scope 含 4 块／7 行、2 块在途、同来源不同 callID、跨查询隔离、10 个输入拒绝与 3 次原生旧 Table 保留核验；六种故障均到达预期检查并停止错误 owner |
| 两查询普通 Q3 | 每查询独立 author owner／CPU engine；每查询两个单行块、最多两块在途；原 formatter → CPU 子进程 → 原 parser／COUNT → 原 SemBench evaluator 全程完成 |
| 两查询结果 | 各 2 个已确认成功终态、无缺失；各 usage 为 10／4，COUNT＝0；原 evaluator 的绝对与相对误差为 0，两条负例分类一致。这些是固定软件响应，不是模型准确率 |
| 四行 v2 定向 scope | 同一来源 `call_index=0/1` 的两个成功，随后已知失败，再成功；4 个终态全部入账，3 成功／1 失败／0 缺失，usage 20／8。原 consumer 只因第三条确切失败而报错；正常关闭完成 |
| 独立末次清点 | 覆盖全部 12 个 namespace、43 个 PID 出生身份；无活动、未知、僵尸元数据或 SHM 残留；正常 host lease 不存在，GPU 为 1 MiB、compute 列表为空 |

两个普通查询的 engine UID／PID 不同；同查询的多个块共用自己的实例。每个查询最终通过同一
作者 Lane 提交关闭记录，再核 engine API、子进程退出、reaper 停止和 author scope 结清。
本轮不声明跨查询同模型复用。四行 scope 的失败不是语义 False，已知 usage 与后续成功均保留。

健康 echo 与最终三个 Filter scope 正常退出。六个故障 echo（包括裸 Python 超时）由监督停止
错误 owner 并确认资源结清，不能计作正常退出。`future.task.get()` 与资源归还仍分别取证。
转换器继续在作者释放 Lane 前独立复制输出，捕获转换错误后让原清理路径返回，再向调用方抛错；
本轮没有新增作者清理补丁，也没有删除原有校验。

## 环境与保留的失败

作者与方法解释器同为 Python3.12.3／同一 base，方法使用独立物理副本的 Arrow16.1；
查询 driver 保留 Arrow24，通过值传输跨环境。固定 LOTUS1.2.4、LiteLLM1.95.0、OpenAI2.50.0
的源码检查通过。最终方法包副本含 34,082 个文件、5,648,764,639 字节，复制前后 SHA 与独立 inode
均核验；未下载软件、模型或数据，未升级共享环境。

作者启动需要 `imxx==0.0.1`，其三个 Python 源文件与固定公开作者提交一致；作者 CPython 的
`Coordinator::Shutdown` 仍无条件导入 Ray 并调用 `shutdown()`。因此副本还保留作者已有
Ray2.56.1 及基础依赖，实测 shutdown 前后 `ray.is_initialized()` 均为 false；没有启动 Ray 后端。

| 失败 | 保留与修复 |
|---|---|
| 首次上传将文件计数当作映射 | 在新 stage 完成全部 1008 个文件及两文件 overlay 校验后才进入环境操作；原失败记录保留 |
| 首次副本消费脱敏后的路径 | 公开包文件名被掩码；改由已安装 distribution 元数据定位，并保留版本／文件集合／SHA／inode 检查。原副本保留，监督确认退出并释放自己的 lease |
| 首 Filter 启动缺少 imxx | Coordinator 异常退出，未产生 engine 终态；资源确认结清后在新副本补齐固定公开包 |
| 下一 Filter 关闭缺少 Ray | 两个 CPU 终态及 10／4 usage 保留，关闭失败使整个查询失败；资源确认结清后补齐原作者关闭依赖，不修改关闭逻辑 |

两个失败 author scope 与最终成功 scope 使用不同 namespace 和输出目录。作者共运行
12 个 scope：7 echo、2 个保留的异常 scope、最终 2 查询和 1 mixed；没有重跑已通过的 echo。
构建和副本准备复用同一 host lease 与既有有限进程监督；各运行使用 8 CPU、一个 CPython
executor、空 CUDA mask。8 GiB RSS／2 GiB SHM 是运行时采样停止阈值，不是内核硬限制。

本次恢复没有重复既有 104 项 CPU 验证；解释器修正有新增单测及已完成的 23 项模块复测，
另仅运行新增四行组合与精确错误门反例。本轮窗口已在末次清点和归档下载后交回集成 owner。

## 接入与模型后续

接点仍是[原模块合同](../../../../code/integrations/imlane_records/README.md)：为原查询 consumer
提供返回 `FilterResults` 的 lazy factory，author finalizer 返回已有 `resource_state`。
没有新线程池、模型池、调度器或 HTTP 服务。默认 v1 与显式 v2 分开协商。

模型阶段由集成 owner 安排，在其当前 Filter 合同和共享关闭协议整合后重新绑定源码；
尤其不能用本轮固定 CPU 引擎的快速关闭认证真实 vLLM 关闭。使用新 `local-model` 配置、新输出
及永久预算，显式提供独立 Arrow16 方法解释器、无 LOTUS 的 vLLM0.25.1 模型解释器、
本地权重和 owner 分配的单 GPU mask。入口为：

```bash
PYTHONPATH=code <driver-python> -m integrations.imlane_records.verify_filter \
  --config <fresh-owner-local-model-config.json> --output <fresh-private-run>
```

模型前须重新核输入、预检、构建／源码及资源配置；结束分别核模型 API、PID 出生身份／后代、
namespace／SHM 与独立 GPU 清点，未知项继续保留 lease。作者 Ray 的 `num_gpus`、环境传播
及自身模型 owner 仍待实现和独立验证。v2 部分未知 usage、运行中模型故障／取消、跨查询同
engine 复用和多执行器吞吐尚无本轮作者实机资格。最终 baseline 的 Daft／Ray Data／IMLane 要求保持。

脱敏证据归档为 `imlane-author-cpu-20261019-evidence.tgz`，146 个成员、145 个文件哈希已核验，
SHA-256 为 `67ea2908ca1cf46547a095dc20c7abb12fcee416a4e161676c3075aefb071c95`。
归档排除大型依赖副本与缓存；原始失败及副本仍保留在私有 artifact 目录。机器记录同时保存
远端原件哈希与脱敏归档哈希，控制操作不从脱敏日志中的路径恢复。
