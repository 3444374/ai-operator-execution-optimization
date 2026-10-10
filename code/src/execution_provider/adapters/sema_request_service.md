# Sema 请求服务接入

本切片服务于 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
Sema 仍拥有 SQL、数据供给、提示、联合提示、原生请求池、限速、解析和结果行关联；接点是 `llm_url` 后的服务。
实现状态与模型观察由[整合报告](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md)记录；本文说明接口、观测和退出处理。
[最新成本诊断](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#adapter-cost-diagnosis)已检查组级复用的实际资源与有限模型样本。
[修复来源的有限模型观察](https://github.com/3444374/ai-operator-execution-optimization/blob/e8051fef151f00349581b8521d1709fb89a54612/experiments/results/postgresql/native_adapter_integration_20261009/README.md#repair-model)属于其实际整合源码和配置，不能据此认定同容量性能或原生请求池前观测已验证。

## 作者产物与采用决定

[固定作者提交](https://github.com/BITQiKangK/SemaSystem/tree/3f2c7182bdaa26c1e8925f486585da25337e687e)公开树只有 README、LICENSE、附录与 `Sema.zip`；归档只有二进制及 macOS 元数据，没有执行器源码或所述实验脚本。

| 项目 | 固定公开产物核对 |
|---|---|
| 版本 | `v0.0.1 8c2d3bd2` |
| 归档 SHA-256，文件完整性摘要 | `e00fc4d1c4e9c99df2ebf647e957ee6943392d2fc74dd22d958d44a71f56632f` |
| 二进制 SHA-256 | `15a534667a668a152524a8756fe5d66c0fa9c9822e73d26029e1a71693a56ec3` |
| 服务设置 | `llm_url`、`llm_model`、`llm_api_key` |
| 原生执行设置 | `threads`、`llm_rate_limit`、`llm_max_burst_seconds`、`semantic_batch_size` |
| 重试 | 135 项非空 SQL 设置未见关闭项；真实二进制探针曾重复发送 |
| 文档差异 | `semantic_voting_rounds` 被固定二进制拒绝 |

原请求含作者 system/user 消息、`temperature=0.0`、`chat_template_kwargs.enable_thinking=false` 及 JSON schema 字符串要求，不含 `max_tokens`。
批量大小 1 对八行发送八次调用，大小 4 将四行联合为一份提示、发送两次；均由作者解析器恢复八行。
服务把每份完整正文作为一个任务，保持提示、请求和响应，不自行拆分、合并或修补非法标签。

## 三条路径与接口

| 路径 | 接入与执行 |
|---|---|
| `sema-native-direct` | `prepare_sema_projection(service=None)`，作者二进制直达模型服务 |
| `sema-native-transparent` | `SemaRequestService` 查询专属 URL，aiohttp 原样转发 |
| `sema-method-semloom-request-service` | `SemaSemLoomService`，公共任务接口、现有核心、Daft 数据准备及 Ray HTTP worker |

[sema_service.py](sema_service.py)复用[原有 CLI 生命周期](../../baselines/text/products/sema.py)：先导入并核对 CSV，再提交同一 SELECT，读完 SQL 结果及完成标记。
[sema_semloom.py](sema_semloom.py)复用 `prepare_native_task`、`NativeTaskSession`、`decode_full_response` 和 `ray_map_factory`；数据准备默认 `daft`，可显式选择既有 `arrow` 直接分批，响应模式为 `full`，不另建方法驱动或调度核心。
SemLoom分支不创建透明转发的HTTP客户端；完成响应只解码一次，原始编码结果仍由Core持有到HTTP写出完成后归还。
执行核心 Core 在服务输入输出（I/O）线程上建立和操作，跨线程取消使用公共信号；持有任务数 `max_held_tasks` 与活动请求数 `max_active_requests` 分别传入现有核心。
当前 work 描述每份完整请求为一个 `work_units`，即请求数表征，不代表已校准的 token 工作量。
HTTP 调用者保留尚未接纳的正文，其数量受有限查询和服务请求上限控制；Core 的任务额度不能单独代表全部前端正文留存。
原 `prepare_projection`、`run_projection` 与实验默认调用方式保持。

## 观测与计时解释

| 现有记录 | 实际作用 |
|---|---|
| `proxy_arrived_ns` → `body_read_ns` | 服务处理入口至完整 HTTP 正文读取 |
| `body_read_ns` → `core_accepted_ns` | 正文校验、任务准备及等待接纳；尚未单列纯容量等待起止 |
| `core_task_sequence` / `request_sequence` | 本服务的任务与 HTTP 出现对应；相同正文的两个出现仍是两个调用 |
| `core_events` | 当前核心及传输事件，包含准备、提交和返回；`observed_ns` 是观察回调时刻 |
| `payload_stage` / `object_put` | Daft／Arrow 准备和 Ray 对象写入；可能按共享批次记录，不能逐行重复计费 |
| SemLoom `forward_started_ns` | Ray RPC，即远程调用入口；发生在远端 worker 的 HTTP 操作之前 |
| `worker_started_ns` → `worker_ended_ns` | worker 入口至完整执行响应返回，含 payload 取值与 HTTP，不是纯模型时间 |
| `model_returned_observed_ns` | driver 收到远程完成；本地单调时钟 |
| `model_returned_ns` / `model_clock_shared` | 仅在核验共享时钟后使用 worker 完成时刻，否则为不可观测 |
| `response_written_ns` | 服务完整响应写出后归还租用，不证明作者已解析 |
| `submitted_ns` → `finished_ns` | Python 包装实际 SELECT 提交至 SQL 结果和完成标记读完 |
| `native_task_ready_ns` / 原生 HTTP 到源行对应 | `unavailable`；作者请求池前时刻与可信行身份没有已核实的接点 |

本地时刻使用 `time.monotonic_ns()`；Ray 通过 Linux boot ID、time namespace 和时钟实现的摘要核对共享时钟，并检查事件先后关系。
同一时钟下可直接计算对应区间；时钟不同只使用各自内部耗时及本地接收时刻，不跨机器相减。
RPC 至本地接收包含 Ray 等待、worker 执行及返回；worker 区间嵌套其中，准备阶段也可能重叠，不能相加或从完整查询时间扣除。
作者线程数、Core 活动额度、实际 HTTP 在途与模型服务执行序列分别记录。
Sema 作者 SQL 进程可以跨查询复用，默认 Core 和 actor 逐查询创建；可选 `group-diagnostic` 在同一控制线程复用设施，任务流、HTTP入口、错误和观察逐查询独立。
查询期间的准备进入释放至结果消费结束（EOF）的完整时间，组级初始化另列，实际 SELECT 提交至读完结果直接记录。
观察包含回调、JSON 和持久记录开销；当前模型观察未采集这些函数的独立 CPU 时间，不能用 CPU fixture 数值扣出虚拟完整查询时间（JCT）。

[作者 README](https://github.com/BITQiKangK/SemaSystem/blob/3f2c7182bdaa26c1e8925f486585da25337e687e/README.md)提供 trace 日志及 `EXPLAIN ANALYZE` 的 token／profile 入口，但没有逐调用池前就绪与行对应的公开字段定义。
这些日志的实际字段、memory/file 保存和时钟语义尚未在固定二进制核实；未取得可信协议前保留上述缺项。
日志到达 Python 的时刻只表示接收，算子总时间不能代替逐请求就绪；stdout 日志还可能混入现有 CSV 消费路径，不默认启用。

## 错误与退出

首个 HTTP 错误保留原状态、原始正文和响应头，写入仓库外 `.first-error.json`，正文以 base64 保存；随后停止发送、取消自有作者进程并拒绝重试转发。
直达路径没有中间服务，不能观察首次模型 HTTP 错误或替作者关闭重试，继续保留有限 SQL 超时和待核对的错误协议。
每份请求仅在选定发送前位置登记一次；`forwarded_posts` 是发送前预留，实际模型接收需独立收据核对。
已经发送或未知的远端工作继续由原核心回收；完成响应的租用留到 HTTP 写入结束，调用者离开后的迟到结果也须归还。

退出依次尝试 HTTP runner、Core、实际创建的 HTTP client 和事件循环；任一步报错仍处理后续动作，保留首次异常及每项动作／类型／单调时刻。
runner 清理报错时使用 [aiohttp 公开接口](https://docs.aiohttp.org/en/stable/web_reference.html#aiohttp.web.BaseRunner)停止剩余 site 和连接；响应持有者未退出则保留 Core 和循环，监听停止不能确认则保留 runner、端口并报告未完成。
取消或线程等待超时继续记录实际线程与循环状态；线程结束不替代监听停止证明。
已有查询或 HTTP 首错继续作为主要原因；退出期间才初次到达的 HTTP 首错补充传播，此前已报告的首错保持原处理；没有主要错误时传播首次清理异常。
`summary.cleanup_failures` 与 `.cleanup.json` 保存退出失败，`first_cleanup_error` 指向首次异常；记录写入失败追加保存，不覆盖查询错误。

## 验证与历史诊断

[固定历史版本](https://github.com/3444374/ai-operator-execution-optimization/blob/7663ab525f0133769dc37114e4059c7d65c396f2/code/src/execution_provider/adapters/sema_request_service.md)保留作者产物检查、全部 CPU 对照值和退出反例；完整原件、失败、来源与恢复摘要继续由 `sema.json` 交接定位。
容量修复让正文只准备一次，提交序号随实际接纳更新；仅实际归还、取消或终止错误唤醒等待者，避免原每次推进反复准备与校验。
作者二进制及实际 Daft／Ray 的 CPU 诊断共 32 查询／2,176 次替身 POST，模型为 0；含带探针与不带探针对照，分别保存，不推断同容量系统加速。
[退出测试](../../../tests/execution_provider/test_sema_service_cleanup.py)十四项继续覆盖 runner、多项关闭失败、等待超时、记录写入、初始化、取消、监听及迟到 HTTP 错误；四个实际 aiohttp／公共 Core 反例原件也保留。
真实级联、多模型、Join、并行阶段展开、原生执行器替换及原生池前时刻仍为 `pending`。
