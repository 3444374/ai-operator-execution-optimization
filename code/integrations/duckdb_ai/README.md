# DuckDB AI 方法＋SemLoom 批次适配

本目录保存公开扩展的最小补丁、批次接口、构建方式与局部验证；总方案和全局登记由整合任务维护。
首版支持单模型、非流式文本 `ai_complete`／`ai_try_complete` Map。
研究对象为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化；本适配是外部数据库方法接入，
不表示已经把 DuckDB SQL 或多算子查询移植到自有 PG。
总体设计见[语义系统方案](../../../experiments/plans/语义系统对照.md#native-semloom-pairs)。

## 固定来源与采用决定

公开来源为 [duckdb-ai v0.4.14](https://github.com/leonardovida/duckdb-ai/tree/9b7b16a5d5bfa97180b8be48d69bd9a4a4106419)，
提交 `9b7b16a5d5bfa97180b8be48d69bd9a4a4106419`。
其 DuckDB 子模块固定 `08e34c447bae34eaee3723cac61f2878b6bdf787`，对应 v1.5.4。
[来源清单](source_identity.json)保留原文件和补丁后文件 SHA-256（文件内容摘要）。
构建脚本核对两个提交，使用 DuckDB 原生构建目标与元数据追加方式；新二进制版本写
`0.4.14-semloom1`，继续使用 `ai` 扩展名，原生社区二进制保留。
ABI（扩展与 DuckDB 的二进制接口）必须与实际 `pragma_version()`、平台和加载结果一致。

已逐项阅读固定版
[批次执行](https://github.com/leonardovida/duckdb-ai/blob/9b7b16a5d5bfa97180b8be48d69bd9a4a4106419/src/duckdb_ai_extension.cpp)
与 [provider](https://github.com/leonardovida/duckdb-ai/blob/9b7b16a5d5bfa97180b8be48d69bd9a4a4106419/src/duckdb_ai_provider.cpp)。

| 原生逻辑 | 本适配采用方式 |
|---|---|
| `AiCompletionFunction`／`AiTryCompleteFunction` 读取当前向量、处理 NULL、解析配置和 secret，生成带行序号的 `jobs` | 保留，先形成同一批合法完整调用 |
| `RunProviderJobs` 选择工作线程，`ProviderExecutorState` 留存线程和队列；每个线程调用 `Complete` | 显式 SemLoom 分支在调用它之前分派整批，跳过这两层 |
| `Complete` 构造调用身份、`RequestPayload`、URL 和完整请求检查 | 拆为 `PrepareCompletion`，原生和新路径共用；不在 Python 重建提示或生成参数 |
| `FetchProviderResponse` 管理响应缓存，`ProviderHttpPost` 使用 `ProviderRequestGuard` 和重试 | 原生分支保留；首版新路径要求响应缓存、重试、请求间隔、每分钟 token 控制和远程 usage 日志关闭 |
| `ParseCompletionResult`、`RecordUsageEvent` 解释完整响应、用量、截断和错误 | 拆为 `FinishCompletion`，两路径共用；每个完成响应立即回交原解析器 |

选择批次接点是源码所证明的工程决定：`Complete` 已处于原工作线程内部，替换它的 HTTP 地址或逐行
回调无法解除前面的线程供给。这里保留 SQL 生成的当前批次，直接使用公共任务入口。
没有运行第二遍原生模型 SQL，也没有重新实现原算子的提示、算法或解析。

## 接口、所有权与供给

[补丁](duckdb_ai_semloom.patch)只修改四个上游文件，另外加入本目录三个 `semloom_batch` 文件；
补丁使用零上下文表示，脚本先核对全部源文件摘要再用 `git apply --unidiff-zero` 应用。
不复制整个上游仓库进项目。[C 接口](semloom_batch.h)一次交出当前批次，包含原始请求字节、
模型、地址、headers、查询／调用身份、向量行序号、原估算工作量、请求超时与准备时刻。
原字段与凭据只在内存中传递，适配器不记录它们。

[Python 薄适配](../../src/semantic_methods/duckdb_ai.py)的 `DuckDBSemLoomBridge` 注册受信任的批次回调。
`DuckDBNativeTaskExecutor` 使用公共 `NativeTaskSession`、`prepare_native_task` 和 `decode_full_response`；
现有执行核心拥有接纳、组织、容量、传输和完成记录。主路径的执行对象必须由
`build_native_execution(config, physical=RayMapConfig(...))` 建立，使用现有 Daft／Ray；
`physical=None` 仅为明确命名的本地诊断。没有新增线程池或另一套调度核心。

原方法在当前向量内可供给最多 2048 个非 NULL 完整调用。新路径解除原工作线程与请求控制的供给限制，
但不提前获得下一 SQL 向量。每个响应按唯一调用身份恢复原行位置，重复文本不合并。
完成反馈可立即运行原解析器：`ai_try_complete` 继续逐行捕获；`ai_complete` 首次致命解析错误停止
未接纳的后续调用，已提交任务按公共核心的退出规则处理。
所有当前向量输出仍需等待该向量函数返回；单模型请求完成不等于 SQL 行已经交付。
跨向量流水、多个模型、级联、Join 和并行阶段展开保持 pending。

原响应正文和 HTTP（模型服务请求协议）状态回交 C++，原解析器继续处理 usage、额外字段、长度截断
与供应商错误；公共层不将 HTTP 错误改成 PG 的简化错误类别。取消回调读取 `ClientContext::IsInterrupted`，
消费者关闭后，尚未确认的远端任务继续由 Engine（公共执行核心）持有；服务拥有者推进清理后才结束执行对象。
回调正在运行时禁止注销，防止 Python 回调或响应缓冲被提前释放。

原生默认值仍是 `SET duckdb_ai_executor = 'native'`。显式选择 `semloom` 后不自动回退；
不支持的政策、混合模型／凭据、非法关联或超量结果都报错。仅支持 `openai_compatible` 文本 provider。
新路径核对固定模型、地址、认证和总超时。连接超时需显式等于总超时，公共传输使用同一总截止时间；
原生 `User-Agent` 在调用说明中保留，实际公共 HTTP 客户端使用自己的识别头。
默认工作量为请求计数，原 token 估算单列；`describe_work` 可传经验证的完整请求描述，不伪称已完成 token 校准。

批次请求／响应的接口上限分别为 32 MiB，单响应接口上限 8 MiB；公共任务的单项请求和完整响应
仍受其约 1 MiB 设置管理，响应额度包含 headers 表示。上述是逻辑字节额度，不表示总进程内存。
公共响应接口没有单独的纯 HTTP 耗时字段，因此新路径的原 usage `elapsed_ms` 写 `-1` 表示不可观测，
不写零，也不把排队或完整任务耗时填进去。token 用量、状态、重试和错误仍由原解析器记录。

## 构建与使用

上游源码、DuckDB 源码、构建缓存和二进制均在仓库外保存。需要 C++17 工具链、CMake、Ninja、
libcurl 开发包。按 runtime 手册先完成实际机器预检，随后运行：

```sh
python3 code/integrations/duckdb_ai/build.py \
  --source <external-duckdb-ai-checkout> --duckdb <external-duckdb-v1.5.4-checkout> \
  --build <external-build-directory> --jobs 2
```

脚本拒绝不同提交和非预期源文件，保存构建日志、补丁／来源／二进制摘要到构建目录。
原社区扩展继续用 `LOAD ai`；新路径在独立连接中开启 `allow_unsigned_extensions`，
使用 `ctypes.CDLL(_duckdb.__file__, mode=ctypes.RTLD_GLOBAL)` 将已核对的 Python DuckDB 运行库用于
扩展符号解析，再按绝对文件路径 `LOAD` 本次二进制，然后创建公共执行对象并注册 `DuckDBSemLoomBridge`。
构建使用 `EXTENSION_STATIC_BUILD=OFF`，扩展不再嵌入另一份 DuckDB。
Python 进程加载的 C++ 标准运行库也需支持编译器使用的符号；本次验证为独立测试进程选择系统运行库，
保留原 Conda 驱动环境。`GLIBCXX_3.4.30` 缺项的首次装载失败已保存。
首版服务拥有者串行运行 SQL，成对路径均设置 `threads=1`；并发 SQL 的共享执行对象接入保持 pending。
所有端点与凭据由外部配置提供。缓存、重试等关闭设置对成对路径一致；不改原生默认。
先加载再注册，查询结束后注销 bridge，推进 Engine 清理，最后关闭执行对象和连接。

## 验证与尚未完成内容

[验证用例](../../tests/semantic_methods/test_duckdb_ai.py)覆盖二进制完整响应、逆序关联、重复／缺项／
错误身份、结果额度、取消、首次解析错误停止、原提示往返、NULL、重复文本、usage 和原生错误解释。
本地五项接口验证通过；八项真实库检查在缺少已编译扩展时明确跳过。
服务器固定源码编译、装载和十三项接口／真实 SQL 检查通过（十二项完整套件与一项新增混合模型检查），
包括原社区二进制与补丁原生分支一致；使用实际 Daft 0.7.21／Ray 2.56.1 的两项 SQL 检查通过。
[检查脚本](verify_ray.py)只访问确定性本机 HTTP 替身，使用 4 个 CPU 槽位、0 GPU、
256 MiB Ray 对象存储，最多 256 次替身请求。Ray 检查确认真实对象写入、HTTP 完成和传输结束，
当前向量一次提供十九个请求，原生请求额度为 1 时，新路径的实际 HTTP 峰值为 2；公共记录与 Job 均已归还。
默认静态构建、C++ 标准选项、旧运行库装载、过长 Ray socket 路径及混合模型用例的早期失败／停止记录均保留。
失败修复只涉及本切片的构建、测试进程与用例，原生模型 SQL 没有被重新运行来替代新执行。
这些检查不支持真实模型质量或方法性能结论。真实模型请求为 0，全局整合与真实模型运行由整合任务完成。

公开上游为 MIT 许可证，原文见 [LICENSE.upstream](LICENSE.upstream)。新增文件和补丁注明了修改身份；
分发包含上游代码的二进制或补丁时一并保留该版权与许可证说明。
另保留 [DuckDB MIT](LICENSE.duckdb)、[RE2 BSD](LICENSE.re2) 和 [yyjson MIT](LICENSE.yyjson) 的版权与许可证原文；
构建所链接的 curl 使用其安装包提供的许可证。实际外部分发仍按项目授权与来源记录处理。

## 常驻接入的 CPU 诊断与局部修复

基于整合提交 `3a5f7bc9`，核对 `resident-source02` 中本桥、公共任务入口和供应商观察包装的摘要相同。
既有六路径模型运行已完成；其中 LOTUS 128 行的原生／本地诊断／Daft＋Ray中位数为
3.999719／30.935126／38.314639 秒，不能将该现象直接归因于 DuckDB。
本次只用真实扩展和确定性 HTTP 替身，在 CPU 48–55、CUDA 可见设备为空、C4 下检查。
当时容器 GPU 访问不可用，使用显式 CPU fixture 预检；这不是当前 GPU 资格证明。

[专用诊断](diagnose_resident.py)调用现有供应商适配，保留完整 payload、原解析、usage、观察和资源处理，
原生分支继续使用自己的线程及请求控制。每条路径在同一连接中连续执行 8、8、128 行并替换输入；
SemLoom 复用同一执行对象和已有 Ray worker 服务。该脚本只用于本接入诊断，不是统一常驻 runner。
修复前的真实扩展回归出现十三条调用封装 417 次；同探针的 128 行本地／Ray 检查分别为 1079／1296 次。
这是反压重试重新建立 `OfferedTask` 与工作量描述，未发现 C++ 重复生成同一完整 JSON 请求。

局部修复保留未接纳的有限任务后缀，只为新位置建立任务；仍按原 offer 宽度补齐，顺序和 C4 保持。
修复后每行只封装一次，连续查询为 8、8、128 次。结果 lease 原本就逐项释放，观测组大小均为 1，
本次没有调整释放或取消逻辑。完整请求指纹、关联、输入替换、原始错误和 usage 检查保持通过。
探针记录函数墙钟、所在线程 CPU、线程 ID、驱动进程 CPU 与线程数；进程 CPU 不包含 Ray worker。
原生回交解析的计时包含 C 接口探针往返，不冒充纯解析器成本。

修复去除了重复工作，仍未显示稳定的完整查询耗时收益；共享 CPU 下的单次样本只作诊断。
准备与响应观察、原生解析成本不能据此解释 LOTUS 的 30.9 秒。首次 Ray 查询的物化等待与第二次
8 行查询分别保存，不把冷等待当成所有查询的逐行成本。其他核心准备／派发开销未在本任务中改写。
服务活跃量按“替身收齐请求至响应正文就绪”记录，排除正文写出及客户端接收；
另检查公共核心实际请求责任不超过 4。早期 TCP handler 存续探针的错误峰值及更正结果均保留。
真实模型复核由主会话安排，本修复新增真实模型请求为 0。

服务器重启后，旧运行摘要与本地原件逐项核对一致。另用独立源码副本组合本修复与公共提交
`5a12e76d`，完成新的只读 CPU 预检、十四项实际扩展回归和九次常驻替身查询；结果全部通过，
各路径连续 8、8、128 行的完整请求指纹一致，每行只封装一次，查询末尾资源已归还。
两项修改分别处理空结果释放时的唤醒和未接纳任务的重复封装，组合检查只采用公共实现的释放修复。
本轮证据已下载并核对摘要，原件继续保存在仓库外；这些检查仍不代表真实模型性能收益。
