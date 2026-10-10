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

## 验证与适用范围

[验证用例](../../tests/semantic_methods/test_duckdb_ai.py)覆盖完整响应、逆序关联、重复／缺项／错误身份、
结果额度、取消、首次解析停止、原提示、NULL、重复文本、usage（供应商用量记录）与原生错误解释。
下表是各轮专项检查，彼此有重复，不累加为一次完整套件；均使用实际扩展或明确的接口替身，模型请求为 0。
总体登记与模型结果见[整合报告](../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md)。

| 源码／专项检查 | 已验证内容 |
|---|---|
| 首版固定源码 | 十三项接口／实际 SQL 及两项 Daft 0.7.21／Ray 2.56.1 SQL 检查通过；原社区二进制与补丁原生分支一致。本地没有已编译扩展时，实际库项明确跳过 |
| `535c048b`；与公共 `5a12e76d` 的组合 | 十四项实际扩展及九次常驻查询通过；每路径连续 8、8、128 行，完整调用指纹、输入替换、usage 和末尾资源归还一致 |
| `8ab8e3e7` 外层桥关闭 | 九项接口及十一项实际扩展检查通过；关闭失败使两种 SQL 函数报错，已有解析错误或取消优先，正常后续查询清除旧诊断 |
| `64f738ad` 内部首错与清理 | 六项 Core／内存响应／ctypes 组合及六项实际扩展检查通过；释放和关闭分别尝试，首错不被附加错误替换 |

[Ray 检查](verify_ray.py)使用四个 CPU 槽位、0 GPU、256 MiB 对象存储和最多 256 次本机替身请求。
当前向量供给十九个请求，原生请求额度为 1 时，新路径 HTTP 峰值为 2，公共记录与 Job（查询任务组）已归还。
[常驻诊断](diagnose_resident.py)在同一连接替换输入，复用 SemLoom 执行对象和 Ray worker；它只用于本接入。
修复前十三行封装 417 次、128 行本地／Ray 封装 1079／1296 次；保留未接纳任务后缀后每行只封装一次。
这是 Python 任务准备重复，未发现 C++ 重复生成完整 JSON；共享 CPU 单次样本未显示稳定的完整查询耗时收益。

内部执行器逐项解码、核对关联并尝试归还 lease（核心结果使用权），再回交原 C++ 解析器。
它显式保留本次执行和交付首错；`last_cleanup_errors` 分别记录释放／关闭原因，`last_cleanup_error` 保留合并摘要。
正常穷尽或消费者提前停止后的必要关闭失败均向上传播；`GeneratorExit` 只作为生成器控制流程。
外层桥完成关闭后才返回 C 状态，原解析／执行／取消首态继续保留，清理错误单列；每批重置诊断，结束时在锁内发布。
故障在真实资源动作完成后注入，复用前检查额度、记录与 Job 已归还；这些检查不证明任意关闭故障都能归还资源。

前后源码、预检、全部重复、失败和慢样本保存在仓库外，包括编译／装载、Ray 路径、混合模型和探针更正记录。
诊断使用 CPU 48–55、CUDA 可见设备为空和请求容量 4；不据此说明当前 GPU 可用性、模型质量或性能收益。
函数探针记录墙钟及所在线程 CPU；驱动进程 CPU 不含 Ray worker，原生消费探针也包含 C 接口往返。
替身活跃区间为收齐请求至响应正文就绪，不含正文写出和客户端接收；首次物化与后续查询分别保留。

公开上游为 MIT 许可证，原文见 [LICENSE.upstream](LICENSE.upstream)。新增文件和补丁注明修改身份；
分发上游代码时保留版权与许可证。另保留 [DuckDB MIT](LICENSE.duckdb)、[RE2 BSD](LICENSE.re2) 和
[yyjson MIT](LICENSE.yyjson) 原文；curl 使用安装包许可证。实际外部分发仍按项目授权与来源记录处理。
