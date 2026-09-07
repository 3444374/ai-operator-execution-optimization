# PostgreSQL 扩展目录重构验证（2026-09-07）

本文件为内部工程验证记录，对应 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
目的为在保持 SemMap/SemFilter 已有同步行为的条件下整理扩展职责与依赖，依据
[主计划§8.1.1](../../../plans/postgresql_ai_semantic_operator_architecture_20260827.md)。
目录与接口说明只由[扩展 README](../../../../code/postgres/semloom_pg/README.md#module-layout)维护。

**服务器补验已通过**：`20b22a55` 的Linux112+4项测试、PG18.3严格构建、SQL regression 1/1和
全部TAP 1758/1758通过；安装件与构建件相同，本轮自有活跃进程为0。补验见下方“服务器补验”，
本地阶段的原始记录不改写。用户随后授权，分支已快进合并本地main；本轮没有真实模型请求。

## 设置与范围

起点为 `41e103f2afff26ef2afe617659670ed80c36ccd8`，工作分支 `codex/pg-module-layout`。
本记录绑定[最终源码文件 SHA-256](raw/source-manifest.json)，覆盖扩展、算子/网关测试与公共 provider。
本地阶段运行于macOS；独立C编译使用Apple clang 17.0.0。该阶段没有安装依赖、连接远程服务器、启动
PostgreSQL 或请求真实模型。HTTP/gateway 测试使用本地合成响应，不能记为真实模型运行。

这是保持行为的工程检查，没有性能指标、质量数据集、对照算法或消融，不做方法有效性结论。
检查对象为消息/摘要、值、配置、错误与现有接线测试；SQL、regression expected 和 TAP 断言没有修改。
本地阶段未验证PG18.3实际编译与数据库生命周期：当时没有找到`pg_config`、PGXS或目标头文件。
随后用户另行授权服务器补验，结果单列如下。

## 各次检查与失败

| 顺序与源码阶段 | 结果 | 原始记录 |
|---|---|---|
| 迁移前基线 | 112/112 通过 | [baseline-postgres.log](raw/baseline-postgres.log) |
| 路径迁移后 | 111 通过，1 失败 | [moved-postgres.log](raw/moved-postgres.log) |
| 接口拆分后、测试修正前 | 112/112 通过 | [interfaces-postgres.log](raw/interfaces-postgres.log) |
| 单独修正 deadline 断言后 | 9/9 通过 | [http-deadline.log](raw/http-deadline.log) |
| 最终算子合同集合 | 112/112 通过 | [final-postgres.log](raw/final-postgres.log) |
| 最终公共 gateway 集合 | 4/4 通过 | [final-gateway.log](raw/final-gateway.log) |
| 七个语义 C 模块和一个中立接口客户端，严格 C11 | 8/8 编译通过 | [build-audit.json](raw/build-audit.json) |
| 生产27个对象、测试6个对象的源码路径 | 全部解析且无遗漏/歧义 | 同上；仅构建图检查 |
| 迁移与提取一致性 | 45文件、原marker函数体与extern声明一致 | [equivalence.json](raw/equivalence.json) |

迁移后失败来自既有 `test_adapter_maps_remote_failures_to_redacted_wire_codes_without_retry`
的 timeout 子场景：总时限仅1ms，DNS/连接之前即可返回正确的 `MODEL_TIMEOUT`，原断言却要求
服务器必须收到一次请求，实际计数为0。接口拆分后的通过没有消除该错误假设，因此继续修正测试：
透传 spy 直接记录 `HTTPConnection.request`，超时场景要求最多一次派发，其他远程错误仍严格一次。
响应阶段超时另有独立测试；没有改变生产适配器、超时值、重试、错误码或隐私断言。
上表保留所有阶段，不把重跑成功覆盖原失败。测试计数是各次检查的用例数，重复运行不累加成覆盖量。

一致性检查比较起点与当前源码：只去掉 include 行，并明确处理 recording schema 常量从 plan
头文件移至 recording 语义合同；其余45个迁移文件逐字相同。marker函数体原样从 extension 提取，
保留部分的extension函数体及原extern声明也逐字核对。中立 `ai_provider_port.h` 字节不变。
C/头文件由48个、9,583行变为52个、9,619行，净增36行用于明确依赖和独立接口，未新增执行机制。

## 复现与证据

从仓库根目录复现现役测试；C 编译命令见 `build-audit.json`：

```bash
PYTHONPATH=code python3 -m unittest discover -s code/tests/postgres -p 'test_*.py' -v
PYTHONPATH=code python3 -m unittest discover -s code/tests/execution_provider -p 'test_*.py' -v
```

[validation.json](raw/validation.json)保存环境、阶段、原日志SHA-256及公开日志SHA-256；日志经公共
redactor处理，工作区绝对路径替换为 `<repo>`，公开副本去掉行尾空白；原日志哈希单列，失败内容不改判。
源码清单记录实际字节，不以当前分支名称代替版本身份。
[PUBLIC_SHA256SUMS.json](PUBLIC_SHA256SUMS.json)登记本目录其余证据文件。

## 可以确认与下一步

独立需求审查（Spec）与规范审查（Standards）各为0项可操作发现；两者都将缺少完整PG18.3验证
列为当时剩余检查。审查覆盖迁移、头依赖、构建对象和测试修正，不代替实际数据库运行。

源码与本地测试支持这次职责迁移及接口依赖收窄保持了所检查的消息、值和同步接线行为。
后续PG18.3补验现已通过；旧真实模型结果仍只属于各自旧版本，本次没有新增模型证据。
组合、异步、多会话、正式资源压力、质量与性能状态均不改变。

本轮所需本地与PG检查已完成。用户授权后，本地main从41e103f2快进至103e2715，无冲突；
验证仍绑定20b22a55，后续只有记录与说明变更。当前尚未推送，原始JSON中的未合并字段保留运行当时状态。
运行时仍使用外部配置，不引入临时机器路径或端点。

## 服务器补验

用户2026-09-07授权服务器验证，锁定提交`20b22a55b4e5dcdbf5dd65649160210721cd0f85`。
以Git bundle创建独立工作树，107项源码文件哈希匹配本地记录，服务器原main仍为干净的`41e103f2`。
core/text只读preflight共28项检查通过，使用既有Python3.12.3 driver环境；编译和测试数据位于数据盘，
PG软件安装到独占prefix。现有PG18.4安装不变，没有启动模型、下载模型或改变已有服务。

服务器没有原PG18.3工具链，因此从[PostgreSQL官方18.3源码目录](https://ftp.postgresql.org/pub/source/v18.3/)
取得发布包，双端核对[官方SHA-256](https://ftp.postgresql.org/pub/source/v18.3/postgresql-18.3.tar.bz2.sha256)：
`d95663fbbf3a80f81a9d98d895266bdcb74ba274bcc04ef6d76630a72dee016f`。
构建启用TAP，关闭本次不使用的readline/ICU功能。补齐Bison/Flex/IPC::Run及其依赖，共新增6个系统包，
没有升级或删除原有包；下载缓存留在数据盘，构建依赖与模型环境分开。

| 检查 | 本次结果 |
|---|---|
| Linux算子合同与公共gateway | 112/112、4/4，无跳过 |
| 扩展构建 | `COPT=-O2 -Werror`；全部生产对象及测试专用plan codec实际编译通过 |
| 加载身份 | 实际server_version为18.3；安装件与构建件SHA-256相同 |
| SQL regression | 1/1；actual与expected逐字节一致 |
| 全部TAP | 7文件、1758/1758；没有筛选文件或调整断言 |
| 清理 | 自有活跃PG/gateway进程0；两次已启动regression集群的socket均关闭 |

[完整摘要](raw/server/verification.json)记录命令、逐步退出码、哈希与失败身份；
[最终installcheck输出](raw/server/pg3-installcheck.log)保留完整文件列表和PASS结果。
安装件与构建件SHA-256均为`2507c1b418430c4d8494326a4cccfc4f91735ecc6c9e99d952ce24e9d1f90506`。

准备和运行失败均保留，未拼接成最终通过：

1. 服务器直连官方源码下载在180秒超时；原部分文件及失败日志保留。本地取得同一官方发布包，
   核对哈希后通过SSH传输，服务器再次核对哈希，再开始构建。
2. 第一次临时驱动未在隔离PATH中找到`runuser`，尚未执行构建或启动PG；失败摘要保留。
   后续使用实际命令的绝对路径，不改公共实现。
3. 第二次严格构建及SQL回归通过，但临时驱动设置`LANG=C`，TAP自行初始化的数据库成为SQL_ASCII，
   被算子既有UTF8检查拒绝。该轮只运行434项TAP，结果为失败；完整日志/临时集群数据移入原轮目录，
   按PID和启动身份终止其遗留的一个fixture gateway。
4. 第三次清理可重建对象，使用新集群和临时目录，指定UTF8 locale与
   `PG_TEST_INITDB_EXTRA_OPTS='--encoding=UTF8'`。重新严格构建并完整运行，1758项全部通过。
   测试中单独创建LATIN1数据库的拒绝用例仍保留并通过。

一次性驱动和完整PG日志留在服务器独占产物目录，公开摘要保存其SHA-256；没有向`code/`添加
临时机器脚本。最终源文件、编译件和数据库结果属于同一提交；只更新验证记录与运行说明。
