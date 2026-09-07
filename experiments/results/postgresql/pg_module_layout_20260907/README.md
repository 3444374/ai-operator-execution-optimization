# PostgreSQL 扩展目录重构验证（2026-09-07）

本文件为内部工程验证记录，对应 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
目的为在保持 SemMap/SemFilter 已有同步行为的条件下整理扩展职责与依赖，依据
[主计划§8.1.1](../../../plans/postgresql_ai_semantic_operator_architecture_20260827.md)。
目录与接口说明只由[扩展 README](../../../../code/postgres/semloom_pg/README.md#module-layout)维护。

## 设置与范围

起点为 `41e103f2afff26ef2afe617659670ed80c36ccd8`，工作分支 `codex/pg-module-layout`。
本记录绑定[最终源码文件 SHA-256](raw/source-manifest.json)，覆盖扩展、算子/网关测试与公共 provider。
测试运行于本地 macOS；独立 C 编译使用 Apple clang 17.0.0。没有安装依赖、连接远程服务器、启动
PostgreSQL 或请求真实模型。HTTP/gateway 测试使用本地合成响应，不能记为真实模型运行。

这是保持行为的工程检查，没有性能指标、质量数据集、对照算法或消融，不做方法有效性结论。
检查对象为消息/摘要、值、配置、错误与现有接线测试；SQL、regression expected 和 TAP 断言没有修改。
PG18.3 的实际编译与数据库生命周期验证为 **pending**：本机没有找到 `pg_config`、PGXS 或目标头文件，
按本轮范围没有重新启用服务器；该分支未合并 main。

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
列为剩余检查。审查覆盖迁移、头依赖、构建对象和测试修正，不代替实际数据库运行。

源码与本地测试支持这次职责迁移及接口依赖收窄保持了所检查的消息、值和同步接线行为。
尚不能确认新构建路径在完整PG18.3环境中通过；旧Linux/TAP/真实模型结果仍只属于各自旧版本。
组合、异步、多会话、正式资源压力、质量与性能状态均不改变。

继续数据库功能前，在具备PG18.3的环境以本分支精确源码完成严格PGXS构建、regression和全部TAP，
再判断本轮是否可以合并。运行时仍使用外部配置，不引入临时机器路径或端点。
