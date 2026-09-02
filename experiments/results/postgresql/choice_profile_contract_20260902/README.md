# 四 C 首个切片：profile 值与规范编码

内部工程验证记录；来源为源码、独立字节向量及本地/Linux 测试，对应
[四 C 专项计划](../../../plans/postgresql_choice_profile_engineering.md)。
源码提交：`d26e210db2391fc4d69032e317488bbef2008028`，基于 `3636e6f8`，仅在独立分支保存。

## 实现范围

- Python 不可变 profile 保存 ID/version/kind/有序 choices，严格校验完整 record 和摘要，错误不回显输入。
- C 新增中立值与 standalone encoder；只有固定宽度类型、借用 bytes、caller buffer，不分配、不做 I/O。
- 固定向量共 114 bytes；domain 加一个 NUL，整数/字节长度为 uint32 big-endian，字符串无终止符。
  实现前以独立 OpenSSL 计算 SHA-256：`941327729217db0ad438a8d0c945750485c6047834229aa40912b254d90a24f7`。
- AiOpenSpec、task/completion、SQL、PG plan、wire v2/v3 和 runtime 均未改。C helper 尚未加入 PGXS。
  Python 的已解码 record 校验不代替 JSON 重复字段检查，也不是 wire v4 实现。

## 验证方法与结果

先运行旧合同，再以缺少新能力的失败测试逐步实现 Python 编码、严格校验、record 和 C 编码。
期望 bytes 先独立展开，不从被测函数生成。测试真实调用 C 共享库，不模拟其行为。

| 验证 | 本地 | Linux 服务器 |
|---|---|---|
| PostgreSQL Python 合同（原 45 + 新 8） | 53/53 | 53/53 |
| gateway migration | 5/5 | 5/5 |
| calibration builder | 10/10 | 10/10 |
| standalone C encoder | C11/Wall/Wextra/Werror/pedantic | 同左，另有 O2 严格编译 |
| PG18.3 extension | 未构建 | O2/Werror 无警告，仅构建 |
| PG regression/TAP、资源 smoke | 未运行 | 未运行 |
| 真实模型请求 / 校准 held-out | 0 / 未使用 | 0 / 未使用 |

新增 8 项覆盖字节/摘要一致性、错误类型/版本/种类/数量/顺序/内容、伪造摘要、无 NUL 终止符的
借用 slice、全部不足 114 bytes 的输出容量、NULL 指针、前后哨兵及失败不发布部分输出。
测试入口是 `code/tests/postgres/test_semloom_generation_profile.py`；旧协议、gateway 和 calibration
分别使用已有 unittest discovery 入口。本地 Ruff 未安装，未运行 lint；Python 语法和实际测试通过。

## 证据与操作范围

[qualification.json](raw/qualification.json)记录源码、文件 SHA、脱敏命令、退出码、测试数量与
构建产物 SHA；[SHA256SUMS](raw/SHA256SUMS)覆盖公开日志和[验证脚本快照](raw/qualify.py)。
原始日志、runtime preflight、Git bundle 和构建产物保存在仓库外；公开记录不包含运行位置。

服务器预检通过，使用明确核对的 PG18.3 工具链，没有使用系统默认的 18.4。
代码仅进入隔离 worktree，测试后已移除该临时 worktree；主工作树未改，未安装扩展、启动数据库
或模型，也未下载依赖。本地 worktree 保留供审核；没有合并或推送。

## 不能声称与下一步

本结果只支持值类型、严格校验与编码一致性。SQL opt-in/schema 3/wire v4、prepared/copyObject、
EXPLAIN/calibration 隔离、新 PG 路径和模型支持均待验证；编译不能代替 regression/TAP 或资源测试。
历史 437/437 与模型失败证据仍绑定原提交。

下一步接入 PG plan 与 options/版本分流，再做中立 open spec、wire v4 和 gateway 接线；随后按专项
计划验证新路径的 PG18.3 生命周期、资源和受预算限制的真实 smoke，不恢复质量或成本校准。
