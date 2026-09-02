# 四 C：choice profile 的 PostgreSQL plan 接入

内部工程验证记录；来源为生产源码、独立编码向量及实际 PG18.3 测试。
对应[四 C 专项计划](../../../plans/postgresql_choice_profile_engineering.md)，不是质量或性能实验。
生产实现为 `00cc6bbf0eba5f57ec0d9bbfed99b0dbdc8a0c0c`；最终测试修订和验收源码为
`134447dd324fe1fbb47c15bc7dbf97fe0b948be0`。独立分支保存，未合并或推送。

## 目的、设置与设计

验证第四个 SQL option 能选择并完整保存三值生成 profile，同时保持旧行为。使用独立 PG18.3
安装副本和临时测试集群；系统默认工具链和既有数据库未覆盖。预检通过，测试只使用合成 SQL、
deterministic gateway/HTTP fixtures 和已有的合成 calibration fixture；真实模型请求为 0，未访问校准 held-out。

schema 3 保存 ID/version/kind/有序 choices/digest，解码逐项复制到指定 context。新 semantic
digest 包含完整 114-byte profile 编码；旧 schema 2 的字段、摘要和 wire v3 保持原样。
普通 EXPLAIN 可检查新计划，实际执行（包括 LIMIT 0、zero-row、NULL-only、prepared）在 child
初始化和 provider 选择前以 `0A000` 拒绝。中立 open spec、wire v4 和 gateway 接线不在本切片内。

测试调用生产 plan 编解码器与真实 `copyObject`，销毁原树及复制树后验证已解码值；不模拟 PG
内存管理。期望摘要来自独立展开并经 OpenSSL 核对的向量。一个实际配置的 UDS 监听器检查新计划
与所有拒绝执行均未建立连接。旧 calibration fixture 先在旧计划匹配，再在仅增加 choice 的计划拒绝。

## 实际结果

| 验证 | 最终结果 |
|---|---|
| PostgreSQL 构建和运行版本 | 18.3；干净 `-O2 -Werror` 构建，无警告 |
| PGXS regression | 1/1，actual/expected 逐字节一致 |
| 完整 TAP | 537/537：既有套件加校准隔离 440，choice plan 专项 97 |
| Python 合同，本地及服务器分别运行 | 68/68：PostgreSQL 53、gateway 5、calibration 10 |
| 中立 header、encoder 和三份 machine 源码 | C11 / Wall / Wextra / Werror / pedantic 通过 |
| 真实模型、真实 calibration artifact、新资源 smoke | 未运行；不新增模型质量、成本精度或 RSS/FD 结论 |

专项覆盖完整复制和 context 生命周期、类型/缺失/重复/额外字段、版本/choices 顺序/内容/摘要篡改、
不进入语义摘要的列绑定、SQL options、prepared/generic plan、GUC 变化与 relation invalidation、明确
执行拒绝、普通 SQL 恢复和零 provider 连接。完整套件继续覆盖 recording Map/Filter、旧 exact Filter、
事务、权限、snapshot、错误、取消与清理。旧 artifact 返回 `rejected / semantic-spec-mismatch`，成本
模型仍是 `semloom.exact_filter.uncalibrated.v1`，没有采用旧系数。

## 失败记录与修正

- `349476b3` 的 SQL 红测试在旧实现上 2/2 失败，原因是第四个 option 不受支持。
- 初次构建缺少 `explain_state.h`，修正后通过；该失败日志保留。
- 严格解码红测试复现空 profile 节点崩溃和超范围列号回绕；已加入空节点检查及窄化前范围检查。
  失败 PG 日志与最终精确 SQLSTATE/消息断言均保留。
- 早期 536 项测试的 socket sentinel 使用了错误 GUC 名称，不支持“零连接”结论。最终修订为实际
  `semloom_pg.gateway_socket`，新增 SHOW 核对，重新干净构建并通过全部 537 项；旧结果单列归档。
- 归档脚本初次 PATH 未含系统 `runuser`，在 initdb 前停止；修正后完整运行。失败输出保留。
- 本地首轮 HTTP fixture 因沙箱禁止监听而失败；获准使用 localhost 监听后 68/68 通过。

## 证据、清理与下一步

[qualification.json](raw/qualification.json)保存最终源码/构建/运行身份、源码 SHA、命令和退出码；
[SHA256SUMS](raw/SHA256SUMS)覆盖脱敏构建、TAP/PG、regression、Python、C11 日志及
[归档脚本](raw/qualify.py)。原始 preflight、bundle、失败尝试和测试目录归档保留在仓库外。
公开 initdb 日志仅移除末尾多余空行以满足格式检查；服务器初始导出另行保留。

本轮临时集群已停止，测试监听器已关闭；只清理本轮隔离 worktree，其他历史 worktree 未动。
本地工作分支保留供审核，服务器 main 未改。历史值合同、437 项和模型失败证据仍绑定各自提交。

下一步只需沿当前分层接入 query-fixed 中立 open spec、严格 wire v4 和 gateway profile 映射；
不继续拆公共 runtime，不恢复质量/成本校准，也不提前进入第二 physical path。
