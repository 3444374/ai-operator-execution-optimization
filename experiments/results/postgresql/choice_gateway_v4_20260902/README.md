# 四 C：gateway wire v4 与 choice 请求映射

内部工程验证记录，2026-09-02。依据生产源码、独立固定向量及实际测试；对应
[四 C 专项计划](../../../plans/postgresql_choice_profile_engineering.md)。源码提交为
`7d72d9ad0998bf308c17edd68ddb38971d30dafc`，保存在独立研发分支，未合并或推送。

## 验证对象和实现范围

目标是让 gateway 接收完整 choice profile 并映射到明确声明支持的固定 HTTP 服务，同时保留旧
wire v2/v3。不是质量、成本、GPU 性能实验，也不是 PostgreSQL choice 端到端验证。

- `wire/semantic.py` 共享 v3/v4 的固定 exact-Filter 编解码；v4 严格检查版本、字段、完整 profile、
  顺序和摘要。task 从实际 messages 重算 semantic identity；重复 JSON key、未知字段和超长序号被拒绝。
- `adapters/semantic_session.py` 共享同步循环，旧 v3 import/runner 保留；不新增 registry、调度或重试。
- fixed config 新增可选 `choice_format=vllm_structured_outputs`。未声明时 choice 在 HTTP 前拒绝；
  声明后只增加 `structured_outputs.choice`，model、messages 和原有 generation 参数逐字段保持。
  服务拒绝后不删除约束重试；UNKNOWN 和非法标签均原样返回，关系判断仍属于 PG parser。
- C、SQL、TAP 源码相对 `94e927d5` 未改。schema 3 的 `0A000` 执行拒绝保留；C `AiOpenSpec`
  映射和 wire-v4 codec 待实现。本次不能证明新配置下的 PG cancel/NULL/keep-drop 或模型解码能力。

参考来源与采用决定只保存在专项计划 C.0，不写入生产/测试代码或注释。没有复制公司源码、prompt、
测试数据、二进制或日志，也没有改动公司仓库。main 上用户未提交的文档不受本分支操作影响。

## 方法、设置和实际结果

服务器先确认仓库外 runtime env，以 `core` 能力组保存预检；使用已有 PostgreSQL 18.3 的独立安装
副本、独立源码 worktree 与 socket-only 临时数据库。未覆盖默认工具链、既有数据库或服务器 main。
模型侧仅用 localhost HTTP fixture 和 deterministic golden adapter，不启动 vLLM、不下载模型。
旧 v3 请求为兼容对照；v4 正常、缺声明、HTTP 400、字段/版本/identity 篡改和 framing 错误为测试条件。

| 验证 | 观测结果 |
|---|---|
| PostgreSQL 构建与运行 | 均为 18.3；干净 `-O2 -Werror` 构建，无警告 |
| PGXS regression | 1/1，actual/expected 逐字节一致 |
| 完整既有 TAP | 537/537（440 项主套件、97 项 choice plan） |
| Python，本地和服务器分别运行 | 83/83：PostgreSQL 协议/静态 68，gateway migration 5，calibration 10 |
| 本轮新增 gateway 测试 | 15 项；包含独立 profile/schema-3/执行/payload/completion 摘要向量 |
| 中立 header、encoder 和三个 machine | C11 / Wall / Wextra / Werror / pedantic 通过 |
| 真实模型请求、held-out、真实 artifact | 0 次真实请求；未访问 held-out，未拟合或生成真实 artifact |
| 新 RSS/FD/线程增长 smoke | 未运行；历史资源证据仍绑定原提交 |

完整既有套件覆盖 recording Map/Filter、旧 exact Filter、ordinary SQL、权限、事务、snapshot、prepared
plan、取消及错误恢复。它证明共享 Python 代码调整未破坏这些旧路径；不将 537 项重复登记为新 PG
choice 执行证据。新 HTTP 测试确认约束缺省时零请求、拒绝时只发一次受约束请求、无降级重试，
后续新 session 正常恢复；canonical CLI 无需额外 PYTHONPATH。

## 开发过程与证据

开发时先补失败测试，再依次实现 v4、session、HTTP 配置和 CLI 分流。测试曾暴露重复 key 被后值
覆盖、超长 sequence 触发 Python 整数转换限制、无效 JSON 错误码不在 allowlist，以及可变导出参数
改变实际请求的问题，修正后保留相应反例。开发期输出保留在任务记录；本目录归档最终完整运行，
不伪造早期红测试文件。服务器最终资格运行一次完成，未遇到产品测试失败。
本地归档器首次将已更新的 README 与测试时快照直接比较而停止；改为核对 Git 提交的全部文件，
再核对工作树内非文档文件，确认生产/测试源码未变后导出日志。这不是产品测试失败。

[qualification.json](raw/qualification.json)保存源码 SHA、实际版本、命令、退出码、测试数量和二进制
摘要；[SHA256SUMS](raw/SHA256SUMS)覆盖全部脱敏服务器日志及[运行脚本](raw/qualify.py)。
本地复跑见 [local/SHA256SUMS](local/SHA256SUMS)。原始 preflight、Git bundle、编译产物、日志和
PG 测试目录压缩包保留在仓库外 `choice_gateway_v4_20260902` 证据包，主/公开 manifest 均已核对。
公开日志中的运行路径经统一 redactor 与路径替换处理，原始文件不进入 Git。

本轮临时数据库已停止，测试端口无监听；证据归档后仅注销并移除本轮服务器 worktree，原始证据包
可用于恢复源码和测试产物。其他历史 worktree/工作负载未清理。本地研发 worktree 保留供审核。

下一步是 C query-fixed open spec 与 wire-v4 接入及 PG 新路径验证；通过后才按专项计划进行资源
检查和受限真实 smoke。模型质量、整轮校准和第二 physical path 仍未推进。
