# Choice：受控资源、请求预算工具与会话限制检查

## 目的与范围

按[四 C 专项 C.5](../../../plans/postgresql_choice_profile_engineering.md#c5-对照请求预算与资源保证)检查新 profile
是否在已声明规模下回收本地资源，为后续受限真实服务检查准备持久预算工具。它是执行基础的工程验证，
不是语义质量、模型性能、成本校准或多算子优化实验。

结论：受控 v3/v4 fixture 资源、取消和阻塞 DNS 恢复检查通过；预算/HTTP 观测工具通过 8 项测试。
零任务双会话检查复现串行 gateway 限制，**多会话与两个 Filter AND 尚未实现**。
本轮真实模型请求为 **0**，未启动模型、访问 held-out 或恢复校准；四 C 尚有受限真实服务检查。

## 身份、环境与合规

- 工具源码：`4464fe9b`；早期工具 `7da954f9`、句柄诊断 `eb4e0b6d` / `0c1fda9f` 均保留。
- PostgreSQL **18.3**：复用 `39007150d5d0f84904fcd0c36b7bab87de7c07c1` 已验证安装，extension SHA-256
  为 `534b1a5245999a85da941a4c2932e06582961f946732b490db98a1c5c9b0fcc6`，与旧证据一致。
  本轮**未重建、未重跑 TAP/regression**；旧 919/919 与 regression 1/1 只证明旧提交，不重新绑定。
- Linux 独立测试集群、Unix socket、合成数据和自有 localhost HTTP fixture；未使用 GPU、公司代码或私有数据。
  runtime preflight 的 core/text-qwen15b 通过，只证明环境可用，不构成模型运行证据。
- 本轮没有修改 PG C、SQL、wire、生产 gateway/HTTP Adapter。新增工具只在 `code/src/experiments/`
  和脚本/测试中，production 不反向导入它们；SQL parser、取消和 provider 生命周期保持。
- 研发分支未合并/推送；main 的用户未提交文档未覆盖。参考 main 新 §8.7 完整工程对照，来源/取舍只在
  计划及本记录：SQL 注册、载体、算子语义、请求/结果、生命周期、外部执行均分别采用、保留或延期。
  本轮落实的是自有观测与资源检查，不移植公司结构，也不把 Adapter 当作全部成果移植。

源码和二进制哈希见 [qualification.json](raw/server/qualification.json)。原始日志、数据目录、Git bundle
及原始 manifest 保留在仓库外持久 artifact 目录；此处只保存经过路径和敏感字段脱敏的日志/观测。

## 预先设置与全部尝试

每 profile 同一 backend/gateway：65,536-byte 合成 input，64 行预热，然后 100/1,000/4,000 行，
共 5,164 次。fixture 恒定返回 TRUE，usage 为每次 prompt=17、output=1，SQL 行数/调用/usage 全部核对。
20 ms 采样 RSS bytes、FD、OS threads；每 cell 结束等待 0.5 s，再取五点中位数作为 settled 值。
这些 usage 是 fixture 值，不是真实 tokenizer 或模型数据。

相对各 profile 预热基线：每进程峰值 RSS 增量 ≤16 MiB，settled 增量 ≤4 MiB；settled FD/threads
恰好回到基线，峰值 FD≤基线+3、threads≤基线+2。DNS 阻塞时仅 gateway settled threads 可多一个。
未按观测结果放宽这些阈值。

| 尝试 | 源码 | 实际结果 |
|---|---|---|
| `run` | `7da954f9` | v3/v4 正常与取消通过；DNS 第 6 次 FD 从 38 降至 37，等值断言失败，整轮未通过 |
| `run-diagnostics` | `eb4e0b6d` | 读取 postgres UID 的 proc 链接被容器权限拒绝，诊断运行未通过 |
| `run-diagnostics-r2` | `0c1fda9f` | 用对应 UID 读句柄；正常与取消通过，DNS 第 5 次 FD 再减少；确认缺少测试表 `_fsm` 文件句柄，socket 未增加 |
| `run-stable` | `4464fe9b` | 先 VACUUM ANALYZE，自有 fixture 表/TOAST 关闭自动 vacuum 后独立重跑，全部通过 |

前三轮不改判通过。FD 下降本身不是泄漏；受控重跑消除测试表后台维护对等值检查的干扰，不以此
宣称已经覆盖并发 vacuum 等环境。首次归档误将仍写入的归档程序自身日志纳入原始 manifest，后改为
排除该文件并生成新的归档；原版仍保留在仓库外，正式原始与公开 manifest 均重新校验通过。

## 正常资源数据

下表是 `run-stable` 的 start / sampled peak / settled end，RSS 单位 bytes。基线、所有采样及
逐查询计数见 [resource-summary.json](raw/server/resource-summary.json) 和 `raw/server/run-stable/`。

| Profile / 行数 | backend RSS | gateway RSS |
|---|---|---|
| v3 / 100 | 23,900,160 / 23,900,160 / 23,900,160 | 23,818,240 / 23,953,408 / 23,851,008 |
| v3 / 1,000 | 23,900,160 / 23,900,160 / 23,900,160 | 23,851,008 / 24,252,416 / 24,055,808 |
| v3 / 4,000 | 23,900,160 / 23,957,504 / 23,957,504 | 24,055,808 / 24,469,504 / 24,244,224 |
| v4 / 100 | 23,957,504 / 23,957,504 / 23,957,504 | 23,683,072 / 23,814,144 / 23,715,840 |
| v4 / 1,000 | 23,957,504 / 23,957,504 / 23,957,504 | 23,715,840 / 24,121,344 / 23,916,544 |
| v4 / 4,000 | 23,957,504 / 23,957,504 / 23,957,504 | 23,916,544 / 24,317,952 / 24,104,960 |

全部六个 cell：backend FD=49/51/49、threads=1/1/1；gateway FD=4/6/4、threads=1/2/1。
两配置各处理 338,427,904 input bytes（含预热）；只是在本规模内未超过预定资源增量，不能证明
任意累计任务量均无增长。20 ms 采样也不保证捕获更短瞬时尖峰，不据此比较两版本性能。

## 取消、DNS 和恢复

- choice HTTP fixture 延迟 300 ms、adapter deadline 1 s，PG statement timeout 50 ms；10 次均为
  `57014`。逐次耗时约为 0.0520、0.0514、0.0515、0.0516、0.0512、0.0515、0.0514、0.0516、
  0.0516、0.0525 秒，均低于预定 2 秒；settled 资源回到基线，最终正常查询成功。
- 单独 gateway 注入阻塞 resolver，10 次均为 `08006`；释放前 HTTP 请求为 0，gateway 只保留
  一个 DNS worker，FD 不增长。释放后正常查询成功，worker 退出并回到 1 线程，HTTP 总计 1 次。
- 观测 subclass 调用原 Adapter；它不修补 completion、改写请求、做重试或替代原 deadline。
  DNS injection 只在明确 fixture 模式允许。不据此声称系统 resolver 可强制取消，或本地断连后远端
  模型立即停算。

## 预算与会话检查

`choice_attempt_ledger.py` 在实际 POST 前 flock、追加记录并 fsync；已有文件必须先验证，最多 100 次。
失败/发送状态不明不退款，损坏、截断、重复字段、错误身份或计数拒绝继续；显式创建不覆盖已有 ledger。
8 项测试覆盖进程式重新打开、并发预留、损坏与持久化失败，以及真实 localhost HTTP 500 仍消费预算、
持久化失败时零发送。它是后续实验工具，**本轮没有创建真实服务累计 ledger 或用它发送模型请求**。

[零任务会话探测](raw/local/session-probe.json)：第一连接已 open 时第二连接在 0.2 秒观察窗内不回握手，
关闭第一连接后第二个成功；发送 task=0、模型请求=0，gateway 正常退出且 socket 删除。
这与 `server.py` 串行 session loop 一致，说明下一组合切片需先满足主计划 §6.4；不是当前单节点回归。

## 复现、清理与结论

```bash
PYTHONPATH=code <driver-python> code/scripts/experiments/run_choice_resource_checks.py \
  --repo <clean-worktree> --root <fresh-artifact-directory> --prefix <qualified-pg18.3-prefix>
```

需按 runtime runbook 先运行 preflight；该 Linux runner 以管理员启动，仅自有 cluster/fixture 子进程
使用 postgres 用户，所有数据库监听限于 Unix socket。记录源码/二进制身份并保留每次失败；不复用结果目录。
本地 68 个 PG/protocol、5 个 gateway、10 个 calibration 和 8 个预算检查合计 **91/91**；服务器本轮
新预算检查 **8/8**，外加上述真实 PG resource queries，不把它们合并成新 TAP 数量。
只清理本切片自有运行进程/临时入口；持久结果保留，其他历史工作负载不在本轮结论范围。

四 C 仍需最多 100 次累计预算下的真实 choice 服务检查。按用户最后更新的安排，完整工程对照后先做
真实 Map 与必要公共整理，再扩展可组合执行/有界多会话（含 Filter → Map）。没有实现这些能力，也没有通过
Filter 质量、真实成本校准或第二 physical path；现有分层不需要因此重做。
