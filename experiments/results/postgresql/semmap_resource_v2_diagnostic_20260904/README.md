# SemMap 资源度量 v2 修复后小规模 diagnostic（2026-09-04）

> **性质**：metric schema v2 度量实现修复后的首轮小规模诊断运行（`--diagnostic`，1 轮 × 100 行）。
> 只回答度量工具是否真的测到了目标资源；`qualification_status` 全部强制 `not_evaluated`，
> 不构成资源资格结论，更不构成四 D 完成。

## 1. 源码与环境

- 分支 `semmap-resource-v2`，源码 commit `a4119e73`（服务器 worktree `/root/autodl-tmp/semmap-res-v2`，工作树 clean）
- 独立 PostgreSQL 18.3（`/root/autodl-tmp/toolchains/postgresql-18.3`，独立 cluster + socket 目录）
- gateway observer 为**源码管理版**（`code/src/experiments/postgresql/semmap_resource_gateway_observer.py`，含 SO_PEERCRED/accepted inode），不再是历史 artifact 副本
- C 客户端为 4D 归档的 single-row-mode `resource_client_v2`（同 workload 语义）
- 模型请求：**0**（fixture-only）

## 2. metric schema / 旧结论边界

- 本轮 metric schema `semloom.pg.resource.v2`（含 pre-run static-review correction，见
  `experiments/plans/postgresql_semmap_generation_contract.md` §8.4.2）
- 2026-09-04 v1 运行维持 `failed` 判定不追溯修改：v1 的 `uds_peak_delta` 实测进程总 FD，
  93 次 attempt 为同一不可逆峰值的重判；归档证据不变
- 本轮 `supersedes measurement implementation: v1`；阈值未放宽（provider UDS client/accepted 各 ≤1、
  same-tick combined ≤2、end=0、total FD end=0、threads end=0、RSS 16/8 与 32/16 MiB）

## 3. workload

1 轮 × 100 行（diagnostic 缩减），input=100,000 bytes/行，fixture output=65,536 bytes，
fixture digest 与 4D v1 相同（`9fb95190…`），采样 0.02s/tick，稳定 baseline 需连续 5 tick
FD identity set 一致。

## 4. 采集有效性

- 稳定 baseline：三个 case 均获得（stress 在 warmup session_end 之后采样）
- tick 状态：约 8% tick 为 `partial`，错误全部为 `fd_set_changed_during_read`——每行任务
  开关 fd 使两次 FD 列举间集合变化，重试 3 次后仍 partial。**无 invalid tick、无零值伪装**
- 按 fail-closed 合同，partial tick 使 stress case 的 measurement 为 `inconclusive`（这是
  诚实输出，不是工具缺陷；下文峰值数字由"仅用 valid+partial tick 的离线重算"给出，作为
  诊断答案而非资格判定）

## 5. provider UDS 归因证据（核心问题）

- session 窗口（含 task 总数 101 = 1 warmup + 100 行）：
  - session 1（warmup，~44ms）：`no_ticks_in_window`——采样 tick 尚未落入，按不可观察记录
  - session 2/3/4（stress 各轮）：全部**唯一归因成功**，`peer_pid` 与被测 backend pid 匹配，
    accepted inode 在窗口内可见，active session 峰值 = 1（同步单会话合同成立）
- backend provider client fd：15（unbound AF_UNIX，经五条件归因后分类 `provider_uds_connected`）
- gateway accepted fd：4/5（绑定路径匹配 provider socket path）

## 6. 第三个 FD 的实际身份

backend 相对 baseline 的新增瞬态 fd 为 `anon_inode:[eventpoll]`（PostgreSQL WaitEventSet 的
epoll 描述符），分类 `eventfd_or_anon_inode`（diagnostic 类，不设门）。run7 中观察到的
eventpoll 现象与本轮一致；它在查询结束后释放（cleanup end delta = 0）。

## 7. 峰值（same-tick，离线重算的诊断答案）

| 指标 | 值 | 阈值 |
|---|---|---|
| backend provider client peak delta | **1**（fd 15） | ≤1 |
| gateway provider accepted peak delta | **1**（fd 4/5 轮换） | ≤1 |
| same-tick combined peak delta | **2** | ≤2 |
| unknown peak delta | **0** | 必须为 0 |
| violations（离线重算） | 无 | — |

## 8. cleanup end-state

各 case 的 cleanup settle（同步采样、对 case baseline 精确等式）：

- stress：total FD / threads / provider session FD end delta 全部 = 0；RSS end delta 在限内
- cancel：SQLSTATE `57014` 与合同一致，恢复查询成功，end delta 归零
- disconnect：disconnect 相 `08006` 与合同一致；恢复相独立 gateway+baseline 下恢复成功
- gateway-exit：`post_exit_sqlstate` 见 gate report 的 correctness 记录（合同值 `08006`）；
  socket path 移除确认；恢复相通过

## 9. 事实 / 推断 / 不能声称

**事实**：v2 度量链路在真实 PG18.3 + UDS 链路上完成了共享 tick 采样、显式有效性、稳定
FD-identity baseline、SO_PEERCRED 归因、same-tick 峰值与 phase 分离评估；诊断负载下 provider
combined peak = 2（阈值内）、unknown = 0、end 全归零；三个实质 session 全部唯一归因。

**推断**：度量工具已能真实区分 provider UDS / eventpoll / relation 类 FD；v1 的 +3 峰值在 v2
口径下分解为 client×1 + accepted×1 + eventpoll×1，与阈值比较时 combined=2 未超限——支持
"v1 是度量实现错误，不是系统泄漏"的判断（该判断此前已由 v1 归档分析的 end-delta=0 佐证）。

**不能声称**：资源资格通过（本轮 not_evaluated）；四 D 完成；8% partial tick 下的正式资格结论；
多会话/异步场景的任何行为。

## 10. 是否允许进入正式资格（决策规则对照）

对照 §八 决策规则：**情况 A**——client +1、accepted +1、combined peak = 2、第三个 FD 明确为
eventpoll（已分类）、所有 end delta = 0。**建议允许进入正式 3×2,000 qualification**，
前置条件剩余两项：①partial tick 比例在高负载下是否仍可接受（正式 run 直接检验）；②正式运行
需独立授权（本诊断不授权正式运行）。正式运行若 partial tick 使 measurement=inconclusive，
按合同输出 not_evaluated，不放宽任何口径。

## 11. 证据位置

- 服务器仓库外 artifact：`/root/autodl-tmp/experiment-artifacts/semmap_resource_v2_diag4_20260904/`
  （summary、stress/cancel/disconnect/gateway_exit 各 case 的 raw trace、fd_lifecycles、
  attribution、gate_report、session events）
- 公开仓库内仅本 README；raw 不入 Git（SHA-256 清单见服务器 artifact 目录）
