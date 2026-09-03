# 生成型 Map 纯值与 Python v5 验证

日期：2026-09-03。内部实现与验收记录，不是模型质量或性能实验。

## 目的与范围

按[四 D 合同](../../../plans/postgresql_semmap_generation_contract.md)实现 Map 的 C/Python 纯值、
规范身份、原始完成值及 Python v5；复用公共 framing、同步 session 和 fixed HTTP Adapter。
为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化提供生成型请求基础，
本轮没有测试优化方法、训练模型或进行成本校准。

源码：`c338d81b` 实现，`425d2b1cfb4a9a2e85dad7e96c4da91fb6884d25` 修复独立复核反例并作为最终验收身份。
分支为 `codex/semmap-message-contract`，尚未合入 main。
前一[消息子切片](../semmap_messages_20260903/README.md)仍绑定 `6903cf46`。

## 设置与合规自检

- C 使用真实编译函数与独立固定向量，不在编码器中分配内存、使用 PG 类型或进行 I/O。
- Python 测试使用合成 golden、socketpair、独立 UDS CLI 和 localhost fake HTTP；无公司材料。
- PG 使用官方 `REL_18_3` 源码身份、独立 prefix/PGDATA/socket 与 `postgres` 测试用户。
  没有覆盖原 prefix 的 `.so` 或服务器主工作树；无模型下载、真实推理、held-out 或资源 smoke。
- 用户已另批准有限真实服务配置，见合同 §8.4.1；本轮未启动该服务，32 次预算使用为 0。
- 原始/失败记录保留；命令、日志及公开副本经过共享 redactor 和路径脱敏。旧历史记录器不改写，
  新[记录器](raw/local/run_check.py)额外脱敏 argv；合成参数检查见 `raw/local/recorder-redaction-probe.*`。

## 设计与实际验证

本轮对照是旧 recording/exact/choice 行为与固定合同向量，不是性能 baseline 或消融。
Map 指令/input 原样编码；新完成值区分表示/model/usage、输出长度、非 stop，拒绝 trim/截断/补零。
v5 严格检查字段、计数、身份、首项及后续实际指令；golden 必须明确提供全部完成元数据。
原 Filter fixture/import/执行身份不变，旧 Adapter 不会自动获得 v5 支持。

公开 CLI 已能接收 Python 构造的 v5 task。固定 HTTP fixture 核对两消息、无 choice、stop=null、
max_tokens=128、原始 Unicode/空白及合法非 stop metadata；同 payload 序号 0→1 正常，重复序号终止。
这不是 PG C client→Python v5 互通或三参数 Map SQL 执行证据。

## 结果

| 检查 | 本地 | 服务器 | 来源 |
|---|---:|---:|---|
| PG/protocol/static/C 值与消息测试 | 108/108 | 108/108 | `425d2b1c-postgres.log` / `tests-postgres.log` |
| gateway CLI/migration | 6/6 | 6/6 | `425d2b1c-gateway.log` / `tests-gateway.log` |
| 原校准机制回归 | 10/10 | 10/10 | `425d2b1c-calibration.log` / `tests-calibration.log` |
| 原 choice 工具回归 | 11/11 | 11/11 | `425d2b1c-choice-tools.log` / `tests-choice-tools.log` |
| PG18.3 warning-free -O2 -Werror | 未在本地构建 | 通过 | `raw/server/build.log` |
| PGXS regression | 未在本地运行 | 1/1 | `raw/server/regression.log` |
| 旧 SQL 路径完整 TAP | 未在本地运行 | 1022/1022 | `raw/server/tap.log` |
| 七个 PG-independent C module 和 neutral header | 值测试内含 C11 | 全部通过 | `raw/server/c11-*.log` |

Python 汇总各为 108+6+10+11=135。全部本地最终日志在 `raw/local/425d2b1c-*`，
服务器[资格摘要](raw/server/qualification.json)记录源码、二进制、PG 版本及每步命令/退出码。
regression actual/expected 原始 SHA 相等；公开输出已去路径/行尾空白，不能冒充未处理原始字节。
[核对摘要](raw/verification.json)验证 40 个服务器公开文件及 98 个源码文件，与 Git 对象一致；
主清单为 [SHA256SUMS](raw/SHA256SUMS)。仓库外持久包保留 prefix、二进制、工作树与机器 preflight。
测试节点正常停止；本轮没有 gateway RSS/FD 或真实模型通过结论。

## 失败与独立复核

`raw/local/red-*` 保留逐步缺失 API 和断言失败；中间 dirty 记录及新文件尚未被跟踪时的部分源码哈希，
均不是最终资格。`c338d81b-*` 是修复前本地通过记录，不代替 `425d2b1c`。
服务器 c338d81b 只同步和建立工作树，没有启动验收；最终运行在独立的 425d2b1c 目录。
确认干净后已注销 c338d81b 临时工作树，提交及 bundle 保留，可重建；本轮正式证据未删除。

两路独立审查及复核见 [review.json](raw/local/review.json)：工程规范发现记录器 argv 脱敏、测试标签两项；
合同发现 v5 继承旧 JSON 超长数字错误分类一项，均已处理。
新增反例先复现 GATEWAY_INTERNAL 和坏首帧使 CLI 退出，再修复：v5 仅在读帧处分类输入错误；
Adapter 自身 ValueError 仍为内部错误。未知版本首帧不猜测错误帧版本，只关连接并继续服务。
旧 v3/v4 会话表现保留；不按错误文本判断或吞掉 Adapter 编程异常。

## 事实、不能声称与下一步

纯值、Python v5 与旧 PG 路径兼容已验证；三参数 Map 的 SQL/安装升级、schema 4 PG plan、
C port/wire 接线、PG＋golden、真实模型及资源验收仍未完成。原 SQL Map 仍是 recording。
本轮没有新增质量、真实成本、性能、组合/多会话、异步或 SemLoom PG 增量接口结论。
下一步按合同接 PG plan/常量/权限，再接 C v5 与完整 PG golden；通过后才使用已确认的真实服务预算。
