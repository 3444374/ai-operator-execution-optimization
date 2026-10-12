# SemLoom执行稳定性与多副本组合检查

`codex/semloom-stability-and-replicas`保留各修补的原提交身份。固定组合源码为`426eebafa16566671836319814b035c9543a86d4`；
本机270项检查中249通过、21跳过，0失败／错误。入口`474706f0`已并入且定点独立复审通过；本记录支持本机软件集成。

## 固定来源与必要依赖

| 来源 | 原提交 | 当前作用 |
|---|---|---|
| 稳定integration起点 | `b2006afe` | 既有接入与执行接口 |
| runtime派发提示 | `2a755809` | 暂停派发时queued不生成consumer immediate；READY仍可交付 |
| runtime明确失败 | `a21fc084` | actor可信同任务payload失败终结本成员；未知RPC／HTTP／取消继续持有 |
| 多副本及其修补 | `fe490e50`、`eb2e8692` | 明确endpoint容量；observer发送前失败及监听身份核对 |
| 入口正文与期限 | `e960fc91`、`474706f0` | 读取前预留正文；明确native停止证明；全部错误writer沿用原截止时刻 |
| 必要有限native服务 | `3f6aaea6`、`c9023fbc`、`d487545b`、`2fad623e` | NativeCall API、有限snapshot及清理证明 |
| 组合反例 | `de04a89d` | 两类发送前结果与另一Job未知责任同时存在 |

`d90f0220`是必要祖先链中的既有服务计划，只包含说明。有限服务三份运行正文在`2fad623e`与`9c642ec7`相同。
本分支没有引入`650f65c8`、`9c642ec7`、`5ee1b90c`、`fa53eb16`、`f9921475`或`5e95852d`的额外接入提交。
九份产品实现逐文件等于相应owner固定提交；只有adapter README发生说明冲突，已保留两侧实际接点。
native_calls本身等于必要祖先`3f6aaea6`；supplier、persistent／shared CLI及daft_runtime没有本次整合改动。

## 反例与组合观察

| 检查对象 | 结果 |
|---|---|
| payload任务身份错误 | 真实actor逻辑确认HTTP未进入；只结束该成员并释放相应Core／对象责任，首错保持 |
| observer发送前失败 | 走既有本地错误结果，child／RPC／put均0；结果租约释放后本Job用量归零 |
| 另一Job未知RPC或HTTP | 两个子场景均保留H=1、C=1、W=1、input=5、result=512及37字节对象；多次advance和局部teardown不产生终态 |
| 后续query复用 | 已归还payload成员完成下一query；两个不同block各释放一次，未知Job用量保持，无请求重试 |
| native停止证明缺失 | None、缺字段、字段None、非布尔值及仅clean=False均保留仍被外部线程持有的4字节正文，报告停止未确认 |
| 合成错误写出期限 | 413／502慢prepare与write_eof、期限已过、关闭连接失败；原first error保持，超时结束协议处理，不启动自动错误writer |
| 监听身份 | 合成proc／resolver验证地址族、IP、port、inode、PID／启动时刻；拒绝同端口错误地址、错误进程及缺地址族 |

组合测试使用实际Core、backend、actor与driver逻辑，Ray ObjectRef及HTTP方法结果由fixture控制，组合自身wire HTTP为0。
其余回归使用本机有限HTTP／Unix socket fixture。所有实际Ray集群、Daft图、模型、GPU及远端服务器运行均0。
12项跳过因缺Arrow／Daft批次依赖；其余4项native实际图、2项副本实际库、3项runtime实际库需要显式启用，当前未启用。

runtime A/B独立审阅通过；`eb2e8692`定点复审16项通过，入口`474706f0`定点复审9项通过，两份复审的Standards／Spec必修均0。
修后的实际Ray／Daft／Arrow多Job组合、真实Linux设备collector、模型内容身份、PG语义／取消及性能仍分别待验证。
旧`fe490e50`的actual03及旧入口服务器记录不转记为本组合或后续修补的资格。

## 旧分支与下一接入

当前本地原分支runtime、多副本、入口`474706f0`、sema-native-service及semantic-adapter-integration均完全包含。
原生相关分支仍保留独有提交：sema-native-executors 6、sema-native-library-checks 4、sema-imlane-adapter 4、
native-executor-baselines／imlane-calls各7、LOTUS native 4、DuckDB native 5、IMLane build fixture 1。
没有删除旧分支或工作树，main未改。原生owner在`codex/native-execution-adapters`独立收敛，再以固定两分支源码组合检查。
必要服务祖先已在本分支保留，后续原生整合应沿该身份去重；各自通过不能替代组合检查。

## 证据与复现

[紧凑验证记录](verification.json)保存固定来源、九份实现摘要、全部模块和跳过原因、两个未知子场景及原始日志摘要。
完整原件在仓库外按运行标识`semloom-stability-local-intake-fix`留存，包含之前263项候选运行和本轮270项运行，失败原件不改写。
独立复审原件按记录中的报告标识和SHA-256向维护者取得；仓库不保存机器目录、远端地址或服务器访问配置。

在仓库根运行，需已有aiohttp／httpx；不安装缺失依赖：

```sh
SEMLOOM_NATIVE_CALL_TEST=0 SEMLOOM_REPLICA_RAY_TEST=0 SEMLOOM_RAY_FAILURE_TEST=0 \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=code:code/tests/execution_provider python3 - <<'PY'
import json, unittest
from pathlib import Path
record = json.loads(Path('experiments/results/diagnostics/semloom_stability_composition_20261012/verification.json').read_text())
suite = unittest.defaultTestLoader.loadTestsFromNames(record['modules'])
result = unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(0 if result.wasSuccessful() else 1)
PY
```
