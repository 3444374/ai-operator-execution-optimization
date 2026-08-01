# OceanBase B1 baseline：门禁 #1 验证通过，当前容器部署受阻

日期：2026-07-31
分支：`claude/oceanbase-baseline`
平台：AutoDL 2×4090 容器（Ubuntu 22.04 jammy），Qwen2.5-7B + 双 vLLM
对应计划：`experiments/plans/database_ai_operator_baseline_matrix_20260729.md` §2（B1）

## 1. 研究问题与门禁

B1 = OceanBase 原生 `AI_COMPLETE` → 同机双 vLLM（matrix §2 第一层核心 baseline），角色是"现有数据库 AI 算子产品 baseline"。

matrix §2 把以下列为 **formal 前置门禁 #1**：「当前可部署的 Community Edition 镜像确实包含 `AI_COMPLETE` 和 `DBMS_AI_SERVICE`」。门禁失败时降为工业参考，不伪造等价 arm。本文档记录该门禁的验证结果与部署尝试。

## 2. 门禁 #1 结果：✅ 通过（静态确证）

远端 `apt install oceanbase-ce`（mirrors.oceanbase.com community/stable，Ubuntu jammy）安装 **OceanBase Community Edition 4.5.0.0**。静态检查 observer 二进制与种子 SQL：

- **observer 二进制** `/home/admin/oceanbase/bin/observer` 的 `strings` 包含：
  - `T_FUN_SYS_AI_COMPLETE`、`ai_complete` 函数入口与完整错误信息（`ai_complete, prompt is not valid`、`ai_complete, input is empty` 等）。
  - `DBMS_AI_SERVICE` 全套过程：`CREATE_AI_MODEL`、`CREATE_AI_MODEL_ENDPOINT`、`ALTER_AI_MODEL_ENDPOINT`、`DROP_AI_MODEL`、`DROP_AI_MODEL_ENDPOINT`（PRAGMA INTERFACE(C, ...) 绑定）。
- **种子 SQL**：`/home/admin/oceanbase/admin/dbms_ai_service_mysql.sql` + `dbms_ai_service_body_mysql.sql`（PACKAGE spec + body，bootstrap 时装入 `sys` 租户）。

**结论**：`AI_COMPLETE` + `DBMS_AI_SERVICE` 存在于 **Community Edition**（非企业版独占）。本地 `code/src/baselines/oceanbase.py` 适配器调用的 `DBMS_AI_SERVICE.CREATE_AI_MODEL` / `CREATE_AI_MODEL_ENDPOINT` 与 `AI_COMPLETE(...)` SQL 都是 CE 实际支持的对象，B1 不是 strawman 对照。

## 3. 部署尝试：❌ 当前容器无法运行 observer

> 完整安装与启动流程（可复用 runbook + 配置坑表 + 已验证启动命令）见 `install_runbook.md`。

### 3.1 已完成
- 安装 `oceanbase-ce` 4.5.0.0 + `oceanbase-ce-libs` + `libaio1` + `strace` + `default-mysql-client`。
- 资源核对：32 核 / cgroup 内存上限 240G（`memory.max=257698037760`，当前用 20G）/ 真实网卡 `eth0`（IP 172.17.0.8）。

### 3.2 已逐个修复的问题

| 问题 | 现象 | 修复 |
|---|---|---|
| `obdeploy` 不在 PyPI；`obclient` 不在 apt 仓库 | pip/`apt install` 都找不到 | 直接用 observer 二进制 + `mysql` CLI / 适配器的 pymysql |
| all-in-one installer 依赖 systemctl | `System has not been booted with systemd as init system (PID 1)` | 绕过 installer，手动起 observer |
| observer 缺 `libaio.so.1` | `error while loading shared libraries` | `apt install libaio1` |
| `memory_limit` 配置 | `2G` → `OB_INVALID_CONFIG`（低于最小值）；`8192` → 被当 bytes 解析≈0M | 用 installer 生成的 `/etc/oceanbase.cnf` 已知可用值 `memory_limit=6G` |
| daemon fork 在无 systemd 容器失败 | 后台 daemon 子进程早夭 | `-N`（nodaemon）+ `nohup &` |

修复后 observer 能解析配置（`Load config succ`）并起 ~20 个线程。

### 3.3 仍阻塞：clog init（step 4/18，errcode -9100）

observer 在 init step 4/18（`init_io` → `ob_server_log_block_mgr.prepare_dir_and_create_meta_`，commit-log 池初始化）`tgkill(self, SIGKILL)` 自杀。strace 已排除下列（都不是根因）：

- **`vm.max_map_count=65530`（只读）**：实际只 257 次 mmap、零失败。
- **overlayfs**：data_dir 放 `/root/autodl-tmp`（md0 真实块设备，非 overlay）同样失败。
- **磁盘空间**：失败时 `df` 未变（observer 没写盘就死）。
- **配置**：`memory_limit=6G` 已 `Load config succ`，过 step 2。

容器有 seccomp 过滤（`/proc/self/status`：`Seccomp: 2`、`Seccomp_filters: 1`），`clone3` 系统调用返回 `ENOSYS`（38 次）。但 observer 实际成功起了 ~20 个线程（`clone` 回退生效），故 **clone3 的 ENOSYS 未必是直接死因**——真正触发 clog `-9100` 的是某个我没完全定位的容器/OB 环境不兼容点。

**经验性结论**：此 AutoDL 容器无法初始化 OceanBase observer；seccomp profile 与只读 kernel 参数从容器内部不可修改，无法在本机修复。

### 3.4 2026-08-02 独立目录复核：阻塞仍然存在

在代码更新后使用全新、不覆盖旧证据的目录
`/root/autodl-tmp/experiment-artifacts/oceanbase_recheck_e674e71_20260802`
重新执行启动门禁。机器仍为普通 AutoDL Docker 容器，`Seccomp: 2`、
`Seccomp_filters: 1`、`vm.max_map_count=65530` 且容器内不可写。observer 再次在
init step 4/18 退出；本次 `observer.log` 明确记录
`prepare_dir_and_create_meta_ failed`、errcode `-9100` / `OB_NO_SUCH_FILE_OR_DIRECTORY`，
2881 未监听。该复核没有执行 bootstrap 或 AI SQL，因而没有产生 B1 性能数据。

这次复核强化的是“当前容器部署门禁稳定失败”，仍不能把原因唯一归结为
`vm.max_map_count` 或断言 OceanBase 产品不可运行；后续不再在相同容器条件下重复消耗
实验时间，只有 seccomp-unconfined/privileged 容器或 VM 条件变化时才重开动态 B1。

## 4. 保留的证据（远端）

- `/home/admin/oceanbase/`：apt 安装的 observer 4.5.0 + libs + admin SQL（含 `dbms_ai_service_*.sql`）。
- `/etc/oceanbase.cnf`：installer 生成的已知可用配置模板（`memory_limit=6G` 等）。
- `/root/obdata/strace{2..7}.log`：多次 strace，含 `OB_INVALID_CONFIG`、`Load config succ`、clog `-9100 prepare_dir_and_create_meta_ failed`、`clone3 ... ENOSYS`、`tgkill(SIGKILL)` 全链证据。
- `/root/obdata/{diag*,ob*}/`：各次启动尝试的 data_dir + stdout。

## 5. 实验参数与指标定义

> 状态：**待部署后采集**。以下为 B1 复跑时的实验级参数与指标词汇定义；本会话 observer 未跑通（见 §3.3），未实际采集到任何数值。此节只为补齐实验级词汇，不构成任何实测结果。

B1 复跑时将使用的实验级参数（调度 / 数据组织层，区别于 §9 的 observer 部署参数）：

| 参数 | 含义 | B1 取值 |
|---|---|---|
| `K`（active work 上限） | 同时在途的请求数 / work 上限，控制对 vLLM 的并发注入 | 待部署后按 matrix §2 设定 |
| `W`（credit 配额） | 每次请求完成后释放 / 补充的 work credit，与 K 共同决定饱和度 | 待部署后按 matrix §2 设定 |
| `token_budget` | 动态 batching 的 token 预算，决定多少行合并为一个 batch（行间合并，每行仍是独立完整请求，不做行内拆分） | 待部署后按 matrix §2 设定 |
| `batch_size` | 单次请求的行数 / token 数组织粒度 | 待部署后按 matrix §2 设定 |

B1 复跑时将采集的指标（与 `experiments/AGENTS.md` §6 新实验指标完整性要求一致）：

| 指标 | 含义 | 状态 |
|---|---|---|
| `tokens/s` | 吞吐主指标，从 vLLM Prometheus `prompt_tokens_total + generation_tokens_total` 计算（AI_COMPLETE 中每行 token 量差异大，不能只看 rows/s） | 待采集 |
| `throughput`（rows/s） | 行级吞吐，作为 tokens/s 的补充对照 | 待采集 |
| `service_p99` | 系统性尾延迟（per-request e2e latency 分布 p99） | 待采集 |
| SLO violation | 违反时延 SLO 的请求占比 | 待采集 |
| MFU | model utilization（GPU 计算利用率） | 待采集 |

注意：本会话未产生上述任何参数的实测取值或指标的实测数值；§4 列出的远端证据仅为安装 / 调试日志，不含运行期指标。

## 6. 实验数据

**当前无 `runs.csv`** —— observer init 失败（见 §3.3 clog `-9100`），端到端链路（OceanBase `AI_COMPLETE` → 同机双 vLLM → 写回）未执行，故本目录无 `runs.csv` / `summary_long.csv` / `comparison_summary.csv` 等任何结果 CSV。

复跑需可部署环境（带 systemd 的 VM 或特权容器，见 §8「下一步 / 复跑条件」）；在 observer 跑通后，由 `code/src/baselines/oceanbase.py` 适配器驱动产生上述 CSV。

## 7. 能声称 / 不能声称

- ✅ **能**：OceanBase CE 4.5.0 内置 `AI_COMPLETE` + `DBMS_AI_SERVICE`（门禁 #1 通过）；B1 测的是 OB 真实原生 AI 算子，不是 strawman。
- ❌ **不能**：B1 的任何性能数字（observer 未跑通，未执行 `AI_COMPLETE`）。
- ❌ **不能**：`DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT` → 同机 vLLM 的连通性、`AI_COMPLETE` 端到端正确性（未动态验证）。
- ❌ **不能**：「OceanBase 不适合做 baseline」——仅此容器部署受阻，是环境问题，不是产品能力问题。

## 8. 下一步 / 复跑条件

- 按 matrix §2：当前把 OceanBase **降为"工业系统参考 / 待部署"**，不伪造 B1 数字；待可部署环境就绪再补 formal。
- 复跑 B1 所需环境（任一）：带 systemd 的 VM；或特权容器（`--security-opt seccomp=unconfined` 或 `--privileged`，且 kernel 参数可写）。在该环境重跑：observer + bootstrap → `DBMS_AI_SERVICE` 注册 AI model endpoint 指向同机双 vLLM → 灌 `sharegpt_multiturn` → 跑 B1 → 与 B0/B2/B4 对比。
- 复跑时直接复用本地 `code/src/baselines/oceanbase.py`（其对 `DBMS_AI_SERVICE` / `AI_COMPLETE` 的调用已确证 CE 支持）；pymysql 2.2.8 已在 driver env。

## 9. 附：用于复跑的已验证启动参数

在可部署环境（能起 observer 的）里，以下参数已在本会话验证到「配置加载成功、线程起来」这一步（卡在容器 clog，非参数问题）：

```bash
OBS=/home/admin/oceanbase/bin/observer
mkdir -p <data_dir on a real (non-overlay) filesystem>
nohup "$OBS" -N -i eth0 -p 2881 -P 2882 -z zone1 -n obcluster -c 1 \
  -d <data_dir> \
  -o memory_limit=6G,system_memory=1G,datafile_size=2G,datafile_next=2G,datafile_maxsize=20G,cpu_count=16,log_disk_size=13G \
  > <observer.out> 2>&1 &
# 然后 bootstrap（连 2881 root@sys）+ 跑适配器
```

注意：`memory_limit` 必须用带 `G` 后缀的字符串（cnf 同款），不能用纯整数（被当 bytes）；`-N` 必须加（容器内 daemon fork 不可靠）。
