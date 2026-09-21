# OceanBase Community Edition 安装与启动流程（runbook）

日期：2026-07-31
分支：`claude/oceanbase-baseline`
配套：`README.md`（B1 门禁验证与部署阻塞结论）

本文档记录在本会话中**实际摸通的安装路径**与**已验证到「配置加载成功、线程起来」的启动命令**，以及踩过的配置坑。observer 最终在此 AutoDL 容器的 init step 4/18（clog）受阻（见 README §3.3），所以 bootstrap 与动态 AI_COMPLETE 验证两步**标注为「预期流程，本会话未执行」**，留给可部署环境复跑。

## 0. 为什么走这条路（而非 obd / all-in-one）

- `obdeploy` 不在 PyPI（`pip install obdeploy` → `No matching distribution`）。
- apt 仓库 `mirrors.oceanbase.com/oceanbase/community/stable/...` 只有 `oceanbase-ce` + `oceanbase-ce-libs`，**没有 `obclient`**（`E: Unable to locate package obclient`）。
- 官方 all-in-one `installer.sh` 能装 observer，但其收尾靠 `systemctl start oceanbase`；本容器无 systemd（PID 1 不是 systemd）→ systemctl 必失败，但 apt 装包已完成。
- **结论**：直接 apt 装 `oceanbase-ce` 拿 observer 二进制，手动起进程（`-N` nodaemon + nohup），用 `mysql` CLI / 适配器的 pymysql（OB MySQL 协议兼容）替代 obclient。

## 1. 前置

- Ubuntu 22.04（jammy）/ x86_64。
- 能访问 `mirrors.oceanbase.com`（国内直连）和 `obbusiness-private.oss-cn-shanghai.aliyuncs.com`（**注意 endpoint 是 oss-cn-shanghai，不是 hangzhou**；hangzhou 会返回 `AccessDenied` + 提示改用 shanghai）。
- root。
- 资源：observer `memory_limit` 最小约 6G；datafile ≥ 2G；data_dir 必须在**真实块设备**（非 overlay）。

## 2. 安装 OceanBase CE

方式 A——官方 installer（会触发 systemctl 报错，但 apt 装包成功）：

```bash
curl -s https://obbusiness-private.oss-cn-shanghai.aliyuncs.com/download-center/opensource/service/installer.sh -o /tmp/ob_installer.sh
bash /tmp/ob_installer.sh
# 末尾 "Failed to connect to bus: Host is down" 是 systemd 缺失，忽略；oceanbase-ce 已装
```

方式 B——手动 apt（更透明，推荐）：

```bash
echo "deb http://mirrors.oceanbase.com/oceanbase/community/stable/$(lsb_release -is | tr 'A-Z' 'a-z')/$(lsb_release -cs)/$(dpkg --print-architecture)/ ./" \
  > /etc/apt/sources.list.d/oceanbase.list
apt-get update
apt-get install -y oceanbase-ce oceanbase-ce-libs
```

装后产物：

- observer：`/home/admin/oceanbase/bin/observer`（`observer (OceanBase_CE 4.5.0.0)`）。
- 配置模板：`/etc/oceanbase.cnf`（installer 生成，**已知可用值**：`memory_limit=6G, system_memory=1G, datafile_size=2G, datafile_next=2G, datafile_maxsize=20G, cpu_count=16, log_disk_size=13G`）。
- admin SQL（含 `DBMS_AI_SERVICE` 包）：`/home/admin/oceanbase/admin/dbms_ai_service_mysql.sql` + `dbms_ai_service_body_mysql.sql`。

## 3. 补系统依赖（observer 不会自动拉）

```bash
apt-get install -y libaio1 default-mysql-client strace
# libaio1              —— observer 需要 libaio.so.1（oceanbase-ce-libs 不含，缺了会 "error while loading shared libraries"）
# default-mysql-client —— bootstrap/验证用（OB MySQL 协议兼容；也可直接用 driver env 里的 pymysql 2.2.8）
# strace               —— 排障（observer 自杀时定位 errcode 用）
```

验证 observer 能加载：

```bash
/home/admin/oceanbase/bin/observer --version        # observer (OceanBase_CE 4.5.0.0)
ldd /home/admin/oceanbase/bin/observer | grep "not found"   # 应无输出
```

## 4. 启动 observer（手动，不走 systemctl）

**关键**：容器无 systemd → systemctl 不可用；且 observer 的 daemon fork 在无 systemd 环境不可靠 → 必须加 `-N`（nodaemon）+ `nohup &`。

```bash
OBS=/home/admin/oceanbase/bin/observer
DATA_DIR=/root/obdata/ob1                 # 必须在真实块设备上（见 §4 配置坑）
mkdir -p "$DATA_DIR"
nohup "$OBS" -N -i eth0 -p 2881 -P 2882 -z zone1 -n obcluster -c 1 \
  -d "$DATA_DIR" \
  -o memory_limit=6G,system_memory=1G,datafile_size=2G,datafile_next=2G,datafile_maxsize=20G,cpu_count=16,log_disk_size=13G \
  > "$DATA_DIR/observer.out" 2>&1 &
```

本会话验证到：以上命令 observer 能 `Load config succ`（过 init step 2 配置阶段）并起 ~20 个线程。卡点在 step 4/18 clog（见 §7）。

### 4.1 配置坑（都踩过，有 strace 证据）

| 坑 | 现象 | 正确做法 |
|---|---|---|
| `memory_limit` 格式 | `2G` → `OB_INVALID_CONFIG (errcode=-4147) value out of [0M,)`；纯整数 `8192` → 被当 bytes≈0M 同样被拒 | **带 `G` 后缀的字符串**（`6G`），对齐 `/etc/oceanbase.cnf`；且 ≥ OB 最小值（约 6G） |
| daemon 模式 | 不加 `-N` 时 fork 后子进程在无 systemd 容器里静默早夭 | **必须 `-N`（nodaemon）+ nohup** |
| data_dir 文件系统 | overlayfs（容器 rootfs `/`）上 clog init 失败 | 放真实块设备（本环境 md0 `/root/autodl-tmp`，但只 7.5G 空；理想是单独数据盘） |
| 系统库 | `libaio.so.1` 缺失 → observer 加载失败 | `apt install libaio1` |

## 5. bootstrap（observer init 完成后）⚠️ 预期流程，本会话未执行

> observer 监听 2881 后，连 `root@sys` 做 cluster bootstrap。本会话 observer 没过 clog init，**此步未实际执行**，命令按 OB 文档给出。

```bash
mysql -h127.0.0.1 -P2881 -uroot -e "ALTER SYSTEM BOOTSTRAP CLUSTER TO 'zone1' DESCRIPTION 'obs1';"
# 之后建业务租户 / 库
```

## 6. 动态验证 AI_COMPLETE ⚠️ 预期流程，本会话未执行

> 门禁 #1（CE 含 `AI_COMPLETE`/`DBMS_AI_SERVICE`）已静态确证（见 README §2）。下面是注册指向同机 vLLM 的 endpoint 并测一次 `AI_COMPLETE` 的预期流程；本地 `code/src/baselines/products/oceanbase.py` 已用 pymysql 实现同样的调用序列。

```sql
CALL DBMS_AI_SERVICE.CREATE_AI_MODEL('qwen25b', '{"type":"completion","model_name":"qwen2.5-7b"}');
CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT('qwen25b_ep',
  '{"ai_model_name":"qwen25b","url":"http://127.0.0.1:8000/v1/chat/completions","access_key":"...","provider":"openai"}');
SELECT AI_COMPLETE('qwen25b', 'hello', JSON_OBJECT('temperature',0.0,'max_tokens',16));
```

## 7. 本容器的阻塞（重要）

当前 AutoDL 容器里，observer 装得上、配置能加载，但 init step 4/18（`clog/log_block_mgr.prepare_dir_and_create_meta_`，errcode -9100）`tgkill(self, SIGKILL)` 自杀。已 strace 排除 max_map_count（257 mmap、零失败）、overlayfs（md0 真实盘同样失败）、磁盘、配置。容器有 seccomp（`Seccomp: 2`）拦 `clone3`（ENOSYS）等；真因未完全定位，**容器内部不可修**（seccomp/kernel 参数只读）。详见 `README.md` §3.3。

**复跑 B1 必须换可部署环境**：带 systemd 的 VM，或特权容器（`--security-opt seccomp=unconfined` 或 `--privileged`，kernel 参数可写）。在该环境重复 §2–§6；本地 `code/src/baselines/products/oceanbase.py` 可直接复用。

## 8. 卸载（如需清理）

```bash
pkill -f bin/observer
apt-get purge -y oceanbase-ce oceanbase-ce-libs
rm -rf /root/obdata /home/admin/oceanbase /etc/oceanbase.cnf /etc/apt/sources.list.d/oceanbase.list
apt-get purge -y libaio1 default-mysql-client strace   # 可选
```

## 9. 远端已保留的本会话证据

- `/home/admin/oceanbase/`：apt 安装的 observer 4.5.0 + libs + admin SQL。
- `/etc/oceanbase.cnf`：installer 生成的已知可用配置。
- `/root/obdata/strace{2..7}.log`：`OB_INVALID_CONFIG`、`Load config succ`、clog `-9100 prepare_dir_and_create_meta_ failed`、`clone3 ENOSYS`、`tgkill(SIGKILL)` 全链证据。
