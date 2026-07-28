# AutoDL 云部署指南

本指南沉淀 2026-07-27 把项目部署到 AutoDL(2× GPU 云服务器)的全流程经验,目标是可在云上复现本机实验并补"多 endpoint / 多 GPU"真实验证缺口(见根 `AGENTS.md` §3、`motivation/results/gpu/multi_endpoint_ray_motivation_20260712.md` 第 83 行)。

指南面向"从零起一台 AutoDL 实例到跑通首个多 endpoint 实验"。所有命令均为 Linux bash(远端)。

## 新对话 / 新 agent 的唯一操作入口

本文件是 AutoDL 环境准备、开机恢复、实验启动和故障恢复的单一 runbook。
新对话不要先搜索历史聊天或重新猜测安装路径，按以下顺序读取：

1. 根 `AGENTS.md`：项目边界与实验规则；
2. 根 `PROJECT_OUTLINE.md` 和
   `experiments/plans/experiment_status_and_gaps.md`：当前唯一实验顺序；
3. 本节：判断是“全新实例准备”还是“已配置实例开机恢复”；
4. 本文件对应的详细章节；实验参数只从 `deploy/autodl/*.example.json`
   模板读取。

| 当前状态 | 直接执行 |
|---|---|
| 全新 AutoDL 实例，尚无代码/环境/模型/数据库 | 下方“全新实例从零准备”，细节依次看 §1、§3–§8 |
| 已配置实例重新开机，服务均停止 | 下方“开机后完整恢复流程” |
| 服务已运行，只需继续实验 | 从“实验 gate 与正式启动”开始，先检查现存 runner |
| runner 中断或依赖服务短暂失败 | 修复依赖后使用同配置、同输出目录加 `--resume`；禁止删目录重跑 |

### 固定路径与职责

| 项目 | 当前约定路径 | 说明 |
|---|---|---|
| Git 仓库 | `/root/autodl-tmp/ai-operator` | 只从 GitHub `main` 同步；保留未跟踪实验结果 |
| runtime env | `/root/autodl-tmp/ai-operator-runtime.env` | 仓库外保存模型、端口、CUDA、容量和 workload 参数 |
| driver Python | `/root/miniconda3/bin/python` | 运行 Ray/Daft/profiler/scenario runner |
| vLLM Python | `/root/autodl-tmp/venvs/vllm-4090/bin/python` | 只运行 vLLM endpoint |
| 模型 | `/root/autodl-tmp/models/<model>` | 由 runtime env 的 `MODEL_PATH` 指向 |
| vLLM 日志 | `/root/autodl-tmp/vllm_logs/` | 每个端口有 log/PID 文件 |
| 编排日志 | `/root/autodl-tmp/logs/` | endpoint 启动、gate、formal runner 日志 |
| 临时 gate 配置 | `/root/autodl-tmp/gates/` | 仓库外机械缩小正式模板，不作为正式结果 |
| 正式结果 | 仓库内 `experiments/results/<unique_run_id>/` | 输出目录必须在启动前不存在 |

### 全新实例从零准备

以下步骤只做一次；纯下载/安装优先在无卡模式完成：

1. 按 §1 选择 CUDA 13.0 镜像和非 Blackwell 双卡，确认宿主驱动 ≥580。
2. 按 §3 从 GitHub 克隆到固定仓库路径，不能上传本地 tar 包替代 Git。
3. 按 §4 安装 driver 依赖；vLLM 0.25.1 使用独立 venv，缓存放数据盘。
4. 复制 `autodl.env.example` 到仓库外 runtime env，逐项填写模型、CUDA、
   GPU/端口、vLLM capacity、数据库、workload、SLO 与 MFU 峰值口径。
5. 按 §5 通过 `download_model.sh` 下载模型，并检查关键文件非空。
6. 按 §6 安装/初始化 PostgreSQL 18 + pgvector，创建 `ai_operator` 数据库。
7. 按 §7 导入 ShareGPT/BurstGPT workload，核对目标
   `workload_name` 的行数、prompt token 上限和模型 context 契约。
8. 执行下方“开机后完整恢复流程”，再做 64 行真实 GPU gate。

环境准备完成的判据不是“命令执行过”，而是上述固定路径存在，driver/vLLM
两个 Python 环境版本可查询，数据库中的目标 workload 行数正确，两个 endpoint
健康，64 行 gate 的 manifest、CSV 和 traces 完整。

### 开机后完整恢复流程

每次 AutoDL 重新开机都按此顺序执行，不要只启动 vLLM：

```bash
cd /root/autodl-tmp/ai-operator

# 1) 先确认没有实验 runner；有 runner 时停止，不得同步代码或重复启动
ps -C python -C python3 -o pid=,etime=,args= |
  grep '[r]un_ai_operator_scenarios.py' || true

# 若准备恢复已有输出，再检查目录级租约；不能只凭 ps 结果判断可恢复
OUTPUT_DIR=experiments/results/<existing_run_id>
test ! -e "$OUTPUT_DIR/.runner-lease.json" ||
  cat "$OUTPUT_DIR/.runner-lease.json"

# 2) 同步代码。未跟踪 experiments/results/ 属于实验数据，不得 git clean
git status --short --branch
source /etc/network_turbo >/dev/null 2>&1
git fetch origin main
git merge --ff-only origin/main

# 3) 先恢复 PostgreSQL，再核对版本、扩展和正式 workload
pg_ctlcluster 18 main start 2>/dev/null || service postgresql start
pg_isready -h 127.0.0.1 -p 5432
PGPASSWORD=postgres psql -h 127.0.0.1 -U postgres -d ai_operator \
  -c "SELECT extversion FROM pg_extension WHERE extname='vector'"
PGPASSWORD=postgres psql -h 127.0.0.1 -U postgres -d ai_operator \
  -c "SELECT workload_name, count(*) FROM documents GROUP BY workload_name"

# 4) 核对仓库外 runtime env 和关键路径
test -f /root/autodl-tmp/ai-operator-runtime.env
set -a
source /root/autodl-tmp/ai-operator-runtime.env
set +a
test -x "$VLLM_VENV/bin/python"
test -d "$MODEL_PATH"
test -d "$CUDA_HOME"
printf 'model=%s gpus=%s ports=%s workload=%s\n' \
  "$COMPLETION_MODEL" "$GPU_IDS" "$PORTS" "$SOURCE_WORKLOAD_NAME"

# 5) 启动受管 endpoint；脚本自行轮询 health/models
bash deploy/autodl/start_endpoints.sh \
  /root/autodl-tmp/ai-operator-runtime.env

# 6) 独立复核两个 endpoint、真实参数和每卡进程
curl -fsS http://127.0.0.1:8000/health
curl -fsS http://127.0.0.1:8001/health
curl -fsS http://127.0.0.1:8000/v1/models
curl -fsS http://127.0.0.1:8001/v1/models
ps -C python -C python3 -o pid=,etime=,args=
nvidia-smi --query-gpu=index,name,utilization.gpu,memory.used,memory.total \
  --format=csv,noheader
```

非交互 agent 不要让首次 JIT 长时间占住 SSH channel。把第 5 步用
`nohup ... </dev/null >唯一日志 2>&1 &` 后台启动，再用短连接轮询日志与
`/health`。启动命令中必须能看到与 runtime env 一致的
`--max-num-batched-tokens`、`--max-num-seqs`、prefix-cache 和 MFU flags。

### 实验 gate 与正式启动

1. 从 `experiment_status_and_gaps.md` 选当前实验，只使用对应已提交模板。
2. gate 配置放 `/root/autodl-tmp/gates/`，只机械修改
   `experiment_id`、`total_rows/db_fetch_rows=64`、`warmup=0`、
   `formal=1` 和保留一个场景；其他参数必须与正式模板相同。
3. gate 通过条件：manifest=`completed`、runs status=`ok`、64 个唯一请求
   exactly-once、request/submission/resource traces 非空、两 endpoint 都有
   submission、`resource_metrics_status=mfu_status=ok`、0 failure。
4. 正式启动前再次检查没有 runner，且正式输出目录和日志均不存在。

```bash
cd /root/autodl-tmp/ai-operator
set -a
source /root/autodl-tmp/ai-operator-runtime.env
set +a

CONFIG=deploy/autodl/<current_template>.example.json
OUTPUT_DIR=experiments/results/<unique_run_id>
RUN_LOG=/root/autodl-tmp/logs/<unique_run_id>.log

ps -C python -C python3 -o args= |
  grep '[r]un_ai_operator_scenarios.py' && exit 1 || true
test ! -e "$OUTPUT_DIR"
test ! -e "$RUN_LOG"

nohup /root/miniconda3/bin/python \
  code/scripts/run_ai_operator_scenarios.py \
  --config "$CONFIG" \
  --profiler code/scripts/postgres_ai_operator_profile.py \
  --python-executable /root/miniconda3/bin/python \
  --output-dir "$OUTPUT_DIR" \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls "$MODEL_METRICS_URLS" \
  --idle-timeout-s 120 \
  </dev/null >"$RUN_LOG" 2>&1 &
```

启动后用短连接检查 runner、`manifest.json`、`runs.csv` 和 GPU。只有
manifest 原子记录首个成功 run、无 incident 且 GPU 工作，才算启动完成。
runner 会在输出目录中原子创建 `.runner-lease.json`，其中记录 host、PID、
进程启动身份、config fingerprint 和代码提交；同一输出目录只允许一个写者。

中断恢复必须复用原 config/output，并先同时检查精确脚本进程和租约：

- 有精确 runner 进程或租约 owner 仍存活时，禁止启动第二个 runner。
- 无进程但租约仍在时，先审计租约字段与 manifest。确认是同一配置、同一输出
  且旧 owner 已消失后，才允许使用 `--resume --recover-stale-lease`。
- 不要手工删除租约、拼接 CSV 或删除失败证据；显式恢复会把 stale-lease
  事件写入 manifest incident，并标记 recovered。
- 正常结束或受控失败会释放租约；进程被强制终止时故意保留租约作为事故证据。

## runtime env 与脚本速查（不是完整开机流程）

本节只说明如何把部署参数交给脚本；每次开机仍必须执行上方完整恢复流程，
不能跳过 PostgreSQL/workload 和 runner 门禁。新环境先复制一份运行配置到
仓库外，再按同一配置下载和启动：

```bash
cd /root/autodl-tmp/ai-operator
cp deploy/autodl/autodl.env.example /root/autodl-tmp/ai-operator-runtime.env
# 切模型：MODEL_ID / MODEL_DIR / MODEL_PATH / COMPLETION_MODEL / token 上限；
# 切数据集：SOURCE_WORKLOAD_NAME；切 GPU/端口：GPU_IDS / PORTS / 峰值口径。

bash deploy/autodl/download_model.sh /root/autodl-tmp/ai-operator-runtime.env
bash deploy/autodl/start_endpoints.sh /root/autodl-tmp/ai-operator-runtime.env
```

`download_model.sh` 每次都会显式加载 `/etc/network_turbo` 并设置
`HF_HUB_DISABLE_XET=1`，不依赖当前 shell 或操作者记得学术加速。
`start_endpoints.sh` 不再写死 1.5B/7B、GPU 数、端口或 context length，也不会
使用宽泛的 `pkill -f`。它只会在明确设置 `STOP_MANAGED_ENDPOINTS=1` 时，根据
自身 PID 文件停止之前由同一脚本启动的 endpoint。脚本会把
`$VLLM_VENV/bin` 加入 `PATH`，使 FlashInfer JIT 能找到同一环境里的
`ninja`；`CUDA_HOME` 与 `CUDA_NVCC_BIN` 仍必须在 runtime env 中明确指向
同一套 CUDA toolkit。

模型和已导入数据库的 workload 可分别通过 `COMPLETION_MODEL` 与
`SOURCE_WORKLOAD_NAME` 切换，不修改代码。新的原始数据格式仍需先转换为
`documents` 的统一字段契约；当前自带 importer 只直接识别 ShareGPT +
BurstGPT，不能把“运行时可切 workload”误写成“任意 raw schema 零适配”。

---

## 0. 适用范围与边界

- 部署对象:PostgreSQL+pgvector、vLLM(多 endpoint)、Ray、Daft、项目代码与 workload 数据。
- **平台边界**:云上结果必须标注 AutoDL 平台、GPU 型号、vLLM 安装方式、PG 版本(见 §11),不得混同本机 RTX 5070 / PG18.4 Docker 结论。
- 本指南**不是** Docker Compose 部署(本机 `deploy/pgai/`、`deploy/postgres18.4/` 才是)。AutoDL 实例本身就是一个 Linux 主机,直接装服务,不做 Docker-in-Docker。

---

## 1. 实例选型

| 项 | 推荐值 | 说明 |
|---|---|---|
| 计费 | **按量计费** | 间歇实验,包月/包日浪费 |
| GPU | 2× 同型号(3090/4090/6000 等) | 每个 endpoint 独占一张卡;本次实测 2× RTX 6000D(85GB/张) |
| 镜像大类 | **基础镜像**(非社区镜像) | 干净、版本自控;社区镜像里 vLLM 版本不可控 |
| 镜像版本 | **PyTorch 2.12.1 / Python 3.12 / Ubuntu 22.04 / CUDA 13.0** | vllm 0.24/0.25 需 cu130→驱动 ≥580→**必须选 CUDA 13 镜像**(见 §1.1);只跑老 cu12 vllm(<0.23)才选 12.8 |
| 数据盘 | 默认(`/root/autodl-tmp`,50GB+) | 模型+数据落这里,跨开关机保留 |
| 文件存储 `autodl-fs` | **不开** | 付费跨实例持久化;模型重下比买存储划算 |
| 学术资源加速 | **非自动**,每个 shell 会话需 `source /etc/network_turbo` | 加速 github/huggingface(代理 `172.20.0.113:12798`);不 source 则三源全慢(见 §5) |

**开机顺序(省钱)**:先「无卡模式开机」(仅 CPU,约 ¥0.1/h)→ 下模型、装依赖、配环境 → 关机 → 「正常开机」(GPU 模式)→ 跑实验 → 跑完立刻关机。多卡按量计费烧钱快,纯 CPU 的 setup 阶段不要用 GPU 模式。

### 1.1 ⚠️ 镜像 CUDA 版本 = 宿主驱动能力(首要判据,踩过坑)

AutoDL 上**镜像的 CUDA 版本决定宿主驱动**:选 CUDA 12.8 镜像 → 驱动 570(最高 CUDA 12.8);选 CUDA 13.0 镜像 → 驱动 580+(支持 CUDA 13)。这是选镜像的首要判据,**比镜像里的 PyTorch 版本重要得多**(PyTorch 会被 uv 覆盖,驱动不会)。

- **vllm 0.24/0.25 自带的 flash-attn 是 cu13 编译** → 需驱动 ≥580 → **必须用 CUDA 13.0 镜像**。
- 若用 CUDA 12.8 镜像(驱动 570),vllm 0.24/0.25 启动报 `CUDA error: CUDA driver version is insufficient for CUDA runtime version`(2026-07-28 在 4090 + 12.8 镜像上实测,flash-attn 的 hardware_info 触发)。
- 踩坑经过:最初按"镜像 PyTorch 版本不重要、uv 会覆盖"选了 12.8,漏看了 vllm 0.25.1 的 cu130 要求驱动 580+——**选镜像先看 vllm 的 CUDA 要求,再看驱动**。
- 反例:6000D 驱动 595(够新)但 sm120 让 flashinfer 挂;4090 + 12.8 镜像 sm89 OK 但驱动 570 跑不了 cu13。**正确组合:4090(sm89)+ CUDA 13 镜像(驱动 580+)+ vllm 0.25.1(cu130)**,三者齐备。

---

## 2. 连接与非交互执行

AutoDL 给 SSH(`ssh -p <端口> root@connect.<region>.seetacloud.com` + 密码)。从本地驱动远端时注意:

### 2.1 conda base 环境不在默认 PATH
非交互 SSH(以及 `python -c`、`cron`)不加载 `.bashrc`,拿不到 `python`/`pip`。两条路:
- 用登录 shell 包命令:`bash -lc '<cmd>'`;
- 或显式 `source /root/miniconda3/etc/profile.d/conda.sh && conda activate base && <cmd>`。

miniconda 路径固定:`/root/miniconda3`(Python 3.12.3)。

### 2.2 从 Windows Git Bash 驱动:关掉 MSYS 路径转换
Git Bash 会把独立的 `/root/...` 参数改写成 `C:\Program Files\Git\root\...` 传给 `python.exe`。驱动远端命令前必须:
```bash
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
```
否则远端 `mkdir -p` / `tar -C` 的目标路径会被改坏。

### 2.3 长任务不要占着 SSH channel
下载模型、`pip install` 这类几分钟到几十分钟的任务,**不要**用一个 `exec` 长连接等它(`stdout.read()` 会卡,且 paramiko 默认超时会砍断远端进程)。正确做法:在远端 `nohup ... </dev/null >log 2>&1 & disown` 后台启动、立即返回,然后用短连接轮询日志/文件大小。stdin 重定向 `</dev/null` 很关键,否则后台进程会占住 channel 的 stdin 管道导致不返回 EOF。

### 2.4 不要用会自匹配的 `pkill -f`
`pkill -f <模式>` 会匹配到"正在执行 pkill 的那个 shell"自己的命令行(里面就含该模式),**自杀**(踩过 3 次)。替代:
- 按精确 PID:`kill <pid>`;
- 按进程名精确匹配:`kill $(pgrep -x wget)`;
- 按父 PID 取子:`kill $(pgrep -P <launcher_pid>)`。

`pgrep -af <长命令模式>` 做“是否已有 runner”检查也可能匹配当前 shell，尤其
当同一条 shell 命令后半段包含待启动的完整 runner 命令时。优先限定进程名：

```bash
ps -C python -C python3 -o pid=,args= |
  grep 'run_ai_operator_scenarios.py'
```

正式启动还必须用“输出目录不存在”作为第二道幂等门禁。

---

## 3. 代码:从 GitHub 克隆(不要上传本地拷贝)

**项目代码必须从 GitHub 克隆**,不要从本机打包上传(本指南初版用过 tar 上传,是错误做法——不可复现、缺文件、无法同步)。克隆后用 `git pull` 同步代码与文档改动,实验产出也可 `git` 提交回库。

```bash
source /etc/network_turbo >/dev/null 2>&1   # 加速 github
cd /root/autodl-tmp
git clone https://github.com/3444374/ai-operator-execution-optimization.git ai-operator
cd ai-operator
```

- **私有库**:用带 token 的 HTTPS(`https://<token>@github.com/3444374/ai-operator-execution-optimization.git`)或上传 SSH key 到 AutoDL 实例后再走 SSH。
- **后续同步**:`cd /root/autodl-tmp/ai-operator && git pull`。
- 实验脚本入口在 `code/scripts/`,策略实现在 `code/src/`,依赖清单 `code/requirements.txt`。运行时 cwd 用项目根 `/root/autodl-tmp/ai-operator`(脚本里相对路径 `data/raw/...`、输出 `experiments/results/...` 都基于根)。

---

## 4. Python 依赖与版本兼容性 ⭐(重点)

### 4.1 目标版本(与本机对齐)
| 包 | 版本 | 来源 / 说明 |
|---|---|---|
| Python | 3.12.3 | 镜像自带 `/root/miniconda3` |
| **vllm** | **0.25.1** | **与本机 Docker `vllm/vllm-openai:v0.25.1` 对齐**(见 `experiments/results/adaptive_admission_controller_20260726/service.json`) |
| torch | **2.11.0** | vllm 0.25.1 的依赖,pip 会自动把它装上(见下) |
| ray | 2.56.1 | pip 解析 |
| daft | 0.7.21 | pip 解析 |
| psycopg[binary] | ≥3.2 | `code/requirements.txt` |
| pyarrow | ≥16,<25 | `code/requirements.txt`(注意上界) |
| transformers | 最新兼容版 | pip 解析 |
| huggingface_hub | 1.24.x | CLI 已改名 `hf`,用 Python API(见 §5) |

### 4.2 关键版本交互:vllm 0.25.1 会升级 torch
镜像自带 `torch 2.8.0+cu128`。但 **vllm 0.25.1 要求 torch 2.11.0**,`pip install vllm==0.25.1` 会:
- 下载 torch-2.11.0-cp312 manylinux wheel(slim,**不含** CUDA)并拖一组 **CUDA 13.x** 的 `nvidia-*-cu13` pip wheel(nvidia-cublas 423MB、nvidia-cufft 205MB、nvidia-cusolver 118MB、nvidia-curand 57MB …合计 ~2GB),**覆盖**镜像的 torch 2.8.0+cu128 与 CUDA 12.8 libs;
- 新 torch + CUDA 13 runtime 跑在宿主 driver(本次为 595.71.05,R595 分支)上,向下兼容,正常工作;
- ⚠️ 实际下载量 **~2.5GB**(vllm 250MB + torch + CUDA13 libs),清华源 ~1.2 MB/s 下需 **30+ 分钟**,留足时间,别误以为卡死。

这是允许的:本机 Docker 用的是 cu129 torch(为 RTX 5070 Blackwell),云上 pip 装的是标准 manylinux torch——**CUDA patch 不同,但 vLLM 行为按版本(0.25.1)对齐**。报告里标注这一差异即可(见 §11)。

### 4.3 安装命令(分两步,避免互覆盖)
```bash
source /root/miniconda3/etc/profile.d/conda.sh && conda activate base
# 1) 先装 vllm,让它自己选定 torch(关键:不要先装 bare torch 把版本锁死)
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple vllm==0.25.1
# 2) 再装其余(跳过 torch,保留 vllm 选定的版本)
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple \
  numpy "pyarrow>=16,<25" ray "psycopg[binary]>=3.2" daft sqlglot connectorx transformers
```

### 4.3.1 推荐 uv + 独立 venv(2026-07-27 4090 实测)
上面 §4.3 把 vllm 装进 base,可行但有两个更优做法(本节实测):
- **用 uv 替代 pip**:`pip install -U uv` 后 `uv pip install vllm==0.25.1`,解析+并行下载比 plain pip 快一个量级(4090 上约 5 分钟 vs pip 30+ 分钟)。
- **vllm 装独立 venv,不进 base**:base 留给 driver(ray/daft/psycopg/transformers + 镜像自带 torch);vllm 单独放 `/root/autodl-tmp/venvs/vllm-4090`,两者 torch 互不污染(镜像 base 自带 torch 2.12.1+cu130,vllm 0.25.1 要 torch 2.11.0,分开避免覆盖)。`vllm serve` 用 venv 的 python 启动。
- **缓存必须放数据盘**:`export UV_CACHE_DIR=/root/autodl-tmp/uv-cache HF_HOME=/root/autodl-tmp/huggingface`,否则 30G 系统盘易满。
- **Python 环境**:`python -m venv /root/autodl-tmp/venvs/vllm-4090`(用 venv,不要 `conda create`,conda solver 在这套频道下会卡 repodata)。

### 4.4 pip 必须用国内镜像,但**不要**走 network_turbo
- AutoDL 官方说明:`/etc/network_turbo` 只加速 github/huggingface,**pip 源保持默认/用常规镜像**——不要给 pip 设 `https_proxy=172.20.0.113:12798`。
- 直连 PyPI 极慢(~200 kB/s),所以用**清华镜像** `-i https://pypi.tuna.tsinghua.edu.cn/simple`(实测稳定 ~1 MB/s)。
- **必须 pin `vllm==0.25.1`**:不 pin 会装最新版(如 0.26.0),与本机 0.25.1 不可比。

### 4.5 验证
```bash
python -c "import vllm,ray,daft,torch; print(vllm.__version__, ray.__version__, daft.__version__, torch.__version__, torch.cuda.is_available(), torch.cuda.device_count())"
# 期望:0.25.1 2.56.1 0.7.21 2.11.0 True 2
```

---

## 5. 模型下载

模型由 `MODEL_ID` 与 `MODEL_DIR` 决定。1.5B、7B 或后续模型都走同一下载脚本，
不修改 Python 或 shell 源码；换模型后只需同步修改 `COMPLETION_MODEL` 和
实验配置中的 context/token 上限。

### 5.1 必开加速 + 禁 Xet
```bash
source /etc/network_turbo >/dev/null 2>&1   # 代理 172.20.0.113:12798,仅 github/hf
export HF_HUB_DISABLE_XET=1                  # 否则走 cas-server.xethub.hf.co 报 401
```
不开 `network_turbo` 时 HF 直连/`hf-mirror.com`/modelscope 全部极慢(8 kB/s ~ 700 kB/s 且会 stall)——这是本次最大的坑。

推荐直接运行：

```bash
bash deploy/autodl/download_model.sh /root/autodl-tmp/ai-operator-runtime.env
```

脚本把上述两项前置条件固化为可执行检查；后面的 wget/Python API 命令仅作为
故障排查或手工备选。

### 5.2 推荐:wget 直连 HF(走 turbo 代理)
```bash
DEST=/root/autodl-tmp/models/Qwen2.5-1.5B-Instruct/model.safetensors
wget -c --tries=10 --timeout=30 --retry-connrefused \
  https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct/resolve/main/model.safetensors \
  -O "$DEST"
```
小文件(config/tokenizer)先用 `huggingface_hub` 拉一遍或一并 wget;wget 自动跟随 302。`-c` 断点续传。

### 5.3 备选:Python API(版本无关)
huggingface_hub 1.x 的 CLI 已从 `huggingface-cli` 改名为 `hf`。为避免 CLI 语法差异,用稳定的 Python API:
```bash
source /etc/network_turbo >/dev/null 2>&1
HF_HUB_DISABLE_XET=1 python -c \
  "from huggingface_hub import snapshot_download; \
   snapshot_download('Qwen/Qwen2.5-1.5B-Instruct', local_dir='/root/autodl-tmp/models/Qwen2.5-1.5B-Instruct')"
```

### 5.4 速率与加速
turbo 代理对 HF 单连接速率波动较大(300 kB/s ~ 9 MB/s)。若持续慢,装 aria2 多连接:
```bash
apt-get update && apt-get install -y aria2
aria2c -x16 -s16 -c -d /root/autodl-tmp/models/Qwen2.5-1.5B-Instruct -o model.safetensors \
  https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct/resolve/main/model.safetensors
```
(apt 与 pip 不冲突,但避免与 pip 同时大流量下载。)

### 5.5 验证
`model.safetensors` ~3.09GB,目录无 `.incomplete`;`configuration.json`、`tokenizer.json` 齐全。

---

## 6. PostgreSQL + pgvector

AutoDL 镜像**无 Docker**,走 apt + PostgreSQL 官方源(PGDG):
```bash
apt-get update
apt-get install -y ca-certificates curl gnupg lsb-release
install -d /usr/share/postgresql-common/pgdg
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] \
  https://apt.postgresql.org/pub/repos/apt jammy-pgdg main" \
  > /etc/apt/sources.list.d/pgdg.list
apt-get update
apt-get install -y postgresql-18 postgresql-18-pgvector
```
起服务 + 建库:
```bash
pg_ctlcluster 18 main start 2>/dev/null || service postgresql start
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres';"
sudo -u postgres psql -c "CREATE DATABASE ai_operator;"
sudo -u postgres psql -d ai_operator -c "CREATE EXTENSION IF NOT EXISTS vector;"
```
连接串:`postgresql://postgres:postgres@localhost:5432/ai_operator`。

**版本边界**:云上 **PG 18.4(apt)≈ 本机 PG 18.4(Docker)**(对齐,仅安装方式不同);公司平台为 18.3,二者不等同。CSV 记 `server_version`,报告标注。(2026-07-27 由 PG16 改为 PG18.4,与本地 baseline 对齐;原 PG16 是保守默认,已废弃。)

---

## 7. workload 数据

实验默认从 `data/raw/` 读(见 `code/scripts/import_ai_complete_workload.py:78-81`):
- ShareGPT:`data/raw/sharegpt_vicuna/ShareGPT_V3_unfiltered_cleaned_split.json`
- BurstGPT:`data/raw/burstgpt/BurstGPT_1.csv`

raw 数据被 gitignore,需在远端现下(github 走 turbo 加速):
```bash
source /etc/network_turbo >/dev/null 2>&1
cd /root/autodl-tmp/ai-operator
mkdir -p data/raw/sharegpt_vicuna data/raw/burstgpt
# ShareGPT(HF,禁 Xet):
HF_HUB_DISABLE_XET=1 python -c "from huggingface_hub import hf_hub_download; \
  hf_hub_download('ryanmarten/ShareGPT-Vicuna', 'ShareGPT_V3_unfiltered_cleaned_split.json', \
  repo_type='dataset', local_dir='data/raw/sharegpt_vicuna')"
# BurstGPT(github):
git clone --depth 1 https://github.com/Sparkessence/BurstGPT.git /tmp/BurstGPT
cp /tmp/BurstGPT/data/BurstGPT_1.csv data/raw/burstgpt/ 2>/dev/null || find /tmp/BurstGPT -name 'BurstGPT*.csv' -exec cp {} data/raw/burstgpt/ \;
```
(若上游路径变动,以 `import_ai_complete_workload.py` 默认值为准调整。)

---

## 8. 启动 vLLM endpoint(里程碑:不依赖 PG)

每张卡一个 endpoint,用 `CUDA_VISIBLE_DEVICES` 钉死。统一入口：

```bash
bash deploy/autodl/start_endpoints.sh /root/autodl-tmp/ai-operator-runtime.env
```

同构实验让所有 GPU 使用同一个 `MODEL_PATH` 与 `COMPLETION_MODEL`。若要更换
模型、GPU 数或端口，只编辑运行配置，不修改 `start_endpoints.sh`。
异构(1.5B + 7B)留作后续"异构 actor pool"实验，不与同构扩展实验混算。

验证:`curl http://127.0.0.1:8000/v1/completions ...`、`nvidia-smi` 看两个 vLLM 分别占 GPU 0/1。

### 8.1 启动前检查清单（2×4090 + vLLM 0.25.1）

正式实验前逐项检查，不要只看到 GPU 显存占用就认为服务已经可用：

1. **编译器与 headers 必须来自同一 CUDA toolkit**。本实例已验证的组合是：

   ```bash
   CUDA_HOME=/usr/local/cuda-13.0
   CUDA_NVCC_BIN=/usr/local/cuda-13.0/bin
   ```

   `nvidia-cuda-nvcc==13.2.86` 安装在 Python 环境的 `nvidia/cu13/bin`
   中，但该共享目录里的 `cuda.h` 与 Torch 均为 CUDA 13.0。用 13.2 nvcc
   编译 13.0 headers 会报
   `CUDA compiler and CUDA toolkit headers are incompatible`。不要因为路径名
   都含 `cu13` 就认为 minor version 一致。

2. **vLLM 虚拟环境工具必须在 PATH 中**。启动脚本应满足：

   ```bash
   export PATH="$VLLM_VENV/bin:$PATH"
   command -v ninja
   ```

   只调用 `$VLLM_VENV/bin/python` 不会自动把 `ninja` 暴露给子进程；
   FlashInfer JIT 会在 warm-up 阶段因 `No such file or directory: 'ninja'`
   失败。

3. **正式 capacity 必须显式固定，且 manifest 与进程命令一致**：

   ```bash
   VLLM_MAX_NUM_BATCHED_TOKENS=8192
   VLLM_MAX_NUM_SEQS=256
   VLLM_EXTRA_ARGS="--no-enable-prefix-caching --enable-mfu-metrics \
   --max-num-batched-tokens 8192 --max-num-seqs 256"
   ```

   `unknown` 不能进入正式实验。上游 token budget、active-work credit 与
   vLLM `max_num_batched_tokens` 是三个不同层次的参数，不可互相代替。

4. **从 Windows 临时同步 shell 脚本时必须转换为 LF**。出现
   `set: pipefail\r: invalid option name` 说明上传的是 CRLF；优先从 Git
   checkout/pull，临时传输必须显式做 CRLF→LF，再运行：

   ```bash
   bash -n deploy/autodl/start_endpoints.sh
   ```

5. **只停止脚本自己管理的 PID**。需要替换 endpoint 时设置
   `STOP_MANAGED_ENDPOINTS=1`；脚本会核对 PID 命令包含 vLLM server 和目标
   port。不要使用宽泛 `pkill -f`，也不要覆盖仍存活但身份不匹配的 PID。

6. **两张卡首次 JIT/图编译会持续数分钟**。后台启动并短轮询：

   ```bash
   nohup env STOP_MANAGED_ENDPOINTS=1 \
     bash deploy/autodl/start_endpoints.sh \
       /root/autodl-tmp/ai-operator-runtime.env \
     </dev/null >/root/autodl-tmp/vllm_restart.log 2>&1 &

   tail -f /root/autodl-tmp/vllm_restart.log
   ```

   不要在编译期间重复启动第二组 endpoint；两个 endpoint 可并行启动，但它们
   共享 FlashInfer cache lock。

### 8.2 启动成功门禁

只有同时满足以下条件，才允许启动正式 scenario runner：

```bash
# 1) 两个 health endpoint 均成功
for p in 8000 8001; do
  curl -sf "http://127.0.0.1:$p/health" >/dev/null || exit 1
done

# 2) 进程命令包含固定 capacity
ps -eo pid,args | grep '[v]llm.entrypoints.openai.api_server'

# 3) 两张 GPU 各有一个服务进程
nvidia-smi --query-compute-apps=pid,gpu_uuid,used_memory --format=csv,noheader

# 4) 模型与 metrics 可读
curl -sf http://127.0.0.1:8000/v1/models
curl -sf http://127.0.0.1:8001/v1/models
curl -sf http://127.0.0.1:8000/metrics | grep -m1 '^vllm:'
curl -sf http://127.0.0.1:8001/metrics | grep -m1 '^vllm:'
```

随后先跑一个小规模门禁，核对 exactly-once、两 endpoint 分流、resource/MFU
trace 与 service metadata，再启动多小时正式实验。GPU 显存已分配但
`/health` 不通只表示模型加载/编译中或已失败，不是成功状态。

### 8.3 常见失败的最短诊断路径

| 现象 | 首查 | 处理 |
|---|---|---|
| `ninja` 不存在 | `command -v ninja`、`echo "$PATH"` | 把 `$VLLM_VENV/bin` 加入 PATH，不要另装一套环境 |
| CUDA compiler/header incompatible | `nvcc --version`、`grep CUDA_VERSION "$CUDA_HOME/include/cuda.h"` | 编译器和 headers 统一指向 `/usr/local/cuda-13.0` |
| `pipefail\r` | `file start_endpoints.sh`、`bash -n` | 转换为 LF，避免从 Windows 原样覆盖 Linux 脚本 |
| 显存占用但 health 失败 | endpoint log 的第一处 root cause | 不重复启动；先处理 JIT/模型初始化错误 |
| 端口健康但配置不明 | `ps -eo pid,args` | 确认模型、context、prefix cache、8192/256 capacity 与 manifest 一致 |
| 启动器长时间不返回 | 检查后台 stdin/stdout | 使用 `nohup ... </dev/null >log 2>&1 &` 后短轮询 |

---

## 9. 跑首个多 endpoint 实验

代码已支持多 endpoint(`code/scripts/postgres_ai_operator_profile.py:325` 的 `--completion-endpoint-urls`,round-robin 路由,无需改代码):
```bash
cd /root/autodl-tmp/ai-operator
# 先灌数据
python code/scripts/import_ai_complete_workload.py \
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator \
  --workload-name sharegpt_burstgpt --max-rows 1024 --batch-rows 500 \
  --tokenizer-path /root/autodl-tmp/models/Qwen2.5-1.5B-Instruct \
  --max-model-len 2048 --completion-max-tokens 16
# 单 endpoint baseline
python code/scripts/postgres_ai_operator_profile.py \
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator \
  --setup --total-rows 128 --db-fetch-rows 128 --ray-batch-rows 8 \
  --operator ai_complete --executor ray_task --model-backend compatible_http \
  --completion-endpoint-url http://127.0.0.1:8000/v1/completions \
  --completion-model qwen2.5-1.5b --completion-max-tokens 32 \
  --source-workload-name sharegpt_burstgpt --data-source daft_postgres --organizer daft \
  --writeback-mode none --experiment-id cloud_single_ep \
  --output experiments/results/cloud_autodl/single_endpoint.csv
# 双 endpoint(各占一张 GPU)
python code/scripts/postgres_ai_operator_profile.py ... \
  --completion-endpoint-urls http://127.0.0.1:8000/v1/completions,http://127.0.0.1:8001/v1/completions \
  --experiment-id cloud_dual_ep \
  --output experiments/results/cloud_autodl/dual_endpoint.csv
```
对比 `operator_wall_s` / `e2e_s` / `rows/s`,回答"独立 GPU 上多 endpoint 路由是否有收益"。

### 9.1 双 GPU 分阶段诊断模板

八个模板按因果问题分阶段运行，不能合成一个大矩阵：

1. `dual_gpu_capacity_scaling.example.json`：相同 per-GPU K 下比较单/双
   endpoint，只回答硬件扩展效率，不作为上游策略容量标定。
2. `dual_gpu_active_work_curve.example.json`：以 request-level submission
   直接扫描每 endpoint 的预测 active-token 配额。组织预算是固定的非处理变量；
   模板覆盖 16K、24K、32K、49K、65K、82K、98K、131K，每档 1 次 warm-up
   加 3 次 formal。先标定 `ACTIVE_WORK_PER_ENDPOINT`，不要把 32768 当作跨模型
   常数。高负载档一旦 OOM、超时或失败，保留 incident 并停止继续增压。
   在所有安全档位中选择首个达到最大已测吞吐 97%、且下一个安全档增益低于
   3% 的最小配额；若不存在，结论必须写 `saturation_not_reached`，不能把最高
   已测点改名为饱和点。
3. `dual_gpu_token_budget_curve.example.json`：关闭 arrival replay，49K
   主点扫描 8/16/32/49K，65K 敏感性点扫描 8/16/32/49/65K，共 9 个场景。
   每个预算都不超过对应 active-work 上限，避免 oversized admission 破坏
   固定-work 语义。它回答等量 offered work 下组织/提交形状是否有收益，
   而不是继续用更大的 batch 暗中增加并发。
4. `dual_gpu_data_organization.example.json`：使用上一步的最佳已测预算并继续
   关闭 arrival replay，避免 50ms flush 在 token budget 生效前关批；在相同
   active work 下回答 fixed rows、sequential token-budget、row-cap-aware 和
   length-align 的数据组织差异。
5. `dual_gpu_request_replay.example.json`：恢复相同 arrival replay/flush，
   比较 whole-submission barrier 与真正的 request-level replenishment。
6. `dual_gpu_actor_pool_shape.example.json`：沿用 request-level 饱和点，
   固定每 endpoint 256 个可见 actor slots，比较 1×256、2×128、4×64。
   16-slot 草案已在启动前否决：按当前约 332 work/request 或
   1337 work/organization batch，它无法维持已测饱和区，会混入 offered-load。
7. `dual_gpu_service_quantum.example.json`：固定上一步最佳 pool、active work
   和 planning budget，比较 whole batch、512/1024/2048/4096 complete-row
   quantum 与 request diagnostic。当前组织批次 P95≈3366、max≈5892，因此
   8192 不会发生切分，禁止把它作为一个伪独立 arm。
8. `dual_gpu_submission_policy.example.json`：在已标定 token budget 和
   active-work 配额上，逐项消融 least-work routing、service-quantum 动态预算
   和 queue-adaptive flush；最后的 combined arm 只检查交互，不替代单项结论。

`${DATABASE_URL}`、`${COMPLETION_MODEL}`、endpoint/metrics URL 等变量在 runner
读取时从环境展开；缺失变量会在启动任何外部工作前报错。容量模板的单 GPU
control 还要求 `SINGLE_COMPLETION_ENDPOINT_URL`、`SINGLE_MODEL_METRICS_URL`
和 `SINGLE_ENDPOINT_GPU_ID`，从而只采样实际工作的 GPU。

```bash
set -a
source /root/autodl-tmp/ai-operator-runtime.env
set +a
python code/scripts/run_ai_operator_scenarios.py \
  --config deploy/autodl/dual_gpu_capacity_scaling.example.json \
  --profiler code/scripts/postgres_ai_operator_profile.py \
  --python-executable /root/autodl-tmp/venvs/vllm-4090/bin/python \
  --output-dir experiments/results/dual_gpu_capacity_scaling \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls "$MODEL_METRICS_URLS"
```

完成硬件 scaling 后，只替换 `--config` 和 `--output-dir`，依次运行
active-work-curve、token-budget-curve、data-organization、request-replay 与
actor-pool-shape、service-quantum、submission-policy 模板。active-work 曲线完成后，token-budget 模板直接使用
已标定的 49K 主点与 65K 敏感性点；固定 offered work 的预算曲线完成后，再把
49K 主点在 SLO/P99 约束下选出的值写入 `BEST_TOKEN_BUDGET`。预算扫描期间
active-work 配额必须不小于同场景单 batch 预算，否则会混入 oversized
admission 语义。每轮都必须等待 runner manifest 为 `complete`，不要手工拼接
失败重跑的 CSV。

动态预算只在 `TOKEN_BUDGET_CANDIDATES` 的静态已测动作中移动。这里的 token
budget 是 Ray 上游关批边界，active-work 是 endpoint admission credit，
二者都不是 vLLM 的 `max_num_batched_tokens`。三者必须分别记录和消融。

Actor Pool 与 service-quantum 两个模板还需要以下运行时变量。pool shape
正式结果出来前，不要启动 quantum 矩阵：

```bash
export ACTIVE_WORK_PER_ENDPOINT=<checkpoint_a_selected_work>
# 由 pool shape 的正式重复选择；三组合法值分别为 1/256、2/128、4/64
export ACTOR_WORKERS_PER_ENDPOINT=<selected_workers>
export RAY_ACTOR_MAX_CONCURRENCY=<selected_concurrency>
# 与所选 pool arm 一致：1/2/4 workers 对应 0.5/0.25/0.125
export RAY_WORKER_NUM_CPUS=<selected_per_actor_cpu>
```

先按本 runbook 的 gate 规则，从 pool 模板机械缩为 64 行且保留全部三个 pool
shape；通过后运行 pool formal。再从 quantum 模板机械缩为 64 行，至少保留
planning-batch、一个 fixed quantum 和 request diagnostic，核对 request/
submission/resource trace、worker ID/index/PID、每 endpoint 256 slots、
exactly-once、零 failure 和 lease cleanup。两个正式矩阵使用不同的全新输出目录，
串行运行，禁止在同一目录 resume 另一份配置。

Pool-shape 模板还固定每 endpoint 的 Ray CPU reservation 为 0.5：
1×0.5、2×0.25、4×0.125。该值是 Ray placement/resource 契约，不等同于操作
系统 CPU 利用率；若任一 arm 因集群最小 fractional resource 限制无法创建，
gate 直接失败并记录 incident，不能只为该 arm 临时增加总 CPU 配额。

已完成的 1024–32768 曲线只能记作 offered-load 诊断：固定的是每 endpoint
四个 batch，而平均每 batch 行数约从 2.3 增至 64，所以可供给的 request
envelope 约从每 endpoint 9 增至 256，vLLM mean running requests 也约从
15.5 增至 310.7。它证明服务仍能被更多并发填充，不能证明 token budget 本身
越大越优。32768 应写作 `BEST_TESTED_TOKEN_BUDGET`，不是容量甜点。

所有正式模板必须从运行环境读取与真实启动参数一致的
`VLLM_MAX_NUM_BATCHED_TOKENS`、`VLLM_MAX_NUM_SEQS` 和
`REQUEST_SLO_MS`；`unknown` 会在任何外部工作启动前被拒绝。

`dual_gpu_submission_policy` 的前五个场景是单因素或逐层增量对照；只有相应
单项在 tokens/s 或 SLO-goodput 上超过重复波动且不恶化 p99，才允许把它保留在
combined candidate。不能因为 combined 最好就倒推每个组成策略都有效。

request-replay 模板保留 `ray_batch_rows=64` 和 `token_budget=8192` 作为组织边界，只有
`submission_granularity=request` 的场景才在关批后展开为单请求。不要用
`ray_batch_rows=1` 伪装 request-level replenishment；那会在组织阶段直接把
每个 packing group 截成一行，并不能验证“批组织 + 请求级持续补位”机制。

request K 不能与 batch K 按相同数值比较。先读取 batch control 的
`batch_rows_mean`，令候选中心约为
`request_K_per_endpoint = batch_K_per_endpoint × batch_rows_mean`。模板中的
K32/K48/K64 是围绕当前约 3 行/batch 的 gate 展开；若正式 batch mean 明显变化，
必须先修改 request K 再运行，而不是事后挑最好看的点。

---

## 10. 操作坑汇总(按踩坑顺序)

| 坑 | 现象 | 解法 |
|---|---|---|
| 没开 network_turbo | HF/modelscope/hf-mirror 全部 8~700 kB/s 或 stall | `source /etc/network_turbo` |
| HF Xet 后端 | `401 Unauthorized cas-server.xethub.hf.co` | `HF_HUB_DISABLE_XET=1` |
| 非交互 SSH 无 python | `python: command not found` | `bash -lc` 或 source conda |
| paramiko 长连下载/安装 | 超时砍断、`stdout.read()` 卡死 | nohup 后台 + 短连接轮询(bgexec 模式) |
| `pkill -f <自身模式>` | exit 127 自杀 | `kill <pid>` / `pgrep -x` / `pgrep -P` |
| Windows Git Bash MSYS | 远端路径变 `C:\Program Files\Git\...` | `export MSYS_NO_PATHCONV=1` |
| pip 直连 PyPI | ~200 kB/s | 清华镜像 `-i ...tsinghua...` |
| pip 走 turbo | 违反 AutoDL 说明 | pip 不设 turbo 代理,只改 `-i` 镜像 |
| tar 上传代码 | 不可复现、缺文件、缺同步 | **git clone from GitHub** |
| vllm 不 pin 版本 | 装到 0.26.x 与本机不可比 | `vllm==0.25.1` |
| torch 2.11 拖 CUDA13 libs(~2GB) | `pip install vllm` 极慢、像卡死 | 非坑,是版本链必然;留 30+ min,清华源串行下 |
| HF CLI 改名 | `huggingface-cli download` 只打印 help 不下载 | huggingface_hub 1.x CLI 是 `hf`;或直接用 Python `snapshot_download` |
| 下载带宽竞争 | pip + wget 同时跑,单跑 10 MB/s → 双跑降到 300 kB/s | 大文件串行(先 pip 后模型,或反之) |
| `rm -rf` 删项目目录 | 连带删掉 gitignored 的 `data/raw/`(并行操作者正在下载) | rm -rf 前先检查 gitignored 目录;多会话/多人先分工 |
| `$(lsb_release -cs)` 为空 | repo 行变 `apt -pgdg`,报 "does not have a Release file" | 硬编码 `jammy`(Ubuntu 22.04);别依赖 lsb_release 已装/在 PATH |
| **Blackwell(sm120)卡** | flashinfer `sm75` 报错 + CUDA12/13 库混 + quack/cutlass API 不匹配,连环失败 | **换非 Blackwell 卡(4090/A100/3090)或用官方 Docker**;详见 §12 |

---

## 10.5 重启后恢复全流程(每次开机必做)

AutoDL 按量计费实例关机后**系统盘保留、/root/autodl-tmp 保留**。重开后只需重启服务，**不需要重新 pip install 或下载模型**。

### 10.5.1 一步恢复(推荐)

```bash
# 1) 启动 PG
pg_ctlcluster 18 main start 2>/dev/null || service postgresql start
# 验证服务与正式 workload；两项均通过后才能启动实验 runner
pg_isready -h 127.0.0.1 -p 5432
PGPASSWORD=postgres psql -h 127.0.0.1 -U postgres -d ai_operator \
  -c "SELECT workload_name, count(*) FROM documents GROUP BY workload_name"

# 2) 按仓库外 runtime env 启动双 vLLM endpoint
bash /root/autodl-tmp/ai-operator/deploy/autodl/start_endpoints.sh \
  /root/autodl-tmp/ai-operator-runtime.env
```

`start_endpoints.sh` 做的事：加载 runtime env → 把对应 vLLM venv 与 CUDA
toolkit 加入环境 → 分别在配置的 GPU/端口启动 vLLM → 轮询 `/health` 直到
就绪 → 检查 models → 打印 GPU 进程。脚本不使用宽泛的 `pkill -f`；只有
`STOP_MANAGED_ENDPOINTS=1` 时才根据自身 PID 文件精确停止受管 endpoint。

实例重启后 PostgreSQL 可能留下 stale PID file；`pg_ctlcluster` 会清理并恢复
服务。不能只检查 vLLM `/health`：正式 gate 还必须连接
`DATABASE_URL`、核对 `SOURCE_WORKLOAD_NAME` 行数。若 gate 因依赖服务未启动而
失败，修复服务后使用 runner `--resume`，保留原 incident 并标记 recovered，
不要删除输出目录伪装成首次成功。

### 10.5.2 手动逐步(调试用)

```bash
# PG
pg_ctlcluster 18 main start
pg_isready

# vLLM(需 venv,不在 base conda!)
source /root/autodl-tmp/venvs/vllm-4090/bin/activate
export PATH="/root/autodl-tmp/venvs/vllm-4090/lib/python3.12/site-packages/nvidia/cuda_nvcc/bin:$PATH"

CUDA_VISIBLE_DEVICES=0 nohup python -m vllm.entrypoints.openai.api_server \
  --model /root/autodl-tmp/models/Qwen2.5-1.5B-Instruct \
  --served-model-name qwen2.5-1.5b --dtype auto \
  --max-model-len 2048 --gpu-memory-utilization 0.9 \
  --no-enable-prefix-caching --enable-mfu-metrics \
  --port 8000 --host 127.0.0.1 \
  </dev/null >/root/autodl-tmp/vllm_logs/ep_8000.log 2>&1 &

CUDA_VISIBLE_DEVICES=1 nohup python -m vllm.entrypoints.openai.api_server \
  --model /root/autodl-tmp/models/Qwen2.5-1.5B-Instruct \
  --served-model-name qwen2.5-1.5b --dtype auto \
  --max-model-len 2048 --gpu-memory-utilization 0.9 \
  --no-enable-prefix-caching --enable-mfu-metrics \
  --port 8001 --host 127.0.0.1 \
  </dev/null >/root/autodl-tmp/vllm_logs/ep_8001.log 2>&1 &

# 轮询就绪(首次启动需 5-12 分钟,flashinfer JIT 编译)
curl -sf http://127.0.0.1:8000/health && echo "8000 OK"
curl -sf http://127.0.0.1:8001/health && echo "8001 OK"
```

### 10.5.3 代码同步

```bash
source /etc/network_turbo >/dev/null 2>&1   # 加速 github
cd /root/autodl-tmp/ai-operator && git pull
```

### 10.5.4 环境速查

| 组件 | 位置 | 验证 |
|------|------|------|
| PG 18.4 | apt, cluster 18/main | `pg_isready` |
| vLLM venv | `/root/autodl-tmp/venvs/vllm-4090` | `source .../bin/activate && python -c "import vllm; print(vllm.__version__)"` → 0.25.1 |
| base conda | `/root/miniconda3` | `source .../profile.d/conda.sh && conda activate base && python` → pyarrow/daft/ray/psycopg 全可用 |
| 模型 | `/root/autodl-tmp/models/Qwen2.5-1.5B-Instruct` | `model.safetensors` 非零(~3GB) |
| 数据集 | `data/raw/sharegpt_vicuna/` + `data/raw/burstgpt/` | ShareGPT ~642MB, BurstGPT ~50MB |
| PG 数据 | `ai_operator` 库 | `PGPASSWORD=postgres psql -h localhost -U postgres -d ai_operator -c "SELECT count(*) FROM documents"` |

### 10.5.5 实验运行环境

实验脚本用 **base conda**(不是 vLLM venv):

```bash
source /root/miniconda3/etc/profile.d/conda.sh && conda activate base
cd /root/autodl-tmp/ai-operator
python code/scripts/run_ai_operator_scenarios.py ... --python-executable /root/miniconda3/bin/python ...
```

关键：`--python-executable` 必须指向 base conda 的 python(有 pyarrow/daft/ray)，**不能**指向 vLLM venv 的 python(只有 vllm)。

---

## 11. 平台边界声明(写进结果报告)

云上实验结果必须标注,不可与本机结论混同:

- **平台**:AutoDL 云服务器,2× GPU(**非 Blackwell**,如 RTX 4090 sm89;**不要用 50xx/6000D 等 sm120 卡,见 §12**),Ubuntu 22.04。
- **Python 环境**:miniconda3,Python 3.12.3。
- **vLLM**:0.25.1(pip 安装,torch 2.11.0 manylinux)。本机为 0.25.1 Docker(cu129)。CUDA patch 不同,vLLM 版本对齐。
- **PostgreSQL**:18.4 + pgvector(apt 装于 AutoDL),**与本机 18.4 Docker 对齐**;公司平台为 18.3,二者不等同。
- **GPU 架构与可比性**:云上非 Blackwell(如 4090 sm89)≠ 本机 RTX 5070(sm120)。**软件栈版本可比,硬件不可比**——单请求延迟、kernel 吞吐、batch 扩展曲线、显存带宽、双卡扩展效率都受架构影响。正式 baseline / 消融 / 优化方案**全部在同一台 2×4090 上重跑**;本机 5070 只用于开发与功能验证,**不拿 5070 结果和 4090 结果直接算优化比例**。研究变量(batch / task-actor 粒度 / in-flight / endpoint routing / fan-in / writeback)与 GPU 架构正交,实验结论仍成立。
- 每条 CSV 记录 `server_version` 与 `pgvector_version`(项目硬性规则,见根 `AGENTS.md` §5)。

---

## 12. Blackwell (sm120) GPU 兼容性注记(重要,血泪记录)

**结论:AutoDL 上 pip 安装的 vLLM 跑不了 Blackwell(sm120)卡——RTX 5070/5080/5090/6000D/6000 Blackwell 全中。要么换非 Blackwell 卡(4090/3090/A100),要么用官方 Docker 镜像。** 本机能跑是靠官方 Docker 镜像里专门 build 过的栈,pip 复现不出来。2026-07-27 在 2× RTX 6000D(sm120)上耗了一下午验证此结论。

### 12.1 现象
vLLM EngineCore 初始化失败:`RuntimeError: FlashInfer requires GPUs with sm75 or higher`(卡明明是 sm120,远超 sm75)。

### 12.2 根因(连锁,逐个揭开)
1. **flashinfer 0.6.13**(vllm 0.25.1 pin)在 sm120 上 CC 检测有 bug → 误报 sm75 检查失败。
2. **CUDA 12/13 库混存**:AutoDL 镜像自带 torch 2.8.0+cu128(CUDA 12 库),pip 升 torch 2.11.0+cu130 又装 CUDA 13 库;`nvidia/cuda_nvrtc/lib/libnvrtc.so.12` 与 `nvidia/cu13/lib/libnvrtc.so.13` 并存。flashinfer/nvrtc 加载到旧的 so.12 → `SM 12.x requires CUDA >= 12.9` → 读不到 sm120。
3. **flashinfer 升级死路**:PyPI 上 `flashinfer-cubin` 最高只到 0.6.13,没有 0.6.14/0.6.15.post1 的匹配 cubin → 版本不匹配,import 直接报错。
4. **quack-kernels 0.5.0**(vllm 0.25.1 留下)vs vllm 0.26.0 带的 `cutlass-dsl 4.6.0` API 不匹配:`cutlass.cute.core.ThrMma` 属性不存在,quack import 就崩。

### 12.3 试过但都不够的 workaround(备查,别再重复)
- `LD_LIBRARY_PATH` 加 `nvidia/*/lib`、甚至把 `nvidia/cu13/lib` 优先 —— 没解决(CUDA12 库仍被其它环节加载)。
- `FLASHINFER_DISABLE_VERSION_CHECK=1` 跳过 cubin 版本检查走 JIT —— 绕开 cubin 报错,但 sm120 检测仍挂。
- `VLLM_ATTENTION_BACKEND=TORCH_SDPA` 换 attention 后端 —— flashinfer 还被启动 kernel autotune 调用,绕不开。
- `--enforce-eager` 关 torch.compile —— quack 在 init 时就 import,绕不开。
- 升 `vllm==0.26.0`(带 flashinfer 0.6.14)+ 升 `quack-kernels==0.6.1` —— quack 过了,flashinfer sm120 仍挂。

### 12.4 真正可行的路
- **换非 Blackwell GPU**(推荐):sm89(RTX 4090)、sm86(RTX 3090)、sm80(A100)。flashinfer 0.6.13 + quack 0.5.0 + CUDA 12 全部原生支持,标准 `pip install vllm==0.25.1` 直接起,和你本机版本严格可比。
- **官方 Docker 镜像**(Blackwell 唯一干净路):`vllm/vllm-openai:v0.25.1-cu129-ubuntu2404`,栈专门 build 过。但 AutoDL 容器内做 Docker-in-Docker 常被权限限制,docker daemon 多半起不来。

### 12.5 选型一句话
AutoDL 租卡跑 pip 装的 vllm,**避开 50xx/6000D/6000 Blackwell**,选 4090 / 3090 / A100。Blackwell 留给本机 Docker 环境。

### 12.6 最终确认(2026-07-27,干净 uv 环境 + 分层诊断)
新建干净 conda env `vllm-bw`,`uv pip install vllm==0.25.1` 自洽拉栈(torch 2.11.0+cu130 + flashinfer 0.6.13 + quack),仍同一报错。**分层诊断**定位失败层:
- **GPU 层**:`torch.cuda.get_device_capability()` → `(12,0)` ✅,识别正确。
- **PyTorch 层**:`torch.cuda.get_arch_list()` → `['sm_75','sm_80','sm_86','sm_90','sm_100','sm_120']` ✅;`_get_cuda_arch_flags()` → `-gencode=arch=compute_120,code=sm_120` ✅。PyTorch 2.11.0+cu130 正确认识并为 sm120 编译。
- **FlashInfer 层**:`current_compilation_context.TARGET_CUDA_ARCHS = set()`(空)——flashinfer 0.6.13 自己的 arch 探测输出 `Failed to get device capability: SM 12.x requires CUDA >= 12.9`,没把 sm120 填进去 → `jit/core.py:check_cuda_arch()` raise `FlashInfer requires GPUs with sm75 or higher`。
- 即:**失败具体在 FlashInfer 0.6.13 的 arch 探测/CompilationContext,不是 GPU、也不是 PyTorch**。`LD_LIBRARY_PATH` 优先 cu13、`FLASHINFER_DISABLE_VERSION_CHECK=1`、`VLLM_ATTENTION_BACKEND=TORCH_SDPA`、`--enforce-eager` 都绕不过(check 在 flashinfer 初始化时无条件触发)。monkeypatch `check_cuda_arch` 成空函数能跳过前置检查,但不能保证后续 sm_120a/sm_120f 编译参数、JIT 内核、数值正确性与正式请求时不崩——不作为正式环境。

**证据支持的结论(只写到此处)**:在本 AutoDL 实例的标准 pip 环境里,vLLM 0.25.1 + PyTorch 2.11.0+cu130 + FlashInfer 0.6.13 这个组合无法完成 sm120 架构检测,服务起不来。**这不等于"FlashInfer 整体不支持 sm120"或"Blackwell 跑不了 vLLM"**——更新版 FlashInfer 已含 SM120 目标,公开 issue(vllm#48898)用几乎相同组合能进入更深阶段;只是本实例 + 这组固定版本不行。

**工程决策(止损)**:平台无 docker/apptainer,项目约束不源码编译、不改依赖源码、固定 vLLM 0.25.1 → 此路线不可用,换非 Blackwell 卡(4090/3090/A100)。这是基于时间成本和实验可复现性的止损决定,**不是对 Blackwell 支持能力的普遍结论**。

---

## 附:本地驱动远端的 SSH helper(参考)

若从本地脚本驱动远端(非交互密码登录),`sshpass`/`plink` 在 Windows 上常缺失,可用 Python+paramiko 自写小 helper:支持 `exec`(短命令,走 `bash -lc`)、`bgexec`(长任务,4s 后主动关 channel,远端 nohup 存活)、`upload_tar`(tar 流走 exec 通道,绕过 SFTP 路径怪异)。凭据只放环境变量,不落盘。该 helper 不入项目库(本地临时),但其模式(尤其 `bgexec` 和 `bash -lc` 包裹)值得任何远程驱动方案沿用。
