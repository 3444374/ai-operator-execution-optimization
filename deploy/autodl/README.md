# AutoDL 云部署指南

本指南沉淀 2026-07-27 把项目部署到 AutoDL(2× GPU 云服务器)的全流程经验,目标是可在云上复现本机实验并补"多 endpoint / 多 GPU"真实验证缺口(见根 `AGENTS.md` §3、`motivation/results/gpu/multi_endpoint_ray_motivation_20260712.md` 第 83 行)。

指南面向"从零起一台 AutoDL 实例到跑通首个多 endpoint 实验"。所有命令均为 Linux bash(远端)。

## 这份指南讲什么（先读这段，搞清概念）

**AutoDL 服务器**：一台云上 2×4090（每卡 24G）Linux GPU 盒子，项目所有 GPU 实验在它上面跑。本指南讲怎么从空实例配到能跑实验 + 开机恢复。

**项目执行栈**（数据怎么走一遍）：
```
PostgreSQL（数据源 + 写回 sink；pgvector 存向量）
  → Daft DataFrame（数据引擎：读出 / 组织成请求）
  → Ray actor（策略执行：数据组织 + 调度/提交控制）
  → 推理引擎（GPU 上算）→ 写回 PostgreSQL + pgvector
```

**推理引擎有两条 track**（本指南核心区分，务必先理解）：

| track | 是什么 | 模型 | 算子 | 部署在哪 |
|---|---|---|---|---|
| **文本（生成式）** | **vLLM**——一个开源 LLM **serving 引擎**，用 continuous batching + prefix cache(APC) + KV cache(PagedAttention) 高吞吐服务**生成式**推理。本项目**不修改 vLLM 内部**，只把它当部署平台 | Qwen2.5-1.5B/7B-Instruct | `AI_COMPLETE`（生成 token 序列） | 本指南 §8 `start_endpoints.sh` |
| **多模态（图像 embedding）** | **CLIP**——**不是**生成式 LLM，是 embedding 模型，把图像压成 512d 向量。**无 KV / 无 prefix / 无生成**，观测层与 vLLM 完全不同 | CLIP ViT-B/32 | `AI_EMBED`（图像→向量） | 单独文档 `deploy/autodl/multimodal_embed_serving.md` |

> **为什么分两条**：文本每行 ~1KB、搬运太轻，binding 瓶颈在 vLLM serving（RC1 实测 db_fetch 1.4–2.4s vs model_wall 27–37s）；图像每行 ~600KB，DB-read / CPU→GPU 搬运变 binding——两条要找/优化的瓶颈不同，引擎也不同（vLLM 服务生成、CLIP 服务 embedding）。

**怎么用本指南**：§1–§7 是**共享平台 setup**（实例/连接/venv/network_turbo/代码同步/模型下载方法/PG，两条 track 都用）；§8 起是**文本 vLLM track**；**图像 CLIP track 全在 `multimodal_embed_serving.md`**。

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
| Git 仓库 | `/root/autodl-tmp/ai-operator` | 只从 GitHub `main` 同步；不再写入新的运行时结果 |
| runtime env | `/root/autodl-tmp/ai-operator-runtime.env` | 仓库外保存模型、端口、CUDA、容量和 workload 参数 |
| driver Python | `/root/miniconda3/bin/python` | 运行 Ray/Daft/profiler/scenario runner |
| vLLM Python | `/root/autodl-tmp/venvs/vllm-4090/bin/python` | 只运行 vLLM endpoint |
| 模型 | `/root/autodl-tmp/models/<model>` | 由 runtime env 的 `MODEL_PATH` 指向 |
| vLLM 日志 | `/root/autodl-tmp/vllm_logs/` | 每个端口有 log/PID 文件 |
| 编排日志 | `/root/autodl-tmp/logs/` | endpoint 启动、gate、formal runner 日志 |
| 临时 gate 配置 | `/root/autodl-tmp/gates/` | 仓库外机械缩小正式模板，不作为正式结果 |
| 运行时结果 | `/root/autodl-tmp/experiment-artifacts/<unique_run_id>/` | 仓库外保存；审计后只把摘要和报告纳入 Git |

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
  grep -E '[r]un_(ai_operator_scenarios|shared_vllm_experiment)\.py' || true

# 若准备恢复已有输出，再检查目录级租约；不能只凭 ps 结果判断可恢复
OUTPUT_DIR=/root/autodl-tmp/experiment-artifacts/<existing_run_id>
test ! -e "$OUTPUT_DIR/.runner-lease.json" ||
  cat "$OUTPUT_DIR/.runner-lease.json"

# 1.5) 清理重启前残留的 stale Ray 集群指针。Ray 进程会随主机重启死亡，但
#      /tmp/ray/ray_current_cluster 指针文件仍在，下一个 ray.init()（无显式 address）
#      会读取它、反复连接死 GCS 直至 ~14 分钟后 ConnectionError，表现为 warmup 卡死。
#      重启后容器 IP 也可能变化，使旧地址双重失效。先 ray stop（若有残留进程），
#      再删除指针；之后 ray.init() 会自动起本地集群。
if pgrep -f '[g]cs_server\|[r]aylet' >/dev/null 2>&1; then
  ray stop -f >/dev/null 2>&1 || true
fi
rm -f /tmp/ray/ray_current_cluster

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

# 6) 独立复核全部已配置 endpoint（$PORTS 可能为 4：8000-8003）、真实参数和每卡进程
for p in ${PORTS//,/ }; do
  curl -fsS "http://127.0.0.1:$p/health"
  curl -fsS "http://127.0.0.1:$p/v1/models"
done
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
OUTPUT_DIR=/root/autodl-tmp/experiment-artifacts/<unique_run_id>
RUN_LOG=/root/autodl-tmp/logs/<unique_run_id>.log

ps -C python -C python3 -o args= |
  grep -E '[r]un_(ai_operator_scenarios|shared_vllm_experiment)\.py' &&
  exit 1 || true
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

`run_ai_operator_scenarios.py` 不是只传 `--config/--output-dir` 就能运行的
包装器。`--profiler`、`--python-executable`、`--health-url` 和
`--metrics-urls` 都是必填项；漏掉时会在 argparse 阶段立即退出，不会创建
manifest 或占用 GPU。新会话必须复制上面的完整命令，不能凭记忆缩写。

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
| **vllm** | **0.25.1 + `bench` extra** | **与本机 Docker `vllm/vllm-openai:v0.25.1` 对齐**；official baseline 还需同版本 `bench` extra(见 `experiments/results/adaptive_admission_controller_20260726/service.json`) |
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
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple 'vllm[bench]==0.25.1'
# 2) 再装其余(跳过 torch,保留 vllm 选定的版本)
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple \
  numpy "pyarrow>=16,<25" ray "psycopg[binary]>=3.2" daft sqlglot connectorx transformers
```

### 4.3.1 推荐 uv + 独立 venv(2026-07-27 4090 实测)
上面 §4.3 把 vllm 装进 base,可行但有两个更优做法(本节实测):
- **用 uv 替代 pip**:`pip install -U uv` 后
  `uv pip install 'vllm[bench]==0.25.1'`,解析+并行下载比 plain pip
  快一个量级(4090 上约 5 分钟 vs pip 30+ 分钟)。
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

# 在独立 vLLM venv 额外验证 official benchmark 能力；不能只验证 serve
/root/autodl-tmp/venvs/vllm-4090/bin/python -c \
  "import vllm,pandas,datasets,matplotlib,seaborn,scipy,plotly; print(vllm.__version__)"
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
# 1) 全部已配置 health endpoint 均成功（$PORTS 可能为 4：8000-8003）
for p in ${PORTS//,/ }; do
  curl -sf "http://127.0.0.1:$p/health" >/dev/null || exit 1
done

# 2) 进程命令包含固定 capacity
ps -eo pid,args | grep '[v]llm.entrypoints.openai.api_server'

# 3) 每张 GPU 上的服务进程与显存占用
nvidia-smi --query-compute-apps=pid,gpu_uuid,used_memory --format=csv,noheader

# 4) 全部 endpoint 的模型与 metrics 可读
for p in ${PORTS//,/ }; do
  curl -sf "http://127.0.0.1:$p/v1/models"
  curl -sf "http://127.0.0.1:$p/metrics" | grep -m1 '^vllm:'
done
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
  --workload-name sharegpt_multiturn --max-rows 2048 --batch-rows 500 \
  --tokenizer-path /root/autodl-tmp/models/Qwen2.5-1.5B-Instruct \
  --max-model-len 2048 --completion-max-tokens 16
# 单 endpoint baseline
python code/scripts/postgres_ai_operator_profile.py \
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator \
  --setup --total-rows 128 --db-fetch-rows 128 --ray-batch-rows 8 \
  --operator ai_complete --executor ray_task --model-backend compatible_http \
  --completion-endpoint-url http://127.0.0.1:8000/v1/completions \
  --completion-model qwen2.5-1.5b --completion-max-tokens 32 \
  --source-workload-name sharegpt_multiturn --data-source daft_postgres --organizer daft \
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
3. `dual_gpu_token_budget_curve.example.json`：feeding formal 通过并冻结
   `ACTIVE_WORK_PER_ENDPOINT` 后，使用 disjoint formal manifest、持久 async
   multi-prompt Completions 和 raw prompt，在同一 active-work 上限下扫描
   2/4/8/16/32/49/65K。它回答等量 offered work 下组织/提交形状是否有收益，
   同时验证预算过小的 RPC/packing 开销与预算过大的关批/HOL/排队代价；不得
   再把 active-work 和 token budget 同时变化。
4. `dual_gpu_data_organization.example.json`：使用上一步的最佳已测预算并继续
   使用同一个 disjoint manifest、async transport、raw prompt 并关闭 arrival
   replay，避免 50ms flush 在 token budget 生效前关批；在相同 active work
   下回答 fixed16、sequential token-budget、row-cap-aware 和 length-align
   的数据组织差异。
5. `dual_gpu_request_replay.example.json`：恢复相同 arrival replay/flush，
   比较 whole-submission barrier 与真正的 request-level replenishment。
6. `dual_gpu_actor_pool_shape.example.json`：沿用 request-level 饱和点，
   固定每 endpoint 256 个可见 actor slots和 0.5 Ray CPU reservation，比较
   1×256、2×128、4×64、8×32、16×16。runner 会按 repeat 交错顺序，避免
   把 GPU 温度或前一场景缓存漂移写成 actor 数效果。选择“达到峰值 97% 的
   最小 actor 数”，而不是追逐单次最高值；同时报告 repeat relative range。
   16×16 只用于确认平台/转折，不自动晋升默认，且必须保留 worker/VMA 证据。
7. `dual_gpu_service_quantum.example.json`：固定上一步最佳 pool、active work
   和 planning budget，比较 whole batch、512/1024/2048/4096 complete-row
   quantum 与 request diagnostic。当前组织批次 P95≈3366、max≈5892，因此
   8192 不会发生切分，禁止把它作为一个伪独立 arm。
8. `dual_gpu_slo_ewma_flush.example.json`：固定 request-level、65K
   per-endpoint active work 和 1×256 actor pool，在高压与临界到达率分别比较
   fixed-50ms、旧 two-level queue-adaptive 和 SLO-aware EWMA。新控制器只改变
   上游关批时间：用 arrival/global-service EWMA 的负载比在 25–50ms 间插值，
   以 oldest request slack 作为硬期限，反馈缺失或过期时回退到 fixed-50ms。
   当前 trace 标定显示 `0.002` 的 p50 offered/service ratio 仍约为 2.9，
   因此临界负载改用 `0.006`；该值是本 workload/双 endpoint 的实验点。
   burst/gap 下的 achieved service rate 不能充当容量，因为它会随 offered
   load 一起下降；模板显式固定前序饱和曲线标定的
   `--flush-service-capacity-tokens-s-per-endpoint 4000` 作为分母下界。
   更换模型、GPU 或 endpoint 数量时必须重新标定，禁止沿用 4000。
9. `dual_gpu_submission_policy.example.json`：在已标定 token budget 和
   active-work 配额上，使用持久 async Completions，并保留 batch-level
   multi-prompt body，逐项消融 least-work routing、service-quantum 动态预算
   和 queue-adaptive flush；最后的 combined arm 只检查交互，不替代单项结论。
10. `dual_gpu_static_k_workload_surface.example.json`：在 actor/token budget
    冻结后，以 low/near-capacity/burst 三种到达压力扫描 K64/128/256。
    完成后必须运行：

    ```bash
    python code/scripts/summarize_static_k_workload_surface.py \
      --runs "$OUTPUT_DIR/runs.csv" \
      --output "$OUTPUT_DIR/adaptive_justification.json" \
      --require-pass
    ```

    只有状态为 `passed` 才允许继续 adaptive formal；退出码 2 表示不同
    workload 的静态最优区间/错配代价不足，应该停止动态策略排名。
11. `dual_gpu_static_credit_prompt_length_gate.example.json`：07-30
    short/long screening 的纠错门禁。它在一个 runner 内交错 short/long，
    显式使用 `httpx_async` 与 output token IDs，并比较 K256、
    K256+W65K、K256+W98K。若某 work cap 未绑定且 bounded wait=0，该臂与
    K256 的 model-request throughput/P99 必须在 5% 内、至少 2/3 repeats
    同向；否则状态保持 `inconclusive`，禁止扩大静态面或启动 adaptive。
    门禁通过后才增加 W49K 和 K×work 交互，不得再把三档 K-only 加三档
    W-only 写成 “K×active-work factorial”。
12. `dual_gpu_endpoint_adaptive_gate.example.json`：仅验证双 endpoint typed
    controller 的独立 state、metrics 和 action trace，不产出性能结论。两个
    endpoint 都必须有 trace、0 failure、最终空队列后，才能制作漂移 formal。

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

完成硬件 scaling 后，依次运行 active-work、feeding 和 token-budget
calibration。三者通过后必须按本文件“冻结校准选择”生成选择文件和环境覆盖；
只有选择文件状态为 `ready`，才允许运行 data-organization、
submission-policy 和 shared-vLLM formal。不得从旧经验预填 8K、49K 或 K64。
预算扫描期间 active-work 配额必须不小于同场景单 batch 预算，否则会混入
oversized admission 语义。每轮都必须等待 runner manifest 为 `complete`，
不要手工拼接失败重跑的 CSV。

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

SLO-aware EWMA flush 必须在 quantum 矩阵确认 request-level 路径可靠后运行。
先从正式模板机械缩为 128 行、每场景 0 warmup + 1 repeat，并保留六个场景；
门禁必须核对 `flush_trace` 中 fixed/queue/SLO 三类 reason、arrival/service
rate、selected wait、oldest age 均可解释，且 request/submission/resource
trace exactly-once、零 failure、租约释放。正式矩阵再使用全新输出目录执行
1 warmup + 3 repeats。高压与临界负载必须分别比较，不能只用饱和场景推断动态
控制有效；若新策略吞吐/SLO-goodput 未提升至少 5% 且 P99 没有独立改善，则不
晋升为默认策略。

Pool-shape 模板还固定每 endpoint 的 Ray CPU reservation 为 0.5：
1×0.5、2×0.25、4×0.125。该值是 Ray placement/resource 契约，不等同于操作
系统 CPU 利用率；若任一 arm 因集群最小 fractional resource 限制无法创建，
gate 直接失败并记录 incident，不能只为该 arm 临时增加总 CPU 配额。

### Shared-vLLM 1/2/4-job 专用启动与清理

该实验使用 `run_shared_vllm_experiment.py`，不能套用单 profiler scenario
runner。控制面是一个 Ray named actor，数据面首轮使用 Ray task，所有 job
必须连接同一显式 Ray head。旧 `run_kmax_interference_experiment.py` 仅保留
历史前后台诊断用途，不得用于正式矩阵。

启动前：

```bash
cd /root/autodl-tmp/ai-operator
set -a
source /root/autodl-tmp/ai-operator-runtime.env
set +a
CONFIG=deploy/autodl/dual_gpu_shared_vllm_gate.example.json
OUTPUT_DIR=experiments/results/dual_gpu_shared_vllm_gate_<unique_id>
RUN_LOG=/root/autodl-tmp/logs/dual_gpu_shared_vllm_gate_<unique_id>.log

# 只读门禁：任一 runner、租约或忙 endpoint 存在时都不启动
ps -C python -C python3 -o pid=,etime=,args= |
  grep -E '[r]un_(ai_operator_scenarios|shared_vllm_experiment)\.py' || true
test ! -e "$OUTPUT_DIR/.runner-lease.json" ||
  cat "$OUTPUT_DIR/.runner-lease.json"
curl -fsS http://127.0.0.1:8000/metrics |
  grep -E '^vllm:num_requests_(running|waiting)' | tail -n 2
curl -fsS http://127.0.0.1:8001/metrics |
  grep -E '^vllm:num_requests_(running|waiting)' | tail -n 2
ray status 2>&1 || true

# 在确认没有旧 Ray workload 后，只启动一个实验专用 head
ray start --head --node-ip-address=127.0.0.1 --port=6380 \
  --disable-usage-stats
export RAY_ADDRESS=127.0.0.1:6380
ray status
```

环境准备仍只执行一次。并发 job 命令由 group runner 生成，配置中禁止
`--setup`、`--reset-documents`、output/trace、Ray address、本地/共享 credit
等 runner-owned 参数。

最小双 GPU gate：

```bash
test ! -e "$OUTPUT_DIR"
test ! -e "$RUN_LOG"
nohup /root/miniconda3/bin/python \
  code/scripts/run_shared_vllm_experiment.py \
  --config "$CONFIG" \
  --profiler code/scripts/postgres_ai_operator_profile.py \
  --python-executable /root/miniconda3/bin/python \
  --output-dir "$OUTPUT_DIR" \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls "$MODEL_METRICS_URLS" \
    --ray-address "$RAY_ADDRESS" \
    --idle-timeout-s 120 \
    --start-delay-s 15 \
    --max-start-lateness-s 2 \
    --max-start-skew-s 0.5 \
  </dev/null >"$RUN_LOG" 2>&1 &
```

该 gate 固定两个 job、每 job 64 行，并依次跑 independent、静态分区和
shared DRR。通过条件不是只看 `status=completed`，还必须核对：

- 每 job request/submission trace 均为 64 个唯一成功请求；
- `group_runs.csv` 为三行、0 incident、0 actor worker failure；
  - 两个 endpoint 都收到请求；
  - 每 job 实际 replay 起点不早于配置起点、迟到不超过 2s，跨 job 起点偏差
    不超过 0.5s；
  - shared-credit final snapshot 的 active/waiting request/work 均为 0；
- 精确峰值不超过每 endpoint 256 requests / 65,536 work；
  - group resource/credit trace 非空，组级 GPU utilization/MFU 可用，Ray 地址
    在所有 job 中一致；
- runner 正常释放目录租约，唯一 coordinator actor 已清理。

  gate 未通过时保留输出目录、日志、manifest、trace 和最终 snapshot，禁止删目录
  重跑或启动 formal。gate 全部通过后，换全新输出目录并将 config 改为
  `dual_gpu_shared_vllm_formal.example.json`；其余命令不变。当前受限
  AutoDL 容器的默认 formal 是
`{1,2,4} job × {independent, static partition, shared DRR}`，
  每场景 1 warmup + 3 repeats。4-job 必须先单独运行
  `dual_gpu_shared_vllm_j4_gate.example.json`，通过后才可使用
  `dual_gpu_shared_vllm_j4_formal.example.json`。完成或保存失败证据后再执行
  `ray stop`；不要在 runner 仍存活时停止 Ray head。

  共享实验的数据面必须使用 `ray_actor + httpx_async`，每 job、每 endpoint
  创建固定数量的持久 actor，并在 actor 内做有界并发。禁止把 4-job 配置改回
  `ray_task`：K256/endpoint 下四个 driver 可同时暴露上千个 task，
  `num_cpus=0.01` 又允许 Ray 扩张到数百 worker，已在
  `vm.max_map_count=65530` 的容器上触发 raylet `SIGABRT`。OMP/OpenBLAS
  单线程限制仍是必要条件，但只能消除每 worker 的线程膨胀，不能限制 worker
  进程数量；group runner 会在外部工作前拒绝 4-job `ray_task` 配置。

  j4 gate 必须额外保存 `cat /proc/sys/vm/max_map_count`、Ray worker 峰值和
  raylet 日志。若固定 actor pool 的 j4 gate 仍触发 VMA/pthread 故障，本机
  只报告 j1/j2；j4 标为宿主能力阻塞，迁移到更高 VMA 的容器后再运行，
  不得在同一 9-cell formal 尾部反复试错。

  coordinator 名称包含 manifest 持久化的 run-instance ID；同一输出目录 resume
  会连接同一物理 run，而新的输出目录会得到全新 actor 名称。失败 gate 保留的
  detached actor 因此不会污染下一次新目录 gate，仍不得手工复用旧输出路径。

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
| 用 `pgrep -af <runner 参数>` 做启动门禁 | 会匹配包含该文本的当前 SSH/bash 包装命令，误判已有 runner | 只枚举 Python：`ps -C python -C python3 -o args= \| grep '[r]un_ai_operator_scenarios.py'` |
| 把 `nohup ... &` 接在一长串 `&&` 后 | `&` 可能后台化整个 AND-list，只返回包装 shell PID；前置失败也可能被表面成功掩盖 | 前置检查用 `set -euo pipefail` 独立执行；`nohup` 单独一条命令，下一行立即保存 `runner_pid=$!` |
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

# 注意：以下为 legacy 2-endpoint 调试基线（每 GPU 1 副本、--gpu-memory-utilization 0.9、
# --max-model-len 2048）。当前 4-endpoint 部署需按 runtime env 的 $PORTS /
# $VLLM_GPU_MEMORY_UTILIZATION / $VLLM_MAX_MODEL_LEN 调整后再用；标准启动应改用
# start_endpoints.sh，本节仅作手动分步调试参考。
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

## OceanBase AI_COMPLETE capability gate (2026-07-29; capability verified 2026-07-31)

OceanBase is an optional product baseline, not a substitute for the
no-Daft/no-Ray bounded HTTP control. Capability gate #1 has PASSED: OceanBase
Community Edition 4.5.0.0 is statically confirmed to contain `AI_COMPLETE` and
`DBMS_AI_SERVICE` (observer binary `T_FUN_SYS_AI_COMPLETE` + seed SQL
`dbms_ai_service_*.sql`); see `experiments/results/oceanbase_b1_gate_20260731/`.
The current blocker is DEPLOYMENT, not capability: in this AutoDL container the
observer clogs at init step 4/18 with errcode -9100 (container seccomp blocks
clone3 / ENOSYS; unfixable from inside), so it must be re-run in a privileged
container or systemd VM. Do not include OceanBase in calibration or formal
results until a deployable host passes the following end-to-end gate:

1. both vLLM Chat Completions endpoints are healthy and idle;
2. the OceanBase version and MySQL-compatible tenant are recorded;
3. `DBMS_AI_SERVICE`, `AI_COMPLETE`, `DBA_OB_AI_MODELS`, and
   `DBA_OB_AI_MODEL_ENDPOINTS` are available;
4. the registered endpoint URL is the intended local
   `/v1/chat/completions` endpoint with provider `openai`;
5. one deterministic prompt completes successfully.

Run the read-only discovery section in
`deploy/autodl/oceanbase_ai_complete_gate.sql` first. The second section creates
a new explicit gate registration and must run only after every `BASELINE_*`
placeholder is replaced. It never drops databases, tenants, tables, models, or
existing endpoints. Preserve failed output as fatal-flaw evidence.

Formal dual-endpoint runs use different model keys and source/result tables for
each endpoint shard. CE 4.5.0 does not lack the AI Function service (capability
gate #1 passed); the current blocking issue is observer deployment (errcode
-9100 / container seccomp / clone3), not CE AI Function availability. Do not
replace a failed OceanBase cell with a custom Python
HTTP loop and label it OceanBase.

## Official baseline 双 GPU gate（2026-07-29）

该 gate 使用
`deploy/autodl/dual_gpu_official_baseline_gate.example.json`，目的只是证明
同一份 64 行 Chat Completions manifest 能由两张卡上的官方/强对照适配器
正确执行，不产生性能结论。calibration 规格在
`dual_gpu_official_baseline_calibration.example.json`；gate 未通过禁止运行。

开机后仍先完整执行 §10.5。随后按本节顺序操作：

1. 只读检查 scenario/shared/baseline runner、`.runner-lease.json`、Ray
   workload、两个 endpoint 的 `/health` 与 `/metrics`、GPU 和远端 git 状态。
   任一 runner、租约或非空 vLLM running/waiting 存在时不启动。
2. 只在 checkout idle 且没有 tracked-file 冲突时 `git pull --ff-only`；
   所有未跟踪结果原样保留。
3. 在 base conda 环境先检查 `httpx/openai/pandas/aiohttp/pymysql` import，
   仅安装缺失的项目声明依赖。Daft Ray 与 Ray Data 必须显式连接同一个已有
   Ray address，不能各自隐式创建 cluster。
   Ray 2.56 的 `ray.data.llm` 会间接导入 Ray Serve，因此还必须验证
   `from ray.data.llm import HttpRequestProcessorConfig`；缺少 Serve 依赖时
   按已安装 Ray 的相同版本补 `ray[data,serve]`，不能只补报错中的单个
   `starlette`。
4. 从正式 workload 导出 64 行 immutable manifest；hash、行数、模型、
   Chat protocol、temperature、输出上限和服务启动参数写入 gate 证据。
   两 endpoint 采用 manifest 中固定分片，不由 adapter 再路由。
   使用已提交的 `export-postgres-manifest`，固定
   `SOURCE_WORKLOAD_NAME`、`ORDER BY doc_id`、`row_offset=0`、
   `max_output_tokens=256` 与 `estimated_output_mode=trace_target`；
   不在远端临时编写 SQL/JSON 转换脚本。
5. 每个 core cell 只运行一次，两个 endpoint shard 同时启动；输出写到
   `experiments/results/dual_gpu_official_baseline_gate_<unique-id>/` 下的独立
   cell/shard 目录。目录已存在即停止，禁止覆盖或 resume 成新 gate。
   使用已提交的 `code/scripts/run_official_baseline_gate.py` 作为唯一 core
   编排入口；它按配置串行 cell、并行双 shard、保存命令/日志、等待队列归零并
   fail closed。禁止在远端临时手拼两个后台命令充当正式 gate runner。
   calibration 只允许用重复的 `--include-cell <id>` 选择已提交 cell，并用
   `--concurrency-override <id>=<per-endpoint-N>` 覆盖其并发。不得为了 C64/C128
   在远端复制或编辑 JSON；每个档位使用全新的 output root，并核对
   `resolved_config.json` 只包含预期 cell 与有效并发。
6. vLLM Bench 必须保存 `--save-detailed` 原始 JSON，再运行
   `normalize-vllm-bench`；其它 adapter 直接写共同 request schema。
7. 每个 cell 运行 `validate-gate`。必须满足 64/64 exactly-once、0 failed、
   两 endpoint 均使用、预测 work skew ≤2%、0 worker failure、相同服务
   元数据且最终 running/waiting 均为 0。

`project_static` 与 `project_token_work` 仍由现有 profiler/scenario runner
运行并保留完整 request/submission/resource trace；本节薄 CLI 不复制项目
调度实现。它们必须使用同一 manifest 对应的数据库行与相同 Chat protocol。
如果尚未提供无损的 manifest-to-profiler 映射，就将这两个 cell 标记为
`blocked`，不得改用相似随机 workload 代替。

OceanBase 是独立可选 capability gate。CE AI Function service 已确认存在
（capability gate #1 通过），当前阻塞为容器级部署（observer clog -9100 /
seccomp / clone3），待特权容器或 VM 内重跑；核心 gate 仍可继续。核心 gate 通过后也必须先
停止并分析 request-body 等价性、真实 HTTP request 数、Daft/Ray Data 的一行
一请求语义和原始 vLLM Bench schema，不能自动启动 calibration 或 formal。

### Official baseline 部署方案速查

部署与实验按以下状态机执行，不能跨级：

```text
main 已通过本地全量测试并推送
  -> 远端只读 idle/git/lease/endpoint/Ray/GPU/PG 检查
  -> 安全 fast-forward（保留未跟踪结果）
  -> base/vLLM 两套 Python API 与版本检查
  -> 只补项目声明的缺失依赖
  -> 64 行 immutable Chat manifest + 固定双 endpoint 分片
  -> 每个 core adapter 的单次双 endpoint gate
  -> exactly-once/元数据/work skew/服务端 token 差分/空队列门禁
  -> 停止并分析
  -> 独立 calibration
  -> 32/64/128/256 transient + 2,048 held-out formal
```

环境职责固定如下：

| 环境 | 用途 | 禁止 |
|---|---|---|
| `/root/miniconda3/bin/python` | manifest、bounded HTTP、Daft、Ray Data、项目 profiler、门禁 | 启动 vLLM |
| `/root/autodl-tmp/venvs/vllm-4090/bin/python` | 两个 vLLM endpoint 与 `vllm.benchmarks.serve` | 运行 Daft/Ray/project driver |
| 同一个现有 Ray address | Daft Ray、Ray Data、项目 Ray 路径 | 每个 adapter 隐式新建不同 cluster |

核心顺序是 `vLLM Bench -> bounded HTTP -> Daft Native -> Daft Ray ->
Ray Data HTTP`，项目 `static/token-work` 通过已有 profiler 运行。OceanBase
只在 CE AI Function capability gate 通过后加入。每个 cell 的两份 endpoint
输出必须放独立新目录；任一 adapter 失败时保留 `raw/`、`requests.csv`、
failed `summary.json` 和外层日志，不自动重试。

`dual_gpu_official_baseline_core_gate_20260729_1725_fix5708e85` 已完成第一轮
5/5 core adapter 功能门禁：每项 64/64 exactly-once、0 incident、两 endpoint
均使用、work skew 0.0085%，最终 running/waiting 为 0。该结果只证明链路可执行。
等价性修复提交 `f2e82bd` 已在全新目录
`dual_gpu_official_baseline_equivalence_gate_20260729_f2e82bd` 再次通过
5/5、最终队列归零；`--skip-chat-template` 与 `(n,n)` 固定 Ray Data actor
pool 均已生效。小 manifest 只有两个 16 行 task，日志虽创建 4 actor，实际只由
1 actor 执行，说明固定建池不等于小作业必然均匀使用全部 actor；后续必须按
actor 数与可并行 task 数联合标定。

该 re-gate 仍不产生性能结论。vLLM Bench、bounded/Ray Data 和 Daft 的客户端
token 统计口径不同，下一道门禁必须对每个 cell、每个 endpoint 在执行前后采样
`vllm:prompt_tokens_total` 与 `vllm:generation_tokens_total`，保存原值和差分。
服务端差分门禁通过前仍禁止 calibration/formal。

### Project profiler 同 manifest 校准与 formal

项目 runtime 不再使用“相似 workload”与 direct baseline 横比。三个已提交模板
依次固定 512 行等价性门禁、512 行校准和 2,048 行 disjoint formal：

- `dual_gpu_same_condition_project_equivalence_gate.example.json`
- `dual_gpu_same_condition_project_calibration.example.json`
- `dual_gpu_same_condition_project_formal.example.json`

两者都强制一行一个 Chat Completions 请求、原始 prompt、`temperature=0`、
`max_tokens=256`、trace-target output cost、no arrival replay、request-level
continuous replenishment、同一 manifest 固定 endpoint 分片和显式
`RAY_ADDRESS`。profiler 会逐行核对 `doc_id/text/prompt_tokens/
target_output_tokens`，并在 `runs.csv` 记录 manifest path、SHA、总行数、
validated rows 与状态；任一不一致即失败。

启动前除通用 idle 检查外，还要执行：

```bash
set -a
source /root/autodl-tmp/ai-operator-runtime.env
set +a

export COMPLETION_CHAT_ENDPOINT_URLS=\
http://127.0.0.1:8000/v1/chat/completions,http://127.0.0.1:8001/v1/chat/completions
export RAY_ADDRESS=127.0.0.1:6380

/root/miniconda3/bin/python - <<'PY'
import ray
ray.init(address="127.0.0.1:6380")
print(ray.cluster_resources())
ray.shutdown()
PY
```

512 行校准复用 direct calibration 已冻结的只读 manifest，不重新导出：

```bash
export PROJECT_CALIBRATION_REQUEST_MANIFEST=\
/root/autodl-tmp/gates/official_baseline_calibration_512_20260729_0f5d60f.jsonl
```

先运行理论等价的 K256 与 nonbinding W98K 门禁。两者各有一个同压力 warm-up
和三个交错 formal repeat；actor ready barrier 在 measured E2E timer 之前，
`actor_ready_s` 单独记录，submission trace schema 5 记录 HTTP request start、
response headers 和 body-read 边界：

```bash
CONFIG=deploy/autodl/dual_gpu_same_condition_project_equivalence_gate.example.json
OUTPUT_DIR=experiments/results/dual_gpu_same_condition_project_equivalence_gate_<unique-id>
RUN_LOG=/root/autodl-tmp/logs/dual_gpu_same_condition_project_equivalence_gate_<unique-id>.log

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

只有两臂 repeat-mean throughput 与 JCT 均在 5% 内、至少 2/3 repeats 落在
该范围、且所有正确性门禁通过，才允许运行完整校准。失败时保留目录、日志、
lease、request/submission/resource traces 与 endpoint 日志，停止；不得通过
换顺序或删除首轮数据继续 formal。`headers_wait` 包含 connect、HTTP ingress、
vLLM queue 与 inference，不能单独解释为 server accept 或 GPU compute。

等价性门禁通过后才运行完整校准：

```bash
CONFIG=deploy/autodl/dual_gpu_same_condition_project_calibration.example.json
OUTPUT_DIR=experiments/results/dual_gpu_same_condition_project_calibration_<unique-id>
RUN_LOG=/root/autodl-tmp/logs/dual_gpu_same_condition_project_calibration_<unique-id>.log

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

校准模板扫描 per-endpoint static K `{32,64,128,256}` 和 active work
`{16384,32768,49152,65536,98304}`，每个配置一个同压力 warm-up 和三个
formal repeat。完成后按预注册 97% ceiling / 相邻增益
<3% 规则选择最小压力点，写入 `PROJECT_STATIC_K_PER_ENDPOINT` 和
`PROJECT_ACTIVE_WORK_PER_ENDPOINT`。不要因为 C256 是 `max_num_seqs` 配置硬上限
就把它误写成已验证的经验平台。

formal manifest 必须来自一个独立的 2,048 行 workload 切片（远端数据库现已持有多个 2,048 行重建 workload：`sharegpt_multiturn` doc_id 300000-302047、`sharegpt_concentrated`、`sharegpt_burstgpt` 等），不再使用旧的 `ORDER BY doc_id LIMIT 2048 OFFSET 512` 或 append `2048..2559` 方案；禁止回用校准行。选定 disjoint workload 后导出只读 manifest：

```bash
PGPASSWORD=postgres psql -h 127.0.0.1 -U postgres -d ai_operator \
  -c "SELECT workload_name, count(*), min(doc_id), max(doc_id) FROM documents GROUP BY workload_name ORDER BY workload_name"

export PROJECT_FORMAL_REQUEST_MANIFEST=\
/root/autodl-tmp/gates/<new-immutable-formal-manifest>.jsonl

/root/miniconda3/bin/python code/scripts/run_official_baseline.py \
  export-postgres-manifest \
  --database-url "$DATABASE_URL" \
  --workload-name "$SOURCE_WORKLOAD_NAME" \
  --row-count 2048 \
  --row-offset 512 \
  --max-output-tokens 256 \
  --estimated-output-mode trace_target \
  --endpoint-count 2 \
  --output "$PROJECT_FORMAL_REQUEST_MANIFEST"
chmod 0444 "$PROJECT_FORMAL_REQUEST_MANIFEST"
```

随后先另外导出 `row_count=64,row_offset=512` 的只读 gate manifest，并在
`/root/autodl-tmp/gates/` 复制 formal 模板，机械改为 64 行、0 warm-up、
1 repeat，令 `PROJECT_FORMAL_REQUEST_MANIFEST` 暂时指向该 gate manifest。
在新目录核对 manifest SHA/validated rows、64/64 exactly-once、双 endpoint、
request/submission/resource traces、0 failure、服务端 counter 和最终空队列。
只有 gate 通过才把变量切回 2,048 行 manifest 并运行原模板的 1 warm-up +
3 formal；失败目录、租约和日志原样保留。

### 已验证部署问题与解决方案

| 现象 | 根因与判定证据 | 处理方式 |
|---|---|---|
| `git merge --ff-only` 报 untracked files would be overwritten | 远端历史正式结果仍是未跟踪文件，而新 `main` 已跟踪同路径 | 先逐文件比较工作副本与 `origin/main:path` 的 SHA-256；不一致时再做内容 diff。把原始文件逐个移动到 `/root/autodl-tmp/premerge-backups/<unique-id>/`，复核移动前后 hash 后再 fast-forward。禁止 `git clean`、直接删除或覆盖整个结果目录。 |
| `group_runs.csv` SHA 不同，但大小只差每行 1 字节 | 远端文件为 CRLF；`tr -d '\r'` 后与 Git 版本 SHA 完全相同。本次 37 行恰好多 37 字节 | 仍保留原始 CRLF 备份；以规范化 hash 证明数值内容一致，再让 Git 恢复受控 LF 版本。不能仅因文件大小接近就假定相同。 |
| `from ray.data.llm import ...` 报 `No module named starlette` | Ray 2.56.1 的 `ray.data.llm` 间接导入 Ray Serve；包元数据明确把 FastAPI/Starlette/gRPC 等列在 `serve` extra | 项目声明 `ray[data,serve]`。远端使用当前 Ray 的精确版本安装，如 `ray[data,serve]==2.56.1`；不要只装 `starlette`，否则仍可能逐个暴露传递依赖。 |
| vLLM Bench 原始结果无法按旧字段解析 | vLLM 0.25.1 的详细结果使用 `input_lens`、`output_lens`、`e2els`，没有 `request_latencies` | 启动前用安装环境源码/API 检查字段；归一化器接受 `e2els`。必须保留 `--save-detailed` 原始 JSON，字段不匹配时停止并加回归测试。 |
| vLLM Bench 与其它 arm 输出随机性不等价 | vLLM 0.25.1 源码明确提示 `bench serve` 不再默认 `temperature=0`；不传参数会破坏 frozen Chat contract | 命令构造器显式传 `--temperature 0` 并用回归测试锁定。升级 vLLM 时在真实 gate 前再次只读检查已安装源码/`--help`，不能依赖历史默认值。 |
| `python -m vllm.benchmarks.serve` 返回 0 但无 stdout/结果 JSON | 0.25.1 的 `benchmarks/serve.py` 只定义函数，没有模块级 `main`；真正 console script 指向 `vllm.entrypoints.cli.main` | 使用相同 vLLM Python 执行 `python -m vllm.entrypoints.cli.main bench serve ...`，回归测试锁定前五个 argv。禁止仅凭子进程退出码 0 判定 benchmark 已执行；还必须存在详细 JSON 并通过归一化。 |
| vLLM Bench 启动后数分钟不发请求，日志出现 Hugging Face repo 重试 | `--model qwen2.5-7b` 是 served alias；0.25.1 在 `--tokenizer` 缺失时把 model id 当 tokenizer id，尝试访问远程仓库 | gate 模板和 runner 必须显式传本机 `MODEL_PATH` 对应的 tokenizer 目录；加载配置和单 shard CLI 都检查目录实际存在。确认两端 queue 始终为 0 后才可终止无效 client，保留 `run_status.json` 和 shard log。 |
| 0.25.1 详细 JSON 有请求结果但没有 `e2els` | 实际文件保留 `start_times`、`ttfts`、逐请求 `itls`；源码的 timeline 路径也以 `ttft + sum(itls)` 重建 latency | 归一化器优先接受直接 E2E 数组，否则按官方源码公式重建；以相对 start time 还原 JCT，并要求重建 duration 与顶层 duration 在 `max(100ms, 2%)` 内一致。失败/错误数组也必须为 0/空。 |
| vLLM Bench 的每行 input token 比 bounded HTTP 多一个 chat wrapper | 0.25.1 `CustomDataset.sample()` 默认先对裸 prompt 执行 `apply_chat_template`；`openai-chat` backend 又把生成后的字符串作为 user content 发给服务端，服务端再次套 template。真实 gate 中首行 92 vs 63 tokens，差值正好对应额外 wrapper | 对 custom 裸 prompt + `openai-chat` 明确传 `--skip-chat-template`，只允许服务端套一次模板。注意修复后 bench 的 `input_lens` 仍是裸 prompt 客户端口径，而服务端 usage 包含一次 chat wrapper，二者不应强制逐行相等；公平比较使用 endpoint-local vLLM 服务端 prompt/generation counter 差分。 |
| bounded HTTP 的 endpoint 1 shard 报全局索引越界 | shard 子进程只持有一条本地 URL，但 immutable manifest 必须保留全局 `endpoint_index=1` | `BoundedHttpConfig` 增加 `endpoint_index_offset`，仅在访问本地 semaphore/URL 时计算局部索引；结果与 gate 仍使用全局 endpoint id，禁止改写 manifest。 |
| Ray Data 已连接 6380，但 actor pending 后报 `ModuleNotFoundError: No module named 'src'` | driver 脚本只把仓库 `code/` 加到本进程 `sys.path`；Ray worker 反序列化 `HttpRequestUDF` 时没有继承该临时修改。日志中“资源不足、0 CPU”是 actor 构造失败后的伴随告警，不是本次根因 | `ray.init(address=...)` 必须同时传 `runtime_env.env_vars.PYTHONPATH=<repo>/code[:existing]`，让同一集群的 worker 可导入项目模块。先以 actor traceback 判定根因，禁止因 pending 告警直接扩容、重启 Ray 或提高并发。修复后使用全新输出目录重跑 gate，失败目录原样保留。 |
| Ray Data 配置 `concurrency=4`，小 gate 只启动 1 个 actor | Ray 2.56 公共说明把整数写成并发上限，而内部 `ProcessorConfig.get_concurrency()` 可把整数解释为 `min=1,max=n` autoscaling；本次日志明确为 `Actors: 1` | 为使 baseline 参数可复现，包装器把 `n` 显式传为 `(n,n)` 固定 actor pool。`batch_size` 仍是每 actor 每 task 的行数，不能当作 vLLM 内部 batch；calibration 必须独立扫描 batch size 与固定 actor 数。 |
| Daft/Ray Data 的 P50/P95 全等，Daft output token 为 0 | Daft `prompt()` 只返回最终文本，当前没有逐请求 usage/timing；Ray Data 官方 Processor 返回 usage，但包装器只能在 shard barrier 外观察开始/结束。把公共 barrier 复制给每行会产生不可比较的“伪 P95” | summary 显式记录 `timing_granularity` 和 `token_accounting`。Daft 标为 `shard_barrier/manifest_prompt_only`，Ray Data 标为 `shard_barrier/server_usage`；正式比较必须补服务侧 token/请求 trace，或只报告 JCT，不得把 barrier P95 与 request-level P95 横比。 |
| 不同 adapter 的 input/output token 无法直接对齐 | vLLM Bench 记录裸 prompt 的 `input_lens`，bounded/Ray Data 记录服务端 usage，Daft 只返回文本；直接比较客户端字段会把 chat-template 与 API 能力差异误写成工作量差异 | gate runner 在每个 cell 前后分别采样每个 endpoint 的 vLLM cumulative prompt/generation counters，保存 `service_counters.json` 并把差分写入 summary。`server_usage` 必须与两项服务端差分完全一致；official bench 只要求 output 与 generation 差分一致；Daft 以服务端差分作为统一工作量证据。任一差分非正、counter 回退、endpoint 集变化或可核对字段不一致均 fail closed。每次采样期间禁止其它请求污染 endpoint。 |
| gate CLI 在失败验证前未留下失败请求行 | 先 summarize/validate，异常发生在写 `requests.csv` 之前 | 现在先原子写 request rows，再验证；失败时写 `status=failed` summary 并保持非零退出。不得用重试覆盖失败现场。 |
| 8000/8001 被误判为服务配置不一致 | 初版 service fingerprint 把 endpoint URL/端口也纳入 hash | 服务指纹只比较模型、协议、temperature 等等价配置；endpoint 地址作为独立拓扑字段审计。实际 vLLM 启动参数仍需从两个进程命令和 service metadata 单独核对。 |
| 已有单 shard CLI，但没有可复现的双 endpoint gate runner | 临时拼接后台命令无法保证两个 shard 先启动再等待，也没有逐 cell 失败即停、空队列盖章和统一日志 | 新增 `run_official_baseline_gate.py`。每个 cell 保存 `commands.json`、两份 shard log、两份归一化结果和 `gate.json`；根 `run_status.json` 记录已完成与 blocked cell。已有输出目录拒绝覆盖。 |
| gate 配置固定 5 个 core arm 与 C32，无法安全只跑 vLLM/bounded C64/C128 | 在远端临时复制 JSON 或手拼 shard 会绕过已提交配置、统一编排和审计证据 | 使用 runner 的 `--include-cell` 与 `--concurrency-override id=N`。未知、重复、非正或覆盖未选 cell 均 fail closed；每档使用新输出根并以 `resolved_config.json` 复核。 |
| C32/C64 时 vLLM Bench 与 bounded 一致，C128 时 bounded 突然落后 | httpx 0.28.1 `AsyncClient` 默认总连接上限 100、keepalive 20；配置 C128 实际没有形成 128 并发。vLLM Bench 日志则确认 peak=128 | bounded client 必须显式把 `Limits.max_connections` 与 `max_keepalive_connections` 设为 `concurrency_per_endpoint × endpoint_count`，用回归测试锁定。修复后只在全新目录重跑被污染的 bounded C128；有效 vLLM C128 不重复。 |
| gate 模板仍写本地历史模型 `qwen2.5-1.5b` | AutoDL 当前两个 endpoint 实际 served model 为 `qwen2.5-7b`；模板与服务元数据不一致会污染同条件比较 | 双 GPU official gate 模板改为 `qwen2.5-7b`。每次开机仍以 runtime env、endpoint 进程命令和 `/metrics` 为准，不从模板猜模型。 |
| `python -m unittest code.tests...` 报标准库 `code` 没有 `tests` | 仓库目录名 `code/` 与 Python 标准库模块同名，不是测试实现失败 | 在仓库根使用 `python -m unittest discover -s code/tests -p 'test_x.py'`。远程封装先保存测试进程退出码，再清理临时环境变量，避免清理命令把失败状态覆盖成 0。 |
| project Chat template 展开时报缺失变量，或仍请求 `/v1/completions` | 旧 runtime env 只有 `COMPLETION_ENDPOINT_URLS`，没有同条件 Chat URL；直接复用会改变协议 | 从更新后的 `autodl.env.example` 补 `COMPLETION_CHAT_ENDPOINT_URLS=.../v1/chat/completions`，启动前打印解析后的模板参数；禁止用 legacy URL 兜底。 |
| 512 校准后无法导出 disjoint 2,048-row formal | 当前 workload 只有 2,048 行，`OFFSET 512` 后数据库实测仅返回 1,536 行 | formal 前补齐并冻结另外 512 行或导入独立 held-out workload；profiler 使用 `--source-row-offset 512`。不得回用 `doc_id=0..511` 或复制行凑数。 |
| project 64 行 gate 在 HTTP 前报 `target_output_tokens mismatch` | official manifest 将 trace target 裁到 `max_tokens=256`，project profiler 曾比较和调度未裁剪的数据库原值；大于 256 的行因此既校验失败又高估 active work | `trace_target_output` 的统一语义为 `min(target_output_tokens, completion_max_tokens)`；校验仍保留 fail-closed。修复提交通过本地完整测试后，必须在全新目录重新运行 64 行 gate，旧失败目录不覆盖。 |
| 同样 512 outstanding 的 W98K 比 K256 慢约 2.83× | 只读诊断排除 active-work 背压、actor 数、payload、output work 和汇总计算；主要差异是首个 full-concurrency cell 在 HTTP/vLLM request wall 多约 28.6s，并出现 endpoint 不对称逐波接纳 | 不选取该单次结果做参数。先加 actor-ready barrier、同压力 warm-up 和 HTTP headers/body timing，只复测理论等价 K256/W98K；门禁未通过禁止扩大矩阵。 |

### 安全补齐 disjoint held-out 行

禁止用 `--start-doc-id 2048` 重新导入开头 512 个 prompt，也禁止使用默认
upsert 补数。只读审计已确认：ShareGPT SHA-256
`35f0e213…f6479ba4`、BurstGPT SHA-256 `4bb37836…e12122`，现有 2,048 行
文本/session 与 Qwen2.5-7B tokenizer 全部一致；原始 pair capacity 90,122，
足够补齐。历史 shell 命令没有保留，不能声称恢复了 exact CLI；现有行的跳过
边界和正式 source predicate 均支持显式 `max_prompt_tokens=1500`。最终是否
同源由下述 2,048 行逐字段核验决定：

```bash
/root/miniconda3/bin/python code/scripts/import_ai_complete_workload.py \
  --database-url "$DATABASE_URL" \
  --sharegpt-json "$EXACT_SHAREGPT_JSON" \
  --burstgpt-csv "$EXACT_BURSTGPT_CSV" \
  --workload-name "$SOURCE_WORKLOAD_NAME" \
  --tokenizer-endpoint-url http://127.0.0.1:8000/tokenize \
  --tokenizer-model "$COMPLETION_MODEL" \
  --max-prompt-tokens 1500 \
  --max-model-len 8192 \
  --completion-max-tokens 256 \
  --max-rows 512 \
  --start-doc-id 2048 \
  --source-row-offset 2048 \
  --verify-existing-prefix-rows 2048 \
  --append-only \
  --dry-run
```

只有输出 `status=verified_dry_run`，并确认数据库仍为 2,048 行后，才允许在同一
idle 窗口用同一命令移除 `--dry-run`。`source-row-offset` 按所有过滤完成后的
eligible rows 计数；`append-only` 遇到任一 doc ID 冲突即使事务失败，不更新旧
行。显式 prompt 上限避免为复现 1,500 边界而伪造历史
`max_model_len - completion_max_tokens` 组合。写入后重新核对连续
`doc_id=0..2559`、总数 2,560，再导出 2,048 行
`OFFSET 512` manifest 并设为 `0444`。若 prefix 任一字段不一致，停止并恢复
过滤证据，不得尝试“近似匹配”或覆盖数据库。

本次远端原始冲突备份位置为
`/root/autodl-tmp/premerge-backups/20260729_shared_vllm_results_before_7267324/`。
它是事故审计副本，不是新的 formal 结果目录。

## 双协议 feeding 校准与正式 baseline 顺序（2026-07-30）

### 目标

先验证项目提交路径能否持续喂饱 vLLM，再测试 token-budget、动态 K 或 adaptive
flush。对固定 512 行 manifest，feeding 主口径使用服务端 token counter 除以
`model_request_wall_s`，并与同协议 bounded service JCT/throughput 比较；
warmed project 必须达到至少 95%，且 model-request JCT 不超过 1.05×、
0 failure、exactly-once、最终队列为空。完整 `operator_wall_s`/E2E 仍报告，
但 source fetch/Daft 时间不能误归因给 vLLM feeding。未通过时停止策略矩阵，
只分析 Ray/HTTP/ingress。

两条轨道不得交叉排名：

| 轨道 | direct/bounded 配置 | project 配置 |
|---|---|---|
| Chat 产品兼容 | 既有 official baseline gate/calibration | `dual_gpu_project_chat_feeding.example.json` |
| multi-prompt Completions 机制 | `dual_gpu_completions_baseline_gate.example.json` | `dual_gpu_project_completions_feeding.example.json` |

Chat project 配置比较旧 threaded `urllib`、持久 `httpx_async` 和
1×256/2×128/4×64 actor 形状；每行仍是一条 Chat 请求，actor 内使用 async
dispatch。Completions 两份配置都比较 fixed rows 1/4/16/32，并保持
`batch_rows × HTTP concurrency=256` per endpoint；project 端一个 Ray
submission 仍发送一个含多条完整 prompt 的 HTTP body。

### 开始前

1. 执行 §10.5 的只读 idle/lease/Ray/endpoint/GPU/PG 检查；
2. 使用独立 git worktree 和全新输出目录，禁止覆盖已有结果；
3. `source deploy/autodl/autodl.env` 后确认
   `COMPLETION_ENDPOINT_URLS` 以 `/v1/completions` 结尾，
   `COMPLETION_CHAT_ENDPOINT_URLS` 以 `/v1/chat/completions` 结尾；
4. 模型不存在时必须按 §5 先启用 AutoDL 学术加速并禁用 Xet；不要直接使用
   未加速的默认 Hugging Face 下载；
5. 两个 vLLM endpoint 必须是同一模型/版本/参数，且 512 行 immutable
   manifest SHA-256 与 direct Chat 校准一致。

### 命令

先跑 multi-prompt direct/bounded 固定行数 gate：

```bash
/root/miniconda3/bin/python code/scripts/run_official_baseline_gate.py \
  --config deploy/autodl/dual_gpu_completions_baseline_gate.example.json \
  --driver-python /root/miniconda3/bin/python \
  --vllm-python /root/autodl-tmp/venvs/vllm-4090/bin/python \
  --output-root \
    experiments/results/dual_gpu_completions_baseline_gate_<unique-id>
```

再分别运行 project Chat 与 Completions feeding 矩阵：

```bash
/root/miniconda3/bin/python code/scripts/run_ai_operator_scenarios.py \
  --config deploy/autodl/dual_gpu_project_chat_feeding.example.json \
  --profiler code/scripts/postgres_ai_operator_profile.py \
  --python-executable /root/miniconda3/bin/python \
  --output-dir experiments/results/dual_gpu_project_chat_feeding_<unique-id> \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls "$MODEL_METRICS_URLS"

/root/miniconda3/bin/python code/scripts/run_ai_operator_scenarios.py \
  --config deploy/autodl/dual_gpu_project_completions_feeding.example.json \
  --profiler code/scripts/postgres_ai_operator_profile.py \
  --python-executable /root/miniconda3/bin/python \
  --output-dir \
    experiments/results/dual_gpu_project_completions_feeding_<unique-id> \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls "$MODEL_METRICS_URLS"
```

每次只允许一个 runner。先检查 `resolved_config.json` 中协议、transport、
actor 数、每 actor concurrency、K 和 manifest，再查看 `runs.csv`、
submission/request trace 与服务端 counter。full feeding 配置是正式校准，
本地或新 worktree 的可运行性验证只需使用 16/64 行 smoke，不得把 smoke
数字写成性能结果。

### 冻结校准选择

feeding、token-budget 和**同协议 actor-shape** 曲线完成后，不得手工凭记忆
修改 8K/K64/actor 数。使用已提交脚本从原始证据生成不可歧义的选择文件和
环境覆盖：

```bash
ARTIFACT_ROOT=/root/autodl-tmp/experiment-artifacts
CALIBRATION_ROOT=/root/autodl-tmp/gates/calibration_<commit>
mkdir -p "$CALIBRATION_ROOT"

/root/miniconda3/bin/python \
  code/scripts/select_strategy_calibration.py \
  --feeding-runs \
    "$ARTIFACT_ROOT/<project-completions-feeding>/runs.csv" \
  --direct-baseline-root \
    "$ARTIFACT_ROOT/<direct-completions-gate>" \
  --token-budget-runs \
    "$ARTIFACT_ROOT/<token-budget-curve>/runs.csv" \
  --actor-shape-runs \
    "$ARTIFACT_ROOT/<completions-actor-shape>/runs.csv" \
  --output "$CALIBRATION_ROOT/selection.json" \
  --env-output "$CALIBRATION_ROOT/calibration.env"

python -m json.tool "$CALIBRATION_ROOT/selection.json"
set -a
source /root/autodl-tmp/ai-operator-runtime.env
source "$CALIBRATION_ROOT/calibration.env"
set +a
```

脚本只接受至少三次成功 formal feeding/token-budget/actor-shape 重复，要求项目
model-request throughput 达到同协议 direct baseline 的 95%，并按
97%-ceiling/下一档增益小于 3% 规则选择最小 token budget。actor shape 必须
保持总 slots 不变，并选择达到峰值 97% 的最小 actor 数。它最终冻结
per-endpoint K、active work 和 **Completions** actor shape；Chat actor 曲线
只能作为协议特定诊断，禁止传给该选择命令。

`dual_gpu_data_organization.example.json`、
`dual_gpu_submission_policy.example.json` 和
`dual_gpu_shared_vllm_formal.example.json` 会在启动任何外部请求前读取同一
选择文件，并逐项核对环境值。文件缺失、evidence 未通过、仍为旧 8K/K64 或
任一值不一致都会 fail closed，且不会创建实验输出目录。

### 放行后的实验

1. 分别标定 Chat actor/K 和 Completions `batch_rows × concurrency` 的最小
   97%-ceiling 点；
2. 在 Completions 轨道固定 active work，扫描 token budget
   2K/4K/8K/16K/32K/49K/65K，证明预算不是越大越好；
3. 生成并核对上述冻结选择文件；
4. 再做 length-align、queue-adaptive flush、dynamic token budget 和动态 K
   单因素消融；
5. 单 job 通过后跑 1/2/4 job；4-job 必须先通过独立 gate，j4-only formal
   用于失败隔离/复验。多 job 子进程
   和 Ray worker 必须继承 `runtime_env.py` 的单线程 BLAS 环境，并使用有界
   persistent actor pool；旧 `ray_task` j4 失败结果不参与排名；
6. 最后在 disjoint held-out、database E2E 和多模态上复验。

### Pinned endpoint active-work 背压故障

若 project request-level 场景在启用
`--max-active-work-per-endpoint` 后报
`preferred endpoint ... is not healthy`，先同时核对：

1. stderr 中失败请求的 `preferred_endpoint_id` 与 estimated work；
2. 失败前该 endpoint 的 local active request/work；
3. 另一 endpoint 是否仍有容量；
4. 两个 `/health`、`/metrics` 与最终队列。

若服务健康、固定 endpoint 仅因加入当前请求会超过 work cap 而被标为
`healthy=false`，这是旧版把容量复用为健康状态的已知缺陷，不得重启 vLLM、
改写 manifest 或改投另一 endpoint。保留失败目录和 lease 证据，使用包含
`EndpointSnapshot.available` 与 typed capacity backpressure 的新提交，在全新
目录先重跑 64 行 gate。门禁必须核对 exactly-once、固定 endpoint 分布、0
worker failure、服务端 counter 和最终空队列；通过后才允许重新启动 512 校准。
