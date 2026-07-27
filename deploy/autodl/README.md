# AutoDL 云部署指南

本指南沉淀 2026-07-27 把项目部署到 AutoDL(2× GPU 云服务器)的全流程经验,目标是可在云上复现本机实验并补"多 endpoint / 多 GPU"真实验证缺口(见根 `AGENTS.md` §3、`motivation/results/gpu/multi_endpoint_ray_motivation_20260712.md` 第 83 行)。

指南面向"从零起一台 AutoDL 实例到跑通首个多 endpoint 实验"。所有命令均为 Linux bash(远端)。

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
| 镜像版本 | **PyTorch 2.8.0 / Python 3.12 / Ubuntu 22.04 / CUDA 12.8** | 见 §4 版本兼容性 |
| 数据盘 | 默认(`/root/autodl-tmp`,50GB+) | 模型+数据落这里,跨开关机保留 |
| 文件存储 `autodl-fs` | **不开** | 付费跨实例持久化;模型重下比买存储划算 |
| 学术资源加速 | **非自动**,每个 shell 会话需 `source /etc/network_turbo` | 加速 github/huggingface(代理 `172.20.0.113:12798`);不 source 则三源全慢(见 §5) |

**开机顺序(省钱)**:先「无卡模式开机」(仅 CPU,约 ¥0.1/h)→ 下模型、装依赖、配环境 → 关机 → 「正常开机」(GPU 模式)→ 跑实验 → 跑完立刻关机。多卡按量计费烧钱快,纯 CPU 的 setup 阶段不要用 GPU 模式。

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

目标:`Qwen/Qwen2.5-1.5B-Instruct` → `/root/autodl-tmp/models/Qwen2.5-1.5B-Instruct/`(safetensors ~3.09GB)。

### 5.1 必开加速 + 禁 Xet
```bash
source /etc/network_turbo >/dev/null 2>&1   # 代理 172.20.0.113:12798,仅 github/hf
export HF_HUB_DISABLE_XET=1                  # 否则走 cas-server.xethub.hf.co 报 401
```
不开 `network_turbo` 时 HF 直连/`hf-mirror.com`/modelscope 全部极慢(8 kB/s ~ 700 kB/s 且会 stall)——这是本次最大的坑。

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
  https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
  > /etc/apt/sources.list.d/pgdg.list
apt-get update
apt-get install -y postgresql-16 postgresql-16-pgvector
```
起服务 + 建库:
```bash
pg_ctlcluster 16 main start 2>/dev/null || service postgresql start
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres';"
sudo -u postgres psql -c "CREATE DATABASE ai_operator;"
sudo -u postgres psql -d ai_operator -c "CREATE EXTENSION IF NOT EXISTS vector;"
```
连接串:`postgresql://postgres:postgres@localhost:5432/ai_operator`。

**版本边界**:云上 PG16 ≠ 本机 PG18.4 Docker ≠ 公司 PG18.3 平台。CSV 记 `server_version`,报告标注。

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

每张卡一个 endpoint,用 `CUDA_VISIBLE_DEVICES` 钉死。脚本(建议存为项目内 `code/scripts/autodl_start_endpoints.sh`):
```bash
#!/usr/bin/env bash
set -u
source /root/miniconda3/etc/profile.d/conda.sh && conda activate base
MODEL=/root/autodl-tmp/models/Qwen2.5-1.5B-Instruct
start_ep() {
  CUDA_VISIBLE_DEVICES=$1 nohup python -m vllm.entrypoints.openai.api_server \
    --model "$MODEL" --served-model-name qwen2.5-1.5b --dtype auto \
    --max-model-len 2048 --gpu-memory-utilization 0.9 \
    --port $2 --host 127.0.0.1 \
    </dev/null >/root/autodl-tmp/vllm_ep_$2.log 2>&1 &
}
pkill -f "vllm.entrypoints" 2>/dev/null; sleep 2   # 这条由脚本文件执行,cmdline 不含该模式,不自匹配
start_ep 0 8000
start_ep 1 8001
for p in 8000 8001; do
  for i in $(seq 1 120); do
    curl -sf "http://127.0.0.1:$p/health" >/dev/null && { echo "$p ready"; break; }
    sleep 3
  done
done
```
homogeneous(两卡都 serve `qwen2.5-1.5b`)即可与本机单 endpoint baseline 可比。异构(1.5B + 7B)留作后续"异构 actor pool"实验。

验证:`curl http://127.0.0.1:8000/v1/completions ...`、`nvidia-smi` 看两个 vLLM 分别占 GPU 0/1。

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
| **Blackwell(sm120)卡** | flashinfer `sm75` 报错 + CUDA12/13 库混 + quack/cutlass API 不匹配,连环失败 | **换非 Blackwell 卡(4090/A100/3090)或用官方 Docker**;详见 §12 |

---

## 11. 平台边界声明(写进结果报告)

云上实验结果必须标注,不可与本机结论混同:

- **平台**:AutoDL 云服务器,2× GPU(**非 Blackwell**,如 RTX 4090 sm89;**不要用 50xx/6000D 等 sm120 卡,见 §12**),Ubuntu 22.04。
- **Python 环境**:miniconda3,Python 3.12.3。
- **vLLM**:0.25.1(pip 安装,torch 2.11.0 manylinux)。本机为 0.25.1 Docker(cu129)。CUDA patch 不同,vLLM 版本对齐。
- **PostgreSQL**:16 + pgvector(apt 装于 AutoDL)。本机为 18.4 Docker;公司平台为 18.3。三者不等同。
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
