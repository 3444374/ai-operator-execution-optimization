# 跨机器运行时与资产管理

本目录解决“同一仓库在不同 GPU、目录布局和软件环境中如何安全启动”的问题。它不替代
AutoDL、文本或图像专项 runbook，也不假设所有机器已经下载模型、数据集和可选 Python
包。

## 三层合同

| 层 | 权威文件 | 负责什么 | 不负责什么 |
|---|---|---|---|
| 机器 | `profiles/*.json` | OS、Python、CPU、GPU、显存、磁盘、命令和必要环境变量 | 最优 batch/K/actor 参数 |
| 软件能力 | `assets.json -> python_groups` | 当前 Python 能否 import 某个能力；缺失时对应哪个 pip spec | 自动修改 CUDA/驱动或混装 vLLM/driver 环境 |
| 外部资产 | `assets.json -> assets` | 模型/数据集的来源、目标、最小完整性条件和许可边界 | PostgreSQL 导入、标签语义和正式实验正确性 |

`LightGBM` 只是通用 `ml-estimators` 能力组的当前成员。以后新增 XGBoost、图像解码、
质量评价或新的下载工具时，按能力组登记，不为某个实验单独写机器脚本。

## 首次迁移流程

下面以单 5070 的 Linux/WSL2 环境为例。原生 Windows 不是正式 GPU 实验平台；建议
使用 WSL2 Ubuntu，使 Ray、Daft、vLLM 和现有 shell runner 保持同一执行语义。

```bash
cd <repository>
cp deploy/runtime/runtime.env.example <persistent-root>/ai-operator-runtime.env
# 编辑这一个仓库外文件：PROJECT_ROOT/ARTIFACT_ROOT/MODEL_ROOT/DATA_ROOT/
# VENV_ROOT、DATABASE_URL、GPU_IDS、模型和 endpoint。

PYTHONPATH=code python code/scripts/environment/manage_environment.py \
  --env-file <persistent-root>/ai-operator-runtime.env \
  --assets-manifest deploy/runtime/assets.json \
  check \
  --machine-profile deploy/runtime/profiles/local_1x5070_linux.json \
  --groups core,text,image,analysis,ml-estimators,download,text-qwen15b,image-clip,workload-text,workload-image-smoke \
  --json-out <artifact-root>/preflight.json
```

首次 `check` 预期会失败并逐项列出缺口；它是只读命令，不会偷偷安装或下载。按需要显式
补齐：

```bash
# 明确指定要修改的 Python；禁止误装进 vLLM serving venv。
PYTHONPATH=code python code/scripts/environment/manage_environment.py \
  --env-file <runtime.env> --assets-manifest deploy/runtime/assets.json \
  install-python --groups core,text,image,analysis,ml-estimators,download \
  --python-executable <driver-venv>/bin/python --dry-run
# 核对命令后去掉 --dry-run。

# 每次只下载一个明确资产；已有且达到完整性门槛时直接返回 ready。
PYTHONPATH=code <driver-venv>/bin/python code/scripts/environment/manage_environment.py \
  --env-file <runtime.env> --assets-manifest deploy/runtime/assets.json \
  download --asset qwen25-15b
```

可下载资产包括 `qwen25-15b`、`qwen25-7b`、`clip-vit-b32`、
`sharegpt-vicuna`、`burstgpt-v2`、`coco-val2017` 和 `coco-train2017`。
HTTP 大文件使用 `.partial` 断点文件并在完成后原子改名；服务端忽略 Range 时会从头覆盖，
不会把两份文件错误拼接。Hugging Face snapshot 需要先安装 `download` 能力。

`imagenet-1k` 是受许可的 `manual` 资产：检查会明确报缺失，下载命令会拒绝绕过授权。
这类资产必须由用户接受条款后放入清单指定目录。

## 数据下载不等于 workload 已就绪

资产管理器只验证原始文件/模型目录。下载后还需运行现有 importer：

- 文本：`code/scripts/data/import_ai_complete_workload.py`；
- COCO：`code/scripts/data/import_coco_images.py`；
- PostgreSQL workload 行数、唯一性和 schema 由实验 runner 的 gate 再验证。

这样避免“ZIP 在磁盘上”被误判成“数据库里已有正式 workload”。正式结果必须同时保存
`preflight.json`、实验 resolved config、Git commit、服务版本和 workload manifest。

## 单 5070 与双 4090 的边界

| 项目 | 单 5070 | AutoDL 双 4090 |
|---|---|---|
| 默认文本 smoke | Qwen2.5-1.5B、1 endpoint | Qwen2.5-7B、2 endpoint |
| 图像 smoke | CLIP、1 GPU actor | CLIP、2 GPU actor |
| 可复用 | 数据/输出合同、策略代码、runner、指标 schema | 同左 |
| 必须重做 | actor/source shape、active work、batch、显存上限、服务容量 | 机器或模型变化后同样重做 |
| MFU | `GPU_PEAK_TFLOPS=0` 时不作 MFU 结论；正式前填精度对应峰值 | 继续使用经核对的 4090 精度口径 |

机器切换只保证“能运行”不能保证“仍处于最优点”。新机器先跑最小 gate，再独立校准，
禁止把双 4090 的 `K/active-work/actor` 最优点复制到单 5070 后直接作正式比较。

## 新增依赖或数据集

1. Python 包按用途加入一个通用 `python_groups`；不要加入每台机器的全量环境。
2. 公共文件使用 `http_file`，模型/HF 数据使用 `huggingface_snapshot`，受许可数据使用
   `manual`。
3. 给资产声明稳定 ID、使用它的 group、目标路径变量、来源和最小字节数；能取得官方
   SHA-256 时再加入 `sha256`。
4. 先补清单测试，再在一台空环境执行 `check -> dry-run install/download -> check`。
5. 数据库 importer、任务质量标签和实验 gate 仍放各自模块，不能塞进环境管理器。

## 不自动处理的内容

- NVIDIA 驱动、CUDA toolkit、Docker/systemd 和 PostgreSQL server 安装；
- vLLM 与 driver 环境的合并；二者必须继续隔离；
- 云密钥、数据库密码、Hugging Face token；runtime env 保存在仓库外；
- 受许可数据、任意 shell 安装命令、正式实验参数自动“优化”。

这些边界让迁移失败表现为明确缺口，而不是在不同机器上静默形成不同实验环境。
