# 多机器运行时、资产与校准管理

本目录解决“同一仓库长期轮流在不同 GPU、目录布局和软件环境中如何安全启动”的问题。它不替代
AutoDL、文本或图像专项 runbook，也不假设所有机器已经下载模型、数据集和可选 Python
包。

## 四层合同

| 层 | 权威文件 | 负责什么 | 不负责什么 |
|---|---|---|---|
| 机器 | `profiles/*.json` | OS、Python、CPU、GPU、显存、磁盘、命令和必要环境变量 | 最优 batch/K/actor 参数 |
| 软件能力 | `assets.json -> python_groups` | 当前 Python 能否 import 某个能力；缺失时对应哪个 pip spec | 自动修改 CUDA/驱动或混装 vLLM/driver 环境 |
| 外部资产 | `assets.json -> assets` | 模型/数据集的来源、目标、最小完整性条件和许可边界 | PostgreSQL 导入、标签语义和正式实验正确性 |
| 性能校准 | 各轨道 calibration matrix + selection contract | 按运行签名选择最小饱和点并冻结证据 | 从 GPU 名称猜最优值、formal 在线调参 |

`LightGBM` 只是通用 `ml-estimators` 能力组的当前成员。以后新增 XGBoost、图像解码、
质量评价或新的下载工具时，按能力组登记，不为某个实验单独写机器脚本。

## 日常多机器工作方式

不是把一台机器的环境“搬到”另一台，而是让每台机器各自保存一个仓库外
`runtime.env`，共同使用 Git 中的代码、资产合同和实验模板：

```text
同一个 Git commit / 实验模板
  ├─ 机器 A 的 ~/.config/ai-operator/runtime.env → 本机路径、endpoint、数据库
  └─ 机器 B 的 ~/.config/ai-operator/runtime.env → 本机路径、endpoint、数据库
                ↓
       自动硬件识别 + profile 选择
                ↓
       本机 preflight + 本机校准合同
                ↓
       冻结 resolved config 后运行实验
```

推荐每台机器把 `AI_OPERATOR_ENV_FILE` 设为自己的持久文件。此后相同命令会自动观察
CPU/GPU 并选择 profile，无需手工区分 5070/双 4090：

```bash
export AI_OPERATOR_ENV_FILE=<persistent-root>/ai-operator-runtime.env
PYTHONPATH=code python code/scripts/environment/manage_environment.py check \
  --groups core,text,image,analysis \
  --json-out <artifact-root>/preflight.json
```

已知硬件优先匹配专用 profile；其他 Linux NVIDIA GPU 落到最低能力更保守的
`generic_linux_nvidia`。报告记录 `profile_selection=automatic`、匿名稳定
`machine_id`、CPU slots、GPU 型号/显存和 driver。只有诊断时才传
`--machine-profile` 强制覆盖，不能用覆盖掩盖硬件不满足合同。

环境文件按以下顺序查找：显式 `--env-file` → `AI_OPERATOR_ENV_FILE` →
`~/.config/ai-operator/runtime.env`。首次在一台机器运行时仍需从模板创建一次：

下面以单 5070 的 Linux/WSL2 环境为例。原生 Windows 不是正式 GPU 实验平台；建议
使用 WSL2 Ubuntu，使 Ray、Daft、vLLM 和现有 shell runner 保持同一执行语义。

```bash
cd <repository>
cp deploy/runtime/runtime.env.example <persistent-root>/ai-operator-runtime.env
# 编辑这一个仓库外文件：PROJECT_ROOT/ARTIFACT_ROOT/MODEL_ROOT/DATA_ROOT/
# VENV_ROOT、DATABASE_URL、GPU_IDS、模型和 endpoint。

PYTHONPATH=code python code/scripts/environment/manage_environment.py check \
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

## 参数如何随机器和 workload 自适应

GPU 型号检测不能可靠推导最优 batch/K/actor/active-work。项目采用“自动识别 + 短校准
+ 冻结复用”，而不是让正式实验在线盲调：

1. 构造运行签名：`machine_id + 实际使用的 GPU IDs/数量 + GPU/driver + model
   ID/revision/dtype + serving 参数 + protocol + workload 长度/分辨率分布 + 规模/持续时间
   regime + 软件版本`。
2. 先做 workload scale ramp，逐步增加行数，直到单 run 至少 60 秒且相邻规模的速率/单位
   成本进入约 3% 平台；小数据下的冷启动主导点不能用于选 batch。随后固定这个稳态规模，
   再扫描 batch/K/actor，禁止同时增加行数和 batch 后把差异归给其中一个。
3. 签名没有可用校准时，先过 correctness gate，再运行该轨道已有的 calibration matrix。
   文本扫描 active-work、token budget、actor shape；图像按被测系统独立扫描 batch、
   actor/source shape 和 active work。
4. 用正式 repeats 的中位数选择“达到已测峰值约 97% 且满足 SLO/正确性门禁的最小
   饱和点”，不选择偶然最高的单次结果。若最高档仍持续上升，记录
   `saturation_not_reached` 并扩展扫描，不能假装已找到平台。
5. 将选择写成带证据 SHA 的 calibration contract；held-out/formal 只读取冻结值，禁止
   边看结果边改。
6. 完整签名相同可以复用；换 GPU、模型/revision、dtype、endpoint 拓扑、vLLM 容量、
   协议或显著改变输入/输出长度及图像分辨率时自动视为失效。只增加同分布行数通常不
   改单请求最优点，但必须确认 run 已达到至少 60 秒稳态和吞吐平台。

这意味着 K 不是“每个实验手调”，也不是一个跨机器常数。当前自动化边界是：机器/profile
检测、校准结果选择、合同 SHA/期望值校验均自动；首次遇到新签名时，仍由操作者启动一次
短 calibration matrix，并把选择器生成的 JSON/env 路径登记到该机器的 runtime env。完成后，
该签名下所有 warmup/formal 都只读同一个 `PROJECT_STATIC_K_PER_ENDPOINT` 与
`PROJECT_ACTIVE_WORK_PER_ENDPOINT`，runner 不在线搜索 K。未来可把“cache miss 后自动排队
校准”工程化，但不得让它与 formal 同时运行或边看结果边改。

现有文本选择器是 `code/scripts/analysis/select_strategy_calibration.py`；静态 workload
曲面的 GO/NO-GO 由 `summarize_static_{k,credit}_workload_surface.py` 判定。图像轨道继续
按 `experiments/plans/image_clip_workload_lock_20260731.md` 独立校准。这里不再发明第二套
选择算法。

“自适应”因此有明确边界：环境/profile 自动选择；现有选择器从可复现校准数据生成冻结
合同，并由调用者按运行签名保存/复用；正式实验不在线改变参数。目前不会在后台擅自
启动耗时 calibration，避免与另一台正在运行实验的机器争抢资源。若以后研究在线控制器，
它必须单独与该冻结静态强 baseline 比较，而不能混入环境管理。

## 单 5070 与双 4090 的边界

| 项目 | 单 5070 | AutoDL 双 4090 |
|---|---|---|
| 默认文本 smoke | Qwen2.5-1.5B、1 endpoint | Qwen2.5-7B、2 endpoint |
| 图像 smoke | CLIP、1 GPU actor | CLIP、2 GPU actor |
| 可复用 | 数据/输出合同、策略代码、runner、指标 schema | 同左 |
| 必须重做 | actor/source shape、active work、batch、显存上限、服务容量 | 机器或模型变化后同样重做 |
| MFU | `GPU_PEAK_TFLOPS=0` 时不作 MFU 结论；正式前填精度对应峰值 | 继续使用经核对的 4090 精度口径 |

机器切换只保证“能运行”不能保证“仍处于最优点”。每次运行签名变化先跑最小 gate，再独立校准，
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
- 受许可数据、任意 shell 安装命令、正式实验期间在线“优化”参数。

这些边界让迁移失败表现为明确缺口，而不是在不同机器上静默形成不同实验环境。
