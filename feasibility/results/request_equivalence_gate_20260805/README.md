# 请求等价门禁（2026-08-05）

证明 DuckDB `ai` 路径与项目 completion 路径对同一 bounded-output 请求发出**相同的 HTTP body**
（model / messages role+content / temperature=0.0 / max_tokens，无多余语义字段、无隐藏 system
prompt），使后续 bounded-output 性能对比的差异**不能归因于 prompt 或设置不同**。这是单请求门禁，
不是长实验或三臂性能排名。原始证据见 `request_equivalence.json`、命令与退出码见 `command.txt`。

## 1. 实验目的

为 DuckDB bounded-output 产品对比（SQuAD 主轨）锁定请求等价前提：两条执行链（DuckDB 扩展 vs
项目 `build_completion_request_body`）送进 vLLM 的请求体必须一致。

## 2. 实验设置

- 服务：AutoDL 2×4090，Qwen2.5-7B，vLLM（prefix cache 保持正式配置，不为门禁改动）。
- DuckDB：v1.5.4 + community `ai` 扩展，`openai_compatible` provider 指向同一 endpoint。
- 项目：`code/src/serving/backends/completion.py::build_completion_request_body`（公共生产构造函数）。
- 唯一 prompt（不在任何 workload，避免 prefix cache 命中污染 token 计数）；cap=16；temperature=0.0。
- 代码 commit：见 `request_equivalence.json::identity.git_commit`。

## 3. 合规性自检

- endpoint `vllm:num_requests_running=0` 于每条隔离单请求**前后**强制为空闲。
- 唯一 prompt；每路径各执行一次。
- 不使用繁忙服务期间的聚合 counter delta，只用隔离单请求前后的 prompt-token delta。

## 4. 实验设计

三方比对 + 隔离单请求交叉校验：

- canonical 合同 `{model, messages:[{role:user,content}], temperature:0.0, max_tokens}`。
- DuckDB 实际 = `ai_completion_request_json(prompt, max_tokens=>N, temperature=>0.0)`（不实际请求）。
- 项目实际 = `build_completion_request_body(model,[prompt],N,"chat_completions",temperature=0.0)`。
- 逐字段 diff、messages role+content、多余/缺失字段、默认 temperature probe。
- 隔离单请求：DuckDB `ai_try_complete` 与项目 `call_compatible_completion_endpoint` 各一次，
  比较 vLLM `prompt_tokens_total` delta。

## 5. 实验数据

- `passed: True`，无失败原因。
- DuckDB payload == canonical：True（无多余/缺失字段）。
- project payload == canonical：True（无多余/缺失字段）。
- 默认 temperature probe：**0.1**（DuckDB 默认；adapter 显式 `temperature=>0.0` 才与项目一致）。
- 隔离单请求 prompt-token delta：**DuckDB 37 / project 37，match: True**。

## 6. 结果解释

两条路径发出**相同的 canonical 请求**；messages 仅 `{role:user, content}`，**无隐藏 system prompt**
（隔离单请求 prompt-token 37=37 进一步佐证）。DuckDB 默认 temperature=0.1 是已知 gotcha，门禁要求
显式 0.0。结论：bounded-output 性能对比的请求等价前提成立。

## 7. 下一步

#5 通过 → 进入专用 SQuAD importer（#3/#7）：`reference_answers` 多答案（JSONB/text[]，EM/F1 取
max）、锁 SQuAD 版本/split/data hash/prompt 模板/固定 cap；不复用通用 bounded importer。
