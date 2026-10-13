# LOTUS图文执行接点与Sema输入核查

本轮只验证固定前端能力和同方法执行接点，没有真实模型、GPU或Ray启动。
独立研究原型为`92bcd05a`；通用LOTUS接线为`607b9f88`，只改`lotus/sdk.py`、`lotus/batch.py`并增加独立测试。
研究假设和静态缓存反例仍见[阶段设计](../../../../docs/research/heterogeneous_ai_dataflow_execution_model_20260811.md#frontend-stage-audit)，
这次接线不采用准备缓存，也没有新增调度或性能结论。

## 实际配置与结果

运行标识`semloom-stage-20261013.z3x6arlg`；既有Python3.12.3、LOTUS1.2.4、LiteLLM1.95.0、OpenAI2.50.0、pandas2.3.3、Pillow12.2.0。
使用两个CPU（24、25），没有安装依赖、下载权重或操作既有模型服务。所有响应来自拦截、MockTransport、本地回调或localhost替身。
首次将测试亲和范围用于整机预检，因2个CPU低于机器profile的10槽要求而失败，原件保留；按整机128槽重新观察通过，实际测试仍只有两个CPU。
开始/就绪wall time未单独采集；unittest运行时长保留在日志，仅作复现信息，不作查询性能值。

| 检查 | 实际结果与范围 |
|---|---|
| 新图文profile | 9/9通过，无跳过；单图文Map每臂4个逻辑调用，Filter→Map每臂4＋2个；完整JSON值、内容列表顺序、原parser/usage、行序与清理一致 |
| SDK错误 | 每臂4个429模拟调用，原生LM与SemLoom错误类型/状态一致；没有真实HTTP模型请求 |
| 旧文本回归 | 54项中50通过，4项失败或错误；在原`92bcd05`的SDK/batch及同一环境复现相同4项，未修订旧测试或生产行为 |
| 固定Sema | 二进制`v0.0.1 8c2d3bd2`、SHA-256 `15a534667a668a152524a8756fe5d66c0fa9c9822e73d26029e1a71693a56ec3`；函数目录没有image/video/vision项。两条普通VARCHAR语义投影分别给PNG路径和data URI，2次localhost调用均为字符串content，没有image_url列表 |
| 已有模型配置 | 已配置根目录的4个config只见Qwen2ForCausalLM、MistralForCausalLM和CLIPModel；CLIP是视觉embedding模型，本次没有核实可服务该图文查询的生成式VLM |

4项旧失败分别是：row-limit旧错误文字断言；不完整JSON响应期待ValueError；Unicode用量用例拿到InternalServerError而非usage；Wait用例patch已不存在的`batch.lotus_response`。
首次回归包遗漏测试helper导致额外导入错误，补齐公开helper后重新核对；初次与修后记录分开保留。
Sema只测试了公开普通语义投影的VARCHAR入口，不由函数目录或两条探针推断闭源二进制绝无其他未公开接口。
论文中的视频出现在相关工作，不是本产物的视觉能力证明：[Sema原文§8](https://arxiv.org/html/2603.11622v1#S8)。

## 接口与证据

`LotusBatchExecutor(..., message_profile="lotus-image-text-v1")`显式接受固定LOTUS生成的PNG data URI图文列表，
额外核对`utils.py`、`dtype_extensions/image.py`和`sem_filter.py`，整个图文批次在打开flow前使用原SDK检查。
默认text、原native LM、生成参数、完整请求及响应重放继续使用原路径；profile字段不进入模型JSON。
配对比较完整JSON值和有序内容，不声称不同HTTP编码器的原始wire字节相同。
image消息仍使用现有完整调用的request-count工作量，不代表视觉token/CPU decode代价已校准，也不声明模型有视觉能力。
视频/音频、任意URL、两Map继续执行中的图像状态、PG图文接入及Daft/Ray图文替换仍未验证。

[测试源码](../../../../code/tests/semantic_methods/test_lotus_multimodal.py)、[接口说明](../../../../code/src/semantic_methods/lotus/README.md#image-text-profile)、
[验证元数据](verification.json)与[脱敏日志](raw/evidence.tar.gz)可复核；压缩包含9份必要观测和`member-identities.json`的私有/公开双摘要。
私有原件按上述运行标识定位。源码上传首次被自动审批拒绝；逐blob证明369文件来自已公开`92bcd05`、其余3文件属于已授权接线、372文件隐私扫描通过后，重新提交获准。未绕过拒绝或移出其他私有材料。
后续真实视觉模型、任务质量、阶段画像与性能比较另需主任务协调能力、来源、预算及共同基线；640/15分钟只是旧建议，本轮未启动该窗口。
