# 调度输入测试数据

[geometric_feedback_saved_input.json.gz](geometric_feedback_saved_input.json.gz) 保存一次CPU替代实现查询的474份控制器输入。
实际Core与进程间通信参与了原查询；模型和Ray采用明确测试夹具。

原源码提交为 `9fc7873136469d7db0f38acb972f8041df871288`。
原压缩文件SHA-256为 `cdf165f98f10f470462b5dd309be623ab09956b69b3099978f2692f02fc4dc71`，
解压内容SHA-256为 `aca5400dfb0c975ecd77290a6d616fe529b88b5e8439837e9ac8491cd7c10260`。
本文件保留全部输入、原选取结果及必要报告；去掉了远程调用诊断等无关报告字段。
各份输入的年龄、时钟、计数和原决定均保持原值，来源期限取原报告的0.5秒。

[来源时钟测试](../test_resource_feedback_source_clock.py)检查未绑定接口的精确复现，以及绑定真实来源期限后的确定性决策。
修改控制器后的固定输入回放只验证决策行为；后续输入仍来自原查询，不代表修改策略后的实际查询或性能。
