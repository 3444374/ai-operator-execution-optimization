# 开题答辩问答库

## 题目是否太大？

答法：

> 这个题目表面上涉及数据库、Ray、模型服务和写回，但我会把研究对象收敛到数据库 AI 算子触发后的外部执行链路。具体实验会先固定 PostgreSQL、Arrow batch、Ray task/actor、HTTP 模型服务和 writeback 这条链路，再逐步做消融，不会同时改造数据库内核和模型 kernel。

## 为什么不是只做 Ray？

答法：

> Ray 是可控执行机制，不是唯一研究对象。现有实验显示单 endpoint 下 Ray 不一定明显优于 Python，只有多 endpoint、路由、反压和 worker 写回等场景下才可能体现价值。因此本课题更准确地说是模型服务感知的外部执行链路优化。

## 和 Snowflake / pgai / PostgresML 有什么关系？

答法：

> Snowflake 说明 AI SQL 算子是工业真实问题，但其内部链路闭源，不能作为可拆分 baseline。pgai 代表 PostgreSQL + 外部 vectorizer worker + embedding endpoint + 写回的路线，和本课题更接近。PostgresML 代表把模型能力放到数据库内或近数据库的路线。本课题选择可控外部链路，重点研究 batch、调度、模型服务调用和写回。

## 当前实验能证明什么？

答法：

> 当前实验能证明这条 GPU-backed 外部链路可以拆分计时，并且 batch、模型服务调用墙钟时间、writeback 都会显著影响端到端时间。它还不能证明最终方法已经优于所有系统，也不能证明 Ray 在所有场景都更好。

## 如果 Ray 效果不好怎么办？

答法：

> 这本身也是有价值的消融结果。课题不会只押注 Ray。后续会比较 Python worker、Ray task、Ray actor、worker 写回和 vectorizer-like 队列写回。如果 Ray 只在多 endpoint 或反压场景有效，研究内容会收敛到这些条件下的调度与资源配比。

## 为什么需要写回优化？

答法：

> 初步实验中，16384 行 Ray actor/coalesced 下 writeback 和 AI operator 阶段已经接近同一量级。如果只优化模型调用或调度，端到端收益会被写回阶段限制。因此后续必须比较 PostgreSQL JSON、pgvector(384)、worker 写回和 Lance / Parquet 这类外部 sink。

