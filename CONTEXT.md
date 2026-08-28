# SemLoom Domain

本文件定义项目当前使用的领域术语。它只说明概念身份，不记录实现状态、运行参数或实验结论。

## Language

**SemLoom**:
面向数据库 AI 语义算子的工作量感知执行与多作业调度系统；它是整个系统的名称，不指某个算法、
数据库、分布式框架或模型服务。
_Avoid_: project、Project、DB-AIEL（作为系统名称）

**Database-Aware AI Execution Layer (DB-AIEL)**:
数据库语义算子与外部模型执行设施之间的架构层名称；SemLoom 是这一架构层的具体系统。
_Avoid_: 把 DB-AIEL 用作 Python 类型、函数或包名前缀

**AI semantic operator**:
由数据库拥有 SQL、计划、关系语义和查询生命周期，并将已编译语义任务交给 execution provider 的
数据库算子。
_Avoid_: 把逐行 UDF、外部 profiler 或普通 HTTP 调用直接称为数据库 AI 语义算子

**SemLoom execution provider**:
在中立 provider interface 后执行 SemLoom 工作描述、数据组织、准入、路由和多作业调度的方法实现。
_Avoid_: project provider、Project provider

**legacy Project identity**:
既有实验、配置和证据中以 `project_*` 或 `Project*` 表示本项目实现的历史身份；它只用于兼容和
可追溯性，不作为新接口的规范术语。
_Avoid_: 为统一品牌而重写历史实验身份
