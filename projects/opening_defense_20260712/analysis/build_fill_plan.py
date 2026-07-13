import json
from pathlib import Path

PROJECT = Path("projects/opening_defense_20260712")
LIB = json.loads((PROJECT / "analysis" / "template.slide_library.json").read_text(encoding="utf-8"))

def slots_for(source_slide):
    for slide in LIB["slides"]:
        if slide["slide_index"] == source_slide:
            return [slot["slot_id"] for slot in slide.get("slots", [])]
    raise KeyError(source_slide)

def replacements(source_slide, values):
    values = dict(values)
    return [{"slot_id": slot_id, "text": values.get(slot_id, "")} for slot_id in slots_for(source_slide)]

def table(table_id, rows):
    cells = []
    for r, row in enumerate(rows):
        for c, text in enumerate(row):
            cells.append({"row": r, "col": c, "text": text})
    return {"table_id": table_id, "cells": cells}

def note(talk, defense):
    return f"汇报讲稿：{talk}\n\n答辩备注：{defense}"

slides = [
    {
        "source_slide": 1,
        "purpose": "cover",
        "layout_rationale": {
            "layout_pattern": "school cover",
            "why_fit": "The template cover already has a large title slot, metadata slot and footer.",
            "risk": "Long title must be split into title and subtitle.",
        },
        "notes": note(
            "本次开题关注数据库 AI 算子触发后的批处理执行过程，重点不是单个模型 kernel，也不是改造完整 Ray，而是数据库数据进入外部 worker、模型服务和写回阶段后的端到端系统问题。",
            "如果被问题目是否过大，强调固定在 PostgreSQL、Arrow batch、Python/Ray task/actor、GPU-backed endpoint 和 writeback 这条可控链路上。",
        ),
        "transition": "fade",
        "replacements": replacements(1, {
            "s01_sh2": "硕士论文开题汇报\n汇报人：艾筠舜    导师：待补充    时间：2026.7",
            "s01_sh3": "面向数据库 AI 算子的模型服务感知批处理执行与写回协同优化研究",
            "s01_sh4": "开题汇报 | Database AI Operators / GPU-backed Model Service / Ray / Writeback | 1",
        }),
    },
    {
        "source_slide": 2,
        "purpose": "agenda",
        "layout_rationale": {
            "layout_pattern": "numbered agenda",
            "why_fit": "The existing six-item agenda layout matches the defense narrative.",
            "risk": "Agenda labels must stay short.",
        },
        "notes": note(
            "汇报按问题驱动展开：先说明数据库 AI 算子为什么带来新的执行问题，再说明研究边界、研究内容、已有实验依据和后续计划。",
            "目录页不要讲过细，重点让老师看到不是工程任务清单，而是问题、方法、评价和边界的闭环。",
        ),
        "transition": "fade",
        "replacements": replacements(2, {
            "s02_sh2": "汇报结构",
            "s02_sh3": "开题汇报 | 数据库 AI 算子执行链路优化 | 2",
            "s02_sh5": "1", "s02_sh6": "研究背景", "s02_sh7": "数据库 AI 算子为什么带来新的执行链路",
            "s02_sh9": "2", "s02_sh10": "问题定义", "s02_sh11": "研究对象、边界与关键难点",
            "s02_sh13": "3", "s02_sh14": "研究内容", "s02_sh15": "调度、写回、策略边界三个闭环",
            "s02_sh17": "4", "s02_sh18": "实验依据", "s02_sh19": "GPU-backed AI_EMBED 画像与动机测试",
            "s02_sh21": "5", "s02_sh22": "技术路线", "s02_sh23": "后续实验、评价指标和消融设计",
            "s02_sh25": "6", "s02_sh26": "计划", "s02_sh27": "进度安排、边界控制和预期成果",
        }),
    },
    {
        "source_slide": 3,
        "purpose": "problem_chain",
        "layout_rationale": {
            "layout_pattern": "left-to-right process with diagnostic table",
            "why_fit": "The template process page can express the database-to-model-service path.",
            "risk": "Table content must be compact to avoid overflow.",
        },
        "notes": note(
            "这一页先把研究对象收紧成一条可测链路：PostgreSQL 读数据，构造 Arrow 和 batch，交给 Python 或 Ray 执行，调用 GPU-backed 模型服务，最后汇聚并写回。",
            "不要说成研究外部链路这个口语词，正式说法是数据库 AI 算子的批处理执行、模型服务调用和写回协同。",
        ),
        "transition": "fade",
        "replacements": replacements(3, {
            "s03_sh2": "问题定义：AI 算子把 SQL 执行扩展到模型服务与写回",
            "s03_sh3": "开题汇报 | 数据库 AI 算子执行链路优化 | 3",
            "s03_sh4": "PostgreSQL\n表数据读取",
            "s03_sh5": ">",
            "s03_sh6": "Arrow / batch\n外部 worker",
            "s03_sh7": ">",
            "s03_sh8": "GPU-backed\nmodel service",
            "s03_sh10": "关键：batch、endpoint queue、fan-in 与 writeback 共同决定 E2E",
            "s03_sh9": "阶段边界 | 主要变量 | 需要回答的问题\nDB fetch / Arrow | rows、batch、RecordBatch | 数据进入外部执行前成本多大\nTask / Actor | task 数、object 数、并行度 | Ray 在什么条件下有效\nModel service | endpoint、queue、in-flight | 如何避免请求粒度和队列放大\nWriteback | fan-in、sink、批量写入 | 写回是否吞掉上游收益",
        }),
        "table_edits": [table("s03_tbl9", [
            ["阶段", "变量", "关注点"],
            ["DB / Arrow", "rows, batch", "数据进入外部执行前的成本"],
            ["Task / Actor", "tasks, objects", "并行调度何时有效"],
            ["Model service", "endpoint, queue", "请求粒度与队列压力"],
            ["Writeback", "fan-in, sink", "写回是否限制收益"],
        ])],
    },
    {
        "source_slide": 5,
        "purpose": "related_systems",
        "layout_rationale": {
            "layout_pattern": "four-column literature/system table",
            "why_fit": "The table page fits a concise system landscape.",
            "risk": "Avoid overclaiming closed-source systems.",
        },
        "notes": note(
            "现有系统提供了场景依据和机制依据，但很多托管 AI SQL 系统内部不可见，不能直接拿来做阶段画像。PostgreSQL 生态和 Ray/Daft/Lance 说明这条路线有落地对象，但本课题不等于做某个产品集成。",
            "如果老师问相关工作是否充分，后续还会补 CCF-A 系统论文和官方文档引用；当前 PPT 先讲研究空缺。",
        ),
        "transition": "fade",
        "replacements": replacements(5, {
            "s05_sh2": "相关系统提供场景，但缺少可拆分的端到端阶段画像",
            "s05_sh3": "开题汇报 | 数据库 AI 算子执行链路优化 | 4",
            "s05_sh5": "研究空缺在于：托管 AI SQL 系统证明场景真实，但阶段成本通常不可见。",
            "s05_sh4": "类别 | 代表系统 | 已提供能力 | 本课题仍需回答\nAI SQL | Snowflake / BigQuery / Oracle | SQL 内调用 AI 函数 | 内部阶段成本不可见\nPostgreSQL AI | pgvector / pgai / PostgresML | 向量存储、worker、近数据库模型 | 缺少统一阶段画像\n执行框架 | Ray / Daft | task、actor、partition、batch | 何时适合 AI 算子链路\nAI 数据存储 | Arrow / Lance | 列式与向量数据表示 | 写回路径如何影响端到端",
        }),
        "table_edits": [table("s05_tbl4", [
            ["类别", "代表系统", "已提供能力", "本课题关注"],
            ["AI SQL", "Snowflake / BigQuery / Oracle", "SQL 内调用 AI 函数", "内部阶段成本通常不可见"],
            ["PostgreSQL AI", "pgvector / pgai / PostgresML", "向量存储、worker、近数据库模型", "需要端到端阶段画像"],
            ["执行框架", "Ray / Daft", "task、actor、partition、batch", "验证调度适用条件"],
            ["AI 数据存储", "Arrow / Lance", "列式与向量数据表示", "分析写回与持久化代价"],
            ["研究空缺", "可控实验链路", "DB + worker + model + sink", "统一评价调度与写回"],
        ])],
    },
    {
        "source_slide": 12,
        "purpose": "framework",
        "layout_rationale": {
            "layout_pattern": "central concept page",
            "why_fit": "The sparse page can carry the overall research framework without crowding.",
            "risk": "Detailed mechanics must be kept in notes.",
        },
        "notes": note(
            "总体框架是三类数据库 AI 算子共享同一条可观测执行过程，再围绕模型服务感知调度、写回协同和多类算子策略边界展开。这样研究内容不是任务清单，而是围绕可检验问题组织。",
            "图中如果现场需要解释，强调 AI_EMBED 只是第一条真实闭环，后续会扩展到 AI predicate 和 AI_COMPLETE。",
        ),
        "transition": "fade",
        "replacements": replacements(12, {
            "s12_sh2": "总体框架：三类 AI 算子共享一套可观测执行过程",
            "s12_sh3": "开题汇报 | 数据库 AI 算子执行链路优化 | 5",
            "s12_sh20": "三类 AI 算子 -> PostgreSQL fetch -> Arrow/batch -> task/actor -> GPU-backed model service -> fan-in -> writeback",
        }),
    },
    {
        "source_slide": 14,
        "purpose": "research_contents",
        "layout_rationale": {
            "layout_pattern": "module table",
            "why_fit": "The table can express problem, method and evaluation for each research content.",
            "risk": "Needs closed-loop wording instead of engineering tasks.",
        },
        "notes": note(
            "研究内容分成三个可合并的大方向：模型服务感知调度、写回协同、多类算子策略边界。每个方向都回答为什么难、怎么做、怎么评价。",
            "如果被问为什么没有把阶段画像作为研究内容，回答阶段画像是动机测试和评价基础，不是最终研究贡献本身。",
        ),
        "transition": "fade",
        "replacements": replacements(14, {
            "s14_sh2": "研究内容：围绕难点、方法和评价形成三个闭环",
            "s14_sh3": "开题汇报 | 数据库 AI 算子执行链路优化 | 6",
            "s14_sh4": "研究内容 | 技术难点 | 拟采用方法 | 评价方式\n批处理执行调度 | batch、task/actor、endpoint、in-flight 互相影响 | 模型服务状态与算子特征共同决定 batch / routing / backpressure | Python/Ray、single/multi endpoint、bounded/unbounded 消融\n写回协同 | 写回可能吞掉上游调度收益 | 比较 driver fan-in、worker-side、queue worker 与不同 sink | writeback_s、e2e_s、一致性、失败重试\n策略边界 | 单一 embedding 经验不能外推 | 在 AI_EMBED、AI_FILTER、AI_COMPLETE 中总结策略选择规则 | rows/s、tokens/s、queue wait、GPU utilization、边界说明",
        }),
        "table_edits": [table("s14_tbl4", [
            ["研究内容", "为什么难", "方法", "评价"],
            ["批处理执行调度", "batch、endpoint、in-flight 相互影响", "联合选择 batch / routing / backpressure", "Python/Ray 与 endpoint 消融"],
            ["写回协同", "writeback 可能限制端到端收益", "比较 driver / worker / queue worker", "writeback_s、e2e_s、一致性"],
            ["策略边界", "embedding 经验不能直接外推", "覆盖三类 AI 算子", "rows/s、tokens/s、queue wait"],
            ["共同基础", "阶段成本需要可测", "统一阶段画像", "消融与对照实验"],
            ["边界控制", "避免题目发散", "不做 GPU kernel / 完整 Ray 改造", "明确不能声称的结论"],
            ["输出", "形成可复现实验链路", "脚本、CSV、图表、论文方法", "结果可追溯"],
        ])],
    },
    {
        "source_slide": 8,
        "purpose": "chapter_experiments",
        "layout_rationale": {
            "layout_pattern": "chapter separator",
            "why_fit": "A section break helps distinguish proposal logic from evidence.",
            "risk": "Keep one sentence only.",
        },
        "notes": note(
            "下面进入实验依据。这里要主动说明实验不是证明课题已经完成，而是证明问题真实、链路可测、后续优化点可展开。",
            "强调真实 GPU-backed 证据优先于 fake/CPU 预研，fake/CPU 只解释变量来源。",
        ),
        "transition": "fade",
        "replacements": replacements(8, {
            "s08_sh4": "开题汇报 | 数据库 AI 算子执行链路优化 | 7",
            "s08_sh5": "实验依据：batch、routing 与 writeback",
        }),
    },
    {
        "source_slide": 15,
        "purpose": "experiment_setup",
        "layout_rationale": {
            "layout_pattern": "experiment path table",
            "why_fit": "The page can summarize setup, stages and boundaries.",
            "risk": "Must clearly separate PG18.4 from PG18.3.",
        },
        "notes": note(
            "当前最重要的是已经有一条真实 GPU-backed AI_EMBED 闭环：本地 PostgreSQL 18.4、Arrow/batch、Python/Ray、CUDA endpoint、fan-in、writeback。它用于开题阶段动机和方法验证，不代表 PostgreSQL 18.3 内部平台最终性能。",
            "答辩时要主动说明当前写回是 JSON text，后续会补 384 维 pgvector 写回。",
        ),
        "transition": "fade",
        "replacements": replacements(15, {
            "s15_sh2": "实验设置：真实 GPU-backed AI_EMBED 端到端画像",
            "s15_sh4": "开题汇报 | 数据库 AI 算子执行链路优化 | 8",
            "s15_sh6": "实验链路覆盖数据库读取、batch 构造、外部执行、CUDA 模型服务、fan-in 和 PostgreSQL 写回。",
            "s15_sh5": "环节 | 当前设置 | 记录指标\n数据库 | PostgreSQL 18.4 local rehearsal | db_fetch_s, writeback_s\n中间表示 | Arrow / batch | arrow_build_s, calls\n执行器 | Python / Ray task / Ray actor | operator_wall_s, fanin_s\n模型服务 | all-MiniLM-L6-v2 on CUDA | model_request_wall_s, bounded_wait_s\n边界 | JSON text writeback | 尚不是 pgvector(384)",
        }),
        "table_edits": [table("s15_tbl5", [
            ["环节", "当前设置", "记录指标"],
            ["数据库", "PostgreSQL 18.4 local rehearsal", "db_fetch_s / writeback_s"],
            ["中间表示", "Arrow / batch", "arrow_build_s / calls"],
            ["执行器", "Python / Ray task / actor", "operator_wall_s / fanin_s"],
            ["模型服务", "CUDA embedding endpoint", "model_request_wall_s / bounded_wait_s"],
        ])],
    },
    {
        "source_slide": 6,
        "purpose": "batch_result",
        "layout_rationale": {
            "layout_pattern": "hypothesis and result table",
            "why_fit": "The table can show fine vs coalesced as a compact evidence page.",
            "risk": "Numbers must stay traceable to CSV.",
        },
        "notes": note(
            "1024 行时，fine 逐行调用发起 1024 次 endpoint 请求，coalesced 只有 4 次请求，端到端耗时从 0.888 秒变成 11.925 秒。这个结果说明调用粒度本身就是系统成本。",
            "不能解释成 GPU 更快或 kernel 优化；收益来自调用粒度和外部 operator 阶段，而不是模型内部优化。",
        ),
        "transition": "fade",
        "replacements": replacements(6, {
            "s06_sh2": "结果 1：逐行 endpoint 调用比 batch 调用慢约 13.4x",
            "s06_sh4": "开题汇报 | 数据库 AI 算子执行链路优化 | 9",
            "s06_sh7": "1024 行真实 GPU-backed AI_EMBED 中，调用粒度是必须控制的一阶成本。",
            "s06_sh6": "Rows | Strategy | Calls | e2e_s | operator_wall_s\n1024 | coalesced | 4 | 0.888 | 0.505\n1024 | fine | 1024 | 11.925 | 11.528\n结论 | fine / coalesced | - | 13.4x | operator 阶段主导\n边界 | 当前 endpoint | - | all-MiniLM-L6-v2 | 非 GPU kernel 结论",
        }),
        "table_edits": [table("s06_tbl6", [
            ["Rows", "Strategy", "Calls", "e2e_s"],
            ["1024", "coalesced", "4", "0.888"],
            ["1024", "fine", "1024", "11.925"],
            ["ratio", "fine / coalesced", "-", "13.4x"],
            ["边界", "当前 AI_EMBED endpoint", "-", "非 kernel 结论"],
        ])],
    },
    {
        "source_slide": 7,
        "purpose": "ray_boundary",
        "layout_rationale": {
            "layout_pattern": "comparison table",
            "why_fit": "The page can contrast single and dual endpoint evidence.",
            "risk": "Avoid saying Ray is generally faster.",
        },
        "notes": note(
            "单 endpoint 下 Python、Ray task 和 Ray actor 接近，所以不能把 Ray 作为天然加速器。双 endpoint 下 Ray 通过并发 routing 降低 operator wall time，但端到端收益仍受 writeback 限制。",
            "如果被问为什么还用 Ray，回答 Ray 是后续多 endpoint、bounded in-flight、actor pool 和 worker writeback 的可控实验 substrate。",
        ),
        "transition": "fade",
        "replacements": replacements(7, {
            "s07_sh2": "结果 2：Ray 的价值边界来自多 endpoint routing",
            "s07_sh4": "开题汇报 | 数据库 AI 算子执行链路优化 | 10",
            "s07_sh6": "Ray 是调度 substrate 和对照对象，不能被写成论文主问题本身。",
            "s07_sh5": "场景 | e2e_s | 含义\n4096 单 endpoint | 3.29-3.36 | Python / Ray 接近\n4096 双 endpoint Python | 3.444 | 顺序 routing\n4096 双 endpoint Ray task | 2.761 | operator 降低\n4096 双 endpoint Ray actor | 2.780 | 与 task 接近",
        }),
        "table_edits": [table("s07_tbl5", [
            ["场景", "e2e_s", "含义"],
            ["4096 单 endpoint", "3.29-3.36", "Python / Ray 接近"],
            ["4096 双 endpoint Python", "3.444", "顺序 routing"],
            ["4096 双 endpoint Ray task", "2.761", "operator 降低"],
            ["4096 双 endpoint Ray actor", "2.780", "与 task 接近"],
        ])],
    },
    {
        "source_slide": 19,
        "purpose": "writeback_result",
        "layout_rationale": {
            "layout_pattern": "result explanation table",
            "why_fit": "The page can explain writeback as the next bottleneck.",
            "risk": "Current writeback boundary must be visible.",
        },
        "notes": note(
            "16K 行时 operator wall time 是 6.473 秒，writeback 是 6.586 秒，两个阶段都接近端到端的一半。双 endpoint 后 operator 降低到 4.628 秒，但 writeback 仍有 6.363 秒。",
            "当前写回是 JSON text，不代表 pgvector(384)。因此它是写回问题的动机，不是最终写回性能结论。",
        ),
        "transition": "fade",
        "replacements": replacements(19, {
            "s19_sh2": "结果 3：writeback 已经足以限制端到端收益",
            "s19_sh4": "开题汇报 | 数据库 AI 算子执行链路优化 | 11",
            "s19_sh6": "只优化模型服务调用不够，写回路径必须和调度一起评价。",
            "s19_sh5": "Rows | Endpoints | operator_wall_s | writeback_s\n16384 | 1 | 6.473 | 6.586\n16384 | 2 | 4.628 | 6.363\n后续 | pgvector(384) | 待补 | 待补\n后续 | worker / queue writeback | 待补 | 待补",
        }),
        "table_edits": [table("s19_tbl5", [
            ["Rows", "Endpoints", "operator", "writeback"],
            ["16384", "1", "6.473", "6.586"],
            ["16384", "2", "4.628", "6.363"],
            ["后续", "pgvector(384)", "待补", "待补"],
            ["后续", "worker / queue writeback", "待补", "待补"],
            ["边界", "当前 JSON text", "非 pgvector 结论", "需复测"],
        ])],
    },
    {
        "source_slide": 21,
        "purpose": "feasibility_summary",
        "layout_rationale": {
            "layout_pattern": "six-card summary",
            "why_fit": "This page can separate real evidence from feasibility checks.",
            "risk": "Labels must not imply all results are final.",
        },
        "notes": note(
            "这一页把证据等级讲清楚：真实 GPU-backed 画像是当前主证据；PG18.4 连接验证只说明环境可用；fake/CPU 预研只说明变量设计依据。",
            "这是回应前面担心 opening 与项目割裂的问题：开题材料必须回到 motivation/results 和项目总纲，而不是让 feasibility 目录承担大纲职责。",
        ),
        "transition": "fade",
        "replacements": replacements(21, {
            "s21_sh2": "可行性分析：证据分层与边界",
            "s21_sh3": "开题汇报 | 数据库 AI 算子执行链路优化 | 12",
            "s21_sh5": "真实链路",
            "s21_sh6": "GPU-backed AI_EMBED 已跑通",
            "s21_sh8": "核心发现",
            "s21_sh9": "batch / routing / writeback 均影响 E2E",
            "s21_sh11": "历史预研",
            "s21_sh12": "fake/CPU 用于变量设计",
            "s21_sh14": "环境验证",
            "s21_sh15": "PG18.4 + pgvector 可连接",
            "s21_sh17": "明确边界",
            "s21_sh18": "非 PG18.3 / 非 pgvector(384)",
            "s21_sh20": "下一步",
            "s21_sh21": "补写回、反压、多 workload",
        }),
    },
    {
        "source_slide": 22,
        "purpose": "evaluation",
        "layout_rationale": {
            "layout_pattern": "metrics table",
            "why_fit": "The page maps research contents to evaluation metrics.",
            "risk": "Keep metrics aligned with experiments.",
        },
        "notes": note(
            "评价不只看端到端耗时，还要看阶段耗时、queue wait、bounded wait、writeback、tokens/s、GPU utilization 和消融。这样后续可以判断收益来自哪里，而不是只给一个总时间。",
            "如果被问实验是否够学术，强调有对照组、消融、指标和适用边界。",
        ),
        "transition": "fade",
        "replacements": replacements(22, {
            "s22_sh2": "评价设计：用阶段指标和消融证明优化是否有效",
            "s22_sh3": "开题汇报 | 数据库 AI 算子执行链路优化 | 13",
            "s22_sh5": "端到端指标：e2e_s / rows/s / tokens/s",
            "s22_sh6": "阶段指标：operator / queue / fan-in / writeback",
            "s22_sh7": "消融对象：batch / routing / in-flight / writeback",
            "s22_sh4": "问题 | 主要指标 | 证明目标\n批处理调度 | e2e_s、operator、queue wait | 调度何时有效\n写回协同 | writeback_s、e2e_s、一致性 | 写回是否限制收益\n策略边界 | rows/s、tokens/s、GPU utilization | 策略是否可迁移\n消融 | 阶段占比变化 | 定位收益来源",
        }),
        "table_edits": [table("s22_tbl4", [
            ["研究问题", "主要指标", "证明目标"],
            ["批处理调度", "e2e_s / operator / queue", "调度何时有效"],
            ["写回协同", "writeback_s / 一致性", "写回是否限制收益"],
            ["策略边界", "rows/s / tokens/s / GPU", "策略是否可迁移"],
            ["消融", "阶段占比变化", "定位收益来源"],
            ["边界", "结论适用范围", "避免过度声称"],
        ])],
    },
    {
        "source_slide": 23,
        "purpose": "timeline",
        "layout_rationale": {
            "layout_pattern": "timeline",
            "why_fit": "The source page already uses phased labels.",
            "risk": "Dates must stay concrete and realistic.",
        },
        "notes": note(
            "后续计划按先闭环、再消融、再扩展 workload 的顺序推进。每个阶段都有可以被证伪的实验目标，避免只朝一个预设结论跑。",
            "如果学校或导师节点不同，时间可以调整；当前重点是顺序和依赖关系。",
        ),
        "transition": "fade",
        "replacements": replacements(23, {
            "s23_sh2": "进度安排：先闭环消融，再扩展 workload",
            "s23_sh3": "开题汇报 | 数据库 AI 算子执行链路优化 | 14",
            "s23_sh5": "2026.07",
            "s23_sh6": "开题材料、GPU-backed 动机结果、pgvector(384) 设计",
            "s23_sh7": "2026.08",
            "s23_sh8": "AI_EMBED 大块消融、worker writeback、bounded in-flight",
            "s23_sh9": "2026.09",
            "s23_sh10": "AI_FILTER / AI_CLASSIFY selectivity-aware 实验",
            "s23_sh11": "10月",
            "s23_sh12": "AI_COMPLETE token / prefix / queue-aware 实验",
            "s23_sh13": "2026.11",
            "s23_sh14": "统一方法、baseline、消融与反证实验",
            "s23_sh15": "2026.12+",
            "s23_sh16": "论文实验、图表、正文和答辩材料",
        }),
        "table_edits": [table("s23_tbl4", [
            ["时间", "重点工作", "输出"],
            ["2026.07-08", "AI_EMBED 闭环与消融", "pgvector / writeback / in-flight 结果"],
            ["2026.09-10", "扩展 predicate 与 LLM workload", "selectivity / token / queue 结果"],
            ["2026.11-12+", "统一方法与论文整理", "baseline、消融、图表、正文"],
        ])],
    },
    {
        "source_slide": 24,
        "purpose": "innovations",
        "layout_rationale": {
            "layout_pattern": "innovation cards",
            "why_fit": "The card page can summarize expected contributions.",
            "risk": "Use expected contributions, not completed claims.",
        },
        "notes": note(
            "预期创新点和三个研究内容一一对应：调度方法、写回协同、策略边界。强调都是预期创新点，还需要后续实验验证。",
            "不要说已经证明了系统最终性能，只说已经有初步阶段画像支撑后续研究。",
        ),
        "transition": "fade",
        "replacements": replacements(24, {
            "s24_sh2": "预期创新点：调度、写回与策略边界",
            "s24_sh3": "开题汇报 | 数据库 AI 算子执行链路优化 | 15",
            "s24_sh5": "批处理执行调度",
            "s24_sh6": "结合算子特征、endpoint 状态和 in-flight 控制选择执行策略。",
            "s24_sh8": "写回协同优化",
            "s24_sh9": "比较 driver、worker、queue worker 与不同 sink。",
            "s24_sh11": "多类算子策略边界",
            "s24_sh12": "比较三类 AI 算子的适用条件与失效边界。",
            "s24_sh14": "可观测实验链路",
            "s24_sh15": "保留脚本、CSV、阶段指标和消融结果，保证结论可追溯。",
        }),
    },
    {
        "source_slide": 25,
        "purpose": "references",
        "layout_rationale": {
            "layout_pattern": "reference ending",
            "why_fit": "The template ending page is suitable for concise references.",
            "risk": "Do not overload the reference page.",
        },
        "notes": note(
            "最后总结三点：场景真实、问题明确、初步证据成立；后续围绕模型服务感知调度和写回协同继续补实验。",
            "答辩时优先回到阶段画像和结论边界。参考文献页只列最关键来源，完整列表在开题报告和 reading list 里维护。",
        ),
        "transition": "fade",
        "replacements": replacements(25, {
            "s25_sh2": "主要参考资料与本地实验报告",
            "s25_sh3": "开题汇报 | 数据库 AI 算子执行链路优化 | 16",
            "s25_sh4": "Ray: A Distributed Framework for Emerging AI Applications, OSDI 2018\nSpark: Cluster Computing with Working Sets, HotCloud 2010\nSnowflake Cortex AISQL / Ray Core Objects / Ray Serve Dynamic Request Batching / Daft Distributed Execution / pgvector / pgai documentation\n本项目实验报告：GPU-Backed AI_EMBED Chain Breakdown, 2026-07-12\n本项目实验报告：Multi-Endpoint Ray Motivation Test, 2026-07-12",
        }),
    },
]

plan = {
    "schema": "template_fill_pptx_plan.v1",
    "status": "confirmed",
    "source_pptx": "opening/templates/opening_ppt_template_version_v6_long_notes_source_checked.pptx",
    "accepted_warnings": [],
    "slides": slides,
}

(PROJECT / "analysis" / "fill_plan.json").write_text(
    json.dumps(plan, ensure_ascii=False, indent=2),
    encoding="utf-8",
)
print(PROJECT / "analysis" / "fill_plan.json")
