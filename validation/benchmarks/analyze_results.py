import argparse
import csv
from pathlib import Path


RESULT_FILES = {
    "small_task": "ray_small_task.csv",
    "object_transfer": "ray_object_transfer.csv",
    "arrow": "arrow_serialization.csv",
    "shuffle": "shuffle_simulation.csv",
    "many_objects": "ray_many_objects.csv",
    "arrow_fanout_fanin": "ray_arrow_fanout_fanin.csv",
    "fake_ai_embed": "fake_ai_embed_pipeline.csv",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze feasibility benchmark CSV results.")
    parser.add_argument("--results-dir", default="validation/results")
    parser.add_argument("--output", default="validation/results/feasibility_report.md")
    return parser.parse_args()


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def to_float(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def mean(values: list[float]) -> float:
    values = [value for value in values if value > 0]
    return sum(values) / len(values) if values else 0.0


def without_warmup(rows: list[dict]) -> list[dict]:
    measured = [row for row in rows if row.get("repeat") not in ("0", "summary")]
    return measured or rows


def analyze_small_task(rows: list[dict]) -> dict:
    data = without_warmup(rows)
    if not data:
        return {
            "status": "missing",
            "evidence": "未发现 Ray small task 结果。",
            "score": 0,
        }

    max_avg_ms = max(to_float(row.get("avg_task_ms")) for row in data)
    max_tasks = max(to_float(row.get("tasks")) for row in data)
    if max_tasks < 100:
        score = 0
        scale_note = "当前 task 数偏小，只能视为 smoke test。"
    else:
        score = 2 if max_avg_ms >= 1.0 else 1 if max_avg_ms >= 0.2 else 0
        scale_note = ""
    return {
        "status": "done",
        "evidence": f"最大 task 数 {int(max_tasks)}，warm-up 后最高平均 task latency {max_avg_ms:.3f} ms。{scale_note}",
        "score": score,
    }


def analyze_object_transfer(rows: list[dict]) -> dict:
    rows = without_warmup(rows)
    if not rows:
        return {
            "status": "missing",
            "evidence": "未发现 Ray object transfer 结果。",
            "score": 0,
        }

    small_rows = [row for row in rows if to_float(row.get("size_kb")) <= 100]
    large_rows = [row for row in rows if to_float(row.get("size_kb")) >= 1024]
    small_roundtrip_ms = mean([to_float(row.get("roundtrip_ms")) for row in small_rows])
    large_mb_s = mean([to_float(row.get("roundtrip_mb_s")) for row in large_rows])
    large_text = f"{large_mb_s:.1f} MB/s" if large_rows else "未测试"

    score = 0
    if small_roundtrip_ms >= 1.0:
        score += 1
    if large_rows and large_mb_s < 500:
        score += 1

    return {
        "status": "done",
        "evidence": (
            f"小对象平均 round-trip {small_roundtrip_ms:.3f} ms；"
            f"大对象平均吞吐 {large_text}。"
        ),
        "score": score,
    }


def analyze_arrow(rows: list[dict]) -> dict:
    rows = without_warmup(rows)
    if not rows:
        return {
            "status": "missing",
            "evidence": "未发现 Arrow serialization 结果。",
            "score": 0,
        }

    max_size_mb = max(to_float(row.get("ipc_size_mb")) for row in rows)
    serialize_ms = mean([to_float(row.get("serialize_ms")) for row in rows])
    deserialize_ms = mean([to_float(row.get("deserialize_ms")) for row in rows])
    if max_size_mb < 1.0:
        score = 0
        scale_note = "当前数据块小于 1MB，只能视为 smoke test。"
    else:
        score = 1 if serialize_ms + deserialize_ms >= 1.0 else 0
        scale_note = ""
    return {
        "status": "done",
        "evidence": (
            f"最大 IPC 大小 {max_size_mb:.2f} MB；"
            f"平均 serialize {serialize_ms:.3f} ms，deserialize {deserialize_ms:.3f} ms。{scale_note}"
        ),
        "score": score,
    }


def analyze_shuffle(rows: list[dict]) -> dict:
    rows = without_warmup(rows)
    if not rows:
        return {
            "status": "missing",
            "evidence": "未发现 shuffle simulation 结果。",
            "score": 0,
        }

    ratios = []
    groups = {}
    for row in rows:
        key = (
            row.get("upstream"),
            row.get("downstream"),
            row.get("rows_per_partition"),
            row.get("repeat"),
        )
        groups.setdefault(key, {})[row.get("strategy")] = to_float(row.get("total_s"))

    for values in groups.values():
        fine_s = values.get("fine", 0.0)
        coalesced_s = values.get("coalesced", 0.0)
        if fine_s > 0 and coalesced_s > 0:
            ratios.append(fine_s / coalesced_s)

    max_objects = max(to_float(row.get("objects")) for row in rows)
    avg_ratio = mean(ratios)
    if max_objects < 64:
        score = 0
        scale_note = "当前 object 数偏小，只能视为 smoke test。"
    else:
        score = 2 if avg_ratio >= 2.0 else 1 if avg_ratio >= 1.2 else 0
        scale_note = ""
    return {
        "status": "done",
        "evidence": f"最大 object 数 {int(max_objects)}；fine/coalesced 平均耗时比 {avg_ratio:.2f}x。{scale_note}",
        "score": score,
    }


def analyze_many_objects(rows: list[dict]) -> dict:
    rows = without_warmup(rows)
    if not rows:
        return {
            "status": "missing",
            "evidence": "未发现 Ray many objects 结果。",
            "score": 0,
        }

    by_count = {}
    for row in rows:
        object_count = int(to_float(row.get("objects")))
        by_count.setdefault(object_count, []).append(to_float(row.get("fanin_ms")))

    min_count = min(by_count)
    max_count = max(by_count)
    min_ms = mean(by_count[min_count])
    max_ms = mean(by_count[max_count])
    ratio = max_ms / min_ms if min_ms > 0 else 0.0
    score = 2 if ratio >= 2.0 else 1 if ratio >= 1.2 else 0
    return {
        "status": "done",
        "evidence": (
            f"固定总数据量下，{min_count} objects fan-in {min_ms:.3f} ms；"
            f"{max_count} objects fan-in {max_ms:.3f} ms；放大 {ratio:.2f}x。"
        ),
        "score": score,
    }


def analyze_arrow_fanout_fanin(rows: list[dict]) -> dict:
    rows = without_warmup(rows)
    if not rows:
        return {
            "status": "missing",
            "evidence": "未发现 Ray Arrow fan-out/fan-in 结果。",
            "score": 0,
        }

    ratios = []
    groups = {}
    for row in rows:
        key = (
            row.get("upstream"),
            row.get("downstream"),
            row.get("total_rows"),
            row.get("embedding_dim"),
            row.get("repeat"),
        )
        groups.setdefault(key, {})[row.get("strategy")] = to_float(row.get("fanin_ms"))

    for values in groups.values():
        fine_ms = values.get("fine", 0.0)
        coalesced_ms = values.get("coalesced", 0.0)
        if fine_ms > 0 and coalesced_ms > 0:
            ratios.append(fine_ms / coalesced_ms)

    max_objects = max(to_float(row.get("objects")) for row in rows)
    avg_ratio = mean(ratios)
    score = 2 if avg_ratio >= 1.5 else 1 if avg_ratio >= 1.2 else 0
    return {
        "status": "done",
        "evidence": (
            f"最大 RecordBatch object 数 {int(max_objects)}；"
            f"fine/coalesced 平均 fan-in 比 {avg_ratio:.2f}x。"
        ),
        "score": score,
    }


def analyze_fake_ai_embed(rows: list[dict]) -> dict:
    rows = without_warmup(rows)
    if not rows:
        return {
            "status": "missing",
            "evidence": "未发现 fake AI_EMBED(text) 端到端结果。",
            "score": 0,
        }

    fanin_ratios = []
    e2e_ratios = []
    groups = {}
    for row in rows:
        key = (
            row.get("upstream"),
            row.get("downstream"),
            row.get("total_rows"),
            row.get("embedding_dim"),
            row.get("text_tokens"),
            row.get("repeat"),
        )
        groups.setdefault(key, {})[row.get("strategy")] = row

    for values in groups.values():
        fine = values.get("fine")
        coalesced = values.get("coalesced")
        if not fine or not coalesced:
            continue

        fine_fanin_ms = to_float(fine.get("fanin_ms"))
        coalesced_fanin_ms = to_float(coalesced.get("fanin_ms"))
        if fine_fanin_ms > 0 and coalesced_fanin_ms > 0:
            fanin_ratios.append(fine_fanin_ms / coalesced_fanin_ms)

        fine_e2e_ms = to_float(fine.get("end_to_end_ms"))
        coalesced_e2e_ms = to_float(coalesced.get("end_to_end_ms"))
        if fine_e2e_ms > 0 and coalesced_e2e_ms > 0:
            e2e_ratios.append(fine_e2e_ms / coalesced_e2e_ms)

    max_objects = max(to_float(row.get("input_objects")) for row in rows)
    avg_fanin_ratio = mean(fanin_ratios)
    avg_e2e_ratio = mean(e2e_ratios)
    score = 2 if avg_e2e_ratio >= 1.2 else 1 if avg_fanin_ratio >= 1.2 else 0
    return {
        "status": "done",
        "evidence": (
            f"最大输入 object 数 {int(max_objects)}；"
            f"fine/coalesced 平均 fan-in 比 {avg_fanin_ratio:.2f}x；"
            f"端到端耗时比 {avg_e2e_ratio:.2f}x。"
        ),
        "score": score,
    }


def choose_direction(analyses: dict) -> tuple[str, list[str]]:
    small_task_score = analyses["small_task"]["score"]
    object_score = analyses["object_transfer"]["score"]
    arrow_score = analyses["arrow"]["score"]
    shuffle_score = analyses["shuffle"]["score"]
    many_objects_score = analyses["many_objects"]["score"]
    arrow_fanout_score = analyses["arrow_fanout_fanin"]["score"]
    fake_ai_embed_score = analyses["fake_ai_embed"]["score"]
    missing = [name for name, item in analyses.items() if item["status"] == "missing"]
    smoke_only = any("smoke test" in item["evidence"] for item in analyses.values())

    reasons = []
    if missing:
        reasons.append("部分关键实验尚未完成，当前结论只能作为阶段性判断。")

    if fake_ai_embed_score >= 2:
        reasons.append("fake AI_EMBED(text) 端到端实验已显示 coalescing 对整体链路有收益，object/fan-in 可作为 Phase 0 入口证据；下一步应拆分收益来源，并验证 task/actor/concurrency、模型服务队列和 backpressure 是否形成更高层瓶颈。")
        return "数据库 AI 算子的特征感知任务划分、并行执行与跨层调度优化", reasons

    object_shuffle_score = object_score + arrow_score + shuffle_score + many_objects_score + arrow_fanout_score
    if object_shuffle_score >= 3:
        reasons.append("object transfer、RecordBatch fan-in 或 shuffle 证据较强，贴合数据库 AI 算子链路中的中间数据传输问题；但仍需要端到端 AI 算子和调度维度验证。")
        return "数据库 AI 算子链路中的 Object Transfer / fan-in 与任务划分优化", reasons

    if small_task_score >= 2:
        reasons.append("当前主要证据来自 task 固定成本，object/shuffle 证据不足。")
        return "task batching，暂不作为当前主线", reasons

    if smoke_only:
        reasons.append("已有结果主要是 smoke test，只能证明脚本可运行，不能支撑当前路线。")
        return "当前路线证据不足，先补端到端动机实验", reasons

    if not missing:
        reasons.append("当前 microbenchmark 没有给出强瓶颈证据，不宜直接进入实现。")
        reasons.append("下一步应补数据库 AI 算子端到端动机测试，再决定是否收敛。")
        return "暂不进入优化实现，优先做端到端动机验证", reasons

    reasons.append("当前缺少足够实验数据，先补齐 Ray 与 Arrow benchmark。")
    return "当前路线证据不足，等待实验补齐", reasons


def render_report(analyses: dict, recommendation: str, reasons: list[str]) -> str:
    if analyses["fake_ai_embed"]["status"] == "done":
        first_next_step = "基于 fake `AI_EMBED(text)` 端到端结果，继续拆分 build、Ray put、fake embedding、fan-in、write 阶段，判断收益主要来自 object coalescing 还是 task 数减少。"
    else:
        first_next_step = "搭建 fake `AI_EMBED(text)` 端到端动机测试，确认 RecordBatch fan-in 现象是否会迁移到批量 embedding / RAG 数据准备链路。"

    lines = [
        "# 前期可行性实验结果分析",
        "",
        "## 1. 当前方向",
        "",
        f"> {recommendation}",
        "",
        "## 2. 当前依据",
        "",
    ]
    for reason in reasons:
        lines.append(f"- {reason}")

    lines.extend(
        [
            "",
            "## 3. 实验证据",
            "",
            "| 实验 | 状态 | 证据 | 证据分 |",
            "|---|---|---|---|",
        ]
    )
    labels = {
        "small_task": "Ray small task",
        "object_transfer": "Ray object transfer",
        "arrow": "Arrow serialization",
        "shuffle": "Shuffle simulation",
        "many_objects": "Ray many objects",
        "arrow_fanout_fanin": "Ray Arrow fan-out/fan-in",
        "fake_ai_embed": "fake AI_EMBED pipeline",
    }
    for key, label in labels.items():
        item = analyses[key]
        lines.append(f"| {label} | {item['status']} | {item['evidence']} | {item['score']} |")

    lines.extend(
        [
            "",
            "## 4. 当前方向依据",
            "",
            "| 观察结果 | 更适合的方向 |",
            "|---|---|",
            "| small task 开销强 | task batching；仅作为备选，不作为当前主线 |",
            "| object transfer 或 Arrow 成本强 | Object Transfer 优化 / Arrow buffer 路径优化 |",
            "| fine shuffle 明显慢于 coalesced | Shuffle batching / partition-aware execution |",
            "| 固定总量下 object 数量越多 fan-in 越慢 | Object coalescing / fan-in 优化 |",
            "| Arrow RecordBatch fine fan-in 明显慢于 coalesced | 数据库 AI 算子批处理链路中的 object 合并优化 |",
            "| fake AI_EMBED 端到端 fine 明显慢于 coalesced | 继续推进真实数据库 AI 算子外部执行链路验证 |",
            "| 上述证据都弱，但 scan 成本强 | Lance scan / filter / projection pushdown |",
            "| 所有证据都弱 | 回到数据库 AI 算子链路做端到端动机验证 |",
            "",
            "## 5. 下一步",
            "",
            f"1. {first_next_step}",
            "2. 如果端到端结果仍显示 Object/Shuffle 证据最强，再做 Daft local vs Ray 和 Lance/Parquet scan 对照。",
            "3. 如果 small task 证据后续变强，再评估 task batching 是否值得作为辅助优化。",
            "4. 如果端到端证据变弱，优先回到 PostgreSQL + pgvector / PostgreSQL 18.3 真实 AI 算子形态验证。",
            "",
        ]
    )
    return "\n".join(lines)


def main():
    args = parse_args()
    results_dir = Path(args.results_dir)
    rows = {
        name: read_csv(results_dir / filename)
        for name, filename in RESULT_FILES.items()
    }
    analyses = {
        "small_task": analyze_small_task(rows["small_task"]),
        "object_transfer": analyze_object_transfer(rows["object_transfer"]),
        "arrow": analyze_arrow(rows["arrow"]),
        "shuffle": analyze_shuffle(rows["shuffle"]),
        "many_objects": analyze_many_objects(rows["many_objects"]),
        "arrow_fanout_fanin": analyze_arrow_fanout_fanin(rows["arrow_fanout_fanin"]),
        "fake_ai_embed": analyze_fake_ai_embed(rows["fake_ai_embed"]),
    }
    recommendation, reasons = choose_direction(analyses)
    report = render_report(analyses, recommendation, reasons)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report)
    print(report)


if __name__ == "__main__":
    main()
