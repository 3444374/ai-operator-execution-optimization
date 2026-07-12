import argparse
import heapq
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
BENCHMARK_DIR = ROOT_DIR / "validation" / "benchmarks"
if str(BENCHMARK_DIR) not in sys.path:
    sys.path.insert(0, str(BENCHMARK_DIR))

from common import now_seconds, print_table, write_csv


def parse_args():
    parser = argparse.ArgumentParser(
        description="Simulate AI operator producer/consumer pressure and model-service backpressure."
    )
    parser.add_argument("--total-requests", type=int, default=512)
    parser.add_argument("--producer-rate", type=float, nargs="+", default=[2000.0, 8000.0])
    parser.add_argument("--replicas", type=int, nargs="+", default=[2, 4])
    parser.add_argument("--queue-limit", type=int, nargs="+", default=[0, 8, 32])
    parser.add_argument("--prompt-tokens", type=int, default=128)
    parser.add_argument("--completion-tokens", type=int, default=64)
    parser.add_argument("--long-request-ratio", type=float, default=0.2)
    parser.add_argument("--long-token-multiplier", type=int, default=8)
    parser.add_argument("--tokens-per-second-per-replica", type=float, default=12000.0)
    parser.add_argument("--submit-overhead-us", type=float, default=15.0)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", default="motivation/results/fake_cpu/backpressure.csv")
    return parser.parse_args()


def make_requests(total: int, prompt_tokens: int, completion_tokens: int, long_ratio: float, long_multiplier: int):
    long_every = max(int(1 / long_ratio), 1) if long_ratio > 0 else total + 1
    requests = []
    for request_id in range(total):
        multiplier = long_multiplier if request_id % long_every == 0 else 1
        tokens = (prompt_tokens + completion_tokens) * multiplier
        prefix_id = request_id % 8
        requests.append({"request_id": request_id, "tokens": tokens, "prefix_id": prefix_id})
    return requests


def choose_replica(replica_available_at: list[float], replica_token_backlog: list[int]) -> int:
    return min(
        range(len(replica_available_at)),
        key=lambda replica_id: (replica_available_at[replica_id], replica_token_backlog[replica_id]),
    )


def simulate(args, producer_rate: float, replicas: int, queue_limit: int, repeat: int) -> dict:
    requests = make_requests(
        args.total_requests,
        args.prompt_tokens,
        args.completion_tokens,
        args.long_request_ratio,
        args.long_token_multiplier,
    )

    producer_interval = 1.0 / producer_rate
    replica_available_at = [0.0 for _ in range(replicas)]
    replica_token_backlog = [0 for _ in range(replicas)]
    in_flight = []
    submit_time = 0.0
    queue_wait_total = 0.0
    max_in_flight = 0
    max_token_backlog = 0
    backpressure_events = 0
    completed = []

    for request in requests:
        arrival_time = request["request_id"] * producer_interval
        submit_time = max(submit_time, arrival_time)

        while in_flight and in_flight[0][0] <= submit_time:
            finish_time, replica_id, tokens = heapq.heappop(in_flight)
            replica_token_backlog[replica_id] -= tokens
            completed.append(finish_time)

        if queue_limit > 0:
            while len(in_flight) >= queue_limit:
                finish_time, replica_id, tokens = heapq.heappop(in_flight)
                replica_token_backlog[replica_id] -= tokens
                completed.append(finish_time)
                submit_time = max(submit_time, finish_time)
                backpressure_events += 1

        replica_id = choose_replica(replica_available_at, replica_token_backlog)
        service_start = max(submit_time, replica_available_at[replica_id])
        service_time = request["tokens"] / args.tokens_per_second_per_replica
        finish_time = service_start + service_time
        queue_wait_total += service_start - submit_time
        replica_available_at[replica_id] = finish_time
        replica_token_backlog[replica_id] += request["tokens"]
        heapq.heappush(in_flight, (finish_time, replica_id, request["tokens"]))

        submit_time += args.submit_overhead_us / 1_000_000.0
        max_in_flight = max(max_in_flight, len(in_flight))
        max_token_backlog = max(max_token_backlog, sum(replica_token_backlog))

    while in_flight:
        finish_time, replica_id, tokens = heapq.heappop(in_flight)
        replica_token_backlog[replica_id] -= tokens
        completed.append(finish_time)

    makespan = max(completed) if completed else 0.0
    total_tokens = sum(request["tokens"] for request in requests)
    avg_queue_wait_ms = (queue_wait_total / max(len(requests), 1)) * 1000.0
    return {
        "repeat": repeat,
        "total_requests": args.total_requests,
        "producer_rate_rps": producer_rate,
        "replicas": replicas,
        "queue_limit": queue_limit,
        "prompt_tokens": args.prompt_tokens,
        "completion_tokens": args.completion_tokens,
        "long_request_ratio": args.long_request_ratio,
        "long_token_multiplier": args.long_token_multiplier,
        "tokens_per_second_per_replica": args.tokens_per_second_per_replica,
        "total_tokens": total_tokens,
        "makespan_ms": makespan * 1000.0,
        "requests_per_second": args.total_requests / makespan if makespan > 0 else 0.0,
        "tokens_per_second": total_tokens / makespan if makespan > 0 else 0.0,
        "avg_queue_wait_ms": avg_queue_wait_ms,
        "max_in_flight": max_in_flight,
        "max_token_backlog": max_token_backlog,
        "backpressure_events": backpressure_events,
    }


def main():
    args = parse_args()
    rows = []
    start = now_seconds()
    for producer_rate in args.producer_rate:
        for replicas in args.replicas:
            for queue_limit in args.queue_limit:
                for repeat in range(args.repeats):
                    rows.append(simulate(args, producer_rate, replicas, queue_limit, repeat))
    elapsed_ms = (now_seconds() - start) * 1000.0
    write_csv(args.output, rows)
    print_table(rows)
    print(f"elapsed_ms={elapsed_ms:.3f}")


if __name__ == "__main__":
    main()
