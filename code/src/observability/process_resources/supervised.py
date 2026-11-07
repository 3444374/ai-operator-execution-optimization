"""Conservative sampled limits for only the supervisor's observed process identities."""

import os
from pathlib import Path
import time

import psutil


class ResourceMonitor:
    def __init__(self, limits, *, shm_root=Path("/dev/shm"), sample_seconds=.1):
        self.limits, self.shm_root, self.sample_seconds = limits, Path(shm_root), sample_seconds
        if not self.shm_root.is_dir():
            raise ValueError("formal resource monitoring requires the actual shared-memory filesystem")
        self.initial_names = set(p.name for p in self.shm_root.iterdir())
        self.initial_shm = self._shm_used()
        self.samples, self.known, self.failures = [], set(), []
        self.last = float("-inf")

    def _shm_used(self):
        value = os.statvfs(self.shm_root)
        return (value.f_blocks - value.f_bfree) * value.f_frsize

    def __call__(self, identities):
        self.known.update(identities)
        now = time.monotonic()
        if now - self.last < self.sample_seconds:
            return
        self.last = now
        rss, widened = 0, []
        for pid, born in self.known:
            try:
                process = psutil.Process(pid)
                if process.create_time() != born or process.status() in (psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE):
                    continue
                rss += process.memory_info().rss
                if hasattr(process, "cpu_affinity") and not set(process.cpu_affinity()) <= set(self.limits["cpu_affinity_slots"]):
                    widened.append(pid)
            except psutil.NoSuchProcess:
                continue
        shm = max(0, self._shm_used() - self.initial_shm)
        self.samples.append(dict(monotonic_s=now, observed_rss_sum_bytes=rss,
            host_shm_used_delta_bytes=shm, cpu_affinity_widened_pids=widened))
        if (rss > self.limits["max_sampled_owned_rss_bytes"]
                or shm > self.limits["max_owned_shm_bytes"] or widened):
            self.failures.append("sampled resource/affinity ceiling exceeded")
            raise RuntimeError(self.failures[-1])

    def finish(self):
        remaining = sorted(set(p.name for p in self.shm_root.iterdir()) - self.initial_names)
        ending_delta = max(0, self._shm_used() - self.initial_shm)
        return dict(passed=bool(self.samples) and not self.failures and not remaining and ending_delta == 0,
            samples=self.samples, failures=self.failures, remaining_new_shm_names=remaining,
            final_host_shm_used_delta_bytes=ending_delta,
            observed_identities=[dict(pid=p, create_time=c) for p, c in sorted(self.known)],
            scope="sum RSS of birth-matched supervisor-observed processes (shared pages may count twice); host SHM used delta under exclusive lease",
            sampling_seconds=self.sample_seconds, kernel_memory_limit_enforced=False,
            unobserved_detached_process_exit_confirmed=False)
