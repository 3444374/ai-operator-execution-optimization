"""Compatibility API for the static comparison launcher.

The maintained experiment harness still invokes the original profiler CLI.
This module does not implement a model backend or a scheduler.
"""
import subprocess

from src.experiments.project_static import (
    ProjectStaticConfig, ProjectStaticRun, SemLoomStaticConfig, SemLoomStaticRun,
    _to_float, _to_int, build_profiler_argv, merge_results, read_completion_evidence,
    read_source_scan_evidence, read_summary_timing, run_project_static, run_semloom_static,
)
