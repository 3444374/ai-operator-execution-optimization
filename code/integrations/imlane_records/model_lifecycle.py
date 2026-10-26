"""The IMLane carrier preserves the shared engine's bounded close protocol."""

import threading

from src.baselines.text.frameworks.daft_engine_process import DEFAULT_CLOSE_TIMEOUT_S


# Includes the engine's API/owned-process close, reaper join and carrier reply.
MODEL_CLOSE_GRACE_S = DEFAULT_CLOSE_TIMEOUT_S + 5.0


def model_closed(report):
    if (type(report) is not dict or report.get("status") != "closed"
            or report.get("process_exit_confirmed") is not True
            or type(report.get("cleanup_errors")) is not list or report["cleanup_errors"]):
        return False
    if report.get("no_model_process_started") is True:
        return True
    api = report.get("engine_api")
    return (type(api) is dict and api.get("status") == "closed"
            and api.get("engine_api_shutdown_confirmed") is True)


def close_owned_model(kernel, *, attempted):
    if kernel is None:
        return dict(status="unknown" if attempted else "closed", process_exit_confirmed=not attempted,
                    gpu_exit_confirmed=None, no_model_process_started=not attempted,
                    cleanup_errors=["model_initialization_handle_unavailable"] if attempted else [])
    try:
        report = kernel.close()
    except Exception as error:
        report = dict(status="unknown", process_exit_confirmed=False, gpu_exit_confirmed=None,
                      cleanup_errors=["model_close: " + type(error).__name__])
    reaper = getattr(kernel, "_reaper", None)
    if reaper is not None and reaper is not threading.current_thread():
        reaper.join(1)
        report = dict(report, owned_reaper_stopped=not reaper.is_alive())
        if reaper.is_alive():
            report.update(status="unknown", cleanup_errors=[*report.get("cleanup_errors", []),
                                                              "model_reaper_still_running"])
    if not model_closed(report) and report.get("status") == "closed":
        report = dict(report, status="unknown", cleanup_errors=[*report.get("cleanup_errors", []),
                                                               "engine_api_shutdown_unconfirmed"])
    return report
