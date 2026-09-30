"""Pytest global configuration and autouse fixtures for thread and config isolation."""
from __future__ import annotations

import os
import threading
import time
from typing import Set
import pytest

from app.config import reload_config


def _is_app_thread(t: threading.Thread) -> bool:
    """Identify if a thread belongs to application background infrastructure."""
    name_lower = t.name.lower()
    if "detectionworker" in name_lower or "senderthread" in name_lower:
        return True
    target_str = str(getattr(t, "_target", "")).lower()
    if "server.run" in target_str or "detectionworker" in target_str:
        return True
    return False


@pytest.fixture(autouse=True)
def thread_and_config_isolation():
    """Autouse fixture that:
    1. Records threading.enumerate() before and after every test.
    2. Restores config and environment on teardown.
    3. Fails the test if an app thread (detection worker, sender, uvicorn) survives its teardown.
    """
    old_env = os.environ.get("ERCT_CONFIG_PATH")
    pre_threads: Set[threading.Thread] = set(threading.enumerate())

    try:
        yield
    finally:
        # 1. Restore config and environment
        if old_env is not None:
            os.environ["ERCT_CONFIG_PATH"] = old_env
            reload_config(old_env)
        else:
            os.environ.pop("ERCT_CONFIG_PATH", None)
            reload_config()

        # 2. Check for surviving app threads
        # Allow short grace period if thread is currently joining
        for _ in range(10):
            surviving = [
                t for t in threading.enumerate()
                if t.is_alive() and _is_app_thread(t)
            ]
            if not surviving:
                break
            time.sleep(0.1)

        if surviving:
            details = ", ".join(f"'{t.name}' (id={t.ident}, alive={t.is_alive()})" for t in surviving)
            pytest.fail(f"App thread(s) survived test teardown: {details}")
