"""Tests for RotatingFileHandler rotation behaviour across all four log handlers.

Each handler (debug/error/api/tide) uses identical RotatingFileHandler config.
This file proves that rotation creates a .1 backup and keeps the active log open.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from incorporator.observability.logger import _ACTIVE_LISTENERS, setup_class_logger


@pytest.mark.parametrize(
    "suffix,level,extras",
    [
        ("debug.log", logging.DEBUG, {"meta": "x", "is_api": False}),
        ("error.log", logging.INFO, {"meta": "x", "is_api": False}),
        ("api.log", logging.INFO, {"meta": "x", "is_api": True}),
        ("tide.log", logging.INFO, {"meta": "x", "is_tide": True, "is_api": False}),
    ],
)
def test_log_rotation_creates_backup_when_max_bytes_exceeded(
    suffix: str,
    level: int,
    extras: dict[str, object],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reset_active_listeners: None,
) -> None:
    """When a log handler exceeds maxBytes, RotatingFileHandler creates a
    .1 backup and continues writing to the active file.  Uniform behaviour
    across api/error/debug/tide handlers — all four use identical
    RotatingFileHandler config.
    """
    # Path isolation: route logs into tmp_path via the env var
    monkeypatch.setenv("INCORPORATOR_LOG_DIR", str(tmp_path))

    cls_name = f"RotTest_{suffix.replace('.', '_')}"
    setup_class_logger(cls_name)

    # Shrink maxBytes on ALL 4 handlers so rotation triggers in ~150 records
    listener = _ACTIVE_LISTENERS[cls_name]
    for h in listener.handlers:
        h.maxBytes = 1024  # 1 KB

    # Emit ~150 records targeting the suffix's filter
    logger = logging.getLogger(cls_name)
    for i in range(150):
        logger.log(level, "record-%d-padding-padding-padding-padding" % i, extra=extras)

    # CRITICAL: drain queue via stop() BEFORE asserting file existence.
    # reset_active_listeners fixture also stops, but the file asserts must
    # run AFTER the background thread is joined. del from registry so the
    # fixture teardown doesn't try to stop a stopped listener.
    listener.stop()
    del _ACTIVE_LISTENERS[cls_name]

    # Assert the .1 backup file exists (rotation occurred) AND the active
    # file is still present (writing continues post-rotation).
    backup_path = tmp_path / f"{cls_name}_{suffix}.1"
    active_path = tmp_path / f"{cls_name}_{suffix}"
    assert backup_path.exists(), f"expected rotation backup at {backup_path}"
    assert active_path.exists(), f"expected active log at {active_path}"
