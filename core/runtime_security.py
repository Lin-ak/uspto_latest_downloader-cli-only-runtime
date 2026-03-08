#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

from core.common import DownloadError, PUBLIC_ERROR_MESSAGES


def _chmod_path(path: Path, mode: int) -> None:
    if not path.exists():
        return
    try:
        path.chmod(mode)
    except OSError as exc:
        raise DownloadError(
            f"无法设置运行时文件权限: {path}",
            code="internal_error",
            public_message=PUBLIC_ERROR_MESSAGES["internal_error"],
        ) from exc


def secure_runtime_artifacts(
    *,
    runtime_dir: Path,
    db_path: Path | None = None,
    lock_path: Path | None = None,
    state_path: Path | None = None,
    extra_files: Iterable[Path] = (),
) -> None:
    if os.name == "nt":
        return

    runtime_dir.mkdir(parents=True, exist_ok=True)
    _chmod_path(runtime_dir, 0o700)

    file_candidates = list(extra_files)
    if db_path is not None:
        file_candidates.extend(
            [
                db_path,
                db_path.with_name(f"{db_path.name}-wal"),
                db_path.with_name(f"{db_path.name}-shm"),
            ]
        )
    if lock_path is not None:
        file_candidates.append(lock_path)
    if state_path is not None:
        file_candidates.extend(
            [
                state_path,
                state_path.with_suffix(".json.migrated"),
            ]
        )

    for candidate in file_candidates:
        if candidate.exists() and candidate.is_file():
            _chmod_path(candidate, 0o600)
