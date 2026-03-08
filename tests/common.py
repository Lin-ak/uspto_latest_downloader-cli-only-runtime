#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from downloader import DownloaderService  # noqa: E402


def make_service(root_dir: Path, **kwargs: object) -> DownloaderService:
    return DownloaderService(
        root_dir=root_dir,
        downloads_dir=root_dir / "downloads",
        partial_dir=root_dir / "downloads" / ".partial",
        runtime_dir=root_dir / "runtime",
        state_path=root_dir / "runtime" / "state.json",
        **kwargs,
    )
