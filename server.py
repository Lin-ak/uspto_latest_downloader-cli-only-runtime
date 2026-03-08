#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from api_paths import (
    HEALTH_READY_PATH,
    PUBLIC_LATEST_FILE_DOWNLOAD_PATH,
    PUBLIC_STATUS_PATH,
)
from app_factory import app, create_app, main, parse_args

__all__ = [
    "PUBLIC_STATUS_PATH",
    "HEALTH_READY_PATH",
    "PUBLIC_LATEST_FILE_DOWNLOAD_PATH",
    "app",
    "create_app",
    "parse_args",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
