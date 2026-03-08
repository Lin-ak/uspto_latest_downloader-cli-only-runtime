#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

HEALTH_READY_PATH = "/health/ready"
PUBLIC_ROUTE_ERRORS = {
    "status": "公开状态暂时不可用，请稍后重试。",
    "download_latest": "最新下载文件暂时不可用，请稍后重试。",
}
PUBLIC_STATUS_PATH = "/api/v1/status"
PUBLIC_LATEST_FILE_DOWNLOAD_PATH = "/api/v1/files/latest/download"
