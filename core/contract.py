#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any


SYNC_LATEST_FILE_OPERATION = "sync_latest_file"
SYNC_LATEST_FILE_RESOURCE = "files/latest"
SYNC_LATEST_FILE_RECOMMENDED_ENTRYPOINT = "run_download_latest_once.py"
SYNC_LATEST_FILE_TRIGGER_POLICY = {
    "recommended_entrypoint": SYNC_LATEST_FILE_RECOMMENDED_ENTRYPOINT,
    "recommended_mode": "scheduled_cli",
    "note": "常规定时同步请固定调用 run_download_latest_once.py。",
}


def success_payload(data: Any, *, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": True,
        "data": data,
    }
    if meta is not None:
        payload["meta"] = meta
    return payload


def error_payload(
    code: str,
    message: str,
    *,
    hint: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    error: dict[str, Any] = {
        "code": code,
        "message": message,
    }
    if hint:
        error["hint"] = hint
    if details is not None:
        error["details"] = details
    return {
        "ok": False,
        "error": error,
    }


def sync_latest_file_payload(
    *,
    outcome: str,
    status: dict[str, Any],
    latest_remote: dict[str, Any] | None,
    last_download: dict[str, Any] | None,
    summary: str | None = None,
) -> dict[str, Any]:
    return success_payload(
        {
            "operation": SYNC_LATEST_FILE_OPERATION,
            "resource": SYNC_LATEST_FILE_RESOURCE,
            "outcome": outcome,
            "status": status,
            "latest_remote": latest_remote,
            "last_download": last_download,
            "summary": summary,
            "trigger_policy": SYNC_LATEST_FILE_TRIGGER_POLICY,
        }
    )
