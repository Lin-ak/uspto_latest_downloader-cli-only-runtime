#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import logging
import sys

from core.common import PUBLIC_ERROR_MESSAGES, error_hint_for_code
from core.contract import (
    SYNC_LATEST_FILE_OPERATION,
    SYNC_LATEST_FILE_RESOURCE,
    error_payload,
    sync_latest_file_payload,
)
from core.logging_utils import configure_logging, log_event
from sync.service import DownloadError, build_latest_service


logger = logging.getLogger(__name__)


def _sanitize_cli_record(record: dict[str, object] | None) -> dict[str, object] | None:
    if not isinstance(record, dict):
        return None

    sanitized = dict(record)
    sanitized.pop("local_path", None)
    return sanitized


def _sanitize_cli_status_payload(status: dict[str, object | None]) -> dict[str, object | None]:
    sanitized = dict(status)
    sanitized.pop("downloads_dir", None)
    sanitized["latest_remote"] = _sanitize_cli_record(sanitized.get("latest_remote"))  # type: ignore[arg-type]
    sanitized["last_download"] = _sanitize_cli_record(sanitized.get("last_download"))  # type: ignore[arg-type]
    history = sanitized.get("download_history")
    if isinstance(history, list):
        sanitized["download_history"] = [
            sanitized_entry
            for item in history
            for sanitized_entry in [_sanitize_cli_record(item)]  # type: ignore[arg-type]
            if sanitized_entry is not None
        ]
    return sanitized


def _fallback_status_payload() -> dict[str, object | None]:
    return {
        "running": False,
        "last_checked_at": None,
        "last_action": None,
        "latest_remote": None,
        "last_download": None,
        "last_error": None,
    }


def _safe_status_payload(service: object) -> dict[str, object | None]:
    try:
        status = service.build_status()
    except Exception as exc:  # noqa: BLE001
        log_event(
            logger,
            logging.ERROR,
            "download_cli_status_snapshot_failed",
            error_code="internal_error",
            error_message=str(exc),
        )
        return _fallback_status_payload()

    if not isinstance(status, dict):
        return _fallback_status_payload()
    return _sanitize_cli_status_payload(status)


def main() -> int:
    configure_logging()
    service = build_latest_service()
    log_event(logger, logging.INFO, "download_cli_started", runtime_db=str(service.db_path))

    try:
        result = service.run_download_latest(trigger_source="cli")
    except DownloadError as exc:
        log_event(
            logger,
            logging.ERROR if not exc.retryable else logging.WARNING,
            "download_cli_finished",
            outcome="error",
            error_code=exc.code,
            retryable=exc.retryable,
        )
        payload = json.dumps(
            error_payload(
                exc.code,
                str(exc),
                hint=error_hint_for_code(exc.code),
                details={
                    "operation": SYNC_LATEST_FILE_OPERATION,
                    "resource": SYNC_LATEST_FILE_RESOURCE,
                    "status": _safe_status_payload(service),
                    "public_message": exc.public_message,
                    "retryable": exc.retryable,
                },
            ),
            ensure_ascii=False,
        )
        sys.stdout.write(f"{payload}\n")
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.exception("download_cli_unexpected_error")
        log_event(
            logger,
            logging.ERROR,
            "download_cli_finished",
            outcome="error",
            error_code="internal_error",
        )
        payload = json.dumps(
            error_payload(
                "internal_error",
                PUBLIC_ERROR_MESSAGES["internal_error"],
                hint=error_hint_for_code("internal_error"),
                details={
                    "operation": SYNC_LATEST_FILE_OPERATION,
                    "resource": SYNC_LATEST_FILE_RESOURCE,
                    "status": _safe_status_payload(service),
                },
            ),
            ensure_ascii=False,
        )
        sys.stdout.write(f"{payload}\n")
        return 1

    log_event(
        logger,
        logging.INFO,
        "download_cli_finished",
        outcome=str(result["action"]),
    )
    payload = json.dumps(
        sync_latest_file_payload(
            outcome=str(result["action"]),
            status=_sanitize_cli_status_payload(result["status"]),
            latest_remote=_sanitize_cli_record(result["latest_remote"]),
            last_download=_sanitize_cli_record(result["last_download"]),
            summary=result.get("summary"),
        ),
        ensure_ascii=False,
        indent=2,
    )
    sys.stdout.write(f"{payload}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
