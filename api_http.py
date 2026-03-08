#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
import mimetypes
from datetime import datetime, timezone
from email.utils import format_datetime, parsedate_to_datetime
from http import HTTPStatus
from pathlib import Path
from typing import Any, Callable

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse, Response

from api_contract import error_payload, success_payload
from downloader_common import error_hint_for_code


logger = logging.getLogger("server")


def log_internal_error(scope: str, exc: Exception) -> None:
    logger.exception("server_internal_error", extra={"structured_data": {"scope": scope}})


def detect_content_type(file_path: Path) -> str:
    content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    if content_type.startswith("text/"):
        return f"{content_type}; charset=utf-8"
    if content_type == "application/javascript":
        return "application/javascript; charset=utf-8"
    return content_type


def json_response(status_code: int, payload: dict[str, Any]) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=payload,
        headers={"Cache-Control": "no-store"},
    )


def success_response(
    data: Any,
    *,
    status_code: int = HTTPStatus.OK,
    meta: dict[str, Any] | None = None,
) -> JSONResponse:
    return json_response(status_code, success_payload(data, meta=meta))


def error_response(
    status_code: int,
    code: str,
    message: str,
    *,
    hint: str | None = None,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    return json_response(
        status_code,
        error_payload(
            code,
            message,
            hint=hint or error_hint_for_code(code),
            details=details,
        ),
    )


ErrorMessageFactory = str | Callable[[Exception], str]
ServiceLoader = Callable[[], Any]


def _resolve_error_message(error_message: ErrorMessageFactory, exc: Exception) -> str:
    return error_message(exc) if callable(error_message) else error_message


def execute_service_call(
    loader: ServiceLoader,
    *,
    error_code: str,
    error_message: ErrorMessageFactory,
    log_scope: str | None = None,
) -> JSONResponse:
    try:
        result = loader()
    except Exception as exc:  # noqa: BLE001
        if log_scope is not None:
            log_internal_error(log_scope, exc)
        return error_response(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            error_code,
            _resolve_error_message(error_message, exc),
        )

    return success_response(result)


def file_response(file_path: Path, cache_control: str, filename: str | None = None) -> FileResponse:
    response = FileResponse(
        file_path,
        media_type=detect_content_type(file_path),
        filename=filename,
    )
    response.headers["Cache-Control"] = cache_control
    return response


def current_download_headers(file_path: Path) -> tuple[dict[str, str], float]:
    stat = file_path.stat()
    last_modified = format_datetime(datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc), usegmt=True)
    etag = f'"{stat.st_mtime_ns:x}-{stat.st_size:x}"'
    headers = {
        "Cache-Control": "public, max-age=0, must-revalidate",
        "ETag": etag,
        "Last-Modified": last_modified,
        "X-Content-Type-Options": "nosniff",
    }
    return headers, stat.st_mtime


def is_conditional_request_fresh(request: Request, etag: str, mtime_seconds: float) -> bool:
    if_none_match = request.headers.get("if-none-match", "")
    if if_none_match:
        candidates = {item.strip() for item in if_none_match.split(",") if item.strip()}
        if "*" in candidates or etag in candidates:
            return True

    if_modified_since = request.headers.get("if-modified-since", "").strip()
    if if_modified_since:
        try:
            parsed = parsedate_to_datetime(if_modified_since)
        except (TypeError, ValueError, IndexError, OverflowError):
            return False

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        if int(mtime_seconds) <= int(parsed.timestamp()):
            return True

    return False


def download_file_response(request: Request, file_path: Path, download_name: str) -> Response:
    headers, mtime_seconds = current_download_headers(file_path)
    if is_conditional_request_fresh(request, headers["ETag"], mtime_seconds):
        return Response(status_code=HTTPStatus.NOT_MODIFIED, headers=headers)

    response = file_response(file_path, headers["Cache-Control"], filename=download_name)
    for header_name, header_value in headers.items():
        response.headers[header_name] = header_value
    return response
