#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from http import HTTPStatus
from typing import Callable

from fastapi import FastAPI, Request
from fastapi.responses import Response

from app.http import (
    download_file_response,
    error_response,
    execute_service_call,
    log_internal_error,
)
from app.paths import (
    PUBLIC_LATEST_FILE_DOWNLOAD_PATH,
    PUBLIC_ROUTE_ERRORS,
    PUBLIC_STATUS_PATH,
)
from app.schemas import ErrorResponseModel, PublicStatusResponseModel
from sync.service import DownloaderService


ServiceGetter = Callable[[], DownloaderService]


def register_public_routes(
    app: FastAPI,
    current_service: ServiceGetter,
) -> None:
    @app.get(
        PUBLIC_STATUS_PATH,
        tags=["public"],
        response_model=PublicStatusResponseModel,
        responses={
            HTTPStatus.INTERNAL_SERVER_ERROR: {"model": ErrorResponseModel},
        },
    )
    def read_public_status() -> Response:
        return execute_service_call(
            lambda: current_service().build_public_status(),
            error_code="public_status_unavailable",
            error_message=PUBLIC_ROUTE_ERRORS["status"],
            log_scope="public-status",
        )

    @app.get(
        PUBLIC_LATEST_FILE_DOWNLOAD_PATH,
        tags=["public"],
        responses={
            HTTPStatus.OK: {
                "content": {
                    "application/zip": {
                        "schema": {
                            "type": "string",
                            "format": "binary",
                        }
                    }
                },
                "description": "Latest local ZIP file",
            },
            HTTPStatus.NOT_FOUND: {"model": ErrorResponseModel},
            HTTPStatus.INTERNAL_SERVER_ERROR: {"model": ErrorResponseModel},
        },
    )
    def read_public_download_latest(request: Request) -> Response:
        try:
            resolved = current_service().resolve_latest_downloaded_file()
        except Exception as exc:  # noqa: BLE001
            log_internal_error("public-download-latest", exc)
            return error_response(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "public_latest_file_download_unavailable",
                PUBLIC_ROUTE_ERRORS["download_latest"],
            )

        if resolved is None:
            return error_response(
                HTTPStatus.NOT_FOUND,
                "latest_file_not_found",
                "当前没有可下载的最新本地文件。",
            )

        file_path, entry = resolved
        download_name = str(entry.get("file_name", file_path.name)).strip() or file_path.name
        return download_file_response(request, file_path, download_name)
