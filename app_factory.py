#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from contextlib import asynccontextmanager
import logging

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware

from api_http import error_response, success_response
from api_paths import HEALTH_READY_PATH, PUBLIC_LATEST_FILE_DOWNLOAD_PATH
from api_routes_public import register_public_routes
from api_schemas import ErrorResponseModel, HealthResponseModel
from downloader import DownloaderService, build_latest_service
from logging_utils import configure_logging, log_event


service = build_latest_service()
logger = logging.getLogger("server")


class SelectiveGZipMiddleware:
    def __init__(self, app, *, minimum_size: int = 1024, excluded_paths: tuple[str, ...] = ()) -> None:
        self.app = app
        self.excluded_paths = set(excluded_paths)
        self.gzip_app = GZipMiddleware(app, minimum_size=minimum_size)

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http" or scope.get("path", "") in self.excluded_paths:
            await self.app(scope, receive, send)
            return
        await self.gzip_app(scope, receive, send)


def create_app(
    downloader_service: DownloaderService | None = None,
    *,
    run_startup_checks: bool = True,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_logging()
        current_service = app.state.service
        if run_startup_checks:
            current_service.reset_running_flag()
            repaired_entries = current_service.repair_download_history_from_disk(if_missing_only=True)
            log_event(
                logger,
                logging.INFO,
                "app_startup_initialized",
                downloads_dir=str(current_service.downloads_dir),
                runtime_db=str(current_service.db_path),
                repaired_entries=repaired_entries,
            )
        yield

    app = FastAPI(
        title="USPTO 最新文件自动下载服务",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.add_middleware(
        SelectiveGZipMiddleware,
        minimum_size=1024,
        excluded_paths=(PUBLIC_LATEST_FILE_DOWNLOAD_PATH,),
    )
    app.state.service = downloader_service or service

    def current_service() -> DownloaderService:
        return app.state.service

    @app.get(
        HEALTH_READY_PATH,
        tags=["health"],
        response_model=HealthResponseModel,
        responses={
            503: {"model": ErrorResponseModel},
        },
    )
    def read_health_ready():
        try:
            current_service().ensure_layout()
            current_service().load_state()
        except Exception:  # noqa: BLE001
            return error_response(
                503,
                "service_not_ready",
                "服务尚未就绪。",
            )

        return success_response(
            {
                "status": "ready",
                "service": "uspto_latest_downloader",
                "checks": {
                    "runtime_db": "ok",
                    "downloads_dir": "ok",
                },
            }
        )

    register_public_routes(app, current_service)
    return app


app = create_app()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="USPTO 最新文件自动下载最小版后端服务")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认 127.0.0.1")
    parser.add_argument("--port", type=int, default=8010, help="监听端口，默认 8010")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configure_logging()
    log_event(
        logger,
        logging.INFO,
        "server_starting",
        host=args.host,
        port=args.port,
        downloads_dir=str(service.downloads_dir),
        runtime_db=str(service.db_path),
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="info", log_config=None)
    return 0
