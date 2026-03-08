#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from contextlib import asynccontextmanager
import logging
from collections import deque
import math
import threading
import time
from http import HTTPStatus

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware

from app.http import error_response, success_response
from app.paths import HEALTH_READY_PATH, PUBLIC_LATEST_FILE_DOWNLOAD_PATH, PUBLIC_STATUS_PATH
from app.routes_public import register_public_routes
from app.schemas import ErrorResponseModel, HealthResponseModel
from core.logging_utils import configure_logging, log_event
from sync.service import DownloaderService, build_latest_service


service = build_latest_service()
logger = logging.getLogger("server")
DEFAULT_RATE_LIMIT_RULES = {
    PUBLIC_STATUS_PATH: (60, 60),
    PUBLIC_LATEST_FILE_DOWNLOAD_PATH: (10, 60),
}


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


class InMemoryRateLimitMiddleware:
    def __init__(self, app, *, rules: dict[str, tuple[int, int]]) -> None:
        self.app = app
        self.rules = {
            path: (max(1, int(limit)), max(1, int(window_seconds)))
            for path, (limit, window_seconds) in rules.items()
        }
        self._state: dict[tuple[str, str], deque[float]] = {}
        self._lock = threading.Lock()

    def _client_key(self, scope: dict) -> str:
        client = scope.get("client")
        if isinstance(client, (tuple, list)) and client:
            host = str(client[0]).strip()
            if host:
                return host
        return "unknown"

    def _consume(self, client_key: str, path: str) -> tuple[bool, int, int]:
        limit, window_seconds = self.rules[path]
        now = time.monotonic()
        window_start = now - window_seconds
        bucket_key = (client_key, path)

        with self._lock:
            hits = self._state.setdefault(bucket_key, deque())
            while hits and hits[0] <= window_start:
                hits.popleft()

            if len(hits) >= limit:
                retry_after = max(1, math.ceil(window_seconds - (now - hits[0])))
                return False, retry_after, limit

            hits.append(now)
            return True, 0, limit

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        path = str(scope.get("path", "")).strip()
        if path not in self.rules:
            await self.app(scope, receive, send)
            return

        allowed, retry_after, limit = self._consume(self._client_key(scope), path)
        if allowed:
            await self.app(scope, receive, send)
            return

        response = error_response(
            HTTPStatus.TOO_MANY_REQUESTS,
            "rate_limited",
            "请求过于频繁，请稍后重试。",
        )
        response.headers["Retry-After"] = str(retry_after)
        response.headers["X-RateLimit-Limit"] = str(limit)
        await response(scope, receive, send)


def create_app(
    downloader_service: DownloaderService | None = None,
    *,
    run_startup_checks: bool = True,
    rate_limit_rules: dict[str, tuple[int, int]] | None = None,
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
    app.add_middleware(
        InMemoryRateLimitMiddleware,
        rules=rate_limit_rules or DEFAULT_RATE_LIMIT_RULES,
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
