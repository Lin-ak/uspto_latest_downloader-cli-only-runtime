#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
import os
from pathlib import Path

from core.common import (
    DEFAULT_BROWSER_SETTLE_MS,
    DEFAULT_COOKIE_CACHE_TTL_SECONDS,
    DEFAULT_FAILURE_COOLDOWN_SECONDS,
    DEFAULT_RETRY_ATTEMPTS,
    DEFAULT_RETRY_BACKOFF_SECONDS,
    DEFAULT_RETRY_JITTER_RATIO,
    DEFAULT_TIMEOUT,
    DEFAULT_USER_AGENT,
    DATASET_PAGE_URL,
    DB_PATH,
    DOWNLOADS_DIR,
    PARTIAL_DIR,
    PRODUCT_URL,
    ROOT_DIR,
    RUNTIME_DIR,
    STATE_PATH,
    DownloadError,
    logger,
    resolve_runtime_paths,
)
from core.logging_utils import log_event
from sync.collaborators import CooldownPolicy, FileStore, RunLock, StateRepository, UpstreamGateway
from sync.use_case import SyncLatestFileUseCase


class DownloaderService:
    def __init__(
        self,
        root_dir: Path = ROOT_DIR,
        downloads_dir: Path = DOWNLOADS_DIR,
        partial_dir: Path = PARTIAL_DIR,
        runtime_dir: Path = RUNTIME_DIR,
        state_path: Path | None = None,
        db_path: Path | None = None,
        lock_path: Path | None = None,
        dataset_page_url: str = DATASET_PAGE_URL,
        product_url: str = PRODUCT_URL,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: float = DEFAULT_TIMEOUT,
        browser_settle_ms: int = DEFAULT_BROWSER_SETTLE_MS,
        cookie_cache_ttl_seconds: int = DEFAULT_COOKIE_CACHE_TTL_SECONDS,
        retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
        retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
        retry_jitter_ratio: float = DEFAULT_RETRY_JITTER_RATIO,
        failure_cooldown_seconds: int = DEFAULT_FAILURE_COOLDOWN_SECONDS,
    ) -> None:
        self.root_dir = root_dir
        self.downloads_dir = downloads_dir
        self.partial_dir = partial_dir
        self.runtime_dir = runtime_dir
        self.state_path = state_path or (runtime_dir / STATE_PATH.name)
        self.db_path = db_path or (runtime_dir / DB_PATH.name)
        self.lock_path = lock_path or (runtime_dir / ".download.lock")
        self.dataset_page_url = dataset_page_url
        self.product_url = product_url
        self.user_agent = user_agent
        self.timeout = timeout
        self.browser_settle_ms = browser_settle_ms
        self.cookie_cache_ttl_seconds = max(0, int(cookie_cache_ttl_seconds))
        self.retry_attempts = max(1, int(retry_attempts))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self.retry_jitter_ratio = max(0.0, float(retry_jitter_ratio))
        self.failure_cooldown_seconds = max(0, int(failure_cooldown_seconds))

        self.state_repository = StateRepository(
            root_dir=self.root_dir,
            downloads_dir=self.downloads_dir,
            partial_dir=self.partial_dir,
            runtime_dir=self.runtime_dir,
            state_path=self.state_path,
            db_path=self.db_path,
            lock_path=self.lock_path,
        )
        self.file_store = FileStore(
            downloads_dir=self.downloads_dir,
            partial_dir=self.partial_dir,
            dataset_page_url=self.dataset_page_url,
            user_agent=self.user_agent,
        )
        self.upstream_gateway = UpstreamGateway(
            repository=self.state_repository,
            file_store=self.file_store,
            dataset_page_url=self.dataset_page_url,
            product_url=self.product_url,
            user_agent=self.user_agent,
            timeout=self.timeout,
            browser_settle_ms=self.browser_settle_ms,
            cookie_cache_ttl_seconds=self.cookie_cache_ttl_seconds,
        )
        self.run_lock = RunLock(
            lock_path=self.lock_path,
            ensure_layout=self.state_repository.ensure_layout,
        )
        self.cooldown_policy = CooldownPolicy(
            repository=self.state_repository,
            failure_cooldown_seconds=self.failure_cooldown_seconds,
        )
        self.sync_latest_file_use_case = SyncLatestFileUseCase(
            repository=self.state_repository,
            file_store=self.file_store,
            upstream_gateway=self.upstream_gateway,
            run_lock=self.run_lock,
            cooldown_policy=self.cooldown_policy,
            downloads_dir=self.downloads_dir,
            db_path=self.db_path,
            retry_attempts=self.retry_attempts,
            retry_backoff_seconds=self.retry_backoff_seconds,
            retry_jitter_ratio=self.retry_jitter_ratio,
        )

    def ensure_layout(self) -> None:
        self.state_repository.ensure_layout()

    def load_state(self) -> dict[str, object]:
        return self.state_repository.load_state()

    def write_state(self, state: dict[str, object]) -> dict[str, object]:
        return self.state_repository.write_state(state)

    def build_status(self, state: dict[str, object] | None = None) -> dict[str, object]:
        return self.state_repository.build_status(state)

    def list_job_runs(self, limit: int = 20, offset: int = 0) -> dict[str, object]:
        return self.state_repository.list_job_runs(limit=limit, offset=offset)

    def load_runtime_cache(self, cache_key: str) -> dict[str, object] | None:
        return self.state_repository.load_runtime_cache(cache_key)

    def write_runtime_cache(
        self,
        cache_key: str,
        value: dict[str, object],
        *,
        expires_at: str | None = None,
    ) -> None:
        self.state_repository.write_runtime_cache(cache_key, value, expires_at=expires_at)

    def delete_runtime_cache(self, cache_key: str) -> None:
        self.state_repository.delete_runtime_cache(cache_key)

    def get_failure_cooldown_snapshot(self) -> dict[str, object]:
        return self.state_repository.get_failure_cooldown_snapshot()

    def set_failure_cooldown(
        self,
        *,
        until: str,
        error_code: str,
        message: str,
        retryable: bool,
    ) -> None:
        self.state_repository.set_failure_cooldown(
            until=until,
            error_code=error_code,
            message=message,
            retryable=retryable,
        )

    def clear_failure_cooldown(self) -> None:
        self.state_repository.clear_failure_cooldown()

    def reset_running_flag(self) -> dict[str, object]:
        return self.state_repository.reset_running_flag()

    def repair_download_history_from_disk(self, *, if_missing_only: bool = False) -> int:
        return self.state_repository.repair_download_history_from_disk(if_missing_only=if_missing_only)

    def run_download_latest(self, trigger_source: str = "manual") -> dict[str, object]:
        return self.sync_latest_file_use_case.execute(trigger_source=trigger_source)


def build_latest_service() -> DownloaderService:
    def _read_int_env(name: str, default: int) -> int:
        raw = os.environ.get(name, "").strip()
        if not raw:
            return default

        try:
            return int(raw)
        except ValueError:
            log_event(
                logger,
                logging.WARNING,
                "service_env_invalid_int",
                variable=name,
                raw_value=raw,
                default=default,
            )
            return default

    def _read_float_env(name: str, default: float) -> float:
        raw = os.environ.get(name, "").strip()
        if not raw:
            return default

        try:
            return float(raw)
        except ValueError:
            log_event(
                logger,
                logging.WARNING,
                "service_env_invalid_float",
                variable=name,
                raw_value=raw,
                default=default,
            )
            return default

    resolved_paths = resolve_runtime_paths(
        os.environ.get("USPTO_ROOT_DIR"),
        os.environ.get("USPTO_DOWNLOADS_DIR"),
        os.environ.get("USPTO_RUNTIME_DIR"),
    )

    return DownloaderService(
        root_dir=resolved_paths["root_dir"],
        downloads_dir=resolved_paths["downloads_dir"],
        partial_dir=resolved_paths["partial_dir"],
        runtime_dir=resolved_paths["runtime_dir"],
        state_path=resolved_paths["state_path"],
        db_path=resolved_paths["db_path"],
        lock_path=resolved_paths["lock_path"],
        cookie_cache_ttl_seconds=_read_int_env(
            "USPTO_COOKIE_CACHE_TTL_SECONDS",
            DEFAULT_COOKIE_CACHE_TTL_SECONDS,
        ),
        retry_jitter_ratio=_read_float_env(
            "USPTO_RETRY_JITTER_RATIO",
            DEFAULT_RETRY_JITTER_RATIO,
        ),
        failure_cooldown_seconds=_read_int_env(
            "USPTO_FAILURE_COOLDOWN_SECONDS",
            DEFAULT_FAILURE_COOLDOWN_SECONDS,
        ),
    )
