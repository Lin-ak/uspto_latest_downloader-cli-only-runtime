#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import fcntl
import json
import logging
import os
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from core.common import PUBLIC_ERROR_MESSAGES, DownloadError, RemoteRecord, iso_now, logger
from core.logging_utils import log_event
from storage.sqlite import DownloaderStorageMixin
from sync.upstream import DownloaderUpstreamMixin
from sync.zip_utils import DownloaderZipMixin


class StateRepository(DownloaderStorageMixin, DownloaderZipMixin):
    def __init__(
        self,
        *,
        root_dir: Path,
        downloads_dir: Path,
        partial_dir: Path,
        runtime_dir: Path,
        state_path: Path,
        db_path: Path,
        lock_path: Path,
    ) -> None:
        self.root_dir = root_dir
        self.downloads_dir = downloads_dir
        self.partial_dir = partial_dir
        self.runtime_dir = runtime_dir
        self.state_path = state_path
        self.db_path = db_path
        self.lock_path = lock_path
        self._state_lock = threading.Lock()

    def upsert_history_entry(self, state: dict[str, Any], entry: dict[str, Any]) -> None:
        self._upsert_history_entry(state, entry)

    def serialize_error(self, exc: Exception) -> dict[str, Any]:
        return self._serialize_error(exc)

    def build_job_run_summary(
        self,
        *,
        outcome: str,
        trigger_source: str,
        latest_remote: dict[str, Any] | None,
        error: dict[str, Any] | None,
    ) -> str:
        return self._build_job_run_summary(
            outcome=outcome,
            trigger_source=trigger_source,
            latest_remote=latest_remote,
            error=error,
        )

    def duration_millis(self, started_at: str, finished_at: str) -> int | None:
        return self._duration_millis(started_at, finished_at)


class FileStore(DownloaderZipMixin):
    def __init__(
        self,
        *,
        downloads_dir: Path,
        partial_dir: Path,
        dataset_page_url: str,
        user_agent: str,
    ) -> None:
        self.downloads_dir = downloads_dir
        self.partial_dir = partial_dir
        self.dataset_page_url = dataset_page_url
        self.user_agent = user_agent

    def ensure_layout(self) -> None:
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.partial_dir.mkdir(parents=True, exist_ok=True)

    def enrich_with_local_state(
        self,
        record: RemoteRecord,
        status: str | None = None,
        *,
        assume_valid: bool = False,
    ) -> dict[str, Any]:
        return self._enrich_with_local_state(record, status, assume_valid=assume_valid)


class UpstreamGateway(DownloaderUpstreamMixin):
    def __init__(
        self,
        *,
        repository: StateRepository,
        file_store: FileStore,
        dataset_page_url: str,
        product_url: str,
        user_agent: str,
        timeout: float,
        browser_settle_ms: int,
        cookie_cache_ttl_seconds: int,
    ) -> None:
        self.repository = repository
        self.file_store = file_store
        self.dataset_page_url = dataset_page_url
        self.product_url = product_url
        self.user_agent = user_agent
        self.timeout = timeout
        self.browser_settle_ms = browser_settle_ms
        self.cookie_cache_ttl_seconds = cookie_cache_ttl_seconds

    def load_runtime_cache(self, cache_key: str) -> dict[str, Any] | None:
        return self.repository.load_runtime_cache(cache_key)

    def write_runtime_cache(
        self,
        cache_key: str,
        value: dict[str, Any],
        *,
        expires_at: str | None = None,
    ) -> None:
        self.repository.write_runtime_cache(cache_key, value, expires_at=expires_at)

    def delete_runtime_cache(self, cache_key: str) -> None:
        self.repository.delete_runtime_cache(cache_key)

    def download_or_skip(self, client: Any, record: RemoteRecord) -> dict[str, Any]:
        return self.file_store.download_or_skip(client, record)

    def _validate_file_name(self, file_name: str) -> str:
        return self.file_store._validate_file_name(file_name)

    def run_latest_attempt(self) -> tuple[RemoteRecord, dict[str, Any]]:
        cookies, used_cached_cookies = self._get_cookies()
        try:
            return self._run_download_latest_attempt_with_cookies(cookies)
        except DownloadError as exc:
            if not used_cached_cookies or not exc.retryable:
                raise

            log_event(
                logger,
                logging.WARNING,
                "cookie_cache_invalidated_after_retryable_failure",
                error_code=exc.code,
                error_message=str(exc),
            )
            self._clear_cached_cookie_jar()
            fresh_cookies, _ = self._get_cookies(force_refresh=True)
            return self._run_download_latest_attempt_with_cookies(fresh_cookies)


class RunLock:
    def __init__(self, *, lock_path: Path, ensure_layout: Callable[[], None]) -> None:
        self.lock_path = lock_path
        self.ensure_layout = ensure_layout

    def acquire(self) -> Any:
        self.ensure_layout()
        lock_file = self.lock_path.open("a+", encoding="utf-8")

        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            lock_file.close()
            log_event(
                logger,
                logging.WARNING,
                "download_job_lock_conflict",
                lock_path=str(self.lock_path),
            )
            raise DownloadError(
                "已有下载任务在运行，请稍后再试。",
                code="download_in_progress",
                public_message=PUBLIC_ERROR_MESSAGES["download_in_progress"],
            ) from exc

        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "acquired_at": iso_now(),
                },
                ensure_ascii=False,
            )
        )
        lock_file.flush()
        return lock_file

    def release(self, lock_file: Any) -> None:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        finally:
            lock_file.close()


class CooldownPolicy:
    def __init__(self, *, repository: StateRepository, failure_cooldown_seconds: int) -> None:
        self.repository = repository
        self.failure_cooldown_seconds = max(0, int(failure_cooldown_seconds))

    def ensure_not_active(self) -> None:
        if self.failure_cooldown_seconds <= 0:
            return

        cooldown = self.repository.get_failure_cooldown_snapshot()
        remaining_seconds = cooldown.get("remaining_seconds")
        if remaining_seconds is None or remaining_seconds <= 0:
            return

        until = str(cooldown.get("until", "")).strip() or None
        raise DownloadError(
            f"最近连续失败，当前冷却到 {until or '稍后'}。请等待冷却结束后再重试。",
            code="cooldown_active",
            public_message=PUBLIC_ERROR_MESSAGES["cooldown_active"],
            retryable=True,
        )

    def apply(self, error_payload: dict[str, Any] | None) -> None:
        if not error_payload or not bool(error_payload.get("retryable")) or self.failure_cooldown_seconds <= 0:
            self.repository.clear_failure_cooldown()
            return

        until = (
            datetime.now().astimezone() + timedelta(seconds=self.failure_cooldown_seconds)
        ).isoformat(timespec="seconds")
        self.repository.set_failure_cooldown(
            until=until,
            error_code=str(error_payload.get("code", "upstream_unavailable")),
            message=str(error_payload.get("public_message") or error_payload.get("message") or ""),
            retryable=bool(error_payload.get("retryable", False)),
        )
