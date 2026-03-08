#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from api_contract import SYNC_LATEST_FILE_RECOMMENDED_ENTRYPOINT, SYNC_LATEST_FILE_TRIGGER_POLICY
import fcntl
import json
import logging
import os
import random
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict

from downloader_common import (
    DEFAULT_COOKIE_CACHE_TTL_SECONDS,
    DATASET_PAGE_URL,
    DB_PATH,
    DEFAULT_BROWSER_SETTLE_MS,
    DEFAULT_RETRY_JITTER_RATIO,
    DEFAULT_RETRY_ATTEMPTS,
    DEFAULT_RETRY_BACKOFF_SECONDS,
    DEFAULT_TIMEOUT,
    DEFAULT_USER_AGENT,
    DEFAULT_FAILURE_COOLDOWN_SECONDS,
    DOWNLOADS_DIR,
    PARTIAL_DIR,
    PRODUCT_URL,
    PUBLIC_ERROR_MESSAGES,
    ROOT_DIR,
    RUNTIME_DIR,
    STATE_PATH,
    DownloadError,
    RemoteRecord,
    iso_now,
    logger,
)
from downloader_storage import DownloaderStorageMixin
from downloader_upstream import DownloaderUpstreamMixin
from downloader_zip import DownloaderZipMixin
from logging_utils import log_event


class DownloaderService(DownloaderStorageMixin, DownloaderZipMixin, DownloaderUpstreamMixin):
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
        self._state_lock = threading.Lock()
        self._random = random.Random()

    def _fallback_status_snapshot(self, state: Dict[str, Any] | None = None) -> Dict[str, Any]:
        payload = self.default_state()
        if isinstance(state, dict):
            payload.update(state)
        payload["downloads_dir"] = str(self.downloads_dir)
        payload["recommended_scheduler_entrypoint"] = SYNC_LATEST_FILE_RECOMMENDED_ENTRYPOINT
        payload["manual_sync_note"] = SYNC_LATEST_FILE_TRIGGER_POLICY["note"]
        payload["last_success_at"] = None
        payload["last_success_age_seconds"] = None
        payload["last_success_outcome"] = None
        payload["last_run_summary"] = None
        payload["consecutive_failure_count"] = 0
        payload["failure_cooldown_until"] = None
        payload["failure_cooldown_remaining_seconds"] = None
        return payload

    def _safe_build_status(self, state: Dict[str, Any] | None = None, *, phase: str) -> Dict[str, Any]:
        try:
            return self.build_status(state)
        except Exception as exc:  # noqa: BLE001
            log_event(
                logger,
                logging.ERROR,
                "download_job_status_snapshot_failed",
                phase=phase,
                error_message=str(exc),
            )
            return self._fallback_status_snapshot(state)

    def _run_cleanup_step(
        self,
        func: Callable[[], Any],
        *,
        step: str,
        suppress_errors: bool,
    ) -> Any | None:
        try:
            return func()
        except Exception as exc:  # noqa: BLE001
            log_event(
                logger,
                logging.ERROR,
                "download_job_cleanup_failed",
                step=step,
                error_message=str(exc),
                suppressed=suppress_errors,
            )
            if suppress_errors:
                return None
            raise

    def _ensure_failure_cooldown_not_active(self) -> None:
        if self.failure_cooldown_seconds <= 0:
            return

        cooldown = self.get_failure_cooldown_snapshot()
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

    def _apply_failure_cooldown(self, error_payload: Dict[str, Any] | None) -> None:
        if not error_payload or not bool(error_payload.get("retryable")) or self.failure_cooldown_seconds <= 0:
            self.clear_failure_cooldown()
            return

        until = (
            datetime.now().astimezone() + timedelta(seconds=self.failure_cooldown_seconds)
        ).isoformat(timespec="seconds")
        self.set_failure_cooldown(
            until=until,
            error_code=str(error_payload.get("code", "upstream_unavailable")),
            message=str(error_payload.get("public_message") or error_payload.get("message") or ""),
            retryable=bool(error_payload.get("retryable", False)),
        )

    def _acquire_run_lock(self) -> Any:
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

    def _release_run_lock(self, lock_file: Any) -> None:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        finally:
            lock_file.close()

    def _run_download_latest_attempt(self) -> tuple[RemoteRecord, Dict[str, Any]]:
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

    def _run_download_latest_with_retries(
        self,
        *,
        trigger_source: str,
        job_run_id: int | None = None,
    ) -> tuple[RemoteRecord, Dict[str, Any], int]:
        last_error: DownloadError | None = None

        for attempt in range(1, self.retry_attempts + 1):
            log_event(
                logger,
                logging.INFO,
                "download_attempt_started",
                job_run_id=job_run_id,
                trigger_source=trigger_source,
                attempt=attempt,
                max_attempts=self.retry_attempts,
            )
            try:
                latest_remote, result = self._run_download_latest_attempt()
                log_event(
                    logger,
                    logging.INFO,
                    "download_attempt_succeeded",
                    job_run_id=job_run_id,
                    trigger_source=trigger_source,
                    attempt=attempt,
                    outcome=str(result.get("action", "")),
                    file_name=latest_remote.file_name,
                    official_data_date=latest_remote.official_data_date,
                )
                return latest_remote, result, attempt
            except DownloadError as exc:
                last_error = exc
                exc.attempts = attempt
                log_event(
                    logger,
                    logging.WARNING if exc.retryable and attempt < self.retry_attempts else logging.ERROR,
                    "download_attempt_failed",
                    job_run_id=job_run_id,
                    trigger_source=trigger_source,
                    attempt=attempt,
                    max_attempts=self.retry_attempts,
                    error_code=exc.code,
                    error_message=str(exc),
                    public_message=exc.public_message,
                    retryable=exc.retryable,
                )
                if not exc.retryable or attempt >= self.retry_attempts:
                    raise

                delay = self.retry_backoff_seconds * (2 ** (attempt - 1))
                jitter_seconds = 0.0
                if delay > 0 and self.retry_jitter_ratio > 0:
                    jitter_seconds = delay * self._random.uniform(0.0, self.retry_jitter_ratio)
                    delay += jitter_seconds
                log_event(
                    logger,
                    logging.WARNING,
                    "download_retry_scheduled",
                    job_run_id=job_run_id,
                    trigger_source=trigger_source,
                    attempt=attempt,
                    next_attempt=attempt + 1,
                    backoff_seconds=delay,
                    jitter_seconds=jitter_seconds,
                    jitter_ratio=self.retry_jitter_ratio,
                    error_code=exc.code,
                )
                if delay > 0:
                    time.sleep(delay)

        if last_error is not None:
            raise last_error

        raise DownloadError(
            "下载任务未执行。",
            code="internal_error",
            public_message=PUBLIC_ERROR_MESSAGES["internal_error"],
        )

    def run_download_latest(self, trigger_source: str = "manual") -> Dict[str, Any]:
        run_lock = self._acquire_run_lock()
        started_at = iso_now()
        state_running_marked = False
        cleanup_state: Dict[str, Any] | None = None
        job_run_id: int | None = None
        checked_at = started_at
        latest_remote: RemoteRecord | None = None
        result: Dict[str, Any] | None = None
        attempts = 0
        error_payload: Dict[str, Any] | None = None
        primary_exception: Exception | None = None

        try:
            job_run_id = self.create_job_run(
                trigger_source=trigger_source,
                started_at=started_at,
                status=self.build_status(),
            )

            self._ensure_failure_cooldown_not_active()

            state = self.load_state()
            state["running"] = True
            self.write_state(state)
            state_running_marked = True

            log_event(
                logger,
                logging.INFO,
                "download_job_started",
                job_run_id=job_run_id,
                trigger_source=trigger_source,
                downloads_dir=str(self.downloads_dir),
                runtime_db=str(self.db_path),
            )

            checked_at = iso_now()
            latest_remote, result, attempts = self._run_download_latest_with_retries(
                trigger_source=trigger_source,
                job_run_id=job_run_id,
            )

            state = self.load_state()
            state["last_checked_at"] = checked_at
            state["last_action"] = result["action"]
            state["latest_remote"] = result["latest_remote"]
            state["last_download"] = result["last_download"]
            state["last_error"] = None
            self._upsert_history_entry(
                state,
                self._enrich_with_local_state(latest_remote, status="downloaded"),
            )
            self.write_state(state)
        except Exception as exc:  # noqa: BLE001
            primary_exception = exc
            attempts = max(attempts, int(getattr(exc, "attempts", 0) or 0))
            error_payload = self._serialize_error(exc)
            if state_running_marked:
                try:
                    state = self.load_state()
                    state["last_checked_at"] = checked_at
                    state["last_action"] = "error"
                    if latest_remote is not None:
                        state["latest_remote"] = self._enrich_with_local_state(latest_remote)
                        state["last_download"] = self._enrich_with_local_state(latest_remote, status="error")
                    state["last_error"] = error_payload
                    self.write_state(state)
                except Exception as state_exc:  # noqa: BLE001
                    log_event(
                        logger,
                        logging.ERROR,
                        "download_job_error_state_writeback_failed",
                        error_message=str(state_exc),
                        original_error_code=(error_payload or {}).get("code"),
                    )
            raise
        finally:
            suppress_cleanup_errors = primary_exception is not None
            try:
                if state_running_marked:
                    cleanup_state = self._run_cleanup_step(
                        self.load_state,
                        step="running_reset_load_state",
                        suppress_errors=suppress_cleanup_errors,
                    )
                    if isinstance(cleanup_state, dict):
                        cleanup_state["running"] = False
                        self._run_cleanup_step(
                            lambda: self.write_state(cleanup_state),
                            step="running_reset_write_state",
                            suppress_errors=suppress_cleanup_errors,
                        )
                final_outcome = str(result["action"]) if result is not None else "error"
                finished_at = iso_now()
                if error_payload is None:
                    self._run_cleanup_step(
                        self.clear_failure_cooldown,
                        step="clear_failure_cooldown",
                        suppress_errors=suppress_cleanup_errors,
                    )
                else:
                    self._run_cleanup_step(
                        lambda: self._apply_failure_cooldown(error_payload),
                        step="apply_failure_cooldown",
                        suppress_errors=suppress_cleanup_errors,
                    )
                final_status = (
                    self._safe_build_status(cleanup_state, phase="before_job_run_finalize")
                    if suppress_cleanup_errors
                    else self.build_status(cleanup_state)
                )
                final_latest_remote = result["latest_remote"] if result is not None else (
                    self._enrich_with_local_state(latest_remote) if latest_remote is not None else None
                )
                final_last_download = result["last_download"] if result is not None else (
                    self._enrich_with_local_state(latest_remote, status="error") if latest_remote is not None else None
                )
                if job_run_id is not None:
                    self._run_cleanup_step(
                        lambda: self.finalize_job_run(
                            job_run_id=job_run_id,
                            checked_at=checked_at,
                            finished_at=finished_at,
                            outcome=final_outcome,
                            attempts=attempts,
                            latest_remote=final_latest_remote,
                            last_download=final_last_download,
                            status=final_status,
                            error=error_payload,
                        ),
                        step="finalize_job_run",
                        suppress_errors=suppress_cleanup_errors,
                    )
                log_event(
                    logger,
                    logging.INFO if error_payload is None else logging.ERROR,
                    "download_job_finished",
                    job_run_id=job_run_id,
                    trigger_source=trigger_source,
                    outcome=final_outcome,
                    attempts=attempts,
                    file_name=latest_remote.file_name if latest_remote is not None else None,
                    official_data_date=latest_remote.official_data_date if latest_remote is not None else None,
                    duration_ms=self._duration_millis(started_at, finished_at),
                    error_code=(error_payload or {}).get("code"),
                )
            finally:
                self._release_run_lock(run_lock)

        if result is None:
            raise DownloadError("下载任务未返回结果。")

        return {
            "action": result["action"],
            "status": self.build_status(),
            "summary": self._build_job_run_summary(
                outcome=str(result["action"]),
                trigger_source=trigger_source,
                latest_remote=result["latest_remote"],
                error=None,
            ),
            "latest_remote": result["latest_remote"],
            "last_download": result["last_download"],
        }


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

    return DownloaderService(
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
