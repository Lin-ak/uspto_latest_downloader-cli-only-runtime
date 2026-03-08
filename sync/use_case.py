#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
import random
import time
from pathlib import Path
from typing import Any, Callable

from core.common import PUBLIC_ERROR_MESSAGES, DownloadError, RemoteRecord, iso_now, logger
from core.logging_utils import log_event
from sync.collaborators import CooldownPolicy, FileStore, RunLock, StateRepository, UpstreamGateway


class SyncLatestFileUseCase:
    def __init__(
        self,
        *,
        repository: StateRepository,
        file_store: FileStore,
        upstream_gateway: UpstreamGateway,
        run_lock: RunLock,
        cooldown_policy: CooldownPolicy,
        downloads_dir: Path,
        db_path: Path,
        retry_attempts: int,
        retry_backoff_seconds: float,
        retry_jitter_ratio: float,
    ) -> None:
        self.repository = repository
        self.file_store = file_store
        self.upstream_gateway = upstream_gateway
        self.run_lock = run_lock
        self.cooldown_policy = cooldown_policy
        self.downloads_dir = downloads_dir
        self.db_path = db_path
        self.retry_attempts = max(1, int(retry_attempts))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self.retry_jitter_ratio = max(0.0, float(retry_jitter_ratio))
        self._random = random.Random()

    def _fallback_status_snapshot(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = self.repository.default_state()
        if isinstance(state, dict):
            payload.update(state)
        payload["downloads_dir"] = str(self.downloads_dir)
        payload["recommended_scheduler_entrypoint"] = "run_download_latest_once.py"
        payload["manual_sync_note"] = "常规定时同步请固定调用 run_download_latest_once.py。"
        payload["last_success_at"] = None
        payload["last_success_age_seconds"] = None
        payload["last_success_outcome"] = None
        payload["last_run_summary"] = None
        payload["consecutive_failure_count"] = 0
        payload["failure_cooldown_until"] = None
        payload["failure_cooldown_remaining_seconds"] = None
        return payload

    def _safe_build_status(self, state: dict[str, Any] | None = None, *, phase: str) -> dict[str, Any]:
        try:
            return self.repository.build_status(state)
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

    def _run_with_retries(
        self,
        *,
        trigger_source: str,
        job_run_id: int | None = None,
    ) -> tuple[RemoteRecord, dict[str, Any], int]:
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
                latest_remote, result = self.upstream_gateway.run_latest_attempt()
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

    def execute(self, trigger_source: str = "manual") -> dict[str, Any]:
        run_lock_handle = self.run_lock.acquire()
        started_at = iso_now()
        state_running_marked = False
        cleanup_state: dict[str, Any] | None = None
        job_run_id: int | None = None
        checked_at = started_at
        latest_remote: RemoteRecord | None = None
        result: dict[str, Any] | None = None
        attempts = 0
        error_payload: dict[str, Any] | None = None
        primary_exception: Exception | None = None
        return_status: dict[str, Any] | None = None

        try:
            job_run_id = self.repository.create_job_run(
                trigger_source=trigger_source,
                started_at=started_at,
                status=self.repository.build_status(),
            )

            self.cooldown_policy.ensure_not_active()

            state = self.repository.load_state()
            state["running"] = True
            self.repository.write_state(state)
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
            latest_remote, result, attempts = self._run_with_retries(
                trigger_source=trigger_source,
                job_run_id=job_run_id,
            )

            state = self.repository.load_state()
            state["last_checked_at"] = checked_at
            state["last_action"] = result["action"]
            state["latest_remote"] = result["latest_remote"]
            state["last_download"] = result["last_download"]
            state["last_error"] = None
            self.repository.upsert_history_entry(
                state,
                self.file_store.enrich_with_local_state(latest_remote, status="downloaded"),
            )
            self.repository.write_state(state)
        except Exception as exc:  # noqa: BLE001
            primary_exception = exc
            attempts = max(attempts, int(getattr(exc, "attempts", 0) or 0))
            error_payload = self.repository.serialize_error(exc)
            if state_running_marked:
                try:
                    state = self.repository.load_state()
                    state["last_checked_at"] = checked_at
                    state["last_action"] = "error"
                    if latest_remote is not None:
                        state["latest_remote"] = self.file_store.enrich_with_local_state(latest_remote)
                        state["last_download"] = self.file_store.enrich_with_local_state(latest_remote, status="error")
                    state["last_error"] = error_payload
                    self.repository.write_state(state)
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
                        self.repository.load_state,
                        step="running_reset_load_state",
                        suppress_errors=suppress_cleanup_errors,
                    )
                    if isinstance(cleanup_state, dict):
                        cleanup_state["running"] = False
                        self._run_cleanup_step(
                            lambda: self.repository.write_state(cleanup_state),
                            step="running_reset_write_state",
                            suppress_errors=suppress_cleanup_errors,
                        )
                final_outcome = str(result["action"]) if result is not None else "error"
                finished_at = iso_now()
                self._run_cleanup_step(
                    lambda: self.cooldown_policy.apply(error_payload),
                    step="apply_failure_cooldown",
                    suppress_errors=suppress_cleanup_errors,
                )
                final_status = self._safe_build_status(
                    cleanup_state,
                    phase="before_job_run_finalize",
                )
                final_latest_remote = result["latest_remote"] if result is not None else (
                    self.file_store.enrich_with_local_state(latest_remote) if latest_remote is not None else None
                )
                final_last_download = result["last_download"] if result is not None else (
                    self.file_store.enrich_with_local_state(latest_remote, status="error")
                    if latest_remote is not None
                    else None
                )
                if job_run_id is not None:
                    self._run_cleanup_step(
                        lambda: self.repository.finalize_job_run(
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
                return_status = self._safe_build_status(
                    cleanup_state,
                    phase="after_job_run_finalize",
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
                    duration_ms=self.repository.duration_millis(started_at, finished_at),
                    error_code=(error_payload or {}).get("code"),
                )
            finally:
                self.run_lock.release(run_lock_handle)

        if result is None:
            raise DownloadError("下载任务未返回结果。")

        return {
            "action": result["action"],
            "status": return_status or self._fallback_status_snapshot(),
            "summary": self.repository.build_job_run_summary(
                outcome=str(result["action"]),
                trigger_source=trigger_source,
                latest_remote=result["latest_remote"],
                error=None,
            ),
            "latest_remote": result["latest_remote"],
            "last_download": result["last_download"],
        }
