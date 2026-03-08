#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from core.common import PUBLIC_ERROR_MESSAGES, DownloadError, error_hint_for_code, iso_now
from core.runtime_security import secure_runtime_artifacts
from storage import job_run_repository
from storage import runtime_cache_repository
from storage import sqlite_connection
from storage import state_repair_service
from storage import state_repository
from storage import status_projection


class DownloaderStorageMixin:
    _FAILURE_COOLDOWN_CACHE_KEY = "sync_failure_cooldown"
    iso_now = staticmethod(iso_now)

    def ensure_layout(self) -> None:
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.partial_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self._secure_runtime_artifacts()

    def default_state(self) -> dict[str, Any]:
        return {
            "running": False,
            "last_checked_at": None,
            "last_action": None,
            "latest_remote": None,
            "last_download": None,
            "last_error": None,
            "download_history": [],
        }

    def _connect_db_unlocked(self):
        return sqlite_connection.connect_db_unlocked(self)

    def _secure_runtime_artifacts(self) -> None:
        secure_runtime_artifacts(
            runtime_dir=self.runtime_dir,
            db_path=self.db_path,
            lock_path=getattr(self, "lock_path", None),
            state_path=getattr(self, "state_path", None),
        )

    def _initialize_db_unlocked(self, connection) -> None:
        sqlite_connection.initialize_db_unlocked(self, connection)

    def _migrate_schema_unlocked(self, connection) -> None:
        sqlite_connection.migrate_schema_unlocked(self, connection)

    def _parse_json_text(self, value: str | None) -> dict[str, Any] | None:
        text = str(value or "").strip()
        if not text:
            return None

        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise DownloadError(
                "SQLite 状态数据损坏，无法解析 JSON。",
                code="internal_error",
                public_message=PUBLIC_ERROR_MESSAGES["internal_error"],
            ) from exc

        return parsed if isinstance(parsed, dict) else None

    def _duration_millis(self, started_at: str, finished_at: str) -> int | None:
        try:
            started = datetime.fromisoformat(started_at)
            finished = datetime.fromisoformat(finished_at)
        except ValueError:
            return None

        return max(0, int((finished - started).total_seconds() * 1000))

    def _age_seconds(self, value: str | None) -> int | None:
        text = str(value or "").strip()
        if not text:
            return None

        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None

        if parsed.tzinfo is None:
            parsed = parsed.astimezone()
        return max(0, int((datetime.now().astimezone() - parsed).total_seconds()))

    def _seconds_until(self, value: str | None) -> int | None:
        text = str(value or "").strip()
        if not text:
            return None

        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None

        if parsed.tzinfo is None:
            parsed = parsed.astimezone()
        return max(0, int((parsed - datetime.now().astimezone()).total_seconds()))

    def _is_cache_expired(self, expires_at: str | None) -> bool:
        value = str(expires_at or "").strip()
        if not value:
            return False

        try:
            expires_at_dt = datetime.fromisoformat(value)
        except ValueError:
            return True

        if expires_at_dt.tzinfo is None:
            expires_at_dt = expires_at_dt.astimezone()

        return expires_at_dt <= datetime.now().astimezone()

    def _create_job_run_unlocked(
        self,
        connection,
        *,
        trigger_source: str,
        started_at: str,
        status: dict[str, Any],
    ) -> int:
        return job_run_repository.create_job_run_unlocked(
            self,
            connection,
            trigger_source=trigger_source,
            started_at=started_at,
            status=status,
        )

    def _finalize_job_run_unlocked(
        self,
        connection,
        *,
        job_run_id: int,
        checked_at: str,
        finished_at: str,
        outcome: str,
        attempts: int,
        latest_remote: dict[str, Any] | None,
        last_download: dict[str, Any] | None,
        status: dict[str, Any],
        error: dict[str, Any] | None,
    ) -> None:
        job_run_repository.finalize_job_run_unlocked(
            self,
            connection,
            job_run_id=job_run_id,
            checked_at=checked_at,
            finished_at=finished_at,
            outcome=outcome,
            attempts=attempts,
            latest_remote=latest_remote,
            last_download=last_download,
            status=status,
            error=error,
        )

    def _deserialize_job_run_row(self, row) -> dict[str, Any]:
        return job_run_repository.deserialize_job_run_row(self, row)

    def _build_job_run_summary(
        self,
        *,
        outcome: str,
        trigger_source: str,
        latest_remote: dict[str, Any] | None,
        error: dict[str, Any] | None,
    ) -> str:
        return job_run_repository.build_job_run_summary(
            self,
            outcome=outcome,
            trigger_source=trigger_source,
            latest_remote=latest_remote,
            error=error,
        )

    def _read_runtime_cache_unlocked(self, connection, *, cache_key: str) -> dict[str, Any] | None:
        return runtime_cache_repository.read_runtime_cache_unlocked(self, connection, cache_key=cache_key)

    def _write_runtime_cache_unlocked(
        self,
        connection,
        *,
        cache_key: str,
        value: dict[str, Any],
        expires_at: str | None,
    ) -> None:
        runtime_cache_repository.write_runtime_cache_unlocked(
            self,
            connection,
            cache_key=cache_key,
            value=value,
            expires_at=expires_at,
        )

    def _delete_runtime_cache_unlocked(self, connection, *, cache_key: str) -> None:
        runtime_cache_repository.delete_runtime_cache_unlocked(self, connection, cache_key=cache_key)

    def _load_legacy_state_file_unlocked(self) -> dict[str, Any] | None:
        return state_repository.load_legacy_state_file_unlocked(self)

    def _write_state_to_db_unlocked(self, connection, state: dict[str, Any]) -> dict[str, Any]:
        return state_repository.write_state_to_db_unlocked(self, connection, state)

    def _read_state_from_db_unlocked(self, connection) -> dict[str, Any] | None:
        return state_repository.read_state_from_db_unlocked(self, connection)

    def _migrate_legacy_state_if_needed_unlocked(self, connection) -> None:
        state_repository.migrate_legacy_state_if_needed_unlocked(self, connection)

    def load_state(self) -> dict[str, Any]:
        return state_repository.load_state(self)

    def write_state(self, state: dict[str, Any]) -> dict[str, Any]:
        return state_repository.write_state(self, state)

    def create_job_run(
        self,
        *,
        trigger_source: str,
        started_at: str,
        status: dict[str, Any],
    ) -> int:
        return job_run_repository.create_job_run(
            self,
            trigger_source=trigger_source,
            started_at=started_at,
            status=status,
        )

    def finalize_job_run(
        self,
        *,
        job_run_id: int,
        checked_at: str,
        finished_at: str,
        outcome: str,
        attempts: int,
        latest_remote: dict[str, Any] | None,
        last_download: dict[str, Any] | None,
        status: dict[str, Any],
        error: dict[str, Any] | None,
    ) -> None:
        job_run_repository.finalize_job_run(
            self,
            job_run_id=job_run_id,
            checked_at=checked_at,
            finished_at=finished_at,
            outcome=outcome,
            attempts=attempts,
            latest_remote=latest_remote,
            last_download=last_download,
            status=status,
            error=error,
        )

    def list_job_runs(self, limit: int = 20, offset: int = 0) -> dict[str, Any]:
        return job_run_repository.list_job_runs(self, limit=limit, offset=offset)

    def get_failure_cooldown_snapshot(self) -> dict[str, Any]:
        return runtime_cache_repository.get_failure_cooldown_snapshot(self)

    def set_failure_cooldown(
        self,
        *,
        until: str,
        error_code: str,
        message: str,
        retryable: bool,
    ) -> None:
        runtime_cache_repository.set_failure_cooldown(
            self,
            until=until,
            error_code=error_code,
            message=message,
            retryable=retryable,
        )

    def clear_failure_cooldown(self) -> None:
        runtime_cache_repository.clear_failure_cooldown(self)

    def get_sync_audit_snapshot(self) -> dict[str, Any]:
        return status_projection.get_sync_audit_snapshot(self)

    def load_runtime_cache(self, cache_key: str) -> dict[str, Any] | None:
        return runtime_cache_repository.load_runtime_cache(self, cache_key)

    def write_runtime_cache(
        self,
        cache_key: str,
        value: dict[str, Any],
        *,
        expires_at: str | None = None,
    ) -> None:
        runtime_cache_repository.write_runtime_cache(self, cache_key, value, expires_at=expires_at)

    def delete_runtime_cache(self, cache_key: str) -> None:
        runtime_cache_repository.delete_runtime_cache(self, cache_key)

    def reset_running_flag(self) -> dict[str, Any]:
        return state_repository.reset_running_flag(self)

    def _build_status_state_for_read(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        return status_projection.build_status_state_for_read(self, state)

    def build_status(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        return status_projection.build_status(self, state)

    def _serialize_error(self, exc: Exception) -> dict[str, Any]:
        if isinstance(exc, DownloadError):
            return {
                "code": exc.code,
                "message": str(exc),
                "public_message": exc.public_message,
                "hint": error_hint_for_code(exc.code),
                "retryable": exc.retryable,
                "at": iso_now(),
            }

        return {
            "code": "internal_error",
            "message": str(exc),
            "public_message": PUBLIC_ERROR_MESSAGES["internal_error"],
            "hint": error_hint_for_code("internal_error"),
            "retryable": False,
            "at": iso_now(),
        }

    def _resolve_entry_path(self, entry: dict[str, Any]):
        return state_repair_service.resolve_entry_path(self, entry)

    def _record_uses_local_file(self, record: dict[str, Any] | None) -> bool:
        return state_repair_service.record_uses_local_file(self, record)

    def _normalize_state_record(
        self,
        record: dict[str, Any] | None,
        *,
        require_local_file: bool = False,
    ) -> dict[str, Any] | None:
        return state_repair_service.normalize_state_record(
            self,
            record,
            require_local_file=require_local_file,
        )

    def _state_reconciliation_fields(self, state: dict[str, Any]) -> dict[str, Any]:
        return state_repair_service.state_reconciliation_fields(self, state)

    def _history_date_or_min(self, value: str):
        return state_repair_service.history_date_or_min(self, value)

    def _seed_history_entries(self, state: dict[str, Any]) -> list[dict[str, Any]]:
        return state_repair_service.seed_history_entries(self, state)

    def _upsert_history_entry(self, state: dict[str, Any], entry: dict[str, Any]) -> None:
        state_repair_service.upsert_history_entry(self, state, entry)

    def _list_cached_downloaded_entries(self, state: dict[str, Any]) -> list[dict[str, Any]]:
        return state_repair_service.list_cached_downloaded_entries(self, state)

    def _select_latest_downloaded_entry(self, state: dict[str, Any]) -> dict[str, Any] | None:
        return state_repair_service.select_latest_downloaded_entry(self, state)

    def _select_public_state_records(
        self,
        state: dict[str, Any],
        *,
        latest_downloaded: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        return state_repair_service.select_public_state_records(
            self,
            state,
            latest_downloaded=latest_downloaded,
        )

    def _reconcile_state_with_disk(self, state: dict[str, Any]) -> int:
        return state_repair_service.reconcile_state_with_disk(self, state)

    def repair_download_history_from_disk(self, *, if_missing_only: bool = False) -> int:
        return state_repair_service.repair_download_history_from_disk(self, if_missing_only=if_missing_only)

    def _history_sort_key(self, entry: dict[str, Any]):
        return state_repair_service.history_sort_key(self, entry)
