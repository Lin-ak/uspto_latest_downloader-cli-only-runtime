#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

from core.common import (
    PUBLIC_ERROR_MESSAGES,
    DownloadError,
    error_hint_for_code,
    iso_now,
    normalize_download_url,
    normalize_release_datetime_text,
    parse_iso_date,
    parse_release_datetime,
)
from core.contract import (
    SYNC_LATEST_FILE_OPERATION,
    SYNC_LATEST_FILE_RECOMMENDED_ENTRYPOINT,
    SYNC_LATEST_FILE_RESOURCE,
    SYNC_LATEST_FILE_TRIGGER_POLICY,
)


class DownloaderStorageMixin:
    _FAILURE_COOLDOWN_CACHE_KEY = "sync_failure_cooldown"

    def ensure_layout(self) -> None:
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.partial_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)

    def default_state(self) -> Dict[str, Any]:
        return {
            "running": False,
            "last_checked_at": None,
            "last_action": None,
            "latest_remote": None,
            "last_download": None,
            "last_error": None,
            "download_history": [],
        }

    def _connect_db_unlocked(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    def _initialize_db_unlocked(self, connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS service_state (
                singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                running INTEGER NOT NULL DEFAULT 0,
                last_checked_at TEXT,
                last_action TEXT,
                latest_remote_json TEXT,
                last_download_json TEXT,
                last_error_json TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS download_history (
                file_name TEXT PRIMARY KEY,
                entry_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS job_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,
                resource TEXT NOT NULL,
                trigger_source TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                checked_at TEXT,
                outcome TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                duration_ms INTEGER,
                summary_text TEXT,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                latest_remote_json TEXT,
                last_download_json TEXT,
                status_json TEXT,
                error_code TEXT,
                error_message TEXT,
                error_public_message TEXT,
                retryable INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runtime_cache (
                cache_key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                expires_at TEXT,
                updated_at TEXT NOT NULL
            );
            """
        )
        self._migrate_schema_unlocked(connection)

    def _migrate_schema_unlocked(self, connection: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(job_runs)").fetchall()
        }
        if "summary_text" not in existing_columns:
            connection.execute("ALTER TABLE job_runs ADD COLUMN summary_text TEXT")
        if "consecutive_failures" not in existing_columns:
            connection.execute("ALTER TABLE job_runs ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0")

    def _parse_json_text(self, value: str | None) -> Dict[str, Any] | None:
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
        connection: sqlite3.Connection,
        *,
        trigger_source: str,
        started_at: str,
        status: Dict[str, Any],
    ) -> int:
        created_at = iso_now()
        cursor = connection.execute(
            """
            INSERT INTO job_runs (
                operation,
                resource,
                trigger_source,
                started_at,
                status_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                SYNC_LATEST_FILE_OPERATION,
                SYNC_LATEST_FILE_RESOURCE,
                trigger_source,
                started_at,
                json.dumps(status, ensure_ascii=False),
                created_at,
                created_at,
            ),
        )
        return int(cursor.lastrowid)

    def _finalize_job_run_unlocked(
        self,
        connection: sqlite3.Connection,
        *,
        job_run_id: int,
        checked_at: str,
        finished_at: str,
        outcome: str,
        attempts: int,
        latest_remote: Dict[str, Any] | None,
        last_download: Dict[str, Any] | None,
        status: Dict[str, Any],
        error: Dict[str, Any] | None,
    ) -> None:
        started_row = connection.execute(
            "SELECT started_at FROM job_runs WHERE id = ?",
            (job_run_id,),
        ).fetchone()
        previous_row = connection.execute(
            """
            SELECT outcome, consecutive_failures
            FROM job_runs
            WHERE id < ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (job_run_id,),
        ).fetchone()
        previous_consecutive_failures = (
            int(previous_row["consecutive_failures"] or 0)
            if previous_row is not None and str(previous_row["outcome"] or "").strip() == "error"
            else 0
        )
        consecutive_failures = previous_consecutive_failures + 1 if outcome == "error" else 0
        summary_text = self._build_job_run_summary(
            outcome=outcome,
            trigger_source=str(
                connection.execute(
                    "SELECT trigger_source FROM job_runs WHERE id = ?",
                    (job_run_id,),
                ).fetchone()["trigger_source"]
            ),
            latest_remote=latest_remote,
            error=error,
        )
        connection.execute(
            """
            UPDATE job_runs
            SET
                checked_at = ?,
                finished_at = ?,
                outcome = ?,
                attempts = ?,
                duration_ms = ?,
                summary_text = ?,
                consecutive_failures = ?,
                latest_remote_json = ?,
                last_download_json = ?,
                status_json = ?,
                error_code = ?,
                error_message = ?,
                error_public_message = ?,
                retryable = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                checked_at,
                finished_at,
                outcome,
                attempts,
                self._duration_millis(
                    started_row["started_at"],
                    finished_at,
                ),
                summary_text,
                consecutive_failures,
                json.dumps(latest_remote, ensure_ascii=False) if latest_remote is not None else None,
                json.dumps(last_download, ensure_ascii=False) if last_download is not None else None,
                json.dumps(status, ensure_ascii=False),
                str((error or {}).get("code", "")).strip() or None,
                str((error or {}).get("message", "")).strip() or None,
                str((error or {}).get("public_message", "")).strip() or None,
                int(bool((error or {}).get("retryable", False))),
                finished_at,
                job_run_id,
            ),
        )

    def _deserialize_job_run_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        error_code = row["error_code"]
        return {
            "id": int(row["id"]),
            "operation": str(row["operation"]),
            "resource": str(row["resource"]),
            "trigger_source": str(row["trigger_source"]),
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "checked_at": row["checked_at"],
            "outcome": row["outcome"],
            "attempts": int(row["attempts"] or 0),
            "duration_ms": int(row["duration_ms"]) if row["duration_ms"] is not None else None,
            "summary": row["summary_text"],
            "consecutive_failures": int(row["consecutive_failures"] or 0),
            "latest_remote": self._parse_json_text(row["latest_remote_json"]),
            "last_download": self._parse_json_text(row["last_download_json"]),
            "status": self._parse_json_text(row["status_json"]),
            "error": {
                "code": error_code,
                "message": row["error_message"],
                "public_message": row["error_public_message"],
                "hint": error_hint_for_code(error_code),
                "retryable": bool(row["retryable"]),
            }
            if row["error_code"] or row["error_message"] or row["error_public_message"]
            else None,
        }

    def _build_job_run_summary(
        self,
        *,
        outcome: str,
        trigger_source: str,
        latest_remote: Dict[str, Any] | None,
        error: Dict[str, Any] | None,
    ) -> str:
        source_label = str(trigger_source or "-").strip() or "-"
        if outcome == "downloaded":
            file_name = str((latest_remote or {}).get("file_name", "")).strip() or "最新 ZIP"
            return f"{source_label} 触发：已下载 {file_name}"
        if outcome == "skipped":
            file_name = str((latest_remote or {}).get("file_name", "")).strip() or "最新 ZIP"
            return f"{source_label} 触发：本地已是最新，跳过 {file_name}"
        if outcome == "error":
            error_code = str((error or {}).get("code", "")).strip() or "internal_error"
            error_message = (
                str((error or {}).get("public_message", "")).strip()
                or str((error or {}).get("message", "")).strip()
                or PUBLIC_ERROR_MESSAGES["internal_error"]
            )
            return f"{source_label} 触发：{error_code} · {error_message}"
        return f"{source_label} 触发：{outcome or '未知结果'}"

    def _read_runtime_cache_unlocked(
        self,
        connection: sqlite3.Connection,
        *,
        cache_key: str,
    ) -> Dict[str, Any] | None:
        row = connection.execute(
            """
            SELECT value_json, expires_at
            FROM runtime_cache
            WHERE cache_key = ?
            """,
            (cache_key,),
        ).fetchone()
        if row is None:
            return None

        if self._is_cache_expired(row["expires_at"]):
            connection.execute("DELETE FROM runtime_cache WHERE cache_key = ?", (cache_key,))
            return None

        value = self._parse_json_text(row["value_json"])
        return value if isinstance(value, dict) else None

    def _write_runtime_cache_unlocked(
        self,
        connection: sqlite3.Connection,
        *,
        cache_key: str,
        value: Dict[str, Any],
        expires_at: str | None,
    ) -> None:
        updated_at = iso_now()
        connection.execute(
            """
            INSERT INTO runtime_cache (
                cache_key,
                value_json,
                expires_at,
                updated_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                value_json = excluded.value_json,
                expires_at = excluded.expires_at,
                updated_at = excluded.updated_at
            """,
            (
                cache_key,
                json.dumps(value, ensure_ascii=False),
                expires_at,
                updated_at,
            ),
        )

    def _delete_runtime_cache_unlocked(
        self,
        connection: sqlite3.Connection,
        *,
        cache_key: str,
    ) -> None:
        connection.execute("DELETE FROM runtime_cache WHERE cache_key = ?", (cache_key,))

    def _load_legacy_state_file_unlocked(self) -> Dict[str, Any] | None:
        if not self.state_path.exists():
            return None

        with self.state_path.open("r", encoding="utf-8") as file:
            payload = json.load(file)

        if not isinstance(payload, dict):
            raise DownloadError("runtime/state.json 顶层不是对象。")

        state = self.default_state()
        state.update(payload)
        return state

    def _write_state_to_db_unlocked(self, connection: sqlite3.Connection, state: Dict[str, Any]) -> Dict[str, Any]:
        normalized = self.default_state()
        normalized.update(state)

        history = normalized.get("download_history", [])
        if not isinstance(history, list):
            history = []
        normalized["download_history"] = [item for item in history if isinstance(item, dict)]

        updated_at = iso_now()
        connection.execute(
            """
            INSERT INTO service_state (
                singleton_id,
                running,
                last_checked_at,
                last_action,
                latest_remote_json,
                last_download_json,
                last_error_json,
                updated_at
            ) VALUES (1, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(singleton_id) DO UPDATE SET
                running = excluded.running,
                last_checked_at = excluded.last_checked_at,
                last_action = excluded.last_action,
                latest_remote_json = excluded.latest_remote_json,
                last_download_json = excluded.last_download_json,
                last_error_json = excluded.last_error_json,
                updated_at = excluded.updated_at
            """,
            (
                int(bool(normalized.get("running"))),
                normalized.get("last_checked_at"),
                normalized.get("last_action"),
                json.dumps(normalized.get("latest_remote"), ensure_ascii=False)
                if normalized.get("latest_remote") is not None
                else None,
                json.dumps(normalized.get("last_download"), ensure_ascii=False)
                if normalized.get("last_download") is not None
                else None,
                json.dumps(normalized.get("last_error"), ensure_ascii=False)
                if normalized.get("last_error") is not None
                else None,
                updated_at,
            ),
        )

        connection.execute("DELETE FROM download_history")
        for entry in normalized["download_history"]:
            file_name = str(entry.get("file_name", "")).strip()
            if not file_name:
                continue
            connection.execute(
                """
                INSERT INTO download_history (file_name, entry_json, updated_at)
                VALUES (?, ?, ?)
                """,
                (
                    file_name,
                    json.dumps(entry, ensure_ascii=False),
                    updated_at,
                ),
            )

        return normalized

    def _read_state_from_db_unlocked(self, connection: sqlite3.Connection) -> Dict[str, Any] | None:
        row = connection.execute(
            """
            SELECT
                running,
                last_checked_at,
                last_action,
                latest_remote_json,
                last_download_json,
                last_error_json
            FROM service_state
            WHERE singleton_id = 1
            """
        ).fetchone()
        if row is None:
            return None

        state = self.default_state()
        state["running"] = bool(row["running"])
        state["last_checked_at"] = row["last_checked_at"]
        state["last_action"] = row["last_action"]
        state["latest_remote"] = self._parse_json_text(row["latest_remote_json"])
        state["last_download"] = self._parse_json_text(row["last_download_json"])
        state["last_error"] = self._parse_json_text(row["last_error_json"])

        history_rows = connection.execute(
            """
            SELECT entry_json
            FROM download_history
            """
        ).fetchall()
        history: List[Dict[str, Any]] = []
        for history_row in history_rows:
            entry = self._parse_json_text(history_row["entry_json"])
            if isinstance(entry, dict):
                history.append(entry)
        history.sort(key=self._history_sort_key, reverse=True)
        state["download_history"] = history
        return state

    def _migrate_legacy_state_if_needed_unlocked(self, connection: sqlite3.Connection) -> None:
        current_state = self._read_state_from_db_unlocked(connection)
        if current_state is not None:
            return

        legacy_state = self._load_legacy_state_file_unlocked()
        if legacy_state is None:
            self._write_state_to_db_unlocked(connection, self.default_state())
            return

        self._write_state_to_db_unlocked(connection, legacy_state)
        migrated_path = self.state_path.with_suffix(".json.migrated")
        self.state_path.replace(migrated_path)

    def load_state(self) -> Dict[str, Any]:
        self.ensure_layout()
        with self._state_lock:
            with closing(self._connect_db_unlocked()) as connection:
                self._initialize_db_unlocked(connection)
                self._migrate_legacy_state_if_needed_unlocked(connection)
                state = self._read_state_from_db_unlocked(connection)
                if state is None:
                    state = self._write_state_to_db_unlocked(connection, self.default_state())
                connection.commit()
                return state

    def write_state(self, state: Dict[str, Any]) -> Dict[str, Any]:
        self.ensure_layout()
        with self._state_lock:
            with closing(self._connect_db_unlocked()) as connection:
                self._initialize_db_unlocked(connection)
                normalized = self._write_state_to_db_unlocked(connection, state)
                connection.commit()
                return normalized

    def create_job_run(
        self,
        *,
        trigger_source: str,
        started_at: str,
        status: Dict[str, Any],
    ) -> int:
        self.ensure_layout()
        with self._state_lock:
            with closing(self._connect_db_unlocked()) as connection:
                self._initialize_db_unlocked(connection)
                job_run_id = self._create_job_run_unlocked(
                    connection,
                    trigger_source=trigger_source,
                    started_at=started_at,
                    status=status,
                )
                connection.commit()
                return job_run_id

    def finalize_job_run(
        self,
        *,
        job_run_id: int,
        checked_at: str,
        finished_at: str,
        outcome: str,
        attempts: int,
        latest_remote: Dict[str, Any] | None,
        last_download: Dict[str, Any] | None,
        status: Dict[str, Any],
        error: Dict[str, Any] | None,
    ) -> None:
        self.ensure_layout()
        with self._state_lock:
            with closing(self._connect_db_unlocked()) as connection:
                self._initialize_db_unlocked(connection)
                self._finalize_job_run_unlocked(
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
                connection.commit()

    def list_job_runs(self, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        self.ensure_layout()
        with self._state_lock:
            with closing(self._connect_db_unlocked()) as connection:
                self._initialize_db_unlocked(connection)
                total_row = connection.execute("SELECT COUNT(*) AS count FROM job_runs").fetchone()
                rows = connection.execute(
                    """
                    SELECT *
                    FROM job_runs
                    ORDER BY id DESC
                    LIMIT ? OFFSET ?
                    """,
                    (limit, offset),
                ).fetchall()
                connection.commit()

        job_runs = [self._deserialize_job_run_row(row) for row in rows]
        return {
            "job_runs": job_runs,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total": int((total_row or {"count": 0})["count"]),
                "count": len(job_runs),
            },
        }

    def get_failure_cooldown_snapshot(self) -> Dict[str, Any]:
        payload = self.load_runtime_cache(self._FAILURE_COOLDOWN_CACHE_KEY)
        if not isinstance(payload, dict):
            return {
                "until": None,
                "remaining_seconds": None,
                "error_code": None,
                "message": None,
                "retryable": False,
            }

        until = str(payload.get("until", "")).strip() or None
        return {
            "until": until,
            "remaining_seconds": self._seconds_until(until),
            "error_code": str(payload.get("error_code", "")).strip() or None,
            "message": str(payload.get("message", "")).strip() or None,
            "retryable": bool(payload.get("retryable", False)),
        }

    def set_failure_cooldown(
        self,
        *,
        until: str,
        error_code: str,
        message: str,
        retryable: bool,
    ) -> None:
        self.write_runtime_cache(
            self._FAILURE_COOLDOWN_CACHE_KEY,
            {
                "until": until,
                "error_code": error_code,
                "message": message,
                "retryable": retryable,
            },
            expires_at=until,
        )

    def clear_failure_cooldown(self) -> None:
        self.delete_runtime_cache(self._FAILURE_COOLDOWN_CACHE_KEY)

    def _build_sync_audit_snapshot_unlocked(self, connection: sqlite3.Connection) -> Dict[str, Any]:
        latest_run = connection.execute(
            """
            SELECT outcome, summary_text, consecutive_failures, finished_at, started_at
            FROM job_runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        last_success = connection.execute(
            """
            SELECT finished_at, started_at, outcome
            FROM job_runs
            WHERE outcome IN ('downloaded', 'skipped')
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        cooldown_payload = self._read_runtime_cache_unlocked(
            connection,
            cache_key=self._FAILURE_COOLDOWN_CACHE_KEY,
        )
        cooldown_until = (
            str((cooldown_payload or {}).get("until", "")).strip() or None
        )
        cooldown_remaining_seconds = self._seconds_until(cooldown_until)
        if cooldown_until and cooldown_remaining_seconds == 0:
            cooldown_until = None

        last_success_at = None
        last_success_outcome = None
        if last_success is not None:
            last_success_at = last_success["finished_at"] or last_success["started_at"]
            last_success_outcome = last_success["outcome"]

        return {
            "last_success_at": last_success_at,
            "last_success_age_seconds": self._age_seconds(last_success_at),
            "last_success_outcome": last_success_outcome,
            "last_run_summary": latest_run["summary_text"] if latest_run is not None else None,
            "consecutive_failure_count": int(latest_run["consecutive_failures"] or 0) if latest_run is not None else 0,
            "failure_cooldown_until": cooldown_until,
            "failure_cooldown_remaining_seconds": cooldown_remaining_seconds,
        }

    def get_sync_audit_snapshot(self) -> Dict[str, Any]:
        self.ensure_layout()
        with self._state_lock:
            with closing(self._connect_db_unlocked()) as connection:
                self._initialize_db_unlocked(connection)
                snapshot = self._build_sync_audit_snapshot_unlocked(connection)
                connection.commit()
                return snapshot

    def load_runtime_cache(self, cache_key: str) -> Dict[str, Any] | None:
        self.ensure_layout()
        with self._state_lock:
            with closing(self._connect_db_unlocked()) as connection:
                self._initialize_db_unlocked(connection)
                value = self._read_runtime_cache_unlocked(connection, cache_key=cache_key)
                connection.commit()
                return value

    def write_runtime_cache(
        self,
        cache_key: str,
        value: Dict[str, Any],
        *,
        expires_at: str | None = None,
    ) -> None:
        self.ensure_layout()
        with self._state_lock:
            with closing(self._connect_db_unlocked()) as connection:
                self._initialize_db_unlocked(connection)
                self._write_runtime_cache_unlocked(
                    connection,
                    cache_key=cache_key,
                    value=value,
                    expires_at=expires_at,
                )
                connection.commit()

    def delete_runtime_cache(self, cache_key: str) -> None:
        self.ensure_layout()
        with self._state_lock:
            with closing(self._connect_db_unlocked()) as connection:
                self._initialize_db_unlocked(connection)
                self._delete_runtime_cache_unlocked(connection, cache_key=cache_key)
                connection.commit()

    def reset_running_flag(self) -> Dict[str, Any]:
        state = self.load_state()
        if state.get("running"):
            state["running"] = False
            self.write_state(state)
        return state

    def _build_status_state_for_read(self, state: Dict[str, Any] | None = None) -> Dict[str, Any]:
        payload = self.default_state()
        payload.update(state or self.load_state())

        if state is None:
            latest_remote, last_download = self._select_public_status_records_for_read(payload)
            download_history = self._list_downloaded_entries()
        else:
            latest_remote, last_download = self._select_public_state_records(
                payload,
                latest_downloaded=self._select_latest_downloaded_entry(payload),
            )
            download_history = self._list_cached_downloaded_entries(payload)

        payload["latest_remote"] = latest_remote
        payload["last_download"] = last_download
        payload["download_history"] = download_history
        return payload

    def build_status(self, state: Dict[str, Any] | None = None) -> Dict[str, Any]:
        payload = self._build_status_state_for_read(state)
        payload["downloads_dir"] = str(self.downloads_dir)
        payload["recommended_scheduler_entrypoint"] = SYNC_LATEST_FILE_RECOMMENDED_ENTRYPOINT
        payload["manual_sync_note"] = SYNC_LATEST_FILE_TRIGGER_POLICY["note"]
        payload.update(self.get_sync_audit_snapshot())
        return payload

    def build_public_status(self) -> Dict[str, Any]:
        state = self.load_state()
        latest_remote, last_download = self._select_public_status_records_for_read(state)
        snapshot = self.get_sync_audit_snapshot()
        return {
            "running": bool(state.get("running")),
            "last_checked_at": state.get("last_checked_at"),
            "last_action": state.get("last_action"),
            "latest_remote": self.sanitize_public_record(latest_remote),
            "last_download": self.sanitize_public_record(last_download),
            "last_error": self._sanitize_public_error(state.get("last_error")),
            "last_success_at": snapshot["last_success_at"],
            "last_success_age_seconds": snapshot["last_success_age_seconds"],
            "last_success_outcome": snapshot["last_success_outcome"],
            "last_run_summary": snapshot["last_run_summary"],
            "consecutive_failure_count": snapshot["consecutive_failure_count"],
            "failure_cooldown_until": snapshot["failure_cooldown_until"],
            "failure_cooldown_remaining_seconds": snapshot["failure_cooldown_remaining_seconds"],
        }

    def sanitize_public_record(self, record: Dict[str, Any] | None) -> Dict[str, Any] | None:
        if not isinstance(record, dict):
            return None

        file_name = str(record.get("file_name", "")).strip()
        if not file_name:
            return None

        return {
            "file_name": file_name,
            "official_data_date": str(record.get("official_data_date", "")).strip(),
            "release_date_raw": normalize_release_datetime_text(str(record.get("release_date_raw", "")).strip()),
            "file_size_bytes": int(record.get("file_size_bytes", 0) or 0),
            "download_url": normalize_download_url(str(record.get("download_url", "")).strip()),
            "downloaded_at": str(record.get("downloaded_at", "")).strip(),
            "status": str(record.get("status", "")).strip() or "available",
        }

    def _sanitize_public_error(self, error: Dict[str, Any] | None) -> Dict[str, Any] | None:
        if not isinstance(error, dict):
            return None

        code = str(error.get("code", "")).strip() or "internal_error"
        public_message = str(error.get("public_message", "")).strip() or PUBLIC_ERROR_MESSAGES.get(
            code,
            PUBLIC_ERROR_MESSAGES["internal_error"],
        )
        if not public_message:
            return None

        return {
            "code": code,
            "message": public_message,
            "hint": error_hint_for_code(code),
            "at": str(error.get("at", "")).strip() or None,
            "retryable": bool(error.get("retryable", False)),
        }

    def _serialize_error(self, exc: Exception) -> Dict[str, Any]:
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

    def _resolve_entry_path(self, entry: Dict[str, Any]) -> Path | None:
        file_name = str(entry.get("file_name", "")).strip()
        if not file_name:
            return None

        try:
            return self._target_path(file_name)
        except DownloadError:
            return None

    def _record_uses_local_file(self, record: Dict[str, Any] | None) -> bool:
        if not isinstance(record, dict):
            return False

        status = str(record.get("status", "")).strip().lower()
        return bool(
            str(record.get("downloaded_at", "")).strip()
            or status in {"downloaded", "skipped"}
        )

    def _normalize_state_record(
        self,
        record: Dict[str, Any] | None,
        *,
        require_local_file: bool = False,
    ) -> Dict[str, Any] | None:
        if not isinstance(record, dict):
            return None

        file_name = str(record.get("file_name", "")).strip()
        if not file_name:
            return None

        if self._record_uses_local_file(record):
            return self._normalize_history_entry(record)
        if require_local_file:
            return None

        try:
            file_size_bytes = int(record.get("file_size_bytes", 0) or 0)
        except (TypeError, ValueError):
            file_size_bytes = 0

        return {
            "file_name": file_name,
            "official_data_date": str(record.get("official_data_date", "")).strip(),
            "release_date_raw": normalize_release_datetime_text(str(record.get("release_date_raw", "")).strip()),
            "file_size_bytes": file_size_bytes,
            "download_url": normalize_download_url(str(record.get("download_url", "")).strip()),
            "downloaded_at": str(record.get("downloaded_at", "")).strip(),
            "status": str(record.get("status", "")).strip() or "available",
        }

    def _state_reconciliation_fields(self, state: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "download_history": state.get("download_history", []),
            "latest_remote": state.get("latest_remote"),
            "last_download": state.get("last_download"),
        }

    def _history_date_or_min(self, value: str) -> date:
        try:
            return parse_iso_date(value)
        except DownloadError:
            return date.min

    def _seed_history_entries(self, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []

        history = state.get("download_history", [])
        if isinstance(history, list):
            entries.extend(item for item in history if isinstance(item, dict))

        for candidate_key in ("latest_remote", "last_download"):
            candidate = state.get(candidate_key)
            if isinstance(candidate, dict):
                entries.append(candidate)

        return entries

    def _upsert_history_entry(self, state: Dict[str, Any], entry: Dict[str, Any]) -> None:
        normalized = self._normalize_history_entry(entry)
        if normalized is None:
            return

        history = state.get("download_history", [])
        if not isinstance(history, list):
            history = []

        updated = False
        for index, current in enumerate(history):
            if not isinstance(current, dict):
                continue
            if str(current.get("file_name", "")).strip() == normalized["file_name"]:
                history[index] = normalized
                updated = True
                break

        if not updated:
            history.append(normalized)

        history.sort(key=self._history_sort_key, reverse=True)
        state["download_history"] = history

    def _list_cached_downloaded_entries(self, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        normalized_entries: Dict[str, Dict[str, Any]] = {}

        for entry in self._seed_history_entries(state):
            normalized = self._normalize_history_entry(entry)
            if normalized is None:
                continue
            normalized_entries[normalized["file_name"]] = normalized

        return sorted(
            normalized_entries.values(),
            key=self._history_sort_key,
            reverse=True,
        )

    def _select_latest_downloaded_entry(self, state: Dict[str, Any]) -> Dict[str, Any] | None:
        return next(iter(self._list_cached_downloaded_entries(state)), None)

    def _select_public_state_records(
        self,
        state: Dict[str, Any],
        *,
        latest_downloaded: Dict[str, Any] | None = None,
    ) -> tuple[Dict[str, Any] | None, Dict[str, Any] | None]:
        raw_latest_remote = state.get("latest_remote")
        raw_last_download = state.get("last_download")

        latest_remote = self._normalize_state_record(raw_latest_remote)
        last_download = self._normalize_state_record(
            raw_last_download,
            require_local_file=True,
        )

        if latest_downloaded is None:
            latest_downloaded = self._select_latest_downloaded_entry(state)

        if latest_remote is None:
            latest_remote = latest_downloaded or (
                raw_latest_remote if isinstance(raw_latest_remote, dict) else None
            )
        if last_download is None:
            last_download = latest_downloaded or (
                raw_last_download if isinstance(raw_last_download, dict) else None
            )

        return latest_remote, last_download

    def _select_public_status_records_for_read(
        self,
        state: Dict[str, Any],
    ) -> tuple[Dict[str, Any] | None, Dict[str, Any] | None]:
        latest_remote = self._normalize_state_record(state.get("latest_remote"))
        last_download = self._normalize_state_record(
            state.get("last_download"),
            require_local_file=True,
        )
        latest_downloaded = (
            next(iter(self._list_downloaded_entries()), None)
            if latest_remote is None or last_download is None
            else None
        )
        return self._select_public_state_records(
            state,
            latest_downloaded=latest_downloaded,
        )

    def _reconcile_state_with_disk(self, state: Dict[str, Any]) -> int:
        cached_entry_count = len(self._list_cached_downloaded_entries(state))
        for entry in self._iter_disk_entries():
            self._upsert_history_entry(state, entry)

        latest_remote, last_download = self._select_public_state_records(
            state,
            latest_downloaded=self._select_latest_downloaded_entry(state),
        )
        if latest_remote is not None:
            state["latest_remote"] = latest_remote
        if last_download is not None:
            state["last_download"] = last_download

        repaired_entry_count = len(self._list_cached_downloaded_entries(state))
        return max(0, repaired_entry_count - cached_entry_count)

    def repair_download_history_from_disk(self, *, if_missing_only: bool = False) -> int:
        state = self.load_state()
        if if_missing_only and self._select_latest_downloaded_entry(state) is not None:
            return 0

        state_before = json.dumps(self._state_reconciliation_fields(state), ensure_ascii=False, sort_keys=True)
        repaired_entry_count = self._reconcile_state_with_disk(state)
        state_after = json.dumps(self._state_reconciliation_fields(state), ensure_ascii=False, sort_keys=True)
        if state_after != state_before:
            self.write_state(state)
        return repaired_entry_count

    def _list_downloaded_entries(self) -> List[Dict[str, Any]]:
        state = self.load_state()
        entries = self._list_cached_downloaded_entries(state)
        if entries:
            return entries

        self.repair_download_history_from_disk(if_missing_only=False)
        repaired_state = self.load_state()
        return self._list_cached_downloaded_entries(repaired_state)

    def resolve_latest_downloaded_file(self) -> tuple[Any, Dict[str, Any]] | None:
        entries = self._list_downloaded_entries()
        for latest_entry in entries:
            target_path = self._resolve_entry_path(latest_entry)
            if target_path is None or not target_path.exists() or not self._is_readable_zip_file(target_path):
                continue
            return target_path, latest_entry

        self.repair_download_history_from_disk(if_missing_only=False)
        for latest_entry in self._list_downloaded_entries():
            target_path = self._resolve_entry_path(latest_entry)
            if target_path is None or not target_path.exists() or not self._is_readable_zip_file(target_path):
                continue
            return target_path, latest_entry

        return None

    def _history_sort_key(self, entry: Dict[str, Any]) -> tuple[date, datetime, str]:
        return (
            self._history_date_or_min(str(entry.get("official_data_date", "")).strip()),
            parse_release_datetime(str(entry.get("release_date_raw", "")).strip()),
            str(entry.get("file_name", "")).strip(),
        )
