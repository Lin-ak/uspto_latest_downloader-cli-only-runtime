#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from contextlib import closing
import sqlite3
from typing import Any

from core.common import PUBLIC_ERROR_MESSAGES, error_hint_for_code
from core.contract import SYNC_LATEST_FILE_OPERATION, SYNC_LATEST_FILE_RESOURCE


def create_job_run_unlocked(
    owner: Any,
    connection: sqlite3.Connection,
    *,
    trigger_source: str,
    started_at: str,
    status: dict[str, Any],
) -> int:
    created_at = owner.iso_now()
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


def build_job_run_summary(
    owner: Any,
    *,
    outcome: str,
    trigger_source: str,
    latest_remote: dict[str, Any] | None,
    error: dict[str, Any] | None,
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


def finalize_job_run_unlocked(
    owner: Any,
    connection: sqlite3.Connection,
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
    summary_text = build_job_run_summary(
        owner,
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
            owner._duration_millis(
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


def deserialize_job_run_row(owner: Any, row: sqlite3.Row) -> dict[str, Any]:
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
        "latest_remote": owner._parse_json_text(row["latest_remote_json"]),
        "last_download": owner._parse_json_text(row["last_download_json"]),
        "status": owner._parse_json_text(row["status_json"]),
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


def create_job_run(
    owner: Any,
    *,
    trigger_source: str,
    started_at: str,
    status: dict[str, Any],
) -> int:
    owner.ensure_layout()
    with owner._state_lock:
        with closing(owner._connect_db_unlocked()) as connection:
            owner._initialize_db_unlocked(connection)
            job_run_id = create_job_run_unlocked(
                owner,
                connection,
                trigger_source=trigger_source,
                started_at=started_at,
                status=status,
            )
            connection.commit()
            return job_run_id


def finalize_job_run(
    owner: Any,
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
    owner.ensure_layout()
    with owner._state_lock:
        with closing(owner._connect_db_unlocked()) as connection:
            owner._initialize_db_unlocked(connection)
            finalize_job_run_unlocked(
                owner,
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


def list_job_runs(owner: Any, limit: int = 20, offset: int = 0) -> dict[str, Any]:
    owner.ensure_layout()
    with owner._state_lock:
        with closing(owner._connect_db_unlocked()) as connection:
            owner._initialize_db_unlocked(connection)
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

    job_runs = [deserialize_job_run_row(owner, row) for row in rows]
    return {
        "job_runs": job_runs,
        "pagination": {
            "limit": limit,
            "offset": offset,
            "total": int((total_row or {"count": 0})["count"]),
            "count": len(job_runs),
        },
    }
