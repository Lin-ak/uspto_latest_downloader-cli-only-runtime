#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from contextlib import closing
import sqlite3
from typing import Any

from core.contract import SYNC_LATEST_FILE_RECOMMENDED_ENTRYPOINT, SYNC_LATEST_FILE_TRIGGER_POLICY


def build_sync_audit_snapshot_unlocked(owner: Any, connection: sqlite3.Connection) -> dict[str, Any]:
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
    cooldown_payload = owner._read_runtime_cache_unlocked(
        connection,
        cache_key=owner._FAILURE_COOLDOWN_CACHE_KEY,
    )
    cooldown_until = (
        str((cooldown_payload or {}).get("until", "")).strip() or None
    )
    cooldown_remaining_seconds = owner._seconds_until(cooldown_until)
    if cooldown_until and cooldown_remaining_seconds == 0:
        cooldown_until = None

    last_success_at = None
    last_success_outcome = None
    if last_success is not None:
        last_success_at = last_success["finished_at"] or last_success["started_at"]
        last_success_outcome = last_success["outcome"]

    return {
        "last_success_at": last_success_at,
        "last_success_age_seconds": owner._age_seconds(last_success_at),
        "last_success_outcome": last_success_outcome,
        "last_run_summary": latest_run["summary_text"] if latest_run is not None else None,
        "consecutive_failure_count": int(latest_run["consecutive_failures"] or 0) if latest_run is not None else 0,
        "failure_cooldown_until": cooldown_until,
        "failure_cooldown_remaining_seconds": cooldown_remaining_seconds,
    }


def get_sync_audit_snapshot(owner: Any) -> dict[str, Any]:
    owner.ensure_layout()
    with owner._state_lock:
        with closing(owner._connect_db_unlocked()) as connection:
            owner._initialize_db_unlocked(connection)
            snapshot = build_sync_audit_snapshot_unlocked(owner, connection)
            connection.commit()
            return snapshot


def build_status_state_for_read(owner: Any, state: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = owner.default_state()
    payload.update(state or owner.load_state())

    download_history = owner._list_cached_downloaded_entries(payload)
    if state is None and not download_history:
        owner.repair_download_history_from_disk(if_missing_only=False)
        payload = owner.default_state()
        payload.update(owner.load_state())
        download_history = owner._list_cached_downloaded_entries(payload)

    latest_downloaded = download_history[0] if download_history else None
    latest_remote, last_download = owner._select_public_state_records(
        payload,
        latest_downloaded=latest_downloaded,
    )

    payload["latest_remote"] = latest_remote
    payload["last_download"] = last_download
    payload["download_history"] = download_history
    return payload


def build_status(owner: Any, state: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = build_status_state_for_read(owner, state)
    payload["downloads_dir"] = str(owner.downloads_dir)
    payload["recommended_scheduler_entrypoint"] = SYNC_LATEST_FILE_RECOMMENDED_ENTRYPOINT
    payload["manual_sync_note"] = SYNC_LATEST_FILE_TRIGGER_POLICY["note"]
    payload.update(owner.get_sync_audit_snapshot())
    return payload
