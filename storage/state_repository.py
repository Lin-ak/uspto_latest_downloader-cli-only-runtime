#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from typing import Any

from core.common import DownloadError


def load_legacy_state_file_unlocked(owner: Any) -> dict[str, Any] | None:
    if not owner.state_path.exists():
        return None

    with owner.state_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    if not isinstance(payload, dict):
        raise DownloadError("runtime/state.json 顶层不是对象。")

    state = owner.default_state()
    state.update(payload)
    return state


def write_state_to_db_unlocked(owner: Any, connection: sqlite3.Connection, state: dict[str, Any]) -> dict[str, Any]:
    normalized = owner.default_state()
    normalized.update(state)

    history = normalized.get("download_history", [])
    if not isinstance(history, list):
        history = []
    normalized["download_history"] = [item for item in history if isinstance(item, dict)]

    updated_at = owner.iso_now()
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


def read_state_from_db_unlocked(owner: Any, connection: sqlite3.Connection) -> dict[str, Any] | None:
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

    state = owner.default_state()
    state["running"] = bool(row["running"])
    state["last_checked_at"] = row["last_checked_at"]
    state["last_action"] = row["last_action"]
    state["latest_remote"] = owner._parse_json_text(row["latest_remote_json"])
    state["last_download"] = owner._parse_json_text(row["last_download_json"])
    state["last_error"] = owner._parse_json_text(row["last_error_json"])

    history_rows = connection.execute(
        """
        SELECT entry_json
        FROM download_history
        """
    ).fetchall()
    history: list[dict[str, Any]] = []
    for history_row in history_rows:
        entry = owner._parse_json_text(history_row["entry_json"])
        if isinstance(entry, dict):
            history.append(entry)
    history.sort(key=owner._history_sort_key, reverse=True)
    state["download_history"] = history
    return state


def migrate_legacy_state_if_needed_unlocked(owner: Any, connection: sqlite3.Connection) -> None:
    current_state = read_state_from_db_unlocked(owner, connection)
    if current_state is not None:
        return

    legacy_state = load_legacy_state_file_unlocked(owner)
    if legacy_state is None:
        write_state_to_db_unlocked(owner, connection, owner.default_state())
        return

    write_state_to_db_unlocked(owner, connection, legacy_state)
    migrated_path = owner.state_path.with_suffix(".json.migrated")
    owner.state_path.replace(migrated_path)


def load_state(owner: Any) -> dict[str, Any]:
    owner.ensure_layout()
    with owner._state_lock:
        with closing(owner._connect_db_unlocked()) as connection:
            owner._initialize_db_unlocked(connection)
            migrate_legacy_state_if_needed_unlocked(owner, connection)
            state = read_state_from_db_unlocked(owner, connection)
            if state is None:
                state = write_state_to_db_unlocked(owner, connection, owner.default_state())
            connection.commit()
            return state


def write_state(owner: Any, state: dict[str, Any]) -> dict[str, Any]:
    owner.ensure_layout()
    with owner._state_lock:
        with closing(owner._connect_db_unlocked()) as connection:
            owner._initialize_db_unlocked(connection)
            normalized = write_state_to_db_unlocked(owner, connection, state)
            connection.commit()
            return normalized


def reset_running_flag(owner: Any) -> dict[str, Any]:
    state = owner.load_state()
    if state.get("running"):
        state["running"] = False
        owner.write_state(state)
    return state
