#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from contextlib import closing
import sqlite3
from typing import Any


def read_runtime_cache_unlocked(
    owner: Any,
    connection: sqlite3.Connection,
    *,
    cache_key: str,
) -> dict[str, Any] | None:
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

    if owner._is_cache_expired(row["expires_at"]):
        connection.execute("DELETE FROM runtime_cache WHERE cache_key = ?", (cache_key,))
        return None

    value = owner._parse_json_text(row["value_json"])
    return value if isinstance(value, dict) else None


def write_runtime_cache_unlocked(
    owner: Any,
    connection: sqlite3.Connection,
    *,
    cache_key: str,
    value: dict[str, Any],
    expires_at: str | None,
) -> None:
    updated_at = owner.iso_now()
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


def delete_runtime_cache_unlocked(
    owner: Any,
    connection: sqlite3.Connection,
    *,
    cache_key: str,
) -> None:
    connection.execute("DELETE FROM runtime_cache WHERE cache_key = ?", (cache_key,))


def load_runtime_cache(owner: Any, cache_key: str) -> dict[str, Any] | None:
    owner.ensure_layout()
    with owner._state_lock:
        with closing(owner._connect_db_unlocked()) as connection:
            owner._initialize_db_unlocked(connection)
            value = read_runtime_cache_unlocked(owner, connection, cache_key=cache_key)
            connection.commit()
            return value


def write_runtime_cache(
    owner: Any,
    cache_key: str,
    value: dict[str, Any],
    *,
    expires_at: str | None = None,
) -> None:
    owner.ensure_layout()
    with owner._state_lock:
        with closing(owner._connect_db_unlocked()) as connection:
            owner._initialize_db_unlocked(connection)
            write_runtime_cache_unlocked(
                owner,
                connection,
                cache_key=cache_key,
                value=value,
                expires_at=expires_at,
            )
            connection.commit()


def delete_runtime_cache(owner: Any, cache_key: str) -> None:
    owner.ensure_layout()
    with owner._state_lock:
        with closing(owner._connect_db_unlocked()) as connection:
            owner._initialize_db_unlocked(connection)
            delete_runtime_cache_unlocked(owner, connection, cache_key=cache_key)
            connection.commit()


def get_failure_cooldown_snapshot(owner: Any) -> dict[str, Any]:
    payload = owner.load_runtime_cache(owner._FAILURE_COOLDOWN_CACHE_KEY)
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
        "remaining_seconds": owner._seconds_until(until),
        "error_code": str(payload.get("error_code", "")).strip() or None,
        "message": str(payload.get("message", "")).strip() or None,
        "retryable": bool(payload.get("retryable", False)),
    }


def set_failure_cooldown(
    owner: Any,
    *,
    until: str,
    error_code: str,
    message: str,
    retryable: bool,
) -> None:
    owner.write_runtime_cache(
        owner._FAILURE_COOLDOWN_CACHE_KEY,
        {
            "until": until,
            "error_code": error_code,
            "message": message,
            "retryable": retryable,
        },
        expires_at=until,
    )


def clear_failure_cooldown(owner: Any) -> None:
    owner.delete_runtime_cache(owner._FAILURE_COOLDOWN_CACHE_KEY)
