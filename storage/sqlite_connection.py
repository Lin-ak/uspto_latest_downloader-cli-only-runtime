#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sqlite3
from typing import Any


def connect_db_unlocked(owner: Any) -> sqlite3.Connection:
    connection = sqlite3.connect(owner.db_path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    return connection


def initialize_db_unlocked(owner: Any, connection: sqlite3.Connection) -> None:
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
    migrate_schema_unlocked(owner, connection)


def migrate_schema_unlocked(owner: Any, connection: sqlite3.Connection) -> None:
    existing_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(job_runs)").fetchall()
    }
    if "summary_text" not in existing_columns:
        connection.execute("ALTER TABLE job_runs ADD COLUMN summary_text TEXT")
    if "consecutive_failures" not in existing_columns:
        connection.execute("ALTER TABLE job_runs ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0")
