#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from core.common import DownloadError, normalize_download_url, normalize_release_datetime_text, parse_iso_date, parse_release_datetime


def resolve_entry_path(owner: Any, entry: dict[str, Any]) -> Path | None:
    file_name = str(entry.get("file_name", "")).strip()
    if not file_name:
        return None

    try:
        return owner._target_path(file_name)
    except DownloadError:
        return None


def record_uses_local_file(owner: Any, record: dict[str, Any] | None) -> bool:
    if not isinstance(record, dict):
        return False

    status = str(record.get("status", "")).strip().lower()
    return bool(
        str(record.get("downloaded_at", "")).strip()
        or status in {"downloaded", "skipped"}
    )


def normalize_state_record(
    owner: Any,
    record: dict[str, Any] | None,
    *,
    require_local_file: bool = False,
) -> dict[str, Any] | None:
    if not isinstance(record, dict):
        return None

    file_name = str(record.get("file_name", "")).strip()
    if not file_name:
        return None

    if record_uses_local_file(owner, record):
        return owner._normalize_history_entry(record)
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


def state_reconciliation_fields(owner: Any, state: dict[str, Any]) -> dict[str, Any]:
    return {
        "download_history": state.get("download_history", []),
        "latest_remote": state.get("latest_remote"),
        "last_download": state.get("last_download"),
    }


def history_date_or_min(owner: Any, value: str) -> date:
    try:
        return parse_iso_date(value)
    except DownloadError:
        return date.min


def seed_history_entries(owner: Any, state: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []

    history = state.get("download_history", [])
    if isinstance(history, list):
        entries.extend(item for item in history if isinstance(item, dict))

    for candidate_key in ("latest_remote", "last_download"):
        candidate = state.get(candidate_key)
        if isinstance(candidate, dict):
            entries.append(candidate)

    return entries


def upsert_history_entry(owner: Any, state: dict[str, Any], entry: dict[str, Any]) -> None:
    normalized = owner._normalize_history_entry(entry)
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

    history.sort(key=owner._history_sort_key, reverse=True)
    state["download_history"] = history


def list_cached_downloaded_entries(owner: Any, state: dict[str, Any]) -> list[dict[str, Any]]:
    normalized_entries: dict[str, dict[str, Any]] = {}

    for entry in seed_history_entries(owner, state):
        normalized = owner._normalize_history_entry(entry)
        if normalized is None:
            continue
        normalized_entries[normalized["file_name"]] = normalized

    return sorted(
        normalized_entries.values(),
        key=owner._history_sort_key,
        reverse=True,
    )


def select_latest_downloaded_entry(owner: Any, state: dict[str, Any]) -> dict[str, Any] | None:
    return next(iter(list_cached_downloaded_entries(owner, state)), None)


def select_public_state_records(
    owner: Any,
    state: dict[str, Any],
    *,
    latest_downloaded: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    raw_latest_remote = state.get("latest_remote")
    raw_last_download = state.get("last_download")

    latest_remote = normalize_state_record(owner, raw_latest_remote)
    last_download = normalize_state_record(
        owner,
        raw_last_download,
        require_local_file=True,
    )

    if latest_downloaded is None:
        latest_downloaded = select_latest_downloaded_entry(owner, state)

    if latest_remote is None:
        latest_remote = latest_downloaded or (
            raw_latest_remote if isinstance(raw_latest_remote, dict) else None
        )
    if last_download is None:
        last_download = latest_downloaded or (
            raw_last_download if isinstance(raw_last_download, dict) else None
        )

    return latest_remote, last_download


def reconcile_state_with_disk(owner: Any, state: dict[str, Any]) -> int:
    cached_entry_count = len(list_cached_downloaded_entries(owner, state))
    for entry in owner._iter_disk_entries():
        upsert_history_entry(owner, state, entry)

    latest_remote, last_download = select_public_state_records(
        owner,
        state,
        latest_downloaded=select_latest_downloaded_entry(owner, state),
    )
    if latest_remote is not None:
        state["latest_remote"] = latest_remote
    if last_download is not None:
        state["last_download"] = last_download

    repaired_entry_count = len(list_cached_downloaded_entries(owner, state))
    return max(0, repaired_entry_count - cached_entry_count)


def repair_download_history_from_disk(owner: Any, *, if_missing_only: bool = False) -> int:
    state = owner.load_state()
    if if_missing_only and select_latest_downloaded_entry(owner, state) is not None:
        return 0

    state_before = json.dumps(state_reconciliation_fields(owner, state), ensure_ascii=False, sort_keys=True)
    repaired_entry_count = reconcile_state_with_disk(owner, state)
    state_after = json.dumps(state_reconciliation_fields(owner, state), ensure_ascii=False, sort_keys=True)
    if state_after != state_before:
        owner.write_state(state)
    return repaired_entry_count


def history_sort_key(owner: Any, entry: dict[str, Any]) -> tuple[date, datetime, str]:
    return (
        history_date_or_min(owner, str(entry.get("official_data_date", "")).strip()),
        parse_release_datetime(str(entry.get("release_date_raw", "")).strip()),
        str(entry.get("file_name", "")).strip(),
    )
