#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import zipfile
from datetime import datetime
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from core.common import (
    OFFICIAL_DATE_FILE_NAME_RE,
    DownloadError,
    PUBLIC_ERROR_MESSAGES,
    RemoteRecord,
    normalize_download_url,
    normalize_release_datetime_text,
    parse_release_datetime,
)


class DownloaderZipMixin:
    def _validate_file_name(self, file_name: str) -> str:
        normalized = str(file_name).strip()
        if not normalized:
            raise DownloadError(
                "官方元数据缺少 fileName。",
                code="invalid_metadata",
                public_message=PUBLIC_ERROR_MESSAGES["invalid_metadata"],
            )

        if (
            normalized in {".", ".."}
            or "/" in normalized
            or "\\" in normalized
            or Path(normalized).name != normalized
        ):
            raise DownloadError(
                f"官方元数据包含非法文件名: {normalized}",
                code="invalid_metadata",
                public_message=PUBLIC_ERROR_MESSAGES["invalid_metadata"],
            )

        return normalized

    def _target_path(self, file_name: str) -> Path:
        return self.downloads_dir / self._validate_file_name(file_name)

    def _partial_path(self, file_name: str) -> Path:
        safe_file_name = self._validate_file_name(file_name)
        return self.partial_dir / f"{safe_file_name}.part"

    def _format_local_mtime(self, path: Path) -> str:
        return datetime.fromtimestamp(path.stat().st_mtime).astimezone().isoformat(timespec="seconds")

    def _looks_like_zip_file(self, path: Path) -> bool:
        if not path.exists() or not path.is_file():
            return False

        return zipfile.is_zipfile(path)

    def _is_readable_zip_file(self, path: Path) -> bool:
        return self._looks_like_zip_file(path)

    def _is_valid_zip_file(self, path: Path) -> bool:
        if not self._looks_like_zip_file(path):
            return False

        try:
            with zipfile.ZipFile(path) as archive:
                return archive.testzip() is None
        except (OSError, zipfile.BadZipFile, zipfile.LargeZipFile):
            return False

    def _has_valid_local_file(self, record: RemoteRecord) -> bool:
        target_path = self._target_path(record.file_name)
        if not target_path.exists():
            return False

        if target_path.stat().st_size != record.file_size_bytes:
            return False

        return self._is_valid_zip_file(target_path)

    def _enrich_with_local_state(
        self,
        record: RemoteRecord,
        status: str | None = None,
        *,
        assume_valid: bool = False,
    ) -> Dict[str, Any]:
        target_path = self._target_path(record.file_name)
        if assume_valid or self._has_valid_local_file(record):
            target_status = status or "downloaded"
            return record.to_dict(
                local_path=str(target_path),
                downloaded_at=self._format_local_mtime(target_path),
                status=target_status,
            )

        return record.to_dict(status=status or "available")

    def _infer_official_date_from_file_name(self, file_name: str) -> str:
        match = OFFICIAL_DATE_FILE_NAME_RE.search(Path(file_name).stem)
        if match is None:
            return ""

        year_text, month_text, day_text = match.groups()
        try:
            return date(
                2000 + int(year_text),
                int(month_text),
                int(day_text),
            ).isoformat()
        except ValueError:
            return ""

    def _build_disk_entry(self, path: Path) -> Dict[str, Any] | None:
        if not self._is_readable_zip_file(path):
            return None

        downloaded_at = self._format_local_mtime(path)
        return {
            "file_name": path.name,
            "official_data_date": self._infer_official_date_from_file_name(path.name),
            "release_date_raw": downloaded_at,
            "file_size_bytes": path.stat().st_size,
            "download_url": "",
            "local_path": str(path),
            "downloaded_at": downloaded_at,
            "status": "downloaded",
        }

    def _iter_disk_entries(self) -> List[Dict[str, Any]]:
        if not self.downloads_dir.exists():
            return []

        entries: List[Dict[str, Any]] = []
        for candidate in self.downloads_dir.iterdir():
            if not candidate.is_file() or candidate.suffix.lower() != ".zip":
                continue
            entry = self._build_disk_entry(candidate)
            if entry is not None:
                entries.append(entry)
        return entries

    def _normalize_history_entry(self, entry: Dict[str, Any]) -> Dict[str, Any] | None:
        if not isinstance(entry, dict):
            return None

        file_name = str(entry.get("file_name", "")).strip()
        if not file_name:
            return None

        target_path = self._resolve_entry_path(entry)
        if target_path is None or not target_path.exists():
            return None

        try:
            file_size_bytes = int(entry.get("file_size_bytes", 0) or 0)
        except (TypeError, ValueError):
            return None

        if file_size_bytes <= 0 or target_path.stat().st_size != file_size_bytes:
            return None

        if not self._is_readable_zip_file(target_path):
            return None

        normalized = dict(entry)
        normalized["local_path"] = str(target_path)
        normalized["release_date_raw"] = normalize_release_datetime_text(
            str(entry.get("release_date_raw", "")).strip()
        )
        normalized["download_url"] = normalize_download_url(str(entry.get("download_url", "")).strip())
        normalized["downloaded_at"] = (
            str(entry.get("downloaded_at", "")).strip() or self._format_local_mtime(target_path)
        )
        normalized["status"] = "downloaded"
        return normalized

    def _history_sort_key(self, entry: Dict[str, Any]) -> tuple[date, Any, str]:
        return (
            self._history_date_or_min(str(entry.get("official_data_date", "")).strip()),
            parse_release_datetime(str(entry.get("release_date_raw", "")).strip()),
            str(entry.get("file_name", "")).strip(),
        )

    def download_or_skip(self, client: Any, record: RemoteRecord) -> Dict[str, Any]:
        if self._has_valid_local_file(record):
            return {
                "action": "skipped",
                "last_download": self._enrich_with_local_state(record, status="skipped"),
                "latest_remote": self._enrich_with_local_state(record, status="downloaded"),
            }

        self.ensure_layout()
        partial_path = self._partial_path(record.file_name)
        target_path = self._target_path(record.file_name)

        try:
            with client.stream(
                "GET",
                record.download_url,
                headers={
                    "Accept": "application/octet-stream, */*",
                    "Referer": self.dataset_page_url,
                    "User-Agent": self.user_agent,
                },
            ) as response:
                response.raise_for_status()
                content_type = str(response.headers.get("content-type", "")).lower()

                with partial_path.open("wb") as file:
                    iterator = response.iter_bytes()
                    first_chunk = next((chunk for chunk in iterator if chunk), b"")
                    if not first_chunk:
                        raise DownloadError(
                            "下载 ZIP 时未收到任何字节。",
                            code="download_failed",
                            public_message=PUBLIC_ERROR_MESSAGES["download_failed"],
                            retryable=True,
                        )

                    first_chunk_text = first_chunk[:128].lstrip().lower()
                    if "html" in content_type or first_chunk_text.startswith(b"<!doctype html") or first_chunk_text.startswith(b"<html"):
                        raise DownloadError(
                            "下载 ZIP 时返回了 HTML/WAF 页面，而不是二进制文件。",
                            code="download_failed",
                            public_message=PUBLIC_ERROR_MESSAGES["download_failed"],
                            retryable=True,
                        )

                    if not first_chunk.startswith(b"PK"):
                        raise DownloadError(
                            "下载内容不是 ZIP 文件。",
                            code="download_failed",
                            public_message=PUBLIC_ERROR_MESSAGES["download_failed"],
                            retryable=True,
                        )

                    bytes_written = file.write(first_chunk)
                    for chunk in iterator:
                        if not chunk:
                            continue
                        bytes_written += file.write(chunk)

                if bytes_written != record.file_size_bytes:
                    raise DownloadError(
                        f"ZIP 文件大小校验失败: 预期 {record.file_size_bytes}，实际 {bytes_written}",
                        code="download_failed",
                        public_message=PUBLIC_ERROR_MESSAGES["download_failed"],
                        retryable=True,
                    )

                partial_path.replace(target_path)

            if not self._is_valid_zip_file(target_path):
                raise DownloadError(
                    "下载完成后的 ZIP 结构校验失败。",
                    code="download_failed",
                    public_message=PUBLIC_ERROR_MESSAGES["download_failed"],
                    retryable=True,
                )
        except Exception as exc:  # noqa: BLE001
            if partial_path.exists():
                partial_path.unlink()
            if isinstance(exc, DownloadError):
                raise
            raise DownloadError(
                f"下载最新 ZIP 失败: {exc}",
                code="download_failed",
                public_message=PUBLIC_ERROR_MESSAGES["download_failed"],
                retryable=True,
            ) from exc

        return {
            "action": "downloaded",
            "last_download": self._enrich_with_local_state(record, status="downloaded", assume_valid=True),
            "latest_remote": self._enrich_with_local_state(record, status="downloaded", assume_valid=True),
        }
