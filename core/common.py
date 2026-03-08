#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
from os import PathLike
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse


ROOT_DIR = Path(__file__).resolve().parents[1]
DOWNLOADS_DIR = ROOT_DIR / "downloads"
PARTIAL_DIR = DOWNLOADS_DIR / ".partial"
RUNTIME_DIR = ROOT_DIR / "runtime"
STATE_PATH = RUNTIME_DIR / "state.json"
DB_PATH = RUNTIME_DIR / "app.db"
DATASET_PAGE_URL = "https://data.uspto.gov/bulkdata/datasets/trtdxfap"
PRODUCT_URL = "https://data.uspto.gov/ui/datasets/products/trtdxfap?includeFiles=true"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)
DEFAULT_BROWSER_SETTLE_MS = 3000
DEFAULT_TIMEOUT = 120.0
DEFAULT_COOKIE_CACHE_TTL_SECONDS = 0
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 1.0
DEFAULT_RETRY_JITTER_RATIO = 0.3
DEFAULT_FAILURE_COOLDOWN_SECONDS = 300
OFFICIAL_DATE_FILE_NAME_RE = re.compile(r"(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)")
ALLOWED_DOWNLOAD_URL_HOSTS = {"data.uspto.gov"}
ALLOWED_DOWNLOAD_URL_PATH_PREFIXES = (
    "/ui/datasets/products/files/TRTDXFAP/",
    "/ui/datasets/products/files/trtdxfap/",
    "/bulkdata/",
)
MIN_RELEASE_DATETIME = datetime.min.replace(tzinfo=timezone.utc)

PUBLIC_ERROR_MESSAGES = {
    "browser_unavailable": "浏览器会话初始化失败，请稍后重试。",
    "upstream_unavailable": "官方数据源暂时不可用，请稍后重试。",
    "download_failed": "最新文件下载失败，请稍后重试。",
    "download_in_progress": "已有下载任务在运行，请稍后再试。",
    "cooldown_active": "最近连续失败，已进入短期冷却窗口，请稍后再试。",
    "invalid_metadata": "官方返回的数据格式异常，请联系管理员。",
    "internal_error": "服务内部处理失败，请联系管理员。",
}

ERROR_CODE_HINTS = {
    "browser_unavailable": "检查 Playwright 与 Chromium 是否安装完成，并确认当前宿主机允许启动无头浏览器。",
    "upstream_unavailable": "检查 USPTO 站点可达性，避免过于频繁的重试；如连续失败，可等待冷却窗口结束后再试。",
    "download_failed": "检查磁盘空间、文件权限和上游返回内容，确认收到的是 ZIP 而不是 HTML/WAF 页面。",
    "download_in_progress": "已有同步任务在执行，请查看 job-runs 或稍后再试，不要并发触发多次同步。",
    "cooldown_active": "说明最近连续失败次数较多，系统正在短暂停止主动访问上游；请等待冷却结束再重试，并先查看最近的 job-runs 错误摘要。",
    "invalid_metadata": "USPTO 元数据结构可能发生变化或返回异常内容，需要人工检查最新 payload。",
    "internal_error": "查看服务日志和 job-runs 详情，确认是哪一步抛出了未预期异常。",
    "latest_file_not_found": "当前还没有本地可下载 ZIP；先执行一次同步，或检查 downloads/ 与 runtime/app.db 是否存在有效数据。",
    "service_not_ready": "说明服务还没完成初始化或运行目录不可写，请先检查容器/进程启动日志。",
}

logger = logging.getLogger("downloader")


class DownloadError(RuntimeError):
    """Raised when metadata retrieval or ZIP download fails."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "download_failed",
        public_message: str | None = None,
        retryable: bool = False,
        attempts: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.public_message = public_message or PUBLIC_ERROR_MESSAGES.get(
            code,
            PUBLIC_ERROR_MESSAGES["internal_error"],
        )
        self.retryable = retryable
        self.attempts = attempts


@dataclass(frozen=True)
class RemoteRecord:
    file_name: str
    official_data_date: str
    release_date_raw: str
    file_size_bytes: int
    download_url: str

    def to_dict(
        self,
        local_path: str = "",
        downloaded_at: str = "",
        status: str = "available",
    ) -> Dict[str, Any]:
        return {
            "file_name": self.file_name,
            "official_data_date": self.official_data_date,
            "release_date_raw": self.release_date_raw,
            "file_size_bytes": self.file_size_bytes,
            "download_url": self.download_url,
            "local_path": local_path,
            "downloaded_at": downloaded_at,
            "status": status,
        }


def iso_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise DownloadError(
            f"官方返回的 fileDataFromDate 非法: {value}",
            code="invalid_metadata",
            public_message=PUBLIC_ERROR_MESSAGES["invalid_metadata"],
        ) from exc


def parse_release_datetime(value: str) -> datetime:
    if not value:
        return MIN_RELEASE_DATETIME

    normalized = value.replace(" ", "T")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return MIN_RELEASE_DATETIME

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_release_datetime_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    parsed = parse_release_datetime(text)
    if parsed == MIN_RELEASE_DATETIME:
        return text
    return parsed.isoformat(timespec="seconds")


def normalize_download_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    parsed = urlparse(text)
    if parsed.scheme.lower() != "https":
        return ""

    host = (parsed.hostname or "").lower()
    if host not in ALLOWED_DOWNLOAD_URL_HOSTS:
        return ""

    path = parsed.path or "/"
    if not any(path.startswith(prefix) for prefix in ALLOWED_DOWNLOAD_URL_PATH_PREFIXES):
        return ""

    return text


def error_hint_for_code(code: str | None) -> str | None:
    normalized = str(code or "").strip()
    if not normalized:
        return None
    return ERROR_CODE_HINTS.get(normalized)


def _resolve_configured_path(value: str | PathLike[str] | None, *, base_dir: Path) -> Path:
    text = str(value or "").strip()
    if not text:
        return base_dir

    path = Path(text).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def resolve_runtime_paths(
    root_dir_value: str | PathLike[str] | None = None,
    downloads_dir_value: str | PathLike[str] | None = None,
    runtime_dir_value: str | PathLike[str] | None = None,
) -> Dict[str, Path]:
    root_dir = _resolve_configured_path(root_dir_value, base_dir=ROOT_DIR)
    downloads_dir = (
        _resolve_configured_path(downloads_dir_value, base_dir=root_dir)
        if str(downloads_dir_value or "").strip()
        else root_dir / "downloads"
    )
    runtime_dir = (
        _resolve_configured_path(runtime_dir_value, base_dir=root_dir)
        if str(runtime_dir_value or "").strip()
        else root_dir / "runtime"
    )

    return {
        "root_dir": root_dir,
        "downloads_dir": downloads_dir,
        "partial_dir": downloads_dir / ".partial",
        "runtime_dir": runtime_dir,
        "state_path": runtime_dir / STATE_PATH.name,
        "db_path": runtime_dir / DB_PATH.name,
        "lock_path": runtime_dir / ".download.lock",
    }
