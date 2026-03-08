#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
from datetime import timedelta
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

from downloader_common import (
    DownloadError,
    PUBLIC_ERROR_MESSAGES,
    RemoteRecord,
    iso_now,
    logger,
    normalize_download_url,
    normalize_release_datetime_text,
    parse_iso_date,
    parse_release_datetime,
)
from logging_utils import log_event

try:
    import httpx
except ImportError:  # pragma: no cover - handled at runtime
    httpx = None  # type: ignore[assignment]


class DownloaderUpstreamMixin:
    _COOKIE_CACHE_KEY = "upstream_cookies"

    def _require_httpx(self) -> Any:
        if httpx is None:
            raise DownloadError(
                "缺少 httpx 依赖。请先执行: pip install -r requirements.txt",
                code="internal_error",
                public_message=PUBLIC_ERROR_MESSAGES["internal_error"],
            )
        return httpx

    def _require_playwright(self) -> Any:
        try:
            from playwright.sync_api import Error as PlaywrightError
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise DownloadError(
                "缺少 playwright 依赖。请先执行: pip install -r requirements.txt && playwright install chromium",
                code="browser_unavailable",
                public_message=PUBLIC_ERROR_MESSAGES["browser_unavailable"],
            ) from exc

        return sync_playwright, PlaywrightError

    def _build_client(self, cookies: Dict[str, str]) -> Any:
        httpx_module = self._require_httpx()
        timeout = httpx_module.Timeout(self.timeout)
        return httpx_module.Client(
            timeout=timeout,
            headers={
                "Accept": "application/json, text/plain, */*",
                "Referer": self.dataset_page_url,
                "User-Agent": self.user_agent,
            },
            cookies=cookies,
            follow_redirects=True,
        )

    def _acquire_cookies(self) -> Dict[str, str]:
        sync_playwright, PlaywrightError = self._require_playwright()

        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                try:
                    context = browser.new_context(user_agent=self.user_agent)
                    page = context.new_page()
                    page.goto(
                        self.dataset_page_url,
                        wait_until="domcontentloaded",
                        timeout=int(self.timeout * 1000),
                    )
                    page.wait_for_timeout(self.browser_settle_ms)
                    return {cookie["name"]: cookie["value"] for cookie in context.cookies()}
                finally:
                    browser.close()
        except PlaywrightError as exc:
            message = str(exc)
            if "Executable doesn't exist" in message:
                raise DownloadError(
                    "未安装 Chromium。请执行: playwright install chromium",
                    code="browser_unavailable",
                    public_message=PUBLIC_ERROR_MESSAGES["browser_unavailable"],
                ) from exc
            raise DownloadError(
                f"浏览器上下文初始化失败: {exc}",
                code="browser_unavailable",
                public_message=PUBLIC_ERROR_MESSAGES["browser_unavailable"],
                retryable=True,
            ) from exc
        except Exception as exc:  # noqa: BLE001
            raise DownloadError(
                f"浏览器上下文初始化失败: {exc}",
                code="browser_unavailable",
                public_message=PUBLIC_ERROR_MESSAGES["browser_unavailable"],
                retryable=True,
            ) from exc

    def _normalize_cookies(self, cookies: Dict[str, Any] | None) -> Dict[str, str]:
        if not isinstance(cookies, dict):
            return {}

        normalized: Dict[str, str] = {}
        for name, value in cookies.items():
            cookie_name = str(name).strip()
            cookie_value = str(value).strip()
            if not cookie_name or not cookie_value:
                continue
            normalized[cookie_name] = cookie_value
        return normalized

    def _cache_cookie_jar(self, cookies: Dict[str, str]) -> None:
        if self.cookie_cache_ttl_seconds <= 0:
            return

        expires_at = (
            datetime.now().astimezone() + timedelta(seconds=self.cookie_cache_ttl_seconds)
        ).isoformat(timespec="seconds")
        self.write_runtime_cache(
            self._COOKIE_CACHE_KEY,
            {"cookies": cookies, "cached_at": iso_now()},
            expires_at=expires_at,
        )
        log_event(
            logger,
            logging.INFO,
            "cookie_cache_stored",
            cookie_count=len(cookies),
            ttl_seconds=self.cookie_cache_ttl_seconds,
            expires_at=expires_at,
        )

    def _load_cached_cookie_jar(self) -> Dict[str, str] | None:
        if self.cookie_cache_ttl_seconds <= 0:
            return None

        cached_payload = self.load_runtime_cache(self._COOKIE_CACHE_KEY)
        if not isinstance(cached_payload, dict):
            return None

        cookies = self._normalize_cookies(cached_payload.get("cookies"))
        if not cookies:
            self.delete_runtime_cache(self._COOKIE_CACHE_KEY)
            return None

        return cookies

    def _clear_cached_cookie_jar(self) -> None:
        self.delete_runtime_cache(self._COOKIE_CACHE_KEY)
        log_event(
            logger,
            logging.WARNING,
            "cookie_cache_cleared",
        )

    def _get_cookies(self, *, force_refresh: bool = False) -> tuple[Dict[str, str], bool]:
        if not force_refresh:
            cached_cookies = self._load_cached_cookie_jar()
            if cached_cookies:
                log_event(
                    logger,
                    logging.INFO,
                    "cookie_cache_hit",
                    cookie_count=len(cached_cookies),
                )
                return cached_cookies, True

        if self.cookie_cache_ttl_seconds > 0:
            log_event(
                logger,
                logging.INFO,
                "cookie_cache_miss" if not force_refresh else "cookie_cache_refresh",
                force_refresh=force_refresh,
            )

        fresh_cookies = self._normalize_cookies(self._acquire_cookies())
        if not fresh_cookies:
            raise DownloadError(
                "浏览器会话未返回有效 cookie。",
                code="browser_unavailable",
                public_message=PUBLIC_ERROR_MESSAGES["browser_unavailable"],
                retryable=True,
            )

        self._cache_cookie_jar(fresh_cookies)
        return fresh_cookies, False

    def _run_download_latest_attempt_with_cookies(
        self,
        cookies: Dict[str, str],
    ) -> tuple[RemoteRecord, Dict[str, Any]]:
        with self._build_client(cookies) as client:
            payload = self.fetch_product_payload(client)
            latest_remote = self.select_latest_remote(payload)
            result = self.download_or_skip(client, latest_remote)
        return latest_remote, result

    def fetch_product_payload(self, client: Any) -> Dict[str, Any]:
        try:
            response = client.get(self.product_url)
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            raise DownloadError(
                f"读取官方元数据失败: {exc}",
                code="upstream_unavailable",
                public_message=PUBLIC_ERROR_MESSAGES["upstream_unavailable"],
                retryable=True,
            ) from exc

        content_type = str(response.headers.get("content-type", "")).lower()
        if "json" not in content_type:
            sample = response.text[:160].replace("\n", " ")
            raise DownloadError(
                f"元数据接口返回非 JSON 内容: {sample}",
                code="upstream_unavailable",
                public_message=PUBLIC_ERROR_MESSAGES["upstream_unavailable"],
                retryable=True,
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise DownloadError(
                f"元数据 JSON 解析失败: {exc}",
                code="upstream_unavailable",
                public_message=PUBLIC_ERROR_MESSAGES["upstream_unavailable"],
                retryable=True,
            ) from exc

        if not isinstance(payload, dict):
            raise DownloadError(
                "官方元数据顶层不是对象。",
                code="invalid_metadata",
                public_message=PUBLIC_ERROR_MESSAGES["invalid_metadata"],
            )

        return payload

    def extract_file_data_bag(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        product_bag = payload.get("bulkDataProductBag")
        if not isinstance(product_bag, list) or not product_bag:
            raise DownloadError(
                "官方元数据缺少 bulkDataProductBag。",
                code="invalid_metadata",
                public_message=PUBLIC_ERROR_MESSAGES["invalid_metadata"],
            )

        file_entries: List[Dict[str, Any]] = []
        for product in product_bag:
            if not isinstance(product, dict):
                raise DownloadError(
                    "官方元数据的 product 节点不是对象。",
                    code="invalid_metadata",
                    public_message=PUBLIC_ERROR_MESSAGES["invalid_metadata"],
                )

            product_file_bag = product.get("productFileBag")
            if not isinstance(product_file_bag, dict):
                raise DownloadError(
                    "官方元数据缺少 productFileBag。",
                    code="invalid_metadata",
                    public_message=PUBLIC_ERROR_MESSAGES["invalid_metadata"],
                )

            file_data_bag = product_file_bag.get("fileDataBag")
            if not isinstance(file_data_bag, list):
                raise DownloadError(
                    "官方元数据缺少 fileDataBag。",
                    code="invalid_metadata",
                    public_message=PUBLIC_ERROR_MESSAGES["invalid_metadata"],
                )

            file_entries.extend(item for item in file_data_bag if isinstance(item, dict))

        return file_entries

    def select_latest_remote(self, payload: Dict[str, Any]) -> RemoteRecord:
        candidates: List[tuple[date, datetime, str, RemoteRecord]] = []

        for item in self.extract_file_data_bag(payload):
            file_name = self._validate_file_name(str(item.get("fileName", "")).strip())
            if not file_name.lower().endswith(".zip"):
                continue

            file_type_text = str(item.get("fileTypeText", "")).strip().lower()
            if file_type_text != "data":
                continue

            official_data_date = str(item.get("fileDataFromDate", "")).strip()
            if not official_data_date:
                raise DownloadError(
                    f"官方元数据缺少 fileDataFromDate: {file_name}",
                    code="invalid_metadata",
                    public_message=PUBLIC_ERROR_MESSAGES["invalid_metadata"],
                )

            download_url = normalize_download_url(str(item.get("fileDownloadURI", "")).strip())
            if not download_url:
                raise DownloadError(
                    f"官方元数据缺少或包含非法 fileDownloadURI: {file_name}",
                    code="invalid_metadata",
                    public_message=PUBLIC_ERROR_MESSAGES["invalid_metadata"],
                )

            try:
                file_size_bytes = int(item.get("fileSize", 0) or 0)
            except (TypeError, ValueError) as exc:
                raise DownloadError(
                    f"官方元数据的 fileSize 非法: {file_name}",
                    code="invalid_metadata",
                    public_message=PUBLIC_ERROR_MESSAGES["invalid_metadata"],
                ) from exc

            release_date_raw = str(item.get("fileReleaseDate", "")).strip() or str(
                item.get("fileLastModifiedDateTime", "")
            ).strip()
            normalized_release_date_raw = normalize_release_datetime_text(release_date_raw)

            record = RemoteRecord(
                file_name=file_name,
                official_data_date=official_data_date,
                release_date_raw=normalized_release_date_raw or release_date_raw,
                file_size_bytes=file_size_bytes,
                download_url=download_url,
            )
            candidates.append(
                (
                    parse_iso_date(official_data_date),
                    parse_release_datetime(normalized_release_date_raw or release_date_raw),
                    file_name,
                    record,
                )
            )

        if not candidates:
            raise DownloadError(
                "官方元数据中没有可下载的 ZIP 数据文件。",
                code="invalid_metadata",
                public_message=PUBLIC_ERROR_MESSAGES["invalid_metadata"],
            )

        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return candidates[-1][3]
