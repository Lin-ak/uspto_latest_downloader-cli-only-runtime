#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from core.common import DownloadError, PUBLIC_ERROR_MESSAGES, RemoteRecord
import run_download_latest_once
from sync.service import build_latest_service

from tests.common import ROOT_DIR, make_service


class DownloaderServiceBehaviorTest(unittest.TestCase):
    def test_build_latest_service_uses_project_root_runtime_paths(self) -> None:
        service = build_latest_service()

        self.assertEqual(service.root_dir, ROOT_DIR)
        self.assertEqual(service.downloads_dir, ROOT_DIR / "downloads")
        self.assertEqual(service.runtime_dir, ROOT_DIR / "runtime")
        self.assertEqual(service.db_path, ROOT_DIR / "runtime" / "app.db")

    def test_build_latest_service_honors_env_path_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir) / "workspace"
            with mock.patch.dict(
                os.environ,
                {
                    "USPTO_ROOT_DIR": str(base_dir),
                    "USPTO_DOWNLOADS_DIR": "artifacts/downloads",
                    "USPTO_RUNTIME_DIR": "state/runtime",
                },
                clear=False,
            ):
                service = build_latest_service()

        self.assertEqual(service.root_dir, base_dir.resolve())
        self.assertEqual(service.downloads_dir, (base_dir / "artifacts" / "downloads").resolve())
        self.assertEqual(service.runtime_dir, (base_dir / "state" / "runtime").resolve())
        self.assertEqual(service.db_path, (base_dir / "state" / "runtime" / "app.db").resolve())

    def test_select_latest_remote_filters_docs_and_uses_stable_sort(self) -> None:
        service = make_service(Path(tempfile.mkdtemp()))
        payload = {
            "bulkDataProductBag": [
                {
                    "productFileBag": {
                        "fileDataBag": [
                            {
                                "fileName": "Trademark-Applications-Documentation.doc",
                                "fileTypeText": "Documentation",
                                "fileDataFromDate": "2026-03-01",
                                "fileDownloadURI": "https://data.uspto.gov/ui/datasets/products/files/TRTDXFAP/doc",
                                "fileReleaseDate": "2026-03-01 00:00:00",
                                "fileSize": 100,
                            },
                            {
                                "fileName": "apc260228.zip",
                                "fileTypeText": "Data",
                                "fileDataFromDate": "2026-02-28",
                                "fileDownloadURI": "https://data.uspto.gov/ui/datasets/products/files/TRTDXFAP/apc260228.zip",
                                "fileReleaseDate": "2026-03-01 00:54:13",
                                "fileSize": 23839515,
                            },
                            {
                                "fileName": "apc260301.zip",
                                "fileTypeText": "Data",
                                "fileDataFromDate": "2026-03-01",
                                "fileDownloadURI": "https://data.uspto.gov/ui/datasets/products/files/TRTDXFAP/apc260301.zip",
                                "fileReleaseDate": "2026-03-02 00:15:00",
                                "fileSize": 1000,
                            },
                            {
                                "fileName": "apc260301b.zip",
                                "fileTypeText": "Data",
                                "fileDataFromDate": "2026-03-01",
                                "fileDownloadURI": "https://data.uspto.gov/ui/datasets/products/files/TRTDXFAP/apc260301b.zip",
                                "fileLastModifiedDateTime": "2026-03-02 00:16:00",
                                "fileSize": 1001,
                            },
                        ]
                    }
                },
                {
                    "productFileBag": {
                        "fileDataBag": [
                            {
                                "fileName": "apc260305.zip",
                                "fileTypeText": "Data",
                                "fileDataFromDate": "2026-03-05",
                                "fileDownloadURI": "https://data.uspto.gov/ui/datasets/products/files/TRTDXFAP/apc260305.zip",
                                "fileReleaseDate": "2026-03-06 00:00:00",
                                "fileSize": 1002,
                            },
                        ]
                    }
                },
            ]
        }

        record = service.select_latest_remote(payload)
        self.assertEqual(record.file_name, "apc260305.zip")
        self.assertEqual(record.official_data_date, "2026-03-05")

    def test_select_latest_remote_rejects_path_traversal_file_name(self) -> None:
        service = make_service(Path(tempfile.mkdtemp()))
        payload = {
            "bulkDataProductBag": [
                {
                    "productFileBag": {
                        "fileDataBag": [
                            {
                                "fileName": "../../runtime/pwn.zip",
                                "fileTypeText": "Data",
                                "fileDataFromDate": "2026-03-05",
                                "fileDownloadURI": "https://example.com/260305",
                                "fileReleaseDate": "2026-03-06 00:00:00",
                                "fileSize": 1002,
                            },
                        ]
                    }
                }
            ]
        }

        with self.assertRaisesRegex(DownloadError, "非法文件名"):
            service.select_latest_remote(payload)

    def test_select_latest_remote_rejects_untrusted_download_url(self) -> None:
        service = make_service(Path(tempfile.mkdtemp()))
        payload = {
            "bulkDataProductBag": [
                {
                    "productFileBag": {
                        "fileDataBag": [
                            {
                                "fileName": "apc260305.zip",
                                "fileTypeText": "Data",
                                "fileDataFromDate": "2026-03-05",
                                "fileDownloadURI": "https://evil.example.com/apc260305.zip",
                                "fileReleaseDate": "2026-03-06 00:00:00",
                                "fileSize": 1002,
                            },
                        ]
                    }
                }
            ]
        }

        with self.assertRaisesRegex(DownloadError, "非法 fileDownloadURI"):
            service.select_latest_remote(payload)

    def test_select_latest_remote_normalizes_release_date_to_aware_iso(self) -> None:
        service = make_service(Path(tempfile.mkdtemp()))
        payload = {
            "bulkDataProductBag": [
                {
                    "productFileBag": {
                        "fileDataBag": [
                            {
                                "fileName": "apc260305.zip",
                                "fileTypeText": "Data",
                                "fileDataFromDate": "2026-03-05",
                                "fileDownloadURI": "https://data.uspto.gov/ui/datasets/products/files/TRTDXFAP/apc260305.zip",
                                "fileReleaseDate": "2026-03-06 00:00:00",
                                "fileSize": 1002,
                            },
                        ]
                    }
                }
            ]
        }

        record = service.select_latest_remote(payload)
        self.assertEqual(record.release_date_raw, "2026-03-06T00:00:00+00:00")

    def test_download_or_skip_uses_existing_valid_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()
            target_path = service.downloads_dir / "apc260228.zip"
            with zipfile.ZipFile(target_path, "w") as archive:
                archive.writestr("sample.txt", "ok")

            record = RemoteRecord(
                file_name="apc260228.zip",
                official_data_date="2026-02-28",
                release_date_raw="2026-03-01 00:54:13",
                file_size_bytes=target_path.stat().st_size,
                download_url="https://example.com/apc260228.zip",
            )

            result = service.download_or_skip(client=None, record=record)
            self.assertEqual(result["action"], "skipped")
            self.assertEqual(result["last_download"]["status"], "skipped")
            self.assertEqual(result["latest_remote"]["status"], "downloaded")
            self.assertEqual(result["last_download"]["local_path"], str(target_path))

    def test_has_valid_local_file_rejects_corrupt_zip_with_matching_size(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()

            target_path = service.downloads_dir / "apc260228.zip"
            target_path.write_bytes(b"PK\x03\x04")

            record = RemoteRecord(
                file_name="apc260228.zip",
                official_data_date="2026-02-28",
                release_date_raw="2026-03-01 00:54:13",
                file_size_bytes=target_path.stat().st_size,
                download_url="https://example.com/apc260228.zip",
            )

            self.assertFalse(service._has_valid_local_file(record))

    def test_run_download_latest_rejects_cross_process_concurrency(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)
            service = make_service(root_dir)
            service.ensure_layout()

            script = f"""
import sys
import time
from pathlib import Path
sys.path.insert(0, {str(ROOT_DIR)!r})
from sync.service import DownloaderService
root = Path({str(root_dir)!r})
service = DownloaderService(
    root_dir=root,
    downloads_dir=root / "downloads",
    partial_dir=root / "downloads" / ".partial",
    runtime_dir=root / "runtime",
    state_path=root / "runtime" / "state.json",
)
lock_file = service._acquire_run_lock()
print("ready", flush=True)
time.sleep(5)
service._release_run_lock(lock_file)
"""
            proc = subprocess.Popen(
                [sys.executable, "-c", script],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                ready = proc.stdout.readline().strip() if proc.stdout is not None else ""
                self.assertEqual(ready, "ready")

                with self.assertRaises(DownloadError) as context:
                    service.run_download_latest()

                self.assertEqual(context.exception.code, "download_in_progress")
            finally:
                proc.terminate()
                proc.wait(timeout=5)
                if proc.stdout is not None:
                    proc.stdout.close()
                if proc.stderr is not None:
                    proc.stderr.close()

    def test_run_download_latest_with_retries_retries_retryable_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir), retry_attempts=3, retry_backoff_seconds=0)
            record = RemoteRecord(
                file_name="apc260304.zip",
                official_data_date="2026-03-04",
                release_date_raw="2026-03-05 00:52:55",
                file_size_bytes=123,
                download_url="https://example.com/apc260304.zip",
            )
            result = {
                "action": "skipped",
                "last_download": record.to_dict(status="skipped"),
                "latest_remote": record.to_dict(status="downloaded"),
            }
            attempt_counter = {"count": 0}

            def fake_attempt() -> tuple[RemoteRecord, dict[str, object]]:
                attempt_counter["count"] += 1
                if attempt_counter["count"] < 3:
                    raise DownloadError(
                        "upstream temporarily unavailable",
                        code="upstream_unavailable",
                        public_message=PUBLIC_ERROR_MESSAGES["upstream_unavailable"],
                        retryable=True,
                    )
                return record, result

            service._run_download_latest_attempt = fake_attempt  # type: ignore[method-assign]

            latest_remote, payload, attempts_used = service._run_download_latest_with_retries(trigger_source="test")
            self.assertEqual(attempt_counter["count"], 3)
            self.assertEqual(latest_remote.file_name, "apc260304.zip")
            self.assertEqual(payload["action"], "skipped")
            self.assertEqual(attempts_used, 3)

    def test_get_cookies_reuses_cached_cookie_jar_until_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir), cookie_cache_ttl_seconds=300)
            acquire_calls: list[dict[str, str]] = []

            def fake_acquire_cookies() -> dict[str, str]:
                cookies = {"session": f"cookie-{len(acquire_calls) + 1}"}
                acquire_calls.append(cookies)
                return cookies

            service._acquire_cookies = fake_acquire_cookies  # type: ignore[method-assign]

            first_cookies, first_from_cache = service._get_cookies()
            second_cookies, second_from_cache = service._get_cookies()

            self.assertEqual(first_cookies, {"session": "cookie-1"})
            self.assertEqual(second_cookies, {"session": "cookie-1"})
            self.assertFalse(first_from_cache)
            self.assertTrue(second_from_cache)
            self.assertEqual(len(acquire_calls), 1)

    def test_run_download_latest_attempt_refreshes_stale_cached_cookies_once(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir), cookie_cache_ttl_seconds=300)
            service.write_runtime_cache(
                "upstream_cookies",
                {"cookies": {"session": "cached-cookie"}},
                expires_at="2099-01-01T00:00:00+00:00",
            )
            fresh_cookie_calls: list[dict[str, str]] = []
            record = RemoteRecord(
                file_name="apc260305.zip",
                official_data_date="2026-03-05",
                release_date_raw="2026-03-06 00:00:00",
                file_size_bytes=1002,
                download_url="https://example.com/260305",
            )
            result = {
                "action": "skipped",
                "last_download": record.to_dict(status="skipped"),
                "latest_remote": record.to_dict(status="downloaded"),
            }

            def fake_acquire_cookies() -> dict[str, str]:
                cookies = {"session": "fresh-cookie"}
                fresh_cookie_calls.append(cookies)
                return cookies

            def fake_attempt_with_cookies(
                cookies: dict[str, str],
            ) -> tuple[RemoteRecord, dict[str, object]]:
                if cookies["session"] == "cached-cookie":
                    raise DownloadError(
                        "cached cookie expired",
                        code="upstream_unavailable",
                        public_message=PUBLIC_ERROR_MESSAGES["upstream_unavailable"],
                        retryable=True,
                    )
                return record, result

            service._acquire_cookies = fake_acquire_cookies  # type: ignore[method-assign]
            service._run_download_latest_attempt_with_cookies = fake_attempt_with_cookies  # type: ignore[method-assign]

            latest_remote, payload = service._run_download_latest_attempt()

            self.assertEqual(latest_remote.file_name, "apc260305.zip")
            self.assertEqual(payload["action"], "skipped")
            self.assertEqual(len(fresh_cookie_calls), 1)
            cached_payload = service.load_runtime_cache("upstream_cookies")
            self.assertEqual(cached_payload, {"cookies": {"session": "fresh-cookie"}, "cached_at": mock.ANY})

    def test_run_download_latest_with_retries_applies_random_jitter(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(
                Path(temp_dir),
                retry_attempts=2,
                retry_backoff_seconds=2.0,
                retry_jitter_ratio=0.5,
            )
            record = RemoteRecord(
                file_name="apc260304.zip",
                official_data_date="2026-03-04",
                release_date_raw="2026-03-05 00:52:55",
                file_size_bytes=123,
                download_url="https://example.com/apc260304.zip",
            )
            result = {
                "action": "skipped",
                "last_download": record.to_dict(status="skipped"),
                "latest_remote": record.to_dict(status="downloaded"),
            }
            attempt_counter = {"count": 0}

            def fake_attempt() -> tuple[RemoteRecord, dict[str, object]]:
                attempt_counter["count"] += 1
                if attempt_counter["count"] == 1:
                    raise DownloadError(
                        "upstream temporarily unavailable",
                        code="upstream_unavailable",
                        public_message=PUBLIC_ERROR_MESSAGES["upstream_unavailable"],
                        retryable=True,
                    )
                return record, result

            service._run_download_latest_attempt = fake_attempt  # type: ignore[method-assign]
            service._random = mock.Mock()
            service._random.uniform.return_value = 0.25

            with mock.patch("sync.service.time.sleep") as sleep_mock:
                latest_remote, payload, attempts_used = service._run_download_latest_with_retries(
                    trigger_source="test"
                )

            self.assertEqual(latest_remote.file_name, "apc260304.zip")
            self.assertEqual(payload["action"], "skipped")
            self.assertEqual(attempts_used, 2)
            sleep_mock.assert_called_once_with(2.5)

    def test_run_download_latest_sets_failure_cooldown_after_retryable_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir), failure_cooldown_seconds=300)

            def fake_run_download_latest_with_retries(
                *,
                trigger_source: str,
                job_run_id: int | None = None,
            ) -> tuple[RemoteRecord, dict[str, object], int]:
                raise DownloadError(
                    "upstream temporarily unavailable",
                    code="upstream_unavailable",
                    public_message=PUBLIC_ERROR_MESSAGES["upstream_unavailable"],
                    retryable=True,
                    attempts=3,
                )

            service._run_download_latest_with_retries = fake_run_download_latest_with_retries  # type: ignore[method-assign]

            with self.assertRaises(DownloadError):
                service.run_download_latest(trigger_source="timer")

            cooldown = service.get_failure_cooldown_snapshot()
            self.assertIsNotNone(cooldown["until"])
            self.assertGreater(cooldown["remaining_seconds"], 0)
            self.assertEqual(cooldown["error_code"], "upstream_unavailable")

    def test_run_download_latest_blocks_when_failure_cooldown_is_active(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir), failure_cooldown_seconds=300)
            service.set_failure_cooldown(
                until="2099-01-01T00:05:00+00:00",
                error_code="upstream_unavailable",
                message=PUBLIC_ERROR_MESSAGES["upstream_unavailable"],
                retryable=True,
            )

            with mock.patch.object(
                service,
                "_run_download_latest_with_retries",
                side_effect=AssertionError("cooldown should block before upstream attempts"),
            ):
                with self.assertRaises(DownloadError) as context:
                    service.run_download_latest(trigger_source="manual")

            self.assertEqual(context.exception.code, "cooldown_active")


class DownloadOnceCliTest(unittest.TestCase):
    def test_cli_success_payload_matches_api_contract(self) -> None:
        service = mock.Mock()
        service.run_download_latest.return_value = {
            "action": "skipped",
            "status": {
                "running": False,
                "last_action": "skipped",
                "downloads_dir": "/srv/app/downloads",
                "download_history": [
                    {
                        "file_name": "apc260305.zip",
                        "local_path": "/srv/app/downloads/apc260305.zip",
                        "status": "downloaded",
                    }
                ],
            },
            "latest_remote": {"file_name": "apc260305.zip", "local_path": "/srv/app/downloads/apc260305.zip"},
            "last_download": {
                "file_name": "apc260305.zip",
                "status": "skipped",
                "local_path": "/srv/app/downloads/apc260305.zip",
            },
        }

        with mock.patch.object(run_download_latest_once, "build_latest_service", return_value=service):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = run_download_latest_once.main()

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["data"]["operation"], "sync_latest_file")
        self.assertEqual(payload["data"]["resource"], "files/latest")
        self.assertEqual(payload["data"]["outcome"], "skipped")
        self.assertIn("summary", payload["data"])
        self.assertEqual(payload["data"]["trigger_policy"]["recommended_entrypoint"], "run_download_latest_once.py")
        self.assertNotIn("downloads_dir", payload["data"]["status"])
        self.assertNotIn("local_path", payload["data"]["latest_remote"])
        self.assertNotIn("local_path", payload["data"]["last_download"])
        self.assertNotIn("local_path", payload["data"]["status"]["download_history"][0])

    def test_cli_error_payload_matches_api_contract(self) -> None:
        service = mock.Mock()
        service.run_download_latest.side_effect = DownloadError(
            "已有下载任务在运行，请稍后再试。",
            code="download_in_progress",
            public_message=PUBLIC_ERROR_MESSAGES["download_in_progress"],
        )
        service.build_status.return_value = {
            "running": True,
            "downloads_dir": "/srv/app/downloads",
            "last_download": {
                "file_name": "apc260305.zip",
                "local_path": "/srv/app/downloads/apc260305.zip",
            },
        }

        with mock.patch.object(run_download_latest_once, "build_latest_service", return_value=service):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = run_download_latest_once.main()

        self.assertEqual(exit_code, 1)
        payload = json.loads(stdout.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "download_in_progress")
        self.assertIn("hint", payload["error"])
        self.assertEqual(payload["error"]["details"]["operation"], "sync_latest_file")
        self.assertEqual(payload["error"]["details"]["resource"], "files/latest")
        self.assertTrue(payload["error"]["details"]["status"]["running"])
        self.assertNotIn("downloads_dir", payload["error"]["details"]["status"])
        self.assertNotIn("local_path", payload["error"]["details"]["status"]["last_download"])

    def test_cli_download_error_still_emits_json_when_status_snapshot_fails(self) -> None:
        service = mock.Mock()
        service.run_download_latest.side_effect = DownloadError(
            "已有下载任务在运行，请稍后再试。",
            code="download_in_progress",
            public_message=PUBLIC_ERROR_MESSAGES["download_in_progress"],
        )
        service.build_status.side_effect = RuntimeError("db unavailable")

        with mock.patch.object(run_download_latest_once, "build_latest_service", return_value=service):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = run_download_latest_once.main()

        self.assertEqual(exit_code, 1)
        payload = json.loads(stdout.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "download_in_progress")
        self.assertEqual(payload["error"]["details"]["operation"], "sync_latest_file")
        self.assertEqual(payload["error"]["details"]["resource"], "files/latest")
        self.assertFalse(payload["error"]["details"]["status"]["running"])

    def test_cli_internal_error_still_emits_json_when_status_snapshot_fails(self) -> None:
        service = mock.Mock()
        service.run_download_latest.side_effect = RuntimeError("sqlite locked")
        service.build_status.side_effect = RuntimeError("db unavailable")

        with mock.patch.object(run_download_latest_once, "build_latest_service", return_value=service):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = run_download_latest_once.main()

        self.assertEqual(exit_code, 1)
        payload = json.loads(stdout.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "internal_error")
        self.assertEqual(payload["error"]["details"]["operation"], "sync_latest_file")
        self.assertEqual(payload["error"]["details"]["resource"], "files/latest")
        self.assertFalse(payload["error"]["details"]["status"]["running"])

    def test_cli_status_fallback_payload_matches_contract_when_status_snapshot_fails(self) -> None:
        service = mock.Mock()
        service.run_download_latest.side_effect = DownloadError(
            "已有下载任务在运行，请稍后再试。",
            code="download_in_progress",
            public_message=PUBLIC_ERROR_MESSAGES["download_in_progress"],
        )
        service.build_status.side_effect = RuntimeError("db unavailable")

        with mock.patch.object(run_download_latest_once, "build_latest_service", return_value=service):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = run_download_latest_once.main()

        self.assertEqual(exit_code, 1)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(
            payload["error"]["details"]["status"],
            {
                "running": False,
                "last_checked_at": None,
                "last_action": None,
                "latest_remote": None,
                "last_download": None,
                "last_error": None,
            },
        )
