#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
import zipfile
from contextlib import closing
from pathlib import Path
from unittest import mock

from downloader import DownloadError, PUBLIC_ERROR_MESSAGES, RemoteRecord

from tests.common import make_service


class DownloaderStorageTest(unittest.TestCase):
    def test_load_state_creates_default_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            state = service.load_state()
            self.assertFalse(state["running"])
            self.assertIsNone(state["last_checked_at"])
            self.assertEqual(state["download_history"], [])
            self.assertEqual(service.db_path, service.runtime_dir / "app.db")
            self.assertTrue(service.db_path.exists())
            self.assertFalse(service.state_path.exists())

    def test_load_state_migrates_legacy_state_json_to_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()
            legacy_state = {
                "running": False,
                "last_checked_at": "2026-03-06T20:54:01+08:00",
                "last_action": "skipped",
                "latest_remote": {
                    "file_name": "apc260305.zip",
                    "official_data_date": "2026-03-05",
                    "release_date_raw": "2026-03-06 00:54:52",
                    "file_size_bytes": 32167494,
                    "download_url": "https://example.com/apc260305.zip",
                    "local_path": str(service.downloads_dir / "apc260305.zip"),
                    "downloaded_at": "2026-03-06T20:54:26+08:00",
                    "status": "downloaded",
                },
                "last_download": None,
                "last_error": None,
                "download_history": [],
            }
            service.state_path.write_text(json.dumps(legacy_state, ensure_ascii=False), encoding="utf-8")

            migrated_state = service.load_state()
            self.assertEqual(migrated_state["last_action"], "skipped")
            self.assertTrue(service.db_path.exists())
            self.assertFalse(service.state_path.exists())
            self.assertTrue(service.state_path.with_suffix(".json.migrated").exists())

    def test_repair_download_history_from_disk_sets_latest_fields_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()

            target_path = service.downloads_dir / "apc260304.zip"
            with zipfile.ZipFile(target_path, "w") as archive:
                archive.writestr("latest.txt", "latest")

            repaired = service.repair_download_history_from_disk(if_missing_only=True)
            state = service.load_state()

            self.assertEqual(repaired, 1)
            self.assertEqual(state["last_download"]["file_name"], "apc260304.zip")
            self.assertEqual(state["latest_remote"]["file_name"], "apc260304.zip")

    def test_resolve_latest_downloaded_file_returns_latest_local_zip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()

            older_path = service.downloads_dir / "apc260303.zip"
            with zipfile.ZipFile(older_path, "w") as archive:
                archive.writestr("older.txt", "older")

            latest_path = service.downloads_dir / "apc260304.zip"
            with zipfile.ZipFile(latest_path, "w") as archive:
                archive.writestr("latest.txt", "latest")

            resolved = service.resolve_latest_downloaded_file()
            self.assertIsNotNone(resolved)

            target_path, entry = resolved  # type: ignore[misc]
            self.assertEqual(target_path, latest_path)
            self.assertEqual(entry["file_name"], "apc260304.zip")

    def test_resolve_latest_downloaded_file_skips_corrupt_history_entry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()

            corrupt_path = service.downloads_dir / "apc260305.zip"
            corrupt_path.write_bytes(b"PK\x03\x04")

            valid_path = service.downloads_dir / "apc260304.zip"
            with zipfile.ZipFile(valid_path, "w") as archive:
                archive.writestr("valid.txt", "valid")

            state = service.load_state()
            state["download_history"] = [
                {
                    "file_name": "apc260305.zip",
                    "official_data_date": "2026-03-05",
                    "release_date_raw": "2026-03-06 00:00:00",
                    "file_size_bytes": corrupt_path.stat().st_size,
                    "download_url": "https://example.com/apc260305.zip",
                    "local_path": str(corrupt_path),
                    "downloaded_at": "2026-03-06T10:00:00+08:00",
                    "status": "downloaded",
                },
                {
                    "file_name": "apc260304.zip",
                    "official_data_date": "2026-03-04",
                    "release_date_raw": "2026-03-05 00:00:00",
                    "file_size_bytes": valid_path.stat().st_size,
                    "download_url": "https://example.com/apc260304.zip",
                    "local_path": str(valid_path),
                    "downloaded_at": "2026-03-05T10:00:00+08:00",
                    "status": "downloaded",
                },
            ]
            service.write_state(state)

            resolved = service.resolve_latest_downloaded_file()
            self.assertIsNotNone(resolved)
            target_path, entry = resolved  # type: ignore[misc]
            self.assertEqual(target_path, valid_path)
            self.assertEqual(entry["file_name"], "apc260304.zip")

    def test_external_local_path_is_ignored_by_download_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()

            external_dir = Path(temp_dir) / "external"
            external_dir.mkdir()
            external_path = external_dir / "apc260304.zip"
            with zipfile.ZipFile(external_path, "w") as archive:
                archive.writestr("external.txt", "external")

            state = service.load_state()
            state["download_history"] = [
                {
                    "file_name": "apc260304.zip",
                    "official_data_date": "2026-03-04",
                    "release_date_raw": "2026-03-05 00:00:00",
                    "file_size_bytes": external_path.stat().st_size,
                    "download_url": "https://data.uspto.gov/ui/datasets/products/files/TRTDXFAP/apc260304.zip",
                    "local_path": str(external_path),
                    "downloaded_at": "2026-03-05T10:00:00+08:00",
                    "status": "downloaded",
                },
            ]
            service.write_state(state)

            resolved = service.resolve_latest_downloaded_file()

            self.assertIsNone(resolved)

    def test_download_resolution_does_not_revalidate_full_zip_structure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()

            latest_path = service.downloads_dir / "apc260304.zip"
            with zipfile.ZipFile(latest_path, "w") as archive:
                archive.writestr("latest.txt", "latest")

            with mock.patch.object(
                service,
                "_is_valid_zip_file",
                side_effect=AssertionError("read paths should not use full ZIP revalidation"),
            ):
                resolved = service.resolve_latest_downloaded_file()

            self.assertIsNotNone(resolved)
            target_path, entry = resolved  # type: ignore[misc]
            self.assertEqual(target_path, latest_path)
            self.assertEqual(entry["file_name"], "apc260304.zip")

    def test_build_public_status_hides_internal_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            state = service.load_state()
            state["running"] = True
            state["last_checked_at"] = "2026-03-06T13:50:00+08:00"
            state["last_action"] = "downloaded"
            state["latest_remote"] = {
                "file_name": "apc260305.zip",
                "official_data_date": "2026-03-05",
                "release_date_raw": "2026-03-06 00:52:55",
                "file_size_bytes": 123456,
                "download_url": "https://example.com/apc260305.zip",
                "local_path": "/tmp/hidden.zip",
                "downloaded_at": "",
                "status": "available",
            }
            state["last_download"] = {
                "file_name": "apc260304.zip",
                "official_data_date": "2026-03-04",
                "release_date_raw": "2026-03-05 00:52:55",
                "file_size_bytes": 123455,
                "download_url": "https://example.com/apc260304.zip",
                "local_path": "/tmp/hidden-too.zip",
                "downloaded_at": "2026-03-06T13:38:09+08:00",
                "status": "downloaded",
            }
            state["last_error"] = {
                "code": "upstream_unavailable",
                "message": "HTTP 500 /srv/app/runtime/state.json",
                "public_message": PUBLIC_ERROR_MESSAGES["upstream_unavailable"],
                "at": "2026-03-06T13:51:00+08:00",
                "retryable": True,
            }
            service.write_state(state)

            payload = service.build_public_status()
            self.assertTrue(payload["running"])
            self.assertEqual(payload["last_action"], "downloaded")
            self.assertEqual(payload["latest_remote"]["file_name"], "apc260305.zip")
            self.assertEqual(payload["last_download"]["file_name"], "apc260304.zip")
            self.assertEqual(payload["last_error"]["code"], "upstream_unavailable")
            self.assertEqual(payload["last_error"]["message"], PUBLIC_ERROR_MESSAGES["upstream_unavailable"])
            self.assertTrue(payload["last_error"]["retryable"])
            self.assertNotIn("local_path", payload["latest_remote"])
            self.assertNotIn("local_path", payload["last_download"])

    def test_repair_download_history_refreshes_stale_latest_fields_from_disk(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()

            latest_path = service.downloads_dir / "apc260304.zip"
            with zipfile.ZipFile(latest_path, "w") as archive:
                archive.writestr("latest.txt", "latest")

            missing_path = service.downloads_dir / "apc260305.zip"
            stale_entry = {
                "file_name": "apc260305.zip",
                "official_data_date": "2026-03-05",
                "release_date_raw": "2026-03-06 00:00:00",
                "file_size_bytes": 123,
                "download_url": "https://data.uspto.gov/ui/datasets/products/files/TRTDXFAP/apc260305.zip",
                "local_path": str(missing_path),
                "downloaded_at": "2026-03-06T10:00:00+08:00",
                "status": "downloaded",
            }

            state = service.load_state()
            state["latest_remote"] = dict(stale_entry)
            state["last_download"] = dict(stale_entry)
            service.write_state(state)

            repaired = service.repair_download_history_from_disk(if_missing_only=False)
            repaired_state = service.load_state()
            public_status = service.build_public_status()

            self.assertEqual(repaired, 1)
            self.assertEqual(repaired_state["latest_remote"]["file_name"], "apc260304.zip")
            self.assertEqual(repaired_state["last_download"]["file_name"], "apc260304.zip")
            self.assertEqual(public_status["latest_remote"]["file_name"], "apc260304.zip")
            self.assertEqual(public_status["last_download"]["file_name"], "apc260304.zip")

    def test_run_download_latest_records_success_job_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
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

            def fake_run_download_latest_with_retries(
                *,
                trigger_source: str,
                job_run_id: int | None = None,
            ) -> tuple[RemoteRecord, dict[str, object], int]:
                self.assertEqual(trigger_source, "test")
                self.assertIsNotNone(job_run_id)
                return record, result, 2

            service._run_download_latest_with_retries = fake_run_download_latest_with_retries  # type: ignore[method-assign]

            payload = service.run_download_latest(trigger_source="test")
            self.assertEqual(payload["action"], "skipped")

            job_runs_payload = service.list_job_runs(limit=10, offset=0)
            self.assertEqual(job_runs_payload["pagination"]["total"], 1)
            job_run = job_runs_payload["job_runs"][0]
            self.assertEqual(job_run["operation"], "sync_latest_file")
            self.assertEqual(job_run["resource"], "files/latest")
            self.assertEqual(job_run["trigger_source"], "test")
            self.assertEqual(job_run["outcome"], "skipped")
            self.assertEqual(job_run["attempts"], 2)
            self.assertIn("summary", job_run)
            self.assertEqual(job_run["consecutive_failures"], 0)
            self.assertIsNone(job_run["error"])
            self.assertEqual(job_run["latest_remote"]["file_name"], "apc260304.zip")
            self.assertEqual(job_run["status"]["last_action"], "skipped")
            self.assertIsNotNone(payload["status"]["last_success_at"])

    def test_run_download_latest_success_path_builds_status_only_when_needed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
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
            original_build_status = service.build_status

            def fake_run_download_latest_with_retries(
                *,
                trigger_source: str,
                job_run_id: int | None = None,
            ) -> tuple[RemoteRecord, dict[str, object], int]:
                return record, result, 1

            service._run_download_latest_with_retries = fake_run_download_latest_with_retries  # type: ignore[method-assign]
            service.build_status = mock.Mock(side_effect=original_build_status)  # type: ignore[method-assign]

            payload = service.run_download_latest(trigger_source="test")

            self.assertEqual(payload["action"], "skipped")
            self.assertEqual(service.build_status.call_count, 3)

    def test_run_download_latest_records_failed_job_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))

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

            with closing(sqlite3.connect(service.db_path)) as connection:
                connection.row_factory = sqlite3.Row
                row = connection.execute(
                    "SELECT outcome, trigger_source, attempts, error_code, error_public_message FROM job_runs ORDER BY id DESC LIMIT 1"
                ).fetchone()

            self.assertIsNotNone(row)
            self.assertEqual(row["outcome"], "error")
            self.assertEqual(row["trigger_source"], "timer")
            self.assertEqual(row["attempts"], 3)
            self.assertEqual(row["error_code"], "upstream_unavailable")
            self.assertEqual(row["error_public_message"], PUBLIC_ERROR_MESSAGES["upstream_unavailable"])

            job_runs_payload = service.list_job_runs(limit=10, offset=0)
            self.assertEqual(job_runs_payload["job_runs"][0]["consecutive_failures"], 1)
            self.assertIn("summary", job_runs_payload["job_runs"][0])

    def test_run_download_latest_preserves_original_failure_when_finalize_status_snapshot_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))

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
            service.build_status = mock.Mock(  # type: ignore[method-assign]
                side_effect=[
                    {"running": False},
                    RuntimeError("status snapshot broken"),
                    RuntimeError("status snapshot broken"),
                    RuntimeError("status snapshot broken"),
                ]
            )

            with self.assertRaises(DownloadError) as context:
                service.run_download_latest(trigger_source="timer")

            self.assertEqual(context.exception.code, "upstream_unavailable")

            job_runs_payload = service.list_job_runs(limit=10, offset=0)
            self.assertEqual(job_runs_payload["pagination"]["total"], 1)
            self.assertEqual(job_runs_payload["job_runs"][0]["error"]["code"], "upstream_unavailable")

    def test_run_download_latest_preserves_original_failure_when_error_state_writeback_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            original_load_state = service.load_state
            load_state_calls = {"count": 0}

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

            def fake_load_state():
                load_state_calls["count"] += 1
                if load_state_calls["count"] == 2:
                    raise RuntimeError("error state writeback failed")
                return original_load_state()

            service._run_download_latest_with_retries = fake_run_download_latest_with_retries  # type: ignore[method-assign]
            service.build_status = mock.Mock(return_value={"running": False})  # type: ignore[method-assign]
            service.load_state = fake_load_state  # type: ignore[method-assign]

            with self.assertRaises(DownloadError) as context:
                service.run_download_latest(trigger_source="timer")

            self.assertEqual(context.exception.code, "upstream_unavailable")
            job_runs_payload = service.list_job_runs(limit=10, offset=0)
            self.assertEqual(job_runs_payload["pagination"]["total"], 1)
            self.assertEqual(job_runs_payload["job_runs"][0]["error"]["code"], "upstream_unavailable")

    def test_run_download_latest_resets_running_flag_when_job_run_creation_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))

            def fake_create_job_run(**kwargs: object) -> int:
                raise RuntimeError("job run insert failed")

            service.create_job_run = fake_create_job_run  # type: ignore[method-assign]

            with self.assertRaisesRegex(RuntimeError, "job run insert failed"):
                service.run_download_latest(trigger_source="test")

            self.assertFalse(service.load_state()["running"])
            lock_file = service._acquire_run_lock()
            service._release_run_lock(lock_file)
