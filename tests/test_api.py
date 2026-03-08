#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import unittest
import zipfile
from pathlib import Path

from fastapi.testclient import TestClient

from app.factory import create_app
from app.paths import (
    HEALTH_READY_PATH,
    PUBLIC_LATEST_FILE_DOWNLOAD_PATH,
    PUBLIC_STATUS_PATH,
)
from tests.common import make_service


class AppStartupTest(unittest.TestCase):
    def test_app_startup_does_not_require_internal_api_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            with TestClient(create_app(service, run_startup_checks=True)):
                pass

    def test_app_startup_resets_running_flag_when_checks_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            state = service.load_state()
            state["running"] = True
            service.write_state(state)

            with TestClient(create_app(service, run_startup_checks=True)):
                pass

            self.assertFalse(service.load_state()["running"])

    def test_app_startup_repairs_download_history_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()
            latest_path = service.downloads_dir / "apc260304.zip"
            with zipfile.ZipFile(latest_path, "w") as archive:
                archive.writestr("latest.txt", "latest")

            with TestClient(create_app(service, run_startup_checks=True)):
                pass

            state = service.load_state()
            self.assertEqual(state["last_download"]["file_name"], "apc260304.zip")


class PublicDownloadEndpointTest(unittest.TestCase):
    def make_client(self, temp_service) -> TestClient:
        return TestClient(create_app(temp_service, run_startup_checks=False))

    def test_public_download_latest_serves_latest_zip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()

            latest_path = service.downloads_dir / "apc260304.zip"
            with zipfile.ZipFile(latest_path, "w") as archive:
                archive.writestr("latest.txt", "latest")

            with self.make_client(service) as client:
                response = client.get(PUBLIC_LATEST_FILE_DOWNLOAD_PATH)

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers["content-type"], "application/zip")
            self.assertIn("attachment;", response.headers.get("content-disposition", ""))
            self.assertIn("apc260304.zip", response.headers.get("content-disposition", ""))
            self.assertIn("etag", response.headers)
            self.assertIn("last-modified", response.headers)
            self.assertEqual(response.content[:2], b"PK")

    def test_public_download_latest_returns_404_when_no_file_exists(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()

            with self.make_client(service) as client:
                response = client.get(PUBLIC_LATEST_FILE_DOWNLOAD_PATH)

            body = response.json()
            self.assertEqual(response.status_code, 404)
            self.assertFalse(body["ok"])
            self.assertEqual(body["error"]["code"], "latest_file_not_found")

    def test_public_download_latest_returns_304_for_matching_etag(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()

            latest_path = service.downloads_dir / "apc260304.zip"
            with zipfile.ZipFile(latest_path, "w") as archive:
                archive.writestr("latest.txt", "latest")

            with self.make_client(service) as client:
                first_response = client.get(PUBLIC_LATEST_FILE_DOWNLOAD_PATH)
                etag = first_response.headers["etag"]
                response = client.get(PUBLIC_LATEST_FILE_DOWNLOAD_PATH, headers={"If-None-Match": etag})

            self.assertEqual(response.status_code, 304)
            self.assertEqual(response.content, b"")
            self.assertEqual(response.headers.get("etag"), etag)

    def test_public_download_latest_returns_304_for_matching_last_modified(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()

            latest_path = service.downloads_dir / "apc260304.zip"
            with zipfile.ZipFile(latest_path, "w") as archive:
                archive.writestr("latest.txt", "latest")

            with self.make_client(service) as client:
                first_response = client.get(PUBLIC_LATEST_FILE_DOWNLOAD_PATH)
                last_modified = first_response.headers["last-modified"]
                response = client.get(PUBLIC_LATEST_FILE_DOWNLOAD_PATH, headers={"If-Modified-Since": last_modified})

            self.assertEqual(response.status_code, 304)
            self.assertEqual(response.content, b"")
            self.assertEqual(response.headers.get("last-modified"), last_modified)

    def test_public_download_latest_does_not_apply_gzip_encoding(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()

            latest_path = service.downloads_dir / "apc260304.zip"
            with zipfile.ZipFile(latest_path, "w") as archive:
                archive.writestr("latest.txt", "latest")

            with self.make_client(service) as client:
                response = client.get(
                    PUBLIC_LATEST_FILE_DOWNLOAD_PATH,
                    headers={"Accept-Encoding": "gzip"},
                )

            self.assertEqual(response.status_code, 200)
            self.assertNotEqual(response.headers.get("content-encoding"), "gzip")

    def test_public_download_latest_is_rate_limited(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()

            latest_path = service.downloads_dir / "apc260304.zip"
            with zipfile.ZipFile(latest_path, "w") as archive:
                archive.writestr("latest.txt", "latest")

            with TestClient(
                create_app(
                    service,
                    run_startup_checks=False,
                    rate_limit_rules={PUBLIC_LATEST_FILE_DOWNLOAD_PATH: (1, 60)},
                )
            ) as client:
                first_response = client.get(PUBLIC_LATEST_FILE_DOWNLOAD_PATH)
                second_response = client.get(PUBLIC_LATEST_FILE_DOWNLOAD_PATH)

            self.assertEqual(first_response.status_code, 200)
            self.assertEqual(second_response.status_code, 429)
            self.assertEqual(second_response.json()["error"]["code"], "rate_limited")
            self.assertIn("Retry-After", second_response.headers)


class PublicApiContractTest(unittest.TestCase):
    def test_health_ready_is_available_without_business_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))

            with TestClient(create_app(service, run_startup_checks=False)) as client:
                ready_response = client.get(HEALTH_READY_PATH)

            self.assertEqual(ready_response.status_code, 200)
            self.assertEqual(ready_response.json()["data"]["status"], "ready")

    def test_health_ready_hides_internal_exception_details(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))

            def broken_load_state():
                raise RuntimeError("/tmp/internal-path.db locked")

            service.load_state = broken_load_state  # type: ignore[method-assign]

            with TestClient(create_app(service, run_startup_checks=False)) as client:
                response = client.get(HEALTH_READY_PATH)

            body = response.json()
            self.assertEqual(response.status_code, 503)
            self.assertFalse(body["ok"])
            self.assertEqual(body["error"]["code"], "service_not_ready")
            self.assertEqual(body["error"]["message"], "服务尚未就绪。")

    def test_docs_are_disabled_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))

            with TestClient(create_app(service, run_startup_checks=False)) as client:
                self.assertEqual(client.get("/health/live").status_code, 404)
                self.assertEqual(client.get("/").status_code, 404)
                self.assertEqual(client.get("/index.html").status_code, 404)
                self.assertEqual(client.get("/static/dashboard.js").status_code, 404)
                self.assertEqual(client.get("/docs").status_code, 404)
                self.assertEqual(client.get("/redoc").status_code, 404)
                self.assertEqual(client.get("/openapi.json").status_code, 404)
                self.assertEqual(client.get("/ops.html").status_code, 404)

    def test_public_status_uses_data_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            state = service.load_state()
            state["running"] = True
            state["last_action"] = "downloaded"
            service.write_state(state)
            service.create_job_run(
                trigger_source="manual",
                started_at="2026-03-06T13:49:00+08:00",
                status=service.build_status(),
            )

            with TestClient(create_app(service, run_startup_checks=False)) as client:
                response = client.get(PUBLIC_STATUS_PATH)

            body = response.json()
            self.assertEqual(response.status_code, 200)
            self.assertTrue(body["ok"])
            self.assertTrue(body["data"]["running"])
            self.assertEqual(body["data"]["last_action"], "downloaded")
            self.assertIn("last_success_at", body["data"])

    def test_public_status_is_rate_limited(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))

            with TestClient(
                create_app(
                    service,
                    run_startup_checks=False,
                    rate_limit_rules={PUBLIC_STATUS_PATH: (1, 60)},
                )
            ) as client:
                first_response = client.get(PUBLIC_STATUS_PATH)
                second_response = client.get(PUBLIC_STATUS_PATH)

            self.assertEqual(first_response.status_code, 200)
            self.assertEqual(second_response.status_code, 429)
            self.assertEqual(second_response.json()["error"]["code"], "rate_limited")
            self.assertIn("Retry-After", second_response.headers)

    def test_legacy_aliases_are_removed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))

            with TestClient(create_app(service, run_startup_checks=False)) as client:
                self.assertEqual(client.get("/api/v1/files").status_code, 404)
                self.assertEqual(client.get("/api/v1/latest-file").status_code, 404)
                self.assertEqual(client.get("/api/v1/download-latest").status_code, 404)
                self.assertEqual(client.get("/api/status").status_code, 404)
                self.assertEqual(client.post("/api/download-latest", json={}).status_code, 404)
