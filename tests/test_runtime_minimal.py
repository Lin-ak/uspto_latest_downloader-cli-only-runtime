#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from core.common import DownloadError
from tests.common import ROOT_DIR, make_service


class RuntimeBehaviorMinimalTest(unittest.TestCase):
    def test_run_download_latest_blocks_when_failure_cooldown_is_active(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir), failure_cooldown_seconds=300)
            service.set_failure_cooldown(
                until="2099-01-01T00:05:00+00:00",
                error_code="upstream_unavailable",
                message="upstream unavailable",
                retryable=True,
            )

            with mock.patch.object(
                service.upstream_gateway,
                "run_latest_attempt",
                side_effect=AssertionError("cooldown should block before upstream access"),
            ):
                with self.assertRaises(DownloadError) as context:
                    service.run_download_latest(trigger_source="manual")

            self.assertEqual(context.exception.code, "cooldown_active")

    def test_run_download_latest_rejects_cross_process_lock_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)
            service = make_service(root_dir)
            ready_path = root_dir / "lock-ready"

            script = f"""
import sys
import time
from pathlib import Path
sys.path.insert(0, {str(ROOT_DIR)!r})
from tests.common import make_service
root = Path({str(root_dir)!r})
service = make_service(root)
handle = service.run_lock.acquire()
(root / "lock-ready").write_text("ready", encoding="utf-8")
time.sleep(5)
service.run_lock.release(handle)
"""

            process = subprocess.Popen([sys.executable, "-c", script])
            try:
                deadline = time.time() + 5
                while not ready_path.exists():
                    if time.time() > deadline:
                        self.fail("lock holder did not become ready in time")
                    time.sleep(0.1)

                with self.assertRaises(DownloadError) as context:
                    service.run_download_latest(trigger_source="manual")

                self.assertEqual(context.exception.code, "download_in_progress")
            finally:
                process.terminate()
                process.wait(timeout=5)

    def test_load_state_migrates_legacy_state_json_to_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.ensure_layout()
            legacy_state = {
                "running": False,
                "last_checked_at": "2026-03-06T20:54:01+08:00",
                "last_action": "skipped",
                "latest_remote": None,
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

    def test_runtime_artifacts_use_owner_only_permissions(self) -> None:
        if os.name == "nt":
            self.skipTest("permission mode checks are POSIX-only")

        with tempfile.TemporaryDirectory() as temp_dir:
            service = make_service(Path(temp_dir))
            service.load_state()
            handle = service.run_lock.acquire()
            try:
                runtime_mode = service.runtime_dir.stat().st_mode & 0o777
                db_mode = service.db_path.stat().st_mode & 0o777
                lock_mode = service.lock_path.stat().st_mode & 0o777

                self.assertEqual(runtime_mode, 0o700)
                self.assertEqual(db_mode, 0o600)
                self.assertEqual(lock_mode, 0o600)
            finally:
                service.run_lock.release(handle)
