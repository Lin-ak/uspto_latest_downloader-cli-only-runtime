#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from unittest import mock

from core.common import PUBLIC_ERROR_MESSAGES
import run_download_latest_once


class DownloadOnceCliMinimalTest(unittest.TestCase):
    def test_cli_success_emits_sanitized_payload(self) -> None:
        service = mock.Mock()
        service.db_path = "/tmp/runtime/app.db"
        service.run_download_latest.return_value = {
            "action": "skipped",
            "status": {
                "running": False,
                "last_action": "skipped",
                "downloads_dir": "/srv/app/downloads",
                "download_history": [
                    {
                        "file_name": "apc260307.zip",
                        "local_path": "/srv/app/downloads/apc260307.zip",
                        "status": "downloaded",
                    }
                ],
            },
            "latest_remote": {"file_name": "apc260307.zip", "local_path": "/srv/app/downloads/apc260307.zip"},
            "last_download": {
                "file_name": "apc260307.zip",
                "status": "skipped",
                "local_path": "/srv/app/downloads/apc260307.zip",
            },
        }

        with mock.patch.object(run_download_latest_once, "build_latest_service", return_value=service):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = run_download_latest_once.main()

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["data"]["outcome"], "skipped")
        self.assertNotIn("downloads_dir", payload["data"]["status"])
        self.assertNotIn("local_path", payload["data"]["latest_remote"])
        self.assertNotIn("local_path", payload["data"]["last_download"])
        self.assertNotIn("local_path", payload["data"]["status"]["download_history"][0])

    def test_cli_internal_error_hides_raw_exception_text(self) -> None:
        service = mock.Mock()
        service.db_path = "/tmp/runtime/app.db"
        service.run_download_latest.side_effect = RuntimeError("sqlite locked: /tmp/secret-path")
        service.build_status.return_value = {
            "running": False,
            "latest_remote": None,
            "last_download": None,
            "last_error": None,
        }

        with mock.patch.object(run_download_latest_once, "build_latest_service", return_value=service):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = run_download_latest_once.main()

        body = stdout.getvalue()
        self.assertEqual(exit_code, 1)
        self.assertNotIn("sqlite locked", body)
        self.assertNotIn("/tmp/secret-path", body)
        payload = json.loads(body)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "internal_error")
        self.assertEqual(payload["error"]["message"], PUBLIC_ERROR_MESSAGES["internal_error"])
