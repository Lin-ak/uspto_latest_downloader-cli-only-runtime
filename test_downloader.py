#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from tests.test_api import *  # noqa: F401,F403
from tests.test_service import *  # noqa: F401,F403
from tests.test_storage import *  # noqa: F401,F403


if __name__ == "__main__":
    unittest.main()
