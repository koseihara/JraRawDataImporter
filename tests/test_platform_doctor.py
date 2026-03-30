from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jvlink_raw_fetcher.platform import run_doctor


class DoctorTests(unittest.TestCase):
    def test_doctor_reports_writable_paths(self) -> None:
        with tempfile.TemporaryDirectory() as archive_dir, tempfile.TemporaryDirectory() as temp_dir:
            checks = run_doctor(archive_dir=archive_dir, temp_dir=temp_dir)

        by_name = {check.name: check for check in checks}
        self.assertIn("archive_dir", by_name)
        self.assertIn("jvlink_temp_dir", by_name)
        self.assertEqual(by_name["archive_dir"].status, "PASS")
        self.assertEqual(by_name["jvlink_temp_dir"].status, "PASS")
        self.assertIn("python_bitness", by_name)
