from __future__ import annotations

import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import job_runner
from archive_store import DataspecArchive
from jvlink_session import ReadResult


class FakeSession:
    scenarios: list[list[object]] = []
    instances: list["FakeSession"] = []

    def __init__(self, sid: str = "UNKNOWN", prog_id: str = "FAKE"):
        if not self.scenarios:
            raise RuntimeError("no more scenarios")
        self.events = list(self.scenarios.pop(0))
        self.skipped = 0
        self.closed = False
        self.instances.append(self)

    def init(self) -> int:
        return 0

    def set_save_path(self, path: str) -> int:
        return 0

    def open(self, dataspec: str, fromtime: str, option: int):
        return 0, 5, 0, "20260329120000"

    def wait_for_download(self, download_count: int) -> bool:
        return True

    def read(self) -> ReadResult:
        if not self.events:
            return ReadResult(0, b"", "")
        event = self.events.pop(0)
        if event == "FAIL":
            raise RuntimeError("synthetic failure")
        filename, payload = event
        return ReadResult(1, payload, filename)

    def skip(self) -> int:
        self.skipped += 1
        return 0

    def close(self) -> None:
        self.closed = True


class RunnerResumeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="jvlink_runner_")
        FakeSession.scenarios = []
        FakeSession.instances = []

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_failed_run_resumes_without_republishing_partial_file(self) -> None:
        FakeSession.scenarios = [
            [
                ("FILE_A", b"A1"),
                ("FILE_A", b"A2"),
                ("FILE_B", b"B1"),
                "FAIL",
            ],
            [
                ("FILE_A", b"A1"),
                ("FILE_A", b"A2"),
                ("FILE_B", b"B1"),
                ("FILE_B", b"B2"),
                ("FILE_C", b"C1"),
            ],
        ]

        runner = job_runner.JobRunner(
            archive_dir=self.temp_dir,
            jvlink_temp_dir=str(Path(self.temp_dir) / "temp"),
        )

        with patch.object(job_runner, "JvLinkSession", FakeSession):
            with self.assertRaises(RuntimeError):
                runner.run_setup("RACE", fromtime="20200101000000", option=4)

            result = runner.run_setup("RACE", fromtime="20200101000000", option=4)

        self.assertEqual(result, "completed")

        store = DataspecArchive(self.temp_dir, "RACE")
        status = store.status()
        self.assertIsNotNone(status["current"])
        manifest = store.current_manifest()
        self.assertEqual(sorted(manifest), ["FILE_A", "FILE_B", "FILE_C"])
        self.assertGreaterEqual(FakeSession.instances[1].skipped, 1)


if __name__ == "__main__":
    unittest.main()
