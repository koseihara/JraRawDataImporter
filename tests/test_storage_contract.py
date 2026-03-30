from __future__ import annotations

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from archive_store import DataspecArchive, ManifestEntry


class StorageContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="jvlink_storage_")

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_manifest_entry_exposes_view_contract(self) -> None:
        entry = ManifestEntry(
            logical_filename="RAVM2024010120240101123456.jvd",
            object_sha256="ab" * 32,
            byte_count=128,
            record_count=3,
        )

        payload = entry.to_dict()
        self.assertEqual(payload["format_code"], "RA")
        self.assertEqual(
            payload["view_relpath"],
            r"RA/RAVM2024010120240101123456.jvd.jvdat".replace("\\", "/"),
        )

    def test_verify_detects_corrupt_object(self) -> None:
        store = DataspecArchive(self.temp_dir, "RACE")
        store.ensure_layout()

        commit_id = "test_commit"
        commit_dir = Path(self.temp_dir) / "RACE" / "commits" / commit_id
        commit_dir.mkdir(parents=True)
        entry = ManifestEntry(
            logical_filename="FILE_A",
            object_sha256="4a966536d06e69f31ab2467f022db7a645815414f6b4baa7acd33134524392df",
            byte_count=8,
            record_count=2,
        )
        (commit_dir / "manifest.jsonl").write_text(
            json.dumps(entry.to_dict(), ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        (commit_dir / "meta.json").write_text(
            json.dumps({"commit_id": commit_id}, ensure_ascii=False),
            encoding="utf-8",
        )
        object_path = store.object_path(entry.object_sha256)
        object_path.parent.mkdir(parents=True, exist_ok=True)
        object_path.write_text("BROKEN\n", encoding="utf-8")
        store.write_ref(
            "current",
            {
                "dataspec": "RACE",
                "commit_id": commit_id,
                "last_successful_timestamp": "20260329120000",
                "file_count": 1,
            },
        )

        result = store.verify()
        self.assertFalse(result["ok"])
        self.assertTrue(any("sha256 mismatch" in error for error in result["errors"]))


if __name__ == "__main__":
    unittest.main()
