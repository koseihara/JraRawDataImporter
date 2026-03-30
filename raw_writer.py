"""
JV-Link の物理ファイル境界ごとに staging へ raw ファイルを書き出す。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from config import RAW_FILE_EXT


@dataclass
class StagedFile:
    logical_filename: str
    staging_name: str
    sha256: str
    byte_count: int
    record_count: int

    def to_dict(self) -> dict:
        return {
            "logical_filename": self.logical_filename,
            "staging_name": self.staging_name,
            "sha256": self.sha256,
            "byte_count": self.byte_count,
            "record_count": self.record_count,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "StagedFile":
        return cls(
            logical_filename=data["logical_filename"],
            staging_name=data["staging_name"],
            sha256=data["sha256"],
            byte_count=int(data["byte_count"]),
            record_count=int(data["record_count"]),
        )


class RawFileWriter:
    def __init__(self, run_dir: Path):
        self.run_dir = Path(run_dir)
        self.staging_dir = self.run_dir / "staging"
        self.manifest_path = self.run_dir / "candidate_manifest.jsonl"
        self.staging_dir.mkdir(parents=True, exist_ok=True)

        existing = self._load_existing_entries()
        self.total_files = len(existing)
        self.total_records = sum(entry.record_count for entry in existing.values())
        self.total_bytes = sum(entry.byte_count for entry in existing.values())
        self._sequence = len(list(self.staging_dir.glob(f"*{RAW_FILE_EXT}"))) + 1

        self._current_filename: Optional[str] = None
        self._current_fh = None
        self._current_hasher = None
        self._current_temp_path: Optional[Path] = None
        self._current_staging_name: Optional[str] = None
        self._current_byte_count = 0
        self._current_record_count = 0
        self._closed_entry: Optional[StagedFile] = None

    def ensure_file_for(self, filename: str) -> bool:
        if filename == self._current_filename and self._current_fh is not None:
            return False
        if self._current_fh is not None:
            self._finalize_current_file()
        self._open_temp_file(filename)
        return True

    def write_record(self, raw_bytes: bytes) -> None:
        if self._current_fh is None:
            raise RuntimeError("staging file is not open")

        self._current_fh.write(raw_bytes)
        self._current_hasher.update(raw_bytes)
        self._current_byte_count += len(raw_bytes)
        if raw_bytes and not raw_bytes.endswith(b"\n"):
            self._current_fh.write(b"\n")
            self._current_hasher.update(b"\n")
            self._current_byte_count += 1

        self._current_record_count += 1
        self.total_records += 1
        self.total_bytes += len(raw_bytes)
        if raw_bytes and not raw_bytes.endswith(b"\n"):
            self.total_bytes += 1

    def close(self) -> None:
        if self._current_fh is not None:
            self._finalize_current_file()

    def abort(self) -> None:
        if self._current_fh is None:
            return

        self._current_fh.close()
        self._current_fh = None
        if self._current_temp_path and self._current_temp_path.exists():
            self._current_temp_path.unlink()

        self._current_filename = None
        self._current_staging_name = None
        self._current_temp_path = None
        self._current_hasher = None
        self._current_byte_count = 0
        self._current_record_count = 0

    def cleanup_temps(self) -> None:
        for tmp in self.staging_dir.glob("*.tmp"):
            tmp.unlink()

    def consume_closed_entry(self) -> Optional[StagedFile]:
        entry = self._closed_entry
        self._closed_entry = None
        return entry

    def _open_temp_file(self, filename: str) -> None:
        safe_name = filename.replace("\\", "_").replace("/", "_").strip()
        if not safe_name:
            safe_name = "unknown"
        staging_name = f"{self._sequence:06d}__{safe_name}{RAW_FILE_EXT}"
        self._sequence += 1

        self._current_filename = filename
        self._current_staging_name = staging_name
        self._current_temp_path = self.staging_dir / f"{staging_name}.tmp"
        self._current_fh = open(self._current_temp_path, "wb")
        self._current_hasher = hashlib.sha256()
        self._current_byte_count = 0
        self._current_record_count = 0

    def _finalize_current_file(self) -> None:
        if self._current_fh is None:
            return

        self._current_fh.close()
        self._current_fh = None

        final_path = self.staging_dir / self._current_staging_name
        self._current_temp_path.replace(final_path)
        entry = StagedFile(
            logical_filename=self._current_filename,
            staging_name=self._current_staging_name,
            sha256=self._current_hasher.hexdigest(),
            byte_count=self._current_byte_count,
            record_count=self._current_record_count,
        )
        self._append_manifest(entry)
        self.total_files += 1
        self._closed_entry = entry

        self._current_filename = None
        self._current_staging_name = None
        self._current_temp_path = None
        self._current_hasher = None
        self._current_byte_count = 0
        self._current_record_count = 0

    def _append_manifest(self, entry: StagedFile) -> None:
        with open(self.manifest_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry.to_dict(), ensure_ascii=False) + "\n")

    def _load_existing_entries(self) -> dict[str, StagedFile]:
        entries: dict[str, StagedFile] = {}
        if not self.manifest_path.exists():
            return entries
        with open(self.manifest_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                entry = StagedFile.from_dict(json.loads(line))
                entries[entry.logical_filename] = entry
        return entries
