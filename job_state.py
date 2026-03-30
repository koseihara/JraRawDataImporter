"""
run_state.json を使って dataspec 単位の実行状態を永続化する。
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


class JobState:
    def __init__(self, run_dir: Path):
        self.run_dir = Path(run_dir)
        self.path = self.run_dir / "run_state.json"
        self._state: dict = {}

    def create(
        self,
        dataspec: str,
        run_id: str,
        mode: str,
        option: int,
        fromtime: str,
    ) -> None:
        now = _now_iso()
        self._state = {
            "dataspec": dataspec,
            "run_id": run_id,
            "status": "running",
            "mode": mode,
            "option": option,
            "fromtime": fromtime,
            "open_read_count": 0,
            "open_download_count": 0,
            "open_last_file_timestamp": "",
            "last_completed_filename": "",
            "processed_files": 0,
            "processed_records": 0,
            "processed_bytes": 0,
            "attempt": 1,
            "started_at": now,
            "updated_at": now,
            "completed_at": "",
            "error_message": "",
            "current_commit_id": None,
        }
        self.save()

    def load(self) -> bool:
        if not self.path.exists():
            return False
        self._state = json.loads(self.path.read_text(encoding="utf-8"))
        return True

    def save(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(f"{self.path.name}.tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self._state, fh, ensure_ascii=False, indent=2)
        tmp.replace(self.path)

    def increment_attempt(self) -> None:
        self._state["attempt"] = self._state.get("attempt", 0) + 1
        self._state["status"] = "running"
        self._state["error_message"] = ""
        self._state["updated_at"] = _now_iso()
        self.save()

    def update_open_result(
        self,
        read_count: int,
        download_count: int,
        last_file_timestamp: str,
    ) -> None:
        self._state["open_read_count"] = read_count
        self._state["open_download_count"] = download_count
        self._state["open_last_file_timestamp"] = last_file_timestamp
        self._state["updated_at"] = _now_iso()
        self.save()

    def update_file_completed(self, filename: str, records: int, nbytes: int) -> None:
        self._state["last_completed_filename"] = filename
        self._state["processed_files"] = self._state.get("processed_files", 0) + 1
        self._state["processed_records"] = records
        self._state["processed_bytes"] = nbytes
        self._state["updated_at"] = _now_iso()
        self.save()

    def mark_failed(self, error_message: str) -> None:
        self._state["status"] = "failed"
        self._state["error_message"] = error_message
        self._state["updated_at"] = _now_iso()
        self.save()

    def mark_completed(self, commit_id: str) -> None:
        now = _now_iso()
        self._state["status"] = "completed"
        self._state["current_commit_id"] = commit_id
        self._state["completed_at"] = now
        self._state["updated_at"] = now
        self.save()

    @property
    def status(self) -> str:
        return self._state.get("status", "")

    @property
    def dataspec(self) -> str:
        return self._state.get("dataspec", "")

    @property
    def run_id(self) -> str:
        return self._state.get("run_id", "")

    @property
    def mode(self) -> str:
        return self._state.get("mode", "")

    @property
    def option(self) -> int:
        return int(self._state.get("option", 0))

    @property
    def fromtime(self) -> str:
        return self._state.get("fromtime", "")

    @property
    def last_completed_filename(self) -> str:
        return self._state.get("last_completed_filename", "")

    @property
    def is_resumable(self) -> bool:
        return self.status in {"running", "failed"}

    @property
    def state(self) -> dict:
        return dict(self._state)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
