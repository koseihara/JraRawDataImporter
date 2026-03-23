"""
同期状態管理 — .sync_state.json で全dataspecの状態を一元管理

setup/update の進捗と最終タイムスタンプを記録し、
差分更新の fromtime 決定と中断検出に使用する。
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class SyncState:
    """
    .sync_state.json の構造:
    {
      "RACE": {
        "last_timestamp": "20260223133238",
        "last_synced_at": "2026-02-28T11:26:13+00:00",
        "file_count": 5027,
        "in_progress": false
      },
      ...
    }
    """

    def __init__(self, archive_dir: Path):
        self._path = Path(archive_dir) / ".sync_state.json"
        self._state: dict = {}

    # ----------------------------------------------------------
    # ロード・セーブ
    # ----------------------------------------------------------

    def load(self) -> None:
        if self._path.exists():
            with open(self._path, "r", encoding="utf-8") as f:
                self._state = json.load(f)

    def save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(self._state, f, ensure_ascii=False, indent=2)

    # ----------------------------------------------------------
    # 読み取り
    # ----------------------------------------------------------

    def get_last_timestamp(self, dataspec: str) -> Optional[str]:
        ds = self._state.get(dataspec, {})
        ts = ds.get("last_timestamp", "")
        return ts if ts else None

    def is_completed(self, dataspec: str) -> bool:
        ds = self._state.get(dataspec, {})
        return bool(ds.get("last_timestamp")) and not ds.get("in_progress", False)

    def is_in_progress(self, dataspec: str) -> bool:
        return self._state.get(dataspec, {}).get("in_progress", False)

    def get_file_count(self, dataspec: str) -> int:
        return self._state.get(dataspec, {}).get("file_count", 0)

    def get_last_synced_at(self, dataspec: str) -> str:
        return self._state.get(dataspec, {}).get("last_synced_at", "")

    def get_all(self) -> dict:
        return dict(self._state)

    # ----------------------------------------------------------
    # 書き込み
    # ----------------------------------------------------------

    def start_sync(self, dataspec: str) -> None:
        """同期開始を記録する。"""
        if dataspec not in self._state:
            self._state[dataspec] = {
                "last_timestamp": "",
                "last_synced_at": "",
                "file_count": 0,
                "in_progress": True,
            }
        else:
            self._state[dataspec]["in_progress"] = True
        self.save()

    def complete_sync(self, dataspec: str, timestamp: str, file_count: int) -> None:
        """同期完了を記録する。last_timestamp はここでのみ更新。"""
        self._state[dataspec] = {
            "last_timestamp": timestamp,
            "last_synced_at": _now_iso(),
            "file_count": file_count,
            "in_progress": False,
        }
        self.save()
        logger.info(
            "同期完了: %s (timestamp=%s, files=%d)",
            dataspec, timestamp, file_count,
        )

    def mark_failed(self, dataspec: str) -> None:
        """同期失敗を記録する（in_progress を維持して中断状態にする）。"""
        if dataspec in self._state:
            self._state[dataspec]["in_progress"] = True
        self.save()

    def reset_dataspec(self, dataspec: str) -> None:
        """dataspec の状態をリセットする（再セットアップ用）。"""
        self._state.pop(dataspec, None)
        self.save()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
