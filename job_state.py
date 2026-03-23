"""
ジョブ状態管理 — 中断再開のための永続化

job_state.json を使って、セットアップの中断・再開を可能にする。
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class JobState:
    """
    1つの dataspec に対するジョブの状態を管理する。

    状態ファイル (job_state.json) の構造:
    {
        "dataspec": "RACE",
        "mode": "setup",
        "option": 4,
        "fromtime": "19860101000000",
        "status": "running",             # running / completed / failed
        "last_processed_filename": "",    # setup再開用: 最後に処理完了したファイル名
        "last_file_timestamp": "",        # normal再開用: JVOpenが返すタイムスタンプ
        "read_count": 0,                  # JVOpenで返された読み取りファイル数
        "download_count": 0,              # JVOpenで返されたダウンロードファイル数
        "processed_files": 0,             # 処理済みファイル数
        "processed_records": 0,           # 処理済みレコード数
        "processed_bytes": 0,             # 処理済みバイト数
        "attempt_count": 0,               # 実行試行回数
        "started_at": "",
        "updated_at": "",
        "completed_at": "",
        "error_message": ""
    }
    """

    def __init__(self, job_dir: Path):
        self._path = Path(job_dir) / "job_state.json"
        self._state: dict = {}

    # ----------------------------------------------------------
    # 初期化・ロード
    # ----------------------------------------------------------

    def create(
        self,
        dataspec: str,
        mode: str,
        option: int,
        fromtime: str,
    ) -> None:
        """新規ジョブ状態を作成する。"""
        self._state = {
            "dataspec": dataspec,
            "mode": mode,
            "option": option,
            "fromtime": fromtime,
            "status": "running",
            "last_processed_filename": "",
            "last_file_timestamp": "",
            "read_count": 0,
            "download_count": 0,
            "processed_files": 0,
            "processed_records": 0,
            "processed_bytes": 0,
            "attempt_count": 1,
            "started_at": _now_iso(),
            "updated_at": _now_iso(),
            "completed_at": "",
            "error_message": "",
        }
        self._save()

    def load(self) -> bool:
        """
        既存の状態ファイルを読み込む。

        Returns:
            True=読み込み成功, False=ファイルなし
        """
        if not self._path.exists():
            return False

        with open(self._path, "r", encoding="utf-8") as f:
            self._state = json.load(f)

        logger.info(
            "ジョブ状態をロード: dataspec=%s, status=%s, last_file=%s, files=%d",
            self._state.get("dataspec"),
            self._state.get("status"),
            self._state.get("last_processed_filename", ""),
            self._state.get("processed_files", 0),
        )
        return True

    # ----------------------------------------------------------
    # 状態更新
    # ----------------------------------------------------------

    def update_open_result(
        self,
        read_count: int,
        download_count: int,
        last_file_timestamp: str,
    ) -> None:
        """JVOpen の結果を記録する。"""
        self._state["read_count"] = read_count
        self._state["download_count"] = download_count
        self._state["last_file_timestamp"] = last_file_timestamp
        self._save()

    def update_file_completed(self, filename: str, records: int, nbytes: int) -> None:
        """1ファイルの処理完了を記録する。"""
        self._state["last_processed_filename"] = filename
        self._state["processed_files"] += 1
        self._state["processed_records"] += records
        self._state["processed_bytes"] += nbytes
        self._state["updated_at"] = _now_iso()
        self._save()

    def mark_completed(self) -> None:
        """ジョブを完了状態にする。"""
        self._state["status"] = "completed"
        self._state["completed_at"] = _now_iso()
        self._state["updated_at"] = _now_iso()
        self._save()
        logger.info("ジョブ完了: %s", self._state["dataspec"])

    def mark_failed(self, error_message: str) -> None:
        """ジョブを失敗状態にする。"""
        self._state["status"] = "failed"
        self._state["error_message"] = error_message
        self._state["updated_at"] = _now_iso()
        self._save()
        logger.error("ジョブ失敗: %s — %s", self._state["dataspec"], error_message)

    def increment_attempt(self) -> None:
        """試行回数をインクリメントする。"""
        self._state["attempt_count"] = self._state.get("attempt_count", 0) + 1
        self._state["status"] = "running"
        self._state["error_message"] = ""
        self._state["updated_at"] = _now_iso()
        self._save()

    # ----------------------------------------------------------
    # プロパティ
    # ----------------------------------------------------------

    @property
    def status(self) -> str:
        return self._state.get("status", "")

    @property
    def last_processed_filename(self) -> str:
        return self._state.get("last_processed_filename", "")

    @property
    def last_file_timestamp(self) -> str:
        return self._state.get("last_file_timestamp", "")

    @property
    def is_resumable(self) -> bool:
        """再開可能かどうか（running or failed で、処理済みファイルがある）"""
        return (
            self._state.get("status") in ("running", "failed")
            and bool(self._state.get("last_processed_filename"))
        )

    @property
    def is_completed(self) -> bool:
        return self._state.get("status") == "completed"

    @property
    def dataspec(self) -> str:
        return self._state.get("dataspec", "")

    @property
    def state(self) -> dict:
        return dict(self._state)

    # ----------------------------------------------------------
    # 内部
    # ----------------------------------------------------------

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(self._state, f, ensure_ascii=False, indent=2)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
