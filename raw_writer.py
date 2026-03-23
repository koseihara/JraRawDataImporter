"""
Raw データのファイル書き出し + manifest 管理

JV-Link の物理ファイル境界ごとに .jvdat ファイルを生成し、
manifest.jsonl にメタデータを記録する。
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config import RAW_FILE_EXT

logger = logging.getLogger(__name__)


class RawFileWriter:
    """
    JV-Link のレコードを物理ファイル単位で保存する。

    構成:
        <job_dir>/
            files/
                <filename1>.jvdat
                <filename2>.jvdat
                ...
            manifest.jsonl
    """

    def __init__(self, job_dir: Path):
        self._job_dir = Path(job_dir)
        self._files_dir = self._job_dir / "files"
        self._manifest_path = self._job_dir / "manifest.jsonl"

        # 現在書き込み中のファイル情報
        self._current_filename: Optional[str] = None
        self._current_fh = None
        self._current_hasher = None
        self._current_record_count: int = 0
        self._current_byte_count: int = 0
        self._current_started_at: Optional[str] = None

        # 累計統計
        self.total_files: int = 0
        self.total_records: int = 0
        self.total_bytes: int = 0

        # ディレクトリ作成
        self._files_dir.mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------
    # ファイル操作
    # ----------------------------------------------------------

    def ensure_file_for(self, filename: str) -> bool:
        """
        指定された JV-Link ファイル名に対応する出力ファイルを準備する。
        ファイル名が変わった場合は現在のファイルを閉じて新しいファイルを開く。

        Args:
            filename: JVRead/JVGets が返した物理ファイル名

        Returns:
            True = ファイルが切り替わった, False = 同じファイル
        """
        if filename == self._current_filename and self._current_fh is not None:
            return False

        # 前のファイルがあれば閉じる
        if self._current_fh is not None:
            self._close_current_file()

        # 新しいファイルを開く
        self._open_new_file(filename)
        return True

    def write_record(self, raw_bytes: bytes) -> None:
        """
        1レコード分の生バイト列を書き込む。

        Args:
            raw_bytes: cp932/SJIS の生バイト列（改行含む場合あり）
        """
        if self._current_fh is None:
            raise RuntimeError("ファイルが開かれていません")

        self._current_fh.write(raw_bytes)

        # レコード末尾に改行がなければ追加（ファイル内で行分割できるように）
        if raw_bytes and not raw_bytes.endswith(b"\n"):
            self._current_fh.write(b"\n")

        self._current_hasher.update(raw_bytes)
        self._current_record_count += 1
        self._current_byte_count += len(raw_bytes)
        self.total_records += 1
        self.total_bytes += len(raw_bytes)

    def close(self) -> None:
        """現在開いているファイルを閉じ、manifest エントリを書く。"""
        if self._current_fh is not None:
            self._close_current_file()

    # ----------------------------------------------------------
    # 内部メソッド
    # ----------------------------------------------------------

    def _open_new_file(self, filename: str) -> None:
        """新しい出力ファイルを開く。"""
        # ファイル名のサニタイズ（パス区切り文字を除去）
        safe_name = filename.replace("\\", "_").replace("/", "_").strip()
        if not safe_name:
            safe_name = f"unknown_{self.total_files:04d}"

        output_path = self._files_dir / f"{safe_name}{RAW_FILE_EXT}"

        # 同名ファイルが既に存在する場合はサフィックスを付ける
        if output_path.exists():
            base = output_path.stem
            idx = 1
            while output_path.exists():
                output_path = self._files_dir / f"{base}_{idx}{RAW_FILE_EXT}"
                idx += 1

        logger.info("ファイル開始: %s => %s", filename, output_path.name)

        self._current_filename = filename
        self._current_fh = open(output_path, "wb")
        self._current_hasher = hashlib.sha256()
        self._current_record_count = 0
        self._current_byte_count = 0
        self._current_started_at = _now_iso()
        self._current_output_path = output_path

    def _close_current_file(self) -> None:
        """現在のファイルを閉じ、manifest にエントリを追記する。"""
        if self._current_fh is None:
            return

        self._current_fh.close()
        self._current_fh = None

        entry = {
            "jvlink_filename": self._current_filename,
            "output_file": self._current_output_path.name,
            "record_count": self._current_record_count,
            "byte_count": self._current_byte_count,
            "sha256": self._current_hasher.hexdigest(),
            "started_at": self._current_started_at,
            "completed_at": _now_iso(),
        }

        self._write_manifest_entry(entry)
        self.total_files += 1

        logger.info(
            "ファイル完了: %s (%d records, %d bytes, sha256=%s)",
            self._current_filename,
            self._current_record_count,
            self._current_byte_count,
            entry["sha256"][:16] + "...",
        )

        self._current_filename = None
        self._current_hasher = None

    def _write_manifest_entry(self, entry: dict) -> None:
        """manifest.jsonl に1行追記する。"""
        with open(self._manifest_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    # ----------------------------------------------------------
    # プロパティ
    # ----------------------------------------------------------

    @property
    def current_filename(self) -> Optional[str]:
        return self._current_filename

    @property
    def job_dir(self) -> Path:
        return self._job_dir


def _now_iso() -> str:
    """UTC の ISO 8601 タイムスタンプを返す。"""
    return datetime.now(timezone.utc).isoformat()
