"""
Raw データのファイル書き出し — dataspec ディレクトリに直接保存

JV-Link の物理ファイル境界ごとに .jvdat ファイルを生成する。
同名ファイルは上書き（差分更新で最新版に置き換える）。
書き込みは .tmp 経由でアトミックに行う。
"""

import logging
from pathlib import Path
from typing import Optional

from config import RAW_FILE_EXT

logger = logging.getLogger(__name__)


class RawFileWriter:
    """
    JV-Link のレコードを dataspec ディレクトリに直接保存する。

    構成:
        <archive>/<dataspec>/
            <filename1>.jvdat
            <filename2>.jvdat
            ...
    """

    def __init__(self, dataspec_dir: Path):
        self._dir = Path(dataspec_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

        # 現在書き込み中のファイル情報
        self._current_filename: Optional[str] = None
        self._current_fh = None
        self._current_temp_path: Optional[Path] = None
        self._current_output_path: Optional[Path] = None

        # 累計統計
        self.total_files: int = 0
        self.total_records: int = 0
        self.total_bytes: int = 0

    # ----------------------------------------------------------
    # ファイル操作
    # ----------------------------------------------------------

    def ensure_file_for(self, filename: str) -> bool:
        """
        指定された JV-Link ファイル名に対応する出力ファイルを準備する。
        ファイル名が変わった場合は現在のファイルを閉じて新しいファイルを開く。

        Returns:
            True = ファイルが切り替わった, False = 同じファイル
        """
        if filename == self._current_filename and self._current_fh is not None:
            return False

        if self._current_fh is not None:
            self._finalize_current_file()

        self._open_temp_file(filename)
        return True

    def write_record(self, raw_bytes: bytes) -> None:
        """1レコード分の生バイト列を書き込む。"""
        if self._current_fh is None:
            raise RuntimeError("ファイルが開かれていません")

        self._current_fh.write(raw_bytes)

        # レコード末尾に改行がなければ追加
        if raw_bytes and not raw_bytes.endswith(b"\n"):
            self._current_fh.write(b"\n")

        self.total_records += 1
        self.total_bytes += len(raw_bytes)

    def close(self) -> None:
        """現在開いているファイルを閉じて .tmp をリネームする。"""
        if self._current_fh is not None:
            self._finalize_current_file()

    def cleanup_temps(self) -> None:
        """残った .tmp ファイルを削除する（前回中断の残骸）。"""
        for tmp in self._dir.glob("*.tmp"):
            logger.info("残存 .tmp 削除: %s", tmp.name)
            tmp.unlink()

    # ----------------------------------------------------------
    # 内部メソッド
    # ----------------------------------------------------------

    def _open_temp_file(self, filename: str) -> None:
        """新しい .tmp ファイルを開く。"""
        safe_name = filename.replace("\\", "_").replace("/", "_").strip()
        if not safe_name:
            safe_name = f"unknown_{self.total_files:04d}"

        self._current_output_path = self._dir / f"{safe_name}{RAW_FILE_EXT}"
        self._current_temp_path = self._current_output_path.with_suffix(".tmp")
        self._current_filename = filename
        self._current_fh = open(self._current_temp_path, "wb")

        logger.info("ファイル開始: %s", safe_name)

    def _finalize_current_file(self) -> None:
        """一時ファイルを閉じてアトミックリネーム。"""
        if self._current_fh is None:
            return

        self._current_fh.close()
        self._current_fh = None

        # .tmp → 本ファイルにリネーム（上書き）
        self._current_temp_path.replace(self._current_output_path)
        self.total_files += 1

        logger.info("ファイル完了: %s", self._current_filename)

        self._current_filename = None
        self._current_temp_path = None
        self._current_output_path = None

    # ----------------------------------------------------------
    # プロパティ
    # ----------------------------------------------------------

    @property
    def current_filename(self) -> Optional[str]:
        return self._current_filename
