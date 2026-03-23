"""
ジョブ実行エンジン

セットアップ取得・差分取得を dataspec ごとに逐次実行する。
SyncState (.sync_state.json) で同期状態を管理する。
"""

import logging
import shutil
import time
from pathlib import Path
from typing import List, Optional

from config import (
    SETUP_DATASPECS,
    OPTION_NORMAL,
    OPTION_SETUP_NO_DLG,
    SETUP_FROM_TIME,
    JVLINK_SID,
)
from jvlink_session import JvLinkSession, ReadResult
from raw_writer import RawFileWriter
from sync_state import SyncState

logger = logging.getLogger(__name__)


class JobRunner:
    """
    セットアップ・差分取得ジョブの実行を管理する。

    archive_dir/
        RACE/                    ← dataspec ディレクトリ（フラット）
            RACE20260101.jvd.jvdat
            ...
        DIFF/
            ...
        .sync_state.json         ← 全 dataspec の同期状態
    """

    def __init__(
        self,
        archive_dir: str | Path,
        jvlink_temp_dir: str = "C:\\JVLinkTemp",
        sid: str = JVLINK_SID,
    ):
        self._archive_dir = Path(archive_dir)
        self._jvlink_temp_dir = jvlink_temp_dir
        self._sid = sid

    # ----------------------------------------------------------
    # セットアップ
    # ----------------------------------------------------------

    def run_all_setup(
        self,
        fromtime: str = SETUP_FROM_TIME,
        dataspecs: Optional[List[str]] = None,
        option: int = OPTION_SETUP_NO_DLG,
    ) -> dict:
        """全 dataspec のセットアップを順次実行する。"""
        targets = dataspecs or list(SETUP_DATASPECS)
        results = {}

        logger.info("=" * 60)
        logger.info("セットアップ開始: %d dataspec", len(targets))
        logger.info("fromtime=%s, option=%d", fromtime, option)
        logger.info("archive_dir=%s", self._archive_dir)
        logger.info("=" * 60)

        for i, ds in enumerate(targets, 1):
            logger.info("")
            logger.info("--- [%d/%d] %s ---", i, len(targets), ds)

            try:
                status = self.run_setup(ds, fromtime, option)
                results[ds] = status
            except Exception:
                logger.exception("dataspec=%s で予期しないエラー", ds)
                results[ds] = "failed"

            logger.info("--- %s: %s ---", ds, results[ds])

        logger.info("")
        logger.info("=" * 60)
        logger.info("セットアップ完了サマリー:")
        for ds, status in results.items():
            logger.info("  %s: %s", ds, status)
        logger.info("=" * 60)

        return results

    def run_setup(
        self,
        dataspec: str,
        fromtime: str = SETUP_FROM_TIME,
        option: int = OPTION_SETUP_NO_DLG,
    ) -> str:
        """
        1つの dataspec のセットアップを実行する。

        中断中 (in_progress) なら既存ファイルを削除してやり直す。
        """
        ds_dir = self._archive_dir / dataspec
        state = SyncState(self._archive_dir)
        state.load()

        # 中断中なら既存ファイルを削除してやり直し
        if state.is_in_progress(dataspec):
            logger.info("%s: 前回中断のため最初からやり直し", dataspec)
            shutil.rmtree(ds_dir, ignore_errors=True)
            state.reset_dataspec(dataspec)

        state.start_sync(dataspec)

        writer = RawFileWriter(ds_dir)
        writer.cleanup_temps()
        session = JvLinkSession(sid=self._sid)

        try:
            # JVInit
            ret = session.init()
            if ret != 0:
                state.mark_failed(dataspec)
                logger.error("JVInit failed: %d", ret)
                return "failed"

            # JVSetSavePath
            ret = session.set_save_path(self._jvlink_temp_dir)
            if ret != 0:
                state.mark_failed(dataspec)
                logger.error("JVSetSavePath failed: %d", ret)
                return "failed"

            # JVOpen
            ret_code, read_count, dl_count, timestamp = session.open(
                dataspec, fromtime, option,
            )
            if ret_code != 0:
                state.mark_failed(dataspec)
                logger.error("JVOpen failed: ret=%d", ret_code)
                return "failed"

            logger.info(
                "JVOpen OK: read_count=%d, dl_count=%d, timestamp=%s",
                read_count, dl_count, timestamp,
            )

            # ダウンロード完了待ち
            if not session.wait_for_download(dl_count):
                state.mark_failed(dataspec)
                logger.error("ダウンロードタイムアウト")
                return "failed"

            # 読み取りループ
            self._read_loop(session, writer)

            # 完了
            writer.close()
            state.complete_sync(dataspec, timestamp, writer.total_files)
            return "completed"

        except Exception:
            writer.close()
            state.mark_failed(dataspec)
            raise

        finally:
            session.close()

    # ----------------------------------------------------------
    # 差分取得
    # ----------------------------------------------------------

    def run_all_diff(
        self,
        dataspecs: Optional[List[str]] = None,
        option: int = OPTION_NORMAL,
    ) -> dict:
        """全 dataspec の差分取得を順次実行する。"""
        targets = dataspecs or list(SETUP_DATASPECS)
        results = {}

        logger.info("=" * 60)
        logger.info("差分取得開始: %d dataspec", len(targets))
        logger.info("archive_dir=%s", self._archive_dir)
        logger.info("=" * 60)

        for i, ds in enumerate(targets, 1):
            logger.info("")
            logger.info("--- [%d/%d] %s (diff) ---", i, len(targets), ds)

            try:
                status = self.run_diff(ds, option)
                results[ds] = status
            except RuntimeError as e:
                logger.warning("%s: %s", ds, e)
                results[ds] = "skipped"
            except Exception:
                logger.exception("dataspec=%s で予期しないエラー", ds)
                results[ds] = "failed"

            logger.info("--- %s: %s ---", ds, results[ds])

        logger.info("")
        logger.info("=" * 60)
        logger.info("差分取得完了サマリー:")
        for ds, status in results.items():
            logger.info("  %s: %s", ds, status)
        logger.info("=" * 60)

        return results

    def run_diff(
        self,
        dataspec: str,
        option: int = OPTION_NORMAL,
    ) -> str:
        """
        1つの dataspec の差分取得を実行する。

        last_timestamp を fromtime として使用。
        セットアップ未完了の場合は RuntimeError。
        """
        state = SyncState(self._archive_dir)
        state.load()

        if not state.is_completed(dataspec):
            raise RuntimeError(
                f"{dataspec}: セットアップが未完了です。先に setup を実行してください"
            )

        fromtime = state.get_last_timestamp(dataspec)
        logger.info("%s: 差分取得 fromtime=%s", dataspec, fromtime)

        ds_dir = self._archive_dir / dataspec
        writer = RawFileWriter(ds_dir)
        writer.cleanup_temps()

        state.start_sync(dataspec)
        session = JvLinkSession(sid=self._sid)

        try:
            ret = session.init()
            if ret != 0:
                state.mark_failed(dataspec)
                logger.error("JVInit failed: %d", ret)
                return "failed"

            ret = session.set_save_path(self._jvlink_temp_dir)
            if ret != 0:
                state.mark_failed(dataspec)
                logger.error("JVSetSavePath failed: %d", ret)
                return "failed"

            ret_code, read_count, dl_count, timestamp = session.open(
                dataspec, fromtime, option,
            )
            if ret_code != 0:
                state.mark_failed(dataspec)
                logger.error("JVOpen failed: ret=%d", ret_code)
                return "failed"

            logger.info(
                "JVOpen OK: read_count=%d, dl_count=%d, timestamp=%s",
                read_count, dl_count, timestamp,
            )

            if not session.wait_for_download(dl_count):
                state.mark_failed(dataspec)
                logger.error("ダウンロードタイムアウト")
                return "failed"

            self._read_loop(session, writer)

            writer.close()

            # 全ファイル処理完了後にのみ timestamp を更新
            file_count = state.get_file_count(dataspec) + writer.total_files
            state.complete_sync(dataspec, timestamp, file_count)
            return "completed"

        except Exception:
            writer.close()
            state.mark_failed(dataspec)
            raise

        finally:
            session.close()

    # ----------------------------------------------------------
    # 読み取りループ
    # ----------------------------------------------------------

    def _read_loop(
        self,
        session: JvLinkSession,
        writer: RawFileWriter,
    ) -> None:
        """JVRead をループし、レコードを RawFileWriter に書き込む。"""
        record_count = 0

        while True:
            result: ReadResult = session.read()
            ret = result.ret_code

            # EOF
            if ret == 0:
                logger.info("EOF到達 (records=%d)", record_count)
                break

            # ファイル切り替え通知
            if ret == -1:
                continue

            # ダウンロード中
            if ret in (-2, -3):
                logger.warning("ダウンロード中 (ret=%d)、3秒待機", ret)
                time.sleep(3)
                continue

            # エラー
            if ret < 0:
                raise RuntimeError(f"JVRead エラー: ret={ret}")

            # 正常読み取り (ret > 0)
            writer.ensure_file_for(result.filename)
            writer.write_record(result.raw_bytes)
            record_count += 1

            if record_count % 10000 == 0:
                logger.info(
                    "  進捗: %d records, file=%s",
                    record_count, result.filename,
                )
