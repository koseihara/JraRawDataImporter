"""
ジョブ実行エンジン

セットアップ取得を dataspec ごとに逐次実行し、
中断・再開をサポートする。
"""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from config import (
    SETUP_DATASPECS,
    OPTION_SETUP_NO_DLG,
    SETUP_FROM_TIME,
    JVLINK_SID,
)
from jvlink_session import JvLinkSession, ReadResult
from raw_writer import RawFileWriter
from job_state import JobState

logger = logging.getLogger(__name__)


class JobRunner:
    """
    セットアップジョブの実行を管理する。

    archive_dir/
        setup/
            RACE/
                20260226_101530/    ← ジョブディレクトリ
                    files/
                    manifest.jsonl
                    job_state.json
            DIFF/
                ...
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
    # セットアップ全体実行
    # ----------------------------------------------------------

    def run_all_setup(
        self,
        fromtime: str = SETUP_FROM_TIME,
        dataspecs: Optional[List[str]] = None,
        option: int = OPTION_SETUP_NO_DLG,
    ) -> dict:
        """
        全 dataspec のセットアップを順次実行する。

        Args:
            fromtime: 取得開始日時
            dataspecs: 対象 dataspec リスト（None=全蓄積系）
            option: JVOpen の option 値（デフォルト=4）

        Returns:
            {dataspec: "completed"/"failed"/"skipped"} の辞書
        """
        targets = dataspecs or SETUP_DATASPECS
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
                status = self.run_single_setup(ds, fromtime, option)
                results[ds] = status
            except Exception as e:
                logger.exception("dataspec=%s で予期しないエラー", ds)
                results[ds] = "failed"

            logger.info("--- %s: %s ---", ds, results[ds])

        # サマリー
        logger.info("")
        logger.info("=" * 60)
        logger.info("セットアップ完了サマリー:")
        for ds, status in results.items():
            logger.info("  %s: %s", ds, status)
        logger.info("=" * 60)

        return results

    # ----------------------------------------------------------
    # 単一 dataspec のセットアップ
    # ----------------------------------------------------------

    def run_single_setup(
        self,
        dataspec: str,
        fromtime: str = SETUP_FROM_TIME,
        option: int = OPTION_SETUP_NO_DLG,
    ) -> str:
        """
        1つの dataspec のセットアップを実行する。
        既存ジョブが中断状態なら再開を試みる。

        Returns:
            "completed" / "failed" / "skipped"
        """
        # ジョブディレクトリの決定
        ds_dir = self._archive_dir / "setup" / dataspec
        job_dir = self._find_or_create_job_dir(ds_dir, dataspec, fromtime, option)

        state = JobState(job_dir)

        # 既に完了済みならスキップ
        if state.load() and state.is_completed:
            logger.info("%s は完了済み、スキップ", dataspec)
            return "skipped"

        # 中断再開 or 新規開始
        is_resume = state.is_resumable
        if is_resume:
            state.increment_attempt()
            logger.info(
                "%s を再開 (last_file=%s)",
                dataspec, state.last_processed_filename,
            )
        else:
            state.create(dataspec, "setup", option, fromtime)
            logger.info("%s を新規開始", dataspec)

        # JV-Link セッション
        session = JvLinkSession(sid=self._sid)
        writer = RawFileWriter(job_dir)

        try:
            # 1) JVInit
            ret = session.init()
            if ret != 0:
                state.mark_failed(f"JVInit failed: {ret}")
                return "failed"

            # 2) JVSetSavePath
            ret = session.set_save_path(self._jvlink_temp_dir)
            if ret != 0:
                state.mark_failed(f"JVSetSavePath failed: {ret}")
                return "failed"

            # 3) JVOpen
            ret_code, read_count, dl_count, timestamp = session.open(
                dataspec, fromtime, option,
            )
            if ret_code != 0:
                state.mark_failed(f"JVOpen failed: ret={ret_code}")
                return "failed"

            state.update_open_result(read_count, dl_count, timestamp)

            # 4) ダウンロード完了待ち
            if not session.wait_for_download(dl_count):
                state.mark_failed("ダウンロードタイムアウト")
                return "failed"

            # 5) 読み取りループ（再開対応あり）
            self._read_loop(
                session, writer, state,
                resume_after=state.last_processed_filename if is_resume else None,
            )

            # 6) 完了
            writer.close()
            state.mark_completed()
            return "completed"

        except Exception as e:
            writer.close()
            state.mark_failed(str(e))
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
        state: JobState,
        resume_after: Optional[str] = None,
    ) -> None:
        """
        JVRead をループし、レコードを RawFileWriter に書き込む。

        Args:
            session: JV-Link セッション
            writer: ファイル書き出し
            state: ジョブ状態
            resume_after: このファイル名まではスキップする（再開用）
        """
        skipping = resume_after is not None and resume_after != ""
        skip_count = 0
        record_count = 0
        prev_filename = ""

        if skipping:
            logger.info("再開: %s 以降までスキップ中...", resume_after)

        while True:
            result: ReadResult = session.read()
            ret = result.ret_code

            # --- EOF ---
            if ret == 0:
                logger.info(
                    "EOF到達 (records=%d, skipped_files=%d)",
                    record_count, skip_count,
                )
                break

            # --- ファイル切り替え ---
            if ret == -1:
                # JV-Linkの仕様: -1 はファイル境界の通知
                # 次の read() で新しいファイルのレコードが来る
                continue

            # --- ダウンロード中 ---
            if ret in (-2, -3):
                logger.warning("ダウンロード中 (ret=%d)、3秒待機", ret)
                time.sleep(3)
                continue

            # --- エラー ---
            if ret < 0:
                raise RuntimeError(f"JVRead エラー: ret={ret}")

            # --- 正常読み取り (ret > 0) ---

            filename = result.filename

            # スキップモード（再開時）
            if skipping:
                if filename != resume_after:
                    # まだ目的のファイルに達していない → スキップ
                    if filename != prev_filename:
                        skip_count += 1
                        prev_filename = filename
                    continue
                else:
                    # 目的のファイルに到達 → このファイルもスキップし、次から処理
                    if filename != prev_filename:
                        skip_count += 1
                        prev_filename = filename
                    continue

            # ファイル名が変わったことを検出
            if filename != prev_filename:
                if prev_filename and skipping is False:
                    # 前のファイルのクローズ処理は writer.ensure_file_for で自動的に行われる
                    pass

                # スキップモードの終了判定
                # resume_after のファイルを完全に読み飛ばし終えた後の最初の新ファイル
                if resume_after and not skipping and skip_count > 0 and prev_filename == resume_after:
                    pass  # 通常処理へ

                prev_filename = filename

            # --- ここでスキップ完了判定 ---
            # スキップ対象ファイルを通過し、新しいファイルに入ったら解除
            if skipping and filename != resume_after:
                skipping = False
                logger.info(
                    "スキップ完了 (%d files skipped)、処理再開: %s",
                    skip_count, filename,
                )

            if skipping:
                continue

            # --- 通常のレコード書き込み ---
            writer.ensure_file_for(filename)
            writer.write_record(result.raw_bytes)
            record_count += 1

            # 定期的な進捗ログと状態保存
            if record_count % 10000 == 0:
                logger.info(
                    "  進捗: %d records, file=%s",
                    record_count, filename,
                )

            if record_count % 50000 == 0:
                state.update_file_completed(
                    filename, writer.total_records, writer.total_bytes,
                )

        # 最後のファイル情報を状態に保存
        if prev_filename:
            state.update_file_completed(
                prev_filename, writer.total_records, writer.total_bytes,
            )

    # ----------------------------------------------------------
    # ジョブディレクトリ管理
    # ----------------------------------------------------------

    def _find_or_create_job_dir(
        self,
        ds_dir: Path,
        dataspec: str,
        fromtime: str,
        option: int,
    ) -> Path:
        """
        既存の中断ジョブがあればそのディレクトリを返し、
        なければ新しいタイムスタンプ付きディレクトリを作成する。
        """
        # 既存の未完了ジョブを探す
        if ds_dir.exists():
            for d in sorted(ds_dir.iterdir(), reverse=True):
                if d.is_dir():
                    state_file = d / "job_state.json"
                    if state_file.exists():
                        state = JobState(d)
                        if state.load() and not state.is_completed:
                            logger.info("既存の中断ジョブを発見: %s", d)
                            return d

        # 新規作成
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        job_dir = ds_dir / timestamp
        job_dir.mkdir(parents=True, exist_ok=True)
        logger.info("新規ジョブディレクトリ: %s", job_dir)
        return job_dir
