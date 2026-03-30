"""
dataspec 単位のセットアップ / 差分更新を transaction 的に実行する。
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import List, Optional

from archive_store import DataspecArchive
from config import (
    JVLINK_SID,
    OPTION_NORMAL,
    OPTION_SETUP_NO_DLG,
    SETUP_DATASPECS,
    SETUP_FROM_TIME,
)
from job_state import JobState
from jvlink_session import JvLinkSession, ReadResult
from raw_writer import RawFileWriter

logger = logging.getLogger(__name__)


class NoCommittedSnapshotError(RuntimeError):
    pass


class JobRunner:
    def __init__(
        self,
        archive_dir: str | Path,
        jvlink_temp_dir: str = "C:\\JVLinkTemp",
        sid: str = JVLINK_SID,
    ):
        self._archive_dir = Path(archive_dir)
        self._jvlink_temp_dir = Path(jvlink_temp_dir)
        self._sid = sid

    def run_all_setup(
        self,
        fromtime: str = SETUP_FROM_TIME,
        dataspecs: Optional[List[str]] = None,
        option: int = OPTION_SETUP_NO_DLG,
    ) -> dict:
        targets = dataspecs or list(SETUP_DATASPECS)
        results = {}
        for ds in targets:
            try:
                results[ds] = self.run_setup(ds, fromtime, option)
            except Exception:
                logger.exception("setup failed unexpectedly for %s", ds)
                results[ds] = "failed"
        return results

    def run_all_diff(
        self,
        dataspecs: Optional[List[str]] = None,
        option: int = OPTION_NORMAL,
    ) -> dict:
        targets = dataspecs or list(SETUP_DATASPECS)
        results = {}
        for ds in targets:
            try:
                results[ds] = self.run_diff(ds, option)
            except NoCommittedSnapshotError as exc:
                logger.warning("%s: %s", ds, exc)
                results[ds] = "skipped"
            except Exception:
                logger.exception("update failed unexpectedly for %s", ds)
                results[ds] = "failed"
        return results

    def run_setup(
        self,
        dataspec: str,
        fromtime: str = SETUP_FROM_TIME,
        option: int = OPTION_SETUP_NO_DLG,
    ) -> str:
        return self._run_dataspec(
            dataspec=dataspec,
            mode="setup",
            fromtime=fromtime,
            option=option,
        )

    def run_diff(
        self,
        dataspec: str,
        option: int = OPTION_NORMAL,
    ) -> str:
        store = DataspecArchive(self._archive_dir, dataspec)
        current = store.load_current_ref()
        if not current:
            raise NoCommittedSnapshotError(
                f"{dataspec}: no committed snapshot exists yet; run setup first"
            )
        return self._run_dataspec(
            dataspec=dataspec,
            mode="update",
            fromtime=current["last_successful_timestamp"],
            option=option,
        )

    def _run_dataspec(
        self,
        dataspec: str,
        mode: str,
        fromtime: str,
        option: int,
    ) -> str:
        store = DataspecArchive(self._archive_dir, dataspec)
        self._jvlink_temp_dir.mkdir(parents=True, exist_ok=True)

        with store.acquire_lock():
            state, resume_after = self._open_or_resume_state(store, dataspec, mode, fromtime, option)
            writer = RawFileWriter(state.run_dir)
            writer.cleanup_temps()
            session = JvLinkSession(sid=self._sid)

            try:
                ret = session.init()
                if ret != 0:
                    state.mark_failed(f"JVInit failed: {ret}")
                    return "failed"

                ret = session.set_save_path(str(self._jvlink_temp_dir))
                if ret != 0:
                    state.mark_failed(f"JVSetSavePath failed: {ret}")
                    return "failed"

                ret_code, read_count, dl_count, timestamp = session.open(dataspec, fromtime, option)
                state.update_open_result(read_count, dl_count, timestamp)
                if ret_code != 0:
                    state.mark_failed(f"JVOpen failed: {ret_code}")
                    return "failed"

                if not session.wait_for_download(dl_count):
                    state.mark_failed("download wait failed")
                    return "failed"

                self._read_loop(session, writer, state, resume_after=resume_after)
                writer.close()
                self._record_closed_entry(writer, state)

                ref = store.commit_run(
                    run_dir=state.run_dir,
                    mode=mode,
                    option=option,
                    fromtime=fromtime,
                    returned_timestamp=timestamp,
                )
                state.mark_completed(ref["commit_id"])
                store.cleanup_run(state.run_dir)
                return "completed"

            except Exception as exc:
                writer.abort()
                state.mark_failed(str(exc))
                raise

            finally:
                session.close()

    def _open_or_resume_state(
        self,
        store: DataspecArchive,
        dataspec: str,
        mode: str,
        fromtime: str,
        option: int,
    ) -> tuple[JobState, Optional[str]]:
        existing_run = store.find_resumable_run(mode=mode, fromtime=fromtime, option=option)
        if existing_run:
            state = JobState(existing_run)
            if not state.load():
                raise RuntimeError(f"{dataspec}: failed to load resumable run {existing_run}")
            state.increment_attempt()
            logger.info("%s: resume run %s", dataspec, state.run_id)
            return state, state.last_completed_filename or None

        run_id = store.create_run_id()
        run_dir = store.create_run_dir(run_id)
        state = JobState(run_dir)
        state.create(
            dataspec=dataspec,
            run_id=run_id,
            mode=mode,
            option=option,
            fromtime=fromtime,
        )
        logger.info("%s: start new run %s", dataspec, run_id)
        return state, None

    def _record_closed_entry(self, writer: RawFileWriter, state: JobState) -> None:
        entry = writer.consume_closed_entry()
        if entry is not None:
            state.update_file_completed(
                filename=entry.logical_filename,
                records=writer.total_records,
                nbytes=writer.total_bytes,
            )

    def _read_loop(
        self,
        session: JvLinkSession,
        writer: RawFileWriter,
        state: JobState,
        resume_after: Optional[str] = None,
    ) -> None:
        skipping = bool(resume_after)
        target_consumed = False
        last_skip_filename = None

        while True:
            result: ReadResult = session.read()
            ret = result.ret_code

            if ret == 0:
                break
            if ret == -1:
                continue
            if ret in (-2, -3):
                time.sleep(3)
                continue
            if ret < 0:
                raise RuntimeError(f"JVRead failed: {ret}")

            filename = result.filename
            if skipping:
                if not target_consumed:
                    if filename != last_skip_filename:
                        session.skip()
                        last_skip_filename = filename
                    if filename == resume_after:
                        target_consumed = True
                    continue

                if filename == resume_after:
                    if filename != last_skip_filename:
                        session.skip()
                        last_skip_filename = filename
                    continue

                skipping = False
                last_skip_filename = None

            changed = writer.ensure_file_for(filename)
            if changed:
                self._record_closed_entry(writer, state)

            writer.write_record(result.raw_bytes)
