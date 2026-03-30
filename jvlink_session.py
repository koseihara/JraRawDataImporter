"""
JV-Link COM インターフェースの薄いラッパー。
"""

from __future__ import annotations

import logging
import time
from typing import NamedTuple

try:
    import win32com.client

    HAS_WIN32COM = True
except ImportError:
    HAS_WIN32COM = False

from config import (
    JVDATA_ENCODING,
    JVLINK_PROG_ID,
    JVLINK_SID,
    READ_BUFFER_SIZE,
    STATUS_POLL_INTERVAL,
    STATUS_POLL_TIMEOUT,
)

logger = logging.getLogger(__name__)


class ReadResult(NamedTuple):
    ret_code: int
    raw_bytes: bytes
    filename: str


class JvLinkSession:
    def __init__(self, prog_id: str = JVLINK_PROG_ID, sid: str = JVLINK_SID):
        self._prog_id = prog_id
        self._sid = sid
        self._com = None
        self._is_open = False

    def init(self) -> int:
        if not HAS_WIN32COM:
            raise RuntimeError("pywin32 is required. Install it with `pip install pywin32`.")
        logger.info("COM object: %s", self._prog_id)
        self._com = win32com.client.Dispatch(self._prog_id)
        ret = int(self._com.JVInit(self._sid))
        logger.info("JVInit(sid=%r) => %d", self._sid, ret)
        return ret

    def set_save_path(self, path: str) -> int:
        ret = int(self._com.JVSetSavePath(path))
        logger.info("JVSetSavePath(%r) => %d", path, ret)
        return ret

    def set_service_key(self, key: str) -> int:
        ret = int(self._com.JVSetServiceKey(key))
        logger.info("JVSetServiceKey() => %d", ret)
        return ret

    def open(
        self,
        dataspec: str,
        fromtime: str,
        option: int,
        react_key: str = "",
    ) -> tuple[int, int, int, str]:
        logger.info(
            "JVOpen(dataspec=%r, fromtime=%r, option=%d, react_key=%r)",
            dataspec,
            fromtime,
            option,
            react_key,
        )
        result = self._com.JVOpen(dataspec, fromtime, option, 0, 0, react_key)
        if not isinstance(result, tuple) or len(result) < 4:
            raise RuntimeError(
                "JVOpen returned an unsupported COM shape. "
                "Use 32-bit Python / pywin32 so out parameters marshal as a tuple."
            )

        ret_code = int(result[0])
        read_count = int(result[1])
        download_count = int(result[2])
        timestamp = str(result[3] or "")
        self._is_open = ret_code >= 0
        logger.info(
            "JVOpen => ret=%d, read_count=%d, download_count=%d, timestamp=%r",
            ret_code,
            read_count,
            download_count,
            timestamp,
        )
        return ret_code, read_count, download_count, timestamp

    def close(self) -> None:
        if self._com and self._is_open:
            self._com.JVClose()
            self._is_open = False
            logger.info("JVClose()")

    def status(self) -> int:
        return int(self._com.JVStatus())

    def wait_for_download(
        self,
        download_count: int,
        poll_interval: float = STATUS_POLL_INTERVAL,
        timeout: float = STATUS_POLL_TIMEOUT,
    ) -> bool:
        if download_count <= 0:
            logger.info("No download wait required (download_count=%d)", download_count)
            return True

        start = time.time()
        while True:
            current = self.status()
            elapsed = time.time() - start

            if current < 0:
                logger.error("JVStatus error during download wait: %d", current)
                return False

            if current >= download_count:
                logger.info(
                    "Download complete (%d/%d, %.1fs)",
                    current,
                    download_count,
                    elapsed,
                )
                return True

            if elapsed > timeout:
                logger.error(
                    "Download timeout (%d/%d, %.1fs)",
                    current,
                    download_count,
                    elapsed,
                )
                return False

            time.sleep(poll_interval)

    def read(self, buffer_size: int = READ_BUFFER_SIZE) -> ReadResult:
        buff = " " * buffer_size
        result = self._com.JVRead(buff, buffer_size, "")
        if not isinstance(result, tuple) or len(result) < 4:
            raise RuntimeError(
                "JVRead returned an unsupported COM shape. "
                "Use 32-bit Python / pywin32 so out parameters marshal as a tuple."
            )

        ret_code = int(result[0])
        data_str = result[1] if len(result) > 1 else ""
        filename = str(result[3] or "")

        if ret_code > 0 and data_str:
            raw_bytes = data_str.encode(JVDATA_ENCODING)
        else:
            raw_bytes = b""
        return ReadResult(ret_code=ret_code, raw_bytes=raw_bytes, filename=filename)

    def skip(self) -> int:
        ret = int(self._com.JVSkip())
        logger.debug("JVSkip() => %d", ret)
        return ret

    def open_config(self) -> None:
        self._com.JVSetUIProperties()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
