"""
JV-Link COM インターフェースの薄いラッパー

JVInit → JVOpen → JVStatus(wait) → JVRead/JVGets loop → JVClose
の一連の操作を Python から呼び出すためのクラス。

NOTE: win32com 経由の COM 呼び出しでは、[out] パラメータの挙動が
      JV-Link のバージョンや Python 環境によって異なる場合がある。
      初回実行時に _probe_com_calling_convention() で自動判定し、
      うまくいかない場合は CALLING_CONVENTION を手動で設定すること。
"""

import time
import logging
from typing import Optional, Tuple, NamedTuple

try:
    import win32com.client
    HAS_WIN32COM = True
except ImportError:
    HAS_WIN32COM = False

from config import (
    JVLINK_PROG_ID,
    JVLINK_SID,
    READ_BUFFER_SIZE,
    STATUS_POLL_INTERVAL,
    STATUS_POLL_TIMEOUT,
    JVDATA_ENCODING,
)

logger = logging.getLogger(__name__)


class ReadResult(NamedTuple):
    """JVRead / JVGets の戻り値"""
    ret_code: int           # >0: 読み取りバイト数, 0: EOF, -1: ファイル切替, <-1: エラー
    raw_bytes: bytes        # レコードの生バイト列（SJIS/cp932）
    filename: str           # JV-Link内部の物理ファイル名


class JvLinkSession:
    """
    JV-Link COM コンポーネントの薄いラッパー。

    使い方:
        session = JvLinkSession()
        session.init()
        session.set_save_path("C:/jvtemp")
        ret, read_count, dl_count, ts = session.open("RACE", "20200101000000", option=4)
        session.wait_for_download(dl_count)
        while True:
            result = session.read()
            if result.ret_code == 0:
                break  # EOF
            ...
        session.close()
    """

    def __init__(self, prog_id: str = JVLINK_PROG_ID, sid: str = JVLINK_SID):
        self._prog_id = prog_id
        self._sid = sid
        self._com = None
        self._is_open = False

    # ----------------------------------------------------------
    # COM 生成・初期化
    # ----------------------------------------------------------

    def init(self) -> int:
        """
        COM オブジェクトを生成し、JVInit を呼ぶ。

        Returns:
            JVInit の戻り値 (0=成功)
        """
        if not HAS_WIN32COM:
            raise RuntimeError(
                "pywin32 がインストールされていません。\n"
                "  pip install pywin32\n"
                "Windows 環境でのみ動作します。"
            )

        logger.info("COM オブジェクト生成: %s", self._prog_id)
        self._com = win32com.client.Dispatch(self._prog_id)

        ret = self._com.JVInit(self._sid)
        logger.info("JVInit(sid=%r) => %s", self._sid, ret)
        return ret

    def set_save_path(self, path: str) -> int:
        """
        JV-Link 内部の一時保存パスを設定。

        Returns:
            JVSetSavePath の戻り値 (0=成功)
        """
        ret = self._com.JVSetSavePath(path)
        logger.info("JVSetSavePath(%r) => %s", path, ret)
        return ret

    def set_service_key(self, key: str) -> int:
        """
        サービスキーを設定（必要な場合）。

        Returns:
            JVSetServiceKey の戻り値
        """
        ret = self._com.JVSetServiceKey(key)
        logger.info("JVSetServiceKey() => %s", ret)
        return ret

    # ----------------------------------------------------------
    # JVOpen / JVClose
    # ----------------------------------------------------------

    def open(
        self,
        dataspec: str,
        fromtime: str,
        option: int,
        react_key: str = "",
    ) -> Tuple[int, int, int, str]:
        """
        JVOpen を呼び出す。

        Args:
            dataspec: データ種別 ("RACE", "DIFF", etc.)
            fromtime: 取得開始日時 "YYYYMMDDHHmmss"
            option: 1=通常, 2=今週, 3=セットアップ, 4=セットアップ(ダイアログ初回のみ)
            react_key: リアクトキー（通常は空文字）

        Returns:
            (return_code, read_count, download_count, last_file_timestamp)

            return_code:
                0  = 正常
               -1  = エラー
               -2  = ダウンロード中（通常は発生しない）

        NOTE: COM の [out] パラメータの返し方は環境依存。
              タプルで返る場合と、引数が変更される場合がある。
              ここでは両方のパターンを試みる。
        """
        logger.info(
            "JVOpen(dataspec=%r, fromtime=%r, option=%d)",
            dataspec, fromtime, option,
        )

        # COM呼び出し — [out] パラメータの返り方を自動判定
        result = self._com.JVOpen(dataspec, fromtime, option, 0, 0, "")

        if isinstance(result, tuple):
            # タプルで返るパターン (多くの環境)
            # (return_code, read_count, download_count, last_file_timestamp)
            ret_code = result[0]
            read_count = result[1] if len(result) > 1 else 0
            dl_count = result[2] if len(result) > 2 else 0
            timestamp = result[3] if len(result) > 3 else ""
        else:
            # スカラーで返るパターン
            ret_code = result
            read_count = 0
            dl_count = 0
            timestamp = ""

        logger.info(
            "  => ret=%d, read_count=%d, download_count=%d, timestamp=%r",
            ret_code, read_count, dl_count, timestamp,
        )

        self._is_open = (ret_code >= 0)
        return (ret_code, read_count, dl_count, timestamp)

    def close(self) -> None:
        """JVClose を呼び出す。"""
        if self._com and self._is_open:
            self._com.JVClose()
            self._is_open = False
            logger.info("JVClose()")

    # ----------------------------------------------------------
    # JVStatus — ダウンロード待ち
    # ----------------------------------------------------------

    def status(self) -> int:
        """
        JVStatus を呼び出す。

        Returns:
            ダウンロード完了ファイル数（download_count に到達すれば完了）
        """
        return self._com.JVStatus()

    def wait_for_download(
        self,
        download_count: int,
        poll_interval: float = STATUS_POLL_INTERVAL,
        timeout: float = STATUS_POLL_TIMEOUT,
    ) -> bool:
        """
        ダウンロード完了を待機する。

        Args:
            download_count: JVOpen で返された download_count
            poll_interval: ポーリング間隔（秒）
            timeout: タイムアウト（秒）

        Returns:
            True=完了, False=タイムアウト
        """
        if download_count <= 0:
            logger.info("ダウンロード不要 (download_count=%d)", download_count)
            return True

        logger.info(
            "ダウンロード待ち開始 (target=%d, timeout=%ds)",
            download_count, timeout,
        )

        start = time.time()
        while True:
            current = self.status()
            elapsed = time.time() - start

            if current >= download_count:
                logger.info(
                    "ダウンロード完了 (%d/%d, %.1fs)",
                    current, download_count, elapsed,
                )
                return True

            if elapsed > timeout:
                logger.error(
                    "ダウンロードタイムアウト (%d/%d, %.1fs)",
                    current, download_count, elapsed,
                )
                return False

            logger.debug(
                "  ダウンロード中 %d/%d (%.0fs)",
                current, download_count, elapsed,
            )
            time.sleep(poll_interval)

    # ----------------------------------------------------------
    # JVRead — レコード読み取り
    # ----------------------------------------------------------

    def read(self, buffer_size: int = READ_BUFFER_SIZE) -> ReadResult:
        """
        JVRead を呼び出し、1レコードを読み取る。

        COM経由で受け取った文字列を cp932 バイト列に変換して返す。
        これにより、JVGets と同等の「生データ保存」が可能。

        Returns:
            ReadResult(ret_code, raw_bytes, filename)

            ret_code:
                >0  = 読み取りバイト数
                 0  = 全データ読み込み終了 (EOF)
                -1  = ファイル切り替え
                -2  = ダウンロード中
                -3  = ダウンロード中（JVRead固有）
                <-3 = エラー
        """
        buff = " " * buffer_size
        result = self._com.JVRead(buff, buffer_size, "")

        if isinstance(result, tuple):
            # JVRead の戻りタプル構造:
            #   [0] = ret_code (>0: 読み取りバイト数, 0: EOF, -1: ファイル切替, etc.)
            #   [1] = データ文字列
            #   [2] = バッファサイズ（そのまま返る、使わない）
            #   [3] = 物理ファイル名
            ret_code = result[0]
            data_str = result[1] if len(result) > 1 else ""
            filename = result[3] if len(result) > 3 else ""
        else:
            ret_code = result
            data_str = ""
            filename = ""

        # 文字列 → cp932 バイト列に変換（Raw保存用）
        if ret_code > 0 and data_str:
            try:
                raw_bytes = data_str.encode(JVDATA_ENCODING)
            except (UnicodeEncodeError, AttributeError):
                # エンコード失敗時はUTF-8で保存（フォールバック）
                raw_bytes = data_str.encode("utf-8") if isinstance(data_str, str) else b""
                logger.warning("cp932エンコード失敗、UTF-8にフォールバック")
        else:
            raw_bytes = b""

        return ReadResult(ret_code=ret_code, raw_bytes=raw_bytes, filename=filename)

    # ----------------------------------------------------------
    # JVSkip — ファイルスキップ（中断再開用）
    # ----------------------------------------------------------

    def skip(self) -> int:
        """
        JVSkip を呼び出し、現在のファイルをスキップする。
        中断再開時に、既に処理済みのファイルを飛ばすために使用。

        Returns:
            JVSkip の戻り値 (0=成功)
        """
        ret = self._com.JVSkip()
        logger.debug("JVSkip() => %d", ret)
        return ret

    # ----------------------------------------------------------
    # ユーティリティ
    # ----------------------------------------------------------

    def open_config(self) -> None:
        """JV-Link の設定ダイアログを開く。"""
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
