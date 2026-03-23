#!/usr/bin/env python3
"""
JV-Link Raw Data Fetcher

使い方:
    python main.py status                      # ダウンロード済みデータの確認
    python main.py setup RACE DIFN BLOD        # セットアップ（dataspec指定）
    python main.py setup --all                 # 全dataspecのセットアップ
    python main.py update                      # 差分更新（セットアップ済みのみ）
    python main.py update RACE                 # 特定dataspecの差分更新
    python main.py config                      # JV-Link 設定ダイアログ
    python main.py migrate                     # 旧構造から新構造へデータ移行
"""

import argparse
import json
import logging
import shutil
import sys
from pathlib import Path

# モジュールパスの追加（同ディレクトリから import するため）
sys.path.insert(0, str(Path(__file__).parent))

from config import SETUP_DATASPECS, SETUP_FROM_TIME
from job_runner import JobRunner
from sync_state import SyncState

DEFAULT_ARCHIVE = r"D:\jvdata"
CONFIG_PATH = Path(__file__).parent / ".jvconfig.json"


# ==============================================================
# 設定ファイル
# ==============================================================

def load_or_create_config() -> dict:
    """設定ファイルを読み込む。存在しなければ初回設定を行う。"""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    print("初回セットアップ: データの保存先を指定してください。")
    archive = input(f"保存先ディレクトリ [{DEFAULT_ARCHIVE}]: ").strip()
    if not archive:
        archive = DEFAULT_ARCHIVE

    config = {"archive_dir": archive, "jvlink_temp_dir": r"C:\JVLinkTemp"}
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print(f"設定を保存しました: {CONFIG_PATH}")
    return config


def get_archive_dir(args) -> str:
    """--archive が明示指定されていればそれを、なければ設定ファイルの値を返す。"""
    if hasattr(args, "archive") and args.archive != DEFAULT_ARCHIVE:
        return args.archive
    config = load_or_create_config()
    return config.get("archive_dir", DEFAULT_ARCHIVE)


def get_temp_dir(args) -> str:
    if hasattr(args, "temp_dir") and args.temp_dir:
        return args.temp_dir
    config = load_or_create_config()
    return config.get("jvlink_temp_dir", r"C:\JVLinkTemp")


# ==============================================================
# ヘルパー関数
# ==============================================================

def _format_bytes(nbytes: int) -> str:
    if nbytes >= 1_000_000_000:
        return f"{nbytes / 1_000_000_000:.1f}GB"
    if nbytes >= 1_000_000:
        return f"{nbytes / 1_000_000:.1f}MB"
    if nbytes >= 1_000:
        return f"{nbytes / 1_000:.1f}KB"
    return f"{nbytes}B"


def _format_timestamp(ts: str) -> str:
    """'20260223133238' -> '2026-02-23'"""
    if len(ts) >= 8:
        return f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}"
    return ts or ""


def _parse_fromtime(raw: str) -> str:
    """YYYYMMDD -> YYYYMMDD000000"""
    if len(raw) == 8:
        return raw + "000000"
    return raw


def _validate_dataspecs(dataspecs: list[str]) -> list[str] | None:
    """不正なdataspecがあればエラー表示してNoneを返す。"""
    invalid = [d for d in dataspecs if d not in SETUP_DATASPECS]
    if invalid:
        print(f"不明な dataspec: {', '.join(invalid)}")
        print(f"有効な dataspec: {', '.join(SETUP_DATASPECS)}")
        return None
    return dataspecs


# ==============================================================
# コマンド実装
# ==============================================================

def cmd_setup(args):
    """セットアップ実行コマンド。"""
    if args.all:
        dataspecs = list(SETUP_DATASPECS)
    elif args.dataspecs:
        dataspecs = _validate_dataspecs(args.dataspecs)
        if dataspecs is None:
            return 1
    else:
        print("dataspec を指定するか --all を使ってください。")
        print(f"例: python main.py setup RACE DIFN")
        print(f"有効: {', '.join(SETUP_DATASPECS)}")
        return 1

    archive = get_archive_dir(args)
    fromtime = _parse_fromtime(args.fromtime)

    state = SyncState(Path(archive))
    state.load()

    # 完了済みdataspecの再セットアップ確認
    targets = []
    for ds in dataspecs:
        if state.is_completed(ds) and not args.force:
            fc = state.get_file_count(ds)
            print(f"\n{ds} は既にセットアップ完了済みです ({fc} files).")
            print("再セットアップすると既存データを削除して最初からダウンロードします。")
            answer = input("続行しますか？ [y/N]: ").strip().lower()
            if answer != "y":
                print(f"{ds}: スキップ")
                continue
            # 既存データとステートを削除
            ds_dir = Path(archive) / ds
            shutil.rmtree(ds_dir, ignore_errors=True)
            state.reset_dataspec(ds)
        targets.append(ds)

    if not targets:
        print("実行対象がありません。")
        return 0

    runner = JobRunner(
        archive_dir=archive,
        jvlink_temp_dir=get_temp_dir(args),
    )

    results = runner.run_all_setup(
        fromtime=fromtime,
        dataspecs=targets,
        option=4,
    )

    if all(v in ("completed", "skipped") for v in results.values()):
        return 0
    return 1


def cmd_update(args):
    """差分更新コマンド。セットアップ済みのdataspecのみ更新する。"""
    dataspecs = None
    if args.dataspecs:
        dataspecs = _validate_dataspecs(args.dataspecs)
        if dataspecs is None:
            return 1

    archive = get_archive_dir(args)

    runner = JobRunner(
        archive_dir=archive,
        jvlink_temp_dir=get_temp_dir(args),
    )

    results = runner.run_all_diff(
        dataspecs=dataspecs,
        option=1,
    )

    if all(v in ("completed", "skipped") for v in results.values()):
        return 0
    return 1


def cmd_config(args):
    """JV-Link 設定ダイアログを開く。"""
    from jvlink_session import JvLinkSession

    with JvLinkSession() as session:
        session.init()
        session.open_config()
    print("JV-Link 設定ダイアログを開きました。")
    return 0


def cmd_status(args):
    """ダウンロード済みデータの状態を表示する。"""
    archive = Path(get_archive_dir(args))

    state = SyncState(archive)
    state.load()

    print(f"\n=== ダウンロード済みデータ ===")
    print(f"{'dataspec':<8}  {'status':<14} {'files':>6}  {'last_update'}")
    print("-" * 50)

    for ds in SETUP_DATASPECS:
        if state.is_completed(ds):
            fc = state.get_file_count(ds)
            ts = _format_timestamp(state.get_last_timestamp(ds) or "")
            print(f"{ds:<8}  {'completed':<14} {fc:>6}  {ts}")
        elif state.is_in_progress(ds):
            print(f"{ds:<8}  {'in progress':<14}")
        else:
            print(f"{ds:<8}  {'not started':<14}")

    return 0


def cmd_migrate(args):
    """旧構造（setup/RACE/timestamp/files/）から新構造（RACE/）へデータを移行する。"""
    archive = Path(get_archive_dir(args))
    state = SyncState(archive)
    state.load()

    migrated = 0

    for ds in SETUP_DATASPECS:
        # 旧構造: archive/setup/<DS>/<timestamp>/files/*.jvdat
        old_ds_dir = archive / "setup" / ds
        if not old_ds_dir.exists():
            continue

        # 旧構造の完了ジョブを探す
        job_dir, job_state_data = _find_old_completed_job(old_ds_dir)
        if job_state_data is None:
            continue

        # 新構造のディレクトリ
        new_ds_dir = archive / ds

        if state.is_completed(ds):
            print(f"{ds}: 既に移行済み、スキップ")
            continue

        # 完了ジョブの files/ ディレクトリを使う
        files_dir = job_dir / "files"
        if not files_dir.is_dir():
            print(f"{ds}: ファイルディレクトリが見つかりません、スキップ")
            continue

        # ファイルをコピー
        new_ds_dir.mkdir(parents=True, exist_ok=True)
        file_count = 0
        for f in files_dir.iterdir():
            if f.is_file() and f.name.endswith(".jvdat"):
                dest = new_ds_dir / f.name
                shutil.copy2(f, dest)
                file_count += 1

        if file_count == 0:
            print(f"{ds}: jvdat ファイルなし、スキップ")
            continue

        # SyncState に記録
        timestamp = job_state_data.get("last_file_timestamp", "")
        state.complete_sync(ds, timestamp, file_count)
        print(f"{ds}: {file_count} files 移行完了 (timestamp={timestamp})")
        migrated += 1

    if migrated == 0:
        print("移行対象がありませんでした。")
    else:
        print(f"\n{migrated} dataspec を移行しました。")
        print("旧ディレクトリ (setup/, diff/) は手動で削除してください。")

    return 0


def _find_old_completed_job(ds_dir: Path) -> tuple[Path | None, dict | None]:
    """旧構造の完了ジョブのディレクトリと job_state.json を返す。"""
    for d in sorted(ds_dir.iterdir(), reverse=True):
        if not d.is_dir():
            continue
        state_file = d / "job_state.json"
        if state_file.exists():
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("status") == "completed":
                return d, data
    return None, None


def setup_logging(verbose: bool = False, log_file: str = None):
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(level=level, format=fmt, handlers=handlers)


# ==============================================================
# CLI パーサー
# ==============================================================

def main():
    parser = argparse.ArgumentParser(
        description="JV-Link Raw Data Fetcher",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="詳細ログ")
    parser.add_argument("--log-file", default=None, help="ログファイルパス")

    subparsers = parser.add_subparsers(dest="command", help="コマンド")

    # --- status ---
    p_status = subparsers.add_parser("status", help="ダウンロード済みデータの確認")
    p_status.add_argument("--archive", "-a", default=DEFAULT_ARCHIVE, help="保存先")
    p_status.set_defaults(func=cmd_status)

    # --- setup ---
    p_setup = subparsers.add_parser("setup", help="セットアップ（全量ダウンロード）")
    p_setup.add_argument("dataspecs", nargs="*", help="対象 dataspec (例: RACE DIFN)")
    p_setup.add_argument("--all", action="store_true", help="全 dataspec")
    p_setup.add_argument("--force", action="store_true", help="確認なしで再セットアップ")
    p_setup.add_argument("--archive", "-a", default=DEFAULT_ARCHIVE, help="保存先")
    p_setup.add_argument("--from", dest="fromtime", default=SETUP_FROM_TIME[:8], help="開始日 (YYYYMMDD)")
    p_setup.add_argument("--temp-dir", default=r"C:\JVLinkTemp", help="JV-Link一時保存先")
    p_setup.set_defaults(func=cmd_setup)

    # --- update ---
    p_update = subparsers.add_parser("update", help="差分更新（セットアップ済みのみ）")
    p_update.add_argument("dataspecs", nargs="*", help="対象 dataspec（省略で全セットアップ済み）")
    p_update.add_argument("--archive", "-a", default=DEFAULT_ARCHIVE, help="保存先")
    p_update.add_argument("--temp-dir", default=r"C:\JVLinkTemp", help="JV-Link一時保存先")
    p_update.set_defaults(func=cmd_update)

    # --- config ---
    p_config = subparsers.add_parser("config", help="JV-Link 設定ダイアログ")
    p_config.set_defaults(func=cmd_config)

    # --- migrate ---
    p_migrate = subparsers.add_parser("migrate", help="旧構造から新構造へデータ移行")
    p_migrate.add_argument("--archive", "-a", default=DEFAULT_ARCHIVE, help="保存先")
    p_migrate.set_defaults(func=cmd_migrate)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # status コマンドではログを抑制
    if args.command == "status":
        setup_logging(verbose=False, log_file=args.log_file)
        logging.getLogger().setLevel(logging.WARNING)
    else:
        setup_logging(verbose=args.verbose, log_file=args.log_file)

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
