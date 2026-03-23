#!/usr/bin/env python3
"""
JV-Link Raw Data Fetcher — CLI エントリーポイント

使い方:
    # 全蓄積系 dataspec のセットアップ（1986年〜）
    python main.py setup --archive ./archive

    # 特定の dataspec のみ
    python main.py setup --archive ./archive --dataspecs RACE DIFF

    # 開始日を指定
    python main.py setup --archive ./archive --from 20200101

    # JV-Link 設定ダイアログを開く
    python main.py config

    # ジョブ状態の確認
    python main.py status --archive ./archive
"""

import argparse
import logging
import sys
from pathlib import Path

# モジュールパスの追加（同ディレクトリから import するため）
sys.path.insert(0, str(Path(__file__).parent))

from config import SETUP_DATASPECS, SETUP_FROM_TIME
from job_runner import JobRunner
from job_state import JobState


def setup_logging(verbose: bool = False, log_file: str = None):
    """ロギングの設定。"""
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(level=level, format=fmt, handlers=handlers)


def cmd_setup(args):
    """セットアップ実行コマンド。"""
    fromtime = args.fromtime + "000000" if len(args.fromtime) == 8 else args.fromtime
    dataspecs = args.dataspecs if args.dataspecs else None

    runner = JobRunner(
        archive_dir=args.archive,
        jvlink_temp_dir=args.temp_dir,
    )

    results = runner.run_all_setup(
        fromtime=fromtime,
        dataspecs=dataspecs,
        option=4,
    )

    # 終了コード
    if all(v in ("completed", "skipped") for v in results.values()):
        return 0
    return 1


def cmd_single(args):
    """単一 dataspec のセットアップ実行コマンド。"""
    fromtime = args.fromtime + "000000" if len(args.fromtime) == 8 else args.fromtime

    runner = JobRunner(
        archive_dir=args.archive,
        jvlink_temp_dir=args.temp_dir,
    )

    status = runner.run_single_setup(
        dataspec=args.dataspec,
        fromtime=fromtime,
        option=4,
    )

    print(f"\n結果: {args.dataspec} => {status}")
    return 0 if status in ("completed", "skipped") else 1


def cmd_config(args):
    """JV-Link 設定ダイアログを開く。"""
    from jvlink_session import JvLinkSession

    session = JvLinkSession()
    session.init()
    session.open_config()
    print("JV-Link 設定ダイアログを開きました。")
    return 0


def cmd_status(args):
    """全ジョブの状態を表示する。"""
    archive = Path(args.archive)
    setup_dir = archive / "setup"

    if not setup_dir.exists():
        print("セットアップディレクトリが見つかりません:", setup_dir)
        return 1

    print(f"{'dataspec':<8} {'status':<12} {'files':>6} {'records':>10} {'last_file'}")
    print("-" * 70)

    for ds in SETUP_DATASPECS:
        ds_dir = setup_dir / ds
        if not ds_dir.exists():
            print(f"{ds:<8} {'not started':<12}")
            continue

        # 最新のジョブディレクトリを探す
        for d in sorted(ds_dir.iterdir(), reverse=True):
            if d.is_dir():
                state = JobState(d)
                if state.load():
                    s = state.state
                    print(
                        f"{ds:<8} {s['status']:<12} "
                        f"{s['processed_files']:>6} "
                        f"{s['processed_records']:>10} "
                        f"{s.get('last_processed_filename', '')[:30]}"
                    )
                    break
        else:
            print(f"{ds:<8} {'no state':<12}")

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="JV-Link Raw Data Fetcher — 蓄積系データのセットアップ取得",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="詳細ログを出力",
    )
    parser.add_argument(
        "--log-file", default=None,
        help="ログファイルパス",
    )

    subparsers = parser.add_subparsers(dest="command", help="サブコマンド")

    # --- setup ---
    p_setup = subparsers.add_parser("setup", help="全 dataspec のセットアップ実行")
    p_setup.add_argument(
        "--archive", "-a", required=True,
        help="保存先ディレクトリ",
    )
    p_setup.add_argument(
        "--from", dest="fromtime", default=SETUP_FROM_TIME[:8],
        help="取得開始日 (YYYYMMDD), デフォルト=19860101",
    )
    p_setup.add_argument(
        "--dataspecs", "-d", nargs="+",
        help="対象 dataspec（指定しない場合は全蓄積系）",
    )
    p_setup.add_argument(
        "--temp-dir", default="C:\\JVLinkTemp",
        help="JV-Link 一時保存ディレクトリ",
    )
    p_setup.set_defaults(func=cmd_setup)

    # --- single ---
    p_single = subparsers.add_parser("single", help="単一 dataspec のセットアップ実行")
    p_single.add_argument(
        "--archive", "-a", required=True,
        help="保存先ディレクトリ",
    )
    p_single.add_argument(
        "dataspec",
        help="対象 dataspec (例: RACE)",
    )
    p_single.add_argument(
        "--from", dest="fromtime", default=SETUP_FROM_TIME[:8],
        help="取得開始日 (YYYYMMDD)",
    )
    p_single.add_argument(
        "--temp-dir", default="C:\\JVLinkTemp",
        help="JV-Link 一時保存ディレクトリ",
    )
    p_single.set_defaults(func=cmd_single)

    # --- config ---
    p_config = subparsers.add_parser("config", help="JV-Link 設定ダイアログ")
    p_config.set_defaults(func=cmd_config)

    # --- status ---
    p_status = subparsers.add_parser("status", help="ジョブ状態の確認")
    p_status.add_argument(
        "--archive", "-a", required=True,
        help="保存先ディレクトリ",
    )
    p_status.set_defaults(func=cmd_status)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    setup_logging(verbose=args.verbose, log_file=args.log_file)

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
