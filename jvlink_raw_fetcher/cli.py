"""Packaged CLI entry point for the downloader."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from archive_store import DataspecArchive
from config import SETUP_DATASPECS, SETUP_FROM_TIME
from job_runner import JobRunner
from jvlink_raw_fetcher.app_config import (
    DEFAULT_ARCHIVE,
    DEFAULT_TEMP_DIR,
    ENV_ARCHIVE_DIR,
    ENV_TEMP_DIR,
    effective_log_level,
    load_user_config,
    resolve_setting,
)
from jvlink_raw_fetcher.platform import ensure_32bit_runtime, run_doctor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="JV-Link raw data downloader")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--log-file", default=None)
    parser.add_argument("--log-level", default=None)
    subparsers = parser.add_subparsers(dest="command")

    p_status = subparsers.add_parser("status")
    p_status.add_argument("--archive", "-a", default=None)
    p_status.set_defaults(func=cmd_status)

    p_doctor = subparsers.add_parser("doctor")
    p_doctor.add_argument("--archive", "-a", default=None)
    p_doctor.add_argument("--temp-dir", default=None)
    p_doctor.set_defaults(func=cmd_doctor)

    p_setup = subparsers.add_parser("setup")
    p_setup.add_argument("dataspecs", nargs="*")
    p_setup.add_argument("--all", action="store_true")
    p_setup.add_argument("--archive", "-a", default=None)
    p_setup.add_argument("--from", dest="fromtime", default=SETUP_FROM_TIME[:8])
    p_setup.add_argument("--temp-dir", default=None)
    p_setup.set_defaults(func=cmd_setup)

    p_update = subparsers.add_parser("update")
    p_update.add_argument("dataspecs", nargs="*")
    p_update.add_argument("--archive", "-a", default=None)
    p_update.add_argument("--temp-dir", default=None)
    p_update.set_defaults(func=cmd_update)

    p_config = subparsers.add_parser("jvlink-config", aliases=["config"])
    p_config.set_defaults(func=cmd_jvlink_config)

    p_verify = subparsers.add_parser("verify")
    p_verify.add_argument("dataspecs", nargs="*")
    p_verify.add_argument("--all", action="store_true")
    p_verify.add_argument("--archive", "-a", default=None)
    p_verify.set_defaults(func=cmd_verify)

    p_refresh = subparsers.add_parser("refresh-view")
    p_refresh.add_argument("dataspecs", nargs="*")
    p_refresh.add_argument("--all", action="store_true")
    p_refresh.add_argument("--archive", "-a", default=None)
    p_refresh.set_defaults(func=cmd_refresh_view)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 1

    config_data, _config_path = load_user_config()
    setup_logging(
        verbose=args.verbose,
        log_file=args.log_file,
        log_level=effective_log_level(args.log_level, config_data),
    )
    try:
        return args.func(args, config_data)
    except RuntimeError as exc:
        print(exc)
        return 1


def cmd_status(args, config_data: dict) -> int:
    archive_root = Path(_archive_dir(args, config_data))
    print(f"{'dataspec':<8} {'status':<12} {'files':>6} {'timestamp':<14} {'commit'}")
    print("-" * 80)
    for ds in SETUP_DATASPECS:
        store = DataspecArchive(archive_root, ds)
        data = store.status()
        active = data["active_runs"]
        current = data["current"]
        if active:
            latest = active[0]
            print(
                f"{ds:<8} {latest.get('status', ''):<12} "
                f"{latest.get('processed_files', 0):>6} "
                f"{latest.get('open_last_file_timestamp', ''):<14} "
                f"{latest.get('run_id', '')}"
            )
        elif current:
            print(
                f"{ds:<8} {'ready':<12} "
                f"{current.get('file_count', 0):>6} "
                f"{current.get('last_successful_timestamp', ''):<14} "
                f"{current.get('commit_id', '')}"
            )
        else:
            print(f"{ds:<8} {'not started':<12}")
    return 0


def cmd_doctor(args, config_data: dict) -> int:
    archive_dir = _archive_dir(args, config_data)
    temp_dir = _temp_dir(args, config_data)
    checks = run_doctor(archive_dir=archive_dir, temp_dir=temp_dir)
    failed = False
    for check in checks:
        print(f"[{check.status:<4}] {check.name}: {check.detail}")
        failed = failed or check.status == "FAIL"
    return 1 if failed else 0


def cmd_setup(args, config_data: dict) -> int:
    ensure_32bit_runtime()
    dataspecs = _setup_dataspecs(args)
    if dataspecs is None:
        return 1
    runner = JobRunner(
        archive_dir=_archive_dir(args, config_data),
        jvlink_temp_dir=_temp_dir(args, config_data),
    )
    results = runner.run_all_setup(
        fromtime=_parse_fromtime(args.fromtime),
        dataspecs=dataspecs,
        option=4,
    )
    return 0 if all(v in {"completed", "skipped"} for v in results.values()) else 1


def cmd_update(args, config_data: dict) -> int:
    ensure_32bit_runtime()
    dataspecs = None
    if args.dataspecs:
        dataspecs = _validate_dataspecs(args.dataspecs)
        if dataspecs is None:
            return 1
    runner = JobRunner(
        archive_dir=_archive_dir(args, config_data),
        jvlink_temp_dir=_temp_dir(args, config_data),
    )
    results = runner.run_all_diff(dataspecs=dataspecs, option=1)
    return 0 if all(v in {"completed", "skipped"} for v in results.values()) else 1


def cmd_jvlink_config(args, config_data: dict) -> int:
    ensure_32bit_runtime()
    from jvlink_session import JvLinkSession

    with JvLinkSession() as session:
        session.init()
        session.open_config()
    print("opened JV-Link configuration dialog")
    return 0


def cmd_verify(args, config_data: dict) -> int:
    archive_root = Path(_archive_dir(args, config_data))
    dataspecs = list(SETUP_DATASPECS) if args.all or not args.dataspecs else _validate_dataspecs(args.dataspecs)
    if dataspecs is None:
        return 1

    overall_ok = True
    for dataspec in dataspecs:
        store = DataspecArchive(archive_root, dataspec)
        status_data = store.status()
        if not status_data["current"] and not status_data["previous"] and not status_data["active_runs"]:
            print(f"{dataspec}: NOT STARTED")
            continue
        result = store.verify()
        status = "OK" if result["ok"] else "FAILED"
        print(
            f"{dataspec}: {status} "
            f"(commits={len(result['checked_commits'])}, objects={result['checked_objects']}, runs={len(result['checked_runs'])})"
        )
        for warning in result["warnings"]:
            print(f"  warning: {warning}")
        for error in result["errors"]:
            print(f"  error: {error}")
        overall_ok = overall_ok and result["ok"]
    return 0 if overall_ok else 1


def cmd_refresh_view(args, config_data: dict) -> int:
    archive_root = Path(_archive_dir(args, config_data))
    dataspecs = list(SETUP_DATASPECS) if args.all or not args.dataspecs else _validate_dataspecs(args.dataspecs)
    if dataspecs is None:
        return 1

    refreshed = 0
    for dataspec in dataspecs:
        store = DataspecArchive(archive_root, dataspec)
        status_data = store.status()
        if not status_data["current"] and not status_data["previous"]:
            print(f"{dataspec}: no committed snapshot; skipped")
            continue
        with store.acquire_lock():
            store.refresh_views()
        print(f"{dataspec}: refreshed view")
        refreshed += 1
    return 0 if refreshed or not dataspecs else 1


def setup_logging(verbose: bool = False, log_file: str | None = None, log_level: str = "INFO") -> None:
    level_name = "DEBUG" if verbose else log_level.upper()
    level = getattr(logging, level_name, logging.INFO)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(level=level, format=fmt, handlers=handlers)


def _parse_fromtime(raw: str) -> str:
    return raw + "000000" if len(raw) == 8 else raw


def _validate_dataspecs(dataspecs: list[str]) -> list[str] | None:
    invalid = [d for d in dataspecs if d not in SETUP_DATASPECS]
    if invalid:
        print(f"invalid dataspec: {', '.join(invalid)}")
        print(f"valid dataspecs: {', '.join(SETUP_DATASPECS)}")
        return None
    return dataspecs


def _setup_dataspecs(args) -> list[str] | None:
    if args.all:
        return list(SETUP_DATASPECS)
    if args.dataspecs:
        return _validate_dataspecs(args.dataspecs)
    print("dataspecs or --all is required")
    return None


def _archive_dir(args, config_data: dict) -> str:
    return resolve_setting(args.archive, ENV_ARCHIVE_DIR, config_data.get("archive_dir"), DEFAULT_ARCHIVE)


def _temp_dir(args, config_data: dict) -> str:
    return resolve_setting(args.temp_dir, ENV_TEMP_DIR, config_data.get("jvlink_temp_dir"), DEFAULT_TEMP_DIR)

