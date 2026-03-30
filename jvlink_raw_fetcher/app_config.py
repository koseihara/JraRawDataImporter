"""User-scoped configuration loading for the downloader."""

from __future__ import annotations

import json
import os
from pathlib import Path

DEFAULT_ARCHIVE = r"D:\jvdata"
DEFAULT_TEMP_DIR = r"C:\JVLinkTemp"
APP_DIR_NAME = "jvlink-raw-fetcher"

ENV_CONFIG_PATH = "JVLINK_RAW_CONFIG_PATH"
ENV_ARCHIVE_DIR = "JVLINK_RAW_ARCHIVE_DIR"
ENV_TEMP_DIR = "JVLINK_RAW_TEMP_DIR"
ENV_LOG_LEVEL = "JVLINK_RAW_LOG_LEVEL"


def default_config_dir() -> Path:
    base = os.environ.get("LOCALAPPDATA")
    if base:
        return Path(base) / APP_DIR_NAME
    return Path.home() / ".config" / APP_DIR_NAME


def default_config_path() -> Path:
    override = os.environ.get(ENV_CONFIG_PATH)
    if override:
        return Path(override)
    return default_config_dir() / "config.json"


def legacy_repo_config_path() -> Path:
    return Path(__file__).resolve().parents[1] / ".jvconfig.json"


def default_config() -> dict:
    return {
        "archive_dir": DEFAULT_ARCHIVE,
        "jvlink_temp_dir": DEFAULT_TEMP_DIR,
        "default_dataspecs": ["RACE"],
        "log_level": "INFO",
    }


def load_user_config() -> tuple[dict, Path]:
    path = default_config_path()
    if path.exists():
        return _read_json(path), path

    legacy_path = legacy_repo_config_path()
    if legacy_path.exists():
        data = _read_json(legacy_path)
        merged = default_config()
        merged.update(data)
        save_user_config(merged, path=path)
        return merged, path

    data = default_config()
    save_user_config(data, path=path)
    return data, path


def save_user_config(data: dict, path: Path | None = None) -> Path:
    target = path or default_config_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f"{target.name}.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    tmp.replace(target)
    return target


def resolve_setting(cli_value: str | None, env_name: str, config_value: str | None, default: str) -> str:
    if cli_value:
        return cli_value
    env_value = os.environ.get(env_name)
    if env_value:
        return env_value
    if config_value:
        return config_value
    return default


def effective_log_level(cli_value: str | None, config_data: dict) -> str:
    return resolve_setting(cli_value, ENV_LOG_LEVEL, config_data.get("log_level"), "INFO").upper()


def _read_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)

