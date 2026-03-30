"""Runtime and environment diagnostics for the downloader."""

from __future__ import annotations

import os
import struct
import tempfile
from dataclasses import dataclass
from pathlib import Path

from config import JVLINK_PROG_ID

try:
    import winreg
except ImportError:  # pragma: no cover - non-Windows runtime
    winreg = None


@dataclass(frozen=True)
class DoctorCheck:
    name: str
    status: str
    detail: str


def is_windows() -> bool:
    return os.name == "nt"


def is_32bit_python() -> bool:
    return struct.calcsize("P") == 4


def ensure_32bit_runtime() -> None:
    if not is_32bit_python():
        raise RuntimeError(
            "JV-Link COM is expected to run under 32-bit Python. "
            "Please run this command with your 32-bit Python interpreter."
        )


def run_doctor(archive_dir: str, temp_dir: str) -> list[DoctorCheck]:
    checks = [
        _check_windows(),
        _check_python_bitness(),
        _check_pywin32(),
        _check_com_registration(),
        _check_com_dispatch(),
        _check_directory_writable("archive_dir", archive_dir),
        _check_directory_writable("jvlink_temp_dir", temp_dir),
    ]
    return checks


def _check_windows() -> DoctorCheck:
    if is_windows():
        return DoctorCheck("windows", "PASS", "Windows runtime detected.")
    return DoctorCheck("windows", "FAIL", "JV-Link downloader supports Windows only.")


def _check_python_bitness() -> DoctorCheck:
    if is_32bit_python():
        return DoctorCheck("python_bitness", "PASS", "32-bit Python runtime detected.")
    return DoctorCheck(
        "python_bitness",
        "WARN",
        "64-bit Python detected. setup/update/jvlink-config require 32-bit Python.",
    )


def _check_pywin32() -> DoctorCheck:
    try:
        import win32com.client  # noqa: F401
    except ImportError:
        return DoctorCheck("pywin32", "FAIL", "pywin32 is not installed.")
    return DoctorCheck("pywin32", "PASS", "pywin32 import succeeded.")


def _check_com_registration() -> DoctorCheck:
    if winreg is None:
        return DoctorCheck("com_registration", "FAIL", "winreg is unavailable on this runtime.")

    for access_mask in (0, getattr(winreg, "KEY_WOW64_32KEY", 0), getattr(winreg, "KEY_WOW64_64KEY", 0)):
        try:
            with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, JVLINK_PROG_ID, 0, winreg.KEY_READ | access_mask):
                return DoctorCheck("com_registration", "PASS", f"{JVLINK_PROG_ID} is registered.")
        except OSError:
            continue
    return DoctorCheck("com_registration", "FAIL", f"{JVLINK_PROG_ID} is not registered.")


def _check_com_dispatch() -> DoctorCheck:
    try:
        import win32com.client
    except ImportError:
        return DoctorCheck("com_dispatch", "FAIL", "Cannot probe COM dispatch without pywin32.")
    if not is_32bit_python():
        return DoctorCheck("com_dispatch", "WARN", "Skipping dispatch probe on 64-bit Python.")
    try:
        win32com.client.Dispatch(JVLINK_PROG_ID)
    except Exception as exc:  # pragma: no cover - requires Windows COM runtime
        return DoctorCheck("com_dispatch", "FAIL", f"Dispatch failed: {exc}")
    return DoctorCheck("com_dispatch", "PASS", "COM dispatch probe succeeded.")


def _check_directory_writable(name: str, raw_path: str) -> DoctorCheck:
    path = Path(raw_path)
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix="jvlink_raw_", dir=path, delete=False) as fh:
            tmp_path = Path(fh.name)
        tmp_path.unlink()
    except Exception as exc:
        return DoctorCheck(name, "FAIL", f"{path} is not writable: {exc}")
    return DoctorCheck(name, "PASS", f"{path} is writable.")

