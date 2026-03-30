"""
dataspec ごとのコミット済み raw アーカイブを管理する。

構成:
    <archive>/<dataspec>/
        refs/
            current.json
            previous.json
        commits/
            <commit_id>/
                meta.json
                manifest.jsonl
        objects/
            ab/
                abcdef....jvdat
        runs/
            <run_id>/
                run_state.json
                staging/
                candidate_manifest.jsonl
        lock
"""

from __future__ import annotations

import json
import os
import shutil
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional

from config import JVDATA_ENCODING, RAW_FILE_EXT


@dataclass(frozen=True)
class ManifestEntry:
    logical_filename: str
    object_sha256: str
    byte_count: int
    record_count: int

    @property
    def object_relpath(self) -> str:
        return f"{self.object_sha256[:2]}/{self.object_sha256}{RAW_FILE_EXT}"

    @property
    def format_code(self) -> str:
        return _logical_group(self.logical_filename)

    @property
    def view_relpath(self) -> str:
        return f"{self.format_code}/{self.logical_filename}{RAW_FILE_EXT}"

    def to_dict(self) -> dict:
        return {
            "logical_filename": self.logical_filename,
            "format_code": self.format_code,
            "view_relpath": self.view_relpath,
            "object_sha256": self.object_sha256,
            "byte_count": self.byte_count,
            "record_count": self.record_count,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ManifestEntry":
        return cls(
            logical_filename=data["logical_filename"],
            object_sha256=data["object_sha256"],
            byte_count=int(data["byte_count"]),
            record_count=int(data["record_count"]),
        )


class DataspecArchive:
    def __init__(self, archive_dir: str | Path, dataspec: str):
        self.archive_root = Path(archive_dir)
        self.dataspec = dataspec
        self.root = self.archive_root / dataspec
        self.refs_dir = self.root / "refs"
        self.commits_dir = self.root / "commits"
        self.objects_dir = self.root / "objects"
        self.runs_dir = self.root / "runs"
        self.view_dir = self.root / "view"
        self.lock_path = self.root / "lock"

    def ensure_layout(self) -> None:
        self.refs_dir.mkdir(parents=True, exist_ok=True)
        self.commits_dir.mkdir(parents=True, exist_ok=True)
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.view_dir.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def acquire_lock(self) -> Iterator[None]:
        self.ensure_layout()
        fd = None
        try:
            fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode("ascii"))
        except FileExistsError as exc:
            raise RuntimeError(f"{self.dataspec}: another run is active ({self.lock_path})") from exc
        try:
            yield
        finally:
            if fd is not None:
                os.close(fd)
            if self.lock_path.exists():
                self.lock_path.unlink()

    def current_ref_path(self) -> Path:
        return self.refs_dir / "current.json"

    def previous_ref_path(self) -> Path:
        return self.refs_dir / "previous.json"

    def load_ref(self, name: str) -> Optional[dict]:
        path = self.refs_dir / f"{name}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def load_current_ref(self) -> Optional[dict]:
        return self.load_ref("current")

    def load_previous_ref(self) -> Optional[dict]:
        return self.load_ref("previous")

    def write_ref(self, name: str, data: Optional[dict]) -> None:
        path = self.refs_dir / f"{name}.json"
        if data is None:
            if path.exists():
                path.unlink()
            return
        _write_json_atomic(path, data)

    def current_manifest(self) -> Dict[str, ManifestEntry]:
        current = self.load_current_ref()
        if not current:
            return {}
        return self.load_commit_manifest(current["commit_id"])

    def load_commit_manifest(self, commit_id: str) -> Dict[str, ManifestEntry]:
        manifest_path = self.commits_dir / commit_id / "manifest.jsonl"
        if not manifest_path.exists():
            return {}
        manifest: Dict[str, ManifestEntry] = {}
        with open(manifest_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                entry = ManifestEntry.from_dict(json.loads(line))
                manifest[entry.logical_filename] = entry
        return manifest

    def create_run_id(self) -> str:
        return f"{_now_id()}_pid{os.getpid()}_{uuid.uuid4().hex[:8]}"

    def create_run_dir(self, run_id: str) -> Path:
        run_dir = self.runs_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    def find_resumable_run(self, mode: str, fromtime: str, option: int) -> Optional[Path]:
        if not self.runs_dir.exists():
            return None
        for run_dir in sorted(self.runs_dir.iterdir(), reverse=True):
            if not run_dir.is_dir():
                continue
            state_path = run_dir / "run_state.json"
            if not state_path.exists():
                continue
            data = json.loads(state_path.read_text(encoding="utf-8"))
            if data.get("status") == "completed":
                continue
            if (
                data.get("mode") == mode
                and data.get("fromtime") == fromtime
                and int(data.get("option", -1)) == int(option)
            ):
                return run_dir
        return None

    def load_candidate_manifest(self, run_dir: Path) -> Dict[str, dict]:
        manifest_path = run_dir / "candidate_manifest.jsonl"
        entries: Dict[str, dict] = {}
        if not manifest_path.exists():
            return entries
        with open(manifest_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                entries[data["logical_filename"]] = data
        return entries

    def commit_run(
        self,
        run_dir: Path,
        mode: str,
        option: int,
        fromtime: str,
        returned_timestamp: str,
    ) -> dict:
        current_ref = self.load_current_ref()
        current_manifest = self.current_manifest()
        staged_entries = self.load_candidate_manifest(run_dir)

        next_manifest: Dict[str, ManifestEntry] = dict(current_manifest)
        for data in staged_entries.values():
            object_sha = data["sha256"]
            object_path = self.object_path(object_sha)
            object_path.parent.mkdir(parents=True, exist_ok=True)
            if not object_path.exists():
                shutil.move(str(run_dir / "staging" / data["staging_name"]), str(object_path))
            next_manifest[data["logical_filename"]] = ManifestEntry(
                logical_filename=data["logical_filename"],
                object_sha256=object_sha,
                byte_count=int(data["byte_count"]),
                record_count=int(data["record_count"]),
            )

        commit_id = run_dir.name
        commit_dir = self.commits_dir / commit_id
        manifest_items = [next_manifest[name] for name in sorted(next_manifest)]
        meta = {
            "dataspec": self.dataspec,
            "commit_id": commit_id,
            "base_commit_id": current_ref["commit_id"] if current_ref else None,
            "run_id": run_dir.name,
            "mode": mode,
            "option": option,
            "fromtime": fromtime,
            "jvopen_last_file_timestamp": returned_timestamp,
            "created_at": _now_iso(),
            "file_count": len(manifest_items),
            "staged_file_count": len(staged_entries),
        }
        self._materialize_commit(commit_dir, manifest_items, meta)

        new_ref = {
            "dataspec": self.dataspec,
            "commit_id": commit_id,
            "last_successful_timestamp": returned_timestamp,
            "file_count": len(manifest_items),
            "updated_at": _now_iso(),
            "encoding": JVDATA_ENCODING,
        }
        self.write_ref("current", new_ref)
        self.write_ref("previous", current_ref)

        self.refresh_views()
        self.garbage_collect()
        return new_ref

    def cleanup_run(self, run_dir: Path) -> None:
        shutil.rmtree(run_dir, ignore_errors=True)

    def object_path(self, sha256_hex: str) -> Path:
        return self.objects_dir / sha256_hex[:2] / f"{sha256_hex}{RAW_FILE_EXT}"

    def garbage_collect(self) -> None:
        keep_commits = {
            ref["commit_id"]
            for ref in (self.load_current_ref(), self.load_previous_ref())
            if ref
        }
        keep_hashes: set[str] = set()
        for commit_id in keep_commits:
            for entry in self.load_commit_manifest(commit_id).values():
                keep_hashes.add(entry.object_sha256)
        for run_dir in self._iter_active_run_dirs():
            for data in self.load_candidate_manifest(run_dir).values():
                object_sha = data.get("sha256")
                if object_sha and self.object_path(object_sha).exists():
                    keep_hashes.add(object_sha)

        for commit_dir in self.commits_dir.iterdir():
            if commit_dir.is_dir() and commit_dir.name not in keep_commits:
                shutil.rmtree(commit_dir, ignore_errors=True)

        for prefix_dir in self.objects_dir.iterdir():
            if not prefix_dir.is_dir():
                continue
            for obj in prefix_dir.iterdir():
                stem = obj.stem
                if stem not in keep_hashes:
                    obj.unlink()
            if not any(prefix_dir.iterdir()):
                prefix_dir.rmdir()

    def status(self) -> dict:
        current = self.load_current_ref()
        previous = self.load_previous_ref()
        active_runs = []
        if self.runs_dir.exists():
            for run_dir in sorted(self.runs_dir.iterdir(), reverse=True):
                state_path = run_dir / "run_state.json"
                if state_path.exists():
                    active_runs.append(json.loads(state_path.read_text(encoding="utf-8")))
        return {
            "dataspec": self.dataspec,
            "current": current,
            "previous": previous,
            "active_runs": active_runs,
        }

    def refresh_views(self) -> None:
        self.ensure_layout()
        self._refresh_view_ref("current", self.load_current_ref())
        self._refresh_view_ref("previous", self.load_previous_ref())

    def verify(self) -> dict:
        self.ensure_layout()
        errors: list[str] = []
        warnings: list[str] = []
        checked_objects: set[str] = set()
        checked_commits: list[str] = []
        checked_runs: list[str] = []

        refs: dict[str, Optional[dict]] = {}
        for ref_name in ("current", "previous"):
            ref_path = self.refs_dir / f"{ref_name}.json"
            if not ref_path.exists():
                refs[ref_name] = None
                continue
            try:
                refs[ref_name] = json.loads(ref_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                errors.append(f"{ref_name}: invalid JSON ({ref_path})")
                refs[ref_name] = None
                continue

            ref = refs[ref_name]
            commit_id = str(ref.get("commit_id", "") or "")
            timestamp = str(ref.get("last_successful_timestamp", "") or "")
            if not commit_id:
                errors.append(f"{ref_name}: missing commit_id")
                continue
            if not timestamp:
                errors.append(f"{ref_name}: missing last_successful_timestamp")
            manifest = self._verify_commit(commit_id, ref_name, errors)
            checked_commits.append(commit_id)
            if ref.get("file_count") is not None and int(ref["file_count"]) != len(manifest):
                errors.append(
                    f"{ref_name}: file_count={ref['file_count']} does not match manifest entries={len(manifest)}"
                )
            for entry in manifest.values():
                checked_objects.add(entry.object_sha256)
                self._verify_object(entry, ref_name, errors)

        if self.runs_dir.exists():
            for run_dir in sorted(self.runs_dir.iterdir()):
                if not run_dir.is_dir():
                    continue
                checked_runs.append(run_dir.name)
                self._verify_run(run_dir, errors, warnings)

        return {
            "dataspec": self.dataspec,
            "ok": not errors,
            "errors": errors,
            "warnings": warnings,
            "checked_commits": checked_commits,
            "checked_objects": len(checked_objects),
            "checked_runs": checked_runs,
        }

    def _iter_active_run_dirs(self) -> Iterator[Path]:
        if not self.runs_dir.exists():
            return
        for run_dir in self.runs_dir.iterdir():
            state_path = run_dir / "run_state.json"
            if not state_path.exists():
                continue
            try:
                data = json.loads(state_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            if data.get("status") != "completed":
                yield run_dir

    def _materialize_commit(
        self,
        commit_dir: Path,
        manifest_items: list[ManifestEntry],
        meta: dict,
    ) -> None:
        manifest_path = commit_dir / "manifest.jsonl"
        meta_path = commit_dir / "meta.json"
        if commit_dir.exists():
            if manifest_path.exists() and meta_path.exists():
                return
            shutil.rmtree(commit_dir, ignore_errors=True)

        tmp_dir = self.commits_dir / f".{commit_dir.name}.tmp"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
        tmp_dir.mkdir(parents=True, exist_ok=False)
        _write_manifest_atomic(tmp_dir / "manifest.jsonl", manifest_items)
        _write_json_atomic(tmp_dir / "meta.json", meta)
        tmp_dir.replace(commit_dir)

    def _verify_commit(
        self,
        commit_id: str,
        label: str,
        errors: list[str],
    ) -> Dict[str, ManifestEntry]:
        commit_dir = self.commits_dir / commit_id
        meta_path = commit_dir / "meta.json"
        manifest_path = commit_dir / "manifest.jsonl"
        if not commit_dir.exists():
            errors.append(f"{label}: commit directory missing ({commit_dir})")
            return {}
        if not meta_path.exists():
            errors.append(f"{label}: meta.json missing ({meta_path})")
        else:
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                errors.append(f"{label}: invalid meta.json ({meta_path})")
            else:
                if meta.get("commit_id") != commit_id:
                    errors.append(f"{label}: meta commit_id mismatch ({meta.get('commit_id')} != {commit_id})")

        manifest: Dict[str, ManifestEntry] = {}
        if not manifest_path.exists():
            errors.append(f"{label}: manifest.jsonl missing ({manifest_path})")
            return manifest

        with open(manifest_path, "r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, start=1):
                raw = line.strip()
                if not raw:
                    continue
                try:
                    entry = ManifestEntry.from_dict(json.loads(raw))
                except Exception as exc:
                    errors.append(f"{label}: invalid manifest line {lineno} ({exc})")
                    continue
                if entry.logical_filename in manifest:
                    errors.append(f"{label}: duplicate logical_filename {entry.logical_filename}")
                    continue
                manifest[entry.logical_filename] = entry
        return manifest

    def _verify_object(self, entry: ManifestEntry, label: str, errors: list[str]) -> None:
        object_path = self.object_path(entry.object_sha256)
        if not object_path.exists():
            errors.append(f"{label}: missing object for {entry.logical_filename} ({object_path})")
            return
        actual_size = object_path.stat().st_size
        if actual_size != entry.byte_count:
            errors.append(
                f"{label}: byte_count mismatch for {entry.logical_filename} ({actual_size} != {entry.byte_count})"
            )
        actual_hash = _hash_file(object_path)
        if actual_hash != entry.object_sha256:
            errors.append(
                f"{label}: sha256 mismatch for {entry.logical_filename} ({actual_hash} != {entry.object_sha256})"
            )

    def _verify_run(self, run_dir: Path, errors: list[str], warnings: list[str]) -> None:
        state_path = run_dir / "run_state.json"
        if not state_path.exists():
            errors.append(f"run {run_dir.name}: missing run_state.json")
            return

        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            errors.append(f"run {run_dir.name}: invalid run_state.json")
            return

        status = state.get("status", "")
        if status not in {"running", "failed", "completed"}:
            errors.append(f"run {run_dir.name}: invalid status {status!r}")

        manifest_entries = self.load_candidate_manifest(run_dir)
        staging_dir = run_dir / "staging"
        for logical_filename, data in manifest_entries.items():
            staging_name = data.get("staging_name", "")
            object_sha = data.get("sha256", "")
            staged_path = staging_dir / staging_name
            object_path = self.object_path(object_sha) if object_sha else None

            if not staged_path.exists() and not (object_path and object_path.exists()):
                errors.append(
                    f"run {run_dir.name}: missing staged/object file for {logical_filename} ({staging_name})"
                )
                continue

            if staged_path.exists():
                actual_size = staged_path.stat().st_size
                if actual_size != int(data.get('byte_count', -1)):
                    errors.append(
                        f"run {run_dir.name}: staged byte_count mismatch for {logical_filename}"
                    )
                actual_hash = _hash_file(staged_path)
                if actual_hash != object_sha:
                    errors.append(f"run {run_dir.name}: staged sha256 mismatch for {logical_filename}")

        for tmp in staging_dir.glob("*.tmp"):
            warnings.append(f"run {run_dir.name}: temporary file remains ({tmp.name})")

    def _refresh_view_ref(self, ref_name: str, ref: Optional[dict]) -> None:
        target_dir = self.view_dir / ref_name
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        if not ref:
            return

        manifest = self.load_commit_manifest(ref["commit_id"])
        for entry in manifest.values():
            dest_path = target_dir / entry.view_relpath
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            source_path = self.object_path(entry.object_sha256)
            _materialize_object(source_path, dest_path)


def _write_json_atomic(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _write_manifest_atomic(path: Path, entries: Iterable[ManifestEntry]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry.to_dict(), ensure_ascii=False) + "\n")
    tmp.replace(path)


def _hash_file(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    digest = sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _materialize_object(source_path: Path, object_path: Path) -> None:
    tmp_path = object_path.with_name(f"{object_path.name}.tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    try:
        os.link(source_path, tmp_path)
    except OSError:
        shutil.copy2(source_path, tmp_path)
    tmp_path.replace(object_path)


def _logical_group(logical_filename: str) -> str:
    if len(logical_filename) >= 2:
        return logical_filename[:2].upper()
    if logical_filename:
        return logical_filename.upper()
    return "_UNKNOWN"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
