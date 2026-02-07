from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_DIR = REPO_ROOT / "data_fiscale" / "raw"
DEFAULT_OUT_DIR = REPO_ROOT / "data_fiscale" / "processed"


@dataclass
class FileMeta:
    path: Path
    sha256: str
    bytes: int
    mtime_utc: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _file_mtime_utc(path: Path) -> str:
    ts = path.stat().st_mtime
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat()


def iter_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*"):
        if p.is_file():
            yield p


def build_manifest(raw_dir: Path, verbose: bool = False) -> list[FileMeta]:
    files = []
    count = 0
    for path in iter_files(raw_dir):
        if path.name.startswith("manifest_"):
            continue
        files.append(
            FileMeta(
                path=path,
                sha256=_sha256_file(path),
                bytes=path.stat().st_size,
                mtime_utc=_file_mtime_utc(path),
            )
        )
        count += 1
        if verbose and count % 100 == 0:
            print(f"[ingest] Scanned {count} file(s)...")
    return files


def _combined_hash(files: list[FileMeta]) -> str:
    h = hashlib.sha256()
    for f in sorted(files, key=lambda x: str(x.path)):
        h.update(f.sha256.encode("utf-8"))
    return h.hexdigest()


def write_versioned_manifest(files: list[FileMeta], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.jsonl"
    with manifest_path.open("w", encoding="utf-8") as f:
        for meta in files:
            row = {
                "path": str(meta.path),
                "sha256": meta.sha256,
                "bytes": meta.bytes,
                "mtime_utc": meta.mtime_utc,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return manifest_path


def write_corpus_version(files: list[FileMeta], out_dir: Path) -> Path:
    version_hash = _combined_hash(files)
    version_date = datetime.now().date().isoformat()
    version_id = f"{version_date}_{version_hash[:12]}"

    payload = {
        "version_id": version_id,
        "version_date": version_date,
        "file_count": len(files),
        "combined_sha256": version_hash,
        "generated_at_utc": _utc_now_iso(),
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "corpus_version.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return out_path


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Create a versioned manifest for raw corpus.")
    parser.add_argument("--raw", default=str(DEFAULT_RAW_DIR), help="Raw input folder")
    parser.add_argument("--out", default=str(DEFAULT_OUT_DIR), help="Processed output folder")
    parser.add_argument("--verbose", action="store_true", help="Verbose progress logs")
    args = parser.parse_args(argv)

    raw_dir = Path(args.raw)
    out_dir = Path(args.out)

    if args.verbose:
        print(f"[ingest] Scanning raw dir: {raw_dir}")
    files = build_manifest(raw_dir, verbose=args.verbose)
    if not files:
        print("No files found in raw corpus.")
        return 1

    version_hash = _combined_hash(files)
    version_date = datetime.now().date().isoformat()
    version_id = f"{version_date}_{version_hash[:12]}"
    version_dir = out_dir / version_id

    if args.verbose:
        print(f"[ingest] Writing manifest: {version_dir}")
    write_versioned_manifest(files, version_dir)
    write_corpus_version(files, version_dir)

    print(f"Corpus version created: {version_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
