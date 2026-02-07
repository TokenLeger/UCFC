from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_DIR = REPO_ROOT / "data_fiscale" / "raw" / "bofip"

# Official BOFiP open data export (JSON) from data.economie.gouv.fr
DEFAULT_MANIFEST_URL = os.getenv(
    "BOFIP_MANIFEST_URL",
    "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/bofip-impots/exports/json",
)


@dataclass
class BofipEntry:
    file_name: str
    download_url: str
    checksum_url: Optional[str]
    date_start: Optional[str]
    date_end: Optional[str]


@dataclass
class DownloadResult:
    path: Path
    sha256: str
    bytes: int
    downloaded_at_utc: str
    entry: BofipEntry


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(url: str, timeout: int = 60) -> list[dict]:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        payload = resp.read().decode("utf-8")
    return json.loads(payload)


def _safe_filename(name: str) -> str:
    return name.replace("/", "_")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _download_file(url: str, dest: Path, timeout: int = 60) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=timeout) as resp, dest.open("wb") as out:
        total = 0
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
            total += len(chunk)
    return total


def _build_entry(row: dict) -> Optional[BofipEntry]:
    download_url = row.get("telechargement")
    if not download_url:
        return None

    file_name = row.get("nom_du_fichier") or Path(
        urllib.parse.urlparse(download_url).path
    ).name

    return BofipEntry(
        file_name=_safe_filename(file_name),
        download_url=download_url,
        checksum_url=row.get("empreinte"),
        date_start=row.get("date_de_debut"),
        date_end=row.get("date_de_fin"),
    )


def fetch_manifest(url: str = DEFAULT_MANIFEST_URL) -> list[BofipEntry]:
    rows = _read_json(url)
    entries: list[BofipEntry] = []
    for row in rows:
        entry = _build_entry(row)
        if entry:
            entries.append(entry)
    return entries


def download_entry(
    entry: BofipEntry,
    out_dir: Path = DEFAULT_RAW_DIR,
    overwrite: bool = False,
    verbose: bool = False,
) -> DownloadResult:
    date_label = "unknown_date"
    if entry.date_start and entry.date_end:
        date_label = f"{entry.date_start}_to_{entry.date_end}"

    dest = out_dir / date_label / entry.file_name
    if dest.exists() and not overwrite:
        if verbose:
            print(f"[bofip] Skip existing: {dest}")
        sha = _sha256_file(dest)
        return DownloadResult(
            path=dest,
            sha256=sha,
            bytes=dest.stat().st_size,
            downloaded_at_utc=_utc_now_iso(),
            entry=entry,
        )

    if verbose:
        print(f"[bofip] Downloading: {entry.download_url} -> {dest}")
    size = _download_file(entry.download_url, dest)
    sha = _sha256_file(dest)

    return DownloadResult(
        path=dest,
        sha256=sha,
        bytes=size,
        downloaded_at_utc=_utc_now_iso(),
        entry=entry,
    )


def download_all(
    entries: Iterable[BofipEntry],
    out_dir: Path = DEFAULT_RAW_DIR,
    overwrite: bool = False,
    verbose: bool = False,
) -> list[DownloadResult]:
    results: list[DownloadResult] = []
    entries = list(entries)
    total = len(entries)
    for idx, entry in enumerate(entries, start=1):
        if verbose:
            print(f"[bofip] ({idx}/{total}) {entry.file_name}")
        results.append(
            download_entry(entry, out_dir=out_dir, overwrite=overwrite, verbose=verbose)
        )
    return results


def write_manifest(results: list[DownloadResult], out_dir: Path = DEFAULT_RAW_DIR) -> Path:
    manifest_path = out_dir / f"manifest_{datetime.now().date().isoformat()}.jsonl"
    with manifest_path.open("w", encoding="utf-8") as f:
        for r in results:
            row = {
                "downloaded_at_utc": r.downloaded_at_utc,
                "path": str(r.path),
                "sha256": r.sha256,
                "bytes": r.bytes,
                "file_name": r.entry.file_name,
                "download_url": r.entry.download_url,
                "checksum_url": r.entry.checksum_url,
                "date_start": r.entry.date_start,
                "date_end": r.entry.date_end,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return manifest_path


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Download BOFiP open data dumps.")
    parser.add_argument("--out", default=str(DEFAULT_RAW_DIR), help="Output folder")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of files")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing")
    parser.add_argument("--verbose", action="store_true", help="Verbose progress logs")
    parser.add_argument(
        "--manifest-url",
        default=DEFAULT_MANIFEST_URL,
        help="Manifest URL (JSON export)",
    )
    args = parser.parse_args(argv)

    entries = fetch_manifest(args.manifest_url)
    if args.limit:
        entries = entries[: args.limit]

    results = download_all(
        entries, out_dir=Path(args.out), overwrite=args.overwrite, verbose=args.verbose
    )
    manifest_path = write_manifest(results, out_dir=Path(args.out))
    print(f"Downloaded {len(results)} file(s). Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
