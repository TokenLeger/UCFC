from __future__ import annotations

import argparse
import hashlib
import re
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_DIR = REPO_ROOT / "data_fiscale" / "raw" / "legi"
DEFAULT_BASE_URL = "https://echanges.dila.gouv.fr/OPENDATA/LEGI/"

_FILE_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)
_TS_RE = re.compile(r"(\d{8})-(\d{6})")


@dataclass
class DownloadResult:
    path: Path
    bytes: int
    sha256: str
    downloaded_at_utc: str
    url: str
    name: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_text(url: str, timeout: int = 60) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        payload = resp.read()
    return payload.decode("utf-8", errors="replace")


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


def list_available_files(base_url: str = DEFAULT_BASE_URL) -> list[str]:
    html = _read_text(base_url)
    names = []
    for href in _FILE_RE.findall(html):
        href = href.strip()
        if href.endswith(".tar.gz") or href.endswith(".tgz") or href.endswith(".tar"):
            names.append(href)
    return sorted(set(names))


def _extract_ts(name: str) -> Optional[tuple[int, int]]:
    m = _TS_RE.search(name)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def _pick_latest(names: Iterable[str]) -> Optional[str]:
    best = None
    best_ts = None
    for name in names:
        ts = _extract_ts(name)
        if ts is None:
            continue
        if best_ts is None or ts > best_ts:
            best_ts = ts
            best = name
    return best


def select_files(
    names: list[str],
    mode: str = "full",
    limit: int = 0,
) -> list[str]:
    mode = mode.lower().strip()
    if mode == "all":
        selected = names
    elif mode == "latest":
        selected = [n for n in names if n.startswith("LEGI_")]
        latest = _pick_latest(selected)
        selected = [latest] if latest else []
    elif mode == "full":
        selected = [n for n in names if n.lower().startswith("freemium_legi_global_")]
        latest = _pick_latest(selected)
        if latest:
            selected = [latest]
        else:
            # fallback to latest delta
            selected = [n for n in names if n.startswith("LEGI_")]
            latest = _pick_latest(selected)
            selected = [latest] if latest else []
    else:
        raise ValueError(f"Unknown mode: {mode}")

    if limit and limit > 0:
        selected = selected[:limit]
    return selected


def download_files(
    base_url: str,
    names: Iterable[str],
    out_dir: Path = DEFAULT_RAW_DIR,
    overwrite: bool = False,
    verbose: bool = False,
) -> list[DownloadResult]:
    results: list[DownloadResult] = []
    names = list(names)
    total = len(names)
    for idx, name in enumerate(names, start=1):
        url = base_url.rstrip("/") + "/" + name
        dest = out_dir / name
        if dest.exists() and not overwrite:
            if verbose:
                print(f"[legi] Skip existing: {dest}")
            results.append(
                DownloadResult(
                    path=dest,
                    bytes=dest.stat().st_size,
                    sha256=_sha256_file(dest),
                    downloaded_at_utc=_utc_now_iso(),
                    url=url,
                    name=name,
                )
            )
            continue

        if verbose:
            print(f"[legi] ({idx}/{total}) Downloading: {url} -> {dest}")
        size = _download_file(url, dest)
        results.append(
            DownloadResult(
                path=dest,
                bytes=size,
                sha256=_sha256_file(dest),
                downloaded_at_utc=_utc_now_iso(),
                url=url,
                name=name,
            )
        )
    return results


def write_manifest(results: list[DownloadResult], out_dir: Path = DEFAULT_RAW_DIR) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / f"manifest_legi_{datetime.now().date().isoformat()}.tsv"
    lines = ["downloaded_at_utc\tpath\tsha256\tbytes\turl\tname"]
    for r in results:
        lines.append(
            f"{r.downloaded_at_utc}\t{r.path}\t{r.sha256}\t{r.bytes}\t{r.url}\t{r.name}"
        )
    manifest_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return manifest_path


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Download LEGI open data dumps (DILA).")
    parser.add_argument("--out", default=str(DEFAULT_RAW_DIR), help="Output folder")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base URL for LEGI dumps")
    parser.add_argument("--mode", default="full", choices=["full", "latest", "all"], help="Download mode")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of files")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--list", action="store_true", help="List available files and exit")
    parser.add_argument("--verbose", action="store_true", help="Verbose progress logs")
    args = parser.parse_args(argv)

    names = list_available_files(args.base_url)
    if args.list:
        print("\n".join(names))
        return 0

    selected = select_files(names, mode=args.mode, limit=args.limit)
    if not selected:
        print("No files selected.")
        return 1

    results = download_files(
        args.base_url,
        selected,
        out_dir=Path(args.out),
        overwrite=args.overwrite,
        verbose=args.verbose,
    )
    manifest_path = write_manifest(results, out_dir=Path(args.out))
    print(f"Downloaded {len(results)} file(s). Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
