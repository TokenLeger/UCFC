from __future__ import annotations

import argparse
import csv
import os
import re
import time
import unicodedata
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INBOX_DIR = REPO_ROOT / "data_fiscale" / "pdf" / "ucfc_pdf_inbox"

BASE_FIELDS = [
    "path",
    "filename",
    "title",
    "year",
    "source",
    "url",
    "priority",
    "notes",
    "date_label",
    "doc_type",
    "jurisdiction",
    "authority_rank",
    "court",
    "case_number",
]

_YEAR_RE = re.compile(r"^(?P<y1>\d{4})(?:-(?P<y2>\d{4}))?", re.ASCII)
_DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; UCFC PDF batch downloader)"
_ALLOWED_EXTS = {".pdf", ".docx", ".doc", ".xlsx", ".xls", ".csv", ".txt", ".md"}


def _parse_stream_arg(value: str) -> tuple[str, Path]:
    if "=" in value:
        name, path = value.split("=", 1)
        if name.strip() and path.strip():
            return name.strip(), Path(path.strip())
    path = Path(value)
    return path.stem or "stream", path


def _slugify(value: str) -> str:
    text = unicodedata.normalize("NFKD", value)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Za-z0-9]+", "_", text)
    text = text.strip("_").lower()
    return text or "document"


def _safe_filename(name: str) -> str:
    cleaned = name.replace("/", "_").replace("\\", "_")
    cleaned = cleaned.replace(":", "_")
    return cleaned


def _normalize_filename(name: str, default_ext: str = ".pdf") -> str:
    stem, ext = os.path.splitext(name)
    ext_l = ext.lower()
    if ext_l not in _ALLOWED_EXTS:
        ext_l = default_ext
        stem = stem or name
    stem = _slugify(stem or name)
    return _safe_filename(f"{stem}{ext_l}")


def _unique_filename(name: str, used: set[str]) -> str:
    if name not in used:
        used.add(name)
        return name
    stem, ext = os.path.splitext(name)
    counter = 2
    while True:
        candidate = f"{stem}_{counter}{ext}"
        if candidate not in used:
            used.add(candidate)
            return candidate
        counter += 1


def _infer_filename(row: dict, idx: int) -> str:
    raw = (row.get("filename") or "").strip()
    if raw:
        return _normalize_filename(raw)

    url = (row.get("url") or "").strip()
    title = (row.get("title") or "").strip()
    name = ""
    if url:
        parsed = urllib.parse.urlparse(url)
        name = Path(parsed.path).name
        if not name or name.lower() in {"pdf", "download", "file"}:
            query = urllib.parse.parse_qs(parsed.query)
            for key in ("uri", "id", "ecli", "num"):
                value = query.get(key)
                if value:
                    name = value[0]
                    break

    if not name:
        name = title or f"document_{idx:04d}"

    return _normalize_filename(name)


def _infer_title(row: dict, filename: str) -> str:
    title = (row.get("title") or "").strip()
    if title:
        return title
    name = os.path.splitext(filename)[0]
    return name.replace("_", " ").replace("-", " ").strip()


def _infer_year(row: dict, filename: str) -> str:
    year = (row.get("year") or row.get("date_label") or "").strip()
    if year:
        return year
    m = _YEAR_RE.match(filename)
    if not m:
        return ""
    y1 = m.group("y1") or ""
    y2 = m.group("y2") or ""
    if y2:
        return f"{y1}-{y2}"
    return y1


def _infer_doc_type_from_filename(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    if ext in {".csv", ".xlsx", ".xls"}:
        return "dataset"
    if ext in {".docx", ".doc", ".txt", ".md"}:
        return "document"
    return "jurisprudence"


def _read_urls_list(path: Path) -> list[dict]:
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        rows.append({"url": line})
    return rows


def _read_manifest_csv(path: Path) -> tuple[list[dict], list[str]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(item) for item in reader if item]
        fields = [f for f in (reader.fieldnames or []) if f]
    return rows, fields


def _next_batch_name(inbox_dir: Path) -> str:
    max_id = 0
    if inbox_dir.exists():
        for p in inbox_dir.iterdir():
            if not p.is_dir():
                continue
            if not p.name.startswith("batch_"):
                continue
            tail = p.name.split("_", 1)[-1]
            if not tail.isdigit():
                continue
            max_id = max(max_id, int(tail))
    return f"batch_{max_id + 1:03d}"


def _resolve_batch_dir(inbox_dir: Path, batch: str) -> tuple[str, Path]:
    if not batch:
        batch = _next_batch_name(inbox_dir)

    if os.sep in batch or "/" in batch or "\\" in batch:
        batch_path = Path(batch)
        return batch_path.name, batch_path

    if not batch.startswith("batch_"):
        batch = f"batch_{batch}"
    return batch, inbox_dir / batch


def _download_file(url: str, dest: Path, timeout: int, overwrite: bool, verbose: bool) -> str:
    if dest.exists() and not overwrite:
        if verbose:
            print(f"[pdf-batch] Skip existing: {dest}")
        return "skipped"

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest.with_suffix(dest.suffix + ".part")
    if tmp_path.exists():
        tmp_path.unlink()

    req = urllib.request.Request(url)
    req.add_header("User-Agent", _DEFAULT_USER_AGENT)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp, tmp_path.open("wb") as out:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
        tmp_path.replace(dest)
        if verbose:
            size = dest.stat().st_size if dest.exists() else 0
            print(f"[pdf-batch] Downloaded {dest.name} ({size} bytes)")
        return "downloaded"
    except Exception as exc:
        if verbose:
            print(f"[pdf-batch] Failed {url}: {exc}")
        if tmp_path.exists():
            tmp_path.unlink()
        return "failed"


def _build_output_rows(
    rows: list[dict],
    batch_dir: Path,
    defaults: dict,
) -> tuple[list[dict], list[str]]:
    used: set[str] = set()
    seen_urls: set[str] = set()
    output: list[dict] = []

    for idx, raw in enumerate(rows, start=1):
        row = {k: (v or "").strip() if isinstance(v, str) else v for k, v in raw.items()}
        url = (row.get("url") or "").strip()
        if not url:
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)

        filename = _unique_filename(_infer_filename(row, idx), used)
        title = _infer_title(row, filename)
        year = _infer_year(row, filename)

        out_row: dict = {}
        out_row.update(row)
        out_row["filename"] = filename
        out_row["path"] = str(batch_dir / filename)
        out_row["title"] = title
        out_row["year"] = row.get("year") or year
        out_row["date_label"] = row.get("date_label") or out_row["year"] or ""
        out_row["source"] = row.get("source") or defaults.get("source", "")
        out_row["priority"] = row.get("priority") or defaults.get("priority", "medium")
        out_row["notes"] = row.get("notes") or defaults.get("notes", "")
        out_row["doc_type"] = row.get("doc_type") or defaults.get("doc_type") or _infer_doc_type_from_filename(filename)
        out_row["jurisdiction"] = row.get("jurisdiction") or defaults.get("jurisdiction", "")
        if not str(row.get("authority_rank") or "").strip():
            if out_row["doc_type"] == "jurisprudence":
                out_row["authority_rank"] = "70"

        output.append(out_row)

    all_fields = set()
    for row in output:
        all_fields.update(row.keys())

    extra_fields = [f for f in sorted(all_fields) if f not in BASE_FIELDS]
    fieldnames = BASE_FIELDS + extra_fields
    return output, fieldnames


def _write_manifest(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Download document batch (PDF/DOCX/CSV/XLSX/TXT) into "
            "data_fiscale/pdf/ucfc_pdf_inbox/batch_*/ from URL list or CSV manifest."
        )
    )
    parser.add_argument(
        "--manifest",
        action="append",
        default=[],
        help="CSV with at least a 'url' column (repeatable, optional name=path)",
    )
    parser.add_argument(
        "--urls",
        action="append",
        default=[],
        help="Text file with one URL per line (repeatable, optional name=path)",
    )
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX_DIR), help="PDF inbox root")
    parser.add_argument("--batch", default="", help="Batch name or path (default: next batch_XXX)")
    parser.add_argument("--doc-type", dest="doc_type", default="")
    parser.add_argument("--jurisdiction", default="")
    parser.add_argument("--source", default="")
    parser.add_argument("--priority", default="medium")
    parser.add_argument("--notes", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--sleep", type=float, default=0.0, help="Seconds to wait between downloads")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    if not args.manifest and not args.urls:
        raise SystemExit("Provide --manifest (CSV) or --urls (text list).")

    inbox_dir = Path(args.inbox)
    batch_name, batch_dir = _resolve_batch_dir(inbox_dir, args.batch)
    batch_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []
    for item in args.manifest:
        stream, path = _parse_stream_arg(item)
        manifest_rows, _ = _read_manifest_csv(path)
        for row in manifest_rows:
            if not (row.get("stream") or "").strip():
                row["stream"] = stream
            rows.append(row)
    for item in args.urls:
        stream, path = _parse_stream_arg(item)
        url_rows = _read_urls_list(path)
        for row in url_rows:
            row["stream"] = stream
            rows.append(row)

    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    defaults = {
        "doc_type": args.doc_type,
        "jurisdiction": args.jurisdiction,
        "source": args.source,
        "priority": args.priority,
        "notes": args.notes,
    }

    out_rows, out_fields = _build_output_rows(rows, batch_dir=batch_dir, defaults=defaults)
    manifest_path = batch_dir / f"{batch_name}_manifest.csv"
    _write_manifest(manifest_path, out_rows, out_fields)

    if args.verbose:
        print(f"[pdf-batch] Manifest: {manifest_path}")

    downloaded = skipped = failed = 0
    for row in out_rows:
        url = (row.get("url") or "").strip()
        if not url:
            continue
        dest = Path(str(row.get("path") or "")).expanduser()
        status = _download_file(url, dest, timeout=args.timeout, overwrite=args.overwrite, verbose=args.verbose)
        if status == "downloaded":
            downloaded += 1
        elif status == "skipped":
            skipped += 1
        else:
            failed += 1
        if args.sleep and args.sleep > 0:
            time.sleep(args.sleep)

    print(
        f"[pdf-batch] Done. downloaded={downloaded} skipped={skipped} failed={failed} batch={batch_name}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
