from __future__ import annotations

import argparse
import csv
import os
import re
import time
import unicodedata
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INBOX_DIR = REPO_ROOT / "data_fiscale" / "pdf" / "ucfc_pdf_inbox"

XSEARCH_URL = "https://www.conseil-etat.fr/xsearch"
DOWNLOAD_URL = "https://www.conseil-etat.fr/plugin"
_DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; UCFC Conseil d'Etat batch downloader)"

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

_DATE_RE = re.compile(r"^\\d{4}-\\d{2}-\\d{2}$", re.ASCII)


def _slugify(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Za-z0-9]+", "_", text)
    text = text.strip("_").lower()
    return text or "document"


def _safe_filename(name: str) -> str:
    cleaned = name.replace("/", "_").replace("\\", "_")
    cleaned = cleaned.replace(":", "_")
    return cleaned


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
    batch = (batch or "").strip()
    if not batch:
        batch = _next_batch_name(inbox_dir)

    if os.sep in batch or "/" in batch or "\\" in batch:
        batch_path = Path(batch)
        return batch_path.name, batch_path

    if not batch.startswith("batch_"):
        batch = f"batch_{batch}"
    return batch, inbox_dir / batch


def _strip_bom(data: bytes) -> bytes:
    if data.startswith(b"\xef\xbb\xbf"):
        return data[3:]
    return data


def _parse_iso_date(value: str) -> Optional[date]:
    value = (value or "").strip()
    if not value or not _DATE_RE.match(value):
        return None
    try:
        y, m, d = value.split("-", 2)
        return date(int(y), int(m), int(d))
    except Exception:
        return None


def _xsearch_url(
    query: str,
    additional_where: str,
    sort: str,
    skip_from: int,
    skip_count: int,
    tab: str,
) -> str:
    params = {
        "text": query,
        "additionalWhereClause": additional_where,
        "sort": sort,
        "skipCount": str(skip_count),
        "skipFrom": str(skip_from),
        "tabSearchValueSelected": tab,
    }
    return XSEARCH_URL + "?" + urllib.parse.urlencode(params)


def _fetch_xsearch(
    query: str,
    additional_where: str,
    sort: str,
    skip_from: int,
    skip_count: int,
    tab: str,
    timeout: int,
) -> tuple[int, list[dict]]:
    url = _xsearch_url(
        query=query,
        additional_where=additional_where,
        sort=sort,
        skip_from=skip_from,
        skip_count=skip_count,
        tab=tab,
    )
    req = urllib.request.Request(url, headers={"User-Agent": _DEFAULT_USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = _strip_bom(resp.read())
    root = ET.fromstring(data)
    total_count_s = root.findtext("TotalCount") or root.findtext("DocumentCount") or "0"
    try:
        total_count = int(total_count_s.strip() or "0")
    except Exception:
        total_count = 0
    docs_parent = root.find("Documents")
    if docs_parent is None:
        return total_count, []
    docs: list[dict] = []
    for doc in docs_parent.findall("Document"):
        payload: dict = {}
        for child in list(doc):
            text = (child.text or "").strip()
            if not text:
                continue
            payload[child.tag] = text
        if payload:
            docs.append(payload)
    return total_count, docs


def _download_url(doc_id: str) -> str:
    params = {
        "plugin": "Service.downloadFilePagePlugin",
        "Index": "Ariane_Web",
        "Id": doc_id,
    }
    return DOWNLOAD_URL + "?" + urllib.parse.urlencode(params)


def _download_file(url: str, dest: Path, timeout: int, overwrite: bool, verbose: bool) -> str:
    if dest.exists() and not overwrite:
        if verbose:
            print(f"[ce] Skip existing: {dest}")
        return "skipped"

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest.with_suffix(dest.suffix + ".part")
    if tmp_path.exists():
        tmp_path.unlink()

    req = urllib.request.Request(url, headers={"User-Agent": _DEFAULT_USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp, tmp_path.open("wb") as out:
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "pdf" not in ctype:
                raise RuntimeError(f"Unexpected content-type: {ctype or 'unknown'}")
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
        tmp_path.replace(dest)
        if verbose:
            print(f"[ce] Downloaded {dest.name} ({dest.stat().st_size} bytes)")
        return "downloaded"
    except Exception as exc:
        if verbose:
            print(f"[ce] Failed {url}: {exc}")
        if tmp_path.exists():
            tmp_path.unlink()
        return "failed"


def _iter_existing_manifests(inbox_dir: Path) -> Iterable[Path]:
    if not inbox_dir.exists():
        return
    for batch_dir in inbox_dir.iterdir():
        if not batch_dir.is_dir():
            continue
        if not batch_dir.name.startswith("batch_"):
            continue
        for manifest in batch_dir.glob("*_manifest.csv"):
            yield manifest


@dataclass(frozen=True)
class Seen:
    ariane_ids: set[str]
    urls: set[str]


def _load_seen(inbox_dir: Path) -> Seen:
    seen_ids: set[str] = set()
    seen_urls: set[str] = set()
    for manifest in _iter_existing_manifests(inbox_dir):
        try:
            with manifest.open("r", encoding="utf-8", errors="replace", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if not row:
                        continue
                    url = (row.get("url") or "").strip()
                    if url:
                        seen_urls.add(url)
                    ariane_id = (row.get("ariane_id") or "").strip()
                    if ariane_id:
                        seen_ids.add(ariane_id)
        except Exception:
            continue
    return Seen(ariane_ids=seen_ids, urls=seen_urls)


def _build_filename(doc: dict, used: set[str]) -> str:
    doc_date = (doc.get("SourceDateTime1") or "").strip()
    number = (doc.get("SourceStr5") or doc.get("SourceCsv1") or "").strip()
    if not number:
        file_name = (doc.get("FileName") or "").strip()
        if "_" in file_name:
            number = file_name.split("_", 1)[0].strip()
        else:
            number = file_name.replace(".pdf", "").strip()
    slug_source = (
        doc.get("SourceStr21")
        or doc.get("Title")
        or doc.get("HtmlSummary")
        or doc.get("Extracts")
        or doc.get("SourceStr9")
        or doc.get("SourceStr30")
        or "decision"
    )
    slug = _slugify(slug_source)
    slug = slug[:80].strip("_") or "decision"
    base = f"{doc_date}_ce_{number}_{slug}.pdf" if number else f"{doc_date}_ce_{slug}.pdf"
    return _unique_filename(_safe_filename(base), used)


def _build_title(doc: dict) -> str:
    number = (doc.get("SourceStr5") or "").strip()
    doc_date = (doc.get("SourceDateTime1") or "").strip()
    ecli = (doc.get("SourceStr30") or "").strip()
    parts = ["Conseil d'État"]
    if number:
        parts.append(f"N° {number}")
    if doc_date:
        parts.append(doc_date)
    if ecli:
        parts.append(ecli)
    return " — ".join(parts)


def _build_notes(doc: dict) -> str:
    pieces: list[str] = []
    decision_type = (doc.get("SourceStr9") or "").strip()
    formation = (doc.get("SourceStr7") or "").strip()
    satisfaction = (doc.get("SourceStr12") or "").strip()
    summary = (doc.get("SourceStr21") or "").strip()
    pcja = (doc.get("SourceCsv3") or "").strip()
    if decision_type:
        pieces.append(decision_type)
    if formation:
        pieces.append(f"Formation: {formation}")
    if satisfaction:
        pieces.append(f"Dispositif: {satisfaction}")
    if pcja:
        pieces.append(f"PCJA: {pcja}")
    if summary:
        clean = " ".join(summary.split())
        pieces.append(clean[:300])
    return "; ".join(pieces)


def _write_manifest(path: Path, rows: list[dict]) -> None:
    all_fields: set[str] = set()
    for row in rows:
        all_fields.update(row.keys())
    extra_fields = [f for f in sorted(all_fields) if f not in BASE_FIELDS]
    fieldnames = BASE_FIELDS + extra_fields
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def download_batch(
    inbox_dir: Path,
    batch: str,
    query: str,
    pcja_tree: str,
    limit: int,
    skip_from: int,
    timeout: int,
    sleep_s: float,
    overwrite: bool,
    verbose: bool,
    min_date: Optional[date],
) -> dict:
    inbox_dir = Path(inbox_dir)
    batch_name, batch_dir = _resolve_batch_dir(inbox_dir, batch)
    batch_dir.mkdir(parents=True, exist_ok=True)

    used_filenames: set[str] = set(p.name for p in batch_dir.glob("*.pdf"))
    seen = _load_seen(inbox_dir)

    additional_where = " and FileExt='pdf'"
    if pcja_tree:
        additional_where += f" and SourceTree1 contains '/{pcja_tree}/'"

    sort = "SourceDateTime1 desc"
    tab = "/Ariane_Web/*"
    page_size = 20

    downloaded_rows: list[dict] = []
    downloaded = skipped = failed = 0
    cursor = max(0, int(skip_from))

    if verbose:
        print(f"[ce] batch={batch_name} dir={batch_dir}")
        print(f"[ce] query={query!r} pcja=/{pcja_tree}/ limit={limit} skip_from={cursor}")

    attempts = 0
    while len(downloaded_rows) < limit:
        total, docs = _fetch_xsearch(
            query=query,
            additional_where=additional_where,
            sort=sort,
            skip_from=cursor,
            skip_count=page_size,
            tab=tab,
            timeout=timeout,
        )
        if not docs:
            break
        if verbose and attempts == 0:
            print(f"[ce] Total candidates: {total}")
        attempts += 1

        for doc in docs:
            if len(downloaded_rows) >= limit:
                break
            doc_id = (doc.get("Id") or "").strip()
            if not doc_id:
                continue
            if doc_id in seen.ariane_ids:
                continue

            doc_date_s = (doc.get("SourceDateTime1") or "").strip()
            doc_date = _parse_iso_date(doc_date_s)
            if min_date and doc_date and doc_date < min_date:
                continue

            url = _download_url(doc_id)
            if url in seen.urls:
                continue

            filename = _build_filename(doc, used=used_filenames)
            dest = batch_dir / filename
            status = _download_file(url, dest, timeout=timeout, overwrite=overwrite, verbose=verbose)
            if status == "downloaded":
                downloaded += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed += 1
                continue

            number = (doc.get("SourceStr5") or "").strip()
            row = {
                "path": str(dest.resolve()),
                "filename": filename,
                "title": _build_title(doc),
                "year": doc_date_s,
                "source": "Conseil d'État (ArianeWeb)",
                "url": url,
                "priority": "high",
                "notes": _build_notes(doc),
                "date_label": doc_date_s,
                "doc_type": "jurisprudence",
                "jurisdiction": "FR",
                "authority_rank": "85",
                "court": "Conseil d'État",
                "case_number": number,
                # Extra metadata.
                "ariane_id": doc_id,
                "ecli": (doc.get("SourceStr30") or "").strip(),
                "formation": (doc.get("SourceStr7") or "").strip(),
                "pcja": (doc.get("SourceCsv3") or "").strip(),
                "collection": (doc.get("SourceStr4") or "").strip(),
                "file_name_original": (doc.get("FileName") or "").strip(),
                "indexation_time": (doc.get("IndexationTime") or "").strip(),
                "decision_type": (doc.get("SourceStr9") or "").strip(),
                "satisfaction": (doc.get("SourceStr12") or "").strip(),
            }
            downloaded_rows.append(row)
            seen.ariane_ids.add(doc_id)
            seen.urls.add(url)

            if sleep_s > 0:
                time.sleep(sleep_s)

        cursor += page_size
        if cursor >= total:
            break
        if attempts > 200:
            break

    manifest_path = batch_dir / f"{batch_name}_manifest.csv"
    _write_manifest(manifest_path, downloaded_rows)

    return {
        "batch": batch_name,
        "batch_dir": str(batch_dir),
        "manifest": str(manifest_path),
        "downloaded": int(downloaded),
        "skipped": int(skipped),
        "failed": int(failed),
        "rows": int(len(downloaded_rows)),
        "query": query,
        "pcja_tree": pcja_tree,
        "skip_from": int(skip_from),
        "min_date": min_date.isoformat() if min_date else "",
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Download Conseil d'État jurisprudence PDFs via xsearch (ArianeWeb).")
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX_DIR))
    parser.add_argument("--batch", default="", help="Batch name (default: next batch_XXX)")
    parser.add_argument("--query", default="impot", help="Full-text query")
    parser.add_argument("--pcja", default="19", help="PCJA tree code (default: 19 = fiscal)")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--skip-from", type=int, default=0)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--sleep", type=float, default=0.2)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--min-date", default="2006-01-01", help="Min decision date (YYYY-MM-DD, empty=none)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    min_date = _parse_iso_date(args.min_date) if (args.min_date or "").strip() else None
    report = download_batch(
        inbox_dir=Path(args.inbox),
        batch=args.batch,
        query=args.query,
        pcja_tree=(args.pcja or "").strip(),
        limit=max(1, int(args.limit)),
        skip_from=max(0, int(args.skip_from)),
        timeout=max(5, int(args.timeout)),
        sleep_s=max(0.0, float(args.sleep)),
        overwrite=bool(args.overwrite),
        verbose=bool(args.verbose),
        min_date=min_date,
    )
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
