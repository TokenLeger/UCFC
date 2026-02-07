from __future__ import annotations

import argparse
import csv
import json
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
import xml.etree.ElementTree as ET


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INBOX_DIR = REPO_ROOT / "data_fiscale" / "pdf" / "ucfc_pdf_inbox"
DEFAULT_PROCESSED_DIR = REPO_ROOT / "data_fiscale" / "processed"

_YEAR_RE = re.compile(r"^(?P<y1>\\d{4})(?:-(?P<y2>\\d{4}))?_", re.ASCII)
_SUPPORTED_EXTS = {".pdf", ".docx", ".csv", ".xlsx", ".txt", ".md"}


@dataclass(frozen=True)
class ManifestRow:
    pdf_path: Path
    filename: str
    title: str
    year: str
    source: str
    url: str
    priority: str
    notes: str
    batch_id: str
    extra: dict


def _find_latest_processed_dir(processed_dir: Path) -> Optional[Path]:
    if not processed_dir.exists():
        return None
    candidates = [p for p in processed_dir.iterdir() if p.is_dir()]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _default_out_path(processed_dir: Path) -> Optional[Path]:
    latest = _find_latest_processed_dir(processed_dir)
    if not latest:
        return None
    normalized_dir = latest / "normalized"
    return normalized_dir / "pdf_inbox.jsonl"


def _read_manifest_csv(manifest_path: Path, batch_id: str) -> list[ManifestRow]:
    rows: list[ManifestRow] = []
    with manifest_path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for item in reader:
            if not item:
                continue
            filename = (item.get("filename") or "").strip()
            path_s = (item.get("path") or "").strip()
            if path_s:
                pdf_path = Path(path_s)
            elif filename:
                pdf_path = manifest_path.parent / filename
            else:
                continue

            title = (item.get("title") or "").strip()
            year = (item.get("year") or item.get("date_label") or "").strip()
            source = (item.get("source") or "").strip()
            url = (item.get("url") or "").strip()
            priority = (item.get("priority") or "").strip()
            notes = (item.get("notes") or "").strip()

            extra: dict = {}
            for k, v in item.items():
                if k in {"path", "filename", "title", "year", "source", "url", "priority", "notes", "date_label"}:
                    continue
                if v is None:
                    continue
                extra[k] = str(v)

            rows.append(
                ManifestRow(
                    pdf_path=pdf_path,
                    filename=filename or pdf_path.name,
                    title=title,
                    year=year,
                    source=source,
                    url=url,
                    priority=priority,
                    notes=notes,
                    batch_id=batch_id,
                    extra=extra,
                )
            )
    return rows


def _iter_batches(inbox_dir: Path) -> Iterable[tuple[str, Path]]:
    if not inbox_dir.exists():
        return
    for p in sorted(inbox_dir.iterdir()):
        if not p.is_dir():
            continue
        if not p.name.startswith("batch_"):
            continue
        yield p.name, p


def _iter_manifest_rows(inbox_dir: Path, verbose: bool) -> Iterable[ManifestRow]:
    for batch_id, batch_dir in _iter_batches(inbox_dir):
        manifests = sorted(batch_dir.glob("*_manifest.csv"))
        if not manifests:
            if verbose:
                print(f"[pdf] Skip {batch_id}: no *_manifest.csv")
            continue
        if len(manifests) > 1 and verbose:
            print(f"[pdf] {batch_id}: multiple manifests, using all ({len(manifests)})")

        for manifest_path in manifests:
            for row in _read_manifest_csv(manifest_path, batch_id=batch_id):
                yield row


def _infer_year(filename: str, fallback: str) -> str:
    if fallback:
        return fallback
    m = _YEAR_RE.match(filename)
    if not m:
        return ""
    y1 = m.group("y1") or ""
    y2 = m.group("y2") or ""
    if y2:
        return f"{y1}-{y2}"
    return y1


def _infer_jurisdiction(row: ManifestRow) -> str:
    source = (row.source or "").lower()
    url = (row.url or "").lower()
    title = (row.title or "").lower()
    if "eur-lex" in source or "eur-lex" in url or "directive (ue)" in title or "directive ue" in title:
        return "UE"
    return "FR"


def _infer_doc_type(row: ManifestRow) -> str:
    source = (row.source or "").lower()
    url = (row.url or "").lower()
    title = (row.title or "").lower()
    if "eur-lex" in source or "eur-lex" in url:
        if "directive" in title:
            return "directive_ue"
        return "norme_ue"
    if "cour des comptes" in source:
        return "rapport"
    if "prelevements obligatoires" in source:
        return "rapport"
    return "document"


def _infer_authority_rank(doc_type: str) -> int:
    # Heuristic scoring for ranking/reranking (0..100).
    # High-level: EU norms > FR norms > jurisprudence > doctrine > reports > other docs.
    if doc_type in {"reglement_ue"}:
        return 90
    if doc_type in {"directive_ue", "norme_ue"}:
        return 80
    if doc_type in {"loi"}:
        return 75
    if doc_type in {"reglement_fr"}:
        return 65
    if doc_type in {"jurisprudence"}:
        return 70
    if doc_type in {"doctrine"}:
        return 50
    if doc_type in {"rapport"}:
        return 25
    return 30


def _clean_page_text(text: str) -> str:
    # Keep it simple: collapse whitespace to improve search/embeddings.
    lines = [" ".join(line.split()) for line in text.splitlines()]
    lines = [line for line in lines if line]
    return " ".join(lines).strip()


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _iter_clean_pages(pdf_path: Path, max_pages: int, verbose: bool) -> Iterable[tuple[int, str]]:
    try:
        from pypdf import PdfReader
    except Exception as exc:
        raise SystemExit("Missing PDF dependency. Install: pip install pypdf") from exc

    reader = PdfReader(str(pdf_path))
    total = len(reader.pages)
    limit = total if max_pages <= 0 else min(total, max_pages)
    for idx in range(limit):
        if verbose and (idx == 0 or (idx + 1) % 25 == 0 or idx + 1 == limit):
            print(f"[pdf] {pdf_path.name}: page {idx + 1}/{limit}")
        raw = reader.pages[idx].extract_text() or ""
        cleaned = _clean_page_text(raw)
        if not cleaned:
            continue
        yield idx, cleaned


def _iter_docx_blocks(path: Path, max_blocks: int, verbose: bool) -> Iterable[tuple[int, str]]:
    try:
        with zipfile.ZipFile(path) as zf:
            xml_bytes = zf.read("word/document.xml")
    except Exception as exc:
        if verbose:
            print(f"[docx] Failed: {path} ({exc})")
        return

    try:
        root = ET.fromstring(xml_bytes)
    except Exception as exc:
        if verbose:
            print(f"[docx] Parse failed: {path} ({exc})")
        return

    idx = 0
    for para in root.iter():
        if _strip_ns(para.tag) != "p":
            continue
        chunks: list[str] = []
        for node in para.iter():
            if _strip_ns(node.tag) == "t" and node.text:
                chunks.append(node.text)
        text = _clean_page_text(" ".join(chunks))
        if not text:
            continue
        yield idx, text
        idx += 1
        if max_blocks > 0 and idx >= max_blocks:
            break


def _iter_text_blocks(path: Path, max_lines: int, verbose: bool) -> Iterable[tuple[int, str]]:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for idx, line in enumerate(handle):
                if max_lines > 0 and idx >= max_lines:
                    break
                text = _clean_page_text(line)
                if not text:
                    continue
                yield idx, text
    except Exception as exc:
        if verbose:
            print(f"[text] Failed: {path} ({exc})")


def _iter_csv_blocks(path: Path, max_rows: int, verbose: bool) -> Iterable[tuple[int, str]]:
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
            reader = csv.reader(handle)
            for idx, row in enumerate(reader):
                if max_rows > 0 and idx >= max_rows:
                    break
                text = _clean_page_text(" | ".join(str(v) for v in row if v is not None))
                if not text:
                    continue
                yield idx, text
    except Exception as exc:
        if verbose:
            print(f"[csv] Failed: {path} ({exc})")


def _read_xlsx_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    try:
        xml_bytes = zf.read("xl/sharedStrings.xml")
    except Exception:
        return []
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []

    strings: list[str] = []
    for si in root.iter():
        if _strip_ns(si.tag) != "si":
            continue
        chunks: list[str] = []
        for node in si.iter():
            if _strip_ns(node.tag) == "t" and node.text:
                chunks.append(node.text)
        text = _clean_page_text(" ".join(chunks))
        strings.append(text)
    return strings


def _iter_xlsx_blocks(path: Path, max_rows: int, verbose: bool) -> Iterable[tuple[int, str]]:
    try:
        with zipfile.ZipFile(path) as zf:
            shared = _read_xlsx_shared_strings(zf)
            sheet_files = sorted(
                name
                for name in zf.namelist()
                if name.startswith("xl/worksheets/sheet") and name.endswith(".xml")
            )
            idx = 0
            for sheet_file in sheet_files:
                try:
                    xml_bytes = zf.read(sheet_file)
                except Exception:
                    continue
                try:
                    root = ET.fromstring(xml_bytes)
                except Exception:
                    continue

                sheet_label = Path(sheet_file).stem
                for row in root.iter():
                    if _strip_ns(row.tag) != "row":
                        continue
                    cells: list[str] = []
                    for cell in row:
                        if _strip_ns(cell.tag) != "c":
                            continue
                        cell_type = cell.attrib.get("t") or ""
                        value = ""
                        v_elem = None
                        for child in cell:
                            tag = _strip_ns(child.tag)
                            if tag == "v":
                                v_elem = child
                                break
                            if tag == "is":
                                texts = [
                                    t.text
                                    for t in child.iter()
                                    if _strip_ns(t.tag) == "t" and t.text
                                ]
                                value = " ".join(texts)
                        if v_elem is not None and v_elem.text:
                            if cell_type == "s":
                                try:
                                    value = shared[int(v_elem.text)]
                                except Exception:
                                    value = v_elem.text
                            else:
                                value = v_elem.text
                        if value:
                            cells.append(value)
                    row_text = _clean_page_text(" | ".join(cells))
                    if not row_text:
                        continue
                    text = f"[{sheet_label}] {row_text}".strip()
                    yield idx, text
                    idx += 1
                    if max_rows > 0 and idx >= max_rows:
                        return
    except Exception as exc:
        if verbose:
            print(f"[xlsx] Failed: {path} ({exc})")


def _iter_clean_blocks(path: Path, max_pages: int, verbose: bool) -> Iterable[tuple[int, str]]:
    suffix = path.suffix.lower()
    if suffix not in _SUPPORTED_EXTS:
        if verbose:
            print(f"[pdf] Unsupported file type: {path}")
        return
    if suffix == ".pdf":
        yield from _iter_clean_pages(path, max_pages=max_pages, verbose=verbose)
        return
    if suffix == ".docx":
        yield from _iter_docx_blocks(path, max_blocks=max_pages, verbose=verbose)
        return
    if suffix in {".txt", ".md"}:
        yield from _iter_text_blocks(path, max_lines=max_pages, verbose=verbose)
        return
    if suffix == ".csv":
        yield from _iter_csv_blocks(path, max_rows=max_pages, verbose=verbose)
        return
    if suffix == ".xlsx":
        yield from _iter_xlsx_blocks(path, max_rows=max_pages, verbose=verbose)
        return


def _chunk_pages(pages: Iterable[tuple[int, str]], chunk_chars: int) -> Iterable[tuple[int, int, str]]:
    start: Optional[int] = None
    last_idx: Optional[int] = None
    buf: list[str] = []
    buf_len = 0
    for idx, page in pages:
        if start is None:
            start = idx
        block = f"----- PAGE {idx + 1} -----\n{page}".strip()
        block_len = len(block)
        if buf and chunk_chars > 0 and (buf_len + 2 + block_len) > chunk_chars:
            if start is not None and last_idx is not None:
                yield start, last_idx, "\n\n".join(buf).strip()
            start = idx
            buf = [block]
            buf_len = block_len
            last_idx = idx
            continue
        else:
            buf.append(block)
            buf_len += (2 if buf_len else 0) + block_len
            last_idx = idx

    if buf and start is not None and last_idx is not None:
        yield start, last_idx, "\n\n".join(buf).strip()


def normalize_pdf_inbox(
    inbox_dir: Path,
    out_path: Path,
    chunk_chars: int,
    max_pages: int,
    verbose: bool,
) -> dict:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    pdf_count = 0
    chunk_count = 0
    skipped_missing = 0
    skipped_empty = 0

    with out_path.open("w", encoding="utf-8") as out:
        for row in _iter_manifest_rows(inbox_dir, verbose=verbose):
            keep_s = str(row.extra.get("keep") or row.extra.get("ucfc_keep") or "").strip().lower()
            if keep_s in {"0", "false", "no", "discard", "skip"}:
                continue

            year = _infer_year(row.filename, row.year)
            title = row.title or row.filename
            jurisdiction = str(row.extra.get("jurisdiction") or "").strip() or _infer_jurisdiction(row)
            doc_type = str(row.extra.get("doc_type") or "").strip() or _infer_doc_type(row)
            status = str(row.extra.get("status") or "").strip() or "current"
            superseded_by = str(row.extra.get("superseded_by") or "").strip()
            stream = str(row.extra.get("stream") or "").strip()
            keywords = str(
                row.extra.get("keywords") or row.extra.get("keyword") or row.extra.get("tags") or ""
            ).strip()
            authority_rank_s = str(row.extra.get("authority_rank") or "").strip()
            authority_rank = int(authority_rank_s) if authority_rank_s.isdigit() else _infer_authority_rank(doc_type)

            pdf_path = row.pdf_path
            if not pdf_path.exists():
                skipped_missing += 1
                if verbose:
                    print(f"[pdf] Missing: {pdf_path}")
                continue

            pdf_count += 1
            wrote_any = False
            page_iter = _iter_clean_blocks(pdf_path, max_pages=max_pages, verbose=verbose)
            for chunk_idx, (p_start, p_end, text) in enumerate(
                _chunk_pages(page_iter, chunk_chars=chunk_chars)
            ):
                wrote_any = True
                chunk_count += 1
                record = {
                    "source": "pdf_inbox",
                    "batch_id": row.batch_id,
                    "source_file": str(pdf_path),
                    "raw_index": int(chunk_idx),
                    "record_id": f"pdf_inbox:{row.batch_id}:{pdf_path.name}:{chunk_idx}",
                    "title": title,
                    "date": year,
                    "url": row.url,
                    "text": text,
                    # Extra metadata for ML / reranking / filtering.
                    "jurisdiction": jurisdiction,
                    "doc_type": doc_type,
                    "authority_rank": int(authority_rank),
                    "status": status,
                    "superseded_by": superseded_by,
                    "stream": stream,
                    "keywords": keywords,
                    "priority": row.priority,
                    "notes": row.notes,
                    "page_start": int(p_start + 1),
                    "page_end": int(p_end + 1),
                }
                out.write(json.dumps(record, ensure_ascii=False) + "\n")

            if not wrote_any:
                skipped_empty += 1
                if verbose:
                    print(f"[pdf] No text extracted: {pdf_path}")

    return {
        "inbox_dir": str(inbox_dir),
        "out_path": str(out_path),
        "pdf_count": int(pdf_count),
        "chunk_count": int(chunk_count),
        "skipped_missing": int(skipped_missing),
        "skipped_empty": int(skipped_empty),
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Normalize UCFC inbox (manifest CSV + PDF/DOCX/CSV/XLSX/TXT -> JSONL chunks)."
    )
    parser.add_argument("--inbox", default=str(DEFAULT_INBOX_DIR), help="Inbox dir containing batch_*/ manifests + PDFs")
    parser.add_argument(
        "--out",
        default="",
        help="Output JSONL path (default: latest processed normalized/pdf_inbox.jsonl)",
    )
    parser.add_argument("--processed", default=str(DEFAULT_PROCESSED_DIR), help="Processed directory (for default --out)")
    parser.add_argument("--chunk-chars", type=int, default=6000, help="Max characters per chunk (0 = no chunking)")
    parser.add_argument("--max-pages", type=int, default=0, help="Max pages per PDF (0 = all pages)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    inbox_dir = Path(args.inbox)
    processed_dir = Path(args.processed)
    out_path = Path(args.out) if args.out else (_default_out_path(processed_dir) or Path(""))
    if not out_path:
        raise SystemExit("No processed versions found. Provide --out or run ingest-v1 first.")

    report = normalize_pdf_inbox(
        inbox_dir=inbox_dir,
        out_path=out_path,
        chunk_chars=max(0, args.chunk_chars),
        max_pages=max(0, args.max_pages),
        verbose=bool(args.verbose),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
