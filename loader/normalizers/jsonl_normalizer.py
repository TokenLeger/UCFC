from __future__ import annotations

import json
import re
import tarfile
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator, Optional, Tuple
import xml.etree.ElementTree as ET

TEXT_KEYS = {
    "texte",
    "text",
    "contenu",
    "content",
    "body",
    "resume",
    "summary",
    "abstract",
    "expose",
}
TITLE_KEYS = {"titre", "title", "libelle", "nom", "objet", "reference"}
ID_KEYS = {"id", "ideli", "idelioralias", "cid", "idtexte", "nor", "num", "identifier"}
URL_KEYS = {"url", "lien", "link", "permalink", "uri"}
DATE_KEYS = {
    "date",
    "datedebut",
    "datefin",
    "datepublication",
    "datemaj",
    "datemiseajour",
}

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_LEGI_ID_TAGS = {"ID", "CID", "NOR", "IDELI", "ID_ELI", "IDTEXTE", "ID_ARTICLE"}
_LEGI_TITLE_TAGS = {"TITRE", "TITREFULL", "TITRE_TA", "TITRE_TXT", "TITLE"}
_LEGI_DATE_TAGS = {"DATE", "DATE_TEXTE", "DATE_PUBLICATION", "DATE_SIGNATURE", "DATE_DEBUT", "DATE_FIN"}


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._chunks: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self._skip_depth += 1

    def handle_endtag(self, tag):
        if tag in {"script", "style"} and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data):
        if self._skip_depth == 0 and data:
            self._chunks.append(data)

    def text(self) -> str:
        return " ".join(self._chunks)


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _first_xml_text(root: ET.Element, tags: set[str]) -> Optional[str]:
    tags_upper = {t.upper() for t in tags}
    for elem in root.iter():
        if _strip_ns(elem.tag).upper() in tags_upper:
            if elem.text and elem.text.strip():
                return _clean_text(elem.text)
    return None


@dataclass
class NormalizeStats:
    source: str
    input_files: int
    records_out: int
    skipped_files: int


def _clean_text(text: str) -> str:
    return " ".join(text.split())


def _lower(s: str) -> str:
    return s.lower().replace("-", "").replace("_", "")


def _collect_values(obj, keys: set[str], max_depth: int = 6) -> list[str]:
    out: list[str] = []
    stack: list[Tuple[object, int]] = [(obj, 0)]
    keys_l = {_lower(k) for k in keys}

    while stack:
        node, depth = stack.pop()
        if depth > max_depth:
            continue

        if isinstance(node, dict):
            for k, v in node.items():
                k_norm = _lower(str(k))
                if k_norm in keys_l and isinstance(v, (str, int, float)):
                    out.append(str(v))
                if isinstance(v, (dict, list)):
                    stack.append((v, depth + 1))
        elif isinstance(node, list):
            for v in node:
                if isinstance(v, (dict, list)):
                    stack.append((v, depth + 1))
                elif isinstance(v, (str, int, float)) and "_list_value" in keys_l:
                    out.append(str(v))

    return out


def _first_value(obj, keys: set[str]) -> Optional[str]:
    values = _collect_values(obj, keys)
    for v in values:
        v = _clean_text(v)
        if v:
            return v
    return None


def _join_values(obj, keys: set[str]) -> Optional[str]:
    values = [_clean_text(v) for v in _collect_values(obj, keys)]
    values = [v for v in values if v]
    if not values:
        return None
    # Remove duplicates while preserving order
    seen = set()
    deduped = []
    for v in values:
        if v in seen:
            continue
        seen.add(v)
        deduped.append(v)
    return "\n".join(deduped)


def _record_id(obj, source: str, source_file: Path, raw_index: int) -> str:
    value = _first_value(obj, ID_KEYS)
    if value:
        return f"{source}:{value}"
    return f"{source}:{source_file.name}:{raw_index}"


def normalize_record(
    obj: dict,
    source: str,
    source_file: Path,
    raw_index: int,
) -> dict:
    return {
        "source": source,
        "source_file": str(source_file),
        "raw_index": raw_index,
        "record_id": _record_id(obj, source, source_file, raw_index),
        "title": _first_value(obj, TITLE_KEYS),
        "url": _first_value(obj, URL_KEYS),
        "date": _first_value(obj, DATE_KEYS),
        "text": _join_values(obj, TEXT_KEYS),
    }


def _record_iterator_for_path(source: str, path: Path) -> Iterator[Tuple[Path, dict, int]]:
    suffix = path.suffix.lower()
    is_tar = suffix == ".tgz" or path.name.lower().endswith(".tar.gz") or suffix == ".tar"
    is_zip = suffix == ".zip"
    is_xml = suffix == ".xml"

    if source == "legi":
        if is_tar:
            return _iter_legi_records_from_tar(path)
        if is_zip:
            return _iter_legi_records_from_zip(path)
        if is_xml:
            return _iter_legi_records_from_xml_file(path)
        return iter(())

    if is_tar:
        return _iter_bofip_records_from_tgz(path)
    if is_zip:
        return _iter_json_records_from_zip(path)
    return _iter_json_records_from_file(path)


def _normalize_file_to_tmp(source: str, path: Path, tmp_path: Path) -> Tuple[int, Optional[str]]:
    records_out = 0
    try:
        with tmp_path.open("w", encoding="utf-8") as out:
            for src_file, obj, raw_index in _record_iterator_for_path(source, path):
                record = normalize_record(obj, source, src_file, raw_index)
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                records_out += 1
        return records_out, None
    except Exception as exc:
        return 0, str(exc)


def _iter_records_from_json(data) -> Iterator[dict]:
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item
    elif isinstance(data, dict):
        # Common containers: records, results, items
        for key in ("records", "results", "items", "data"):
            val = data.get(key)
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        yield item
                return
        yield data


def _read_json_bytes(payload: bytes) -> Optional[object]:
    try:
        return json.loads(payload.decode("utf-8"))
    except Exception:
        try:
            return json.loads(payload.decode("utf-8", errors="replace"))
        except Exception:
            return None


def _iter_json_records_from_file(path: Path) -> Iterator[Tuple[Path, dict, int]]:
    if path.suffix.lower() == ".jsonl":
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for idx, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    yield path, obj, idx
        return

    with path.open("rb") as f:
        payload = f.read()
    data = _read_json_bytes(payload)
    if data is None:
        return

    for idx, obj in enumerate(_iter_records_from_json(data)):
        yield path, obj, idx


def _iter_json_records_from_zip(path: Path) -> Iterator[Tuple[Path, dict, int]]:
    with zipfile.ZipFile(path) as zf:
        for name in zf.namelist():
            if not (name.lower().endswith(".json") or name.lower().endswith(".jsonl")):
                continue
            payload = zf.read(name)
            data = _read_json_bytes(payload)
            if data is None:
                continue
            for idx, obj in enumerate(_iter_records_from_json(data)):
                yield path, obj, idx


def normalize_source_dir(
    source: str,
    input_dir: Path,
    output_path: Path,
    verbose: bool = False,
    workers: int = 1,
) -> NormalizeStats:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    records_out = 0
    input_files = 0
    skipped_files = 0

    if verbose:
        print(f"[normalize:{source}] Scanning: {input_dir}")

    # Collect candidate files
    paths: list[Path] = []
    for path in input_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.name.startswith("manifest_"):
            continue

        suffix = path.suffix.lower()
        is_tar = suffix == ".tgz" or path.name.lower().endswith(".tar.gz") or suffix == ".tar"
        is_zip = suffix == ".zip"
        is_xml = suffix == ".xml"

        if source == "legi":
            if not (is_tar or is_zip or is_xml):
                continue
        else:
            if suffix not in {".json", ".jsonl", ".zip"} and not is_tar:
                continue
            if is_tar and source != "bofip":
                continue

        paths.append(path)

    paths = sorted(paths, key=lambda p: str(p))
    input_files = len(paths)

    # Parallel path: one worker per input file -> temp parts -> merge
    if workers and workers > 1 and len(paths) > 1:
        if verbose:
            print(f"[normalize:{source}] Using workers={workers} for {len(paths)} files")
        with TemporaryDirectory(prefix=f"ucfc_norm_{source}_") as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            part_paths: dict[Path, Path] = {}
            with ProcessPoolExecutor(max_workers=workers) as executor:
                future_map = {}
                for path in paths:
                    part_path = tmp_dir / f"{path.name}.part.jsonl"
                    future = executor.submit(_normalize_file_to_tmp, source, path, part_path)
                    future_map[future] = (path, part_path)

                for future in as_completed(future_map):
                    path, part_path = future_map[future]
                    try:
                        count, error = future.result()
                    except Exception as exc:
                        skipped_files += 1
                        if verbose:
                            print(f"[normalize:{source}] SKIP (error): {path} ({exc})")
                        continue
                    if error:
                        skipped_files += 1
                        if verbose:
                            print(f"[normalize:{source}] SKIP (error): {path} ({error})")
                        continue
                    part_paths[path] = part_path
                    records_out += count
                    if verbose:
                        print(f"[normalize:{source}] OK: {path}")

            with output_path.open("w", encoding="utf-8") as out:
                for path in paths:
                    part_path = part_paths.get(path)
                    if not part_path or not part_path.exists():
                        continue
                    with part_path.open("r", encoding="utf-8") as part:
                        for line in part:
                            out.write(line)
        if verbose:
            print(
                f"[normalize:{source}] Done. files={input_files} records={records_out} skipped={skipped_files}"
            )
        return NormalizeStats(
            source=source,
            input_files=input_files,
            records_out=records_out,
            skipped_files=skipped_files,
        )

    # Sequential path
    with output_path.open("w", encoding="utf-8") as out:
        for path in paths:
            try:
                for src_file, obj, raw_index in _record_iterator_for_path(source, path):
                    record = normalize_record(obj, source, src_file, raw_index)
                    out.write(json.dumps(record, ensure_ascii=False) + "\n")
                    records_out += 1
                if verbose:
                    print(f"[normalize:{source}] OK: {path}")
            except Exception as exc:
                skipped_files += 1
                if verbose:
                    print(f"[normalize:{source}] SKIP (error): {path} ({exc})")

    if verbose:
        print(
            f"[normalize:{source}] Done. files={input_files} records={records_out} skipped={skipped_files}"
        )

    return NormalizeStats(
        source=source,
        input_files=input_files,
        records_out=records_out,
        skipped_files=skipped_files,
    )


def _bofip_meta_from_path(name: str) -> dict:
    parts = Path(name).parts
    date = None
    for part in parts:
        if _DATE_RE.fullmatch(part):
            date = part
            break
    date_idx = parts.index(date) if date in parts else -1
    doc_id = parts[date_idx - 1] if date_idx > 0 else None
    domain = parts[date_idx - 2] if date_idx > 1 else None
    section = parts[date_idx - 3] if date_idx > 2 else None
    title_parts = [p for p in (section, domain, doc_id) if p]
    title = " ".join(title_parts) if title_parts else None
    return {
        "bofip_path": name,
        "bofip_section": section,
        "bofip_domain": domain,
        "bofip_doc_id": doc_id,
        "date": date,
        "title": title,
    }


def _extract_text_from_xml(payload: bytes) -> str:
    try:
        root = ET.fromstring(payload)
    except Exception:
        return ""
    text = " ".join(root.itertext())
    return _clean_text(text)


def _extract_text_from_html(payload: bytes) -> str:
    try:
        text = payload.decode("utf-8")
    except Exception:
        text = payload.decode("utf-8", errors="replace")
    parser = _HTMLTextExtractor()
    parser.feed(text)
    return _clean_text(parser.text())


def _parse_legi_xml(payload: bytes) -> Optional[dict]:
    try:
        root = ET.fromstring(payload)
    except Exception:
        return None
    text = _clean_text(" ".join(root.itertext()))
    if not text:
        return None
    return {
        "id": _first_xml_text(root, _LEGI_ID_TAGS),
        "title": _first_xml_text(root, _LEGI_TITLE_TAGS),
        "date": _first_xml_text(root, _LEGI_DATE_TAGS),
        "text": text,
    }


def _iter_legi_records_from_xml_file(path: Path) -> Iterator[Tuple[Path, dict, int]]:
    payload = path.read_bytes()
    record = _parse_legi_xml(payload)
    if record is None:
        return
    yield path, record, 0


def _iter_legi_records_from_zip(path: Path) -> Iterator[Tuple[Path, dict, int]]:
    idx = 0
    with zipfile.ZipFile(path) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".xml"):
                continue
            try:
                payload = zf.read(name)
                record = _parse_legi_xml(payload)
                if record is None:
                    continue
                src = Path(f"{path}::{name}")
                yield src, record, idx
                idx += 1
            except Exception:
                continue


def _iter_legi_records_from_tar(path: Path) -> Iterator[Tuple[Path, dict, int]]:
    idx = 0
    with tarfile.open(path, "r:*") as tf:
        for member in tf.getmembers():
            if not member.isfile():
                continue
            name = member.name
            if not name.lower().endswith(".xml"):
                continue
            try:
                f = tf.extractfile(member)
                if f is None:
                    continue
                payload = f.read()
                record = _parse_legi_xml(payload)
                if record is None:
                    continue
                src = Path(f"{path}::{name}")
                yield src, record, idx
                idx += 1
            except Exception:
                continue


def _iter_bofip_records_from_tgz(path: Path) -> Iterator[Tuple[Path, dict, int]]:
    idx = 0
    with tarfile.open(path, "r:*") as tf:
        for member in tf.getmembers():
            if not member.isfile():
                continue
            name = member.name
            lower = name.lower()
            if not (lower.endswith(".xml") or lower.endswith(".html") or lower.endswith(".htm")):
                continue
            f = tf.extractfile(member)
            if f is None:
                continue
            payload = f.read()
            if lower.endswith(".xml"):
                text = _extract_text_from_xml(payload)
            else:
                text = _extract_text_from_html(payload)
            if not text:
                continue
            meta = _bofip_meta_from_path(name)
            record = {
                "id": meta.get("bofip_doc_id") or f"{path.name}:{idx}",
                "title": meta.get("title"),
                "date": meta.get("date"),
                "text": text,
                "bofip_path": meta.get("bofip_path"),
                "bofip_section": meta.get("bofip_section"),
                "bofip_domain": meta.get("bofip_domain"),
                "bofip_doc_id": meta.get("bofip_doc_id"),
            }
            yield path, record, idx
            idx += 1
