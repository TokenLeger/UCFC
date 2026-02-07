from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Iterable, Optional


API_BASE = "https://opendata.justice-administrative.fr/recherche/api"
SHARE_BASE = "https://opendata.justice-administrative.fr/recherche/shareFile"
DEFAULT_JURISDICTION = "CE"
DEFAULT_SOURCE = "Open Data Justice administrative"
MAX_LIMIT = 200
_DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; UCFC Open Data client)"


def _fetch_json(url: str, timeout: int = 60) -> dict:
    req = urllib.request.Request(url)
    req.add_header("User-Agent", _DEFAULT_USER_AGENT)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = resp.read().decode("utf-8")
    return json.loads(payload)


def _build_search_url(query: str, jurisdiction: Optional[str], limit: int) -> str:
    limit = max(1, min(int(limit), MAX_LIMIT))
    q = urllib.parse.quote(query.strip())
    if jurisdiction:
        j = urllib.parse.quote(jurisdiction.strip())
        return f"{API_BASE}/model_search_juri/openData/{j}/{q}/{limit}"
    return f"{API_BASE}/Simple_Search/openData/{q}/{limit}"


def _iter_sources(payload: dict) -> Iterable[dict]:
    node: object = payload
    if isinstance(node, dict) and "decisions" in node:
        node = node["decisions"]
    if isinstance(node, dict) and "body" in node:
        node = node["body"]
    if isinstance(node, dict) and "hits" in node:
        node = node["hits"]
    if isinstance(node, dict) and "hits" in node:
        node = node["hits"]
    if not isinstance(node, list):
        return []
    for hit in node:
        if not isinstance(hit, dict):
            continue
        source = hit.get("_source")
        if isinstance(source, dict):
            yield source
        else:
            yield hit


def _share_url(source: dict) -> Optional[str]:
    ident = str(source.get("Identification") or "").strip()
    code = str(source.get("Code_Juridiction") or "").strip()
    if not ident or not code:
        return None
    ident = ident.split(".", 1)[0]
    return f"{SHARE_BASE}/{code}/{ident}"


def _title(source: dict) -> str:
    t = str(source.get("Type_Decision") or "").strip()
    jur = str(source.get("Nom_Juridiction") or "").strip()
    num = str(source.get("Numero_Dossier") or "").strip()
    parts = [p for p in [t, jur, num] if p]
    if parts:
        return " ".join(parts)
    return str(source.get("Identification") or "").strip()


def _year(source: dict) -> str:
    date = str(source.get("Date_Lecture") or "").strip()
    if len(date) >= 4:
        return date[:4]
    return ""


def _write_urls(urls: list[str], out_path: Optional[Path]) -> None:
    payload = "\n".join(urls) + ("\n" if urls else "")
    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload, encoding="utf-8")
        return
    print(payload, end="")


def _write_csv(rows: list[dict], out_path: Optional[Path]) -> None:
    fieldnames = [
        "url",
        "title",
        "year",
        "source",
        "doc_type",
        "jurisdiction",
        "court",
        "case_number",
        "date_label",
    ]
    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        handle = out_path.open("w", encoding="utf-8", newline="")
    else:
        handle = sys.stdout

    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k, "") for k in fieldnames})

    if out_path:
        handle.close()


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch Open Data Justice administrative decisions and output shareFile URLs."
    )
    parser.add_argument("--query", required=True, help="Keyword query (ex: TVA)")
    parser.add_argument("--jurisdiction", default=DEFAULT_JURISDICTION, help="Juridiction code (ex: CE)")
    parser.add_argument("--limit", type=int, default=100, help="Max results (1..200)")
    parser.add_argument("--format", choices=["urls", "csv"], default="urls")
    parser.add_argument("--out", default="", help="Output file (optional, default stdout)")
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--doc-type", default="jurisprudence")
    parser.add_argument("--jurisdiction-label", default="FR")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    url = _build_search_url(args.query, args.jurisdiction, args.limit)
    if args.verbose:
        print(f"[opendata] GET {url}")
    payload = _fetch_json(url)
    sources = list(_iter_sources(payload))
    urls: list[str] = []
    rows: list[dict] = []
    for source in sources:
        share = _share_url(source)
        if not share:
            continue
        urls.append(share)
        rows.append(
            {
                "url": share,
                "title": _title(source),
                "year": _year(source),
                "source": args.source,
                "doc_type": args.doc_type,
                "jurisdiction": args.jurisdiction_label,
                "court": str(source.get("Nom_Juridiction") or "").strip(),
                "case_number": str(source.get("Numero_Dossier") or "").strip(),
                "date_label": str(source.get("Date_Lecture") or "").strip(),
            }
        )

    out_path = Path(args.out).expanduser() if args.out else None
    if args.format == "csv":
        _write_csv(rows, out_path)
    else:
        _write_urls(urls, out_path)

    if args.verbose:
        print(f"[opendata] results={len(urls)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
