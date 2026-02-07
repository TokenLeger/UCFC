from __future__ import annotations

import argparse
import json
import unicodedata
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PROCESSED_DIR = REPO_ROOT / "data_fiscale" / "processed"

# Known CGI text ID (LEGI)
CGI_TEXT_IDS = {"LEGITEXT000006069577"}


def _norm(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text.lower()


def _find_latest_legi_jsonl(processed_dir: Path) -> Optional[Path]:
    candidates: list[Path] = []
    if not processed_dir.exists():
        return None
    for p in processed_dir.iterdir():
        if not p.is_dir():
            continue
        cand = p / "normalized" / "legi.jsonl"
        if cand.exists():
            candidates.append(cand)
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _is_cgi(record: dict, extra_ids: set[str]) -> bool:
    source_file = record.get("source_file") or ""
    record_id = record.get("record_id") or ""
    title = record.get("title") or ""

    for text_id in CGI_TEXT_IDS | extra_ids:
        if text_id and (text_id in source_file or text_id in record_id):
            return True

    title_norm = _norm(title)
    if "code general des impots" in title_norm:
        return True
    return False


def extract_cgi(input_path: Path, output_path: Path, extra_ids: set[str], limit: int = 0, verbose: bool = False) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    in_count = 0
    out_count = 0
    with input_path.open("r", encoding="utf-8") as src, output_path.open("w", encoding="utf-8") as out:
        for line in src:
            if not line.strip():
                continue
            in_count += 1
            try:
                record = json.loads(line)
            except Exception:
                continue
            if _is_cgi(record, extra_ids):
                out.write(line if line.endswith("\n") else line + "\n")
                out_count += 1
                if limit and out_count >= limit:
                    break
    if verbose:
        print(f"[cgi] input={in_count} output={out_count} out_path={output_path}")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Extract CGI records from LEGI normalized JSONL.")
    parser.add_argument("--in", dest="input_path", default="", help="Path to legi.jsonl (defaults to latest)")
    parser.add_argument("--out", default="", help="Output JSONL path (default: legi_cgi.jsonl alongside input)")
    parser.add_argument("--text-id", action="append", default=[], help="Additional LEGITEXT id to include")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of output records")
    parser.add_argument("--verbose", action="store_true", help="Verbose logs")
    args = parser.parse_args(argv)

    input_path = Path(args.input_path) if args.input_path else _find_latest_legi_jsonl(DEFAULT_PROCESSED_DIR)
    if not input_path or not input_path.exists():
        raise SystemExit("legi.jsonl not found. Provide --in or run ingest-v1 first.")

    output_path = Path(args.out) if args.out else input_path.with_name("legi_cgi.jsonl")
    extra_ids = {i.strip() for i in args.text_id if i.strip()}
    extract_cgi(input_path, output_path, extra_ids, limit=args.limit, verbose=args.verbose)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
