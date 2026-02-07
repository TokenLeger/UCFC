from __future__ import annotations

import argparse
import heapq
import json
import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from loader import usage_log

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROCESSED_DIR = REPO_ROOT / "data_fiscale" / "processed"
TOKEN_RE = re.compile(r"[a-zA-Z0-9]{2,}")


@dataclass
class ScoredHit:
    score: float
    source: str
    record_id: str
    title: str
    date: str
    url: str
    source_file: str
    raw_index: int
    snippet: str


def _norm_chars(text: str) -> tuple[str, list[int]]:
    norm_chars: list[str] = []
    index_map: list[int] = []
    for idx, ch in enumerate(text):
        decomp = unicodedata.normalize("NFKD", ch)
        for part in decomp:
            if unicodedata.combining(part):
                continue
            norm_chars.append(part.casefold())
            index_map.append(idx)
    return "".join(norm_chars), index_map


def _normalize(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c)
    ).casefold()


def _extract_query(query: str) -> tuple[list[str], list[str]]:
    phrases = re.findall(r'"([^"]+)"', query)
    rest = re.sub(r'"[^"]+"', " ", query)
    tokens = [t for t in TOKEN_RE.findall(_normalize(rest)) if t]
    phrases = [p for p in (p.strip() for p in phrases) if p]
    return tokens, phrases


def _find_latest_processed_dir(processed_dir: Path) -> Optional[Path]:
    if not processed_dir.exists():
        return None
    candidates = [p for p in processed_dir.iterdir() if p.is_dir()]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _iter_jsonl(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def _load_inputs(input_path: Optional[Path], processed_dir: Path, sources: list[str]) -> list[Path]:
    if input_path:
        if input_path.is_dir():
            return sorted(input_path.glob("*.jsonl"))
        if input_path.exists():
            return [input_path]
        raise SystemExit(f"Input path not found: {input_path}")

    latest = _find_latest_processed_dir(processed_dir)
    if not latest:
        raise SystemExit("No processed versions found. Run ingest-v1 first.")
    normalized_dir = latest / "normalized"
    if not normalized_dir.exists():
        raise SystemExit(f"Missing normalized dir: {normalized_dir}")

    files = sorted(normalized_dir.glob("*.jsonl"))
    if sources:
        allow = {s.strip().lower() for s in sources if s.strip()}
        files = [p for p in files if p.stem.lower() in allow]
    return files


def _build_snippet(text: str, tokens: list[str], phrases: list[str], max_chars: int) -> str:
    if not text:
        return ""
    if max_chars <= 0:
        return ""

    norm_text, index_map = _norm_chars(text)
    targets = [
        _normalize(p) for p in phrases
    ] + tokens
    targets = [t for t in targets if t]

    hit_pos = None
    hit_len = 0
    for t in targets:
        pos = norm_text.find(t)
        if pos != -1 and (hit_pos is None or pos < hit_pos):
            hit_pos = pos
            hit_len = len(t)

    if hit_pos is None:
        return text[:max_chars].strip()

    orig_pos = index_map[hit_pos] if hit_pos < len(index_map) else 0
    start = max(0, orig_pos - max_chars // 2)
    end = min(len(text), start + max_chars)
    snippet = text[start:end].strip()
    return snippet


def _score_record(
    record: dict,
    tokens: list[str],
    phrases: list[str],
    match: str,
    scan_chars: int,
    snippet_chars: int,
) -> tuple[float, str]:
    title = record.get("title") or ""
    text = record.get("text") or ""
    combined = f"{title}\n{text}" if text else title
    if scan_chars > 0:
        combined = combined[:scan_chars]

    norm_combined = _normalize(combined)
    norm_title = _normalize(title)

    token_hits = 0
    score = 0.0
    for token in tokens:
        if not token:
            continue
        count = norm_combined.count(token)
        if count:
            token_hits += 1
            score += float(count)
        if token in norm_title:
            score += 2.0

    phrase_hits = 0
    for phrase in phrases:
        norm_phrase = _normalize(phrase)
        if not norm_phrase:
            continue
        if norm_phrase in norm_combined:
            phrase_hits += 1
            score += 5.0
            if norm_phrase in norm_title:
                score += 5.0

    if match == "all":
        if tokens and token_hits < len(tokens):
            return 0.0, ""
        if phrases and phrase_hits < len(phrases):
            return 0.0, ""

    if score <= 0:
        return 0.0, ""

    snippet = _build_snippet(combined, tokens, phrases, snippet_chars)
    return score, snippet


def search(
    query: str,
    input_path: Optional[Path],
    processed_dir: Path,
    sources: list[str],
    limit: int,
    match: str,
    scan_chars: int,
    snippet_chars: int,
) -> list[ScoredHit]:
    tokens, phrases = _extract_query(query)
    if not tokens and not phrases:
        return []

    files = _load_inputs(input_path, processed_dir, sources)
    if not files:
        return []

    heap: list[tuple[float, int, ScoredHit]] = []
    seq = 0

    for path in files:
        for record in _iter_jsonl(path):
            score, snippet = _score_record(record, tokens, phrases, match, scan_chars, snippet_chars)
            if score <= 0:
                continue
            hit = ScoredHit(
                score=score,
                source=str(record.get("source") or path.stem),
                record_id=str(record.get("record_id") or ""),
                title=str(record.get("title") or ""),
                date=str(record.get("date") or ""),
                url=str(record.get("url") or ""),
                source_file=str(record.get("source_file") or str(path)),
                raw_index=int(record.get("raw_index") or 0),
                snippet=snippet,
            )
            if len(heap) < limit:
                heapq.heappush(heap, (score, seq, hit))
            else:
                if score > heap[0][0]:
                    heapq.heapreplace(heap, (score, seq, hit))
            seq += 1

    results = [item[2] for item in sorted(heap, key=lambda x: (-x[0], x[1]))]
    return results


def _default_user() -> str:
    return os.getenv("UCFC_USER") or os.getenv("USER") or "local"


def _default_ip() -> str:
    return os.getenv("UCFC_IP") or "local"


def _log_usage(query: str, sources: list[str], agent: str, action: str) -> None:
    event = usage_log.AccessEvent(
        user_name=_default_user(),
        client_ip=_default_ip(),
        agent_name=agent,
        action=action,
        resource=",".join(sources) if sources else None,
        query_text=query,
    )
    usage_log.log_access(event)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Recherche extractive sur JSONL normalises.")
    parser.add_argument("--query", required=True, help="Texte de recherche (guillemets pour phrase)")
    parser.add_argument("--in", dest="input_path", default="", help="Fichier JSONL ou dossier normalized")
    parser.add_argument("--processed", default=str(DEFAULT_PROCESSED_DIR))
    parser.add_argument("--source", action="append", default=[], help="Filtrer par source (bofip, legi, etc.)")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--match", choices=["any", "all"], default="any")
    parser.add_argument("--scan-chars", type=int, default=20000)
    parser.add_argument("--snippet-chars", type=int, default=240)
    parser.add_argument("--no-snippet", action="store_true")
    parser.add_argument("--json", action="store_true", help="Sortie JSONL")
    parser.add_argument("--agent", default="ucfc_cli")
    parser.add_argument("--action", default="qa_search")
    parser.add_argument("--no-log", action="store_true")
    args = parser.parse_args(argv)

    input_path = Path(args.input_path) if args.input_path else None
    processed_dir = Path(args.processed)
    snippet_chars = 0 if args.no_snippet else args.snippet_chars

    if not args.no_log:
        _log_usage(args.query, args.source, args.agent, args.action)

    results = search(
        query=args.query,
        input_path=input_path,
        processed_dir=processed_dir,
        sources=args.source,
        limit=max(1, args.limit),
        match=args.match,
        scan_chars=args.scan_chars,
        snippet_chars=snippet_chars,
    )

    if not results:
        print("Je ne sais pas.")
        return 0

    if args.json:
        for hit in results:
            print(json.dumps(hit.__dict__, ensure_ascii=False))
        return 0

    for idx, hit in enumerate(results, start=1):
        print(f"[{idx}] score={hit.score:.2f} source={hit.source} record_id={hit.record_id}")
        if hit.title:
            print(f"     title: {hit.title}")
        if hit.date:
            print(f"     date: {hit.date}")
        if hit.url:
            print(f"     url: {hit.url}")
        if hit.source_file:
            print(f"     file: {hit.source_file} (raw_index={hit.raw_index})")
        if hit.snippet:
            print(f"     snippet: {hit.snippet}")
        print("")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
