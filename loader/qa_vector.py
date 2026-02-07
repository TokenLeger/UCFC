from __future__ import annotations

import argparse
import json
import os
import sqlite3
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import numpy as np

from loader import usage_log

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROCESSED_DIR = REPO_ROOT / "data_fiscale" / "processed"
DEFAULT_INDEX_DIR = REPO_ROOT / "data_fiscale" / "index" / "vector"


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


def _count_records(paths: list[Path]) -> int:
    total = 0
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    total += 1
    return total


def _build_text(record: dict, max_chars: int) -> str:
    title = record.get("title") or ""
    text = record.get("text") or ""
    combined = f"{title}\n{text}" if text else title
    if max_chars > 0:
        combined = combined[:max_chars]
    return combined


def _snippet(text: str, max_chars: int) -> str:
    if not text:
        return ""
    if max_chars <= 0:
        return ""
    return text[:max_chars].strip()


def _ensure_meta_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
              id INTEGER PRIMARY KEY,
              source TEXT,
              record_id TEXT,
              title TEXT,
              date TEXT,
              url TEXT,
              source_file TEXT,
              raw_index INTEGER,
              text_excerpt TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_meta_source ON meta(source)")
        conn.commit()


def _insert_meta(conn: sqlite3.Connection, rows: list[tuple]) -> None:
    conn.executemany(
        """
        INSERT INTO meta (
          id, source, record_id, title, date, url, source_file, raw_index, text_excerpt
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _load_model(model_name: str):
    try:
        from sentence_transformers import SentenceTransformer
    except Exception as exc:
        raise SystemExit(
            "Missing dependencies. Install with: pip install -r requirements-ml.txt"
        ) from exc
    return SentenceTransformer(model_name)


def build_index(
    input_path: Optional[Path],
    processed_dir: Path,
    sources: list[str],
    out_dir: Path,
    model_name: str,
    batch_size: int,
    max_chars: int,
    log_every: int,
    overwrite: bool,
    verbose: bool,
) -> Path:
    paths = _load_inputs(input_path, processed_dir, sources)
    if not paths:
        raise SystemExit("No input files found.")

    total = _count_records(paths)
    if total == 0:
        raise SystemExit("No records to index.")

    if verbose:
        print(f"[vec] Loading model: {model_name}")
    model = _load_model(model_name)
    dim = model.get_sentence_embedding_dimension()
    if verbose:
        print(f"[vec] Records: {total} dim={dim} batch_size={batch_size} max_chars={max_chars}")

    if out_dir.exists():
        emb_path = out_dir / "embeddings.npy"
        meta_path = out_dir / "meta.sqlite3"
        if (emb_path.exists() or meta_path.exists()) and not overwrite:
            raise SystemExit(
                "Index already exists. Use --overwrite or choose a new --out directory."
            )
        if overwrite:
            shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    emb_path = out_dir / "embeddings.npy"
    meta_path = out_dir / "meta.sqlite3"
    config_path = out_dir / "index.json"

    emb = np.lib.format.open_memmap(emb_path, mode="w+", dtype="float32", shape=(total, dim))
    _ensure_meta_db(meta_path)

    index_id = 0
    last_log = 0
    with sqlite3.connect(meta_path) as conn:
        for path in paths:
            if verbose:
                print(f"[vec] Indexing: {path}")
            file_count = 0
            batch_records: list[dict] = []
            batch_texts: list[str] = []
            for record in _iter_jsonl(path):
                file_count += 1
                batch_records.append(record)
                batch_texts.append(_build_text(record, max_chars))
                if len(batch_texts) >= batch_size:
                    vectors = model.encode(
                        batch_texts,
                        batch_size=batch_size,
                        normalize_embeddings=True,
                        show_progress_bar=False,
                    )
                    emb[index_id : index_id + len(vectors)] = vectors
                    rows = []
                    for offset, rec in enumerate(batch_records):
                        rec_id = index_id + offset + 1
                        rows.append(
                            (
                                rec_id,
                                str(rec.get("source") or path.stem),
                                str(rec.get("record_id") or ""),
                                str(rec.get("title") or ""),
                                str(rec.get("date") or ""),
                                str(rec.get("url") or ""),
                                str(rec.get("source_file") or str(path)),
                                int(rec.get("raw_index") or 0),
                                _snippet(_build_text(rec, max_chars), 500),
                            )
                        )
                    _insert_meta(conn, rows)
                    conn.commit()
                    index_id += len(vectors)
                    if verbose and log_every > 0:
                        if (index_id - last_log) >= log_every or index_id == total:
                            print(f"[vec] Progress: {index_id}/{total}")
                            last_log = index_id
                    batch_records = []
                    batch_texts = []

            if batch_texts:
                vectors = model.encode(
                    batch_texts,
                    batch_size=batch_size,
                    normalize_embeddings=True,
                    show_progress_bar=False,
                )
                emb[index_id : index_id + len(vectors)] = vectors
                rows = []
                for offset, rec in enumerate(batch_records):
                    rec_id = index_id + offset + 1
                    rows.append(
                        (
                            rec_id,
                            str(rec.get("source") or path.stem),
                            str(rec.get("record_id") or ""),
                            str(rec.get("title") or ""),
                            str(rec.get("date") or ""),
                            str(rec.get("url") or ""),
                            str(rec.get("source_file") or str(path)),
                            int(rec.get("raw_index") or 0),
                            _snippet(_build_text(rec, max_chars), 500),
                        )
                    )
                _insert_meta(conn, rows)
                conn.commit()
                index_id += len(vectors)
                if verbose and log_every > 0:
                    if (index_id - last_log) >= log_every or index_id == total:
                        print(f"[vec] Progress: {index_id}/{total}")
                        last_log = index_id
            if verbose:
                print(f"[vec] File done. records={file_count}")

    meta = {
        "created_at": datetime.now().isoformat(),
        "model": model_name,
        "total_records": int(total),
        "vector_dim": int(dim),
        "sources": sources,
    }
    config_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    if verbose:
        print(f"[vec] Done. records={index_id} index_dir={out_dir}")

    return out_dir


def _load_index(out_dir: Path):
    config_path = out_dir / "index.json"
    emb_path = out_dir / "embeddings.npy"
    meta_path = out_dir / "meta.sqlite3"
    if not config_path.exists() or not emb_path.exists() or not meta_path.exists():
        raise SystemExit("Index missing. Run qa-index first.")

    config = json.loads(config_path.read_text(encoding="utf-8"))
    emb = np.lib.format.open_memmap(emb_path, mode="r", dtype="float32")
    dim = int(config.get("vector_dim") or 0)
    total = int(config.get("total_records") or 0)
    if dim <= 0 or total <= 0:
        raise SystemExit("Invalid index metadata.")
    emb = emb.reshape((total, dim))
    return config, emb, meta_path


def search(
    query: str,
    out_dir: Path,
    limit: int,
    chunk_size: int,
    snippet_chars: int,
    sources: Optional[list[str]] = None,
    oversample: int = 20,
) -> list[ScoredHit]:
    config, emb, meta_path = _load_index(out_dir)
    model = _load_model(config.get("model") or "sentence-transformers/all-MiniLM-L6-v2")
    qvec = model.encode([query], normalize_embeddings=True)[0]

    top_scores: list[tuple[float, int]] = []
    total = emb.shape[0]
    source_set = {s.strip().lower() for s in (sources or []) if s.strip()}
    candidate_limit = limit
    if source_set:
        candidate_limit = min(total, max(limit, limit * max(1, oversample)))

    for start in range(0, total, chunk_size):
        end = min(total, start + chunk_size)
        scores = emb[start:end] @ qvec
        if scores.size == 0:
            continue
        if len(top_scores) < candidate_limit:
            for i, score in enumerate(scores):
                top_scores.append((float(score), start + i))
            top_scores.sort(key=lambda x: x[0], reverse=True)
            top_scores = top_scores[:candidate_limit]
        else:
            for i, score in enumerate(scores):
                score = float(score)
                if score <= top_scores[-1][0]:
                    continue
                top_scores.append((score, start + i))
                top_scores.sort(key=lambda x: x[0], reverse=True)
                top_scores = top_scores[:candidate_limit]

    if not top_scores:
        return []

    ids = [idx + 1 for _, idx in top_scores]
    hits: list[ScoredHit] = []
    with sqlite3.connect(meta_path) as conn:
        cursor = conn.execute(
            "SELECT id, source, record_id, title, date, url, source_file, raw_index, text_excerpt FROM meta WHERE id IN (%s)"
            % ",".join(["?"] * len(ids)),
            ids,
        )
        rows = cursor.fetchall()
        by_id = {row[0]: row for row in rows}
    for score, idx in top_scores:
        row = by_id.get(idx + 1)
        if not row:
            continue
        _, source, record_id, title, date, url, source_file, raw_index, text_excerpt = row
        if source_set and (source or "").lower() not in source_set:
            continue
        snippet = _snippet(text_excerpt or "", snippet_chars)
        hits.append(
            ScoredHit(
                score=score,
                source=source or "",
                record_id=record_id or "",
                title=title or "",
                date=date or "",
                url=url or "",
                source_file=source_file or "",
                raw_index=int(raw_index or 0),
                snippet=snippet,
            )
        )
        if len(hits) >= limit:
            break

    return hits


def main_index(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build vector index from normalized JSONL.")
    parser.add_argument("--out", default=str(DEFAULT_INDEX_DIR))
    parser.add_argument("--processed", default=str(DEFAULT_PROCESSED_DIR))
    parser.add_argument("--in", dest="input_path", default="", help="Fichier JSONL ou dossier normalized")
    parser.add_argument("--source", action="append", default=[], help="Filtrer par source (bofip, legi, etc.)")
    parser.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--max-chars", type=int, default=4000)
    parser.add_argument("--log-every", type=int, default=5000)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    input_path = Path(args.input_path) if args.input_path else None
    out_dir = Path(args.out)
    processed_dir = Path(args.processed)

    build_index(
        input_path=input_path,
        processed_dir=processed_dir,
        sources=args.source,
        out_dir=out_dir,
        model_name=args.model,
        batch_size=max(1, args.batch_size),
        max_chars=max(200, args.max_chars),
        log_every=max(0, args.log_every),
        overwrite=args.overwrite,
        verbose=args.verbose,
    )
    return 0


def main_search(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Vector search over indexed corpus.")
    parser.add_argument("--query", required=True)
    parser.add_argument("--index", default=str(DEFAULT_INDEX_DIR))
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--chunk-size", type=int, default=50000)
    parser.add_argument("--snippet-chars", type=int, default=240)
    parser.add_argument("--source", action="append", default=[])
    parser.add_argument("--oversample", type=int, default=20)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--agent", default="ucfc_cli")
    parser.add_argument("--action", default="qa_vector_search")
    parser.add_argument("--no-log", action="store_true")
    args = parser.parse_args(argv)

    if not args.no_log:
        _log_usage(args.query, [], args.agent, args.action)

    hits = search(
        query=args.query,
        out_dir=Path(args.index),
        limit=max(1, args.limit),
        chunk_size=max(1000, args.chunk_size),
        snippet_chars=max(0, args.snippet_chars),
        sources=args.source,
        oversample=max(1, args.oversample),
    )

    if not hits:
        print("Je ne sais pas.")
        return 0

    if args.json:
        for hit in hits:
            print(json.dumps(hit.__dict__, ensure_ascii=False))
        return 0

    for idx, hit in enumerate(hits, start=1):
        print(f"[{idx}] score={hit.score:.4f} source={hit.source} record_id={hit.record_id}")
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
    raise SystemExit(main_search())
