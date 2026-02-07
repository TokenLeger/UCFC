from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from loader import qa_extractive, usage_log

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROCESSED_DIR = REPO_ROOT / "data_fiscale" / "processed"
DEFAULT_OUT_DIR = REPO_ROOT / "fiches"

DEFAULT_THEMES = [
    {
        "slug": "impot-sur-le-revenu",
        "title": "Impot sur le revenu",
        "queries": ["impot sur le revenu", "revenu imposable"],
    },
    {
        "slug": "impot-sur-les-societes",
        "title": "Impot sur les societes",
        "queries": ["impot sur les societes"],
    },
    {
        "slug": "tva",
        "title": "TVA",
        "queries": ["taxe sur la valeur ajoutee", "TVA"],
    },
    {
        "slug": "plus-values",
        "title": "Plus-values",
        "queries": ["plus-value", "plus-values"],
    },
    {
        "slug": "revenus-fonciers",
        "title": "Revenus fonciers",
        "queries": ["revenus fonciers"],
    },
    {
        "slug": "micro-entreprise",
        "title": "Micro-entreprise",
        "queries": ["micro-entreprise", "micro entreprise"],
    },
    {
        "slug": "prelevement-a-la-source",
        "title": "Prelevement a la source",
        "queries": ["prelevement a la source"],
    },
    {
        "slug": "ifi",
        "title": "IFI",
        "queries": ["IFI", "impot sur la fortune immobiliere"],
    },
]

SLUG_RE = re.compile(r"[^a-z0-9]+")


@dataclass
class Theme:
    slug: str
    title: str
    queries: list[str]


@dataclass
class Section:
    name: str
    sources: list[str]
    hits: list[qa_extractive.ScoredHit]
    texts: dict[str, str]


def _slugify(text: str) -> str:
    text = text.lower().strip()
    text = SLUG_RE.sub("-", text).strip("-")
    return text or "fiche"


def _load_themes(path: Optional[Path], inline: list[str]) -> list[Theme]:
    themes: list[Theme] = []
    if path:
        payload = json.loads(path.read_text(encoding="utf-8"))
        for item in payload:
            slug = item.get("slug") or _slugify(item.get("title") or "fiche")
            title = item.get("title") or slug
            queries = [q for q in item.get("queries", []) if isinstance(q, str) and q.strip()]
            if queries:
                themes.append(Theme(slug=slug, title=title, queries=queries))
    if inline:
        for item in inline:
            slug = _slugify(item)
            themes.append(Theme(slug=slug, title=item.strip(), queries=[item.strip()]))
    if themes:
        return themes
    return [Theme(**item) for item in DEFAULT_THEMES]


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


def _latest_version_id(processed_dir: Path) -> str:
    latest = qa_extractive._find_latest_processed_dir(processed_dir)
    if not latest:
        return "unknown"
    return latest.name


def _merge_hits(hits: Iterable[qa_extractive.ScoredHit]) -> list[qa_extractive.ScoredHit]:
    by_id: dict[str, qa_extractive.ScoredHit] = {}
    for hit in hits:
        key = hit.record_id or hit.source_file or ""
        if not key:
            continue
        prev = by_id.get(key)
        if not prev or hit.score > prev.score:
            by_id[key] = hit
    return sorted(by_id.values(), key=lambda h: (-h.score, h.record_id))


def _resolve_source_paths(
    source: str,
    input_path: Optional[Path],
    processed_dir: Path,
) -> list[Path]:
    if input_path:
        if input_path.is_file():
            return [input_path]
        if input_path.is_dir():
            cand = input_path / f"{source}.jsonl"
            if cand.exists():
                return [cand]
            return sorted(input_path.glob("*.jsonl"))
    latest = qa_extractive._find_latest_processed_dir(processed_dir)
    if not latest:
        return []
    normalized_dir = latest / "normalized"
    cand = normalized_dir / f"{source}.jsonl"
    if cand.exists():
        return [cand]
    return []


def _detect_cgi_source(input_path: Optional[Path], processed_dir: Path) -> str:
    if _resolve_source_paths("legi_cgi", input_path, processed_dir):
        return "legi_cgi"
    return "legi"


def _filter_sources(section_sources: list[str], allowlist: list[str]) -> list[str]:
    if not allowlist:
        return section_sources
    allow = {s.strip().lower() for s in allowlist if s.strip()}
    return [s for s in section_sources if s.lower() in allow]


def _run_queries(
    queries: list[str],
    sources: list[str],
    input_path: Optional[Path],
    processed_dir: Path,
    use_vector: bool,
    vector_index: Optional[Path],
    limit: int,
    scan_chars: int,
    snippet_chars: int,
    agent: str,
    action: str,
    no_log: bool,
    verbose: bool,
) -> list[qa_extractive.ScoredHit]:
    all_hits: list[qa_extractive.ScoredHit] = []
    for query in queries:
        if verbose:
            print(f"[fiche] Query: {query}")
        if not no_log:
            _log_usage(query, sources, agent, action)
        if use_vector:
            try:
                from loader import qa_vector
            except Exception as exc:
                raise SystemExit(
                    "Vector search requested but dependencies are missing. Install requirements-ml.txt"
                ) from exc
            hits = qa_vector.search(
                query=query,
                out_dir=vector_index or qa_vector.DEFAULT_INDEX_DIR,
                limit=limit,
                chunk_size=50000,
                snippet_chars=snippet_chars,
                sources=sources,
                oversample=20,
            )
        else:
            hits = qa_extractive.search(
                query=query,
                input_path=input_path,
                processed_dir=processed_dir,
                sources=sources,
                limit=limit,
                match="any",
                scan_chars=scan_chars,
                snippet_chars=snippet_chars,
            )
        if verbose:
            print(f"[fiche] Hits: {len(hits)}")
        all_hits.extend(hits)
    return _merge_hits(all_hits)[:limit]


def _hydrate_texts(
    hits: list[qa_extractive.ScoredHit],
    input_path: Optional[Path],
    processed_dir: Path,
    max_text_chars: int,
    verbose: bool,
) -> dict[str, str]:
    targets: dict[Path, set[str]] = {}
    for hit in hits:
        record_id = hit.record_id
        if not record_id:
            continue
        for path in _resolve_source_paths(hit.source, input_path, processed_dir):
            targets.setdefault(path, set()).add(record_id)

    texts: dict[str, str] = {}
    for path, wanted in targets.items():
        if not wanted:
            continue
        if verbose:
            print(f"[fiche] Hydrate texts from: {path} (need={len(wanted)})")
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                rid = str(rec.get("record_id") or "")
                if rid and rid in wanted:
                    text = str(rec.get("text") or "")
                    if max_text_chars > 0:
                        text = text[:max_text_chars]
                    texts[rid] = text
                    wanted.remove(rid)
                    if not wanted:
                        break
        if wanted and verbose:
            print(f"[fiche] Missing {len(wanted)} texts for {path}")
    return texts


def build_fiche(
    theme: Theme,
    input_path: Optional[Path],
    processed_dir: Path,
    sources: list[str],
    limit: int,
    cadre_limit: int,
    doctrine_limit: int,
    cles_limit: int,
    scan_chars: int,
    snippet_chars: int,
    agent: str,
    action: str,
    no_log: bool,
    verbose: bool,
    use_vector: bool,
    vector_index: Optional[Path],
    max_text_chars: int,
) -> list[Section]:
    if verbose:
        print(f"[fiche] Theme: {theme.title} queries={len(theme.queries)}")

    cgi_source = _detect_cgi_source(input_path, processed_dir)
    cadre_sources = _filter_sources([cgi_source], sources)
    doctrine_sources = _filter_sources(["bofip"], sources)
    cles_sources = _filter_sources(list({*cadre_sources, *doctrine_sources}), sources)

    sections: list[Section] = []

    if cadre_sources:
        if verbose:
            print(f"[fiche] Section: Cadre legal (CGI) sources={cadre_sources}")
        cadre_hits = _run_queries(
            queries=theme.queries,
            sources=cadre_sources,
            input_path=input_path,
            processed_dir=processed_dir,
            use_vector=use_vector,
            vector_index=vector_index,
            limit=max(1, cadre_limit),
            scan_chars=scan_chars,
            snippet_chars=snippet_chars,
            agent=agent,
            action=action,
            no_log=no_log,
            verbose=verbose,
        )
        cadre_texts = _hydrate_texts(cadre_hits, input_path, processed_dir, max_text_chars, verbose)
        sections.append(Section(name="Cadre legal (CGI)", sources=cadre_sources, hits=cadre_hits, texts=cadre_texts))
    elif verbose:
        print("[fiche] Section: Cadre legal (CGI) skipped (source not available)")

    if doctrine_sources:
        if verbose:
            print(f"[fiche] Section: Doctrine (BOFiP) sources={doctrine_sources}")
        doctrine_hits = _run_queries(
            queries=theme.queries,
            sources=doctrine_sources,
            input_path=input_path,
            processed_dir=processed_dir,
            use_vector=use_vector,
            vector_index=vector_index,
            limit=max(1, doctrine_limit),
            scan_chars=scan_chars,
            snippet_chars=snippet_chars,
            agent=agent,
            action=action,
            no_log=no_log,
            verbose=verbose,
        )
        doctrine_texts = _hydrate_texts(
            doctrine_hits, input_path, processed_dir, max_text_chars, verbose
        )
        sections.append(Section(name="Doctrine (BOFiP)", sources=doctrine_sources, hits=doctrine_hits, texts=doctrine_texts))
    elif verbose:
        print("[fiche] Section: Doctrine (BOFiP) skipped (source not available)")

    if cles_sources:
        if verbose:
            print(f"[fiche] Section: Extraits cles sources={cles_sources}")
        cles_hits = _run_queries(
            queries=theme.queries,
            sources=cles_sources,
            input_path=input_path,
            processed_dir=processed_dir,
            use_vector=use_vector,
            vector_index=vector_index,
            limit=max(1, cles_limit),
            scan_chars=scan_chars,
            snippet_chars=snippet_chars,
            agent=agent,
            action=action,
            no_log=no_log,
            verbose=verbose,
        )
        cles_texts = _hydrate_texts(cles_hits, input_path, processed_dir, max_text_chars, verbose)
        sections.append(Section(name="Extraits cles", sources=cles_sources, hits=cles_hits, texts=cles_texts))

    return sections


def write_markdown(
    theme: Theme,
    sections: list[Section],
    out_path: Path,
    version_id: str,
    generated_at: str,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append(f"# {theme.title}")
    lines.append("")
    lines.append(f"- Generated: {generated_at}")
    lines.append(f"- Version: {version_id}")
    lines.append(f"- Queries: {', '.join(theme.queries)}")
    lines.append("")

    if not sections:
        lines.append("Je ne sais pas.")
    else:
        for section in sections:
            lines.append(f"## {section.name}")
            lines.append("")
            if not section.hits:
                lines.append("Je ne sais pas.")
                lines.append("")
                continue
            for idx, hit in enumerate(section.hits, start=1):
                lines.append(f"### Source {idx}")
                lines.append("")
                lines.append(f"- Source: {hit.source}")
                if hit.title:
                    lines.append(f"- Title: {hit.title}")
                if hit.date:
                    lines.append(f"- Date: {hit.date}")
                if hit.url:
                    lines.append(f"- URL: {hit.url}")
                lines.append(f"- Record: {hit.record_id}")
                lines.append(f"- File: {hit.source_file} (raw_index={hit.raw_index})")
                full_text = section.texts.get(hit.record_id, "")
                if full_text:
                    lines.append("")
                    lines.append("Texte source:")
                    lines.append("```text")
                    lines.append(full_text)
                    lines.append("```")
                elif hit.snippet:
                    lines.append("")
                    lines.append("Extrait:")
                    lines.append("```text")
                    lines.append(hit.snippet)
                    lines.append("```")
                lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")


def write_json(
    theme: Theme,
    sections: list[Section],
    out_path: Path,
    version_id: str,
    generated_at: str,
) -> None:
    payload = {
        "title": theme.title,
        "slug": theme.slug,
        "generated_at": generated_at,
        "version": version_id,
        "queries": theme.queries,
        "sections": [],
    }
    for section in sections:
        entry = {
            "name": section.name,
            "sources": section.sources,
            "hits": [],
        }
        for hit in section.hits:
            entry["hits"].append(
                {
                    "score": hit.score,
                    "source": hit.source,
                    "record_id": hit.record_id,
                    "title": hit.title,
                    "date": hit.date,
                    "url": hit.url,
                    "source_file": hit.source_file,
                    "raw_index": hit.raw_index,
                    "snippet": hit.snippet,
                    "text": section.texts.get(hit.record_id, ""),
                }
            )
        payload["sections"].append(entry)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build extractive fiches from normalized JSONL.")
    parser.add_argument("--out", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--processed", default=str(DEFAULT_PROCESSED_DIR))
    parser.add_argument("--in", dest="input_path", default="", help="Fichier JSONL ou dossier normalized")
    parser.add_argument("--themes", default="", help="Path to themes JSON")
    parser.add_argument("--theme", action="append", default=[], help="Theme title (repeatable)")
    parser.add_argument("--source", action="append", default=[], help="Filtrer par source (bofip, legi, etc.)")
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--scan-chars", type=int, default=20000)
    parser.add_argument("--snippet-chars", type=int, default=240)
    parser.add_argument("--agent", default="ucfc_cli")
    parser.add_argument("--action", default="fiche_build")
    parser.add_argument("--no-log", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--use-vector", action="store_true")
    parser.add_argument("--vector-index", default="", help="Vector index directory")
    parser.add_argument("--max-text-chars", type=int, default=4000, help="0 = full text")
    parser.add_argument("--cadre-limit", type=int, default=0)
    parser.add_argument("--doctrine-limit", type=int, default=0)
    parser.add_argument("--cles-limit", type=int, default=0)
    args = parser.parse_args(argv)

    processed_dir = Path(args.processed)
    input_path = Path(args.input_path) if args.input_path else None
    themes_path = Path(args.themes) if args.themes else None
    themes = _load_themes(themes_path, args.theme)

    version_id = _latest_version_id(processed_dir)
    generated_at = datetime.now().isoformat()

    out_dir = Path(args.out)
    for theme in themes:
        cadre_limit = args.cadre_limit or args.limit
        doctrine_limit = args.doctrine_limit or args.limit
        cles_limit = args.cles_limit or args.limit
        sections = build_fiche(
            theme=theme,
            input_path=input_path,
            processed_dir=processed_dir,
            sources=args.source,
            limit=max(1, args.limit),
            cadre_limit=max(1, cadre_limit),
            doctrine_limit=max(1, doctrine_limit),
            cles_limit=max(1, cles_limit),
            scan_chars=args.scan_chars,
            snippet_chars=args.snippet_chars,
            agent=args.agent,
            action=args.action,
            no_log=args.no_log,
            verbose=args.verbose,
            use_vector=args.use_vector,
            vector_index=Path(args.vector_index) if args.vector_index else None,
            max_text_chars=args.max_text_chars,
        )
        slug = _slugify(theme.slug or theme.title)
        out_path = out_dir / f"{slug}.md"
        write_markdown(theme, sections, out_path, version_id, generated_at)
        write_json(theme, sections, out_path.with_suffix(".json"), version_id, generated_at)
        print(f"[fiche] {theme.title} -> {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
