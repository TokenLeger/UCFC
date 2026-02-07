from __future__ import annotations

import argparse
import json
import unicodedata
from pathlib import Path

from loader.connectors import bofip_downloader, legifrance_piste, legi_open_data
from loader.filters import legi_cgi_extract
from loader import pipeline_ingest, pipeline_v1, orchestrator_v1, qa_extractive, fiches_builder, qa_vector

REPO_ROOT = Path(__file__).resolve().parent


def _cmd_bofip(args: argparse.Namespace) -> int:
    entries = bofip_downloader.fetch_manifest(args.manifest_url)
    if args.limit:
        entries = entries[: args.limit]
    results = bofip_downloader.download_all(
        entries, out_dir=args.out, overwrite=args.overwrite, verbose=args.verbose
    )
    manifest_path = bofip_downloader.write_manifest(results, out_dir=args.out)
    print(f"Downloaded {len(results)} file(s). Manifest: {manifest_path}")
    return 0


def _cmd_legifrance(args: argparse.Namespace) -> int:
    params = {}
    for item in args.param:
        if "=" not in item:
            raise SystemExit(f"Invalid param: {item}. Use key=value")
        k, v = item.split("=", 1)
        params[k] = v

    if args.body and args.body_file:
        raise SystemExit("Use only one of --body or --body-file.")

    body = None
    if args.body:
        try:
            body = json.loads(args.body)
        except Exception as exc:
            raise SystemExit(f"Invalid JSON in --body: {exc}") from exc
    elif args.body_file:
        try:
            body = json.loads(Path(args.body_file).read_text(encoding="utf-8"))
        except Exception as exc:
            raise SystemExit(f"Invalid JSON in --body-file: {exc}") from exc

    method = (args.method or "").strip().upper()
    if not method:
        spec_path = Path(args.spec) if args.spec else _find_legifrance_spec()
        method = _infer_legifrance_method(args.path, spec_path) or "GET"

    client = legifrance_piste.PisteClient(legifrance_piste.env_config())
    payload = client.request_json(
        args.path,
        params=params or None,
        method=method,
        body=body,
        verbose=args.verbose,
    )

    out_path = args.out / f"{args.name}.json"
    legifrance_piste.save_json(payload, out_path)

    print(f"Saved: {out_path}")
    return 0


def _find_legifrance_spec() -> Path:
    api_docs = REPO_ROOT / "loader" / "API_docs"
    if not api_docs.exists():
        raise SystemExit("API_docs not found. Expected loader/API_docs.")

    for p in api_docs.glob("*.json"):
        name = unicodedata.normalize("NFKD", p.name).encode("ascii", "ignore").decode("ascii")
        if "legifrance" in name.lower():
            return p

    raise SystemExit("No Legifrance swagger JSON found in loader/API_docs.")


def _infer_legifrance_method(path: str, spec_path: Path) -> str:
    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    item = payload.get("paths", {}).get(path, {})
    if "post" in item:
        return "POST"
    if "get" in item:
        return "GET"
    if "put" in item:
        return "PUT"
    if "patch" in item:
        return "PATCH"
    if "delete" in item:
        return "DELETE"
    return ""


def _cmd_legifrance_list_paths(args: argparse.Namespace) -> int:
    spec_path = Path(args.spec) if args.spec else _find_legifrance_spec()
    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    paths = sorted(payload.get("paths", {}).keys())
    if args.filter:
        needle = args.filter.lower()
        paths = [p for p in paths if needle in p.lower()]

    print("\n".join(paths))
    return 0


def _cmd_ingest_version(args: argparse.Namespace) -> int:
    argv = ["--raw", str(args.raw), "--out", str(args.out)]
    if args.verbose:
        argv.append("--verbose")
    return pipeline_ingest.main(argv)


def _cmd_ingest_v1(args: argparse.Namespace) -> int:
    version_dir = pipeline_v1.run(args.raw, args.out, verbose=args.verbose, workers=args.workers)
    print(f"Pipeline V1 complete: {version_dir}")
    return 0


def _cmd_legi_download(args: argparse.Namespace) -> int:
    argv = [
        "--out",
        str(args.out),
        "--base-url",
        args.base_url,
        "--mode",
        args.mode,
    ]
    if args.limit:
        argv.extend(["--limit", str(args.limit)])
    if args.overwrite:
        argv.append("--overwrite")
    if args.list:
        argv.append("--list")
    if args.verbose:
        argv.append("--verbose")
    return legi_open_data.main(argv)


def _cmd_legi_extract_cgi(args: argparse.Namespace) -> int:
    argv = []
    if args.input_path:
        argv.extend(["--in", str(args.input_path)])
    if args.out:
        argv.extend(["--out", str(args.out)])
    for text_id in args.text_id:
        argv.extend(["--text-id", text_id])
    if args.limit:
        argv.extend(["--limit", str(args.limit)])
    if args.verbose:
        argv.append("--verbose")
    return legi_cgi_extract.main(argv)


def _cmd_orchestrate_v1(args: argparse.Namespace) -> int:
    argv = [
        "--raw",
        str(args.raw),
        "--out",
        str(args.out),
        "--legifrance-plan",
        str(args.legifrance_plan),
        "--legifrance-out",
        str(args.legifrance_out),
        "--judilibre-plan",
        str(args.judilibre_plan),
        "--judilibre-out",
        str(args.judilibre_out),
        "--justice-back-plan",
        str(args.justice_back_plan),
        "--justice-back-out",
        str(args.justice_back_out),
        "--legi-mode",
        args.legi_mode,
        "--legi-base-url",
        args.legi_base_url,
        "--legi-out",
        str(args.legi_out),
        "--bofip-manifest-url",
        str(args.bofip_manifest_url),
    ]
    if args.skip_bofip:
        argv.append("--skip-bofip")
    if args.skip_legifrance:
        argv.append("--skip-legifrance")
    if args.skip_judilibre:
        argv.append("--skip-judilibre")
    if args.skip_justice_back:
        argv.append("--skip-justice-back")
    if args.legi_open_data:
        argv.append("--legi-open-data")
    if args.skip_ingest:
        argv.append("--skip-ingest")
    if args.bofip_limit:
        argv.extend(["--bofip-limit", str(args.bofip_limit)])
    if args.bofip_overwrite:
        argv.append("--bofip-overwrite")
    if args.legifrance_max_pages:
        argv.extend(["--legifrance-max-pages", str(args.legifrance_max_pages)])
    if args.workers:
        argv.extend(["--workers", str(args.workers)])
    if args.legi_limit:
        argv.extend(["--legi-limit", str(args.legi_limit)])
    if args.legi_overwrite:
        argv.append("--legi-overwrite")
    if args.judilibre_max_pages:
        argv.extend(["--judilibre-max-pages", str(args.judilibre_max_pages)])
    if args.justice_back_max_pages:
        argv.extend(["--justice-back-max-pages", str(args.justice_back_max_pages)])
    if args.verbose:
        argv.append("--verbose")
    return orchestrator_v1.main(argv)


def _cmd_qa_search(args: argparse.Namespace) -> int:
    argv = [
        "--query",
        args.query,
        "--processed",
        str(args.processed),
        "--limit",
        str(args.limit),
        "--match",
        args.match,
        "--scan-chars",
        str(args.scan_chars),
        "--snippet-chars",
        str(args.snippet_chars),
        "--agent",
        args.agent,
        "--action",
        args.action,
    ]
    if args.input_path:
        argv.extend(["--in", str(args.input_path)])
    for source in args.source:
        argv.extend(["--source", source])
    if args.no_snippet:
        argv.append("--no-snippet")
    if args.json:
        argv.append("--json")
    if args.no_log:
        argv.append("--no-log")
    return qa_extractive.main(argv)


def _cmd_fiches_build(args: argparse.Namespace) -> int:
    argv = [
        "--out",
        str(args.out),
        "--processed",
        str(args.processed),
        "--limit",
        str(args.limit),
        "--scan-chars",
        str(args.scan_chars),
        "--snippet-chars",
        str(args.snippet_chars),
        "--agent",
        args.agent,
        "--action",
        args.action,
    ]
    if args.input_path:
        argv.extend(["--in", str(args.input_path)])
    if args.themes:
        argv.extend(["--themes", str(args.themes)])
    for theme in args.theme:
        argv.extend(["--theme", theme])
    for source in args.source:
        argv.extend(["--source", source])
    if args.use_vector:
        argv.append("--use-vector")
    if args.vector_index:
        argv.extend(["--vector-index", str(args.vector_index)])
    if args.max_text_chars is not None:
        argv.extend(["--max-text-chars", str(args.max_text_chars)])
    if args.no_log:
        argv.append("--no-log")
    if args.verbose:
        argv.append("--verbose")
    return fiches_builder.main(argv)


def _cmd_qa_index(args: argparse.Namespace) -> int:
    argv = [
        "--out",
        str(args.out),
        "--processed",
        str(args.processed),
        "--model",
        args.model,
        "--batch-size",
        str(args.batch_size),
        "--max-chars",
        str(args.max_chars),
        "--log-every",
        str(args.log_every),
    ]
    if args.input_path:
        argv.extend(["--in", str(args.input_path)])
    for source in args.source:
        argv.extend(["--source", source])
    if args.verbose:
        argv.append("--verbose")
    if args.overwrite:
        argv.append("--overwrite")
    return qa_vector.main_index(argv)


def _cmd_qa_search_vec(args: argparse.Namespace) -> int:
    argv = [
        "--query",
        args.query,
        "--index",
        str(args.index),
        "--limit",
        str(args.limit),
        "--chunk-size",
        str(args.chunk_size),
        "--snippet-chars",
        str(args.snippet_chars),
        "--agent",
        args.agent,
        "--action",
        args.action,
    ]
    if args.json:
        argv.append("--json")
    if args.no_log:
        argv.append("--no-log")
    return qa_vector.main_search(argv)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pfc", description="UCFC CLI (PFC)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_bofip = sub.add_parser("bofip-download", help="Download BOFiP open data")
    p_bofip.add_argument("--out", default=bofip_downloader.DEFAULT_RAW_DIR, type=Path)
    p_bofip.add_argument("--limit", type=int, default=0)
    p_bofip.add_argument("--overwrite", action="store_true")
    p_bofip.add_argument("--verbose", action="store_true", help="Verbose progress logs")
    p_bofip.add_argument(
        "--manifest-url",
        default=bofip_downloader.DEFAULT_MANIFEST_URL,
        help="Manifest URL (JSON export)",
    )
    p_bofip.set_defaults(func=_cmd_bofip)

    p_legifrance = sub.add_parser("legifrance-fetch", help="Fetch from Legifrance PISTE")
    p_legifrance.add_argument("--path", required=True)
    p_legifrance.add_argument("--out", default=legifrance_piste.DEFAULT_RAW_DIR, type=Path)
    p_legifrance.add_argument("--name", default="legifrance_payload")
    p_legifrance.add_argument("--param", action="append", default=[])
    p_legifrance.add_argument("--method", default="", help="HTTP method (auto from swagger if empty)")
    p_legifrance.add_argument("--body", default="", help="JSON body string")
    p_legifrance.add_argument("--body-file", default="", help="Path to JSON body file")
    p_legifrance.add_argument("--spec", default="", help="Path to Legifrance swagger JSON")
    p_legifrance.add_argument("--verbose", action="store_true", help="Verbose progress logs")
    p_legifrance.set_defaults(func=_cmd_legifrance)

    p_legi_paths = sub.add_parser("legifrance-list-paths", help="List Legifrance endpoints (swagger)")
    p_legi_paths.add_argument("--spec", default="", help="Path to Legifrance swagger JSON")
    p_legi_paths.add_argument("--filter", default="", help="Filter endpoints by substring")
    p_legi_paths.set_defaults(func=_cmd_legifrance_list_paths)

    p_ingest = sub.add_parser("ingest-version", help="Create versioned manifest")
    p_ingest.add_argument("--raw", default=pipeline_ingest.DEFAULT_RAW_DIR, type=Path)
    p_ingest.add_argument("--out", default=pipeline_ingest.DEFAULT_OUT_DIR, type=Path)
    p_ingest.add_argument("--verbose", action="store_true", help="Verbose progress logs")
    p_ingest.set_defaults(func=_cmd_ingest_version)

    p_ingest_v1 = sub.add_parser("ingest-v1", help="Versioning + JSONL normalization")
    p_ingest_v1.add_argument("--raw", default=pipeline_v1.DEFAULT_RAW_DIR, type=Path)
    p_ingest_v1.add_argument("--out", default=pipeline_v1.DEFAULT_OUT_DIR, type=Path)
    p_ingest_v1.add_argument("--verbose", action="store_true", help="Verbose progress logs")
    p_ingest_v1.add_argument("--workers", type=int, default=1, help="Parallel workers for normalization")
    p_ingest_v1.set_defaults(func=_cmd_ingest_v1)

    p_legi = sub.add_parser("legi-download", help="Download LEGI open data dumps (DILA)")
    p_legi.add_argument("--out", default=legi_open_data.DEFAULT_RAW_DIR, type=Path)
    p_legi.add_argument("--base-url", default=legi_open_data.DEFAULT_BASE_URL)
    p_legi.add_argument("--mode", default="full", choices=["full", "latest", "all"])
    p_legi.add_argument("--limit", type=int, default=0)
    p_legi.add_argument("--overwrite", action="store_true")
    p_legi.add_argument("--list", action="store_true")
    p_legi.add_argument("--verbose", action="store_true", help="Verbose progress logs")
    p_legi.set_defaults(func=_cmd_legi_download)

    p_legi_cgi = sub.add_parser("legi-extract-cgi", help="Extract CGI records from LEGI JSONL")
    p_legi_cgi.add_argument("--in", dest="input_path", default="", help="Path to legi.jsonl (default latest)")
    p_legi_cgi.add_argument("--out", default="", help="Output JSONL path")
    p_legi_cgi.add_argument("--text-id", action="append", default=[], help="Additional LEGITEXT id to include")
    p_legi_cgi.add_argument("--limit", type=int, default=0)
    p_legi_cgi.add_argument("--verbose", action="store_true", help="Verbose logs")
    p_legi_cgi.set_defaults(func=_cmd_legi_extract_cgi)

    p_orch = sub.add_parser("orchestrate-v1", help="Orchestrate BOFiP + Legifrance + ingest")
    p_orch.add_argument("--raw", default=orchestrator_v1.DEFAULT_RAW_DIR, type=Path)
    p_orch.add_argument("--out", default=orchestrator_v1.DEFAULT_OUT_DIR, type=Path)
    p_orch.add_argument("--skip-bofip", action="store_true")
    p_orch.add_argument("--skip-legifrance", action="store_true")
    p_orch.add_argument("--skip-judilibre", action="store_true")
    p_orch.add_argument("--skip-justice-back", action="store_true")
    p_orch.add_argument("--skip-ingest", action="store_true")
    p_orch.add_argument("--bofip-limit", type=int, default=0)
    p_orch.add_argument("--bofip-overwrite", action="store_true")
    p_orch.add_argument("--bofip-manifest-url", default=bofip_downloader.DEFAULT_MANIFEST_URL)
    p_orch.add_argument("--legifrance-plan", default=orchestrator_v1.DEFAULT_LEGI_PLAN, type=Path)
    p_orch.add_argument("--legifrance-out", default=orchestrator_v1.DEFAULT_LEGI_OUT, type=Path)
    p_orch.add_argument("--legifrance-max-pages", type=int, default=0)
    p_orch.add_argument("--legi-open-data", action="store_true")
    p_orch.add_argument("--legi-mode", default="full", choices=["full", "latest", "all"])
    p_orch.add_argument("--legi-base-url", default=orchestrator_v1.DEFAULT_LEGI_OPEN_BASE)
    p_orch.add_argument("--legi-out", default=orchestrator_v1.DEFAULT_LEGI_OPEN_OUT, type=Path)
    p_orch.add_argument("--legi-limit", type=int, default=0)
    p_orch.add_argument("--legi-overwrite", action="store_true")
    p_orch.add_argument("--judilibre-plan", default=orchestrator_v1.DEFAULT_JUDILIBRE_PLAN, type=Path)
    p_orch.add_argument("--judilibre-out", default=orchestrator_v1.DEFAULT_JUDILIBRE_OUT, type=Path)
    p_orch.add_argument("--judilibre-max-pages", type=int, default=0)
    p_orch.add_argument("--justice-back-plan", default=orchestrator_v1.DEFAULT_JUSTICE_PLAN, type=Path)
    p_orch.add_argument("--justice-back-out", default=orchestrator_v1.DEFAULT_JUSTICE_OUT, type=Path)
    p_orch.add_argument("--justice-back-max-pages", type=int, default=0)
    p_orch.add_argument("--verbose", action="store_true", help="Verbose progress logs")
    p_orch.add_argument("--workers", type=int, default=1, help="Parallel workers for normalization")
    p_orch.set_defaults(func=_cmd_orchestrate_v1)

    p_qa = sub.add_parser("qa-search", help="Recherche extractive sur JSONL normalisés")
    p_qa.add_argument("--query", required=True, help='Texte de recherche (guillemets pour phrase)')
    p_qa.add_argument("--in", dest="input_path", default="", help="Fichier JSONL ou dossier normalized")
    p_qa.add_argument("--processed", default=qa_extractive.DEFAULT_PROCESSED_DIR, type=Path)
    p_qa.add_argument("--source", action="append", default=[], help="Filtrer par source (bofip, legi, etc.)")
    p_qa.add_argument("--limit", type=int, default=5)
    p_qa.add_argument("--match", choices=["any", "all"], default="any")
    p_qa.add_argument("--scan-chars", type=int, default=20000)
    p_qa.add_argument("--snippet-chars", type=int, default=240)
    p_qa.add_argument("--no-snippet", action="store_true")
    p_qa.add_argument("--json", action="store_true")
    p_qa.add_argument("--agent", default="ucfc_cli")
    p_qa.add_argument("--action", default="qa_search")
    p_qa.add_argument("--no-log", action="store_true")
    p_qa.set_defaults(func=_cmd_qa_search)

    p_fiches = sub.add_parser("fiches-build", help="Generer des fiches extractives (Markdown)")
    p_fiches.add_argument("--out", default=fiches_builder.DEFAULT_OUT_DIR, type=Path)
    p_fiches.add_argument("--processed", default=fiches_builder.DEFAULT_PROCESSED_DIR, type=Path)
    p_fiches.add_argument("--in", dest="input_path", default="", help="Fichier JSONL ou dossier normalized")
    p_fiches.add_argument("--themes", default="", help="Path to themes JSON")
    p_fiches.add_argument("--theme", action="append", default=[], help="Theme title (repeatable)")
    p_fiches.add_argument("--source", action="append", default=[], help="Filtrer par source (bofip, legi, etc.)")
    p_fiches.add_argument("--limit", type=int, default=8)
    p_fiches.add_argument("--scan-chars", type=int, default=20000)
    p_fiches.add_argument("--snippet-chars", type=int, default=240)
    p_fiches.add_argument("--agent", default="ucfc_cli")
    p_fiches.add_argument("--action", default="fiche_build")
    p_fiches.add_argument("--no-log", action="store_true")
    p_fiches.add_argument("--verbose", action="store_true")
    p_fiches.add_argument("--use-vector", action="store_true")
    p_fiches.add_argument("--vector-index", default="", type=Path)
    p_fiches.add_argument("--max-text-chars", type=int, default=4000)
    p_fiches.set_defaults(func=_cmd_fiches_build)

    p_qindex = sub.add_parser("qa-index", help="Build vector index (ML)")
    p_qindex.add_argument("--out", default=qa_vector.DEFAULT_INDEX_DIR, type=Path)
    p_qindex.add_argument("--processed", default=qa_vector.DEFAULT_PROCESSED_DIR, type=Path)
    p_qindex.add_argument("--in", dest="input_path", default="", help="Fichier JSONL ou dossier normalized")
    p_qindex.add_argument("--source", action="append", default=[], help="Filtrer par source (bofip, legi, etc.)")
    p_qindex.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")
    p_qindex.add_argument("--batch-size", type=int, default=64)
    p_qindex.add_argument("--max-chars", type=int, default=4000)
    p_qindex.add_argument("--log-every", type=int, default=5000)
    p_qindex.add_argument("--overwrite", action="store_true")
    p_qindex.add_argument("--verbose", action="store_true")
    p_qindex.set_defaults(func=_cmd_qa_index)

    p_qsearch = sub.add_parser("qa-search-vec", help="Vector search (ML)")
    p_qsearch.add_argument("--query", required=True)
    p_qsearch.add_argument("--index", default=qa_vector.DEFAULT_INDEX_DIR, type=Path)
    p_qsearch.add_argument("--limit", type=int, default=5)
    p_qsearch.add_argument("--chunk-size", type=int, default=50000)
    p_qsearch.add_argument("--snippet-chars", type=int, default=240)
    p_qsearch.add_argument("--json", action="store_true")
    p_qsearch.add_argument("--agent", default="ucfc_cli")
    p_qsearch.add_argument("--action", default="qa_vector_search")
    p_qsearch.add_argument("--no-log", action="store_true")
    p_qsearch.set_defaults(func=_cmd_qa_search_vec)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
