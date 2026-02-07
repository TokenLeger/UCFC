from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from loader import pipeline_v1
from loader.connectors import bofip_downloader, legifrance_piste, legi_open_data
from loader.legifrance_bulk import run_plan


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_DIR = REPO_ROOT / "data_fiscale" / "raw"
DEFAULT_OUT_DIR = REPO_ROOT / "data_fiscale" / "processed"
DEFAULT_LEGI_PLAN = REPO_ROOT / "loader" / "legifrance_bulk_plan.json"
DEFAULT_LEGI_OUT = DEFAULT_RAW_DIR / "legifrance"
DEFAULT_LEGI_OPEN_OUT = DEFAULT_RAW_DIR / "legi"
DEFAULT_LEGI_OPEN_BASE = legi_open_data.DEFAULT_BASE_URL
DEFAULT_JUDILIBRE_PLAN = REPO_ROOT / "loader" / "judilibre_bulk_plan.json"
DEFAULT_JUDILIBRE_OUT = DEFAULT_RAW_DIR / "judilibre"
DEFAULT_JUSTICE_PLAN = REPO_ROOT / "loader" / "justice_back_bulk_plan.json"
DEFAULT_JUSTICE_OUT = DEFAULT_RAW_DIR / "justice_back"


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="UCFC orchestrator V1 (BOFiP + Legifrance + ingest)")
    parser.add_argument("--raw", default=str(DEFAULT_RAW_DIR), help="Raw input folder")
    parser.add_argument("--out", default=str(DEFAULT_OUT_DIR), help="Processed output folder")
    parser.add_argument("--skip-bofip", action="store_true", help="Skip BOFiP download")
    parser.add_argument("--skip-legifrance", action="store_true", help="Skip Legifrance bulk")
    parser.add_argument("--skip-judilibre", action="store_true", help="Skip JUDILIBRE bulk")
    parser.add_argument("--skip-justice-back", action="store_true", help="Skip Justice back bulk")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip ingest-v1")
    parser.add_argument("--workers", type=int, default=1, help="Parallel workers for normalization")
    parser.add_argument("--verbose", action="store_true", help="Verbose progress logs")

    parser.add_argument("--bofip-limit", type=int, default=0, help="Limit BOFiP files")
    parser.add_argument("--bofip-overwrite", action="store_true", help="Overwrite existing BOFiP files")
    parser.add_argument("--bofip-manifest-url", default=bofip_downloader.DEFAULT_MANIFEST_URL)

    parser.add_argument("--legifrance-plan", default=str(DEFAULT_LEGI_PLAN), help="Bulk plan JSON")
    parser.add_argument("--legifrance-out", default=str(DEFAULT_LEGI_OUT), help="Legifrance output folder")
    parser.add_argument("--legifrance-max-pages", type=int, default=0, help="Override pagination max pages")
    parser.add_argument("--legi-open-data", action="store_true", help="Download LEGI open data dumps")
    parser.add_argument("--legi-mode", default="full", choices=["full", "latest", "all"])
    parser.add_argument("--legi-base-url", default=str(DEFAULT_LEGI_OPEN_BASE))
    parser.add_argument("--legi-out", default=str(DEFAULT_LEGI_OPEN_OUT))
    parser.add_argument("--legi-limit", type=int, default=0)
    parser.add_argument("--legi-overwrite", action="store_true")
    parser.add_argument("--judilibre-plan", default=str(DEFAULT_JUDILIBRE_PLAN), help="JUDILIBRE plan JSON")
    parser.add_argument("--judilibre-out", default=str(DEFAULT_JUDILIBRE_OUT), help="JUDILIBRE output folder")
    parser.add_argument("--judilibre-max-pages", type=int, default=0, help="Override pagination max pages")
    parser.add_argument("--justice-back-plan", default=str(DEFAULT_JUSTICE_PLAN), help="Justice back plan JSON")
    parser.add_argument("--justice-back-out", default=str(DEFAULT_JUSTICE_OUT), help="Justice back output folder")
    parser.add_argument("--justice-back-max-pages", type=int, default=0, help="Override pagination max pages")

    args = parser.parse_args(argv)

    raw_dir = Path(args.raw)
    out_dir = Path(args.out)

    if not args.skip_bofip:
        if args.verbose:
            print("[orchestrator] BOFiP download start")
        entries = bofip_downloader.fetch_manifest(args.bofip_manifest_url)
        if args.bofip_limit:
            entries = entries[: args.bofip_limit]
        bofip_downloader.download_all(
            entries,
            out_dir=raw_dir / "bofip",
            overwrite=args.bofip_overwrite,
            verbose=args.verbose,
        )

    config = None
    if not (args.skip_legifrance and args.skip_judilibre and args.skip_justice_back):
        config = legifrance_piste.env_config()

    if not args.skip_legifrance:
        if args.verbose:
            print("[orchestrator] Legifrance bulk start")
        max_pages = args.legifrance_max_pages if args.legifrance_max_pages else None
        run_plan(
            Path(args.legifrance_plan),
            Path(args.legifrance_out),
            config,  # type: ignore[arg-type]
            verbose=args.verbose,
            max_pages=max_pages,
        )

    if args.legi_open_data:
        if args.verbose:
            print("[orchestrator] LEGI open data download start")
        names = legi_open_data.list_available_files(args.legi_base_url)
        selected = legi_open_data.select_files(names, mode=args.legi_mode, limit=args.legi_limit)
        if not selected:
            print("[orchestrator] LEGI open data: no files selected")
        else:
            results = legi_open_data.download_files(
                args.legi_base_url,
                selected,
                out_dir=Path(args.legi_out),
                overwrite=args.legi_overwrite,
                verbose=args.verbose,
            )
            legi_open_data.write_manifest(results, out_dir=Path(args.legi_out))

    if not args.skip_judilibre:
        if args.verbose:
            print("[orchestrator] JUDILIBRE bulk start")
        max_pages = args.judilibre_max_pages if args.judilibre_max_pages else None
        run_plan(
            Path(args.judilibre_plan),
            Path(args.judilibre_out),
            config,  # type: ignore[arg-type]
            verbose=args.verbose,
            max_pages=max_pages,
        )

    if not args.skip_justice_back:
        if args.verbose:
            print("[orchestrator] Justice back bulk start")
        max_pages = args.justice_back_max_pages if args.justice_back_max_pages else None
        run_plan(
            Path(args.justice_back_plan),
            Path(args.justice_back_out),
            config,  # type: ignore[arg-type]
            verbose=args.verbose,
            max_pages=max_pages,
        )

    if not args.skip_ingest:
        if args.verbose:
            print("[orchestrator] Ingest V1 start")
        pipeline_v1.run(raw_dir, out_dir, verbose=args.verbose, workers=args.workers)

    if args.verbose:
        print("[orchestrator] Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
