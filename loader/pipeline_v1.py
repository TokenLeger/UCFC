from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from loader import pipeline_ingest
from loader.normalizers.jsonl_normalizer import normalize_source_dir

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_DIR = REPO_ROOT / "data_fiscale" / "raw"
DEFAULT_OUT_DIR = REPO_ROOT / "data_fiscale" / "processed"


def run(raw_dir: Path, out_dir: Path, verbose: bool = False, workers: int = 1) -> Path:
    if verbose:
        print(f"[v1] Scanning raw dir: {raw_dir}")
    files = pipeline_ingest.build_manifest(raw_dir, verbose=verbose)
    if not files:
        raise RuntimeError("No files found in raw corpus.")

    version_hash = pipeline_ingest._combined_hash(files)
    version_date = datetime.now().date().isoformat()
    version_id = f"{version_date}_{version_hash[:12]}"
    version_dir = out_dir / version_id

    if verbose:
        print(f"[v1] Writing manifest: {version_dir}")
    pipeline_ingest.write_versioned_manifest(files, version_dir)
    pipeline_ingest.write_corpus_version(files, version_dir)

    normalized_dir = version_dir / "normalized"
    report = {
        "generated_at": datetime.now().isoformat(),
        "version_id": version_id,
        "sources": {},
    }

    for source in ("bofip", "legifrance", "legi", "judilibre", "justice_back"):
        input_dir = raw_dir / source
        output_path = normalized_dir / f"{source}.jsonl"
        if input_dir.exists():
            if verbose:
                print(f"[v1] Normalizing source: {source}")
            stats = normalize_source_dir(
                source, input_dir, output_path, verbose=verbose, workers=workers
            )
            report["sources"][source] = {
                "input_files": stats.input_files,
                "records_out": stats.records_out,
                "skipped_files": stats.skipped_files,
                "output": str(output_path),
            }
        else:
            report["sources"][source] = {
                "input_files": 0,
                "records_out": 0,
                "skipped_files": 0,
                "output": str(output_path),
                "note": "input_dir_missing",
            }

    report_path = version_dir / "normalization_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return version_dir


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="UCFC pipeline V1 (versioning + JSONL normalization)")
    parser.add_argument("--raw", default=str(DEFAULT_RAW_DIR))
    parser.add_argument("--out", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--verbose", action="store_true", help="Verbose progress logs")
    parser.add_argument("--workers", type=int, default=1, help="Parallel workers for normalization")
    args = parser.parse_args(argv)

    version_dir = run(Path(args.raw), Path(args.out), verbose=args.verbose, workers=args.workers)
    print(f"Pipeline V1 complete: {version_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
