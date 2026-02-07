from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Optional


def _parse_pages(pages: str, total: int) -> list[int]:
    if not pages:
        return list(range(total))
    out: set[int] = set()
    for part in pages.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start_s, end_s = part.split("-", 1)
            start = max(1, int(start_s))
            end = max(start, int(end_s))
            for i in range(start, min(end, total) + 1):
                out.add(i - 1)
        else:
            idx = max(1, int(part)) - 1
            if idx < total:
                out.add(idx)
    return sorted(out)


def extract_text(
    pdf_path: Path,
    pages: str = "",
    max_chars: int = 0,
    verbose: bool = False,
) -> str:
    try:
        from pypdf import PdfReader
    except Exception as exc:
        raise SystemExit("Missing PDF dependency. Install: pip install pypdf") from exc

    reader = PdfReader(str(pdf_path))
    total = len(reader.pages)
    indices = _parse_pages(pages, total)
    chunks: list[str] = []
    for idx in indices:
        if verbose:
            print(f"[pdf] page {idx + 1}/{total}")
        text = reader.pages[idx].extract_text() or ""
        if max_chars > 0:
            text = text[:max_chars]
        chunks.append(f"----- PAGE {idx + 1} -----\n{text}".strip())
    return "\n\n".join(chunks)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Extract text from PDF")
    parser.add_argument("--in", dest="input_path", required=True, help="PDF file path")
    parser.add_argument("--out", default="", help="Output text file (optional)")
    parser.add_argument("--pages", default="", help='Pages to extract (e.g. "1-3,5")')
    parser.add_argument("--max-chars", type=int, default=0, help="Max chars per page (0 = full)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    pdf_path = Path(args.input_path)
    if not pdf_path.exists():
        raise SystemExit(f"PDF not found: {pdf_path}")

    text = extract_text(pdf_path, pages=args.pages, max_chars=args.max_chars, verbose=args.verbose)
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
        print(f"Saved: {out_path}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
