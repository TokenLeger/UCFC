from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from loader.connectors.legifrance_piste import PisteClient, PisteConfig, save_json


@dataclass
class BulkResult:
    path: str
    out_path: Path
    ok: bool
    error: Optional[str] = None


def _count_items(payload: Any) -> Optional[int]:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("results", "items", "data", "records", "textes"):
            val = payload.get(key)
            if isinstance(val, list):
                return len(val)
    return None


def run_plan(
    plan_path: Path,
    out_dir: Path,
    config: PisteConfig,
    verbose: bool = False,
    max_pages: Optional[int] = None,
) -> list[BulkResult]:
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    api_base = plan.get("api_base")
    if api_base:
        config = PisteConfig(
            token_url=config.token_url,
            api_base=api_base,
            client_id=config.client_id,
            client_secret=config.client_secret,
            scope=config.scope,
        )
    client = PisteClient(config)
    requests = plan.get("requests", [])
    results: list[BulkResult] = []

    for req in requests:
        if not req.get("enabled", True):
            if verbose:
                print(f"[bulk] SKIP disabled: {req.get('path')}")
            continue

        path = req.get("path")
        method = (req.get("method") or "GET").upper()
        params = req.get("params")
        body = req.get("body")
        name = req.get("name") or path.strip("/").replace("/", "_")
        subdir = req.get("out_dir", "")
        paginate = req.get("paginate")

        target_dir = out_dir / subdir if subdir else out_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        if not paginate:
            try:
                payload = client.request_json(path, params=params, method=method, body=body, verbose=verbose)
                out_path = target_dir / f"{name}.json"
                save_json(payload, out_path)
                results.append(BulkResult(path=path, out_path=out_path, ok=True))
                if verbose:
                    print(f"[bulk] Saved: {out_path}")
            except Exception as exc:
                results.append(BulkResult(path=path, out_path=target_dir / f"{name}.json", ok=False, error=str(exc)))
                if verbose:
                    print(f"[bulk] ERROR {path}: {exc}")
            continue

        page_param = paginate.get("pageParam", "pageNumber")
        start_page = int(paginate.get("start", 1))
        page_size_param = paginate.get("pageSizeParam", "pageSize")
        page_size = int(paginate.get("pageSize", 100))
        stop_on_empty = bool(paginate.get("stopOnEmpty", True))
        max_pages_local = int(paginate.get("maxPages", 1))
        if max_pages is not None:
            max_pages_local = max_pages

        for offset in range(max_pages_local):
            page_number = start_page + offset
            body_page = dict(body or {})
            body_page[page_param] = page_number
            body_page[page_size_param] = body_page.get(page_size_param, page_size)
            try:
                payload = client.request_json(
                    path,
                    params=params,
                    method=method,
                    body=body_page,
                    verbose=verbose,
                )
                out_path = target_dir / f"{name}_p{page_number}.json"
                save_json(payload, out_path)
                results.append(BulkResult(path=path, out_path=out_path, ok=True))
                if verbose:
                    print(f"[bulk] Saved: {out_path}")
                if stop_on_empty:
                    count = _count_items(payload)
                    if count is not None and count == 0:
                        if verbose:
                            print(f"[bulk] Stop pagination (empty page) for {path}")
                        break
            except Exception as exc:
                results.append(
                    BulkResult(path=path, out_path=target_dir / f"{name}_p{page_number}.json", ok=False, error=str(exc))
                )
                if verbose:
                    print(f"[bulk] ERROR {path} p{page_number}: {exc}")
                break

    return results
