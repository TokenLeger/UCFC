from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_DIR = REPO_ROOT / "data_fiscale" / "raw" / "legifrance"

SANDBOX_TOKEN_URL = "https://sandbox-oauth.piste.gouv.fr/api/oauth/token"
SANDBOX_API_BASE = "https://sandbox-api.piste.gouv.fr/dila/legifrance/lf-engine-app"
DEFAULT_SCOPE = "openid"


@dataclass
class PisteConfig:
    token_url: str
    api_base: str
    client_id: str
    client_secret: str
    scope: Optional[str] = None


class PisteClient:
    def __init__(self, config: PisteConfig) -> None:
        self.config = config
        self._token: Optional[str] = None
        self._token_expiry: Optional[float] = None

    def _token_valid(self) -> bool:
        return self._token is not None and self._token_expiry is not None and time.time() < self._token_expiry

    def _fetch_token(self) -> str:
        data = {
            "grant_type": "client_credentials",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
        }
        if self.config.scope:
            data["scope"] = self.config.scope

        payload = urllib.parse.urlencode(data).encode("utf-8")
        req = urllib.request.Request(self.config.token_url, data=payload, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")

        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8")
        token_data = json.loads(body)

        access_token = token_data.get("access_token")
        if not access_token:
            raise RuntimeError("PISTE token response missing access_token")

        expires_in = int(token_data.get("expires_in", 3600))
        self._token = access_token
        self._token_expiry = time.time() + max(60, expires_in - 60)
        return access_token

    def get_token(self) -> str:
        if self._token_valid():
            return self._token  # type: ignore[return-value]
        return self._fetch_token()

    def request_json(
        self,
        path: str,
        params: Optional[dict] = None,
        method: str = "GET",
        body: Optional[dict] = None,
        verbose: bool = False,
    ) -> dict:
        token = self.get_token()
        base = self.config.api_base.rstrip("/")
        url = f"{base}/{path.lstrip('/')}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"

        method = method.upper().strip()
        if verbose:
            print(f"[piste] {method} {path}")
        data = None
        if method in {"POST", "PUT", "PATCH"}:
            if body is not None:
                data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            else:
                data = b""

        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/json")
        if method in {"POST", "PUT", "PATCH"}:
            req.add_header("Content-Type", "application/json")

        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8")
        return json.loads(body)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def save_json(payload: dict, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def env_config() -> PisteConfig:
    env = os.getenv("PISTE_ENV", "sandbox").strip().lower()
    token_url = os.getenv("PISTE_TOKEN_URL", "").strip()
    api_base = os.getenv("PISTE_API_BASE", "").strip()
    client_id = os.getenv("PISTE_CLIENT_ID", "").strip()
    client_secret = os.getenv("PISTE_CLIENT_SECRET", "").strip()
    scope = os.getenv("PISTE_SCOPE", DEFAULT_SCOPE).strip() or None

    if not token_url or not api_base:
        if env == "sandbox":
            token_url = SANDBOX_TOKEN_URL
            api_base = SANDBOX_API_BASE

    if not all([token_url, api_base, client_id, client_secret]):
        raise RuntimeError(
            "Missing PISTE config. Set PISTE_CLIENT_ID and PISTE_CLIENT_SECRET, "
            "and optionally PISTE_ENV=sandbox (default) or define PISTE_TOKEN_URL and PISTE_API_BASE."
        )

    return PisteConfig(
        token_url=token_url,
        api_base=api_base,
        client_id=client_id,
        client_secret=client_secret,
        scope=scope,
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch Legifrance via PISTE API.")
    parser.add_argument("--path", help="API path under PISTE base (e.g. /legifrance/...)")
    parser.add_argument("--out", default=str(DEFAULT_RAW_DIR), help="Output folder")
    parser.add_argument("--name", default="legifrance_payload", help="Output filename (without extension)")
    parser.add_argument("--param", action="append", default=[], help="Query param key=value")
    parser.add_argument("--method", default="GET", help="HTTP method (GET/POST/PUT/PATCH/DELETE)")
    parser.add_argument("--body", default="", help="JSON body string")
    parser.add_argument("--body-file", default="", help="Path to JSON body file")
    parser.add_argument("--verbose", action="store_true", help="Verbose progress logs")
    args = parser.parse_args(argv)

    if not args.path:
        raise SystemExit("--path is required (PISTE API path)")

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

    client = PisteClient(env_config())
    payload = client.request_json(
        args.path,
        params=params or None,
        method=args.method,
        body=body,
        verbose=args.verbose,
    )

    out_dir = Path(args.out)
    out_path = out_dir / f"{args.name}_{datetime.now().date().isoformat()}.json"
    save_json(payload, out_path)

    print(f"Saved: {out_path} (ts={_utc_now_iso()})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
