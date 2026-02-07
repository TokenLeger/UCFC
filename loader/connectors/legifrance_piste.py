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
SANDBOX_AUTH_URL = "https://sandbox-oauth.piste.gouv.fr/api/oauth/authorize"
SANDBOX_API_BASE = "https://sandbox-api.piste.gouv.fr/dila/legifrance/lf-engine-app"
DEFAULT_SCOPE = "openid"
DEFAULT_AUTH_FLOW = "client_credentials"
DEFAULT_TOKEN_CACHE = REPO_ROOT / "data_fiscale" / "auth" / "piste_token.json"


@dataclass
class PisteConfig:
    token_url: str
    api_base: str
    client_id: str
    client_secret: str
    scope: Optional[str] = None
    api_key: Optional[str] = None
    api_key_header: str = "KeyId"
    auth_url: str = SANDBOX_AUTH_URL
    auth_flow: str = DEFAULT_AUTH_FLOW
    redirect_uri: Optional[str] = None
    token_cache: Optional[Path] = None
    access_token: Optional[str] = None


class PisteClient:
    def __init__(self, config: PisteConfig) -> None:
        self.config = config
        self._token: Optional[str] = None
        self._token_expiry: Optional[float] = None

    def _token_valid(self) -> bool:
        return self._token is not None and self._token_expiry is not None and time.time() < self._token_expiry

    def _fetch_token_client_credentials(self) -> str:
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

    def _load_access_token(self) -> Optional[str]:
        if self.config.access_token:
            self._token = self.config.access_token
            self._token_expiry = time.time() + 3600
            return self._token
        cache = self.config.token_cache
        if cache and cache.exists():
            try:
                data = json.loads(cache.read_text(encoding="utf-8"))
            except Exception:
                return None
            token = data.get("access_token")
            expires_at = data.get("expires_at")
            if not token:
                return None
            if isinstance(expires_at, (int, float)) and time.time() >= float(expires_at):
                return None
            self._token = token
            if isinstance(expires_at, (int, float)):
                self._token_expiry = float(expires_at)
            else:
                self._token_expiry = time.time() + 3600
            return self._token
        return None

    def get_token(self) -> str:
        if self._token_valid():
            return self._token  # type: ignore[return-value]
        if self.config.auth_flow in {"access_code", "authorization_code", "auth_code", "code"}:
            token = self._load_access_token()
            if token:
                return token
            raise RuntimeError(
                "Missing access token for accessCode flow. "
                "Run: pfc_cli.py legifrance-auth --redirect-uri <uri> (then --code <code>)."
            )
        return self._fetch_token_client_credentials()

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
        if self.config.api_key:
            req.add_header(self.config.api_key_header, self.config.api_key)
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


def build_authorize_url(config: PisteConfig, state: Optional[str] = None) -> str:
    if not config.redirect_uri:
        raise RuntimeError("Missing redirect URI for accessCode flow.")
    params = {
        "response_type": "code",
        "client_id": config.client_id,
        "redirect_uri": config.redirect_uri,
    }
    if config.scope:
        params["scope"] = config.scope
    if state:
        params["state"] = state
    return f"{config.auth_url}?{urllib.parse.urlencode(params)}"


def exchange_code_for_token(config: PisteConfig, code: str) -> dict:
    if not config.redirect_uri:
        raise RuntimeError("Missing redirect URI for accessCode flow.")
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": config.client_id,
        "client_secret": config.client_secret,
        "redirect_uri": config.redirect_uri,
    }
    if config.scope:
        data["scope"] = config.scope
    payload = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(config.token_url, data=payload, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode("utf-8")
    token_data = json.loads(body)

    access_token = token_data.get("access_token")
    if not access_token:
        raise RuntimeError("Token exchange missing access_token")
    expires_in = int(token_data.get("expires_in", 3600))
    token_data["expires_at"] = time.time() + max(60, expires_in - 60)
    token_data["obtained_at"] = _utc_now_iso()
    if config.token_cache:
        config.token_cache.parent.mkdir(parents=True, exist_ok=True)
        config.token_cache.write_text(json.dumps(token_data, ensure_ascii=False, indent=2), encoding="utf-8")
    return token_data


def env_config() -> PisteConfig:
    env = os.getenv("PISTE_ENV", "sandbox").strip().lower()
    token_url = os.getenv("PISTE_TOKEN_URL", "").strip()
    api_base = os.getenv("PISTE_API_BASE", "").strip()
    client_id = os.getenv("PISTE_CLIENT_ID", "").strip()
    client_secret = os.getenv("PISTE_CLIENT_SECRET", "").strip()
    scope = os.getenv("PISTE_SCOPE", DEFAULT_SCOPE).strip() or None
    api_key = os.getenv("PISTE_API_KEY", "").strip() or None
    api_key_header = os.getenv("PISTE_API_KEY_HEADER", "KeyId").strip() or "KeyId"
    auth_url = os.getenv("PISTE_AUTH_URL", "").strip()
    auth_flow = os.getenv("PISTE_AUTH_FLOW", DEFAULT_AUTH_FLOW).strip().lower() or DEFAULT_AUTH_FLOW
    redirect_uri = os.getenv("PISTE_REDIRECT_URI", "").strip() or None
    token_cache = os.getenv("PISTE_TOKEN_CACHE", "").strip()
    access_token = os.getenv("PISTE_ACCESS_TOKEN", "").strip() or None

    if not token_url or not api_base:
        if env == "sandbox":
            token_url = SANDBOX_TOKEN_URL
            api_base = SANDBOX_API_BASE
            if not auth_url:
                auth_url = SANDBOX_AUTH_URL
    if not auth_url:
        auth_url = SANDBOX_AUTH_URL
    if not token_cache:
        token_cache = str(DEFAULT_TOKEN_CACHE)

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
        api_key=api_key,
        api_key_header=api_key_header,
        auth_url=auth_url,
        auth_flow=auth_flow,
        redirect_uri=redirect_uri,
        token_cache=Path(token_cache) if token_cache else None,
        access_token=access_token,
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
