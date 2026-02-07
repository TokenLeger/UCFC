from __future__ import annotations

import hashlib
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = REPO_ROOT / "data_fiscale" / "usage_log.sqlite3"
STORE_QUERY_TEXT = os.getenv("USAGE_LOG_STORE_QUERY_TEXT", "false").lower() == "true"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS access_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_utc TEXT NOT NULL,
  user_name TEXT NOT NULL,
  client_ip TEXT NOT NULL,
  agent_name TEXT NOT NULL,
  action TEXT NOT NULL,
  request_id TEXT,
  session_id TEXT,
  user_agent TEXT,
  resource TEXT,
  query_text TEXT,
  query_hash TEXT,
  notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_access_log_ts ON access_log (ts_utc);
CREATE INDEX IF NOT EXISTS idx_access_log_user ON access_log (user_name);
CREATE INDEX IF NOT EXISTS idx_access_log_ip ON access_log (client_ip);
CREATE INDEX IF NOT EXISTS idx_access_log_agent ON access_log (agent_name);
"""


@dataclass
class AccessEvent:
    user_name: str
    client_ip: str
    agent_name: str
    action: str
    request_id: Optional[str] = None
    session_id: Optional[str] = None
    user_agent: Optional[str] = None
    resource: Optional[str] = None
    query_text: Optional[str] = None
    notes: Optional[str] = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sanitize(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    return " ".join(text.split())


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.commit()


def log_access(event: AccessEvent, db_path: Path = DEFAULT_DB_PATH) -> None:
    """
    Log an access event. Sensitive business data must NOT be passed here.
    By default, query text is NOT stored; only a hash is stored unless
    USAGE_LOG_STORE_QUERY_TEXT=true is set.
    """
    init_db(db_path)

    query_text = _sanitize(event.query_text)
    query_hash = _sha256(query_text) if query_text else None
    if not STORE_QUERY_TEXT:
        query_text = None

    payload = (
        _utc_now_iso(),
        _sanitize(event.user_name),
        _sanitize(event.client_ip),
        _sanitize(event.agent_name),
        _sanitize(event.action),
        _sanitize(event.request_id),
        _sanitize(event.session_id),
        _sanitize(event.user_agent),
        _sanitize(event.resource),
        query_text,
        query_hash,
        _sanitize(event.notes),
    )

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO access_log (
              ts_utc, user_name, client_ip, agent_name, action,
              request_id, session_id, user_agent, resource, query_text, query_hash, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        conn.commit()
