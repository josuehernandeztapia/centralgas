"""
HTTP Basic Auth for the dashboard + data endpoints.

HU-DASH-X.1 — Phase 1 auth:
  - Single admin credential.
  - User/password read from env (Fly secrets).
  - Constant-time comparison to prevent timing attacks.
  - Applied to: /dashboard, /upload-excel, /transactions, /stats,
                /sobreprecio/distribution.
  - Public: /health, / (service info).

Env vars:
  DASHBOARD_USER          default "admin"
  DASHBOARD_PASSWORD      required (plain text; stored in Fly secret which is encrypted at rest)
  DASHBOARD_AUTH_DISABLED set to "1" to disable (local dev only — NEVER in prod)

Set via:
  fly secrets set DASHBOARD_USER=admin DASHBOARD_PASSWORD='<strong-pass>' --app central-gas-agent
"""

from __future__ import annotations

import logging
import os
import secrets
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

logger = logging.getLogger("auth")

_security = HTTPBasic(realm="Central Gas Dashboard")


def _creds_configured() -> Optional[tuple[str, str]]:
    """Return (user, password) if configured, else None."""
    user = os.environ.get("DASHBOARD_USER", "admin").strip()
    password = os.environ.get("DASHBOARD_PASSWORD", "").strip()
    if not password:
        return None
    return (user, password)


def require_auth(credentials: HTTPBasicCredentials = Depends(_security)) -> str:
    """
    FastAPI dependency — raises 401 unless Basic-auth credentials match the
    configured DASHBOARD_USER / DASHBOARD_PASSWORD.

    Returns the authenticated username (useful for audit logs later).
    """
    # Escape hatch for local dev ONLY.
    if os.environ.get("DASHBOARD_AUTH_DISABLED") == "1":
        return "dev"

    configured = _creds_configured()
    if configured is None:
        # Fail closed: if no password is set in prod, refuse access instead
        # of letting the dashboard be accidentally public.
        logger.error("DASHBOARD_PASSWORD not set — refusing all requests")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Dashboard auth not configured. Set DASHBOARD_PASSWORD env var.",
        )

    exp_user, exp_pass = configured
    got_user = (credentials.username or "").encode("utf-8")
    got_pass = (credentials.password or "").encode("utf-8")
    ok_user = secrets.compare_digest(got_user, exp_user.encode("utf-8"))
    ok_pass = secrets.compare_digest(got_pass, exp_pass.encode("utf-8"))

    if not (ok_user and ok_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": 'Basic realm="Central Gas Dashboard"'},
        )
    return credentials.username
