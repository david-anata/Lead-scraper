"""Authenticated probe that shows whether a Haiku call used the AI Gateway."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Request

from sales_support_agent.services.llm_client import llm_ping

router = APIRouter()


@router.get("/api/admin/llm-ping")
def admin_llm_ping(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict:
    """One Haiku call. Auth is the internal API key. No database or Instantly work."""

    configured = str(getattr(request.app.state.settings, "internal_api_key", "") or "")
    if configured and (x_internal_api_key or "") != configured:
        raise HTTPException(status_code=401, detail="Invalid internal API key.")
    return llm_ping()
