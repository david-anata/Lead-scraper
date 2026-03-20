"""Thin Canva Connect API client for deck generation."""

from __future__ import annotations

import base64
from hashlib import sha256
from typing import Any
from urllib.parse import urlencode

import requests

from sales_support_agent.config import Settings


class CanvaClient:
    def __init__(self, settings: Settings):
        self.settings = settings

    def is_configured(self) -> bool:
        return bool(
            self.settings.canva_client_id
            and self.settings.canva_client_secret
            and self.settings.canva_redirect_uri
            and self.settings.canva_brand_template_id
        )

    def build_authorize_url(self, *, state: str, code_verifier: str) -> str:
        challenge = base64.urlsafe_b64encode(sha256(code_verifier.encode("utf-8")).digest()).decode("utf-8").rstrip("=")
        query = urlencode(
            {
                "client_id": self.settings.canva_client_id,
                "redirect_uri": self.settings.canva_redirect_uri,
                "response_type": "code",
                "scope": " ".join(self.settings.canva_scopes),
                "state": state,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
            }
        )
        return f"{self.settings.canva_authorize_url}?{query}"

    def exchange_code(self, *, code: str, code_verifier: str) -> dict[str, Any]:
        response = requests.post(
            self.settings.canva_token_url,
            auth=(self.settings.canva_client_id, self.settings.canva_client_secret),
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self.settings.canva_redirect_uri,
                "code_verifier": code_verifier,
            },
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(f"Canva token exchange failed ({response.status_code}): {response.text}")
        return response.json()

    def refresh_access_token(self, refresh_token: str) -> dict[str, Any]:
        response = requests.post(
            self.settings.canva_token_url,
            auth=(self.settings.canva_client_id, self.settings.canva_client_secret),
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(f"Canva token refresh failed ({response.status_code}): {response.text}")
        return response.json()

    def get_user_capabilities(self, access_token: str) -> dict[str, Any]:
        return self._request("GET", "users/me/capabilities", access_token=access_token)

    def get_brand_template_dataset(self, brand_template_id: str, access_token: str) -> dict[str, Any]:
        return self._request("GET", f"brand-templates/{brand_template_id}/dataset", access_token=access_token)

    def create_autofill_job(
        self,
        *,
        access_token: str,
        brand_template_id: str,
        title: str,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "autofills",
            access_token=access_token,
            json_body={
                "brand_template_id": brand_template_id,
                "title": title,
                "data": data,
            },
        )

    def get_autofill_job(self, job_id: str, access_token: str) -> dict[str, Any]:
        return self._request("GET", f"autofills/{job_id}", access_token=access_token)

    def get_design(self, design_id: str, access_token: str) -> dict[str, Any]:
        return self._request("GET", f"designs/{design_id}", access_token=access_token)

    def _request(
        self,
        method: str,
        path: str,
        *,
        access_token: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = requests.request(
            method=method,
            url=f"{self.settings.canva_api_base_url.rstrip('/')}/{path.lstrip('/')}",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            params=params,
            json=json_body,
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(f"Canva API request failed ({response.status_code}) for {path}: {response.text}")
        if not response.content:
            return {}
        return response.json()
