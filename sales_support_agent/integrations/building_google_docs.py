"""Google Doc contract drafts for approved Building agreements.

Copies an approved template Doc and fills its ``{{placeholders}}`` from the
frozen agreement package. Purpose-limited in the same way as the QuickBooks
client: it creates a draft and stops. It never emails anyone, never requests a
signature, and never touches the signature block — placeholders are replaced in
place, so a Google eSignature field in the template survives untouched.

Sending the draft and requesting the signature stay deliberate operator steps in
Google Docs.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Optional

DRIVE_API = "https://www.googleapis.com/drive/v3"
DOCS_API = "https://docs.googleapis.com/v1"
#: Drive scope is needed to copy into a shared drive; Docs scope to fill it in.
SCOPES = (
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
)
PLACEHOLDER_RE = re.compile(r"\{\{\s*([a-z0-9_]+)\s*\}\}")


class BuildingGoogleDocsError(RuntimeError):
    """Raised when a contract draft cannot be produced."""


class BuildingContractDocsClient:
    """Creates filled contract drafts. Never sends and never signs."""

    def __init__(
        self,
        *,
        service_account_json: Optional[str] = None,
        template_document_id: Optional[str] = None,
        drive_folder_id: Optional[str] = None,
        delegated_subject: Optional[str] = None,
    ) -> None:
        self.service_account_json = (
            service_account_json
            if service_account_json is not None
            else os.getenv("BUILDING_CONTRACT_GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
            or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
        )
        self.template_document_id = (
            template_document_id
            if template_document_id is not None
            else os.getenv("BUILDING_CONTRACT_TEMPLATE_DOC_ID", "").strip()
        )
        self.drive_folder_id = (
            drive_folder_id
            if drive_folder_id is not None
            else os.getenv("BUILDING_CONTRACT_DRIVE_FOLDER_ID", "").strip()
        )
        self.delegated_subject = (
            delegated_subject
            if delegated_subject is not None
            else os.getenv("BUILDING_CONTRACT_GOOGLE_SUBJECT", "").strip()
        )
        self._session: Any = None

    @property
    def readiness_error(self) -> str:
        """Explain exactly what is missing, so the page can say so plainly."""

        if not self.service_account_json:
            return "Google service-account credentials are missing."
        if not self.template_document_id:
            return (
                "No contract template Doc is configured. Set "
                "BUILDING_CONTRACT_TEMPLATE_DOC_ID and share that Doc with the "
                "service account."
            )
        if not self.drive_folder_id:
            return (
                "No Drive folder is configured for generated contracts. Set "
                "BUILDING_CONTRACT_DRIVE_FOLDER_ID to a shared drive folder the "
                "service account can write to."
            )
        return ""

    @property
    def configured(self) -> bool:
        return not self.readiness_error

    def _authorized_session(self) -> Any:
        if not self.configured:
            raise BuildingGoogleDocsError(self.readiness_error)
        if self._session is None:
            try:
                from google.auth.transport.requests import AuthorizedSession
                from google.oauth2 import service_account
            except ModuleNotFoundError as exc:  # pragma: no cover - dependency guard
                raise BuildingGoogleDocsError(
                    "google-auth is required to create contract drafts."
                ) from exc
            try:
                info = json.loads(self.service_account_json)
            except json.JSONDecodeError as exc:
                raise BuildingGoogleDocsError(
                    "The contract service-account JSON is not valid JSON."
                ) from exc
            credentials = service_account.Credentials.from_service_account_info(
                info, scopes=list(SCOPES)
            )
            if self.delegated_subject:
                credentials = credentials.with_subject(self.delegated_subject)
            self._session = AuthorizedSession(credentials)
        return self._session

    def _check(self, response: Any, action: str) -> dict[str, Any]:
        if response.status_code >= 400:
            raise BuildingGoogleDocsError(
                f"Google refused to {action} ({response.status_code}): "
                f"{response.text[:300]}"
            )
        return response.json() if response.content else {}

    @property
    def service_account_email(self) -> str:
        """The identity that must be shared on the template and folder.

        An address, not a secret. Surfacing it turns "permission denied" into
        "share with this account", which is the actual fix.
        """

        try:
            return str(json.loads(self.service_account_json).get("client_email") or "")
        except (json.JSONDecodeError, TypeError):
            return ""

    def preflight(self) -> dict[str, Any]:
        """Check the template and folder are actually reachable.

        Configuration being present is not the same as access being granted, and
        a service account starts with access to nothing. Reports which of the two
        is missing rather than failing later inside a copy.
        """

        report: dict[str, Any] = {
            "configured": self.configured,
            "service_account_email": self.service_account_email,
            "template_readable": False,
            "folder_writable": False,
            "problems": [],
        }
        if not self.configured:
            report["problems"].append(self.readiness_error)
            return report
        session = self._authorized_session()
        template = session.get(
            f"{DRIVE_API}/files/{self.template_document_id}",
            params={"fields": "id,name", "supportsAllDrives": "true"},
            timeout=20,
        )
        if template.status_code < 400:
            report["template_readable"] = True
        else:
            report["problems"].append(
                f"The template Doc is not shared with {self.service_account_email or 'the service account'} "
                "(Viewer is enough)."
            )
        folder = session.get(
            f"{DRIVE_API}/files/{self.drive_folder_id}",
            params={"fields": "id,name,capabilities/canAddChildren",
                    "supportsAllDrives": "true"},
            timeout=20,
        )
        if folder.status_code < 400:
            if (folder.json().get("capabilities") or {}).get("canAddChildren"):
                report["folder_writable"] = True
            else:
                report["problems"].append(
                    f"{self.service_account_email or 'The service account'} can see the "
                    "contracts folder but cannot add files to it. Give it Editor."
                )
        else:
            report["problems"].append(
                f"The contracts folder is not shared with {self.service_account_email or 'the service account'} "
                "(Editor is required)."
            )
        return report

    def template_placeholders(self) -> list[str]:
        """Return the placeholder names the template actually uses.

        Lets the workspace warn about a placeholder the booking cannot fill
        before a draft is made, rather than shipping a contract with a gap in it.
        """

        session = self._authorized_session()
        data = self._check(
            session.get(
                f"{DOCS_API}/documents/{self.template_document_id}", timeout=20
            ),
            "read the contract template",
        )
        found: list[str] = []
        for element in _iter_text(data.get("body", {}).get("content", [])):
            for match in PLACEHOLDER_RE.finditer(element):
                if match.group(1) not in found:
                    found.append(match.group(1))
        return found

    def create_contract_draft(
        self,
        *,
        title: str,
        values: dict[str, Any],
    ) -> dict[str, str]:
        """Copy the template, fill its placeholders, and return the draft link.

        The copy keeps every element of the template, including the signature
        block, because only placeholder text is rewritten.
        """

        session = self._authorized_session()
        copied = self._check(
            session.post(
                f"{DRIVE_API}/files/{self.template_document_id}/copy",
                params={"supportsAllDrives": "true"},
                json={"name": title, "parents": [self.drive_folder_id]},
                timeout=30,
            ),
            "copy the contract template",
        )
        document_id = str(copied.get("id") or "")
        if not document_id:
            raise BuildingGoogleDocsError("Google returned no document ID for the copy.")

        requests_payload = [
            {
                "replaceAllText": {
                    "containsText": {"text": "{{" + key + "}}", "matchCase": True},
                    "replaceText": "" if value is None else str(value),
                }
            }
            for key, value in sorted(values.items())
        ]
        if requests_payload:
            self._check(
                session.post(
                    f"{DOCS_API}/documents/{document_id}:batchUpdate",
                    json={"requests": requests_payload},
                    timeout=30,
                ),
                "fill the contract draft",
            )
        return {
            "document_id": document_id,
            "document_url": f"https://docs.google.com/document/d/{document_id}/edit",
        }


def _iter_text(content: list[dict[str, Any]]) -> list[str]:
    """Yield every text run in a Docs body, including inside tables."""

    out: list[str] = []
    for element in content or []:
        paragraph = element.get("paragraph")
        if paragraph:
            for run in paragraph.get("elements", []):
                text = (run.get("textRun") or {}).get("content")
                if text:
                    out.append(text)
        table = element.get("table")
        if table:
            for row in table.get("tableRows", []):
                for cell in row.get("tableCells", []):
                    out.extend(_iter_text(cell.get("content", [])))
    return out
