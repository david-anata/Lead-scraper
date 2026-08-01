"""Validation for URLs accepted by public website-to-Agent workflows."""

from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlparse


def _public_address(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    return address.is_global


def public_http_url(value: str) -> str:
    """Return a normalized public HTTP(S) URL, or ``""`` when unsafe.

    Hostnames must resolve exclusively to globally routable addresses. This
    keeps public Rate Sheet requests away from loopback, link-local, private,
    multicast, reserved, and metadata-service targets.
    """

    raw = str(value or "").strip()
    if not raw:
        return ""
    candidate = raw if "://" in raw else f"https://{raw}"
    try:
        parsed = urlparse(candidate)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
        ):
            return ""
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        try:
            literal = ipaddress.ip_address(parsed.hostname)
        except ValueError:
            addresses = {
                item[4][0]
                for item in socket.getaddrinfo(
                    parsed.hostname,
                    port,
                    type=socket.SOCK_STREAM,
                )
            }
        else:
            addresses = {str(literal)}
    except (OSError, ValueError):
        return ""
    if not addresses or not all(_public_address(address) for address in addresses):
        return ""
    return parsed.geturl()
