"""Utilities for sealing OAuth tokens before storing them locally."""

from __future__ import annotations

import base64
import hashlib
import hmac
import os


def seal_token(secret: str, plaintext: str) -> str:
    if not plaintext:
        return ""
    if not secret:
        raise RuntimeError("A token secret is required to seal tokens.")

    salt = os.urandom(16)
    nonce = os.urandom(16)
    key = hashlib.pbkdf2_hmac("sha256", secret.encode("utf-8"), salt, 120_000, dklen=32)
    payload = plaintext.encode("utf-8")
    ciphertext = _xor_keystream(payload, key=key, nonce=nonce)
    mac = hmac.new(key, nonce + ciphertext, hashlib.sha256).digest()
    token = b"|".join(
        [
            b"v1",
            base64.urlsafe_b64encode(salt),
            base64.urlsafe_b64encode(nonce),
            base64.urlsafe_b64encode(ciphertext),
            base64.urlsafe_b64encode(mac),
        ]
    )
    return token.decode("utf-8")


def unseal_token(secret: str, sealed: str) -> str:
    if not sealed:
        return ""
    if not secret:
        raise RuntimeError("A token secret is required to unseal tokens.")

    try:
        version, salt_b64, nonce_b64, ciphertext_b64, mac_b64 = sealed.encode("utf-8").split(b"|", 4)
        if version != b"v1":
            raise ValueError("Unsupported token version.")
        salt = base64.urlsafe_b64decode(salt_b64)
        nonce = base64.urlsafe_b64decode(nonce_b64)
        ciphertext = base64.urlsafe_b64decode(ciphertext_b64)
        provided_mac = base64.urlsafe_b64decode(mac_b64)
    except Exception as exc:
        raise RuntimeError("Stored token could not be decoded.") from exc

    key = hashlib.pbkdf2_hmac("sha256", secret.encode("utf-8"), salt, 120_000, dklen=32)
    expected_mac = hmac.new(key, nonce + ciphertext, hashlib.sha256).digest()
    if not hmac.compare_digest(provided_mac, expected_mac):
        raise RuntimeError("Stored token failed verification.")

    plaintext = _xor_keystream(ciphertext, key=key, nonce=nonce)
    return plaintext.decode("utf-8")


def _xor_keystream(payload: bytes, *, key: bytes, nonce: bytes) -> bytes:
    blocks: list[bytes] = []
    counter = 0
    while sum(len(block) for block in blocks) < len(payload):
        blocks.append(
            hmac.new(key, nonce + counter.to_bytes(4, "big"), hashlib.sha256).digest()
        )
        counter += 1
    keystream = b"".join(blocks)[: len(payload)]
    return bytes(left ^ right for left, right in zip(payload, keystream))
