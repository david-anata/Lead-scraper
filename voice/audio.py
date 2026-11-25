"""G.711 µ-law framing.

Twilio Media Streams and the xAI realtime API both speak raw `audio/pcmu`
at 8 kHz. Payloads stay base64. Outbound audio is cut into 20 ms frames
(160 bytes), which is what Twilio plays smoothly.
"""

from __future__ import annotations

import base64


FRAME_BYTES = 160


def mulaw_frames(payload_b64: str, frame_bytes: int = FRAME_BYTES) -> list[str]:
    if not payload_b64:
        return []
    try:
        raw = base64.b64decode(payload_b64, validate=False)
    except Exception:
        return []
    frames: list[str] = []
    for index in range(0, len(raw), frame_bytes):
        chunk = raw[index : index + frame_bytes]
        if chunk:
            frames.append(base64.b64encode(chunk).decode("ascii"))
    return frames
