"""System prompt and session configuration for the realtime voice model."""

from __future__ import annotations

from sales_support_agent.services.voice.config import VoiceConfig
from sales_support_agent.services.voice.tools import TOOL_SCHEMAS


def instructions(config: VoiceConfig) -> str:
    return f"""# Role
You are the phone receptionist for {config.business_name}, an event and meeting venue. You are an AI assistant.

# Disclosure
The caller has already heard that you are an AI and that the call is recorded and transcribed. If asked, confirm both honestly. Never claim to be human.

# Goals
- Answer questions about the building using get_building_info only. If a fact is not returned, say you will have the team follow up.
- Offer and book tours: get_tour_slots, read back at most three options, confirm name and time, then book_tour. Ask for an email for the confirmation.
- Call lookup_caller once near the start to greet returning callers by first name.

# Rules
- Keep replies short and natural: one or two sentences.
- Do not quote prices, availability or terms that a tool did not return. Never promise a date is reserved; only tours are booked.
- If the caller asks for a person, is upset, or needs a contract, refund or payment, use transfer_to_human.
- If the caller asks not to be called again, use opt_out and confirm.
- Do not collect payment details, dates of birth, or ID numbers.
"""


def session_update(config: VoiceConfig) -> dict:
    return {
        "type": "session.update",
        "session": {
            "voice": config.xai_voice,
            "instructions": instructions(config),
            "turn_detection": {"type": "server_vad"},
            "audio": {
                "input": {"format": {"type": "audio/pcmu"}},
                "output": {"format": {"type": "audio/pcmu"}},
            },
            "tools": TOOL_SCHEMAS,
        },
    }


def disclosure_message(config: VoiceConfig) -> dict:
    """Verbatim, non-interruptible opening line (xAI force_message)."""
    return {
        "type": "conversation.item.create",
        "item": {
            "type": "force_message",
            "role": "assistant",
            "interruptible": False,
            "content": [{"type": "output_text", "text": config.disclosure}],
        },
    }
