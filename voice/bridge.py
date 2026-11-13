"""Bridge one Twilio Media Stream to one xAI realtime session.

Audio is passed through as G.711 µ-law. When the caller starts talking, the
Twilio playback buffer is cleared so the model can be interrupted. A transfer
parks the caller in a conference with hold music and dials the on-call phone
on its own leg. That socket drops when the call is updated. If the on-call
phone does not press 1, Twilio brings the caller back and a new socket resumes
the assistant.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
from typing import Any
from urllib.parse import quote

from voice.audio import mulaw_frames
from voice.calendar_availability import CalendarAvailability
from voice.known_callers import known_greeting
from voice.lifecycle import begin_transfer, finalize_call
from voice.screening import DAVID_CALLBACK_LINE
from voice.lines import parse_known_numbers
from voice.prompts import build_instructions
from voice.session import CallSession
from voice.settings import Settings
from voice.tools import execute_tool, session_update_event
from voice.twiml import RING_TIMEOUT_SECONDS, apology_twiml, caller_hold_twiml, resume_twiml
from voice.notify import TwilioRest
from voice.leads import SupabaseStore
from voice.screening import one_line

logger = logging.getLogger(__name__)


class CallBridge:
    def __init__(
        self,
        *,
        settings: Settings,
        session: CallSession,
        store: SupabaseStore,
        twilio: TwilioRest,
        calendar: CalendarAvailability,
        websocket: Any,
    ) -> None:
        self.settings = settings
        self.session = session
        self.store = store
        self.twilio = twilio
        self.calendar = calendar
        self.websocket = websocket
        self.stream_sid = ""
        self.xai: Any = None
        self.known_numbers = parse_known_numbers(settings.known_caller_numbers)
        self._ready = asyncio.Event()
        self._stop = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._dial_lock = asyncio.Lock()
        self._audio_gen = 0
        self._greeted = False
        self._inbound: list[str] = []
        self._pending_tools = 0
        self._tools_ready = 0
        self._response_done = False
        self._continuing = False

    async def run(self, start_message: dict[str, Any] | None = None) -> None:
        if self.session.resume == "message":
            # The hold is over. This socket is the message-taking leg, so it
            # may write the call log when it closes.
            self.session.holding_for_transfer = False
            if self.session.transfer_state == "dialing":
                self.session.transfer_state = "messaging"
        if start_message:
            await self._on_twilio(start_message)
        xai_task = asyncio.create_task(self._xai_loop())
        limit_task = asyncio.create_task(self._max_duration())
        try:
            while not self._stop.is_set():
                incoming = await self.websocket.receive()
                if incoming.get("type") == "websocket.disconnect":
                    break
                raw = incoming.get("text")
                if not raw:
                    continue
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                await self._on_twilio(message)
        except Exception as exc:
            if exc.__class__.__name__ != "WebSocketDisconnect":
                logger.exception("Twilio media socket closed with an error")
        finally:
            self._stop.set()
            xai_task.cancel()
            limit_task.cancel()
            await asyncio.gather(xai_task, limit_task, return_exceptions=True)
            await finalize_call(
                self.session,
                settings=self.settings,
                store=self.store,
                twilio=self.twilio,
            )

    async def _on_twilio(self, message: dict[str, Any]) -> None:
        event = message.get("event")
        if event == "start":
            start = message.get("start") or {}
            self.stream_sid = str(start.get("streamSid") or "")
        elif event == "media":
            payload = str((message.get("media") or {}).get("payload") or "")
            if payload:
                await self._forward_caller_audio(payload)
        elif event == "stop":
            self._stop.set()

    async def _forward_caller_audio(self, payload: str) -> None:
        if self.xai is None or not self._ready.is_set():
            self._inbound.append(payload)
            return
        await self._xai_send({"type": "input_audio_buffer.append", "audio": payload})

    async def _xai_loop(self) -> None:
        try:
            async with _connect_xai(self.settings) as ws:
                self.xai = ws
                await self._xai_send(
                    session_update_event(self.settings, build_instructions(self.session))
                )
                asyncio.create_task(self._ready_fallback())
                async for raw in ws:
                    if self._stop.is_set():
                        break
                    if isinstance(raw, bytes):
                        continue
                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    await self._on_xai(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("xAI realtime session failed")
            if not self.session.transfer_started:
                await asyncio.to_thread(
                    self.twilio.update_call, self.session.call_sid, apology_twiml()
                )
        finally:
            self._stop.set()

    async def _on_xai(self, event: dict[str, Any]) -> None:
        kind = str(event.get("type") or "")
        if kind == "session.updated" and not self._greeted:
            await self._greet()
        elif kind in {"response.output_audio.delta", "response.audio.delta"}:
            await self._play(str(event.get("delta") or ""))
        elif kind == "input_audio_buffer.speech_started":
            await self._barge_in()
        elif kind == "response.output_audio_transcript.done":
            text = str(event.get("transcript") or "")
            if text:
                self.session.add_turn("Anata", text)
        elif kind == "conversation.item.input_audio_transcription.updated":
            item_id = str(event.get("item_id") or "caller")
            self.session.caller_partials[item_id] = str(event.get("transcript") or "")
        elif kind == "conversation.item.input_audio_transcription.completed":
            text = str(event.get("transcript") or "")
            if text:
                self.session.add_turn("Caller", text)
                self.session.caller_partials.pop(str(event.get("item_id") or ""), None)
        elif kind == "response.function_call_arguments.done":
            await self._on_function(event)
        elif kind == "response.done":
            self._response_done = True
            if self._tools_ready:
                await self._continue_after_tools()
            elif self.session.transfer_requested and not self.session.transfer_started:
                asyncio.create_task(self._dial_after_pause())
        elif kind == "error":
            logger.warning("xAI returned an error event type")

    async def _greet(self) -> None:
        if self._greeted or self.xai is None:
            return
        self._greeted = True
        if self.session.resume == "message":
            text = missed_transfer_line(self.session)
        else:
            text = known_greeting(self.session) or self.session.line.spoken_greeting()
        if self.session.ring_policy == "always" and self.session.resume != "message":
            asyncio.create_task(self._ring_known_caller())
        await self._xai_send(
            {
                "type": "conversation.item.create",
                "item": {
                    "type": "force_message",
                    "role": "assistant",
                    "interruptible": True,
                    "content": [{"type": "output_text", "text": text}],
                },
            }
        )
        self._ready.set()
        await self._flush_inbound()

    async def _ready_fallback(self) -> None:
        await asyncio.sleep(2)
        if not self._greeted:
            await self._greet()

    async def _flush_inbound(self) -> None:
        buffered = self._inbound
        self._inbound = []
        for payload in buffered:
            await self._xai_send({"type": "input_audio_buffer.append", "audio": payload})

    async def _play(self, delta: str) -> None:
        if not delta or not self.stream_sid:
            return
        generation = self._audio_gen
        for frame in mulaw_frames(delta):
            if generation != self._audio_gen or self._stop.is_set():
                return
            await self._twilio_send(
                {"event": "media", "streamSid": self.stream_sid, "media": {"payload": frame}}
            )

    async def _barge_in(self) -> None:
        self._audio_gen += 1
        if self.stream_sid:
            await self._twilio_send({"event": "clear", "streamSid": self.stream_sid})

    async def _on_function(self, event: dict[str, Any]) -> None:
        self._pending_tools += 1
        try:
            result = await asyncio.to_thread(
                execute_tool,
                str(event.get("name") or ""),
                event.get("arguments"),
                session=self.session,
                settings=self.settings,
                calendar=self.calendar,
                known_numbers=self.known_numbers,
            )
        except Exception:
            logger.exception("Tool failed")
            result = {
                "error": "tool_failed",
                "instruction": "Say the team will confirm that, and keep going.",
            }
        call_id = str(event.get("call_id") or event.get("item_id") or "")
        await self._xai_send(
            {
                "type": "conversation.item.create",
                "item": {
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": json.dumps(result),
                },
            }
        )
        self._pending_tools -= 1
        self._tools_ready += 1
        if str(event.get("name") or "") == "request_transfer":
            logger.info(
                "transfer decision %s for %s",
                result.get("transfer"),
                self.session.call_sid,
            )
        if result.get("transfer") == "approved":
            asyncio.create_task(self._transfer_watchdog())
        await self._continue_after_tools()

    async def _continue_after_tools(self) -> None:
        if self._continuing or not self._response_done or self._pending_tools or not self._tools_ready:
            return
        self._continuing = True
        self._tools_ready = 0
        self._response_done = False
        # The tool turn's audio has already been sent. Give Twilio a moment to
        # finish playing it before the model speaks the result.
        await asyncio.sleep(0.6)
        await self._xai_send({"type": "response.create"})
        self._continuing = False

    async def _ring_known_caller(self) -> None:
        """Ring David for a directory caller whose policy is always. No screening."""

        await asyncio.sleep(5)
        if self._stop.is_set() or self.session.transfer_started:
            return
        self.session.asked_for_david = True
        self.session.transfer_requested = True
        if not self.session.reason:
            self.session.reason = f"Known {self.session.known_caller_category} caller"
        await self._start_dial()

    async def _dial_after_pause(self) -> None:
        await asyncio.sleep(3)
        await self._start_dial()

    async def _transfer_watchdog(self) -> None:
        await asyncio.sleep(12)
        await self._start_dial()

    async def _start_dial(self) -> None:
        async with self._dial_lock:
            if self._stop.is_set() or self.session.transfer_started or not self.session.transfer_requested:
                return
            begin_transfer(self.session)
        http_base = self.settings.http_base()
        parent = self.session.call_sid
        twiml = caller_hold_twiml(http_base=http_base, call_sid=parent)
        ok = await asyncio.to_thread(self.twilio.update_call, parent, twiml)
        if not ok:
            self.session.holding_for_transfer = False
            self.session.transfer_started = False
            self.session.transfer_state = ""
            self.session.transfer_requested = False
            self.session.action = "message"
            await self._ask_for_message("The transfer could not be placed. Take a message.")
            return
        parent_query = quote(parent, safe="")
        agent_sid = await asyncio.to_thread(
            self.twilio.create_call,
            to_number=self.settings.forward_to_number,
            from_number=self.session.dialed_number,
            url=f"{http_base}/voice/whisper?parent={parent_query}",
            status_callback=f"{http_base}/voice/agent-status?parent={parent_query}",
            amd_status_callback=f"{http_base}/voice/agent-amd?parent={parent_query}",
            timeout=RING_TIMEOUT_SECONDS,
        )
        if agent_sid:
            self.session.agent_call_sid = agent_sid
            logger.info("transfer dial started call=%s", parent)
            return
        logger.info("transfer dial failed call=%s", parent)
        # The caller is already in the conference. Bring them back here rather
        # than leaving them on hold. The closing socket must not finalize yet.
        self.session.resume = "message"
        self.session.transfer_state = "messaging"
        self.session.transfer_requested = False
        self.session.callback_needed = True
        self.session.action = (
            "needs_callback" if self.session.asked_for_david else "transfer_no_answer_message"
        )
        self.session.transfer_outcome = "no_answer"
        await asyncio.to_thread(
            self.twilio.update_call,
            parent,
            resume_twiml(self.settings, http_base=http_base, call_sid=parent),
        )

    async def _ask_for_message(self, text: str) -> None:
        await self._xai_send(
            {
                "type": "conversation.item.create",
                "item": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": text}],
                },
            }
        )
        await self._xai_send({"type": "response.create"})

    async def _max_duration(self) -> None:
        await asyncio.sleep(self.settings.max_call_seconds)
        if self.session.transfer_state == "dialing":
            return
        from voice.twiml import goodbye_twiml

        self.session.action = self.session.action or "message"
        self.session.summary = self.session.summary or "Call reached the time limit."
        await asyncio.to_thread(
            self.twilio.update_call, self.session.call_sid, goodbye_twiml()
        )

    async def _xai_send(self, payload: dict[str, Any]) -> None:
        if self.xai is None:
            return
        try:
            await self.xai.send(json.dumps(payload))
        except Exception:
            logger.warning("Could not write to the xAI socket")

    async def _twilio_send(self, payload: dict[str, Any]) -> None:
        async with self._send_lock:
            try:
                await self.websocket.send_text(json.dumps(payload))
            except Exception:
                self._stop.set()


def _connect_xai(settings: Settings) -> Any:
    import websockets

    url = f"{settings.xai_realtime_url}?model={settings.xai_model}"
    headers = {"Authorization": f"Bearer {settings.xai_api_key}"}
    parameters = inspect.signature(websockets.connect).parameters
    header_kw = "additional_headers" if "additional_headers" in parameters else "extra_headers"
    return websockets.connect(
        url,
        max_size=8_000_000,
        open_timeout=10,
        ping_interval=20,
        **{header_kw: headers},
    )


def missed_transfer_line(session: CallSession) -> str:
    """Spoken when the on-call leg did not accept. David gets the callback text."""

    if session.asked_for_david:
        return DAVID_CALLBACK_LINE
    return (
        "I'm sorry, I couldn't reach anyone just now. "
        "I'll take a message and make sure it gets to the team."
    )


def whisper_line(session: CallSession) -> str:
    name = session.caller_name or "A caller"
    reason = one_line(session.reason or session.summary or "needs a person", 120)
    number = session.caller_number or "an unknown number"
    return f"Anata transfer. {name}, calling from {number}. {reason}. Press 1 to accept."
