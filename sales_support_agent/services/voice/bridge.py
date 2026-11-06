"""Bridge one Twilio Media Stream to one xAI realtime session."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.voice import VoiceCall
from sales_support_agent.services.voice import prompt, tools, twilio
from sales_support_agent.services.voice.config import VoiceConfig

logger = logging.getLogger(__name__)

XaiConnect = Callable[[VoiceConfig], Awaitable[Any]]


async def default_xai_connect(config: VoiceConfig) -> Any:
    import websockets

    return await websockets.connect(
        f"{config.xai_realtime_url}?model={config.xai_voice_model}",
        additional_headers={"Authorization": f"Bearer {config.xai_api_key}"},
        max_size=2**22,
    )


class CallBridge:
    """Pumps audio both ways, runs tools, and records the transcript."""

    def __init__(
        self,
        *,
        config: VoiceConfig,
        session_factory: Any,
        twilio_ws: Any,
        xai_connect: XaiConnect = default_xai_connect,
        finalize: Optional[Callable[[tools.CallContext, str], None]] = None,
    ) -> None:
        self.config = config
        self.session_factory = session_factory
        self.twilio_ws = twilio_ws
        self.xai_connect = xai_connect
        self.finalize = finalize
        self.stream_sid = ""
        self.ctx: Optional[tools.CallContext] = None
        self.call_id = ""
        self.transcript: list[dict] = []
        self._pending_tool_output = False
        self._closed = asyncio.Event()

    async def _await_start(self) -> Optional[dict]:
        while True:
            raw = await self.twilio_ws.receive_text()
            message = json.loads(raw)
            if message.get("event") == "start":
                return message["start"]
            if message.get("event") == "stop":
                return None

    def _open_call_record(self, start: dict) -> None:
        params = start.get("customParameters") or {}
        call_sid = start.get("callSid", "")
        with session_scope(self.session_factory) as session:
            row = session.query(VoiceCall).filter_by(call_sid=call_sid).one_or_none()
            if row is None:
                row = VoiceCall(id=tools.new_id(), call_sid=call_sid)
                session.add(row)
            row.from_number = params.get("from", "")[:32]
            row.to_number = params.get("to", "")[:32]
            row.status = "in_progress"
            row.disclosure_text = self.config.disclosure
            row.started_at = datetime.now(timezone.utc)
            self.call_id = row.id

    async def run(self) -> None:
        start = await self._await_start()
        if start is None:
            return
        params = start.get("customParameters") or {}
        call_sid = start.get("callSid", "")
        if not twilio.valid_stream_token(self.config.twilio_auth_token, call_sid, params.get("token", "")):
            logger.warning("voice stream rejected: bad token for %s", call_sid)
            await self.twilio_ws.close(code=1008)
            return
        self.stream_sid = start.get("streamSid", "")
        self.ctx = tools.CallContext(
            config=self.config,
            session_factory=self.session_factory,
            call_sid=call_sid,
            from_number=params.get("from", ""),
        )
        await asyncio.to_thread(self._open_call_record, start)
        outcome = "completed"
        xai = await self.xai_connect(self.config)
        try:
            await xai.send(json.dumps(prompt.session_update(self.config)))
            await xai.send(json.dumps(prompt.disclosure_message(self.config)))
            self.ctx.state["disclosure_played_at"] = datetime.now(timezone.utc)
            await asyncio.gather(self._twilio_to_xai(xai), self._xai_to_twilio(xai))
        except Exception as exc:  # noqa: BLE001
            logger.warning("voice bridge error on %s: %s", call_sid, exc)
            outcome = "error"
            self.ctx.state["error"] = type(exc).__name__
        finally:
            try:
                await xai.close()
            except Exception:  # noqa: BLE001
                pass
            self.ctx.state["transcript"] = self.transcript
            if self.finalize is not None:
                await asyncio.to_thread(self.finalize, self.ctx, outcome)

    async def _twilio_to_xai(self, xai: Any) -> None:
        try:
            while not self._closed.is_set():
                message = json.loads(await self.twilio_ws.receive_text())
                event = message.get("event")
                if event == "media":
                    await xai.send(json.dumps({"type": "input_audio_buffer.append", "audio": message["media"]["payload"]}))
                elif event == "stop":
                    break
        except Exception as exc:  # noqa: BLE001 — caller hung up / socket closed
            logger.info("twilio stream closed: %s", type(exc).__name__)
        finally:
            self._closed.set()
            try:
                await xai.close()
            except Exception:  # noqa: BLE001
                pass

    async def _send_twilio(self, payload: dict) -> None:
        payload["streamSid"] = self.stream_sid
        await self.twilio_ws.send_text(json.dumps(payload))

    async def _xai_to_twilio(self, xai: Any) -> None:
        try:
            async for raw in xai:
                if isinstance(raw, bytes):
                    continue
                await self.handle_xai_event(xai, json.loads(raw))
                if self._closed.is_set():
                    break
        except Exception as exc:  # noqa: BLE001
            if not self._closed.is_set():
                raise
            logger.info("xai socket closed after hangup: %s", type(exc).__name__)
        finally:
            self._closed.set()

    async def handle_xai_event(self, xai: Any, event: dict) -> None:
        kind = event.get("type", "")
        if kind in {"response.output_audio.delta", "response.audio.delta"}:
            await self._send_twilio({"event": "media", "media": {"payload": event.get("delta", "")}})
        elif kind == "input_audio_buffer.speech_started":
            await self._send_twilio({"event": "clear"})
        elif kind == "conversation.item.input_audio_transcription.completed":
            self._append("caller", event.get("transcript", ""))
        elif kind in {"response.output_audio_transcript.done", "response.audio_transcript.done"}:
            self._append("agent", event.get("transcript", ""))
        elif kind == "response.function_call_arguments.done":
            await self._run_tool(xai, event)
        elif kind == "response.done" and self._pending_tool_output:
            self._pending_tool_output = False
            if not (self.ctx and self.ctx.state.get("transferred")):
                await xai.send(json.dumps({"type": "response.create"}))
        elif kind == "error":
            logger.warning("xai realtime error: %s", str(event.get("error"))[:300])

    def _append(self, speaker: str, text: str) -> None:
        text = (text or "").strip()
        if text:
            self.transcript.append({"speaker": speaker, "text": text, "at": datetime.now(timezone.utc).isoformat()})

    async def _run_tool(self, xai: Any, event: dict) -> None:
        assert self.ctx is not None
        try:
            args = json.loads(event.get("arguments") or "{}")
        except json.JSONDecodeError:
            args = {}
        result = await asyncio.to_thread(tools.dispatch, self.ctx, event.get("name", ""), args)
        await xai.send(
            json.dumps(
                {
                    "type": "conversation.item.create",
                    "item": {"type": "function_call_output", "call_id": event.get("call_id", ""), "output": json.dumps(result, default=str)},
                }
            )
        )
        self._pending_tool_output = True
