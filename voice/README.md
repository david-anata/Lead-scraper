# Anata voice receptionist

Phone and SMS front desk for Anata (Anata Building / anata Inc., Lehi, Utah).
This is a separate process from the lead scraper and the sales support agent.
It is meant to run as its own Render web service, because the call stays on a
websocket for the whole conversation.

## What a call does

1. Twilio requests `POST /voice/inbound`.
2. The service validates the Twilio signature and returns TwiML that connects a
   bidirectional Media Stream.
3. Caller audio is G.711 µ-law at 8 kHz. That payload is forwarded unchanged to
   xAI's realtime voice API (`wss://api.x.ai/v1/realtime`, model
   `grok-voice-latest`), which is configured for `audio/pcmu` in both directions.
   Nothing is resampled.
4. The assistant answers from three knowledge files, loaded in this order:
   `voice/data/front_desk.md` (lines, caller types, routing, closed topics),
   `voice/data/anata_building.md` (workspace, Arena, tenants), and
   `voice/data/anata_inc.md` (marketing, fulfillment, software). Prices and
   date status come back from tools, not from the model's memory. To correct
   the assistant, edit the matching file; the tests check that no unpublished
   dollar amount slips in.
5. A live transfer parks the caller in a Twilio conference with hold music
   (the standard hold-music twimlet on `waitUrl`) and dials the on-call phone
   as its own call. That phone hears the whisper and must press 1 within 25
   seconds. If it does not, the ring times out, or answering-machine detection
   hears voicemail, that leg is dropped and the caller returns to the assistant
   to leave a message. Voicemail is never a successful transfer, and
   `callback_needed` is set. Screening in `voice/screening.py` still decides
   whether a transfer is allowed.
6. When the call ends, a non-spam call is summarized by SMS to
   `NOTIFY_SMS_NUMBER` and every call is written to Supabase `call_log`.
   Event and tour leads are also inserted into `building_inquiries`, which is
   the table the building admin already lists.

xAI can also take a SIP call directly. This service does not use that. A SIP
REFER would hand the caller away, and there would be no way to come back to
the assistant when the forwarded phone does not answer. Keeping Twilio as the
call owner is what makes the fallback work. The cost is one extra network hop
(Twilio → this service → xAI).

## Webhooks to set on each Twilio number

Use the public origin of this service (`PUBLIC_BASE_URL`, or the Render
`onrender.com` URL) and HTTP POST.

| Twilio console field | Method | Path |
| --- | --- | --- |
| Voice — A call comes in | POST | `/voice/inbound` |
| Messaging — A message comes in | POST | `/voice/sms` |

Example, once the service is at `https://anata-voice-receptionist.onrender.com`:

- Voice URL: `https://anata-voice-receptionist.onrender.com/voice/inbound`
- SMS URL: `https://anata-voice-receptionist.onrender.com/voice/sms`

Do not point the voice status callback, the dial action, or the whisper URL
yourself. The service sets these:

- `POST /voice/dial-result` — the caller's conference leg ended
- `POST /voice/whisper` — spoken only to the on-call phone, then waits for digit 1
- `POST /voice/accept` — the on-call phone pressed 1 and is joined to the conference
- `POST /voice/agent-status` — the on-call leg ended without a press 1
- `POST /voice/agent-amd` — voicemail detection on the on-call leg
- `POST /voice/sms-status` — delivery receipt for a summary text
- `GET /voice/media` — Media Stream websocket (`wss://` on the same host, no query string). The stream token is a `<Parameter>`, because Twilio does not accept query parameters on a Stream URL.

`GET /health` is the Render health check. It returns 200 even when secrets are
still missing, and lists the missing variable names. It does not list values.

## Several numbers, one service

Set `LINE_CONTEXTS_JSON` to a JSON object. Keys are E.164 numbers. `default`
is used when the dialed number is not listed. The 555 number below is a
placeholder, not a real line:

```json
{
  "default": {"id": "anata", "label": "Anata"},
  "+15550001111": {"id": "front_desk", "label": "Anata Building front desk"}
}
```

Line ids the front desk rules know: `main`, `front_desk`, `anata_services`,
`gabe_line`, `val_line`, and `test`. An unknown id is treated like `main`.

The label is what the greeting says. A line labeled `Anata` is greeted as
"Thanks for calling Anata." Any other label is greeted as "Thanks for calling
the {label}."

## Environment variables

Required before the line will answer:

| Variable | Purpose |
| --- | --- |
| `XAI_API_KEY` | xAI realtime voice. Server-side only. |
| `TWILIO_ACCOUNT_SID` | Twilio REST account. |
| `TWILIO_AUTH_TOKEN` | Webhook signatures, Media Stream tokens, REST calls. |
| `PUBLIC_BASE_URL` | `https://…` origin Twilio uses. Signatures are checked against this. `RENDER_EXTERNAL_URL` is used when this is empty. |

Required for the rest of the behavior:

| Variable | Purpose |
| --- | --- |
| `FORWARD_TO_NUMBER` | E.164 number for approved live transfers. Not stored in the repo. |
| `NOTIFY_SMS_NUMBER` | E.164 number that receives the after-call and inbound-SMS summary. |
| `VOICE_DATABASE_URL` | Preferred logger. Postgres URL for the `voice_logger` role through the Supabase pooler. Include `sslmode=require`; the app forces that mode if it is missing or weaker. |
| `SUPABASE_URL` | REST fallback. Same project as the building admin. |
| `SUPABASE_SERVICE_ROLE_KEY` | REST fallback when `VOICE_DATABASE_URL` is empty. `SUPABASE_SERVICE_KEY` is an alias. |

Optional:

| Variable | Default | Purpose |
| --- | --- | --- |
| `SMS_AUTOREPLY` | `false` | When `false`, inbound texts are forwarded and not answered. Turn on only after A2P 10DLC registration. |
| `RECORDING_ENABLED` | `false` | When `true`, the caller hears "This call may be recorded" and Twilio starts a recording. Transcripts are stored either way. |
| `VOICE_SIGNATURE_VALIDATION` | `true` | Keep this true outside local smoke tests. |
| `LINE_CONTEXTS_JSON` | default Anata line | Per-number greeting context. |
| `KNOWN_CALLER_NUMBERS` | empty | Comma-separated E.164 numbers that count as known customers for transfer. |
| `XAI_VOICE_MODEL` | `grok-voice-latest` | Realtime model. |
| `XAI_VOICE` | `ara` | Voice id. `ara` is the warm front-desk voice. Speech plays at 0.95 speed. |
| `XAI_REALTIME_URL` | `wss://api.x.ai/v1/realtime` | Override only if xAI changes the endpoint. |
| `VOICE_EVENTS_CALENDAR_ID` | the Anata events calendar | Google calendar id. `BUILDING_GOOGLE_CALENDAR_ID` is also read. |
| `GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON` | empty | Service account JSON. The calendar must be shared with that account. `BUILDING_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON` is also read. |
| `BUILDING_GOOGLE_CALENDAR_DELEGATED_SUBJECT` | empty | Only if the calendar requires domain-wide delegation. |
| `BUILDING_DEFAULT_LEAD_OWNER` | `building-operator` | `assigned_owner` on new inquiries. |
| `TWILIO_MESSAGING_SERVICE_SID` | empty | Used as the sender for summary texts when set. Otherwise the dialed Twilio number is the sender. |
| `VOICE_MAX_CALL_SECONDS` | `720` | The call is wrapped up at this limit. |

No staff phone numbers, API keys, or customer rows belong in the repo.
`/health` is safe to open: it reports which names are empty.

## Supabase

`supabase/migrations/20261110220000_voice_call_log.sql` creates
`public.call_log` only. It is already applied on the agent Supabase project
(anata-agent-staging). Row level security is on, and `anon` / `authenticated`
have no privileges. The service role key bypasses that, which is how this
server writes. `agent_app` can read the table the same way it reads the other
building tables. Running the file again is safe.

The migration does not change `building_inquiries` or any other existing
table. A lead insert uses the columns the admin already reads: `kind`
(`event`, `tour`, or `workspace`), `source = voice`, `payload_json.eventType`,
`payload_json.groupSize`, and `_lifecycle.stage = new`. Spam is written to
`call_log` only.

`/health` reports `logging_ready` from a live privilege probe when
`VOICE_DATABASE_URL` is set. The probe requires
`has_table_privilege(current_user, 'public.call_log', 'SELECT')` plus
`has_column_privilege` on the columns this role writes. Column `SELECT` on
`call_sid` alone is not enough for the upsert, so that grant does not make
`logging_ready` true. Table-level `UPDATE` on `call_log` and table-level
`INSERT` on `building_inquiries` stay false under these column grants and are
not part of the probe. A present variable with a denied role reports
`logging_ready: false` and `logging_probe: denied` or `unreachable`.
A successful probe is cached for 30 seconds. When only the service-role REST
pair is set, there is no privilege probe and `logging_probe` is `not_probed`.

The inquiry insert and the call-log upsert use separate transactions. An
inquiry failure is logged on its own and the call log is still written.
`source` on a voice lead is `voice`. The full transcript and the full summary
are stored. `sms_notified` stays false until the message status callback says
`delivered`. An undelivered text, including error 30034 while A2P is not
approved, stays false. A Twilio 2xx on send is only acceptance, not delivery.

Writes run in a worker thread, not on the call audio loop. The Postgres path
writes only `call_log` and `building_inquiries`. The REST path still links a
contact when the service-role key is the writer.

### Grants for `voice_logger`

No sequences. `call_log.id` defaults to `gen_random_uuid()` (executable by
public), and inquiry ids are created in the app. Both tables have row level
security and this role does not bypass it, so the policies below are required
or every insert is denied.

```sql
GRANT INSERT, UPDATE (
  call_sid, channel, caller_number, dialed_number, line_id,
  started_at, ended_at, duration_seconds, transcript_summary, transcript,
  category, lead_fields, action_taken, transfer_attempted, transfer_outcome,
  sms_notified, building_inquiry_id, updated_at
) ON TABLE public.call_log TO voice_logger;

-- Table SELECT, not SELECT (call_sid). With row level security on,
-- INSERT … ON CONFLICT DO UPDATE reads the existing row and raises
-- "permission denied for table call_log" unless this grant is present.
GRANT SELECT ON TABLE public.call_log TO voice_logger;

GRANT INSERT (
  id, idempotency_key, kind, source, source_reference, offering_id,
  name, email, phone, preferred_date, status, assigned_owner, response_due_at,
  consent_to_contact, consent_to_marketing, hubspot_contact_id, payload_json,
  created_at, updated_at
) ON TABLE public.building_inquiries TO voice_logger;

GRANT SELECT (id, idempotency_key)
  ON TABLE public.building_inquiries TO voice_logger;

CREATE POLICY voice_logger_insert_call_log ON public.call_log
  FOR INSERT TO voice_logger WITH CHECK (true);
CREATE POLICY voice_logger_update_call_log ON public.call_log
  FOR UPDATE TO voice_logger USING (true) WITH CHECK (true);
CREATE POLICY voice_logger_select_call_log ON public.call_log
  FOR SELECT TO voice_logger USING (true);
CREATE POLICY voice_logger_insert_inquiries ON public.building_inquiries
  FOR INSERT TO voice_logger WITH CHECK (source = 'voice');
CREATE POLICY voice_logger_select_voice_inquiries ON public.building_inquiries
  FOR SELECT TO voice_logger USING (source = 'voice');
```

`call_log` has table-level `SELECT`. Column `SELECT` on `call_sid` is not a
substitute: the upsert is `INSERT … ON CONFLICT DO UPDATE`, and with row
level security that command needs table `SELECT` or Postgres denies the
table. `building_inquiries` is not granted `UPDATE`. Its `SELECT` is only
`id` and `idempotency_key`, so a repeated call can find the inquiry id after
`ON CONFLICT DO NOTHING`. Nothing else is granted: not contacts,
relationships, audit events, or their sequences.

If the calendar credentials are missing, or the calendar call fails, the
assistant takes the date and says the team will confirm. It does not guess.

## Local test

From the repo root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r voice/requirements.txt pytest httpx
pytest voice/tests -q
VOICE_SIGNATURE_VALIDATION=false PUBLIC_BASE_URL=http://localhost:8090 \
  XAI_API_KEY=local TWILIO_ACCOUNT_SID=ACLOCAL TWILIO_AUTH_TOKEN=local \
  python voice/scripts/local_smoke.py
```

To place a real test call, expose the process and point one Twilio number at it:

```bash
uvicorn voice.main:app --host 0.0.0.0 --port 8090
# in another shell, with the public URL ngrok (or similar) prints:
# PUBLIC_BASE_URL=https://<public-host>
```

Set that number's Voice URL and SMS URL as in the table above. Put the real
`XAI_API_KEY`, Twilio credentials, `FORWARD_TO_NUMBER`, and `NOTIFY_SMS_NUMBER`
in the environment. Leave `SMS_AUTOREPLY` false and `RECORDING_ENABLED` false
until you mean to change them.

`/health` should show `calls_ready: true` before you dial.

## Render

`anata-voice-receptionist` is a second web service in the root `render.yaml`.
It installs `voice/requirements.txt` and listens on `0.0.0.0:$PORT`.
Keep `numInstances` at 1. Call state lives in the process, so a second
instance would lose the transfer fallback.

Use a plan that does not spin down. A sleeping free instance misses calls.
Starter is the plan set in the blueprint; move up if overlapping calls get tight.

After the first blueprint sync, fill the `sync: false` secrets in the
dashboard, set `PUBLIC_BASE_URL` to the service origin, apply the SQL
migration, then set the Twilio webhooks. Check `/health` again.

## Screening

These decisions are code, not a prompt:

- Spam, robocalls, and vendor or marketing pitches are never forwarded,
  even if the caller says it is urgent. The call is logged and not texted.
- A building emergency, security problem, or active tenant with an urgent
  access or safety issue can be forwarded.
- A known customer who wants a person now can be forwarded. "Known" means the
  caller number is in `KNOWN_CALLER_NUMBERS`, or the model marks them as a
  known customer. There is no tenant directory yet.
- A caller who asks for a person, the owner, or David is transferred, as is
  a contract question, a pricing exception, or anything the line cannot answer.
- A sales transfer can also happen when a prospect has a name, one concrete
  interest (date, guest count, event type, or company), and wants to talk now.
- If `FORWARD_TO_NUMBER` is empty, every transfer becomes a message.

## Limits

- xAI documents this realtime voice at about $0.08 per minute and sub-second
  model latency. The phone path adds the Twilio and Render hops, so a turn is
  slower than a browser sitting next to the API. Barge-in clears Twilio's
  playback buffer when xAI reports that the caller started speaking.
- The docs used here (`docs.x.ai`, speech-to-speech and the voice REST
  reference, checked November 10, 2026) do not publish a concurrent-session
  cap. Confirm the current limit in the xAI console before pointing several
  busy numbers at one key.
- Reasoning is turned down (`reasoning.effort = none`) so the front desk stays
  short. The model is `grok-voice-latest` (currently grok-voice-3.0).
- Summary SMS uses the Twilio number or a messaging service. Until A2P 10DLC
  is registered, Twilio may accept the send and then mark it undelivered
  with error 30034. The call is still logged, and `sms_notified` stays false
  unless a later status callback says delivered. Inbound auto-reply stays off
  for the same reason.
- Arena events are self-serve. The renter sets up and resets inside the booked
  time. There is no free teardown after Saturday midnight or Sunday 10:00 p.m.
  Security staffing is never required. On a big party the assistant offers it
  once as an optional add-on and records `security_preference` as yes, no, or
  undecided.
- One Render instance. A restart mid-call drops the in-memory session; the
  caller would need to call back. The durable record is written at the end.
- Office prices and office availability are not on anatabuilding.com, so the
  assistant will not quote them. Arena add-on prices that are on the policies
  page are in the knowledge file. Cleanup follows that page: trash is bagged
  into the building's cans and is not hauled away; decorations and food the
  client brought are removed.
- The events calendar is read with `calendar.readonly`. Event titles are not
  sent to the model. A clear day is described as clear and not reserved.
