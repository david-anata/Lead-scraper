-- AI voice receptionist: call log, AI-call consent, do-not-call list.
-- Apply in the Supabase SQL editor before enabling VOICE_AGENT_ENABLED.
-- Idempotent and additive. The app also creates these tables on boot
-- (_ensure_voice_tables) outside Vercel.

CREATE TABLE IF NOT EXISTS voice_calls (
    id VARCHAR(64) PRIMARY KEY,
    call_sid VARCHAR(64) NOT NULL UNIQUE,
    direction VARCHAR(16) NOT NULL DEFAULT 'inbound',
    from_number VARCHAR(32) NOT NULL DEFAULT '',
    to_number VARCHAR(32) NOT NULL DEFAULT '',
    status VARCHAR(32) NOT NULL DEFAULT 'ringing',
    outcome VARCHAR(64) NOT NULL DEFAULT '',
    disclosure_text TEXT NOT NULL DEFAULT '',
    disclosure_played_at TIMESTAMPTZ NULL,
    matched_inquiry_id VARCHAR(64) NOT NULL DEFAULT '',
    caller_name VARCHAR(255) NOT NULL DEFAULT '',
    caller_email VARCHAR(255) NOT NULL DEFAULT '',
    transcript_json JSON NOT NULL DEFAULT '[]',
    tool_calls_json JSON NOT NULL DEFAULT '[]',
    summary TEXT NOT NULL DEFAULT '',
    tour_event_id VARCHAR(255) NOT NULL DEFAULT '',
    tour_start_at TIMESTAMPTZ NULL,
    transferred BOOLEAN NOT NULL DEFAULT FALSE,
    summary_email_status VARCHAR(32) NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    duration_seconds INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at TIMESTAMPTZ NULL
);
CREATE INDEX IF NOT EXISTS ix_voice_calls_started_at ON voice_calls (started_at);
CREATE INDEX IF NOT EXISTS ix_voice_calls_from_number ON voice_calls (from_number);
CREATE INDEX IF NOT EXISTS ix_voice_calls_outcome ON voice_calls (outcome);
CREATE INDEX IF NOT EXISTS ix_voice_calls_matched_inquiry_id ON voice_calls (matched_inquiry_id);

CREATE TABLE IF NOT EXISTS voice_consents (
    id VARCHAR(64) PRIMARY KEY,
    phone VARCHAR(32) NOT NULL,
    email VARCHAR(255) NOT NULL DEFAULT '',
    source VARCHAR(64) NOT NULL DEFAULT '',
    source_ref VARCHAR(128) NOT NULL DEFAULT '',
    consent_text TEXT NOT NULL DEFAULT '',
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ NULL,
    revoked_reason VARCHAR(128) NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS ix_voice_consents_phone ON voice_consents (phone);
CREATE INDEX IF NOT EXISTS ix_voice_consents_email ON voice_consents (email);

CREATE TABLE IF NOT EXISTS voice_dnc (
    phone VARCHAR(32) PRIMARY KEY,
    reason VARCHAR(128) NOT NULL DEFAULT 'caller_request',
    source VARCHAR(64) NOT NULL DEFAULT 'voice_agent',
    call_sid VARCHAR(64) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
