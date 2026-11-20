-- Additive call log for the Anata voice receptionist.
-- Apply to the live database before the phone line takes traffic.
-- Safe to re-run: the table is created only if it is missing, and the
-- grants below do not open the table to the public Data API.

create table if not exists public.call_log (
    id uuid primary key default gen_random_uuid(),
    call_sid text not null unique,
    channel text not null default 'voice' check (channel in ('voice', 'sms')),
    caller_number text not null default '',
    dialed_number text not null default '',
    line_id text not null default '',
    started_at timestamptz,
    ended_at timestamptz,
    duration_seconds integer,
    transcript_summary text not null default '',
    transcript text not null default '',
    category text not null default 'other'
        check (category in ('lead', 'tenant', 'vendor', 'urgent', 'spam', 'other')),
    lead_fields jsonb not null default '{}'::jsonb,
    action_taken text not null default '',
    transfer_attempted boolean not null default false,
    transfer_outcome text not null default '',
    sms_notified boolean not null default false,
    building_inquiry_id text not null default '',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists call_log_started_at_idx on public.call_log (started_at desc);
create index if not exists call_log_category_idx on public.call_log (category);

alter table public.call_log enable row level security;

revoke all on table public.call_log from anon, authenticated;
grant select, insert, update, delete on table public.call_log to service_role;

-- The building admin connects as agent_app, which bypasses RLS. Grant it the
-- same access when that role exists so a later admin view can read the log.
do $$
begin
  if exists (select 1 from pg_roles where rolname = 'agent_app') then
    grant select, insert, update, delete on table public.call_log to agent_app;
  end if;
end $$;
