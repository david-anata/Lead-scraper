-- Self-reported "how did you hear about us" next to the dialed tracking line.
-- heard_about_raw is the caller's words. heard_about_source is the category.
-- dialed_line_source is the tracking-line label (or the line id when the
-- number has no label), so the two can be compared. Safe to re-run.

alter table public.call_log
    add column if not exists heard_about_raw text not null default '',
    add column if not exists heard_about_source text not null default 'unknown',
    add column if not exists dialed_line_source text not null default '';

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'call_log_heard_about_source_check'
      and conrelid = 'public.call_log'::regclass
  ) then
    alter table public.call_log
      add constraint call_log_heard_about_source_check
      check (heard_about_source in (
        'google_search',
        'google_maps',
        'website',
        'peerspace',
        'eventective',
        'other_event_site',
        'referral',
        'social',
        'returning_customer',
        'drive_by',
        'other',
        'unknown'
      ));
  end if;
end $$;

-- Past calls already stored the dialed line on lead_fields.source or line_id.
update public.call_log
set dialed_line_source = coalesce(
    nullif(btrim(lead_fields->>'source'), ''),
    nullif(btrim(line_id), ''),
    ''
)
where dialed_line_source = ''
  and channel = 'voice';

create index if not exists call_log_heard_about_started_idx
    on public.call_log (heard_about_source, started_at desc);

comment on column public.call_log.heard_about_raw is
    'Caller words for how they heard about Anata. Empty when they were not asked or did not answer.';
comment on column public.call_log.heard_about_source is
    'Normalized self-reported source. Compare with dialed_line_source, which is the number they called.';
comment on column public.call_log.dialed_line_source is
    'Tracking-line label for the dialed Twilio number, or the line id when that number has no label.';

create or replace view public.call_source_counts as
select
    (coalesce(started_at, created_at) at time zone 'America/Denver')::date as call_date,
    heard_about_source,
    coalesce(nullif(btrim(dialed_line_source), ''), 'unattributed') as dialed_line_source,
    count(*)::integer as call_count
from public.call_log
where channel = 'voice'
group by 1, 2, 3;

comment on view public.call_source_counts is
    'Daily voice-call counts by self-reported source and dialed tracking line. Filter call_date for a range.';

alter view public.call_source_counts set (security_invoker = true);

revoke all on table public.call_source_counts from public, anon, authenticated;
grant select on table public.call_source_counts to service_role;

do $$
begin
  if exists (select 1 from pg_roles where rolname = 'agent_app') then
    grant select on table public.call_source_counts to agent_app;
  end if;
  if exists (select 1 from pg_roles where rolname = 'voice_logger') then
    grant insert, update (heard_about_raw, heard_about_source, dialed_line_source)
      on table public.call_log to voice_logger;
    grant select on table public.call_source_counts to voice_logger;
  end if;
end $$;
