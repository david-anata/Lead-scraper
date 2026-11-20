-- Directory of callers the receptionist already knows.
-- voice_logger may read every row and may insert an untagged inbound number.
-- David tags category, company, notes, and ring_policy himself. No seed rows.

create table if not exists public.known_callers (
    id uuid primary key default gen_random_uuid(),
    phone_e164 text not null unique,
    name text not null default '',
    company text not null default '',
    category text not null default 'unknown'
        check (category in ('tenant', 'client', 'landlord_legal', 'vendor', 'vip', 'unknown')),
    notes text not null default '',
    ring_policy text
        check (ring_policy is null or ring_policy in ('always', 'urgent_only', 'never')),
    source text not null default '',
    active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table public.known_callers enable row level security;

revoke all on table public.known_callers from anon, authenticated;

grant select on table public.known_callers to voice_logger;
grant insert (name, phone_e164, category, source)
    on table public.known_callers to voice_logger;

drop policy if exists voice_logger_select_known_callers on public.known_callers;
create policy voice_logger_select_known_callers
    on public.known_callers
    for select to voice_logger
    using (true);

drop policy if exists voice_logger_insert_unknown_callers on public.known_callers;
create policy voice_logger_insert_unknown_callers
    on public.known_callers
    for insert to voice_logger
    with check (category = 'unknown' and source = 'inbound_call');

alter table public.call_log
    add column if not exists known_caller_id text not null default '',
    add column if not exists known_caller_category text not null default '';

grant insert, update (known_caller_id, known_caller_category)
    on table public.call_log to voice_logger;
