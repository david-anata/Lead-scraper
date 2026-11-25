-- Let the receptionist remember callers it has identified.
-- A manual row (any source other than auto, and any tagged inbound stub)
-- keeps its name, company, and category. Auto learning may fill blanks
-- and may always refresh last_reason and last_called_at.
-- Safe to re-run.

alter table public.known_callers
    add column if not exists last_reason text not null default '',
    add column if not exists last_called_at timestamptz;

alter table public.known_callers drop constraint if exists known_callers_category_check;
alter table public.known_callers
    add constraint known_callers_category_check
    check (category in (
        'tenant', 'client', 'landlord_legal', 'vendor', 'vip', 'spam', 'lead', 'unknown'
    ));

grant insert (company, last_reason, last_called_at)
    on table public.known_callers to voice_logger;
grant update (
    name, company, category, source, last_reason, last_called_at, updated_at
) on table public.known_callers to voice_logger;

drop policy if exists voice_logger_insert_auto_callers on public.known_callers;
create policy voice_logger_insert_auto_callers
    on public.known_callers
    for insert to voice_logger
    with check (
        source = 'auto'
        and category in (
            'tenant', 'client', 'landlord_legal', 'vendor', 'vip', 'spam', 'lead', 'unknown'
        )
    );

drop policy if exists voice_logger_update_known_callers on public.known_callers;
create policy voice_logger_update_known_callers
    on public.known_callers
    for update to voice_logger
    using (true)
    with check (true);

create or replace function public.known_callers_keep_manual()
returns trigger
language plpgsql
as $$
begin
    if tg_op = 'UPDATE' and old.source is distinct from 'auto' then
        if old.source = 'inbound_call'
           and old.name = ''
           and old.category in ('', 'unknown') then
            new.source := 'auto';
        else
            if old.name <> '' then
                new.name := old.name;
            end if;
            if old.category not in ('', 'unknown') then
                new.category := old.category;
            end if;
            if old.company <> '' then
                new.company := old.company;
            end if;
            new.ring_policy := old.ring_policy;
            new.notes := old.notes;
            new.source := old.source;
            new.active := old.active;
            new.phone_e164 := old.phone_e164;
        end if;
    end if;
    new.updated_at := now();
    return new;
end;
$$;

drop trigger if exists known_callers_keep_manual on public.known_callers;
create trigger known_callers_keep_manual
    before update on public.known_callers
    for each row
    execute function public.known_callers_keep_manual();

-- Backfill callers whose name is already stored on a call log.
-- Employment-verification calls are indexed as spam. Manual rows keep
-- their name, company, and category; only the last reason and time move.
insert into public.known_callers (
    phone_e164, name, company, category, source, last_reason, last_called_at
)
select
    phone,
    name,
    case when lower(company) in ('personal', 'tenant', 'none') then '' else company end,
    case
        when company ilike '%truework%'
            or reason ilike '%truework%'
            or reason ilike '%employment verification%'
            then 'spam'
        when category = 'tenant' then 'tenant'
        when category = 'vendor' then 'vendor'
        when category = 'spam' then 'spam'
        when category = 'lead' then 'lead'
        else 'lead'
    end,
    'auto',
    reason,
    called_at
from (
    select
        caller_number as phone,
        btrim(lead_fields->>'name') as name,
        btrim(coalesce(lead_fields->>'company', '')) as company,
        category,
        left(btrim(coalesce(nullif(transcript_summary, ''), lead_fields->>'reason', '')), 500) as reason,
        coalesce(ended_at, started_at, now()) as called_at,
        row_number() over (
            partition by caller_number
            order by coalesce(ended_at, started_at) desc nulls last
        ) as rn
    from public.call_log
    where caller_number ~ '^\+[1-9][0-9]{7,14}$'
      and coalesce(lead_fields->>'name', '') ~ '^[A-Za-z][A-Za-z .''-]{1,78}$'
      and lower(btrim(lead_fields->>'name')) not in (
          'someone', 'unknown', 'a caller', 'caller', 'phone caller', 'text caller', 'unknown caller'
      )
) ranked
where rn = 1
  and (
      category in ('lead', 'tenant', 'vendor', 'spam')
      or (
          category = 'other'
          and (
              length(company) >= 2
              or reason ilike '%truework%'
              or reason ilike '%employment verification%'
          )
      )
  )
on conflict (phone_e164) do update set
    name = excluded.name,
    company = excluded.company,
    category = excluded.category,
    source = excluded.source,
    last_reason = excluded.last_reason,
    last_called_at = excluded.last_called_at,
    updated_at = now();
