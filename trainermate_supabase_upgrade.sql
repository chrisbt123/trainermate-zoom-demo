-- TrainerMate incremental Supabase upgrade
-- Keeps current plan model: free / paid / admin.
-- Safe to run more than once.

-- Release/update metadata for the remote update engine.
create table if not exists public.app_releases (
  id uuid primary key default gen_random_uuid(),
  version text not null unique,
  minimum_supported_version text not null default '1.0.0',
  download_url text,
  installer_sha256 text,
  release_notes text not null default '',
  is_mandatory boolean not null default false,
  mandatory_after timestamp with time zone,
  created_by text,
  created_at timestamp with time zone not null default now()
);

create table if not exists public.password_reset_tokens (
  id uuid primary key default gen_random_uuid(),
  account_id uuid references public.accounts(id) on delete cascade,
  ndors_trainer_id text not null,
  email text not null,
  token_hash text not null unique,
  expires_at timestamp with time zone not null,
  used_at timestamp with time zone,
  created_at timestamp with time zone not null default now()
);

-- Optional device health fields for future admin filtering/reporting.
alter table public.accounts add column if not exists password_hash text;
alter table public.accounts add column if not exists password_set_at timestamp with time zone;
alter table public.accounts add column if not exists password_must_change boolean not null default false;
alter table public.accounts add column if not exists last_login_at timestamp with time zone;
alter table public.devices add column if not exists app_version text;
alter table public.devices add column if not exists build text;
alter table public.devices add column if not exists last_ip text;
alter table public.devices add column if not exists health_status text;
alter table public.devices add column if not exists provider_count integer not null default 0;
alter table public.devices add column if not exists zoom_status text;
alter table public.devices add column if not exists needs_attention boolean not null default false;
alter table public.devices add column if not exists last_command_at timestamp with time zone;

-- Useful commercial/support metadata without limiting devices/providers.
alter table public.licences add column if not exists issued_by text;
alter table public.licences add column if not exists notes text;
alter table public.licences add column if not exists upgrade_from text;

-- Audit metadata for admin/support safety.
alter table public.audit_log add column if not exists severity text not null default 'info';
alter table public.audit_log add column if not exists source text not null default 'admin';
alter table public.audit_log add column if not exists ip_address text;

-- Helpful uniqueness/indexes. If duplicates already exist, run a duplicate cleanup first.
create unique index if not exists idx_accounts_ndors_trainer_id on public.accounts (lower(ndors_trainer_id));
create unique index if not exists idx_devices_account_device on public.devices (account_id, device_id);
create unique index if not exists idx_usage_account_id on public.usage (account_id);
create unique index if not exists idx_licences_licence_key on public.licences (licence_key);
create index if not exists idx_password_reset_tokens_lookup on public.password_reset_tokens (token_hash, ndors_trainer_id, used_at);

-- Account identity guardrails. Run duplicate checks before adding these indexes on an existing database:
-- select lower(ndors_trainer_id), count(*) from public.accounts group by lower(ndors_trainer_id) having count(*) > 1;
-- select lower(primary_email), count(*) from public.accounts where coalesce(primary_email, '') <> '' group by lower(primary_email) having count(*) > 1;
-- select lower(email), count(*) from public.account_logins group by lower(email) having count(*) > 1;
create unique index if not exists idx_accounts_primary_email_unique_lower
  on public.accounts (lower(primary_email))
  where coalesce(primary_email, '') <> '';
create unique index if not exists idx_account_logins_email_unique_lower
  on public.account_logins (lower(email));
