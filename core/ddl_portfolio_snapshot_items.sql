-- Crie APENAS se ainda não existir (necessário para Patch 6).
create table if not exists public.portfolio_snapshot_items (
  snapshot_id uuid not null references public.portfolio_snapshots(id) on delete cascade,
  ticker text not null,
  peso numeric null,
  created_at timestamp with time zone not null default now(),
  constraint portfolio_snapshot_items_pkey primary key (snapshot_id, ticker)
);

create index if not exists ix_portfolio_snapshot_items_snapshot_id
  on public.portfolio_snapshot_items (snapshot_id);
