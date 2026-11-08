create function reject_store_mutation() returns trigger
language plpgsql as $$
begin
    raise exception '% on % is not permitted', TG_OP, TG_TABLE_NAME using errcode = '42501';
end;
$$;

create trigger accounts_immutable
before update or delete or truncate on accounts
for each statement execute function reject_store_mutation();

create trigger account_attributes_immutable
before update or delete or truncate on account_attributes
for each statement execute function reject_store_mutation();

create trigger fee_requests_immutable
before update or delete or truncate on fee_requests
for each statement execute function reject_store_mutation();

create trigger batch_outbox_retained
before delete or truncate on batch_outbox
for each statement execute function reject_store_mutation();

create function protect_outbox_batch_identity() returns trigger
language plpgsql as $$
begin
    if new.batch_id is distinct from old.batch_id then
        raise exception 'batch_id cannot change' using errcode = '42501';
    end if;
    return new;
end;
$$;

create trigger batch_outbox_identity
before update on batch_outbox
for each row execute function protect_outbox_batch_identity();
