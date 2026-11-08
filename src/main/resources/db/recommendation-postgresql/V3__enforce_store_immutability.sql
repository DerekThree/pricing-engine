create function reject_store_mutation() returns trigger
language plpgsql as $$
begin
    raise exception '% on % is not permitted', TG_OP, TG_TABLE_NAME using errcode = '42501';
end;
$$;

create trigger account_recommendations_immutable
before update or delete or truncate on account_recommendations
for each statement execute function reject_store_mutation();

create trigger fee_request_recommendations_immutable
before update or delete or truncate on fee_request_recommendations
for each statement execute function reject_store_mutation();

create trigger recommendation_eligibility_reasons_immutable
before update or delete or truncate on recommendation_eligibility_reasons
for each statement execute function reject_store_mutation();

create trigger recommendation_outbox_retained
before delete or truncate on recommendation_outbox
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

create trigger recommendation_outbox_identity
before update on recommendation_outbox
for each row execute function protect_outbox_batch_identity();
