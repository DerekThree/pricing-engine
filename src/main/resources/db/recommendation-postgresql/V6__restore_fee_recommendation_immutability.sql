create trigger fee_request_recommendations_immutable
before update or delete or truncate on fee_request_recommendations
for each statement execute function reject_store_mutation();
