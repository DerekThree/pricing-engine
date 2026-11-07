revoke all on database "${recommendationDatabase}" from public;
revoke all on database "${recommendationDatabase}" from "${processorLogin}", "${engineLogin}";
grant connect on database "${recommendationDatabase}" to "${processorLogin}", "${engineLogin}";

revoke all on schema public from public, "${processorLogin}", "${engineLogin}";
grant usage on schema public to "${processorLogin}", "${engineLogin}";
revoke all on all tables in schema public from public, "${processorLogin}", "${engineLogin}";
revoke all on all sequences in schema public from public, "${processorLogin}", "${engineLogin}";

grant insert, select on recommendation_outbox, account_recommendations, fee_request_recommendations,
recommendation_eligibility_reasons to "${engineLogin}";
grant usage, select on all sequences in schema public to "${engineLogin}";

grant select on recommendation_outbox, account_recommendations, fee_request_recommendations,
recommendation_eligibility_reasons to "${processorLogin}";
grant update on recommendation_outbox to "${processorLogin}";
