revoke all on database "${accountDatabase}" from public;
revoke all on database "${accountDatabase}" from "${mockServerLogin}", "${engineLogin}";
grant connect on database "${accountDatabase}" to "${mockServerLogin}", "${engineLogin}";

revoke all on schema public from public, "${mockServerLogin}", "${engineLogin}";
grant usage on schema public to "${mockServerLogin}", "${engineLogin}";
revoke all on all tables in schema public from public, "${mockServerLogin}", "${engineLogin}";
revoke all on all sequences in schema public from public, "${mockServerLogin}", "${engineLogin}";

grant insert, select on accounts, account_attributes, fee_requests, batch_outbox
to "${mockServerLogin}";
grant usage, select on all sequences in schema public to "${mockServerLogin}";

grant select on accounts, account_attributes, fee_requests, batch_outbox to "${engineLogin}";
grant update on batch_outbox to "${engineLogin}";
