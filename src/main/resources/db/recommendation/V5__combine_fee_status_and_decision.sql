update fee_request_recommendations
set status = decision
where status = 'OK' and decision in ('CHARGED', 'WAIVED');

alter table fee_request_recommendations drop column decision;
