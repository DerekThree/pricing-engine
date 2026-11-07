alter table recommendation_outbox
    drop constraint fk_recommendation_outbox_batch;

alter table account_recommendations
    drop constraint fk_account_recommendations_batch;

alter table account_recommendations
    add constraint fk_account_recommendations_batch
    foreign key (batch_id) references recommendation_outbox (batch_id);

drop table recommendation_batches;
