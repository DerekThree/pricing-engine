alter table pricing_plan_fees
    drop constraint fk_pricing_plan_fees_fee_id;

alter table pricing_plan_fees
    add constraint fk_pricing_plan_fees_fee_id
        foreign key (fee_id) references fees (id) on delete restrict;

alter table pricing_plan_fee_reasons
    drop constraint fk_pricing_plan_fee_reasons_reason_id;

alter table pricing_plan_fee_reasons
    add constraint fk_pricing_plan_fee_reasons_reason_id
        foreign key (reason_id) references eligibility_reasons (id) on delete restrict;
