create table pricing_plan_fees (
    pricing_plan_id bigint not null,
    fee_id bigint not null,
    amount numeric(19, 4) not null,
    constraint pk_pricing_plan_fees primary key (pricing_plan_id, fee_id),
    constraint fk_pricing_plan_fees_pricing_plan_id
        foreign key (pricing_plan_id) references pricing_plans (id) on delete cascade,
    constraint fk_pricing_plan_fees_fee_id
        foreign key (fee_id) references fees (id)
);

create table pricing_plan_fee_reasons (
    pricing_plan_id bigint not null,
    fee_id bigint not null,
    reason_id bigint not null,
    constraint pk_pricing_plan_fee_reasons primary key (pricing_plan_id, fee_id, reason_id),
    constraint fk_pricing_plan_fee_reasons_pricing_plan_fee
        foreign key (pricing_plan_id, fee_id) references pricing_plan_fees (pricing_plan_id, fee_id) on delete cascade,
    constraint fk_pricing_plan_fee_reasons_reason_id
        foreign key (reason_id) references eligibility_reasons (id) on delete cascade
);
