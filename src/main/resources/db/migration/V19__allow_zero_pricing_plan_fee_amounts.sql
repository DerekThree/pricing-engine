alter table pricing_plan_fees
    drop constraint chk_pricing_plan_fees_amount_positive;

alter table pricing_plan_fees
    add constraint chk_pricing_plan_fees_amount_nonnegative
        check (amount >= 0);
