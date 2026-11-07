alter table pricing_plan_fees
    add constraint chk_pricing_plan_fees_amount_positive
        check (amount > 0);
