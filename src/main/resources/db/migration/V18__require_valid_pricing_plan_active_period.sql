alter table pricing_plans
    add constraint chk_pricing_plans_active_period_order
        check (active_from <= active_through);
