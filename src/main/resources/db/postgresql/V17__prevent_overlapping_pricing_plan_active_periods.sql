create extension if not exists btree_gist;

alter table pricing_plans
    add constraint excl_pricing_plans_active_period
        exclude using gist (
            product_id with =,
            region_id with =,
            daterange(active_from, active_through, '[]') with &&
        );
