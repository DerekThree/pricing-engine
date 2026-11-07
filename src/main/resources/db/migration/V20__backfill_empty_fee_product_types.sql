insert into fee_product_types (fee_id, sort_order, product_type)
select fee.id, product_types.sort_order, product_types.product_type
from fees fee
cross join (
    select 0 as sort_order, 'DEPOSIT' as product_type
    union all select 1, 'CD'
    union all select 2, 'CREDIT'
) product_types
where not exists (
    select 1
    from fee_product_types
    where fee_id = fee.id
);
