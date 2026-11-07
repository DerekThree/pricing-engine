insert into account_attribute_product_types (attribute_id, sort_order, product_type)
select attribute.id, product_types.sort_order, product_types.product_type
from account_attributes attribute
cross join (
    select 0 as sort_order, 'DEPOSIT' as product_type
    union all select 1, 'CD'
    union all select 2, 'CREDIT'
) product_types
where not exists (
    select 1
    from account_attribute_product_types
    where attribute_id = attribute.id
);
