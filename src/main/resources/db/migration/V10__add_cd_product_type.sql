alter table products drop constraint chk_products_product_type;

alter table products add constraint chk_products_product_type
check (product_type in ('DEPOSIT', 'CD', 'CREDIT'));

alter table fee_product_types drop constraint chk_fee_product_types_product_type;

alter table fee_product_types add constraint chk_fee_product_types_product_type
check (product_type in ('DEPOSIT', 'CD', 'CREDIT'));
