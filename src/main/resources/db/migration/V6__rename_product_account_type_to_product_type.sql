alter table products
    rename column account_type to product_type;

alter table products
    rename constraint chk_products_account_type to chk_products_product_type;
