alter table fee_product_types
    drop constraint uk_fee_product_types_fee_id_product_type;

alter table fee_product_types
    drop constraint pk_fee_product_types;

alter table fee_product_types
    drop column sort_order;

alter table fee_product_types
    add constraint pk_fee_product_types primary key (fee_id, product_type);

alter table account_attribute_product_types
    drop constraint uk_account_attribute_product_types_attribute_id_product_type;

alter table account_attribute_product_types
    drop constraint pk_account_attribute_product_types;

alter table account_attribute_product_types
    drop column sort_order;

alter table account_attribute_product_types
    add constraint pk_account_attribute_product_types primary key (attribute_id, product_type);
