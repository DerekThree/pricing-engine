create table account_attribute_product_types (
    attribute_id bigint not null,
    sort_order integer not null,
    product_type varchar(20) not null,
    constraint pk_account_attribute_product_types primary key (attribute_id, sort_order),
    constraint fk_account_attribute_product_types_attribute_id foreign key (attribute_id)
        references account_attributes (id) on delete cascade,
    constraint uk_account_attribute_product_types_attribute_id_product_type
        unique (attribute_id, product_type),
    constraint chk_account_attribute_product_types_product_type
        check (product_type in ('DEPOSIT', 'CD', 'CREDIT'))
);
