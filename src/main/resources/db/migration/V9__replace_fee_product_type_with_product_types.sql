create table fee_product_types (
    fee_id bigint not null,
    sort_order integer not null,
    product_type varchar(20) not null,
    constraint pk_fee_product_types primary key (fee_id, sort_order),
    constraint fk_fee_product_types_fee_id foreign key (fee_id) references fees (id) on delete cascade,
    constraint uk_fee_product_types_fee_id_product_type unique (fee_id, product_type),
    constraint chk_fee_product_types_product_type check (product_type in ('DEPOSIT', 'CREDIT'))
);

insert into fee_product_types (fee_id, sort_order, product_type)
select id, 0, product_type
from fees;

alter table fees drop constraint chk_fees_product_type;

alter table fees drop column product_type;
