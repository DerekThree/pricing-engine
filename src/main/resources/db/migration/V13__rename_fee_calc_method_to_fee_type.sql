alter table fees drop constraint chk_fees_calc_method;

alter table fees rename column calc_method to fee_type;

update fees set fee_type = 'FLAT' where fee_type = 'FIXED';

alter table fees add constraint chk_fees_fee_type check (fee_type in ('FLAT', 'PERCENT'));
