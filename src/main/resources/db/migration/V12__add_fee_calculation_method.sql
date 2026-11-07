alter table fees add column calc_method varchar(20);

update fees set calc_method = 'FIXED';

alter table fees alter column calc_method set not null;

alter table fees add constraint chk_fees_calc_method check (calc_method in ('FIXED', 'PERCENT'));
