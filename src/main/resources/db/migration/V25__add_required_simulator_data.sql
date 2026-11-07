-- Source: scripts\seed\001_reference_data.sql
insert into branches (branch_code, branch_name, state, zip_code, updated_on, updated_by)
values
    ('BR100', 'Downtown Branch', 'NY', '10001', current_timestamp, 'seed'),
    ('BR101', 'Harbor Branch', 'NY', '11201', current_timestamp, 'seed'),
    ('BR200', 'Lakeside Branch', 'IL', '60601', current_timestamp, 'seed'),
    ('BR201', 'Northside Branch', 'IL', '60611', current_timestamp, 'seed'),
    ('BR300', 'Hill Country Branch', 'TX', '78701', current_timestamp, 'seed'),
    ('BR301', 'Riverwalk Branch', 'TX', '78205', current_timestamp, 'seed');

insert into products (product_code, product_name, product_type, updated_on, updated_by)
values
    ('CHK', 'Everyday Checking', 'DEPOSIT', current_timestamp, 'seed'),
    ('CHKPLUS', 'Premium Checking', 'DEPOSIT', current_timestamp, 'seed'),
    ('SAV', 'High Yield Savings', 'DEPOSIT', current_timestamp, 'seed'),
    ('MMSAV', 'Money Market Savings', 'DEPOSIT', current_timestamp, 'seed'),
    ('CD12', '12 Month Certificate', 'CD', current_timestamp, 'seed'),
    ('CD3', '3 Month Certificate', 'CD', current_timestamp, 'seed'),
    ('AUTO', 'Auto Loan', 'CREDIT', current_timestamp, 'seed'),
    ('PERS', 'Personal Loan', 'CREDIT', current_timestamp, 'seed'),
    ('MORT', 'Home Mortgage', 'CREDIT', current_timestamp, 'seed');

insert into regions (region_code, region_name, updated_on, updated_by)
values
    ('EAST', 'Eastern Region', current_timestamp, 'seed'),
    ('MID', 'Midwest Region', current_timestamp, 'seed'),
    ('SOUTH', 'Southern Region', current_timestamp, 'seed');

insert into region_branches (region_id, branch_id)
select region.id, branch.id
from regions region
join branches branch on (region.region_code, branch.branch_code) in (
    ('EAST', 'BR100'), ('EAST', 'BR101'), ('MID', 'BR200'),
    ('MID', 'BR201'), ('SOUTH', 'BR300'), ('SOUTH', 'BR301')
);

insert into region_states (region_id, state_code)
select region.id, state_code
from regions region
join (values ('EAST', 'NY'), ('MID', 'IL'), ('SOUTH', 'TX')) as states(region_code, state_code)
    on states.region_code = region.region_code;

insert into region_zip_codes (region_id, zip_code)
select region.id, zip_code
from regions region
join (values
    ('EAST', '10001'), ('EAST', '11201'), ('MID', '60601'),
    ('MID', '60611'), ('SOUTH', '78701'), ('SOUTH', '78205')
) as zip_codes(region_code, zip_code) on zip_codes.region_code = region.region_code;

-- Source: scripts\seed\002_account_attributes_and_eligibility.sql
insert into account_attributes (attribute_code, attribute_name, attribute_type, updated_on, updated_by)
values
    ('AGE', 'Account Holder Age', 'INTEGER', current_timestamp, 'seed'),
    ('BAL', 'Account Balance', 'DECIMAL', current_timestamp, 'seed'),
    ('STATE', 'Customer State', 'TEXT', current_timestamp, 'seed'),
    ('TENURE', 'Customer Tenure', 'INTEGER', current_timestamp, 'seed'),
    ('INCOME', 'Annual Income', 'DECIMAL', current_timestamp, 'seed'),
    ('LTV', 'Loan To Value Ratio', 'DECIMAL', current_timestamp, 'seed'),
    ('TERM', 'Loan Term Months', 'INTEGER', current_timestamp, 'seed'),
    ('OPENED', 'Account Opened Date', 'DATE', current_timestamp, 'seed'),
    ('AUTOPAY', 'Auto Pay Enrolled', 'BOOLEAN', current_timestamp, 'seed');

insert into account_attribute_product_types (attribute_id, product_type)
select attribute.id, product_types.product_type
from account_attributes attribute
join (values
    ('AGE', 'DEPOSIT'), ('AGE', 'CD'), ('AGE', 'CREDIT'),
    ('BAL', 'DEPOSIT'), ('BAL', 'CD'), ('STATE', 'CREDIT'),
    ('TENURE', 'DEPOSIT'), ('INCOME', 'CREDIT'), ('LTV', 'CREDIT'),
    ('TERM', 'CREDIT'), ('OPENED', 'DEPOSIT'), ('OPENED', 'CD'),
    ('AUTOPAY', 'CREDIT')
) as product_types(attribute_code, product_type)
    on product_types.attribute_code = attribute.attribute_code;

insert into eligibility_reasons (reason_code, reason_name, updated_on, updated_by)
values
    ('LOYAL', 'Loyal Customer', current_timestamp, 'seed'),
    ('PREMIUM', 'Premium Balance', current_timestamp, 'seed'),
    ('INCOME', 'High Income Borrower', current_timestamp, 'seed'),
    ('LOWLTV', 'Low Loan To Value', current_timestamp, 'seed'),
    ('YOUNG', 'Young Customer', current_timestamp, 'seed'),
    ('ESTABLISHED', 'Established Account', current_timestamp, 'seed'),
    ('SENIOR', 'Senior Customer', current_timestamp, 'seed'),
    ('AUTOPAY', 'Auto Pay Enrolled', current_timestamp, 'seed'),
    ('SAVAGE', 'Mature Saver', current_timestamp, 'seed'),
    ('SAVBAL', 'Savings Balance', current_timestamp, 'seed'),
    ('SAVTENURE', 'Long-Term Saver', current_timestamp, 'seed'),
    ('CHKPAGE', 'Mature Premium Customer', current_timestamp, 'seed'),
    ('CHKPBAL', 'Premium Checking Balance', current_timestamp, 'seed'),
    ('CHKPTENURE', 'Long-Term Premium Customer', current_timestamp, 'seed');

insert into eligibility_reason_conditions (reason_id, attribute_id, operator, attribute_value)
select reason.id, attribute.id, conditions.operator, conditions.attribute_value
from eligibility_reasons reason
join (values
    ('LOYAL', 'TENURE', '>=', '5'),
    ('PREMIUM', 'BAL', '>=', '50000'),
    ('INCOME', 'INCOME', '>=', '75000'), ('LOWLTV', 'LTV', '<=', '80'),
    ('YOUNG', 'AGE', '<', '40'), ('ESTABLISHED', 'OPENED', '<=', '2020-01-01'),
    ('SENIOR', 'AGE', '>=', '60'), ('AUTOPAY', 'AUTOPAY', '=', 'true'),
    ('SAVAGE', 'AGE', '>=', '40'), ('SAVBAL', 'BAL', '>=', '20000'),
    ('SAVTENURE', 'TENURE', '>=', '7'), ('CHKPAGE', 'AGE', '>=', '50'),
    ('CHKPBAL', 'BAL', '>=', '45000'), ('CHKPTENURE', 'TENURE', '>=', '9')
) as conditions(reason_code, attribute_code, operator, attribute_value)
    on conditions.reason_code = reason.reason_code
join account_attributes attribute on attribute.attribute_code = conditions.attribute_code;

-- Source: scripts\seed\003_fees.sql
insert into fees (fee_code, fee_name, fee_type, updated_on, updated_by)
values
    ('ANNUAL', 'Annual Fee', 'FLAT', current_timestamp, 'seed'),
    ('ATM', 'ATM Fee', 'FLAT', current_timestamp, 'seed'),
    ('EARLYWDRAW', 'Early Withdrawal Fee', 'PERCENT', current_timestamp, 'seed'),
    ('EXTOVERDRAFT', 'Extended Overdraft Fee', 'FLAT', current_timestamp, 'seed'),
    ('FOREIGNATM', 'Foreign ATM Fee', 'FLAT', current_timestamp, 'seed'),
    ('FOREIGNTXN', 'Foreign Transaction Fee', 'PERCENT', current_timestamp, 'seed'),
    ('INACTIVITY', 'Inactivity Fee', 'FLAT', current_timestamp, 'seed'),
    ('LATEPMT', 'Late Payment Fee', 'FLAT', current_timestamp, 'seed'),
    ('MONTHLY', 'Monthly Fee', 'FLAT', current_timestamp, 'seed'),
    ('ORIGIN', 'Origination Fee', 'PERCENT', current_timestamp, 'seed'),
    ('OVERDRAFT', 'Overdraft Fee', 'FLAT', current_timestamp, 'seed'),
    ('PROCESS', 'Processing Fee', 'FLAT', current_timestamp, 'seed'),
    ('RETPAYMENT', 'Returned Payment Fee', 'FLAT', current_timestamp, 'seed'),
    ('WIRE', 'Wire Fee', 'FLAT', current_timestamp, 'seed');

insert into fee_product_types (fee_id, product_type)
select fee.id, product_types.product_type
from fees fee
join (values
    ('ANNUAL', 'DEPOSIT'), ('ANNUAL', 'CREDIT'),
    ('ATM', 'DEPOSIT'), ('EARLYWDRAW', 'CD'),
    ('EXTOVERDRAFT', 'DEPOSIT'), ('FOREIGNATM', 'DEPOSIT'),
    ('FOREIGNTXN', 'DEPOSIT'), ('INACTIVITY', 'DEPOSIT'),
    ('LATEPMT', 'CREDIT'), ('MONTHLY', 'DEPOSIT'),
    ('ORIGIN', 'CREDIT'), ('OVERDRAFT', 'DEPOSIT'),
    ('PROCESS', 'CREDIT'), ('RETPAYMENT', 'DEPOSIT'),
    ('RETPAYMENT', 'CREDIT'), ('WIRE', 'DEPOSIT')
) as product_types(fee_code, product_type) on product_types.fee_code = fee.fee_code;

-- Source: scripts\seed\004_pricing_plans.sql
insert into pricing_plans (
    plan_code, plan_name, product_id, region_id, active_from, active_through, updated_on, updated_by
)
select plans.plan_code, plans.plan_name, product.id, region.id,
       plans.active_from, plans.active_through, current_timestamp, 'seed'
from (values
    ('CHK27H1', '2027 Checking First Half', 'CHK', 'EAST', date '2027-01-01', date '2027-06-30'),
    ('CHK27H2', '2027 Checking Second Half', 'CHK', 'EAST', date '2027-07-01', date '2027-12-31'),
    ('CHKP27H1', '2027 Premium Checking First Half', 'CHKPLUS', 'MID', date '2027-01-01', date '2027-06-30'),
    ('CHKP27H2', '2027 Premium Checking Second Half', 'CHKPLUS', 'MID', date '2027-07-01', date '2027-12-31'),
    ('SAV27H1', '2027 Savings First Half', 'SAV', 'MID', date '2027-01-01', date '2027-06-30'),
    ('SAV27H2', '2027 Savings Second Half', 'SAV', 'MID', date '2027-07-01', date '2027-12-31'),
    ('MMS27H1', '2027 Money Market First Half', 'MMSAV', 'EAST', date '2027-01-01', date '2027-06-30'),
    ('MMS27H2', '2027 Money Market Second Half', 'MMSAV', 'EAST', date '2027-07-01', date '2027-12-31'),
    ('CD27H1', '2027 Certificate First Half', 'CD12', 'SOUTH', date '2027-01-01', date '2027-06-30'),
    ('CD27H2', '2027 Certificate Second Half', 'CD12', 'SOUTH', date '2027-07-01', date '2027-12-31'),
    ('CD327H1', '2027 3 Month Certificate First Half', 'CD3', 'MID', date '2027-01-01', date '2027-06-30'),
    ('CD327H2', '2027 3 Month Certificate Second Half', 'CD3', 'MID', date '2027-07-01', date '2027-12-31'),
    ('AUTO27H1', '2027 Auto Loan First Half', 'AUTO', 'EAST', date '2027-01-01', date '2027-06-30'),
    ('AUTO27H2', '2027 Auto Loan Second Half', 'AUTO', 'EAST', date '2027-07-01', date '2027-12-31'),
    ('PERS27H1', '2027 Personal Loan First Half', 'PERS', 'MID', date '2027-01-01', date '2027-06-30'),
    ('PERS27H2', '2027 Personal Loan Second Half', 'PERS', 'MID', date '2027-07-01', date '2027-12-31'),
    ('MORT27H1', '2027 Home Mortgage First Half', 'MORT', 'SOUTH', date '2027-01-01', date '2027-06-30'),
    ('MORT27H2', '2027 Home Mortgage Second Half', 'MORT', 'SOUTH', date '2027-07-01', date '2027-12-31')
) as plans(plan_code, plan_name, product_code, region_code, active_from, active_through)
join products product on product.product_code = plans.product_code
join regions region on region.region_code = plans.region_code;

insert into pricing_plan_fees (pricing_plan_id, fee_id, amount)
select plan.id, fee.id,
       case
           when product.product_code in ('CHK', 'CHKPLUS') and fee.fee_code = 'MONTHLY' then 12.00
           when product.product_code = 'MORT' and fee.fee_code = 'PROCESS' then 500.00
           else prices.amount
       end
from pricing_plans plan
join products product on product.id = plan.product_id
join fee_product_types fee_product_type on fee_product_type.product_type = product.product_type
join fees fee on fee.id = fee_product_type.fee_id
join (values
    ('DEPOSIT', 'ANNUAL', 25.00), ('DEPOSIT', 'ATM', 3.00),
    ('DEPOSIT', 'EXTOVERDRAFT', 35.00), ('DEPOSIT', 'FOREIGNATM', 5.00),
    ('DEPOSIT', 'FOREIGNTXN', 3.00), ('DEPOSIT', 'INACTIVITY', 10.00),
    ('DEPOSIT', 'MONTHLY', 5.00), ('DEPOSIT', 'OVERDRAFT', 35.00),
    ('DEPOSIT', 'RETPAYMENT', 25.00), ('DEPOSIT', 'WIRE', 20.00),
    ('CD', 'EARLYWDRAW', 2.50),
    ('CREDIT', 'ANNUAL', 50.00), ('CREDIT', 'LATEPMT', 35.00),
    ('CREDIT', 'ORIGIN', 1.00), ('CREDIT', 'PROCESS', 150.00),
    ('CREDIT', 'RETPAYMENT', 25.00)
) as prices(product_type, fee_code, amount)
    on prices.product_type = product.product_type
   and prices.fee_code = fee.fee_code;

insert into pricing_plan_fee_reasons (pricing_plan_id, fee_id, reason_id)
select plan.id, fee.id, reason.id
from (values
    ('CHK27H1', 'MONTHLY', 'LOYAL'), ('CHK27H2', 'MONTHLY', 'LOYAL'),
    ('CHK27H1', 'MONTHLY', 'YOUNG'), ('CHK27H2', 'MONTHLY', 'YOUNG'),
    ('CHK27H1', 'ANNUAL', 'YOUNG'), ('CHK27H2', 'ANNUAL', 'YOUNG'),
    ('CHK27H1', 'ATM', 'YOUNG'), ('CHK27H2', 'ATM', 'YOUNG'),
    ('CHK27H1', 'OVERDRAFT', 'YOUNG'), ('CHK27H2', 'OVERDRAFT', 'YOUNG'),
    ('CHKP27H1', 'MONTHLY', 'LOYAL'), ('CHKP27H2', 'MONTHLY', 'LOYAL'),
    ('CHKP27H1', 'MONTHLY', 'ESTABLISHED'),
    ('CHKP27H2', 'MONTHLY', 'ESTABLISHED'),
    ('CHKP27H1', 'ANNUAL', 'CHKPAGE'), ('CHKP27H2', 'ANNUAL', 'CHKPAGE'),
    ('CHKP27H1', 'ATM', 'CHKPBAL'), ('CHKP27H2', 'ATM', 'CHKPBAL'),
    ('CHKP27H1', 'OVERDRAFT', 'CHKPTENURE'), ('CHKP27H2', 'OVERDRAFT', 'CHKPTENURE'),
    ('SAV27H1', 'MONTHLY', 'PREMIUM'), ('SAV27H2', 'MONTHLY', 'PREMIUM'),
    ('SAV27H1', 'MONTHLY', 'ESTABLISHED'), ('SAV27H2', 'MONTHLY', 'ESTABLISHED'),
    ('SAV27H1', 'ANNUAL', 'SAVAGE'), ('SAV27H2', 'ANNUAL', 'SAVAGE'),
    ('SAV27H1', 'ATM', 'SAVBAL'), ('SAV27H2', 'ATM', 'SAVBAL'),
    ('SAV27H1', 'OVERDRAFT', 'SAVTENURE'), ('SAV27H2', 'OVERDRAFT', 'SAVTENURE'),
    ('MMS27H1', 'MONTHLY', 'PREMIUM'), ('MMS27H2', 'MONTHLY', 'PREMIUM'),
    ('CD27H1', 'EARLYWDRAW', 'SENIOR'), ('CD27H2', 'EARLYWDRAW', 'SENIOR'),
    ('AUTO27H1', 'ORIGIN', 'INCOME'), ('AUTO27H2', 'ORIGIN', 'INCOME'),
    ('AUTO27H1', 'ORIGIN', 'AUTOPAY'), ('AUTO27H2', 'ORIGIN', 'AUTOPAY'),
    ('PERS27H1', 'PROCESS', 'INCOME'), ('PERS27H2', 'PROCESS', 'INCOME'),
    ('MORT27H1', 'ORIGIN', 'LOWLTV'), ('MORT27H1', 'PROCESS', 'LOWLTV'),
    ('MORT27H2', 'ORIGIN', 'LOWLTV'), ('MORT27H2', 'PROCESS', 'LOWLTV')
) as assignments(plan_code, fee_code, reason_code)
join pricing_plans plan on plan.plan_code = assignments.plan_code
join fees fee on fee.fee_code = assignments.fee_code
join eligibility_reasons reason on reason.reason_code = assignments.reason_code;

