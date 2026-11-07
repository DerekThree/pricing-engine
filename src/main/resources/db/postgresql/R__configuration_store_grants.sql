revoke all on database "${configurationDatabase}" from public;
revoke all on database "${configurationDatabase}" from "${backendLogin}", "${engineLogin}";
grant connect on database "${configurationDatabase}" to "${backendLogin}", "${engineLogin}";

revoke all on schema public from public, "${backendLogin}", "${engineLogin}";
grant usage on schema public to "${backendLogin}", "${engineLogin}";
revoke all on all tables in schema public from public, "${backendLogin}", "${engineLogin}";
revoke all on all sequences in schema public from public, "${backendLogin}", "${engineLogin}";

grant select, insert, update, delete on
    branches, products, pricing_plans, regions, region_branches, region_states, region_zip_codes,
    account_attributes, account_attribute_product_types, eligibility_reasons,
    eligibility_reason_conditions, fees, fee_product_types, pricing_plan_fees,
    pricing_plan_fee_reasons
to "${backendLogin}";
grant usage, select on all sequences in schema public to "${backendLogin}";

grant select on
    branches, products, pricing_plans, regions, region_branches, region_states, region_zip_codes,
    account_attributes, account_attribute_product_types, eligibility_reasons,
    eligibility_reason_conditions, fees, fee_product_types, pricing_plan_fees,
    pricing_plan_fee_reasons
to "${engineLogin}";
