alter table region_branches
    add constraint uk_region_branches_branch_code unique (branch_code);

alter table region_states
    add constraint uk_region_states_state_code unique (state_code);

alter table region_zip_codes
    add constraint uk_region_zip_codes_zip_code unique (zip_code);
