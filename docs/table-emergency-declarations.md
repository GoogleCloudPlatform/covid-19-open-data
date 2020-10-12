[Back to main page](../README.md)

# Emergency Declarations
This dataset contains emergency declarations and mitigation policies for each US state starting on
January 20. The data was aggregated by the [LawAtlas Project](https://lawatlas.org/).

For more information about each field see the
[LawAtlas Project dataset page](https://lawatlas.org/datasets/covid-19-emergency-declarations) and
the corresponding [codebook][2].

## URL
This table can be found at the following URL:
* [lawatlas-emergency-declarations.csv](https://storage.googleapis.com/covid19-open-data/v2/lawatlas-emergency-declarations.csv)

## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | US_CA |
| **lawatlas_mitigation_policy** | `integer` `[0-1]` | Has the state instituted legal action aimed at mitigating the spread of COVID-19? | 0 |
| **lawatlas_state_emergency** | `integer` `[0-1]` | Is there an emergency declaration in effect in the state? | 0 |
| **lawatlas_emerg_statewide** | `integer` `[0-1]` | Does the emergency declaration apply statewide? | 0 |
| **lawatlas_travel_requirement** | `integer` `[0-1]` | Is there a restriction on travelers? | 0 |
| **lawatlas_traveler_type_all_people_entering_the_state** | `integer` `[0-1]` | What types of travelers are restricted? | 0 |
| **lawatlas_traveler_type_travelers_from_specified_states** | `integer` `[0-1]` | What types of travelers are restricted? | 0 |
| **lawatlas_traveler_type_travelers_from_specified_countries** | `integer` `[0-1]` | What types of travelers are restricted? | 0 |
| **lawatlas_traveler_type_general_international_travelers** | `integer` `[0-1]` | What types of travelers are restricted? | 0 |
| **lawatlas_traveler_type_all_air_travelers** | `integer` `[0-1]` | What types of travelers are restricted? | 0 |
| **lawatlas_requirement_type_traveler_must_self_quarantine** | `integer` `[0-1]` | What does the traveler restriction mandate? | 0 |
| **lawatlas_requirement_type_traveler_must_inform_others_of_travel** | `integer` `[0-1]` | What does the traveler restriction mandate? | 0 |
| **lawatlas_requirement_type_checkpoints_must_be_established** | `integer` `[0-1]` | What does the traveler restriction mandate? | 0 |
| **lawatlas_requirement_type_travel_requirement_must_be_posted** | `integer` `[0-1]` | What does the traveler restriction mandate? | 0 |
| **lawatlas_travel_statewide** | `integer` `[0-1]` | Does the restriction apply statewide? | 0 |
| **lawatlas_home_requirement** | `integer` `[0-1]` | Is there a requirement that residents stay home? | 0 |
| **lawatlas_home_except_engaging_in_essential_business_activities** | `integer` `[0-1]` | What are the exceptions to the stay-at-home requirement? | 0 |
| **lawatlas_home_except_obtaining_necessary_supplies** | `integer` `[0-1]` | What are the exceptions to the stay-at-home requirement? | 0 |
| **lawatlas_home_except_accessing_emergency_services** | `integer` `[0-1]` | What are the exceptions to the stay-at-home requirement? | 0 |
| **lawatlas_home_except_caring_for_a_person_outside_the_home** | `integer` `[0-1]` | What are the exceptions to the stay-at-home requirement? | 0 |
| **lawatlas_home_except_caring_for_a_pet_outside_the_home** | `integer` `[0-1]` | What are the exceptions to the stay-at-home requirement? | 0 |
| **lawatlas_home_except_engaging_in_outdoor_activities** | `integer` `[0-1]` | What are the exceptions to the stay-at-home requirement? | 0 |
| **lawatlas_home_except_attending_religious_services** | `integer` `[0-1]` | What are the exceptions to the stay-at-home requirement? | 0 |
| **lawatlas_home_except_engaging_in_essential_health_care_services** | `integer` `[0-1]` | What are the exceptions to the stay-at-home requirement? | 0 |
| **lawatlas_home_statewide** | `integer` `[0-1]` | Does the requirement apply statewide?  | 0 |
| **lawatlas_curfew_reg** | `integer` `[0-1]` | Is there a required curfew? | 0 |
| **lawatlas_mask_requirement** | `integer` `[0-1]` | Is there a requirement that residents use masks? | 0 |
| **lawatlas_mask_statewide** | `integer` `[0-1]` | Does the requirement apply statewide?  | 0 |
| **lawatlas_business_close** | `integer` `[0-1]` | Is there a requirement that businesses close? | 0 |
| **lawatlas_business_type_all_non_essential_businesses** | `integer` `[0-1]` | What types of businesses are required to close? | 0 |
| **lawatlas_business_type_non_essential_retail_businesses** | `integer` `[0-1]` | What types of businesses are required to close? | 0 |
| **lawatlas_business_type_entertainment_businesses** | `integer` `[0-1]` | What types of businesses are required to close? | 0 |
| **lawatlas_business_type_personal_service_businesses** | `integer` `[0-1]` | What types of businesses are required to close? | 0 |
| **lawatlas_business_type_restaurants** | `integer` `[0-1]` | What types of businesses are required to close? | 0 |
| **lawatlas_business_type_bars** | `integer` `[0-1]` | What types of businesses are required to close? | 0 |
| **lawatlas_business_type_fitness_centers** | `integer` `[0-1]` | What types of businesses are required to close? | 0 |
| **lawatlas_essential_def_appliance_stores** | `integer` `[0-1]` | What is considered an essential retail business? | 0 |
| **lawatlas_essential_def_convenience_stores** | `integer` `[0-1]` | What is considered an essential retail business? | 0 |
| **lawatlas_essential_def_gas_stations** | `integer` `[0-1]` | What is considered an essential retail business? | 0 |
| **lawatlas_essential_def_grocery_stores** | `integer` `[0-1]` | What is considered an essential retail business? | 0 |
| **lawatlas_essential_def_gun_stores** | `integer` `[0-1]` | What is considered an essential retail business? | 0 |
| **lawatlas_essential_def_hardware_stores** | `integer` `[0-1]` | What is considered an essential retail business? | 0 |
| **lawatlas_essential_def_liquor_stores** | `integer` `[0-1]` | What is considered an essential retail business? | 0 |
| **lawatlas_essential_def_pharmacies** | `integer` `[0-1]` | What is considered an essential retail business? | 0 |
| **lawatlas_essential_def_marijuana_dispensaries** | `integer` `[0-1]` | What is considered an essential retail business? | 0 |
| **lawatlas_essential_def_pet_stores** | `integer` `[0-1]` | What is considered an essential retail business? | 0 |
| **lawatlas_essential_def_type_of_essential_retail_business_is_not_specified** | `integer` `[0-1]` | What is considered an essential retail business? | 0 |
| **lawatlas_essential_def_no_restriction_on_retail_businesses** | `integer` `[0-1]` | What is considered an essential retail business? | 0 |
| **lawatlas_business_statewide** | `integer` `[0-1]` | Does the requirement apply statewide? | 0 |
| **lawatlas_rest_restrict** | `integer` `[0-1]` | Is there a restriction on restaurant operations? | 0 |
| **lawatlas_service_type_takeout** | `integer` `[0-1]` | What type of service is allowed? | 0 |
| **lawatlas_service_type_delivery** | `integer` `[0-1]` | What type of service is allowed? | 0 |
| **lawatlas_service_type_limited_on_site_service** | `integer` `[0-1]` | What type of service is allowed? | 0 |
| **lawatlas_rest_statewide** | `integer` `[0-1]` | Does the requirement apply statewide? | 0 |
| **lawatlas_schools_requirement** | `integer` `[0-1]` | Is there a requirement that schools close? | 0 |
| **lawatlas_schools_type_private_elementary_schools** | `integer` `[0-1]` | What type of schools are required to close? | 0 |
| **lawatlas_schools_type_private_secondary_schools** | `integer` `[0-1]` | What type of schools are required to close? | 0 |
| **lawatlas_schools_type_public_elementary_schools** | `integer` `[0-1]` | What type of schools are required to close? | 0 |
| **lawatlas_schools_type_public_secondary_schools** | `integer` `[0-1]` | What type of schools are required to close? | 0 |
| **lawatlas_schools_type_colleges_and_universities** | `integer` `[0-1]` | What type of schools are required to close? | 0 |
| **lawatlas_schools_type_technical_schools** | `integer` `[0-1]` | What type of schools are required to close? | 0 |
| **lawatlas_schools_type_type_of_school_not_specified** | `integer` `[0-1]` | What type of schools are required to close? | 0 |
| **lawatlas_schools_statewide** | `integer` `[0-1]` | Does the requirement apply statewide? | 0 |
| **lawatlas_gathering_ban** | `integer` `[0-1]` | Is there a ban on gatherings? | 0 |
| **lawatlas_gathering_type** | `integer` `[0-17]` | What size gatherings are banned? See [codebook][2] for more information. | 0 |
| **lawatlas_gathering_statewide** | `integer` `[0-1]` | Does the requirement apply statewide? | 0 |
| **lawatlas_med_restrict** | `integer` `[0-1]` | Is there a restriction on medical procedures? | 0 |
| **lawatlas_med_except_delay_would_threaten_patients_health** | `integer` `[0-1]` | What procedures are allowed under the restriction? | 0 |
| **lawatlas_med_except_delay_would_threaten_patients_life** | `integer` `[0-1]` | What procedures are allowed under the restriction? | 0 |
| **lawatlas_med_except_procedure_needed_to_treat_emergency** | `integer` `[0-1]` | What procedures are allowed under the restriction? | 0 |
| **lawatlas_med_except_procedure_does_not_deplete_hospital_capacity** | `integer` `[0-1]` | What procedures are allowed under the restriction? | 0 |
| **lawatlas_med_except_procedure_does_not_deplete_personal_protective_equipment** | `integer` `[0-1]` | What procedures are allowed under the restriction? | 0 |
| **lawatlas_med_except_family_planning_services** | `integer` `[0-1]` | What procedures are allowed under the restriction? | 0 |
| **lawatlas_med_except_no_exception_specified** | `integer` `[0-1]` | What procedures are allowed under the restriction? | 0 |
| **lawatlas_abortion_essential_new** | `integer` `[0-1]` | Are abortion services permitted under the restriction? | 0 |
| **lawatlas_med_statewide** | `integer` `[0-1]` | Does the requirement apply statewide? | 0 |
| **lawatlas_correct_requirement** | `integer` `[0-1]` | Is there a requirement regarding the operations of correctional facilities? | 0 |
| **lawatlas_correct_facility_all_state_facilities** | `integer` `[0-1]` | To what type of facility does the requirement apply? | 0 |
| **lawatlas_correct_facility_all_department_of_corrections_facilities** | `integer` `[0-1]` | To what type of facility does the requirement apply? | 0 |
| **lawatlas_correct_facility_all_county_jails** | `integer` `[0-1]` | To what type of facility does the requirement apply? | 0 |
| **lawatlas_correct_facility_juvenile_detention_centers** | `integer` `[0-1]` | To what type of facility does the requirement apply? | 0 |
| **lawatlas_correct_type_intakes_suspended** | `integer` `[0-1]` | What type of requirement exists? | 0 |
| **lawatlas_correct_type_duty_to_receive_prisoners_suspended** | `integer` `[0-1]` | What type of requirement exists? | 0 |
| **lawatlas_correct_type_transfers_to_custody_suspended** | `integer` `[0-1]` | What type of requirement exists? | 0 |
| **lawatlas_correct_type_release_of_inmates** | `integer` `[0-1]` | What type of requirement exists? | 0 |
| **lawatlas_correct_type_rules_regarding_inmate_release_suspended** | `integer` `[0-1]` | What type of requirement exists? | 0 |
| **lawatlas_correct_type_release_notice_suspended** | `integer` `[0-1]` | What type of requirement exists? | 0 |
| **lawatlas_correct_type_cease_in_person_parole_hearings** | `integer` `[0-1]` | What type of requirement exists? | 0 |
| **lawatlas_correct_type_develop_process_for_virtual_parole_hearings** | `integer` `[0-1]` | What type of requirement exists? | 0 |
| **lawatlas_correct_type_visitation_suspended** | `integer` `[0-1]` | What type of requirement exists? | 0 |
| **lawatlas_correct_statewide** | `integer` `[0-1]` | Does the requirement apply statewide? | 0 |
| **lawatlas_state_preempt** | `integer` `[0-1]` | Does the state explicitly preempt local regulation of social distancing? | 0 |
| **lawatlas_action_preempt_imposing_additional_social_distancing_limitations_on_essential_business** | `integer` `[0-1]` | What local measures are preempted by state action? | 0 |
| **lawatlas_action_preempt_imposing_additional_restrictions_on_public_spaces** | `integer` `[0-1]` | What local measures are preempted by state action? | 0 |
| **lawatlas_action_preempt_restricting_scope_of_services_of_an_essential_business** | `integer` `[0-1]` | What local measures are preempted by state action? | 0 |
| **lawatlas_action_preempt_expanding_the_definition_of_non-essential_business** | `integer` `[0-1]` | What local measures are preempted by state action? | 0 |
| **lawatlas_action_preempt_restricting_the_hours_of_operation_of_an_essential_business** | `integer` `[0-1]` | What local measures are preempted by state action?  | 0 |
| **lawatlas_action_preempt_imposing_restrictions_that_conflict_with_state_order** | `integer` `[0-1]` | What local measures are preempted by state action? | 0 |
| **lawatlas_action_preempt_restricting_the_performance_of_an_essential_function** | `integer` `[0-1]` | What local measures are preempted by state action? | 0 |
| **lawatlas_action_preempt_restricting_people_from_leaving_home** | `integer` `[0-1]` | What local measures are preempted by state action? | 0 |
| **lawatlas_action_preempt_restricting_the_operations_of_schools** | `integer` `[0-1]` | What local measures are preempted by state action? | 0 |
| **lawatlas_action_preempt_imposing_gathering_bans** | `integer` `[0-1]` | What local measures are preempted by state action? | 0 |
| **lawatlas_local_allow** | `integer` `[0-1]` | Does the state explicitly allow local authorities to impose additional requirements? | 0 |

[2]: https://monqcle.com/upload/5f3aeae85594f45c1b8b4950/download
