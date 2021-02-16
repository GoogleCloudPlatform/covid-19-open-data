[Back to main page](../README.md)

# Government Response
Summary of a government's response to the events, including a *stringency index*, collected from
[University of Oxford][1].

For more information about each field and how the overall stringency index is
computed, see the [Oxford COVID-19 government response tracker][1].


## URL
This table can be found at the following URLs depending on the choice of format:
* [oxford-government-response.csv](https://storage.googleapis.com/covid19-open-data/v3/oxford-government-response.csv)
* [oxford-government-response.json](https://storage.googleapis.com/covid19-open-data/v3/oxford-government-response.json)


## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | US_CA |
| **school_closing** | `integer` `[0-3]` | Schools are closed | 2 |
| **workplace_closing** | `integer` `[0-3]` | Workplaces are closed | 2 |
| **cancel_public_events** | `integer` `[0-3]` | Public events have been cancelled | 2 |
| **restrictions_on_gatherings** | `integer` `[0-3]` | Gatherings of non-household members are restricted | 2 |
| **public_transport_closing** | `integer` `[0-3]` | Public transport is not operational | 0 |
| **stay_at_home_requirements** | `integer` `[0-3]` | Self-quarantine at home is mandated for everyone | 0 |
| **restrictions_on_internal_movement** | `integer` `[0-3]` | Travel within country is restricted | 1 |
| **international_travel_controls** | `integer` `[0-3]` | International travel is restricted | 3 |
| **income_support** | `integer` `[USD]` | Value of fiscal stimuli, including spending or tax cuts | 20449287023 |
| **debt_relief** | `integer` `[0-3]` | Debt/contract relief for households | 0 |
| **fiscal_measures** | `integer` `[USD]` | Value of fiscal stimuli, including spending or tax cuts | 20449287023 |
| **international_support** | `integer` `[USD]` | Giving international support to other countries | 274000000 |
| **public_information_campaigns** | `integer` `[0-2]` | Government has launched public information campaigns | 1 |
| **testing_policy** | `integer` `[0-3]` | Country-wide COVID-19 testing policy | 1 |
| **contact_tracing** | `integer` `[0-2]` | Country-wide contact tracing policy | 1 |
| **emergency_investment_in_healthcare** | `integer` `[USD]` | Emergency funding allocated to healthcare | 500000 |
| **investment_in_vaccines** | `integer` `[USD]` | Emergency funding allocated to vaccine research | 100000 |
| **facial_coverings** | `integer` `[0-4]` | Policies on the use of facial coverings outside the home | 2 |
| **vaccination_policy** | `integer` `[0-5]` | Policies for vaccine delivery for different groups | 2 |
| **stringency_index** | `double` `[0-100]` | Overall stringency index | 71.43 |


## Sources of data

<details>
<summary>Show data sources</summary>


| Data | Source | License and Terms of Use |
| ---- | ------ | ------------------------ |
| Government response data | [Oxford COVID-19 government response tracker][1] | [CC BY](https://github.com/OxCGRT/covid-policy-tracker/blob/master/LICENSE.txt) |

</details>
