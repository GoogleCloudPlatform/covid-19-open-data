[Back to main page](../README.md)

# Health
Health related indicators for each region.

## URL
This table can be found at the following URLs depending on the choice of format:
* [health.csv](https://storage.googleapis.com/covid19-open-data/v2/health.csv)
* [health.json](https://storage.googleapis.com/covid19-open-data/v2/health.json)

## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | BN |
| **life_expectancy** | `double` `[years]` |Average years that an individual is expected to live | 75.722 |
| **smoking_prevalence** | `double` `[%]` | Percentage of smokers in population | 16.9 |
| **diabetes_prevalence** | `double` `[%]` | Percentage of persons with diabetes in population | 13.3 |
| **infant_mortality_rate** | `double` | Infant mortality rate (per 1,000 live births) | 9.8 |
| **adult_male_mortality_rate** | `double` | Mortality rate, adult, male (per 1,000 male adults) | 143.719 |
| **adult_female_mortality_rate** | `double` | Mortality rate, adult, female (per 1,000 male adults) | 98.803 |
| **pollution_mortality_rate** | `double` | Mortality rate attributed to household and ambient air pollution, age-standardized (per 100,000 population) | 13.3 |
| **comorbidity_mortality_rate** | `double` `[%]` | Mortality from cardiovascular disease, cancer, diabetes or cardiorespiratory disease between exact ages 30 and 70 | 16.6 |
| **hospital_beds** | `double` | Hospital beds (per 1,000 people) | 2.7 |
| **nurses** | `double` | Nurses and midwives (per 1,000 people) | 5.8974 |
| **physicians** | `double` | Physicians (per 1,000 people) | 1.609 |
| **health_expenditure** | `double` `[USD]` | Health expenditure per capita | 671.4115 |
| **out_of_pocket_health_expenditure** | `double` `[USD]` | Out-of-pocket health expenditure per capita | 34.756348 |

Note that the majority of the health indicators are only available at the country level.
