[Back to main page](../README.md)

# Population
This dataset contains population by demographic in England reported by clinical
commissiong group (CCG) recorded in 2019. The data are collected and published
by the UK government.


## URLs
A copy of the raw data is stored here:
* [https://storage.cloud.google.com/covid19-open-data/dm-c19-data/raw/SAPE22DT6a-mid-2019-ccg-2020-estimates-unformatted.xlsx](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/raw/SAPE22DT6a-mid-2019-ccg-2020-estimates-unformatted.xlsx).

All standardized data is stored here, where the date "20210419" can be edited to
retrieve any date starting from January 30, 2020 (though the data is always the
same because it is not recorded daily):
* [https://storage.cloud.google.com/covid19-open-data/dm-c19-data/standardized/20210419/population.csv](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/standardized/20210419/population.csv).

All mergeable data is stored here, where the date "20210419" can be edited to
retrieve any date starting from January 30, 2020 (though the data is always the
same because it is not recorded daily):
* [https://storage.cloud.google.com/covid19-open-data/dm-c19-data/mergeable/20210419/population.csv](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/mergeable/20210419/population.csv).


## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the CCG | GB_ENG_E38000007 |
| **population** | `integer` | Total population | 10000 |
| **population_female** | `integer` | Population classified as female | 5000 |
| **population_male** | `integer` | Population classified as male | 5000 |
| **population_age_00_09** | `integer` | Population classified as age 0-9 | 1000 |
| **population_age_10_19** | `integer` | Population classified as age 10-19 | 1000 |
| **population_age_20_29** | `integer` | Population classified as age 20-29 | 1000 |
| **population_age_30_39** | `integer` | Population classified as age 30-39 | 1000 |
| **population_age_40_49** | `integer` | Population classified as age 40-49 | 1000 |
| **population_age_50_59** | `integer` | Population classified as age 50-59 | 1000 |
| **population_age_60_69** | `integer` | Population classified as age 60-69 | 1000 |
| **population_age_70_79** | `integer` | Population classified as age 70-79 | 1000 |
| **population_age_80_89** | `integer` | Population classified as age 80-89 | 1000 |
| **population_age_90_99** | `integer` | Population classified as age 90-99 | 1000 |
| **population_age_80_and_older** | `integer` | Population classified as age 80 and older | 2000 |


## Sources of data

<details>
<summary>Show data sources</summary>


| Data | Source | License and Terms of Use |
| ---- | ------ | ------------------------ |
| Population | [Population 2019](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/clinicalcommissioninggroupmidyearpopulationestimates), [Raw .zip](https://www.ons.gov.uk/file?uri=%2fpeoplepopulationandcommunity%2fpopulationandmigration%2fpopulationestimates%2fdatasets%2fclinicalcommissioninggroupmidyearpopulationestimates%2fmid2019sape22dt6a/sape22dt6amid2019ccg2020estimatesunformatted.zip)
 | [OGL v.3](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) |

</details>
