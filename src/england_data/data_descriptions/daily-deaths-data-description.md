[Back to main page](../README.md)

# Daily deaths
This dataset contains daily deaths associated with COVID-19 in England, reported
by individual NHS trust, beginning March 1, 2020. The data are collected and
published by the UK government.


## URLs
Historical copies of all raw data is stored here, where the date "20210419" can
be edited to retrieve any date starting from March 1, 2020:
* [https://storage.cloud.google.com/covid19-open-data/dm-c19-data/raw/20210419/daily_deaths.csv](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/raw/20210419/daily_deaths.csv).

All standardized data is stored here, where the date "20210419" can be edited to
retrieve any date starting from March 1, 2020:
* [https://storage.cloud.google.com/covid19-open-data/dm-c19-data/standardized/20210419/daily_deaths.csv](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/standardized/20210419/daily_deaths.csv).

All mergeable data is stored here, where the date "20210419" can be edited to
retrieve any date starting from March 1, 2020:
* [https://storage.cloud.google.com/covid19-open-data/dm-c19-data/mergeable/20210419/daily_deaths.csv](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/mergeable/20210419/daily_deaths.csv).


## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | US_CA |
| **new_deceased** | `integer` | Confirmed COVID-19 related deaths reported this day | 642 |

## Sources of data

<details>
<summary>Show data sources</summary>


| Data | Source | License and Terms of Use |
| ---- | ------ | ------------------------ |
| Daily Deaths | [COVID-19 daily deaths - latest](https://www.england.nhs.uk/statistics/statistical-work-areas/covid-19-daily-deaths)
 | [OGL v.3](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) |

</details>
