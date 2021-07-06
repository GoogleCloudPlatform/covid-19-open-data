[Back to main page](../README.md)

# Daily cases
This dataset contains daily confirmed cases of COVID-19 in England reported by
lower-tier local authority (LTLA), beginning January 30, 2020. The data are
collected and published by the UK government.


## URLs
Historical copies of all raw data is stored here, where the date "20210419" can
be edited to retrieve any date starting from January 30, 2020:
* [https://storage.cloud.google.com/covid19-open-data/dm-c19-data/raw/20210419/daily_cases.csv](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/raw/20210419/daily_cases.csv).

All standardized data is stored here, where the date "20210419" can be edited to
retrieve any date starting from January 30, 2020:
* [https://storage.cloud.google.com/covid19-open-data/dm-c19-data/standardized/20210419/daily_cases.csv](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/standardized/20210419/daily_cases.csv).

All mergeable data is stored here, where the date "20210419" can be edited to
retrieve any date starting from January 30, 2020:
* [https://storage.cloud.google.com/covid19-open-data/dm-c19-data/mergeable/20210419/daily_cases.csv](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/mergeable/20210419/daily_cases.csv).


## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | US_CA |
| **new_confirmed** | `integer` | Confirmed positive COVID-19 tests reported this day | 2163 |

## Sources of data

<details>
<summary>Show data sources</summary>


| Data | Source | License and Terms of Use |
| ---- | ------ | ------------------------ |
| Daily Cases | [Coronavirus cases - latest](https://coronavirus.data.gov.uk/downloads/csv/coronavirus-cases_latest.csv)
 | [OGL v.3](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) |

</details>
