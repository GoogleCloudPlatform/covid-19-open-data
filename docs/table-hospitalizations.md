[Back to main page](../README.md)

# Hospitalizations
Information related to patients of COVID-19 and hospitals.

## URL
This table can be found at the following URLs depending on the choice of format:
* [demographics.csv](https://storage.googleapis.com/covid19-open-data/v2/demographics.csv)
* [demographics.json](https://storage.googleapis.com/covid19-open-data/v2/demographics.json)

## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **new_hospitalized\*** | `integer` | Count of new cases hospitalized after positive test on this date | 34 |
| **new_intensive_care\*** | `integer` | Count of new cases admitted into ICU after a positive COVID-19 test on this date | 2 |
| **new_ventilator\*** | `integer` | Count of new COVID-19 positive cases which require a ventilator on this date | 13 |
| **total_hospitalized\*\*** | `integer` | Cumulative sum of cases hospitalized after positive test to date | 6447 |
| **total_intensive_care\*\*** | `integer` | Cumulative sum of cases admitted into ICU after a positive COVID-19 test to date | 133 |
| **total_ventilator\*\*** | `integer` | Cumulative sum of COVID-19 positive cases which require a ventilator to date | 133 |
| **current_hospitalized\*\*** | `integer` | Count of current (active) cases hospitalized after positive test to date | 34 |
| **current_intensive_care\*\*** | `integer` | Count of current (active) cases admitted into ICU after a positive COVID-19 test to date | 2 |
| **current_ventilator\*\*** | `integer` | Count of current (active) COVID-19 positive cases which require a ventilator to date | 13 |

\*Values can be negative, typically indicating a correction or an adjustment in the way they were
measured. For example, a case might have been incorrectly flagged as recovered one date so it will
be subtracted from the following date.

\*\*Total count will not always amount to the sum of daily counts, because many authorities make
changes to criteria for counting cases, but not always make adjustments to the data. There is also
potential missing data. All of that makes the total counts *drift* away from the sum of all daily
counts over time, which is why the cumulative values, if reported, are kept in a separate column.
