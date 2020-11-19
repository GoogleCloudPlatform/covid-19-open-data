[Back to main page](../README.md)

# Epidemiology
Information related to the COVID-19 infections for each date-region pair.

## URL
This table can be found at the following URLs depending on the choice of format:
* [epidemiology.csv](https://storage.googleapis.com/covid19-open-data/v2/epidemiology.csv)
* [epidemiology.json](https://storage.googleapis.com/covid19-open-data/v2/epidemiology.json)

## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **new_confirmed<sup>1</sup>** | `integer` | Count of new cases confirmed after positive test on this date | 34 |
| **new_deceased<sup>1</sup>** | `integer` | Count of new deaths from a positive COVID-19 case on this date | 2 |
| **new_recovered<sup>1</sup>** | `integer` | Count of new recoveries from a positive COVID-19 case on this date | 13 |
| **new_tested<sup>2</sup>** | `integer` | Count of new COVID-19 tests performed on this date | 13 |
| **total_confirmed<sup>3</sup>** | `integer` | Cumulative sum of cases confirmed after positive test to date | 6447 |
| **total_deceased<sup>3</sup>** | `integer` | Cumulative sum of deaths from a positive COVID-19 case to date | 133 |
| **total_tested<sup>2,3</sup>** | `integer` | Cumulative sum of COVID-19 tests performed to date | 133 |

<sup>1</sup>Values can be negative, typically indicating a correction or an adjustment in the way
they were measured. For example, a case might have been incorrectly flagged as recovered one date so
it will be subtracted from the following date.\
<sup>2</sup>Some health authorities only report PCR testing. This variable usually refers to total
number of tests and not tested persons, but some health authorities only report tested persons.\
<sup>3</sup>Total count will not always amount to the sum of daily counts, because many authorities
make changes to criteria for counting cases, but not always make adjustments to the data. There is
also potential missing data. All of that makes the total counts *drift* away from the sum of all
daily counts over time, which is why the cumulative values, if reported, are kept in a separate
column.
