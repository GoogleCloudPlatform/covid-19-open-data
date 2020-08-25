[Back to main page](../README.md)

# By Sex
Epidemiology and hospitalizations data stratified by sex.

Values in this table are stratified versions of the columns available in the
[epidemiology](./table-epidemiology.md) and [hospitalizatons](./table-hospitalizations.md) tables.
Each row contains each variable with either `_male`, `_female` or `_sex_other` suffix:
`{new_deceased_male: 45, new_deceased_female: 32, new_deceased_sex_other: 10, new_tested_male: 45, new_tested_female: 32, ...}`.

Several things worth noting about this table:
* This table contains very sparse data, with very few combinations of regions and variables
  available.
* Records without a known sex are discarded, so the sum of all ages may not necessary amount to
  the variable from the corresponding table.

## URL
This table can be found at the following URLs depending on the choice of format:
* [by-sex.csv](https://storage.googleapis.com/covid19-open-data/v2/by-sex.csv)
* [by-sex.json](https://storage.googleapis.com/covid19-open-data/v2/by-sex.json)

## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | FR |
| **`${statistic}_sex_male`** | `integer` | Value of `${statistic}` for male individuals | 87 |
| **`${statistic}_sex_female`** | `integer` | Value of `${statistic}` for female individuals | 68 |
| **`${statistic}_sex_other`** | `integer` | Value of `${statistic}` for other individuals | 12 |

</details>
