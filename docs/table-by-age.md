[Back to main page](../README.md)

# By Age
Epidemiology and hospitalizations data stratified by age.

Values in this table are stratified versions of the columns available in the
[epidemiology](#epidemiology) and [hospitalizatons](#hospitalizations) tables. Each row contains up
to 10 distinct bins, for example:
`{new_deceased_age_00: 1, new_deceased_age_01: 45, ... , new_deceased_age_09: 32}`.

Each row may have different bins, depending on the data source. This table tries to capture the raw
data with as much fidelity as possible up to 10 bins. The range of each bin is encoded into the
`age_bin_${index}` variable, for example:
`{age_bin_00: 0-9, age_bin_01: 10-19, age_bin_02: 20-29, ... , age_bin_09: 90-}`.

Several things worth noting about this table:
* This table contains very sparse data, with very few combinations of regions and variables
  available.
* Records without a known age bin are discarded, so the sum of all ages may not necessary amount to
  the variable from the corresponding table.
* The upper and lower range of the range are inclusive values. For example, range `0-9` includes
  individuals with age zero up to (and including) 9.
* A row may have less than 10 bins, but never more than 10. For example:
  `{age_bin_00: 0-49, age_bin_01: 50-, age_bin_02: null, ...}`

## URL
This table can be found at the following URLs depending on the choice of format:
* [by-age.csv](https://storage.googleapis.com/covid19-open-data/v2/by-age.csv)
* [by-age.json](https://storage.googleapis.com/covid19-open-data/v2/by-age.json)

## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | FR |
| **`${statistic}`\_age\_bin\_`${index}`** | `integer` | Value of `${statistic}` for age bin `${index}` | 139 |
| **age\_bin\_`${index}`** | `integer` | Range for the age values inside of bin `${index}`, both ends inclusive | 30-39 |
