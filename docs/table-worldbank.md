[Back to main page](../README.md)

# WorldBank
Most recent value for each indicator of the [WorldBank Database][1].

Refer to the [WorldBank documentation][1] for more details, or refer to the
[worldbank_indicators.csv](../src/data/worldbank_indicators.csv) file for a short description of each
indicator. Each column uses the indicator code as its name, and the rows are filled with the values
for the corresponding `key`.

Note that WorldBank data is only available at the country level and it's not included in the main
table. If no values are reported by WorldBank for the country since 2015, the row value will be
null.

## URL
This table can be found at the following URLs depending on the choice of format:
* [worldbank.csv](https://storage.googleapis.com/covid19-open-data/v2/worldbank.csv)
* [worldbank.json](https://storage.googleapis.com/covid19-open-data/v2/worldbank.json)

## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | ES |
| **`${indicator}`** | `double` | Value of the indicator corresponding to this column, column name is indicator code | 0 |

[1]: https://data.worldbank.org
