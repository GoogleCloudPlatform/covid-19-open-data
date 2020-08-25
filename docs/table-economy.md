[Back to main page](../README.md)

# Economy
Information related to the economic development for each region.

## URL
This table can be found at the following URLs depending on the choice of format:
* [demographics.csv](https://storage.googleapis.com/covid19-open-data/v2/demographics.csv)
* [demographics.json](https://storage.googleapis.com/covid19-open-data/v2/demographics.json)

## Schema
| Name | Name | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **gdp** | `integer` `[USD]` | Gross domestic product; monetary value of all finished goods and services | 24450604878 |
| **gdp_per_capita** | `integer` `[USD]` | Gross domestic product divided by total population | 1148 |
| **human_capital_index** | `double` `[0-1]` | Mobilization of the economic and professional potential of citizens | 0.765 |
