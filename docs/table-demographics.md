[Back to main page](../README.md)

# Demographics
Information related to the population demographics for each region.

## URL
This table can be found at the following URLs depending on the choice of format:
* [demographics.csv](https://storage.googleapis.com/covid19-open-data/v2/demographics.csv)
* [demographics.json](https://storage.googleapis.com/covid19-open-data/v2/demographics.json)

## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | KR |
| **population** | `integer` | Total count of humans | 51606633 |
| **population_male** | `integer` | Total count of males | 25846211 |
| **population_female** | `integer` | Total count of females | 25760422 |
| **rural_population** | `integer` | Population in a rural area | 9568386 |
| **urban_population** | `integer` | Population in an urban area | 42038247 |
| **largest_city_population** | `integer` | Population in the largest city of the region | 9963497 |
| **clustered_population** | `integer` | Population in urban agglomerations of more than 1 million | 25893097 |
| **population_density** | `double` `[persons per squared kilometer]` | Population per squared kilometer of land area | 529.3585 |
| **human_development_index** | `double` `[0-1]` | Composite index of life expectancy, education, and per capita income indicators | 0.903 |
| **population_age_`${lower}`_`${upper}`\*** | `integer` | Estimated population between the ages of `${lower}` and `${upper}`, both inclusive | 42038247 |
| **population_age_80_and_older\*** | `integer` | Estimated population over the age of 80 | 477081 |

\*Structured population data is estimated from the WorldPop project. Refer to the
[WorldPop documentation](https://www.worldpop.org/geodata/summary?id=24798) for more details.
