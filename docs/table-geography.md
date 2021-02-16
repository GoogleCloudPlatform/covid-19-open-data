[Back to main page](../README.md)

# Geography
Information related to the geography for each region.


## URL
This table can be found at the following URLs depending on the choice of format:
* [geography.csv](https://storage.googleapis.com/covid19-open-data/v3/geography.csv)
* [geography.json](https://storage.googleapis.com/covid19-open-data/v3/geography.json)


## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **latitude** | `double` | Floating point representing the geographic coordinate | 30.9756 |
| **longitude** | `double` | Floating point representing the geographic coordinate | 112.2707 |
| **elevation** | `integer` `[meters]` | Elevation above the sea level | 875 |
| **area** | `integer` [squared kilometers] | Area encompassing this region | 3729 |
| **rural_area** | `integer` [squared kilometers] | Area encompassing rural land in this region | 3729 |
| **urban_area** | `integer` [squared kilometers] | Area encompassing urban land this region | 3729 |
| **open_street_maps** | `string` | OpenStreetMap relation ID corresponding to this key | 165475 |


## Sources of data

<details>
<summary>Show data sources</summary>


| Data | Source | License and Terms of Use |
| ---- | ------ | ------------------------ |
| Geography | [Wikidata](https://wikidata.org) | [CC0][1] |
| Geography | [WorldBank](https://worldbank.org) | [CC BY](https://www.worldbank.org/en/about/legal/terms-of-use-for-datasets) |

</details>


[1]: https://www.wikidata.org/wiki/Wikidata:Licensing
