[Back to main page](../README.md)

# Geography
Information related to the geography for each region.

## URL
This table can be found at the following URLs depending on the choice of format:
* [demographics.csv](https://storage.googleapis.com/covid19-open-data/v2/demographics.csv)
* [demographics.json](https://storage.googleapis.com/covid19-open-data/v2/demographics.json)

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
