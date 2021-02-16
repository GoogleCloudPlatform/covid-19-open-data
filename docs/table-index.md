[Back to main page](../README.md)

# Index
Non-temporal data related to countries and regions. It includes keys, codes and names for each
region, which is helpful for displaying purposes or when merging with other data.

## URL
This table can be found at the following URLs depending on the choice of format:
* [index.csv](https://storage.googleapis.com/covid19-open-data/v3/index.csv)
* [index.json](https://storage.googleapis.com/covid19-open-data/v3/index.json)

## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | US_CA_06001 |
| **place_id** | `string` | A textual identifier that uniquely identifies a place in the Google Places database and on Google Maps ([details](https://developers.google.com/places/web-service/place-id))| ChIJd_Y0eVIvkIARuQyDN0F1LBA |
| **wikidata** | `string` | Wikidata ID corresponding to this key | Q107146 |
| **datacommons** | `string` | DataCommons ID corresponding to this key | geoId/06001 |
| **country_code** | `string` | ISO 3166-1 alphanumeric 2-letter code of the country | US |
| **country_name** | `string` | American English name of the country, subject to change | United States of America |
| **subregion1_code** | `string` | (Optional) ISO 3166-2 or NUTS 2/3 code of the subregion | CA |
| **subregion1_name** | `string` | (Optional) American English name of the subregion, subject to change | California |
| **subregion2_code** | `string` | (Optional) FIPS code of the county (or local equivalent) | 06001 |
| **subregion2_name** | `string` | (Optional) American English name of the county (or local equivalent), subject to change | Alameda County |
| **3166-1-alpha-2** | `string` | ISO 3166-1 alphanumeric 2-letter code of the country | US |
| **3166-1-alpha-3** | `string` | ISO 3166-1 alphanumeric 3-letter code of the country | USA |
| **aggregation_level** | `integer` `[0-2]` | Level at which data is aggregated, i.e. country, state/province or county level | 2 |
