[Back to main page](../README.md)

# Weather
Daily weather information from nearest station reported by NOAA.


## URL
This table can be found at the following URLs depending on the choice of format:
* [weather.csv](https://storage.googleapis.com/covid19-open-data/v2/weather.csv)
* [weather.json](https://storage.googleapis.com/covid19-open-data/v2/weather.json)


## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | US_CA |
| **noaa_station\*** | `string` | Identifier for the weather station | USC00206080 |
| **noaa_distance\*** | `double` `[kilometers]` | Distance between the location coordinates and the weather station | 28.693 |
| **average_temperature** | `double` `[celsius]` | Recorded hourly average temperature | 11.22 |
| **minimum_temperature** | `double` `[celsius]` | Recorded hourly minimum temperature | 1.74 |
| **maximum_temperature** | `double` `[celsius]` | Recorded hourly maximum temperature | 19.42 |
| **rainfall** | `double` `[millimeters]` | Rainfall during the entire day | 51.0 |
| **snowfall** | `double` `[millimeters]` | Snowfall during the entire day | 0.0 |
| **dew_point** | `double` `[celsius]` | Temperature to which air must be cooled to become saturated with water vapor | 10.88 |
| **relative_humidity** | `double` `[%]` | The amount of water vapor present in air expressed as a percentage of the amount needed for saturation at the same temperature | 43.09 |

\*The reported data corresponds to the average of the nearest 10 stations within a 300km radius. The
columns `noaa_station` and `noaa_distance` refer to the nearest station only.


## Sources of data

<details>
<summary>Show data sources</summary>


| Data | Source | License and Terms of Use |
| ---- | ------ | ------------------------ |
| Weather | [NOAA](https://www.ncei.noaa.gov) | [Attribution required, non-commercial use](https://www.wmo.int/pages/prog/www/ois/Operational_Information/Publications/Congress/Cg_XII/res40_en.html) |

</details>
