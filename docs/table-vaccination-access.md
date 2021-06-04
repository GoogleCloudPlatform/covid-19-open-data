[Back to main page](../README.md)

# COVID-19 Vaccination Access Dataset
*Updated June 9, 2021*


## Terms of use
To download or use the data, you must agree to the Google [Terms of Service](https://policies.google.com/terms).


## Summary
This dataset characterizes access to COVID-19 vaccination sites based on travel times. We’re releasing this data to help public health officials, researchers, and healthcare providers to identify areas with insufficient access, deploy interventions, and research these issues—you shouldn’t use this dataset for other purposes.

To visualize the data, try exploring the [Vaccine Equity Planner](https://vaccineplanner.org/) tool by [Ariadne Labs](https://www.ariadnelabs.org/) and [Boston Children’s Hospital](https://www.childrenshospital.org/), which is powered by this dataset.


## About the data
This data shows *catchment areas* surrounding COVID-19 vaccination sites (sometimes called *facilities*). A catchment area represents the area within which a site can be reached within a designated period of time. Each vaccination site has a number of catchment areas, each representing a combination of a typical traveling time (for example, 15 minutes or less) and mode of transport (such as, walking, driving, or public transport).

The data covers vaccination sites in the US that are known to Google and will be refreshed weekly, reflecting changes in the availability and accessibility of vaccination sites. We’ll explore expanding to other regions as we obtain further vaccination site and geospatial information, as well as feedback from public health organizations, researchers, and users of this data.

This dataset uses Google Maps Platform [Directions API](https://developers.google.com/maps/documentation/directions/overview), the same one that helps calculate directions in Google Maps. The dataset doesn’t rely on any user data.


### How we process the data
First, we gather a list of the vaccination sites in a country from authoritative sources, such as  government, retail pharmacies, and data aggregators.

Next, we divide the territory of each country into roughly square regions of approximately 600m x 600m. The vaccination sites (destinations) and the starting points of a journey (sources) are treated as if at the centers of these squares.

Finally, to compute the catchment area boundaries we do the following for each vaccination site:
* Using Google Maps’ [Directions API](https://developers.google.com/maps/documentation/directions/overview) we compute the travel time and distance required to reach that site from all the squares in its vicinity (up to a radius of 50 km).
* To compute the catchment boundary for a particular mode of transport and particular travel time threshold:
  * We unify all squares in the vicinity of the site that can be reached using the chosen mode of transport within the chosen travel time.
  * We draw a boundary surrounding the unified area.
  * To optimize the data, we smooth the boundary while minimizing the distortion of the original shape.


## Data Availability

You can download the dataset from the following links:

| Country | Mode of Transport | Download link |
| :----: | :----: | ----------- |
| US | Driving | [facility-boundary-us-drive.csv](https://storage.googleapis.com/covid19-open-data/covid19-vaccination-access/facility-boundary-us-drive.csv) |
| US | Transit | [facility-boundary-us-transit.csv](https://storage.googleapis.com/covid19-open-data/covid19-vaccination-access/facility-boundary-us-transit.csv) |
| US | Walking | [facility-boundary-us-walk.csv](https://storage.googleapis.com/covid19-open-data/covid19-vaccination-access/facility-boundary-us-walk.csv) |
| US | All modes | [facility-boundary-us-all.csv](https://storage.googleapis.com/covid19-open-data/covid19-vaccination-access/facility-boundary-us-all.csv) |

Other options to explore and work with the dataset include:

* Explore the data using [Vaccine Equity Planner](https://vaccineplanner.org/) by [Ariadne Labs](https://www.ariadnelabs.org/) and [Boston Children’s Hospital's](https://www.childrenshospital.org/)  (US only).
* Analyze the data alongside other covariates in the [COVID-19 Open-Data repository](https://goo.gle/covid-19-open-data).
* Run queries in Google Cloud’s COVID-19 Public Dataset Program *(coming soon)*.

We’ll continue to update this product based on feedback from public health officials and researchers involved in the COVID-19 vaccination efforts. Our published data will remain publicly available to support long-term research and evaluation.


## Schema
| Name | Type | Description | Example |
| ---- | :----: | ----------- | ----------- |
| **facility_place_id** | `string` | The Google [Place ID](https://developers.google.com/places/web-service/place-id) of the vaccination site. | ChIJV3woGFkSK4cRWP9s3-kIFGk |
| **facility_provider_id** | `string` | An identifier imported from the provider of the vaccination site information. In the US, we use the ID provided by [VaccineFinder](http://vaccines.gov) when available. | 7ede5bd5-44da-4a59-b4d9-b3a49c53472c|
| **facility_name** | `string` | The name of the vaccination site. | St. Joseph's Hospital |
| **facility_latitude** | `double` | The latitude of the vaccination site. | 36.0507 |
| **facility_longitude** | `double` | The longitude of the vaccination site. | 41.4356 |
| **facility_country_region** | `string` | The name of the country or region in English.  | United States |
| **facility_country_region_code** | `string` | The [ISO 3166-1](https://en.wikipedia.org/wiki/ISO_3166-1) code for the country or region. | US |
| **facility_sub_region_1** | `string` | The name of a region in the country | California |
| **facility_sub_region_1_code** | `string` | A country-specific [ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2) code for the region. | US_CA |
| **facility_sub_region_2** | `string` | The name (or type) of a region in the country. Typically a subdivision of sub_region_1 | Santa Clara County or municipal_borough. |
| **facility_sub_region_2_code** | `string` | In the US, the [FIPS code](https://en.wikipedia.org/wiki/FIPS_county_code) for a US county (or equivalent). | 06085 |
| **facility_region_place_id** | `string` | The Google [Place ID](https://developers.google.com/places/web-service/place-id) for the most-specific region, used in Google Places API and on Google Maps. | ChIJd_Y0eVIvkIARuQyDN0F1LBA |
| **mode_of_transportation** | `string` | The mode of transport used to calculate the catchment boundary. | driving |
| **travel_time_threshold_minutes** | `int` | The maximum travel time, in minutes, used to calculate the catchment boundary. | 30 |
| **facility_catchment_boundary** | `GeoJSON string representation` | A [GeoJSON](https://geojson.org/) representation of the catchment area boundary of the site, for a particular mode of transportation and travel time threshold. Consists of multiple latitude and longitude points. |  |

## Attribution
If you publish results based on this dataset, please cite as:<br/>
```
Google LLC "Google COVID-19 Vaccination Access Dataset".
http://goo.gle/covid19vaccinationaccessdataset, Accessed: <date>.
```

## Feedback
We’d love to hear about your project and learn more about your case studies. We’d also appreciate your feedback on the data and documentation, or any unexpected results. Please email us at covid-19-vaccination-access-feedback@google.com.


## Dataset changes
Jun 9, 2021 - Initial release
