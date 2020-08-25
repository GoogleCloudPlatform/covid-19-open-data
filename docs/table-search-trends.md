[Back to main page](../README.md)

# Search Trends
The **COVID-19 Search Trends symptoms dataset** shows aggregated, anonymized trends in Google
searches for symptoms (and some related topics). The dataset provides a daily or weekly time series
for each region showing the relative volume of searches for each symptom.

This dataset is intended to help researchers to better understand the impact of COVID-19. It
shouldn’t be used for medical diagnostic, prognostic, or treatment purposes. It also isn’t intended
to be used for guidance on personal travel plans.

To learn more about the dataset, how we generate it and preserve privacy, read the
[data documentation](https://storage.googleapis.com/gcp-public-data-symptom-search/COVID-19%20Search%20Trends%20symptoms%20dataset%20documentation%20.pdf).

**Terms**: In order to download or use the data or reports, you must agree to the
Google [Terms of Service](https://policies.google.com/terms).

## URL
The **COVID-19 Search Trends symptoms dataset** is presented in CSV form joined with our known
location keys, and can be downloaded at the following locations:
* [search-trends.csv](https://storage.googleapis.com/covid19-open-data/v2/search-trends.csv)

## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | US_CA |
| **TODO** | `string` |  TODO: description | TODO |
| **TODO** | `int` |  TODO: description | TODO |
