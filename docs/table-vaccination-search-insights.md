[Back to main page](../README.md)

# COVID-19 Vaccination Search Insights
*Updated July 31, 2021*


## Terms of use
To download or use the data, you must agree to the Google [Terms of Service](https://policies.google.com/terms).


## Summary
This aggregated, anonymized data shows trends in search patterns related to COVID-19 vaccination. We’re making this data available because we heard from public health officials that trends in search patterns could help to design, target, and evaluate public education campaigns.

To visualize the data, try exploring our [interactive dashboard](http://goo.gle/covid19vaccinationinsights).


## About this data
These trends reflect the *relative interest* of Google searches related to COVID-19 vaccination. We split searches, by information need, across 3 categories:

1. **COVID-19 vaccination**. All searches related to COVID-19 vaccination, indicating overall search interest in the topic.
2. **Vaccination intent**. Searches related to eligibility, availability, and accessibility of vaccines.
3. **Safety and side effects**. Searches related to the safety and side effects of the vaccines.

We selected these categories based on the input from public health experts, as well as taking into consideration:
- data quality—for example, clear user intent
- privacy—for example, significant search volumes

The data will initially cover the period starting from January 2021, and going forward we’ll gradually expand its range through weekly updates. Each update will cover the recent week (Mon-Sun) and will be available within 3 days—to allow time for data processing and validation.

These trends represent Google Search users and might not represent the exact behavior of a wider population. We expect, however, that any systematic regional biases will remain stable over the period covered by the dataset.

### How we process the data
The data shows the relative interest in the different search categories within a geographical region. To generate, normalize, and scale the weekly time series we do the following for each region:

1. First, we count the queries mapped to each of the categories in that region for that week. To set the region, we [estimate the location](https://policies.google.com/technologies/location-data) where the query was made. When counting the queries, a given anonymous search user can contribute at most once to each category per day, and to at most 3 different categories per day.
2. Next, we divide this count by the total volume of queries in that region for that week to calculate relative interest. We call this ratio the *normalized interest* of a category. This is a relatively small number, which reflects the fraction of all search queries in that region that are related to the topic of COVID-19 vaccination, or one of its subcategories.
3. We then find the maximum weekly value of the *normalized interest* for the general *COVID-19 vaccination* category, at the US national level, which occured on the week starting at March 8. We scale this maximum value to 100, by multiplying it by a number we call the *fixed scaling factor*.
4. We store the fixed scaling factor computed in step 3, and in subsequent releases we use it to scale values in all regions—skipping this step.
5. Finally, using our fixed scaling factor, we linearly scale all the other normalized interest, across regions, categories, and time. These values can be lower or higher than 100. We call these values *scaled normalized interest*.

Because all scaled normalized interest values share the same scaling factor, you can do the following:
- Compare the relative interest of categories across all regions over any time interval.
- Calculate the fraction of COVID-19 vaccination queries that focus on the topic of vaccination intent. To do this for a region, divide the *scaled normalized interest* of the *Vaccination intent* or *Safety and side effects* categories by the *COVID-19 vaccination* category.

We would like to report trends for all regions, but sometimes we cannot do this. When the weekly volume of the data for a given region does not meet quality or privacy thresholds, we may not provide data for some or all categories in that region. In such a case, the data for that region will be aggregated in its parent region (e.g., US county data will be counted as part of its state trends).


### Preserving privacy and quality
To preserve user privacy, we use [differential privacy](https://www.youtube.com/watch?v=FfAdemDkLsc&feature=youtu.be) which adds artificial noise to our data while enabling high quality results without identifying any individual person. 

To further protect users’ privacy, we ensure that no personal information is included in the data, and we don’t link any related search-based inferences to an individual user.

To ensure accuracy after adding noise, we estimate the magnitude of change due to the noise. We retain all the values that (after the addition of noise) have 80% probability to be within 15% of the original value and we remove the noisey values. This sometimes leads to missing data points, as explained in **How we process the data** section.

You can learn more about the privacy and quality methods used to generate the data by reading this [anonymization process description](https://arxiv.org/abs/2107.01179).

## Data availability
This data table can be found at the following locations:
* Processed file: [vaccination-search-insights.csv][1]
* Raw data: [Global_vaccination_search_insights.csv][2]

Other options to explore and work with the data include:
1. Explore or download the data using our [interactive dashboard](http://goo.gle/covid19vaccinationinsights).
2. Run queries in Google Cloud’s [COVID-19 Public Dataset Program](http://console.cloud.google.com/marketplace/product/bigquery-public-datasets/covid19-vaccination-search-insights).
3. Analyze the data alongside other covariates in the [COVID-19 Open-Data repository](http://goo.gle/covid-19-open-data).

## Schema
| Name | Type | Description | Example |
| ---- | :----: | ----------- | ----------- |
| **key**\* | `string` | Unique string identifying the region | US_CA |
| **date** | `string` | The first day of the weekly interval (starting on Monday) on which the searches took place. For example, in the weekly data the row labeled 2021-04-19 represents the search activity for the week of April 19 to April 25, 2021, inclusive. Calendar days start and end at midnight, Pacific Standard Time. | 2021-04-19 |
| **country_region**\*\* | `string` | The name of the country or region in English.  | United States |
| **country_region_code**\*\* | `string` | The [ISO 3166-1](https://en.wikipedia.org/wiki/ISO_3166-1) code for the country or region. | US |
| **sub_region_1**\*\* | `string` | The name of a region in the country. | California |
| **sub_region_1_code**\*\* | `string` | A country-specific [ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2) code for the region. | US_CA |
| **sub_region_2**\*\* | `string` | The name (or type) of a region in the country. Typically a subdivision of sub_region_1. | Santa Clara County or municipal_borough. |
| **sub_region_2_code**\*\* | `string` | In the US, the [FIPS code](https://en.wikipedia.org/wiki/FIPS_county_code) for a US county (or equivalent). | 06085 |
| **sub_region_3**\*\* | `string` | The name (or type) of a region in the country. Typically a subdivision of sub_region_2 | Downtown or postal_code. |
| **sub_region_3_code**\*\* | `string` | In the US, the [ZIP code](https://en.wikipedia.org/wiki/ZIP_Code). | 94303 |
| **place_id**\*\* | `string` | The Google [Place ID](https://developers.google.com/places/web-service/place-id) for the most-specific region, used in Google Places API and on Google Maps. | ChIJd_Y0eVIvkIARuQyDN0F1LBA |
| **sni_covid19_vaccination** | `double` | The scaled normalized interest related to all COVID-19 vaccinations topics for the region and date. Empty when data isn’t available. | 87.02 |
| **sni_vaccination_intent** | `double` | The scaled normalized interest for all searches related to eligibility, availability, and accessibility for the region and date. Empty when data isn’t available. | 22.69 |
| **sni_safety_side_effects** | `double` | The scaled normalized interest for all searches related to safety and side effects of the vaccines for the region and date. Empty when data isn’t available. | 17.96 |

\*Only available in the [processed data table][1].

\*\*Only available in the [raw data table][2].

## Attribution
If you publish results based on this dataset, please cite as:<br/>
```
Google LLC "Google COVID-19 Vaccination Search Insights".
http://goo.gle/covid19vaccinationinsights, Accessed: <date>.
```

## Feedback
We’d love to hear about your project and learn more about your case studies. We’d also appreciate your feedback on the dashboard, data and documentation, or any unexpected results. Please email us at covid-19-search-trends-feedback@google.com.

## Dataset changes
July 31, 2021 - Documented classifier training and evaluation, anonymization process and categories hierarchy.<br/>
June 30, 2021 - Public release


[1]: https://storage.googleapis.com/covid19-open-data/v2/vaccination-search-insights.csv
[2]: https://storage.googleapis.com/covid19-open-data/covid19-vaccination-search-insights/Global_vaccination_search_insights.csv
