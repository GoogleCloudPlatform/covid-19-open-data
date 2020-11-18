[Back to main page](../README.md)

# COVID-19 Search Trends symptoms dataset

## Terms of use
To download or use the data, you must agree to the Google [Terms of Service](https://policies.google.com/terms).

## Summary
This aggregated, anonymized dataset shows trends in search patterns for symptoms and is intended to help researchers to better understand the impact of COVID-19.

Public health experts indicated that trends in search patterns might be helpful in broadly understanding how COVID-19 impacts communities and even in detecting outbreaks earlier. You shouldn’t assume that the data is a recording of real-world clinical events, or use this data for medical diagnostic, prognostic, or treatment purposes.

To visualize the data, try exploring these [interactive charts and map of symptom search trends](https://pair-code.github.io/covid19_symptom_dataset).

## About this data
This data reflects the volume of Google searches for a broad set of health symptoms, signs, and conditions. *To keep things simple in this documentation, we will refer to all of these collectively as symptoms*. The data covers hundreds of symptoms such as *fever*, *difficulty breathing*, and *stress*—based on the following:
- a symptom’s prevalence in Google’s searches
- data quality and privacy considerations

For each day, we count the searches mapped to each of these symptoms and organize the data by geographic region. The resulting dataset is a daily or weekly time series for each region showing the relative frequency of searches for each symptom.

A single search query can be mapped to more than one symptom. For example, we map a search for “acid reflux and coughing up mucus” to three symptoms: *Cough*, *Gastroesophageal reflux disease*, and *Heartburn*.

The dataset covers the recent period and we’ll gradually expand its range as part of regular updates. Each update will bring the coverage to within three days of the day of the update.

Although we are releasing the dataset in English, we count searches in other languages. In each supported country, we include the languages needed to cover the majority of symptom search queries. For example, in the United States we support Spanish and English.

The data represents a sample of our users and might not represent the exact behavior of a wider population.

### Preserving privacy
For this dataset, we use [differential privacy](https://www.youtube.com/watch?v=FfAdemDkLsc&feature=youtu.be), which adds artificial noise to our datasets while enabling high quality results without identifying any individual person.

To further protect people’s privacy, we ensure that no personal information or individual search queries are included in the dataset, and we don’t link any search-based health inferences to an individual user. More information about the privacy methods used to generate the dataset can be found in this [report][1].

### How we process the data
The data shows the *relative popularity* of symptoms in searches within a geographical region.

When the daily volume of the data for a given region does not meet quality or privacy thresholds, we do the following:

1. Try to provide a given symptom at the weekly resolution.
2. If we cannot meet our quality or privacy thresholds at the weekly resolution, we do not provide the data for the symptom in that region.

As a result, for a given region, some symptoms will appear exclusively in the daily time series, some exclusively in the weekly time series, and some might not appear at all (if the search data for those symptoms is very scarce).

To normalize and scale the daily and the weekly time series (processed separately), we do the following for each region:

1. First, the algorithm counts the number of searches for each symptom in that region for that day/week.
2. Next, we divide this count by the total number of Search users in the region for that day/week to calculate relative popularity (which can be interpreted as the probability that a user in this region will search for the given symptom on that day/week). We refer to this ratio as the *normalized popularity* of a symptom.
3. We then find the maximum value of the normalized popularity across the entire published time range for that region, over all symptoms using the chosen time resolution (day/week). We scale this maximum value to 100. All the other values are mapped to proportionally smaller values (linear scaling) in the range 0-100.
4. Finally, we store the scaling factor and use it to scale values (for the same region and time resolution) in subsequent releases. In future updates, when a symptom popularity exceeds the previously-observed maximum value (found in step 3), then the new scaled value will be larger than 100.

For each region and time resolution, we scale all the normalized popularities using the same scaling factor. In a single region, you can compare the relative popularity of two (or more) symptoms (at the same time resolution) over any time interval. However, you should not compare the values of symptom popularity across regions or time resolutions — the region and time resolution specific scalings make these comparisons meaningless.

## URL
This data table can be found at the following locations depending on the format and how it is aggregated:
* [search-trends-daily.csv](https://storage.googleapis.com/covid19-open-data/v2/search-trends-daily.csv)
* [search-trends-weekly.csv](https://storage.googleapis.com/covid19-open-data/v2/search-trends-weekly.csv)

## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | US_CA |
| **country_region** | `string` | The name of the country in English. | United States |
| **country_region_code** | `string` | The [ISO 3166-1](https://en.wikipedia.org/wiki/ISO_3166-1) code for the country. | US |
| **sub_region_1** | `string` | The name of a region in the country. | California |
| **sub_region_1_code** | `string` | A country-specific [ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2) code for the region. | US-CA |
| **sub_region_2** | `string` | The name of a subdivision of the region above. For example, *Santa Clara County*. |  |
| **sub_region_2_code** | `string` | For the US - The [FIPS code](https://en.wikipedia.org/wiki/FIPS_county_code) for a US county (or equivalent). | 06085 |
| **date** | `string` | The day on which the searches took place. For weekly data, this is the first day of the 7-day weekly interval starting on Monday. For example, in the weekly data the row labeled *2020-07-13* represents the search activity for the week of July 13 to July 19, 2020, inclusive. | 2020-07-13 |
| **`${symptom name}`** | `double` `[0-100]` | Repeated for each symptom. Reflects the normalized search volume for this symptom, for the specified date and region. The field may be empty when data is not available. | 87.02 |

## Availability
We will continue to update this dataset while public health experts find it useful in their work to stop the spread of COVID-19. We will also take into account feedback from public health researchers, civil society groups, and the communities at large.

## Attribution
If you publish results based on this dataset, please cite as:<br/>
```
Google LLC "Google COVID-19 Search Trends symptoms dataset".
http://goo.gle/covid19symptomdataset, Accessed: <date>.
```
## Feedback
We would love your feedback on the dataset and documentation, or any unexpected results.<br/> Please email your feedback to covid-19-search-trends-feedback@google.com.

[1]: https://arxiv.org/abs/2009.01265
