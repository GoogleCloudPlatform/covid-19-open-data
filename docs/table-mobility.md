[Back to main page](../README.md)

# Mobility
Various metrics related to the movement of people, including [Google's Mobility Reports][1].


## Google COVID-19 Community Mobility Reports

### Terms of use
In order to download or use the data or reports, you must agree to the Google
[Terms of Service](https://policies.google.com/terms).

### Mobility Reports documentation
This dataset is intended to help remediate the impact of COVID-19. It shouldn’t be used for medical
diagnostic, prognostic, or treatment purposes. It also isn’t intended to be used for guidance on
personal travel plans.

The data shows how visits to places, such as grocery stores and parks, are changing in each
geographic region. Learn how you can use this report in your work by visiting
[Community Mobility Reports Help](https://support.google.com/covid19-mobility).

Location accuracy and the understanding of categorized places varies from region to region, so we
don’t recommend using this data to compare changes between countries, or between regions with
different characteristics (e.g. rural versus urban areas).

We’ll leave a region or category out of the dataset if we don’t have sufficient statistically
significant levels of data. To learn how we calculate these trends and preserve privacy, read
[About this data](#about-this-data) below.

### About this data
These datasets show how visits and length of stay at different places change compared to a baseline.
We calculate these changes using the same kind of aggregated and anonymized data used to show
[popular times](https://support.google.com/business/answer/6263531) for places in Google Maps.

Changes for each day are compared to a baseline value for that day of the week:
- The baseline is the median value, for the corresponding day of the week, during the 5-week period
  Jan 3–Feb 6, 2020.
- The datasets show trends over several months with the most recent data representing approximately
  2-3 days ago—this is how long it takes to produce the datasets.

What data is included in the calculation depends on user settings, connectivity, and whether it
meets our privacy threshold. When the data doesn't meet quality and privacy thresholds, you might
see empty fields for certain places and dates.

We include categories that are useful to social distancing efforts as well as access to essential
services.

We calculate these insights based on data from users who have opted-in to Location History for their
Google Account, so the data represents a sample of our users. As with all samples, this may or may
not represent the exact behavior of a wider population.


## Updates and improvements
We continue to improve our reports as places close and reopen. We updated the way we calculate
changes for *Groceries & pharmacy*, *Retail & recreation*, *Transit stations*, and *Parks*
categories. For regions published before May 2020, the data may contain a consistent shift either up
or down that starts between April 11–18, 2020.

On October 5, 2020, we added an improvement to the dataset to ensure consistent data reporting in the
*Groceries & pharmacy*, *Retail & recreation*, *Transit*, *Parks*, and *Workplaces* categories. The
update applies to all regions, starting on August 17, 2020.

### Preserving privacy
The Community Mobility Datasets were developed to be helpful while adhering to our stringent privacy
protocols and protecting people’s privacy. No personally identifiable information, like an
individual’s location, contacts or movement, is made available at any point.

Insights in these reports are created with aggregated, anonymized sets of data from users who have
turned on the [Location History](https://support.google.com/accounts/answer/3118687) setting, which
is off by default. People who have Location History turned on can choose to turn it off at any time
from their [Google Account](https://myaccount.google.com/activitycontrols) and can always delete
Location History data directly from their [Timeline](https://www.google.com/maps/timeline).

The reports are powered by the same world-class anonymization technology that we use in our products
every day to keep your activity data private and secure. This includes
[differential privacy](https://www.youtube.com/watch?v=FfAdemDkLsc&feature=youtu.be), which adds
artificial noise to our datasets, enabling us to generate insights without identifying any
individual person. These privacy-preserving protections also ensure that the absolute number of
visits isn’t shared.

Visit Google’s [Privacy Policy](https://policies.google.com/privacy) to learn more about how we keep
your data private, safe and secure.


## URL
[Google's Mobility Reports][1] are joined with our known location keys, and can be downloaded at the
following locations:
* [mobility.csv](https://storage.googleapis.com/covid19-open-data/v2/mobility.csv)
* [mobility.json](https://storage.googleapis.com/covid19-open-data/v2/mobility.json)


## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | US_CA |
| **mobility_grocery_and_pharmacy** | `double` `[%]` |  Percentage change in visits to places like grocery markets, food warehouses, farmers markets, specialty food shops, drug stores, and pharmacies compared to baseline | -15 |
| **mobility_parks** | `double` `[%]` |  Percentage change in visits to places like local parks, national parks, public beaches, marinas, dog parks, plazas, and public gardens compared to baseline | -15 |
| **mobility_transit_stations** | `double` `[%]` |  Percentage change in visits to places like public transport hubs such as subway, bus, and train stations compared to baseline | -15 |
| **mobility_retail_and_recreation** | `double` `[%]` |  Percentage change in visits to restaurants, cafes, shopping centers, theme parks, museums, libraries, and movie theaters compared to baseline | -15 |
| **mobility_residential** | `double` `[%]` |  Percentage change in visits to places of residence compared to baseline | -15 |
| **mobility_workplaces** | `double` `[%]` |  Percentage change in visits to places of work compared to baseline | -15 |


## Sources of data

<details>
<summary>Show data sources</summary>


| Data | Source | License and Terms of Use |
| ---- | ------ | ------------------------ |
| Google Mobility data | <https://www.google.com/covid19/mobility/> | [Google Terms of Service](https://policies.google.com/terms) |

</details>

[1]: https://www.google.com/covid19/mobility/
