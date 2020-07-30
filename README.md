# COVID-19 Open Data

This repository contains datasets of daily time-series data related to COVID-19 for 50+ countries
around the world. The data is at the spatial resolution of states/provinces for most regions and at
county/municipality resolution for Argentina, Brazil, Chile, Colombia, Czech Republic, Mexico,
Netherlands, Peru, United Kingdom, and USA. All regions are assigned a unique location key, which
resolves discrepancies between ISO / NUTS / FIPS codes, etc. The different aggregation levels are:
* 0: Country
* 1: Province, state, or local equivalent
* 2: Municipality, county, or local equivalent
* 3: Locality which may not follow strict hierarchical order, such as "city" or "nursing homes in X
  location"

There are multiple types of data:
* Outcome data `Y(i,t)`, such as cases, deaths, tests, for regions i and time t
* Static covariate data `X(i)`, such as population size, GDP, latitude/ longitude
* Dynamic covariate data `X(i,t)`, such as mobility, weather
* Dynamic interventional data `A(i,t)`, such as government lockdowns

The data is drawn from multiple sources, as listed [below](#sources-of-data), and stored in separate
csv / json files, which can be easily merged due to the use of consistent geographic (and temporal)
keys.

| Table | Keys<sup>1</sup> | Content | URL | Source<sup>2</sup> |
| ----- | ---------------- | ------- | --- | ------------------ |
| [Main](#main) | `[key][date]` | Flat table with records from (almost) all other tables joined by `date` and/or `key`  | [main.csv](https://storage.googleapis.com/covid19-open-data/v2/main.csv) | All tables below |
| [Index](#index) | `[key]` | Various names and codes, useful for joining with other datasets | [index.csv](https://storage.googleapis.com/covid19-open-data/v2/index.csv), [index.json](https://storage.googleapis.com/covid19-open-data/v2/index.json) | Wikidata, DataCommons |
| [Demographics](#demographics) | `[key]` | Various (current<sup>3</sup>) population statistics | [demographics.csv](https://storage.googleapis.com/covid19-open-data/v2/demographics.csv), [demographics.json](https://storage.googleapis.com/covid19-open-data/v2/demographics.json) | Wikidata, DataCommons |
| [Economy](#economy) | `[key]` | Various (current<sup>3</sup>) economic indicators | [economy.csv](https://storage.googleapis.com/covid19-open-data/v2/economy.csv), [economy.json](https://storage.googleapis.com/covid19-open-data/v2/economy.json) | Wikidata, DataCommons |
| [Epidemiology](#epidemiology) | `[key][date]` | COVID-19 cases, deaths, recoveries and tests | [epidemiology.csv](https://storage.googleapis.com/covid19-open-data/v2/epidemiology.csv), [epidemiology.json](https://storage.googleapis.com/covid19-open-data/v2/epidemiology.json) | Various<sup>2</sup> |
| [Geography](#geography) | `[key]` | Geographical information about the region | [geography.csv](https://storage.googleapis.com/covid19-open-data/v2/geography.csv), [geography.json](https://storage.googleapis.com/covid19-open-data/v2/geography.json) | Wikidata |
| [Health](#health) | `[key]` | Health indicators for the region | [health.csv](https://storage.googleapis.com/covid19-open-data/v2/health.csv), [health.json](https://storage.googleapis.com/covid19-open-data/v2/geography.json) | Wikidata, WorldBank |
| [Hospitalizations](#hospitalizations) | `[key][date]` | Information related to patients of COVID-19 and hospitals |  [hospitalizations.csv](https://storage.googleapis.com/covid19-open-data/v2/hospitalizations.csv), [hospitalizations.json](https://storage.googleapis.com/covid19-open-data/v2/hospitalization.json) | Various<sup>2</sup> |
| [Mobility](#mobility) | `[key][date]` | Various metrics related to the movement of people | [mobility.csv](https://storage.googleapis.com/covid19-open-data/v2/mobility.csv), [mobility.json](https://storage.googleapis.com/covid19-open-data/v2/mobility.json) | Google |
| [Government Response](#government-response) | `[key][date]` | Government interventions and their relative stringency | [oxford-government-response.csv](https://storage.googleapis.com/covid19-open-data/v2/oxford-government-response.csv), [oxford-government-response.json](https://storage.googleapis.com/covid19-open-data/v2/oxford-government-response.json) | University of Oxford |
| [Weather](#weather) | `[key][date]` | Dated meteorological information for each region | [weather.csv](https://storage.googleapis.com/covid19-open-data/v2/weather.csv), [weather.json](https://storage.googleapis.com/covid19-open-data/v2/weather.json) | NOAA |
| [WorldBank](#worldbank) | `[key]` | Latest record for each indicator from WorldBank for all reporting countries | [worldbank.csv](https://storage.googleapis.com/covid19-open-data/v2/worldbank.csv), [worldbank.json](https://storage.googleapis.com/covid19-open-data/v2/worldbank.json) | WorldBank |
| [WorldPop](#worldpop) | `[key]` | Demographics data extracted from WorldPop | [worldpop.csv](https://storage.googleapis.com/covid19-open-data/v2/worldpop.csv), [worldpop.json](https://storage.googleapis.com/covid19-open-data/v2/worldpop.json) | WorldPop |
| [By Age](#by-age) | `[key][date]` | Epidemiology and hospitalizations data stratified by age | [by-age.csv](https://storage.googleapis.com/covid19-open-data/v2/by-age.csv), [by-age.json](https://storage.googleapis.com/covid19-open-data/v2/by-age.json) | Various<sup>2</sup> |
| [By Sex](#by-sex) | `[key][date]` | Epidemiology and hospitalizations data stratified by sex | [by-sex.csv](https://storage.googleapis.com/covid19-open-data/v2/by-sex.csv), [by-sex.json](https://storage.googleapis.com/covid19-open-data/v2/by-sex.json) | Various<sup>2</sup> |

<sup>1</sup> `key` is a unique string for the specific geographical region built from a combination
of codes such as `ISO 3166`, `NUTS`, `FIPS` and other local equivalents.\
<sup>2</sup> Refer to the [data sources](#sources-of-data) for specifics about each data source and
the associated terms of use.\
<sup>3</sup> Datasets without a `date` column contain the most recently reported information for
each datapoint to date.

For more information about how to use these files see the section about
[using the data](#use-the-data), and for more details about each dataset see the section about
[understanding the data](#understand-the-data).



## Why another dataset?

There are many other public COVID-19 datasets. However, we believe this dataset is unique in the way
that it merges multiple global sources, at a fine spatial resolution, using a consistent set of
region keys. We hope this will make it easier for researchers to use. We are also very transparent
about the [data sources](#sources-of-data), and the [code](src/README.md) for ingesting and merging
the data is easy to understand and modify.



## Explore the data

|     |     |     |
| --- | --- | --- |
| A simple visualization tool was built to explore the Open COVID-19 datasets, the [Open COVID-19 Explorer][12]: [![](https://github.com/open-covid-19/explorer/raw/master/screenshots/explorer.png)][12] | If you want to see [interactive charts with a unique UX][14], don't miss what [@Mahks](https://github.com/Mahks) built using the Open COVID-19 dataset: [![](https://i.imgur.com/cIODOtp.png)][14] | You can also check out the great work of [@quixote79](https://github.com/quixote79), [a MapBox-powered interactive map site][13]: [![](https://i.imgur.com/nFwxJId.png)][13] |
| Experience [clean, clear graphs with smooth animations][15] thanks to the work of [@jmullo](https://github.com/jmullo): [![](https://i.imgur.com/xdCzsUO.png)][15] | Become an armchair epidemiologist with the [COVID-19 timeline simulation tool][19] built by [@LeviticusMB](https://github.com/LeviticusMB): [![](https://i.imgur.com/4iWaP7E.png)][19] | Whether you want an interactive map, compare stats or look at charts, [@saadmas](https://github.com/saadmas) has you covered with a [COVID-19 Daily Tracking site][20]: [![](https://i.imgur.com/rAJvLSI.png)][20] |
| Compare per-million data at [Omnimodel][21] thanks to [@OmarJay1](https://github.com/OmarJay1): [![](https://i.imgur.com/RG7ZKXp.png)][21] | Look at responsive, comprehensive charts thanks to the work of [@davidjohnstone](https://github.com/davidjohnstone): [![](https://i.imgur.com/ZbfMKvu.png)](https://www.cyclinganalytics.com/covid19) | [Reproduction Live][30] lets you track COVID-19 outbreaks in your region and visualise the spread of the virus over time: [![](https://reproduction.live/open-covid-19.png)][30] |



## Use the data

The data is available as CSV and JSON files, which are published in Github Pages so they can be
served directly to Javascript applications without the need of a proxy to set the correct headers
for CORS and content type. Even if you only want the CSV files, using the URL served by Github Pages
is preferred in order to avoid caching issues and potential, future breaking changes.

For the purpose of making the data as easy to use as possible, there is a [main](#main) table
which contains the columns of all other tables joined by `key` and `date`. However,
performance-wise, it may be better to download the data separately and join the tables locally.

Each region has its own version of the main table, so you can pull all the data for a specific
region using a single endpoint, the URL for each region is:
* Data for `key` in CSV format: `https://storage.googleapis.com/covid19-open-data/v2/${key}/main.csv`
* Data for `key` in JSON format: `https://storage.googleapis.com/covid19-open-data/v2/${key}/main.json`

Each table has a full version as well as subsets with only the last 30, 14, 7 and 1 days of data.
The full version is accessible at the URL described [in the table above](#open-covid-19-dataset).
The subsets can be found by appending the number of days to the path. For example, the subsets of
the main table are available at the following locations:
* Full version: https://storage.googleapis.com/covid19-open-data/v2/main.csv
* Latest: https://storage.googleapis.com/covid19-open-data/v2/latest/main.csv

Note that the `latest` version contains the last non-null record for each key, whereas all others
contain the last `N` days of data (all of which could be null for some keys). All of the above
listed tables have a corresponding JSON version; simply replace `csv` with `json` in the link.

If you are trying to use this data alongside your own datasets, then you can use the [Index](#index)
table to get access to the ISO 3166 / NUTS / FIPS code, although administrative subdivisions are
not consistent among all reporting regions. For example, for the intra-country reporting, some EU
countries use NUTS2, others NUTS3 and many ISO 3166-2 codes.

You can find several examples in the [examples subfolder](examples) with code showcasing how to load
and analyze the data for several programming environments. If you want the short version, here are a
few snippets to get started.

### BigQuery
This dataset is part of the [BigQuery Public Datasets Program][22], so you may use BigQuery to run
SQL queries directly from the
[online query editor](https://cloud.google.com/bigquery?p=bigquery-public-data&d=covid19_open_data&page=dataset).

### Google Colab
You can use Google Colab if you want to run your analysis without having to install anything in your
computer, simply go to this URL: https://colab.research.google.com/github/open-covid-19/data.

### Google Sheets
You can import the data directly into Google Sheets, as long as you stay within the size limits.
For instance, the following formula loads the latest epidemiology data into the current sheet:
```python
=IMPORTDATA("https://storage.googleapis.com/covid19-open-data/v2/latest/epidemiology.csv")
```
Note that Google Sheets has a size limitation, so only data from the `latest` subfolder can be
imported automatically. To work around that, simply download the file and import it via the File
menu.

### R
If you prefer R, then this is all you need to do to load the epidemiology data:
```R
data <- read.csv("https://storage.googleapis.com/covid19-open-data/v2/main.csv")
```

### Python
In Python, you need to have the package `pandas` installed to get started:
```python
import pandas
data = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v2/main.csv")
```

### jQuery
Loading the JSON file using jQuery can be done directly from the output folder,
this code snippet loads the epidemiology table into the `data` variable:
```javascript
$.getJSON("https://storage.googleapis.com/covid19-open-data/v2/epidemiology.json", data => { ... }
```

### Powershell
You can also use Powershell to get the latest data for a country directly from
the command line, for example to query the latest epidemiology data for Australia:
```powershell
Invoke-WebRequest 'https://storage.googleapis.com/covid19-open-data/v2/latest/epidemiology.csv' | ConvertFrom-Csv | `
    where key -eq 'AU' | select date,total_confirmed,total_deceased,total_recovered
```



## Understand the data

Make sure that you are using the URL [linked at the table above](#covid-19-open-data) and not the
raw GitHub file, the latter is subject to change at any moment in non-compatible ways, and due to
the configuration of GitHub's raw file server you may run into potential caching issues.

Missing values will be represented as nulls, whereas zeroes are used when a true value of zero is
reported.

### Main
Flat table with records from all other tables joined by `key` and `date`. See below for information
about all the different tables and columns. Tables not included in the main table are:
* [WorldBank](#worldbank): A subset of individual indicators are added as columns to other tables
  instead; for example, the [health](#health) table.
* [WorldPop](#worldpop): Age and sex demographics breakdowns are normalized and added to the
  [demographics](#demographics) table instead.
* [By Age](#by-age): Age breakdowns of epidemiology and hospitalization data are normalized and
  added to the by-age-normalized table instead (TABLE TO BE ADDED).
* [By Sex](#by-sex): Sex breakdowns of epidemiology and hospitalization data are normalized and
  added to the by-sex-normalized table instead (TABLE TO BE ADDED).

### Index
Non-temporal data related to countries and regions. It includes keys, codes and names for each
region, which is helpful for displaying purposes or when merging with other data:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | US_CA_06001 |
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

### Demographics
Information related to the population demographics for each region:

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
| **population_age_`${lower}`_`${upper}`** | `integer` | Estimated population between the ages of `${lower}` and `${upper}`, both inclusive | 42038247 |

### Economy
Information related to the economic development for each region:

| Name | Name | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **gdp** | `integer` `[USD]` | Gross domestic product; monetary value of all finished goods and services | 24450604878 |
| **gdp_per_capita** | `integer` `[USD]` | Gross domestic product divided by total population | 1148 |
| **human_capital_index** | `double` `[0-1]` | Mobilization of the economic and professional potential of citizens | 0.765 |

### Epidemiology
Information related to the COVID-19 infections for each date-region pair:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **new_confirmed<sup>1</sup>** | `integer` | Count of new cases confirmed after positive test on this date | 34 |
| **new_deceased<sup>1</sup>** | `integer` | Count of new deaths from a positive COVID-19 case on this date | 2 |
| **new_recovered<sup>1</sup>** | `integer` | Count of new recoveries from a positive COVID-19 case on this date | 13 |
| **new_tested<sup>2</sup>** | `integer` | Count of new COVID-19 tests performed on this date | 13 |
| **total_confirmed<sup>3</sup>** | `integer` | Cumulative sum of cases confirmed after positive test to date | 6447 |
| **total_deceased<sup>3</sup>** | `integer` | Cumulative sum of deaths from a positive COVID-19 case to date | 133 |
| **total_tested<sup>2,3</sup>** | `integer` | Cumulative sum of COVID-19 tests performed to date | 133 |

<sup>1</sup>Values can be negative, typically indicating a correction or an adjustment in the way
they were measured. For example, a case might have been incorrectly flagged as recovered one date so
it will be subtracted from the following date.\
<sup>2</sup>When the reporting authority makes a distinction between PCR and antibody testing, only
PCR tests are reported here.\
<sup>3</sup>Total count will not always amount to the sum of daily counts, because many authorities
make changes to criteria for counting cases, but not always make adjustments to the data. There is
also potential missing data. All of that makes the total counts *drift* away from the sum of all
daily counts over time, which is why the cumulative values, if reported, are kept in a separate
column.

### Geography
Information related to the geography for each region:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **latitude** | `double` | Floating point representing the geographic coordinate | 30.9756 |
| **longitude** | `double` | Floating point representing the geographic coordinate | 112.2707 |
| **elevation** | `integer` `[meters]` | Elevation above the sea level | 875 |
| **area** | `integer` [squared kilometers] | Area encompassing this region | 3729 |
| **rural_area** | `integer` [squared kilometers] | Area encompassing rural land in this region | 3729 |
| **urban_area** | `integer` [squared kilometers] | Area encompassing urban land this region | 3729 |

### Health
Health related indicators for each region:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | BN |
| **life_expectancy** | `double` `[years]` |Average years that an individual is expected to live | 75.722 |
| **smoking_prevalence** | `double` `[%]` | Percentage of smokers in population | 16.9 |
| **diabetes_prevalence** | `double` `[%]` | Percentage of persons with diabetes in population | 13.3 |
| **infant_mortality_rate** | `double` | Infant mortality rate (per 1,000 live births) | 9.8 |
| **adult_male_mortality_rate** | `double` | Mortality rate, adult, male (per 1,000 male adults) | 143.719 |
| **adult_female_mortality_rate** | `double` | Mortality rate, adult, female (per 1,000 male adults) | 98.803 |
| **pollution_mortality_rate** | `double` | Mortality rate attributed to household and ambient air pollution, age-standardized (per 100,000 population) | 13.3 |
| **comorbidity_mortality_rate** | `double` `[%]` | Mortality from cardiovascular disease, cancer, diabetes or cardiorespiratory disease between exact ages 30 and 70 | 16.6 |
| **hospital_beds** | `double` | Hospital beds (per 1,000 people) | 2.7 |
| **nurses** | `double` | Nurses and midwives (per 1,000 people) | 5.8974 |
| **physicians** | `double` | Physicians (per 1,000 people) | 1.609 |
| **health_expenditure** | `double` `[USD]` | Health expenditure per capita | 671.4115 |
| **out_of_pocket_health_expenditure** | `double` `[USD]` | Out-of-pocket health expenditure per capita | 34.756348 |

Note that the majority of the health indicators are only available at the country level.

### Hospitalizations
Information related to patients of COVID-19 and hospitals:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **new_hospitalized\*** | `integer` | Count of new cases hospitalized after positive test on this date | 34 |
| **new_intensive_care\*** | `integer` | Count of new cases admitted into ICU after a positive COVID-19 test on this date | 2 |
| **new_ventilator\*** | `integer` | Count of new COVID-19 positive cases which require a ventilator on this date | 13 |
| **total_hospitalized\*\*** | `integer` | Cumulative sum of cases hospitalized after positive test to date | 6447 |
| **total_intensive_care\*\*** | `integer` | Cumulative sum of cases admitted into ICU after a positive COVID-19 test to date | 133 |
| **total_ventilator\*\*** | `integer` | Cumulative sum of COVID-19 positive cases which require a ventilator to date | 133 |
| **current_hospitalized\*\*** | `integer` | Count of current (active) cases hospitalized after positive test to date | 34 |
| **current_intensive_care\*\*** | `integer` | Count of current (active) cases admitted into ICU after a positive COVID-19 test to date | 2 |
| **current_ventilator\*\*** | `integer` | Count of current (active) COVID-19 positive cases which require a ventilator to date | 13 |

\*Values can be negative, typically indicating a correction or an adjustment in the way they were
measured. For example, a case might have been incorrectly flagged as recovered one date so it will
be subtracted from the following date.

\*\*Total count will not always amount to the sum of daily counts, because many authorities make
changes to criteria for counting cases, but not always make adjustments to the data. There is also
potential missing data. All of that makes the total counts *drift* away from the sum of all daily
counts over time, which is why the cumulative values, if reported, are kept in a separate column.

### Mobility
[Google's Mobility Reports][17] are presented in CSV form joined with our known location keys as
[mobility.csv](https://storage.googleapis.com/covid19-open-data/v2/mobility.csv) with the following columns:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | US_CA |
| **mobility_transit_stations** | `double` `[%]` |  Percentage change in visits to transit station locations compared to baseline | -15 |
| **mobility_retail_and_recreation** | `double` `[%]` |  Percentage change in visits to retail and recreation locations compared to baseline | -15 |
| **mobility_grocery_and_pharmacy** | `double` `[%]` |  Percentage change in visits to grocery and pharmacy locations compared to baseline | -15 |
| **mobility_parks** | `double` `[%]` |  Percentage change in visits to park locations compared to baseline | -15 |
| **mobility_residential** | `double` `[%]` |  Percentage change in visits to residential locations compared to baseline | -15 |
| **mobility_workplaces** | `double` `[%]` |  Percentage change in visits to workplace locations compared to baseline | -15 |

These Community Mobility Reports aim to provide insights into what has changed in response to
policies aimed at combating COVID-19. The reports chart movement trends over time by geography,
across different categories of places.

* Link to data: https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv
* Help Center: https://support.google.com/covid19-mobility
* Terms: In order to download or use the data or reports, you must agree to the [Google Terms of Service](https://policies.google.com/terms)

### Government Response
Summary of a government's response to the events, including a *stringency index*, collected from
[University of Oxford][18]:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | US_CA |
| **school_closing** | `integer` `[0-3]` | Schools are closed | 2 |
| **workplace_closing** | `integer` `[0-3]` | Workplaces are closed | 2 |
| **cancel_public_events** | `integer` `[0-3]` | Public events have been cancelled | 2 |
| **restrictions_on_gatherings** | `integer` `[0-3]` | Gatherings of non-household members are restricted | 2 |
| **public_transport_closing** | `integer` `[0-3]` | Public transport is not operational | 0 |
| **stay_at_home_requirements** | `integer` `[0-3]` | Self-quarantine at home is mandated for everyone | 0 |
| **restrictions_on_internal_movement** | `integer` `[0-3]` | Travel within country is restricted | 1 |
| **international_travel_controls** | `integer` `[0-3]` | International travel is restricted | 3 |
| **income_support** | `integer` `[USD]` | Value of fiscal stimuli, including spending or tax cuts | 20449287023 |
| **debt_relief** | `integer` `[0-3]` | Debt/contract relief for households | 0 |
| **fiscal_measures** | `integer` `[USD]` | Value of fiscal stimuli, including spending or tax cuts | 20449287023 |
| **international_support** | `integer` `[USD]` | Giving international support to other countries | 274000000 |
| **public_information_campaigns** | `integer` `[0-2]` | Government has launched public information campaigns | 1 |
| **testing_policy** | `integer` `[0-3]` | Country-wide COVID-19 testing policy | 1 |
| **contact_tracing** | `integer` `[0-2]` | Country-wide contact tracing policy | 1 |
| **emergency_investment_in_healthcare** | `integer` `[USD]` | Emergency funding allocated to healthcare | 500000 |
| **investment_in_vaccines** | `integer` `[USD]` | Emergency funding allocated to vaccine research | 100000 |
| **stringency_index** | `double` `[0-100]` | Overall stringency index | 71.43 |

For more information about each field and how the overall stringency index is
computed, see the [Oxford COVID-19 government response tracker][18].

### Weather
Daily weather information from nearest station reported by NOAA:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | US_CA |
| **noaa_station\*** | `string` | Identifier for the weather station | USC00206080 |
| **noaa_distance\*** | `double` `[kilometers]` | Distance between the location coordinates and the weather station | 28.693 |
| **average_temperature** | `double` `[celsius]` | Recorded hourly average temperature | 11.2 |
| **minimum_temperature** | `double` `[celsius]` | Recorded hourly minimum temperature | 1.7 |
| **maximum_temperature** | `double` `[celsius]` | Recorded hourly maximum temperature | 19.4 |
| **rainfall** | `double` `[millimeters]` | Rainfall during the entire day | 51.0 |
| **snowfall** | `double` `[millimeters]` | Snowfall during the entire day | 0.0 |

\*The reported weather station refers to the nearest station which provides temperature
measurements, but rainfall and snowfall may come from a different nearby weather station. In all
cases, only weather stations which are at most 300km from the location coordinates are considered.

### WorldBank
Most recent value for each indicator of the [WorldBank Database][25].

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | ES |
| **`${indicator}`** | `double` | Value of the indicator corresponding to this column, column name is indicator code | 0 |

Refer to the [WorldBank documentation][25] for more details, or refer to the
[worldbank_indicators.csv](./src/data/worldbank_indicators.csv) file for a short description of each
indicator. Each column uses the indicator code as its name, and the rows are filled with the values
for the corresponding `key`.

Note that WorldBank data is only available at the country level and it's not included in the main
table. If no values are reported by WorldBank for the country since 2015, the row value will be
null.

### WorldPop
Demographics data extracted from WorldPop, estimating total number of people per region broken down
by age and sex groupings.

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | ES |
| **`${sex}`\_`${age_bin}`** | `double` | Total number of people categorized as `${sex}` (`m` or `f`) in age bin `${age_bin}` | 1334716 |

Refer to the [WorldPop documentation](https://www.worldpop.org/geodata/summary?id=24798) for more
details. This data is normalized into buckets that are consistent with other tables and added into
the [demographics](#demographics) table; it is kept as a separate table to preserve access to the
original data without any modification beyond aggregation by regional unit.

### By Age
Epidemiology and hospitalizations data stratified by age:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | FR |
| **`${statistic}`\_age\_bin\_`${index}`** | `integer` | Value of `${statistic}` for age bin `${index}` | 139 |
| **age\_bin\_`${index}`** | `integer` | Range for the age values inside of bin `${index}`, both ends inclusive | 30-39 |

Values in this table are stratified versions of the columns available in the
[epidemiology](#epidemiology) and [hospitalizatons](#hospitalizations) tables. Each row contains up
to 10 distinct bins, for example:
`{new_deceased_age_00: 1, new_deceased_age_01: 45, ... , new_deceased_age_09: 32}`.

Each row may have different bins, depending on the data source. This table tries to capture the raw
data with as much fidelity as possible up to 10 bins. The range of each bin is encoded into the
`age_bin_${index}` variable, for example:
`{age_bin_00: 0-9, age_bin_01: 10-19, age_bin_02: 20-29, ... , age_bin_09: 90-}`.

Several things worth noting about this table:
* This table contains very sparse data, with very few combinations of regions and variables
  available.
* Records without a known age bin are discarded, so the sum of all ages may not necessary amount to
  the variable from the corresponding table.
* The upper and lower range of the range are inclusive values. For example, range `0-9` includes
  individuals with age zero up to (and including) 9.
* A row may have less than 10 bins, but never more than 10. For example:
  `{age_bin_00: 0-49, age_bin_01: 50-, age_bin_02: null, ...}`

### By Sex
Epidemiology and hospitalizations data stratified by sex:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | FR |
| **`${statistic}_sex_male`** | `integer` | Value of `${statistic}` for male individuals | 87 |
| **`${statistic}_sex_female`** | `integer` | Value of `${statistic}` for female individuals | 68 |

Values in this table are stratified versions of the columns available in the
[epidemiology](#epidemiology) and [hospitalizatons](#hospitalizations) tables. Each row contains
each variable with either `_male` or `_female` suffix:
`{new_deceased_male: 45, new_deceased_female: 32, new_tested_male: 45, new_tested_female: 32, ...}`.

Several things worth noting about this table:
* This table contains very sparse data, with very few combinations of regions and variables
  available.
* Records without a known sex are discarded, so the sum of all ages may not necessary amount to
  the variable from the corresponding table.

### Notes about the data
For countries where both country-level and subregion-level data is available, the entry which has a
null value for the subregion level columns in the `index` table indicates upper-level aggregation.
For example, if a data point has values
`{country_code: US, subregion1_code: CA, subregion2_code: null, ...}` then that record will have
data aggregated at the subregion1 (i.e. state/province) level. If `subregion1_code`were null, then
it would be data aggregated at the country level.

Another way to tell the level of aggregation is the `aggregation_level` of the `index` table, see
the [schema documentation](#index) for more details about how to interpret it.

Please note that, sometimes, the country-level data and the region-level data come from different
sources so adding up all region-level values may not equal exactly to the reported country-level
value. See the [data loading tutorial][7] for more information.

There is also a [notices.csv](src/data/notices.csv) file which is manually updated with quirks about
the data. The goal is to be able to query by key and date, to get a list of applicable notices to
the requested subset.



## Contribute

Technical contributions to the data extraction pipeline are welcomed, take a look at the
[source directory](src/README.md) for more information.

If you spot an error in the data, feel free to open an issue on this repository and we will review
it.

If you do something cool with the data, for example related to visualization or analysis, please let
us know!



## For Data Owners
We have carefully checked the license and attribution information on each data source included in
this repository, and in many cases have contacted the data owners directly to ask how they would
like to be attributed.

If you are the owner of a data source included here and would like us to remove data, add or alter
an attribution, or add or alter license information, please open an issue on this repository and
we will happily consider your request.



## Licensing

The output data files are published under the [CC BY-SA](./output/CC-BY-SA) license. All data is
subject to the terms of agreement individual to each data source, refer to the
[sources of data](#sources-of-data) table for more details. All other code and assets are published
under the [Apache License 2.0](./LICENSE).



## Sources of data

All data in this repository is retrieved automatically. When possible, data is retrieved directly
from the relevant authorities, like a country's ministry of health.

| Data | Source | License and Terms of Use |
| ---- | ------ | ------------------------ |
| Metadata | [Wikipedia](https://wikidata.org) | [CC BY-SA][24] |
| Demographics | [Wikidata](https://wikidata.org) | [CC0][23] |
| Demographics | [DataCommons](https://datacommons.org) | [Attribution required](https://policies.google.com/terms) |
| Demographics | [WorldBank](https://worldbank.org) | [CC BY 4.0](https://www.worldbank.org/en/about/legal/terms-of-use-for-datasets) |
| Demographics | [WorldPop](https://www.worldpop.org) | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) |
| Economy | [Wikidata](https://wikidata.org) | [CC0][23] |
| Economy | [DataCommons](https://datacommons.org) | [Attribution required](https://policies.google.com/terms) |
| Economy | [WorldBank](https://worldbank.org) | [CC BY 4.0](https://www.worldbank.org/en/about/legal/terms-of-use-for-datasets) |
| Geography | [Wikidata](https://wikidata.org) | [CC0][23] |
| Geography | [WorldBank](https://worldbank.org) | [CC BY 4.0](https://www.worldbank.org/en/about/legal/terms-of-use-for-datasets) |
| Health | [Wikidata](https://wikidata.org) | [CC0][23] |
| Health | [WorldBank](https://worldbank.org) | [CC BY 4.0](https://www.worldbank.org/en/about/legal/terms-of-use-for-datasets) |
| Weather | [NOAA](https://www.ncei.noaa.gov) | [Attribution required, non-commercial use](https://www.wmo.int/pages/prog/www/ois/Operational_Information/Publications/Congress/Cg_XII/res40_en.html) |
| Google Mobility data | <https://www.google.com/covid19/mobility/> | [Google Terms of Service](https://policies.google.com/terms) |
| Government response data | [Oxford COVID-19 government response tracker][18] | [CC BY 4.0](https://github.com/OxCGRT/covid-policy-tracker/blob/master/LICENSE.txt) |
| Country-level data | [ECDC](https://www.ecdc.europa.eu) | [Attribution required](https://www.ecdc.europa.eu/en/copyright) |
| Country-level data | [Our World in Data](https://ourworldindata.org) | [CC BY 4.0](https://ourworldindata.org/how-to-use-our-world-in-data#how-is-our-work-copyrighted) |
| Afghanistan | [HDX](https://data.humdata.org/dataset/afghanistan-covid-19-statistics-per-province) | [CC BY-SA][28] |
| Argentina | [Datos Argentina](https://datos.gob.ar/) | [Public domain](https://datos.gob.ar/acerca/seccion/marco-legal) |
| Australia | <https://covid-19-au.com/> | [Attribution required, educational and academic research purposes](https://covid-19-au.com/faq) |
| Austria | [COVID19 EU Data](https://github.com/covid19-eu-zh/covid19-eu-data) | [MIT](https://github.com/covid19-eu-zh/covid19-eu-data/issues/57) |
| Bangladesh | [HDX](https://data.humdata.org/dataset/district-wise-quarantine-for-covid-19) | [CC BY-SA][28] |
| Bolivia | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| Brazil | [Brazil Ministério da Saúde](https://coronavirus.saude.gov.br/) | [Creative Commons Atribuição](http://www.opendefinition.org/licenses/cc-by) |
| Brazil (Rio de Janeiro) | <http://www.data.rio/> | [Dados abertos](https://www.data.rio/datasets/f314453b3a55434ea8c8e8caaa2d8db5) |
| Brazil (Ceará) | <https://saude.ce.gov.br> | [Dados abertos](https://cearatransparente.ce.gov.br/portal-da-transparencia) |
| Canada | [Department of Health Canada](https://www.canada.ca/en/public-health) | [Attribution required](https://www.canada.ca/en/transparency/terms.html) |
| Chile | [Ministerio de Ciencia de Chile](http://www.minciencia.gob.cl/COVID19) | [Terms of use](http://www.minciencia.gob.cl/sites/default/files/1771596.pdf) |
| China | [DXY COVID-19 dataset](https://github.com/BlankerL/DXY-COVID-19-Data) | [MIT](https://github.com/BlankerL/DXY-COVID-19-Data/blob/master/LICENSE) |
| Colombia | [Datos Abiertos Colombia](https://www.datos.gov.co) | [Attribution required](https://herramientas.datos.gov.co/es/terms-and-conditions-es) || Costa Rica | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| Costa Rica | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| Cuba | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| Czech Republic | [Ministry of Health of the Czech Republic](https://onemocneni-aktualne.mzcr.cz/covid-19) | [Open Data](https://www.jmir.org/2020/5/e19367) |
| Democratic Republic of Congo | [HDX](https://data.humdata.org/dataset/democratic-republic-of-the-congo-coronavirus-covid-19-subnational-cases) | [CC BY-SA][28] |
| Ecuador | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| El Salvador | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| Finland | [Finnish institute for health and welfare](https://thl.fi/en/web/thlfi-en) | [CC BY 4.0](https://thl.fi/en/web/thlfi-en/statistics/statistical-databases/open-data) |
| France | [data.gouv.fr](https://data.gouv.fr) | [Open License 2.0](https://www.etalab.gouv.fr/licence-ouverte-open-licence) |
| Germany | <https://github.com/jgehrcke/covid-19-germany-gae> | [MIT](https://github.com/jgehrcke/covid-19-germany-gae/blob/master/LICENSE) |
| Guatemala | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| Haiti | [HDX](https://data.humdata.org/dataset/haiti-covid-19-subnational-cases) | [CC BY-SA][28] |
| Honduras | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| India | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019-20_coronavirus_pandemic_data/India_medical_cases) | [CC BY-SA][24] |
| India | [Covid 19 India Organisation](https://github.com/covid19india/api) | [CC BY-SA][29] |
| Indonesia | <https://catchmeup.id/covid-19> | [Permission required](https://catchmeup.id/ketentuan-pelayanan) |
| Italy | [Italy's Department of Civil Protection](https://github.com/pcm-dpc/COVID-19) | [CC BY 4.0](https://github.com/pcm-dpc/COVID-19/blob/master/LICENSE) |
| Iraq | [HDX](https://data.humdata.org/dataset/iraq-coronavirus-covid-19-subnational-cases) | [CC BY-SA][28] |
| Japan | <https://github.com/swsoyee/2019-ncov-japan> | [MIT](https://github.com/swsoyee/2019-ncov-japan/blob/master/LICENSE) |
| Libya | [HDX](https://data.humdata.org/dataset/libya-coronavirus-covid-19-subnational-cases) | [CC BY-SA][28] |
| Luxembourg | [data.public.lu](https://data.public.lu/fr/datasets/donnees-covid19)| [CC0](https://data.public.lu/fr/datasets/?license=cc-zero) |
| Malaysia | [Wikipedia](https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_Malaysia) | [CC BY-SA][24] |
| Mexico | <https://github.com/mexicovid19/Mexico-datos> | [MIT](https://github.com/mexicovid19/Mexico-datos/blob/master/LICENSE.md) |
| Mexico | [Secretaría de Salud Mexico](https://coronavirus.gob.mx/) | [Attribution Required](https://datos.gob.mx/libreusomx) |
| Netherlands | [RIVM](https://data.rivm.nl/covid-19) | [Public Domain](https://databronnencovid19.nl/Disclaimer) |
| Nicaragua | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| Norway | [COVID19 EU Data](https://github.com/covid19-eu-zh/covid19-eu-data) | [MIT](https://github.com/covid19-eu-zh/covid19-eu-data/issues/57) |
| Pakistan | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019-20_coronavirus_pandemic_data/Pakistan_medical_cases) | [CC BY-SA][24] |
| Panama | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| Paraguay | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| Peru | [Datos Abiertos Peru](https://www.datosabiertos.gob.pe/group/datos-abiertos-de-covid-19) | [ODC BY][31] |
| Philippines | [Philippines Department of Health](http://www.doh.gov.ph/covid19tracker) | [Attribution required](https://drive.google.com/file/d/1LzY2eLzZQdLR9yuoNufGEBN5Ily8ZTdV) |
| Poland | [COVID19 EU Data](https://github.com/covid19-eu-zh/covid19-eu-data) | [MIT](https://github.com/covid19-eu-zh/covid19-eu-data/issues/57) |
| Portugal | [COVID-19: Portugal](https://github.com/carlospramalheira/covid19) | [MIT](https://github.com/carlospramalheira/covid19/blob/master/LICENSE) |
| Romania | <https://github.com/adrianp/covid19romania> | [CC0](https://github.com/adrianp/covid19romania/blob/master/LICENSE) |
| Russia | <https://стопкоронавирус.рф> | [CC BY-SA][29] |
| Slovenia | <https://www.gov.si> | [CC BY-SA][24] |
| South Africa| [FinMango COVID-19 Data](https://finmango.org/covid) | [CC BY-SA](https://finmango.org/covid) |
| South Africa| [Data Science for Social Impact research group, the University of Pretoria](https://github.com/dsfsi/covid19za) | [CC BY-SA](https://github.com/dsfsi/covid19za/blob/master/data/LICENSE.md) |
| South Korea | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019%E2%80%9320_coronavirus_pandemic_data/South_Korea_medical_cases) | [CC BY-SA][24] |
| Spain | [Government Authority](https://covid19.isciii.es) | [Attribution required](https://www.mscbs.gob.es/avisoLegal/home.html) |
| Sudan | [HDX](https://data.humdata.org/dataset/sudan-coronavirus-covid-19-subnational-cases) | [CC BY-SA][28] |
| Sweden | [Public Health Agency of Sweden](https://www.folkhalsomyndigheten.se/the-public-health-agency-of-sweden/) |  |
| Switzerland | [OpenZH data](https://open.zh.ch) | [CC 4.0](https://github.com/openZH/covid_19/blob/master/LICENSE) |
| United Kingdom | <https://github.com/tomwhite/covid-19-uk-data> | [The Unlicense](https://github.com/tomwhite/covid-19-uk-data/blob/master/LICENSE.txt) |
| United Kingdom | <https://coronavirus-staging.data.gov.uk/> | Attribution required, [Open Government Licence v3.0][32] |
| Uruguay | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| USA | [NYT COVID Dataset](https://github.com/nytimes) | [Attribution required, non-commercial use](https://github.com/nytimes/covid-19-data/blob/master/LICENSE) |
| USA | [COVID Tracking Project](https://covidtracking.com) | [CC BY-NC 4.0](https://covidtracking.com/license) |
| USA (New York) | [New York City Health Department](https://www1.nyc.gov/site/doh/covid/covid-19-data.page) | [Public Domain](https://www1.nyc.gov/home/terms-of-use.page) |
| USA (Texas) | [Texas Department of State Health Services](https://dshs.texas.gov) | [Attribution required, non-commercial use](https://dshs.texas.gov/policy/copyright.shtm) |
| Venezuela | [HDX](https://data.humdata.org/dataset/corona-virus-covid-19-cases-and-deaths-in-venezuela) | [CC BY-SA][28] |
| Venezuela | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |



## Running the data extraction pipeline

To run the data extraction pipeline, first install the dependencies:
```sh
pip install -r requirements.txt
```

Then run the following script from the source folder to update all datasets:
```sh
cd src
python update.py
```

See the [source documentation](src) for more technical details.



## Acknowledgments

The following persons have made significant contributions to this project:

* Oscar Wahltinez
* Kevin Murphy
* Michael Brenner
* Matt Lee
* Anthony Erlinger
* Mayank Daswani
* Pranali Yawalkar



# Recommended citation

Please use the following when citing this project as a source of data:

```
@article{Wahltinez2020,
  author = "Oscar Wahltinez and Matt Lee and Anthony Erlinger and Mayank Daswani and Pranali Yawalkar and Kevin Murphy and Michael Brenner",
  year = 2020,
  title = "COVID-19 Open-Data: curating a fine-grained, global-scale data repository for SARS-CoV-2",
  note = "Work in progress",
 url = {https://github.com/GoogleCloudPlatform/covid-19-open-data},
}
```



[1]: https://github.com/CSSEGISandData/COVID-19
[2]: https://www.ecdc.europa.eu
[3]: https://github.com/BlankerL/DXY-COVID-19-Data
[4]: https://web.archive.org/web/20200314143253/https://www.salute.gov.it/nuovocoronavirus
[5]: https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports
[6]: https://github.com/open-covid-19/data/issues/16
[7]: https://github.com/open-covid-19/data/examples/data_loading.ipynb
[8]: https://web.archive.org/web/20200320122944/https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov-China/situacionActual.htm
[9]: https://covidtracking.com
[10]: https://github.com/pcm-dpc/COVID-19
[12]: https://open-covid-19.github.io/explorer
[13]: https://kepler.gl/demo/map?mapUrl=https://dl.dropboxusercontent.com/s/cofdctuogawgaru/COVID-19_Dataset.json
[14]: https://www.starlords3k.com/covid19.php
[15]: https://kiksu.net/covid-19/
[16]: https://www.canada.ca/en/public-health/services/diseases/2019-novel-coronavirus-infection.html
[17]: ./docs/google-mobility-reports.md
[18]: https://www.bsg.ox.ac.uk/research/research-projects/oxford-covid-19-government-response-tracker
[19]: https://auditter.info/covid-timeline
[20]: https://www.coronavirusdailytracker.info/
[21]: https://omnimodel.com/
[22]: https://cloud.google.com/marketplace/product/bigquery-public-datasets/covid19-open-data
[23]: https://www.wikidata.org/wiki/Wikidata:Licensing
[24]: https://en.wikipedia.org/wiki/Wikipedia:Copyrights
[25]: https://worldbank.org
[26]: https://github.com/DataScienceResearchPeru/covid-19_latinoamerica
[27]: https://github.com/DataScienceResearchPeru/covid-19_latinoamerica/blob/master/LICENSE.md
[28]: https://data.humdata.org/about/license
[29]: http://creativecommons.org/licenses/by/4.0/
[30]: https://reproduction.live/
[31]: http://opendefinition.org/licenses/odc-by/
[32]: https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/
