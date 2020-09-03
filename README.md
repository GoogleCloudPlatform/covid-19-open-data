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
| [Main](#main-table) | `[key][date]` | Flat table with records from (almost) all other tables joined by `date` and/or `key`; see below for more details | [main.csv](https://storage.googleapis.com/covid19-open-data/v2/main.csv) | All tables below |
| [Index](./docs/table-index.md) | `[key]` | Various names and codes, useful for joining with other datasets | [index.csv](https://storage.googleapis.com/covid19-open-data/v2/index.csv), [index.json](https://storage.googleapis.com/covid19-open-data/v2/index.json) | Wikidata, DataCommons |
| [Demographics](./docs/table-demographics.md) | `[key]` | Various (current<sup>3</sup>) population statistics | [demographics.csv](https://storage.googleapis.com/covid19-open-data/v2/demographics.csv), [demographics.json](https://storage.googleapis.com/covid19-open-data/v2/demographics.json) | Wikidata, DataCommons |
| [Economy](./docs/table-economy.md) | `[key]` | Various (current<sup>3</sup>) economic indicators | [economy.csv](https://storage.googleapis.com/covid19-open-data/v2/economy.csv), [economy.json](https://storage.googleapis.com/covid19-open-data/v2/economy.json) | Wikidata, DataCommons |
| [Epidemiology](./docs/table-epidemiology.md) | `[key][date]` | COVID-19 cases, deaths, recoveries and tests | [epidemiology.csv](https://storage.googleapis.com/covid19-open-data/v2/epidemiology.csv), [epidemiology.json](https://storage.googleapis.com/covid19-open-data/v2/epidemiology.json) | Various<sup>2</sup> |
| [Geography](./docs/table-geography.md) | `[key]` | Geographical information about the region | [geography.csv](https://storage.googleapis.com/covid19-open-data/v2/geography.csv), [geography.json](https://storage.googleapis.com/covid19-open-data/v2/geography.json) | Wikidata |
| [Health](./docs/table-health.md) | `[key]` | Health indicators for the region | [health.csv](https://storage.googleapis.com/covid19-open-data/v2/health.csv), [health.json](https://storage.googleapis.com/covid19-open-data/v2/geography.json) | Wikidata, WorldBank |
| [Hospitalizations](./docs/table-hospitalizations.md) | `[key][date]` | Information related to patients of COVID-19 and hospitals |  [hospitalizations.csv](https://storage.googleapis.com/covid19-open-data/v2/hospitalizations.csv), [hospitalizations.json](https://storage.googleapis.com/covid19-open-data/v2/hospitalization.json) | Various<sup>2</sup> |
| [Mobility](./docs/table-mobility.md) | `[key][date]` | Various metrics related to the movement of people.<br/><br/>To download or use the data, you must agree to the Google [Terms of Service](https://policies.google.com/terms). | [mobility.csv](https://storage.googleapis.com/covid19-open-data/v2/mobility.json), [mobility.json](https://storage.googleapis.com/covid19-open-data/v2/mobility.json) | Google |
| [Search Trends](./docs/table-search-trends.md) | `[key][date]` | Trends in symptom search volumes due to COVID-19.<br/><br/>To download or use the data, you must agree to the Google [Terms of Service](https://policies.google.com/terms). | [search-trends-daily.csv](https://storage.googleapis.com/covid19-open-data/v2/search-trends-daily.csv), [search-trends-daily.json](https://storage.googleapis.com/covid19-open-data/v2/search-trends-daily.json) | Google |
| [Government Response](./docs/table-government-response.md) | `[key][date]` | Government interventions and their relative stringency | [oxford-government-response.csv](https://storage.googleapis.com/covid19-open-data/v2/oxford-government-response.csv), [oxford-government-response.json](https://storage.googleapis.com/covid19-open-data/v2/oxford-government-response.json) | University of Oxford |
| [Weather](./docs/table-weather.md) | `[key][date]` | Dated meteorological information for each region | [weather.csv](https://storage.googleapis.com/covid19-open-data/v2/weather.csv) | NOAA |
| [WorldBank](./docs/table-worldbank.md) | `[key]` | Latest record for each indicator from WorldBank for all reporting countries | [worldbank.csv](https://storage.googleapis.com/covid19-open-data/v2/worldbank.csv), [worldbank.json](https://storage.googleapis.com/covid19-open-data/v2/worldbank.json) | WorldBank |
| [By Age](./docs/table-by-age.md) | `[key][date]` | Epidemiology and hospitalizations data stratified by age | [by-age.csv](https://storage.googleapis.com/covid19-open-data/v2/by-age.csv), [by-age.json](https://storage.googleapis.com/covid19-open-data/v2/by-age.json) | Various<sup>2</sup> |
| [By Sex](./docs/table-by-sex.md) | `[key][date]` | Epidemiology and hospitalizations data stratified by sex | [by-sex.csv](https://storage.googleapis.com/covid19-open-data/v2/by-sex.csv), [by-sex.json](https://storage.googleapis.com/covid19-open-data/v2/by-sex.json) | Various<sup>2</sup> |

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

The data is available as CSV and JSON files, which are published in Google Cloud Storage so they can
be served directly to Javascript applications without the need of a proxy to set the correct headers
for CORS and content type.

For the purpose of making the data as easy to use as possible, there is a [main](#main) table
which contains the columns of all other tables joined by `key` and `date`. However,
performance-wise, it may be better to download the data separately and join the tables locally.

Each region has its own version of the main table, so you can pull all the data for a specific
region using a single endpoint, the URL for each region is:
* Data for `key` in CSV format: `https://storage.googleapis.com/covid19-open-data/v2/${key}/main.csv`
* Data for `key` in JSON format: `https://storage.googleapis.com/covid19-open-data/v2/${key}/main.json`

Each table has a full version as well as subsets with only the last day of data.
The full version is accessible at the URL described [in the table above](#open-covid-19-dataset).
The subsets can be found by appending `latest` to the path. For example, the subsets of the main
table are available at the following locations:
* Latest: https://storage.googleapis.com/covid19-open-data/v2/latest/main.csv
* Time series: https://storage.googleapis.com/covid19-open-data/v2/main.csv

Note that the `latest` version contains the last non-null record for each key. All of the above
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
[online query editor](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=covid19_open_data&page=dataset).

### Google Colab
You can use Google Colab if you want to run your analysis without having to install anything in your
computer, simply go to this URL: https://colab.research.google.com/github/GoogleCloudPlatform/covid-19-open-data.

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

For information about each table, see the corresponding documentation linked
[above](#covid-19-open-data).

### Main table
Flat table with records from all other tables joined by `key` and `date`. See below for information
about all the different tables and columns. Tables not included in the main table are:
* [WorldBank](./docs/table-worldbank.md): A subset of individual indicators are added as columns to other
  tables instead; for example, the [health](./docs/table-health.md) table.
* [By Age](./docs/table-by-age.md): Age breakdowns of epidemiology and hospitalization data are normalized and
  added to the by-age-normalized table instead (TABLE TO BE ADDED).
* [By Sex](./docs/table-by-sex.md): Sex breakdowns of epidemiology and hospitalization data are normalized and
  added to the by-sex-normalized table instead (TABLE TO BE ADDED).

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



## Contribute

Technical contributions to the data extraction pipeline are welcomed, take a look at the
[source directory](./src/README.md) for more information.

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
<details>
<summary>Show data sources</summary>


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
| Google Search Trends | <https://console.cloud.google.com/marketplace/product/bigquery-public-datasets/covid19-search-trends> | [Google Terms of Service](https://policies.google.com/terms) |
| Government response data | [Oxford COVID-19 government response tracker][18] | [CC BY 4.0](https://github.com/OxCGRT/covid-policy-tracker/blob/master/LICENSE.txt) |
| Country-level data | [ECDC](https://www.ecdc.europa.eu) | [Attribution required](https://www.ecdc.europa.eu/en/copyright) |
| Country-level data | [Our World in Data](https://ourworldindata.org) | [CC BY 4.0](https://ourworldindata.org/how-to-use-our-world-in-data#how-is-our-work-copyrighted) |
| Afghanistan | [HDX](https://data.humdata.org/dataset/afghanistan-covid-19-statistics-per-province) | [CC BY-SA][28] |
| Argentina | [Datos Argentina](https://datos.gob.ar/) | [Public domain](https://datos.gob.ar/acerca/seccion/marco-legal) |
| Australia | <https://covid-19-au.com/> | [Attribution required, educational and academic research purposes](https://covid-19-au.com/faq) |
| Austria | [COVID19 EU Data](https://github.com/covid19-eu-zh/covid19-eu-data) | [MIT](https://github.com/covid19-eu-zh/covid19-eu-data/issues/57) |
| Bangladesh | [HDX](https://data.humdata.org/dataset/district-wise-quarantine-for-covid-19) | [CC BY-SA][28] |
| Belgium | [Belgian institute for health](https://epistat.wiv-isp.be) | [Attribution required, educational and academic research purposes](https://www.health.belgium.be/en/legal-information) |
| Bolivia | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| Brazil | [Brazil Ministério da Saúde](https://coronavirus.saude.gov.br/) | [Creative Commons Atribuição](http://www.opendefinition.org/licenses/cc-by) |
| Brazil (Rio de Janeiro) | <http://www.data.rio/> | [Dados abertos](https://www.data.rio/datasets/f314453b3a55434ea8c8e8caaa2d8db5) |
| Brazil (Ceará) | <https://saude.ce.gov.br> | [Dados abertos](https://cearatransparente.ce.gov.br/portal-da-transparencia) |
| Canada | [Department of Health Canada](https://www.canada.ca/en/public-health) | [Attribution required](https://www.canada.ca/en/transparency/terms.html) |
| Canada | [COVID-19 Canada Open Data Working Group](https://art-bd.shinyapps.io/covid19canada/) | [CC BY-SA](https://github.com/ishaberry/Covid19Canada/blob/master/LICENSE.MD) |
| Chile | [Ministerio de Ciencia de Chile](http://www.minciencia.gob.cl/COVID19) | [Terms of use](http://www.minciencia.gob.cl/sites/default/files/1771596.pdf) |
| Chile (2017 Census) | [Instituto Nacional de Estadística](https://www.ine.cl/estadisticas/sociales/censos-de-poblacion-y-vivienda/poblacion-y-vivienda) | [CC BY-SA](https://datos.gob.cl/) |
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
| Mexico | [Secretaría de Salud Mexico](https://coronavirus.gob.mx/) | [Attribution Required](https://datos.gob.mx/libreusomx) |
| Mexico (2010 Census) | [INEGI](https://www.inegi.org.mx/programas/ccpv/2010/default.html) | [Attribution Required](https://datos.gob.mx/libreusomx) |
| Netherlands | [RIVM](https://data.rivm.nl/covid-19) | [Public Domain](https://databronnencovid19.nl/Disclaimer) |
| Nicaragua | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| Norway | [COVID19 EU Data](https://github.com/covid19-eu-zh/covid19-eu-data) | [MIT](https://github.com/covid19-eu-zh/covid19-eu-data/issues/57) |
| Pakistan | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019-20_coronavirus_pandemic_data/Pakistan_medical_cases) | [CC BY-SA][24] |
| Panama | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| Paraguay | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| Peru | [Datos Abiertos Peru](https://www.datosabiertos.gob.pe/group/datos-abiertos-de-covid-19) | [ODC BY][31] |
| Peru (2017 Census) | [INEI](https://censos2017.inei.gob.pe/redatam/) | [ODC BY][31] |
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
| Spain (Canary Islands) | [Gobierno de Canarias](https://www3.gobiernodecanarias.org/sanidad/scs/tematica.jsp?idCarpeta=e01092c2-7d66-11ea-871d-cb574c2473a4) | [Attribution required](https://www.gobiernodecanarias.org/principal/avisolegal.html) |
| Spain (Madrid) | [Datos Abiertos Madrid](https://www.comunidad.madrid/gobierno/datos-abiertos) | [Attribution required](https://www.comunidad.madrid/gobierno/datos-abiertos/reutiliza#condiciones-uso) |
| Sudan | [HDX](https://data.humdata.org/dataset/sudan-coronavirus-covid-19-subnational-cases) | [CC BY-SA][28] |
| Sweden | [Public Health Agency of Sweden](https://www.folkhalsomyndigheten.se/the-public-health-agency-of-sweden/) |  |
| Switzerland | [OpenZH data](https://open.zh.ch) | [CC 4.0](https://github.com/openZH/covid_19/blob/master/LICENSE) |
| Ukraine | [National Security and Defense Council of Ukraine](https://covid19.rnbo.gov.ua/) | [CC 4.0](https://www.kmu.gov.ua/#layout-footer) |
| United Kingdom | <https://github.com/tomwhite/covid-19-uk-data> | [The Unlicense](https://github.com/tomwhite/covid-19-uk-data/blob/master/LICENSE.txt) |
| United Kingdom | <https://coronavirus.data.gov.uk/> | Attribution required, [Open Government Licence v3.0][32] |
| Uruguay | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |
| USA | [NYT COVID Dataset](https://github.com/nytimes) | [Attribution required, non-commercial use](https://github.com/nytimes/covid-19-data/blob/master/LICENSE) |
| USA | [COVID Tracking Project](https://covidtracking.com) | [CC BY-NC 4.0](https://covidtracking.com/license) |
| USA (New York) | [New York City Health Department](https://www1.nyc.gov/site/doh/covid/covid-19-data.page) | [Public Domain](https://www1.nyc.gov/home/terms-of-use.page) |
| USA (Texas) | [Texas Department of State Health Services](https://dshs.texas.gov) | [Attribution required, non-commercial use](https://dshs.texas.gov/policy/copyright.shtm) |
| Venezuela | [HDX](https://data.humdata.org/dataset/corona-virus-covid-19-cases-and-deaths-in-venezuela) | [CC BY-SA][28] |
| Venezuela | [Latin America Covid-19 Data Repository][26] | [CC BY-SA][27] |

</details>



## Running the data extraction pipeline

See the [source documentation](src) for more technical details.



## Acknowledgments and collaborations

This project has been done in collaboration with [FinMango](https://finmango.org/covid), which
provided great insights about the impact of the pandemic on the local economies and also helped with
research and manual curation of data sources for many regions including South Africa and US states.

The following persons have made significant contributions to this project:

* Oscar Wahltinez
* Kevin Murphy
* Michael Brenner
* Matt Lee
* Anthony Erlinger
* Mayank Daswani
* Pranali Yawalkar
* Zack Ontiveros
* Ruth Alcantara
* Donny Cheung



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
[17]: https://www.google.com/covid19/mobility/
[18]: https://www.bsg.ox.ac.uk/research/research-projects/oxford-covid-19-government-response-tracker
[19]: https://auditter.info/covid-timeline
[20]: https://www.coronavirusdailytracker.info/
[21]: https://omnimodel.com/
[22]: https://console.cloud.google.com/marketplace/product/bigquery-public-datasets/covid19-open-data
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
